package kvdb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/xiatianliang1024gm/kvdb/internal/key"
)

// 这个文件是 M7（docs/EXTENSIONS.md §4.1 + §5.1）的验收测试：范围删除
// 与半开上界 / Prefix 扫描。
//
// 范围删除的五条验收项（EXTENSIONS.md §4.1）对应关系：
//
//	1. 删一个范围后范围内 Get 不到、范围外不受影响      → TestDeleteRangeBasics
//	2. 对之前取的快照不可见                            → TestDeleteRangeSnapshotIsolation
//	3. 覆盖全范围的 Compaction 后空间回收、墓碑退休    → TestDeleteRangeCompactionReclaims
//	4. 崩溃重启后范围墓碑仍在                          → TestDeleteRangeSurvivesRestart
//	5. 更深层还有数据时范围墓碑不得退休                → TestDeleteRangeKeepsTombstoneWhileDeepDataExists

// ── 验收 1：可见性与批次编解码 ────────────────────────────────────

func TestDeleteRangeBasics(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		mustPut(t, db, k, "v-"+k)
	}

	// 范围删除与其它操作混在同一个批次里：批次原子性对它同样成立。
	b := NewWriteBatch()
	if err := b.Put([]byte("x"), []byte("v-x")); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteRange([]byte("c"), []byte("f")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := db.Write(b); err != nil {
		t.Fatal(err)
	}

	// [c, f) 内的键消失，边界与范围外完好。
	for _, k := range []string{"a", "c", "d", "e"} {
		mustMiss(t, db, k)
	}
	for _, k := range []string{"b", "f", "g", "h", "x"} {
		if got := mustGet(t, db, k); got != "v-"+k {
			t.Fatalf("Get(%q) = %q", k, got)
		}
	}

	// 迭代器同样看不到被遮蔽的键。
	it := db.NewIterator(nil)
	defer it.Close()
	var got []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"b", "f", "g", "h", "x"}; !equalSlices(got, want) {
		t.Fatalf("迭代 = %v, want %v", got, want)
	}

	// 空区间是调用方 bug，直接报错。
	if err := db.DeleteRange([]byte("f"), []byte("c")); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("DeleteRange(f, c) = %v, want ErrInvalidRange", err)
	}
	if err := db.DeleteRange([]byte("f"), []byte("f")); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("DeleteRange(f, f) = %v, want ErrInvalidRange", err)
	}
	if err := db.DeleteRange(nil, []byte("c")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("DeleteRange(nil, c) = %v, want ErrEmptyKey", err)
	}
}

func TestWriteBatchDeleteRangeCodec(t *testing.T) {
	b := NewWriteBatch()
	if err := b.Put([]byte("p"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteRange([]byte("a"), []byte("m")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("z")); err != nil {
		t.Fatal(err)
	}
	if b.Len() != 3 {
		t.Fatalf("Len = %d, want 3", b.Len())
	}

	// 编码 → 解码（WAL 重放的路径）往返后记录必须一致。
	decoded, err := decodeBatch(b.Encode())
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	type rec struct {
		seq   uint64
		kind  key.Kind
		uk    string
		value string
	}
	var got []rec
	if err := decoded.rangeRecords(b.Sequence(), func(seq uint64, kind key.Kind, uk, value []byte) bool {
		got = append(got, rec{seq, kind, string(uk), string(value)})
		return true
	}); err != nil {
		t.Fatal(err)
	}
	want := []rec{
		{0, key.TypeValue, "p", "1"},
		{1, key.TypeRangeDeletion, "a", "m"},
		{2, key.TypeDeletion, "z", ""},
	}
	if len(got) != len(want) {
		t.Fatalf("记录数 = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("记录 %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// ── 验收 2：快照隔离 ─────────────────────────────────────────────

func TestDeleteRangeSnapshotIsolation(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	for _, k := range []string{"a", "b", "c"} {
		mustPut(t, db, k, "v-"+k)
	}

	snap := db.GetSnapshot()
	defer snap.Release()

	if err := db.DeleteRange([]byte("a"), []byte("c")); err != nil {
		t.Fatal(err)
	}

	// 新读者看不到 [a, c) 里的任何键。
	mustMiss(t, db, "a")
	mustMiss(t, db, "b")
	if got := mustGet(t, db, "c"); got != "v-c" {
		t.Fatalf("Get(c) = %q", got)
	}

	// 快照取在删除之前：它的视图纹丝不动（点查与迭代两条路径都要验证）。
	if v, err := snap.Get([]byte("a")); err != nil || string(v) != "v-a" {
		t.Fatalf("snap.Get(a) = (%q, %v), want (v-a, nil)", v, err)
	}
	it := snap.NewIterator(nil)
	defer it.Close()
	var got []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if want := []string{"a", "b", "c"}; !equalSlices(got, want) {
		t.Fatalf("快照迭代 = %v, want %v", got, want)
	}
}

// ── 验收 3：Compaction 回收空间并退休墓碑 ────────────────────────

func TestDeleteRangeCompactionReclaims(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 32 << 10
		o.L0CompactionTrigger = 2
		o.LevelBaseSize = 1 << 20
	})
	defer db.Close()

	const n = 200
	for i := 0; i < n; i++ {
		if err := db.Put(benchKey(i), []byte("payload-payload-payload")); err != nil {
			t.Fatal(err)
		}
	}
	// 数据全在内存时发范围删除：墓碑提交到 Version，旧记录还躺在 MemTable 里。
	if err := db.DeleteRange(benchKey(0), benchKey(n)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i += 7 {
		mustMiss(t, db, string(benchKey(i)))
	}

	// 用区间外的前缀（"zzz" 排在 "key" 之后）灌 filler 把 MemTable 挤出去：
	// 被遮蔽的旧记录随 Flush 进 L0，再被 Compaction 整段丢弃 —— 这是范围删除
	// 唯一真正回收空间的时刻；区间内再无数据后墓碑随之退休。
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = 'z'
	}
	const filler = 4000
	for i := 0; i < filler; i++ {
		if err := db.Put([]byte(fmt.Sprintf("zzz%06d", i)), payload); err != nil {
			t.Fatal(err)
		}
	}

	// 等后台收敛到"墓碑已退休"：它退休的前提是区间内再无任何数据，
	// 所以这个条件同时保证了被遮蔽的记录已经被物理清掉。
	waitForStats(t, db, "范围墓碑退休", func(s Stats) bool {
		return s.RangeTombstones == 0 && !s.HasImmutable
	})

	// 范围内的键仍然全部不可见，filler 完好，遮蔽丢弃确实发生了。
	for i := 0; i < n; i += 7 {
		mustMiss(t, db, string(benchKey(i)))
	}
	for i := 0; i < filler; i += 97 {
		if v, err := db.Get([]byte(fmt.Sprintf("zzz%06d", i))); err != nil || len(v) != len(payload) {
			t.Fatalf("filler %d 丢失或损坏: %v", i, err)
		}
	}
	if s := db.Stats(); s.Compaction.DroppedRecords == 0 {
		t.Fatal("Compaction 没有丢弃任何记录，范围遮蔽没有生效")
	}
	it := db.NewIterator(&IteratorOptions{Prefix: benchKey(0)[:3]})
	defer it.Close()
	if it.Valid() {
		t.Fatalf("范围内不应再有任何键，却扫到 %q", it.Key())
	}
}

// ── 验收 4：重启后范围墓碑仍在 ───────────────────────────────────

func TestDeleteRangeSurvivesRestart(t *testing.T) {
	opts := DefaultOptions(t.TempDir())
	opts.SyncWrites = true
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"a", "b", "c", "d"} {
		mustPut(t, db, k, "v-"+k)
	}
	if err := db.DeleteRange([]byte("b"), []byte("d")); err != nil {
		t.Fatal(err)
	}
	// Close 刻意不落 MemTable：重启必然走 WAL 重放，
	// 范围墓碑的 Manifest 持久化与重放幂等性一起被这里覆盖。
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	mustGet(t, db2, "a")
	mustMiss(t, db2, "b")
	mustMiss(t, db2, "c")
	mustGet(t, db2, "d")
	if s := db2.Stats(); s.RangeTombstones != 1 {
		t.Fatalf("重启后 RangeTombstones = %d, want 1（重复提交必须被去重，不能翻倍）", s.RangeTombstones)
	}
}

// ── 验收 5：更深层还有数据时墓碑不得退休 ─────────────────────────

func TestDeleteRangeKeepsTombstoneWhileDeepDataExists(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 32 << 10
		o.L0CompactionTrigger = 2
		o.LevelBaseSize = 1 << 20
	})
	defer db.Close()

	// 数据落盘（Flush / Compaction 收敛），然后才发范围删除。
	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = 'k'
	}
	const n = 100
	for i := 0; i < n; i++ {
		if err := db.Put(benchKey(i), payload); err != nil {
			t.Fatal(err)
		}
	}
	waitConverged(t, db)

	// 快照取在范围删除**之前**：它必须继续看到全部旧数据。
	snap := db.GetSnapshot()
	defer snap.Release()

	if err := db.DeleteRange(benchKey(0), benchKey(n)); err != nil {
		t.Fatal(err)
	}
	// 之后没有任何写入，Compaction 没有理由再动这些文件：
	// 墓碑必须留在 Version 上继续遮蔽磁盘里的旧数据。
	waitConverged(t, db)
	if s := db.Stats(); s.RangeTombstones != 1 {
		t.Fatalf("区间内还有数据时 RangeTombstones = %d, want 1（提前退休会让旧值复活）", s.RangeTombstones)
	}

	// 磁盘上明明还有数据，读路径全靠墓碑遮蔽。
	for i := 0; i < n; i += 7 {
		mustMiss(t, db, string(benchKey(i)))
	}

	for i := 0; i < n; i += 7 {
		if v, err := snap.Get(benchKey(i)); err != nil || len(v) != len(payload) {
			t.Fatalf("快照应当看到 benchKey(%d): %v", i, err)
		}
	}
}

// ── 5.1：半开上界与 Prefix ───────────────────────────────────────

func TestHalfOpenUpperBoundOnDisk(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, db, k, "v")
	}

	// 上界不含：[b, d) 应该正好是 b、c。
	it := db.NewIterator(&IteratorOptions{LowerBound: []byte("b"), UpperBound: []byte("d")})
	var got []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	it.Close()
	if want := []string{"b", "c"}; !equalSlices(got, want) {
		t.Fatalf("[b, d) = %v, want %v", got, want)
	}

	// Seek 到半开上界（target == upper）必须直接失效。
	it2 := db.NewIterator(&IteratorOptions{UpperBound: []byte("c")})
	defer it2.Close()
	it2.Seek([]byte("c"))
	if it2.Valid() {
		t.Fatalf("Seek(upper) 应当失效，却指向 %q", it2.Key())
	}

	// Prefix：全 0xFF 前缀没有后继，等价于无上界。
	pfx := db.NewIterator(&IteratorOptions{Prefix: []byte{0xFF, 0xFF}})
	defer pfx.Close()
	pfx.SeekToFirst()
	if pfx.Valid() {
		t.Fatalf("0xFFFF 前缀不该匹配任何 key，却扫到 %q", pfx.Key())
	}
}

func TestPrefixScanGroupsByPrefix(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	for _, k := range []string{"user:1", "user:2", "user:3", "session:1", "z"} {
		mustPut(t, db, k, "v")
	}

	it := db.NewIterator(&IteratorOptions{Prefix: []byte("user:")})
	defer it.Close()
	var got []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"user:1", "user:2", "user:3"}; !equalSlices(got, want) {
		t.Fatalf("Prefix(user:) = %v, want %v", got, want)
	}
}
