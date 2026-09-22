package kvdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// 这个文件是 M8（docs/EXTENSIONS.md §4.3）的 db 层验收测试。
//
// 验收项与用例的对应关系：
//
//	1. 连续 1000 次 Merge(+1) 读回正确            → TestMergeThousandIncrements
//	2. Compaction 之后值仍正确、链被折叠          → TestMergeCompactionFoldsChain
//	3. 快照点之后的 merge 对快照不可见            → TestMergeSnapshotIsolation
//	4. Merge → Delete → Merge 序列结果正确        → TestMergeDeleteMergeSequence
//	5. 经 WAL 重放后（重启）merge 结果仍正确      → TestMergeSurvivesRestart（含在 1 里）
//	6. 换算子名 / 去掉算子打开 ⇒ 拒绝             → TestMergeOperatorNameFrozen
//	7. 迭代器折叠（含跨来源、边界、Next 推进）    → TestMergeIteratorFolding
//	8. 范围删除遮蔽 operand 链                    → TestMergeAfterRangeDeletion
//	9. 未配算子时写入被拒                         → TestMergeWithoutOperatorRejected

// incrOp 是计数器算子：value = 8 字节大端 uint64，折叠 = 求和（满足结合律）。
type incrOp struct{ name string }

func (o incrOp) Name() string {
	if o.name == "" {
		return "kvdb.test.incr"
	}
	return o.name
}

func (incrOp) FullMerge(userKey, base []byte, operands [][]byte) ([]byte, error) {
	var acc uint64
	if base != nil {
		acc = binary.BigEndian.Uint64(base)
	}
	for _, op := range operands {
		acc += binary.BigEndian.Uint64(op)
	}
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, acc)
	return out, nil
}

func (incrOp) PartialMerge(userKey, a, b []byte) ([]byte, bool) {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, binary.BigEndian.Uint64(a)+binary.BigEndian.Uint64(b))
	return out, true
}

// strictIncrOp 与 incrOp 相同，但 PartialMerge 恒 ok=false：
// 不满足（或不愿承诺）结合律的算子必须走这条路径，引擎要保证结果仍然正确。
type strictIncrOp struct{ incrOp }

func (strictIncrOp) PartialMerge(userKey, a, b []byte) ([]byte, bool) { return nil, false }

// boomOp 的 FullMerge 永远报错：验证读路径把算子故障如实上抛。
type boomOp struct{}

func (boomOp) Name() string { return "kvdb.test.boom" }
func (boomOp) FullMerge(userKey, base []byte, operands [][]byte) ([]byte, error) {
	return nil, errors.New("boom")
}
func (boomOp) PartialMerge(userKey, a, b []byte) ([]byte, bool) { return nil, false }

func counter(n uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return b[:]
}

func readCounter(t *testing.T, db *DB, k string) uint64 {
	t.Helper()
	v, err := db.Get([]byte(k))
	if err != nil {
		t.Fatalf("Get(%q): %v", k, err)
	}
	if len(v) != 8 {
		t.Fatalf("Get(%q) 长度 = %d, want 8", k, len(v))
	}
	return binary.BigEndian.Uint64(v)
}

func mustMerge(t *testing.T, db *DB, k string, delta uint64) {
	t.Helper()
	if err := db.Merge([]byte(k), counter(delta)); err != nil {
		t.Fatalf("Merge(%q, +%d): %v", k, delta, err)
	}
}

// ── 验收 1 + 5：1000 次自增读回正确；重启（WAL 重放）后仍正确 ────

func TestMergeThousandIncrements(t *testing.T) {
	dir := t.TempDir()
	mk := func() Options {
		opts := DefaultOptions(dir)
		opts.SyncWrites = true // 保证每条 merge 都已落 WAL
		opts.MergeOperator = incrOp{}
		return opts
	}

	db, err := Open(mk())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("ctr"), counter(0)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		mustMerge(t, db, "ctr", 1)
	}
	if got := readCounter(t, db, "ctr"); got != 1000 {
		t.Fatalf("1000 次自增后 = %d, want 1000", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Close 刻意不落 MemTable：重启必然重放整条 WAL，包括全部 merge 记录。
	db2, err := Open(mk())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := readCounter(t, db2, "ctr"); got != 1000 {
		t.Fatalf("WAL 重放后 = %d, want 1000", got)
	}
}

// ── 验收 2：Compaction 之后值仍正确，链被折叠成单条 Value ─────────

func TestMergeCompactionFoldsChain(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MergeOperator = incrOp{}
		o.MemTableSize = 32 << 10
		o.L0CompactionTrigger = 2
		o.LevelBaseSize = 1 << 20
	})
	defer db.Close()

	if err := db.Put([]byte("ctr"), counter(0)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		mustMerge(t, db, "ctr", 1)
	}
	// 灌 filler（互不重复的 key，不产生覆盖丢弃）把 MemTable 挤出去并触发
	// L0 Compaction：折叠发生在后台，这是"读路径的固有成本由 Compaction
	// 抵消"的直接验证。
	payload := strings.Repeat("z", 256)
	for i := 0; i < 400; i++ {
		if err := db.Put([]byte(fmt.Sprintf("zzz%06d", i)), []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	waitForStats(t, db, "Compaction 收敛", func(s Stats) bool {
		return !s.HasImmutable && len(s.Levels) > 1 && s.Levels[1].Files > 0
	})

	if got := readCounter(t, db, "ctr"); got != 1000 {
		t.Fatalf("Compaction 后 = %d, want 1000", got)
	}
	// 1001 条链记录（base + 1000 operand）被折叠：它们都应计入 DroppedRecords。
	// filler 的 key 互不重复，不会贡献丢弃数。
	if s := db.Stats(); s.Compaction.DroppedRecords < 1000 {
		t.Errorf("DroppedRecords = %d, want >= 1000（链必须被折叠而不是原样堆积）", s.Compaction.DroppedRecords)
	}

	// 折叠后的数据经重启（走 Manifest + SST 路径）仍然正确。
	dir := db.opts.Dir
	op := db.opts.MergeOperator
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions(dir)
	opts.MergeOperator = op
	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := readCounter(t, db2, "ctr"); got != 1000 {
		t.Fatalf("重启后 = %d, want 1000", got)
	}
}

// PartialMerge 恒 ok=false 的算子（不承诺结合律）必须同样正确：
// 引擎原样保留 operand，全靠 FullMerge 折叠。
func TestMergeCompactionWithStrictOperator(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MergeOperator = strictIncrOp{}
		o.MemTableSize = 32 << 10
		o.L0CompactionTrigger = 2
	})
	defer db.Close()

	if err := db.Put([]byte("ctr"), counter(0)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		mustMerge(t, db, "ctr", 2)
	}
	payload := strings.Repeat("z", 256)
	for i := 0; i < 400; i++ {
		if err := db.Put([]byte(fmt.Sprintf("zzz%06d", i)), []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	waitForStats(t, db, "Compaction 收敛", func(s Stats) bool {
		return !s.HasImmutable && len(s.Levels) > 1 && s.Levels[1].Files > 0
	})
	if got := readCounter(t, db, "ctr"); got != 400 {
		t.Fatalf("Compaction 后 = %d, want 400", got)
	}
}

// ── 验收 3：快照隔离 ─────────────────────────────────────────────

func TestMergeSnapshotIsolation(t *testing.T) {
	db := openTestDB(t, func(o *Options) { o.MergeOperator = incrOp{} })
	defer db.Close()

	if err := db.Put([]byte("ctr"), counter(10)); err != nil {
		t.Fatal(err)
	}
	snap := db.GetSnapshot()
	defer snap.Release()

	mustMerge(t, db, "ctr", 1)
	mustMerge(t, db, "ctr", 2)

	if got := readCounter(t, db, "ctr"); got != 13 {
		t.Fatalf("最新视图 = %d, want 13", got)
	}
	// 快照看到的必须是"取快照那一刻"的完整折叠结果。
	v, err := snap.Get([]byte("ctr"))
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint64(v); got != 10 {
		t.Fatalf("快照视图 = %d, want 10", got)
	}

	// 快照迭代器同理。
	it := snap.NewIterator(nil)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if got := binary.BigEndian.Uint64(it.Value()); got != 10 {
			t.Fatalf("快照迭代器 = %d, want 10", got)
		}
	}
	it.Close()
}

// ── 验收 4：Merge → Delete → Merge ──────────────────────────────

func TestMergeDeleteMergeSequence(t *testing.T) {
	db := openTestDB(t, func(o *Options) { o.MergeOperator = incrOp{} })
	defer db.Close()

	mustMerge(t, db, "k", 2) // 无 base 的链：FullMerge(nil, [2])
	mustMerge(t, db, "k", 3)
	if got := readCounter(t, db, "k"); got != 5 {
		t.Fatalf("删除前 = %d, want 5", got)
	}

	// 删除之前的快照仍能看到 5。
	snap := db.GetSnapshot()
	defer snap.Release()

	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	mustMiss(t, db, "k")

	// 删除之后再叠：从 nil base 重新开始，墓碑之前的链全部作废。
	mustMerge(t, db, "k", 7)
	if got := readCounter(t, db, "k"); got != 7 {
		t.Fatalf("删除后再叠 = %d, want 7", got)
	}
	v, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint64(v); got != 5 {
		t.Fatalf("快照视图 = %d, want 5", got)
	}

	// Put 覆盖之后 Merge 照常叠加。
	if err := db.Put([]byte("k"), counter(100)); err != nil {
		t.Fatal(err)
	}
	mustMerge(t, db, "k", 1)
	if got := readCounter(t, db, "k"); got != 101 {
		t.Fatalf("Put 后再叠 = %d, want 101", got)
	}
}

// ── M7 × M8：范围删除遮蔽 operand 链 ─────────────────────────────

func TestMergeAfterRangeDeletion(t *testing.T) {
	db := openTestDB(t, func(o *Options) { o.MergeOperator = incrOp{} })
	defer db.Close()

	mustMerge(t, db, "ctr", 100)
	mustMerge(t, db, "ctr", 20)
	if err := db.DeleteRange([]byte("ctr"), []byte("ctr\x00")); err != nil {
		t.Fatal(err)
	}
	// 范围墓碑把整条链都盖住了：链内记录逐条判遮蔽后全部跳过，
	// 新 operand 从 nil base 重新开始。
	mustMerge(t, db, "ctr", 5)
	if got := readCounter(t, db, "ctr"); got != 5 {
		t.Fatalf("范围删除后 = %d, want 5", got)
	}
}

// ── 验收 6：算子名字写进 Manifest，记过即冻结 ─────────────────────

func TestMergeOperatorNameFrozen(t *testing.T) {
	dir := t.TempDir()
	mk := func(name string) Options {
		opts := DefaultOptions(dir)
		opts.SyncWrites = false
		opts.MergeOperator = incrOp{name: name}
		return opts
	}

	db, err := Open(mk("kvdb.test.incr.v1"))
	if err != nil {
		t.Fatal(err)
	}
	mustMerge(t, db, "k", 1)
	mustMerge(t, db, "k", 2)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 换名字 → 拒绝。
	if _, err := Open(mk("kvdb.test.incr.v2")); err == nil {
		t.Fatal("换算子名打开同一目录应当报配置不匹配")
	} else if !strings.Contains(err.Error(), "merge operator") {
		t.Fatalf("错误信息应当提到 merge operator，得到：%v", err)
	}
	// 去掉算子 → 同样拒绝：没有算子，未折叠的 operand 读不回来。
	if _, err := Open(DefaultOptions(dir)); err == nil {
		t.Fatal("已记录算子名的目录不允许在无算子下打开")
	}
	// 原名 → 正常，且值正确。
	db2, err := Open(mk("kvdb.test.incr.v1"))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := readCounter(t, db2, "k"); got != 3 {
		t.Fatalf("原名打开后 = %d, want 3", got)
	}

	// 反方向：没写过 merge 记录的目录，配不配算子都随便。
	plainDir := t.TempDir()
	plain := openTestDB(t, nil)
	mustPut(t, plain, "a", "1")
	plain.Close()
	opts := DefaultOptions(plainDir)
	opts.MergeOperator = incrOp{}
	db3, err := Open(opts)
	if err != nil {
		t.Fatalf("老目录首次配算子应当允许：%v", err)
	}
	db3.Close()
}

// ── 验收 9：未配算子时写入被拒 ───────────────────────────────────

func TestMergeWithoutOperatorRejected(t *testing.T) {
	db := openTestDB(t, nil) // 没配 MergeOperator
	defer db.Close()

	if err := db.Merge([]byte("k"), counter(1)); !errors.Is(err, ErrNoMergeOperator) {
		t.Fatalf("DB.Merge = %v, want ErrNoMergeOperator", err)
	}
	b := NewWriteBatch()
	if err := b.Merge([]byte("k"), counter(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.Write(b); !errors.Is(err, ErrNoMergeOperator) {
		t.Fatalf("Write(含 merge 的批次) = %v, want ErrNoMergeOperator", err)
	}
	// 两条路径都必须"什么都没写"。
	mustMiss(t, db, "k")
}

// ── 验收 7：迭代器折叠 ───────────────────────────────────────────

func TestMergeIteratorFolding(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MergeOperator = incrOp{}
		o.MemTableSize = 32 << 10
	})
	defer db.Close()

	// 一部分落在 SST（会被 filler 挤出去），一部分留在 MemTable：
	// 迭代器必须跨来源收集整条链。
	if err := db.Put([]byte("a"), counter(1)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		mustMerge(t, db, "a", 1) // SST 侧：1 + 10 = 11
	}
	payload := strings.Repeat("z", 512)
	for i := 0; i < 200; i++ {
		if err := db.Put([]byte(fmt.Sprintf("zzz%06d", i)), []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	waitForStats(t, db, "a 落盘", func(s Stats) bool { return s.FlushBytes > 0 })
	for i := 0; i < 5; i++ {
		mustMerge(t, db, "a", 1) // MemTable 侧：+5
	}
	mustMerge(t, db, "b", 100)
	if err := db.Put([]byte("c"), counter(7)); err != nil {
		t.Fatal(err)
	}

	// 全表扫描：三个 key 都在，值折叠正确；Next 跨过 merge key（含
	// "折叠已把流推进到下一个 key"的路径）不得漏记录。
	it := db.NewIterator(nil)
	defer it.Close()
	got := map[string]uint64{}
	var order []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := string(it.Key())
		if _, ok := got[string(it.Key())]; ok {
			t.Fatalf("key %q 出现了两次", k)
		}
		got[k] = binary.BigEndian.Uint64(it.Value())
		order = append(order, k)
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if got["a"] != 16 || got["b"] != 100 || got["c"] != 7 {
		t.Fatalf("折叠结果 = %v, want a=16 b=100 c=7", got)
	}
	// 键序列：a / b / c 在前且各出现一次，filler（zzz*）跟在后面。
	if len(order) != 3+200 {
		t.Fatalf("迭代键数 = %d, want %d", len(order), 3+200)
	}
	for i, want := range []string{"a", "b", "c"} {
		if order[i] != want {
			t.Fatalf("迭代键序列[%d] = %q, want %q（完整序列：%v...）", i, order[i], want, order[:6])
		}
	}

	// Seek 定位到 merge key，再 Next：验证"越过 merge 链"之后扫描继续正常。
	it2 := db.NewIterator(nil)
	defer it2.Close()
	it2.Seek([]byte("b"))
	if !it2.Valid() || string(it2.Key()) != "b" {
		t.Fatalf("Seek(b) 落点 = %q", it2.Key())
	}
	it2.Next()
	if !it2.Valid() || string(it2.Key()) != "c" {
		t.Fatalf("Next 后落点 = %q, want c", it2.Key())
	}

	// 上界扫描在 merge key 之前停住。
	it3 := db.NewIterator(&IteratorOptions{UpperBound: []byte("b")})
	defer it3.Close()
	var seen []string
	for it3.SeekToFirst(); it3.Valid(); it3.Next() {
		seen = append(seen, string(it3.Key()))
	}
	if len(seen) != 1 || seen[0] != "a" {
		t.Fatalf("上界扫描 = %v, want [a]", seen)
	}
}

// FullMerge 报错：读路径如实上抛，迭代器进入错误状态。
func TestMergeOperatorErrorPropagates(t *testing.T) {
	db := openTestDB(t, func(o *Options) { o.MergeOperator = boomOp{} })
	defer db.Close()

	mustMerge(t, db, "k", 1)
	if _, err := db.Get([]byte("k")); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Get 应当上抛算子错误，得到：%v", err)
	}

	it := db.NewIterator(nil)
	defer it.Close()
	for it.SeekToFirst(); it.Valid(); it.Next() {
	}
	if err := it.Error(); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("迭代器应当报算子错误，得到：%v", err)
	}
}
