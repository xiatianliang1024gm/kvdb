package kvdb

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// 这个文件是 M10（docs/EXTENSIONS.md §5.2 / §5.4 / §5.5）的验收测试：
// MultiGet、Flush / Sync / CompactRange、GetInto。三项全是纯 API 级改动，
// 不碰磁盘格式，验收焦点是"语义正确 + 并发不死锁"：
//
//	5.2 MultiGet   → TestMultiGetBasics / TestMultiGetClosed
//	5.4 Flush      → TestFlushPersistsMemTable / TestFlushWithConcurrentWriters
//	5.4 Sync       → TestSyncReturnsDurableSeq
//	5.4 CompactRange → TestCompactRangeReclaimsAfterDeleteRange / TestCompactRangePushesDownToBottomLevel
//	5.5 GetInto    → TestGetIntoReusesBuffer

// ── 5.2 MultiGet ─────────────────────────────────────────────────

func TestMultiGetBasics(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	mustPut(t, db, "a", "va")
	mustPut(t, db, "c", "") // 空 value：存在但内容为空
	mustPut(t, db, "d", "vd")
	mustPut(t, db, "d", "vd2") // 覆盖后 d 的可见版本是 vd2
	mustPut(t, db, "e", "ve")
	mustPut(t, db, "g", "vg")
	if err := db.Delete([]byte("e")); err != nil {
		t.Fatal(err)
	}

	// 乱序输入 + 缺失 + 重复：结果必须与输入按下标严格对齐。
	keys := [][]byte{[]byte("g"), []byte("missing"), []byte("a"), []byte("d"), []byte("g"), []byte("e"), []byte("c")}
	values, errs := db.MultiGet(keys)

	wantVals := map[int]string{0: "vg", 2: "va", 3: "vd2", 4: "vg", 6: ""}
	for idx, want := range wantVals {
		if errs[idx] != nil {
			t.Fatalf("MultiGet[%d](%q) err = %v, want nil", idx, keys[idx], errs[idx])
		}
		if string(values[idx]) != want {
			t.Fatalf("MultiGet[%d](%q) = %q, want %q", idx, keys[idx], values[idx], want)
		}
	}
	for _, idx := range []int{1, 5} { // missing 与已删除的都是 ErrNotFound
		if !errors.Is(errs[idx], ErrNotFound) {
			t.Fatalf("MultiGet[%d](%q) err = %v, want ErrNotFound", idx, keys[idx], errs[idx])
		}
		if values[idx] != nil {
			t.Fatalf("MultiGet[%d] value = %q, want nil", idx, values[idx])
		}
	}

	// 重复 key 的两份结果必须独立：改一份不影响另一份。
	values[0][0] = 'X'
	if string(values[4]) != "vg" {
		t.Fatalf("重复下标的结果被联动修改: MultiGet[4] = %q", values[4])
	}

	// 空 key 报 ErrEmptyKey，只算它自己的，不拖垮整批
	//（"bad" 不存在，它的 ErrNotFound 是正常结果，正好证明这点）。
	values, errs = db.MultiGet([][]byte{[]byte("bad"), nil, []byte("a")})
	if !errors.Is(errs[1], ErrEmptyKey) {
		t.Fatalf("MultiGet 空 key err = %v, want ErrEmptyKey", errs[1])
	}
	if !errors.Is(errs[0], ErrNotFound) || errs[2] != nil || string(values[2]) != "va" {
		t.Fatalf("空 key 影响了整批: errs=%v values[2]=%q", errs, values[2])
	}

	// 空批次与 nil 都是合法空操作。
	if vs, es := db.MultiGet(nil); len(vs) != 0 || len(es) != 0 {
		t.Fatalf("MultiGet(nil) = (%d, %d) 项, want 0 项", len(vs), len(es))
	}

	// 每次调用内重复 key 只查一次：第一轮唯一 key 6 个（g/missing/a/d/e/c），
	// 第二轮 2 个（bad/a），去重只在单次调用内做，跨调用各自计数 → 共 8。
	if got := db.Stats().Gets; got != 8 {
		t.Fatalf("Stats().Gets = %d, want 8（重复 key 不应重复计数）", got)
	}
}

func TestMultiGetClosed(t *testing.T) {
	db := openTestDB(t, nil)
	mustPut(t, db, "a", "1")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	values, errs := db.MultiGet([][]byte{[]byte("a"), []byte("b")})
	for i, err := range errs {
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("MultiGet[%d] err = %v, want ErrClosed", i, err)
		}
		if values[i] != nil {
			t.Fatalf("MultiGet[%d] value = %q, want nil", i, values[i])
		}
	}
}

// ── 5.5 GetInto ──────────────────────────────────────────────────

func TestGetIntoReusesBuffer(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	mustPut(t, db, "k", "value-001")

	// 容量足够：返回值必须复用 dst 的底层数组，一个字节都不分配。
	buf := make([]byte, 0, 64)
	res, err := db.GetInto(buf, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(res) != "value-001" {
		t.Fatalf("GetInto = %q", res)
	}
	if &res[0] != &buf[:1][0] { // buf 自身 len=0，取底层数组首址要借 cap 切片
		t.Fatal("容量足够时 GetInto 应复用 dst 的底层数组")
	}

	// 容量不足：退化为分配行为，返回完整值，dst 本身不被触碰。
	tiny := make([]byte, 0, 2)
	res, err = db.GetInto(tiny, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(res) != "value-001" {
		t.Fatalf("容量不足时 GetInto = %q, want 完整值", res)
	}
	if len(tiny) != 0 {
		t.Fatalf("dst 被写入了 %d 字节, want 0", len(tiny))
	}

	// 空 value：存在但内容为空，返回 dst 的零长度前缀而非新分配。
	mustPut(t, db, "empty", "")
	res, err = db.GetInto(buf, []byte("empty"))
	if err != nil || len(res) != 0 {
		t.Fatalf("GetInto(empty) = (%q, %v), want (空, nil)", res, err)
	}

	// 不存在 → ErrNotFound；空 key → ErrEmptyKey。
	if _, err := db.GetInto(buf, []byte("missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetInto(missing) err = %v, want ErrNotFound", err)
	}
	if _, err := db.GetInto(buf, nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("GetInto(nil key) err = %v, want ErrEmptyKey", err)
	}
}

// ── 5.4 Flush ────────────────────────────────────────────────────

func TestFlushPersistsMemTable(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, func(o *Options) { o.Dir = dir })
	defer func() { _ = db.Close() }()

	for i := 0; i < 50; i++ {
		mustPut(t, db, fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i))
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// 落盘完成的直接证据：没有待刷的 Immutable，MemTable 换成了新的空表。
	s := db.Stats()
	if s.HasImmutable {
		t.Fatal("Flush 返回后仍有 Immutable 待落盘")
	}
	if s.MemTableSize != 0 {
		t.Fatalf("Flush 返回后 MemTableSize = %d, want 0", s.MemTableSize)
	}
	if s.FlushBytes == 0 {
		t.Fatal("Flush 返回后 FlushBytes = 0, 数据没有落成 SST")
	}

	// 数据照常可读，且 Flush 之后还能继续写、继续 Flush。
	if got := mustGet(t, db, "k049"); got != "v049" {
		t.Fatalf("Flush 后 Get(k049) = %q", got)
	}
	mustPut(t, db, "after", "flushed")
	if err := db.Flush(); err != nil {
		t.Fatalf("第二次 Flush failed: %v", err)
	}

	// 空库 / 空表上的 Flush 是零成本空操作。
	if err := db.Flush(); err != nil {
		t.Fatalf("空 MemTable 上的 Flush failed: %v", err)
	}

	// 关库再开：上一次 Flush 之前的数据应当已经在 SST 里，不需要 WAL 重放。
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if n := db2.RecoveryReport().ReplayedRecords; n != 0 {
		t.Fatalf("Flush 后重启重放了 %d 条 WAL 记录, want 0（Flush 的意义就在这里）", n)
	}
	for i := 0; i < 50; i++ {
		if got := mustGet(t, db2, fmt.Sprintf("k%03d", i)); got != fmt.Sprintf("v%03d", i) {
			t.Fatalf("重启后 Get(k%03d) = %q", i, got)
		}
	}
	if got := mustGet(t, db2, "after"); got != "flushed" {
		t.Fatalf("重启后 Get(after) = %q", got)
	}
}

func TestFlushWithConcurrentWriters(t *testing.T) {
	// 写者持续涌入时 Flush 不能死锁、不能丢写：Flush 只承诺"调用那一刻
	// 的表落盘"，之后的写入进新表，由下一次 Flush（或 WAL）兜底。
	db := openTestDB(t, nil)
	defer db.Close()

	const writers = 4
	const perWriter = 200
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := db.Put([]byte(fmt.Sprintf("w%d-%04d", w, i)), []byte("v")); err != nil {
					errs[w] = err
					return
				}
			}
		}(w)
	}

	for round := 0; round < 5; round++ {
		if err := db.Flush(); err != nil {
			t.Fatalf("并发写入期间的 Flush failed: %v", err)
		}
	}
	wg.Wait()
	for w, err := range errs {
		if err != nil {
			t.Fatalf("写者 %d 失败: %v", w, err)
		}
	}
	// 全部写完后再 Flush 一次并等待：此时所有数据必须可读。
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			if _, err := db.Get([]byte(fmt.Sprintf("w%d-%04d", w, i))); err != nil {
				t.Fatalf("Get(w%d-%04d) 失败: %v", w, i, err)
			}
		}
	}
}

// ── 5.4 Sync ─────────────────────────────────────────────────────

func TestSyncReturnsDurableSeq(t *testing.T) {
	// SyncWrites 关闭：写路径不做 fsync，Sync 是唯一的耐久手段。
	db := openTestDB(t, nil)
	defer db.Close()

	// 空库：耐久水位是 0，fsync 空日志不是错误。
	seq, err := db.Sync()
	if err != nil || seq != 0 {
		t.Fatalf("空库 Sync = (%d, %v), want (0, nil)", seq, err)
	}

	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	want := db.Stats().LastSequence
	seq, err = db.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if seq != want {
		t.Fatalf("Sync 返回耐久序列号 %d, want 已提交水位 %d", seq, want)
	}
	if n := db.Stats().WALFsyncs; n < 1 {
		t.Fatalf("Sync 之后 WALFsyncs = %d, want >= 1", n)
	}
	mustGet(t, db, "a") // Sync 不影响可见性

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("关库后 Sync err = %v, want ErrClosed", err)
	}
}

// ── 5.4 CompactRange ─────────────────────────────────────────────

func TestCompactRangeReclaimsAfterDeleteRange(t *testing.T) {
	// M10 的核心场景：范围删除之后，空间回收只能等 Compaction 触及区间。
	// 自动 Compaction 是分数驱动的 —— 这里把触发阈值调到永远够不着，
	// 然后验证 CompactRange 能手动把空间收回来、墓碑退休。
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 16 << 10 // 16KB：几百字节就想撑出多个 SST
		o.L0CompactionTrigger = 1000
		o.LevelBaseSize = 1 << 30 // L1 以下的自动 Compaction 也别掺和
	})
	defer func() { _ = db.Close() }()

	put := func(prefix string, n int) {
		t.Helper()
		// 前后各 Flush 一次：上一个前缀在 MemTable 里的残留记录先单独落盘，
		// 本前缀写完再单独落盘 —— 保证没有任何文件同时装着两个前缀。
		// 退休判据是保守的（M7 设计如此：按文件区间求交），横跨被删段的
		// 文件会让墓碑一直活着，那条是合法状态，但这条测试要验证退休这一侧。
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			// ~100B 的 value，撑块也撑 MemTable。
			mustPut(t, db, fmt.Sprintf("%s%04d", prefix, i), fmt.Sprintf("%s-value-%04d-%s", prefix, i, "0123456789012345678901234567890123456789"))
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	put("a:", 40)
	put("b:", 300) // 中段：要被整段范围删除的
	put("c:", 40)

	if err := db.DeleteRange([]byte("b:"), []byte("c0")); err != nil {
		t.Fatal(err)
	}

	before := db.Stats()
	if before.RangeTombstones == 0 {
		t.Fatal("Flush 之后范围墓碑应当在版本上")
	}
	beforeBytes := levelBytes(&before)

	// 只压被清空的段（真实用例：清表之后催那一块的回收）。
	// 不用无界 CompactRange：它会把三个前缀归并进同一个输出文件，
	// 文件区间横跨被删段，墓碑按保守退休判据（按文件区间求交）就
	// 不再满足退休条件 —— 那是合法状态，只是这条测试要验证退休这一侧。
	if err := db.CompactRange([]byte("b:"), []byte("c0")); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}

	after := db.Stats()
	afterBytes := levelBytes(&after)
	if afterBytes >= beforeBytes {
		t.Fatalf("CompactRange 之后磁盘字节没有下降: %d -> %d", beforeBytes, afterBytes)
	}
	if after.RangeTombstones != 0 {
		t.Fatalf("CompactRange 之后范围墓碑仍有 %d 条, want 0（区间已清空，应当退休）", after.RangeTombstones)
	}

	// 数据语义不受影响：b: 段消失，a: / c: 完好。
	for i := 0; i < 300; i++ {
		mustMiss(t, db, fmt.Sprintf("b:%04d", i))
	}
	for i := 0; i < 40; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("a:%04d", i))); err != nil {
			t.Fatalf("Get(a:%04d) 失败: %v", i, err)
		}
		if _, err := db.Get([]byte(fmt.Sprintf("c:%04d", i))); err != nil {
			t.Fatalf("Get(c:%04d) 失败: %v", i, err)
		}
	}

	// 空区间与逆区间都是空操作。
	if err := db.CompactRange([]byte("z"), []byte("a")); err != nil {
		t.Fatalf("逆区间 CompactRange failed: %v", err)
	}
}

func TestCompactRangePushesDownToBottomLevel(t *testing.T) {
	// 无界 CompactRange 的另一条语义：区间内（这里是全表）的数据逐层
	// 下推，收敛后应当全部落在最底层。
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 16 << 10
		o.L0CompactionTrigger = 1000
		o.LevelBaseSize = 1 << 30
	})
	defer func() { _ = db.Close() }()

	for i := 0; i < 120; i++ {
		mustPut(t, db, fmt.Sprintf("k%04d", i), fmt.Sprintf("value-%04d-0123456789012345678901234567890123456789", i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.CompactRange(nil, nil); err != nil {
		t.Fatal(err)
	}

	s := db.Stats()
	bottom := len(s.Levels) - 1
	if s.Levels[bottom].Files == 0 {
		t.Fatal("CompactRange 之后最底层没有文件, 下推没有发生")
	}
	for level := 0; level < bottom; level++ {
		if n := s.Levels[level].Files; n != 0 {
			t.Fatalf("第 %d 层仍有 %d 个文件, want 0（应全部下推到底）", level, n)
		}
	}
	// 下推只是搬家：数据一条不少。
	for i := 0; i < 120; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("k%04d", i))); err != nil {
			t.Fatalf("Get(k%04d) 失败: %v", i, err)
		}
	}
}

// levelBytes 汇总各层的 SST 字节数（磁盘占用的代理指标）。
func levelBytes(s *Stats) uint64 {
	var total uint64
	for _, l := range s.Levels {
		total += l.Bytes
	}
	return total
}
