package kvdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"kvdb/internal/version"
)

// 这个文件是 M3（写优化）的验收测试。
//
// M3 引入 Manifest + Version + 后台 Compaction，目标只有一个：
// **写入量增长时，L0 的文件数不跟着涨**。L0 的每个文件都是一次点查要额外探测的对象，
// 它的数量直接等于读放大的下限；M2 的时候它会随写入线性增长，读延迟随之塌掉。
//
// 所以这里断言的都不是"某个函数返回了什么"，而是三条可以观察的规模事实：
//
//	1. L0 的文件数稳定在 L0CompactionTrigger 附近，不随写入量增长；
//	2. L1 以下同层文件的 key 区间互不重叠（L1+ 二分查找成立的前提）；
//	3. 读放大（一次点查碰几个文件）在数据量翻十倍之后基本不变。

// ── 脚手架 ────────────────────────────────────────────────────────

// benchKey 生成定长的顺序 key，避免 key 编码的开销混进规模判断。
func benchKey(i int) []byte {
	return []byte(fmt.Sprintf("key%08d", i))
}

// waitForStats 轮询直到数据库达到期望状态，超时则失败并打印当时的规模。
//
// 后台的 Flush 与 Compaction 都是异步的，"L0 有几个文件"这类断言只有在它们
// 停下来之后才有意义。这里不去猜需要 sleep 多久，而是等一个明确的收敛条件。
func waitForStats(t *testing.T, db *DB, what string, cond func(Stats) bool) Stats {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last Stats
	for {
		last = db.Stats()
		if cond(last) {
			return last
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待「%s」超时：%s", what, describeStats(last))
		}
		time.Sleep(3 * time.Millisecond)
	}
}

// waitConverged 等到"数据已全部落盘、且 L0 已被收拾到触发阈值以下"。
//
// 判据里的 !HasImmutable 很关键：Put 返回只代表这条数据进了 MemTable 或者被冻结，
// 最后一张被冻结的表可能还在后台落盘。HasImmutable 为 false 才说明没有在途的 Flush。
func waitConverged(t *testing.T, db *DB) Stats {
	t.Helper()
	return waitForStats(t, db, "后台收敛", func(s Stats) bool {
		return !s.HasImmutable && s.Levels[0].Files < db.opts.L0CompactionTrigger
	})
}

func describeStats(s Stats) string {
	var b strings.Builder
	fmt.Fprintf(&b, "files=%d immutable=%v", s.Files, s.HasImmutable)
	for i, lv := range s.Levels {
		if lv.Files == 0 && i > 0 {
			continue
		}
		fmt.Fprintf(&b, " L%d=%d/%dB", i, lv.Files, lv.Bytes)
	}
	fmt.Fprintf(&b, " compactions=%d gets=%d probes=%d", s.Compaction.Count, s.Gets, s.ReadProbes)
	return b.String()
}

// checkLevelInvariant 校验 M3 的两条布局不变式。任何一次 Compaction 之后都必须成立。
//
//   - L0 按文件编号升序：编号大 = 新，读路径从后往前扫，靠的就是这个顺序；
//   - L1 以下按 Smallest 升序且区间互不重叠：FindFile 的二分查找全靠它。
//     一旦重叠，点查会静默地少读一个文件 —— 表现为"数据偶尔读不到"，极难复现。
func checkLevelInvariant(t *testing.T, db *DB) {
	t.Helper()
	db.mu.RLock()
	defer db.mu.RUnlock()
	v := db.v
	if v == nil {
		t.Fatal("版本为空")
	}

	l0 := v.Files(0)
	for i := 1; i < len(l0); i++ {
		if l0[i-1].Num >= l0[i].Num {
			t.Fatalf("L0 必须按编号升序：%d 排在 %d 之后", l0[i].Num, l0[i-1].Num)
		}
	}
	for level := 1; level < v.NumLevels(); level++ {
		files := v.Files(level)
		for i := 1; i < len(files); i++ {
			prev, cur := files[i-1], files[i]
			if db.icmp.Compare(prev.Smallest, cur.Smallest) >= 0 {
				t.Fatalf("L%d 没有按 Smallest 升序：%d 的 %q 不小于 %d 的 %q",
					level, prev.Num, prev.Smallest, cur.Num, cur.Smallest)
			}
			if db.icmp.Compare(prev.Largest, cur.Smallest) >= 0 {
				t.Fatalf("L%d 的 %d 与 %d 区间重叠（同层不重叠的二分前提被破坏）",
					level, prev.Num, cur.Num)
			}
		}
	}
}

// load 写入 n 个 key，value 固定长度。
func load(t *testing.T, db *DB, from, to int, value string) {
	t.Helper()
	for i := from; i < to; i++ {
		if err := db.Put(benchKey(i), []byte(value)); err != nil {
			t.Fatalf("Put(key%08d) failed: %v", i, err)
		}
	}
}

// ── 验收 1：L0 文件数不随写入量增长 ───────────────────────────────

func TestCompactionKeepsL0Bounded(t *testing.T) {
	const keys = 20000
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 128 << 10 // 逼出几十次 Flush
		o.LevelBaseSize = 64 << 20
	})
	defer db.Close()

	load(t, db, 0, keys, "v")
	s := waitConverged(t, db)

	t.Logf("写入 %d 个 key 之后：%s", keys, describeStats(s))

	trigger := db.opts.L0CompactionTrigger
	if s.Levels[0].Files >= trigger {
		t.Errorf("L0 有 %d 个文件，应当收敛到阈值 %d 以下", s.Levels[0].Files, trigger)
	}
	if s.Compaction.Count == 0 {
		t.Error("应当至少发生过一次 Compaction")
	}
	if s.Levels[1].Files == 0 {
		t.Error("L1 应当有文件（L0 的搬运目的地）")
	}
	// 几十次 Flush 最后只应该剩下十几个文件。这个上界比"L0 < 阈值"更能说明
	// 分层的意义：文件总数不再与写入量成正比。
	if s.Files > 30 {
		t.Errorf("文件总数 %d 偏多，分层没有起到收敛作用", s.Files)
	}
	checkLevelInvariant(t, db)

	// 数据一条都不能少。
	for i := 0; i < keys; i += 997 {
		if got := mustGet(t, db, string(benchKey(i))); got != "v" {
			t.Fatalf("Get(key%08d) = %q, want v", i, got)
		}
	}
}

// 数据量足够大时，L1 也会被撑满，于是往 L2 搬 —— 分层结构是逐层长出来的。
func TestCompactionOverflowsIntoDeeperLevel(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 64 << 10
		o.LevelBaseSize = 64 << 10 // L1 只有 64KB，很快撑满
	})
	defer db.Close()

	const keys = 20000
	load(t, db, 0, keys, "v")
	// 两个条件都必须满足才算收敛："L2 里有文件"说明分层确实往下长了，
	// 而"L0 回到阈值以下"说明在途的 L0→L1 搬运已经做完。只看前者会抓到一个
	// 中间态：L2 刚被写出来，L0 还堆着好几个文件。
	s := waitForStats(t, db, "搬到 L2 且 L0 收敛", func(s Stats) bool {
		return !s.HasImmutable &&
			len(s.Levels) > 2 && s.Levels[2].Files > 0 &&
			s.Levels[0].Files < db.opts.L0CompactionTrigger
	})
	t.Logf("溢出到 L2：%s", describeStats(s))

	if s.Levels[0].Files >= db.opts.L0CompactionTrigger {
		t.Errorf("L0 又涨回 %d 个文件", s.Levels[0].Files)
	}
	checkLevelInvariant(t, db)
	for i := 0; i < keys; i += 331 {
		if got := mustGet(t, db, string(benchKey(i))); got != "v" {
			t.Fatalf("Get(key%08d) = %q, want v", i, got)
		}
	}
}

// ── 验收 2：读放大不随数据量增长 ──────────────────────────────────

// 数据量翻十倍之后，一次点查碰的文件数必须基本不变。
//
// 这里用 ReadProbes/Gets 而不是墙钟延迟：延迟受机器负载、缓存命中、GC 影响，
// 在 CI 上抖动很大，而"碰了几个文件"是读路径真正的、可复现的成本。
// （延迟本身由 kvdb-bench 的 sweep 模式负责观察，那里会跑多档数据量的曲线拟合。）
//
// 两次测量都必须在**已经进入稳态**（发生过至少一次 Compaction）之后做，
// 否则第一次测到的只是"数据还没多到需要分层"的状态，比较没有意义 —— 这个坑踩过。
func TestReadAmplificationStaysBoundedAfterTenfoldGrowth(t *testing.T) {
	const (
		readSet = 1000  // 每次测量都读同一批 key（最老的那批）
		phase1  = 2000  // 第一轮的规模
		phase2  = 20000 // 十倍
	)
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 8 << 10
		o.LevelBaseSize = 64 << 20
	})
	defer db.Close()

	// measure 用固定的那批 key 测读放大。那批 key 总是被后续写入挤出了 MemTable，
	// 所以测到的是真真切切的读路径，而不是内存命中。
	measure := func(what string) float64 {
		s := waitConverged(t, db)
		checkLevelInvariant(t, db)
		if s.Compaction.Count == 0 {
			t.Fatalf("%s：还没发生过 Compaction，必须等进入稳态再测", what)
		}
		checkLevelInvariant(t, db)

		before := db.Stats()
		for i := 0; i < readSet; i++ {
			mustGet(t, db, string(benchKey(i)))
		}
		after := db.Stats()

		gets := after.Gets - before.Gets
		probes := after.ReadProbes - before.ReadProbes
		if gets == 0 {
			t.Fatal("点查计数没有增加")
		}
		amp := float64(probes) / float64(gets)
		t.Logf("%s：读放大 %.2f（%d 次点查碰了 %d 次文件）；%s", what, amp, gets, probes, describeStats(after))
		return amp
	}

	load(t, db, 0, phase1, "v")
	small := measure("小规模")

	load(t, db, phase1, phase2, "v")
	big := measure("十倍规模")

	// 一次点查最多碰 L0 的每个文件 + 每层各一个候选文件。L0 收敛在阈值以下、
	// 层数按对数增长，所以这个数应当是个小常数，和数据量无关。
	if big > 6 {
		t.Errorf("读放大 %.2f 偏大：L0 或层数没有收敛", big)
	}
	if big > small+2 {
		t.Errorf("数据量翻十倍之后读放大从 %.2f 涨到 %.2f，读路径没有稳住", small, big)
	}
}

// ── 验收 3：Compaction 期间读视图稳定 ─────────────────────────────

// 快照握着的旧版本不能被 Compaction 当成垃圾收掉。
func TestSnapshotSurvivesCompaction(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 64 << 10
	})
	defer db.Close()

	const keys = 1500
	load(t, db, 0, keys, "before")
	snap := db.GetSnapshot()
	defer snap.Release()

	// 全部覆盖写 + 删掉一半，再写足够多的数据把 Compaction 逼出来。
	load(t, db, 0, keys, "after")
	for i := 0; i < keys; i += 2 {
		if err := db.Delete(benchKey(i)); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}
	load(t, db, keys, 20000, "pad")
	s := waitConverged(t, db)
	if s.Compaction.Count == 0 {
		t.Fatal("应当至少发生过一次 Compaction，否则这个用例没测到东西")
	}
	t.Logf("Compaction 之后：%s", describeStats(s))

	// 快照仍然要看到它创建时的样子。
	for i := 0; i < keys; i += 37 {
		got, err := snap.Get(benchKey(i))
		if err != nil {
			t.Fatalf("快照 Get(key%08d) failed: %v", i, err)
		}
		if string(got) != "before" {
			t.Fatalf("快照 Get(key%08d) = %q, want before", i, got)
		}
	}
	// 最新视图则应当看到覆盖与删除的结果。
	if got := mustGet(t, db, string(benchKey(1))); got != "after" {
		t.Errorf("最新视图 Get(key00000001) = %q, want after", got)
	}
	mustMiss(t, db, string(benchKey(2)))

	// 快照自己的迭代器也要能读到完整的老数据。
	it := snap.NewIterator(nil)
	defer it.Close()
	n := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if string(it.Value()) != "before" {
			t.Fatalf("快照迭代器读到 %s=%q, want before", it.Key(), it.Value())
		}
		n++
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if n != keys {
		t.Errorf("快照迭代器读到 %d 个 key, want %d", n, keys)
	}
}

// 迭代器握着的旧版本同理：它脚下的文件不能被删掉，否则遍历到一半就报"文件不存在"。
func TestIteratorSurvivesCompaction(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 64 << 10
	})
	defer db.Close()

	const keys = 1200
	load(t, db, 0, keys, "before")
	waitConverged(t, db)

	it := db.NewIterator(nil)
	defer it.Close()
	it.SeekToFirst()
	if !it.Valid() {
		t.Fatal("迭代器开局就该有效")
	}
	// 只走到第一半，然后让后台把数据搬个遍。
	for i := 0; i < 300; i++ {
		it.Next()
	}

	load(t, db, keys, 15000, "pad")
	load(t, db, 0, keys, "after")
	s := waitConverged(t, db)
	if s.Compaction.Count == 0 {
		t.Fatal("应当至少发生过一次 Compaction")
	}

	// 接着走完：脚下是老版本，读到的必须还是老数据。
	n := 300
	for ; it.Valid(); it.Next() {
		if string(it.Value()) != "before" {
			t.Fatalf("迭代器读到 %s=%q, want before（旧版本的文件被提前删掉了）", it.Key(), it.Value())
		}
		n++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("迭代出错: %v", err)
	}
	if n != keys {
		t.Errorf("迭代器读到 %d 条, want %d", n, keys)
	}
}

// ── 验收 4：重开与迁移 ────────────────────────────────────────────

// 重开之后层号必须原样回来：Manifest 记的是"文件在哪一层"，
// 而不是"目录里有哪些文件"。这正是不再需要扫描目录、也不再需要猜的原因。
//
// 注意这里不能要求"重开前后的文件数完全一致"：Close 刻意不刷 MemTable
// （内存里的数据本来就在 WAL 里），所以重开时会重放 WAL 并多落一个 L0 文件。
// 真正要断言的是一条更弱但更要紧的性质：**已经在 L1 以下的文件，重开后还在
// 原来的层**。这是目录扫描做不到的 —— 扫描出来的文件天然无从判断层号。
func TestReopenPreservesLevelLayout(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MemTableSize = 64 << 10
	opts.LevelBaseSize = 64 << 20 // 大到不会被撑满，L1 不会自己往 L2 走

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8000; i++ {
		if err := db.Put(benchKey(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	waitConverged(t, db)
	before := layoutOf(db)
	if len(before[1]) == 0 {
		t.Fatal("重开前的用例前提没满足：L1 还没有文件")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if rep := db2.RecoveryReport(); rep.RecoveredFromScan {
		t.Error("目录里有 Manifest，不应当走扫描迁移")
	}
	after := layoutOf(db2)
	t.Logf("重开前 %v\n重开后 %v", before, after)

	for level := 1; level < len(before); level++ {
		for _, num := range before[level] {
			if !hasFileNum(after[level], num) {
				t.Errorf("文件 %d 重开前在 L%d，重开后却不在这一层（after L%d=%v）",
					num, level, level, after[level])
			}
		}
	}
	if len(after[0]) > len(before[0])+1 {
		t.Errorf("L0 从 %d 涨到 %d：重放 WAL 最多多落一个文件", len(before[0]), len(after[0]))
	}
	if got := db2.Stats(); got.Files == 0 {
		t.Error("重开后不该一个文件都没有")
	}
	checkLevelInvariant(t, db2)
	for i := 0; i < 8000; i += 173 {
		if got := mustGet(t, db2, string(benchKey(i))); got != "v" {
			t.Fatalf("重开后 Get(key%08d) = %q, want v", i, got)
		}
	}
}

// layoutOf 取出各层的文件编号（索引即层号）。
func layoutOf(db *DB) [][]uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([][]uint64, db.v.NumLevels())
	for level := 0; level < db.v.NumLevels(); level++ {
		for _, f := range db.v.Files(level) {
			out[level] = append(out[level], f.Num)
		}
	}
	return out
}

func hasFileNum(nums []uint64, want uint64) bool {
	for _, n := range nums {
		if n == want {
			return true
		}
	}
	return false
}

// M2 及更早的目录没有 Manifest：打开时必须退化成一次目录扫描，
// 把文件全部当 L0 收养，并且**不能**因为分不清层号就丢数据。
func TestMigrateLegacyDirectoryWithoutManifest(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MemTableSize = 64 << 10
	opts.LevelBaseSize = 64 << 20

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	const keys = 4000
	for i := 0; i < keys; i++ {
		if err := db.Put(benchKey(i), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	before := waitConverged(t, db)
	if before.Levels[1].Files == 0 {
		t.Fatal("用例前提没满足：L1 还没有文件")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 把目录退化成 M2 的形态：删掉 CURRENT 与所有 Manifest。
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	sstCount := 0
	for _, e := range entries {
		switch {
		case e.Name() == version.CurrentName, strings.HasPrefix(e.Name(), version.ManifestPrefix):
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				t.Fatal(err)
			}
		case strings.HasSuffix(e.Name(), ".sst"):
			sstCount++
		}
	}
	if sstCount == 0 {
		t.Fatal("目录里应当有 SST 文件")
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("迁移旧目录不该失败: %v", err)
	}
	defer db2.Close()

	rep := db2.RecoveryReport()
	if !rep.RecoveredFromScan {
		t.Error("没有 Manifest 时必须走扫描迁移")
	}
	if len(rep.DiscardedFiles) != 0 {
		t.Errorf("这些文件都是完整的，不该被丢弃: %v", rep.DiscardedFiles)
	}
	s := db2.Stats()
	t.Logf("迁移后 %s（目录里有 %d 个 SST）", describeStats(s), sstCount)
	// 扫描出来的文件不知道区间关系，只能全放 L0；再加上重放 WAL 落下来的那一个。
	// 这里不去断言精确数字：后台可能已经开始整理它们了，断言会变成竞态。
	if s.Files < sstCount {
		t.Errorf("迁移后只剩 %d 个文件，少于目录里的 %d 个：有文件丢了", s.Files, sstCount)
	}

	// 迁移路径最大的坑：所有 MemTable 都已落盘时 WAL 是空的，重放不出任何序列号，
	// lastSeq 会退回 0。此时新写入必须仍然大于老数据的序列号，否则同 key 的
	// 新值会与老记录撞号，读出来还是旧值。
	for i := 0; i < keys; i++ {
		if err := db2.Put(benchKey(i), []byte("new")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < keys; i += 61 {
		if got := mustGet(t, db2, string(benchKey(i))); got != "new" {
			t.Fatalf("迁移后覆盖写 Get(key%08d) = %q, want new（序列号撞号了）", i, got)
		}
	}

	// 迁移路径最大的坑：所有 MemTable 都已落盘时 WAL 是空的，重放不出任何序列号，
	// lastSeq 会退回 0。此时新写入必须仍然大于老数据的序列号，否则同 key 的
	// 新值会与老记录撞号，读出来还是旧值。
	for i := 0; i < keys; i++ {
		if err := db2.Put(benchKey(i), []byte("new")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < keys; i += 61 {
		if got := mustGet(t, db2, string(benchKey(i))); got != "new" {
			t.Fatalf("迁移后覆盖写 Get(key%08d) = %q, want new（序列号撞号了）", i, got)
		}
	}

	// 开局那次提醒应当把这些文件收敛下去。
	after := waitConverged(t, db2)
	if after.Compaction.Count == 0 {
		t.Error("迁移进来的文件应当被后台整理一次")
	}
	if after.Levels[1].Files == 0 {
		t.Error("整理之后 L1 应当有文件")
	}
	checkLevelInvariant(t, db2)
}

// 恢复时对"孤儿文件"的分类：完整的记 ObsoleteFiles，残缺的记 DiscardedFiles。
//
// 两者都删（它们都不在任何版本里，数据另有归属），但对用户的意义不同：
// 前者是"写完但没提交成功"，后者是"连内容都不完整"。
func TestRecoveryClassifiesOrphanFiles(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	// MemTable 开得很小，好让写入真的落成 SST —— Close 刻意不刷 MemTable，
	// 用默认阈值的话目录里一个 .sst 都不会有，也就没有"孤儿"可言。
	opts.MemTableSize = 1 << 10

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if err := db.Put(benchKey(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	waitForFiles(t, db, 1)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 找一个真实存在的 SST 当模板。
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var template string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sst") {
			template = filepath.Join(dir, e.Name())
			break
		}
	}
	if template == "" {
		t.Fatal("目录里应当有 SST")
	}

	// 一个完整的孤儿：模拟"文件写完并 fsync 了，但 Manifest 里的提交还没落盘就崩溃"。
	const intact = "000900.sst"
	// 一个残缺的孤儿：模拟"写到一半就崩溃"。
	const torn = "000901.sst"
	blob, err := os.ReadFile(template)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, intact), blob, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, torn), blob[:len(blob)/2], 0o644); err != nil {
		t.Fatal(err)
	}

	// 这两个文件都不在任何版本里，恢复时必须被清掉并如实分类。
	//
	// 注意这条断言还顺带守住了另一个更容易漏的漏洞：**关闭那一刻正在收尾的
	// Compaction 留下的孤儿**。它会以完全相同的形态出现在这里（多出一个
	// ObsoleteFiles），所以这里的期望值严格写成"只有我造的这一个"。
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("孤儿文件不该让 Open 失败: %v", err)
	}
	defer db2.Close()

	rep := db2.RecoveryReport()
	if len(rep.ObsoleteFiles) != 1 || rep.ObsoleteFiles[0] != intact {
		t.Errorf("ObsoleteFiles = %v, want [%s]", rep.ObsoleteFiles, intact)
	}
	if len(rep.DiscardedFiles) != 1 || rep.DiscardedFiles[0] != torn {
		t.Errorf("DiscardedFiles = %v, want [%s]", rep.DiscardedFiles, torn)
	}
	for _, name := range []string{intact, torn} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("孤儿文件 %s 没有被删除: %v", name, err)
		}
	}
	// 老数据必须还在。
	for i := 0; i < 500; i++ {
		if got := mustGet(t, db2, string(benchKey(i))); got != "v" {
			t.Fatalf("恢复后 Get(key%08d) = %q, want v", i, got)
		}
	}
}

// ── 验收 5：Stats 要能反映写优化的效果 ───────────────────────────

func TestStatsReportLevelsAndAmplification(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 64 << 10
		o.LevelBaseSize = 64 << 20
	})
	defer db.Close()

	const keys = 6000
	load(t, db, 0, keys, "v")
	s := waitConverged(t, db)
	// 再读一轮，让读放大的分子分母都有值。
	for i := 0; i < keys; i += 7 {
		mustGet(t, db, string(benchKey(i)))
	}
	s = db.Stats()

	if len(s.Levels) != db.opts.MaxLevels {
		t.Errorf("Levels 长度 = %d, want %d", len(s.Levels), db.opts.MaxLevels)
	}
	if s.WALBytes == 0 {
		t.Error("WALBytes 应当有值")
	}
	if s.FlushBytes == 0 {
		t.Error("FlushBytes 应当有值")
	}
	if c := s.Compaction; c.Count == 0 || c.InputBytes == 0 || c.OutputBytes == 0 {
		t.Errorf("Compaction 统计不完整: %+v", c)
	}
	if s.Gets == 0 || s.ReadProbes == 0 {
		t.Errorf("读路径计数不完整: gets=%d probes=%d", s.Gets, s.ReadProbes)
	}
	if s.ReadProbes < s.Gets {
		t.Errorf("一次点查至少碰一个文件：probes=%d < gets=%d", s.ReadProbes, s.Gets)
	}
	// 写放大是"引擎写出去的字节 / 用户放进来的字节"，至少要大于 1（WAL 那一次）。
	user := uint64(keys) * uint64(len(benchKey(0))+1)
	written := s.WALBytes + s.FlushBytes + s.Compaction.OutputBytes
	if written <= user {
		t.Errorf("写放大 %.2f 不合理（written=%d user=%d）", float64(written)/float64(user), written, user)
	}
	t.Logf("%s；written=%d user=%d", describeStats(s), written, user)
}

// 打开过程中万一需要读文件，也必须是在锁外。这里用"打开一个有内容的目录"来兜底
// 检查一遍：既然 Open 能返回，说明恢复路径没有自己把自己锁死。
func TestOpenWithExistingDataDoesNotLockUp(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MemTableSize = 32 << 10

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3000; i++ {
		if err := db.Put(benchKey(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		d, err := Open(opts)
		if err == nil {
			err = d.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("重开失败: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Open 卡住了：恢复路径里出现了自锁")
	}
}

// 目录里应当只剩引擎自己的文件：孤儿文件在打开时就被清掉了，不会越积越多。
func TestNoStrayFilesAfterCycles(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MemTableSize = 32 << 10

	for round := 0; round < 3; round++ {
		db, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2000; i++ {
			if err := db.Put(benchKey(round*2000+i), []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		waitConverged(t, db)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ssts []string
	var manifests []string
	for _, e := range entries {
		switch {
		case strings.HasSuffix(e.Name(), ".sst"):
			ssts = append(ssts, e.Name())
		case strings.HasPrefix(e.Name(), version.ManifestPrefix):
			manifests = append(manifests, e.Name())
		}
	}
	sort.Strings(manifests)
	if len(manifests) > 1 {
		t.Errorf("多次打开之后应当只剩一份 Manifest，实际 %v", manifests)
	}
	if len(ssts) > 40 {
		t.Errorf("SST 文件堆积了 %d 个：%v", len(ssts), ssts)
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	checkLevelInvariant(t, db)
	for i := 0; i < 6000; i += 113 {
		if got := mustGet(t, db, string(benchKey(i))); got != "v" {
			t.Fatalf("Get(key%08d) = %q, want v", i, got)
		}
	}
}
