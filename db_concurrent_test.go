package kvdb

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

// 这个文件是 M4（一致性）的验收测试。
//
// M4 的验收标准只有一句话：**并发压测下 race detector 无告警**。但"没有告警"
// 本身太弱 —— 一个把写入全部串行化的实现同样没有告警。所以这里断言的是三件事：
//
//	1. 并发写下不丢记录、不串号（序列号稠密分配）；
//	2. 批次的原子性在并发下依然成立：读者永远不会看到"半批可见"；
//	3. 组提交真的在合并 fsync：WALFsyncs 远小于 WriteBatches。
//
// 第 3 条是 M4 相对 M3 唯一的行为差异，也是它存在的全部理由 ——
// 写性能的天花板由 fsync 次数决定，合并率就是吞吐的倍数。

// ── 组提交 ────────────────────────────────────────────────────────

// TestGroupCommitAmortizesFsync 检验并发写者确实被合并进了更少的 fsync。
//
// 这是组提交的核心断言：WriteBatches 是写者次数，WALFsyncs 是真正落到磁盘上的
// fsync 次数。SyncWrites 为真时，后者的倒数就是吞吐的上限 —— 所以
// "WALFsyncs ≪ WriteBatches" 与"吞吐提高了同样多倍"是同一句话。
func TestGroupCommitAmortizesFsync(t *testing.T) {
	const (
		writers    = 16
		perWriter  = 64
		wantWrites = writers * perWriter
	)

	db := openTestDB(t, func(o *Options) {
		o.SyncWrites = true // 组提交只有在真的要 fsync 时才有意义
	})
	defer db.Close()

	// 所有写者一起出发，保证它们有机会落在同一组里。
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWriter; i++ {
				k := []byte(fmt.Sprintf("g%02d-%04d", w, i))
				if err := db.Put(k, k); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Put failed: %v", err)
	}

	s := db.Stats()
	if s.WriteBatches != wantWrites {
		t.Fatalf("WriteBatches = %d, want %d", s.WriteBatches, wantWrites)
	}
	if s.WALFsyncs == 0 {
		t.Fatalf("WALFsyncs = 0, 但 SyncWrites 为真；组提交没有落到 fsync 上")
	}
	if s.WALFsyncs >= s.WriteBatches {
		t.Fatalf("WALFsyncs = %d >= WriteBatches = %d：组提交没有合并任何 fsync",
			s.WALFsyncs, s.WriteBatches)
	}
	if s.MaxWriteGroup < 2 {
		t.Fatalf("MaxWriteGroup = %d，%d 个并发写者一次都没能凑成一组", s.MaxWriteGroup, writers)
	}
	t.Logf("%d 次写入合并成 %d 个提交组（最大一组 %d 个），只 fsync 了 %d 次：合并率 %.2f、每次 fsync 摊销 %.1f 次写",
		s.WriteBatches, s.WriteGroups, s.MaxWriteGroup, s.WALFsyncs,
		float64(s.WriteBatches)/float64(s.WriteGroups),
		float64(s.WriteBatches)/float64(s.WALFsyncs))
}

// TestWALFsyncsDisabledWhenSyncWritesOff 检验 SyncWrites 关闭时一次 fsync 都不做。
//
// 这条看似平凡，但它是上面那条测试的对照组：如果 WALFsyncs 在关掉 SyncWrites 时
// 还在涨，那"WALFsyncs ≪ WriteBatches"就不能说明任何事。
func TestWALFsyncsDisabledWhenSyncWritesOff(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.SyncWrites = false
	})
	defer db.Close()

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := db.Put([]byte(fmt.Sprintf("n%02d-%03d", w, i)), []byte("v")); err != nil {
					t.Errorf("Put failed: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if s := db.Stats(); s.WALFsyncs != 0 {
		t.Fatalf("WALFsyncs = %d, want 0 (SyncWrites 已关闭)", s.WALFsyncs)
	}
}

// ── 并发正确性 ────────────────────────────────────────────────────

// TestConcurrentWritersKeepAllRecords 用多个写者各写自己的一段 key 空间。
//
// 除了"一条都不能丢"，它还断言序列号是**稠密分配**的：LastSequence 恰好等于
// 用户放进去的记录条数。并发下分配序列号最容易出的两种错（重号、跳号）都会
// 在这里立刻暴露 —— 重号会让 LastSequence 偏小，跳号会让它偏大。
func TestConcurrentWritersKeepAllRecords(t *testing.T) {
	const (
		writers   = 8
		perWriter = 200
	)

	db := openTestDB(t, nil)
	defer db.Close()

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := []byte(fmt.Sprintf("w%02d-%05d", w, i))
				if err := db.Put(k, k); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Put failed: %v", err)
	}

	want := uint64(writers * perWriter)
	if got := db.Stats().LastSequence; got != want {
		t.Fatalf("LastSequence = %d, want %d（序列号必须稠密分配）", got, want)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			k := fmt.Sprintf("w%02d-%05d", w, i)
			if got := mustGet(t, db, k); got != k {
				t.Fatalf("Get(%q) = %q", k, got)
			}
		}
	}
}

// TestWriteBatchesStayAtomicUnderConcurrentWrites 检验"半批可见"不会发生。
//
// 每个写者把**同一个值**同时写进一对 key（一个批次），读者在同一个快照上读这一对，
// 要求它们永远一致：要么都还没出现，要么都是同一个值。
//
// 批次原子性有两个可能被破坏的地方，这条测试把它们都覆盖了：
//
//   - 组提交把一条批次的多个记录打散到不同的提交组（每条记录仍然只用一条 WAL
//     记录承载，所以不该发生）；
//   - 读者的快照落在某个提交组的中间（整组在同一个 db.mu 临界区里应用、
//     lastSeq 最后才推进，所以也不该发生）。
//
// 注意这里必须用快照读：两次独立的 Get 本来就可能跨越一次提交，
// 那时候"读到一对不一致的值"是正确的语义，而不是 bug。
func TestWriteBatchesStayAtomicUnderConcurrentWrites(t *testing.T) {
	const (
		writers = 4
		rounds  = 300
	)

	db := openTestDB(t, nil)
	defer db.Close()

	stop := make(chan struct{})
	var writersWG, readersWG sync.WaitGroup
	fail := make(chan error, writers+writers)

	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			left := []byte(fmt.Sprintf("pair%02d-left", w))
			right := []byte(fmt.Sprintf("pair%02d-right", w))
			for i := 0; i < rounds; i++ {
				v := []byte(strconv.Itoa(i))
				b := NewWriteBatch()
				if err := b.Put(left, v); err != nil {
					fail <- err
					return
				}
				if err := b.Put(right, v); err != nil {
					fail <- err
					return
				}
				if err := db.Write(b); err != nil {
					fail <- err
					return
				}
			}
		}(w)
	}

	for w := 0; w < writers; w++ {
		readersWG.Add(1)
		go func(w int) {
			defer readersWG.Done()
			left := []byte(fmt.Sprintf("pair%02d-left", w))
			right := []byte(fmt.Sprintf("pair%02d-right", w))
			for {
				select {
				case <-stop:
					return
				default:
				}

				s := db.GetSnapshot()
				lv, lerr := s.Get(left)
				rv, rerr := s.Get(right)
				s.Release()

				if err := pairErr(left, right, lv, lerr, rv, rerr); err != nil {
					fail <- err
					return
				}
			}
		}(w)
	}

	writersWG.Wait()
	close(stop)
	readersWG.Wait()
	close(fail)
	for err := range fail {
		t.Fatalf("批次原子性被破坏: %v", err)
	}
}

// pairErr 校验一对 key 在同一个快照上读到的结果是否自洽。
func pairErr(left, right []byte, lv []byte, lerr error, rv []byte, rerr error) error {
	missing := func(err error) bool { return errors.Is(err, ErrNotFound) }
	switch {
	case missing(lerr) && missing(rerr):
		return nil // 这一批还没提交，两边都没出现
	case missing(lerr) != missing(rerr):
		return fmt.Errorf("%s / %s 只有一边存在（l=%q/%v r=%q/%v）",
			left, right, lv, lerr, rv, rerr)
	case lerr != nil || rerr != nil:
		return fmt.Errorf("%s / %s 读到意外错误：l=%v r=%v", left, right, lerr, rerr)
	}
	if !bytes.Equal(lv, rv) {
		return fmt.Errorf("%s = %q 而 %s = %q，同一批次的两个 key 不一致", left, lv, right, rv)
	}
	return nil
}

// TestConcurrentOverwritesThenSnapshotIsolation 检验快照的隔离性在并发覆盖写下成立。
//
// 快照一旦取到，写者再怎么覆盖也不该改变它看到的值 —— 这是"一致性"最直观的定义。
func TestConcurrentOverwritesThenSnapshotIsolation(t *testing.T) {
	const (
		writers = 4
		rounds  = 500
	)

	db := openTestDB(t, nil)
	defer db.Close()

	if err := db.Put([]byte("hot"), []byte("seed")); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var writersWG, readersWG sync.WaitGroup
	const concurrentReaders = 4
	// 容量按"可能出错的 goroutine 总数"给足，避免测试自己在错误通道上堵住。
	fail := make(chan error, writers+concurrentReaders)

	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			for i := 0; i < rounds; i++ {
				if err := db.Put([]byte("hot"), []byte(fmt.Sprintf("w%d-%d", w, i))); err != nil {
					fail <- err
					return
				}
			}
		}(w)
	}

	for r := 0; r < concurrentReaders; r++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				snap := db.GetSnapshot()
				before, err := snap.Get([]byte("hot"))
				if err != nil {
					fail <- fmt.Errorf("snapshot Get: %w", err)
					snap.Release()
					return
				}
				// 再读一次，中间允许写者大量覆盖：快照视图必须纹丝不动。
				after, err := snap.Get([]byte("hot"))
				snap.Release()
				if err != nil {
					fail <- fmt.Errorf("snapshot Get: %w", err)
					return
				}
				if !bytes.Equal(before, after) {
					fail <- fmt.Errorf("快照内两次读到不同的值：%q vs %q", before, after)
					return
				}
			}
		}()
	}

	writersWG.Wait()
	close(stop)
	readersWG.Wait()
	close(fail)
	for err := range fail {
		t.Fatalf("快照隔离被破坏: %v", err)
	}
}

// TestSnapshotPinsOldVersionsAcrossCompaction 检验存活快照真的挡住了 Compaction 的丢弃。
//
// 它和 M3 的 TestRunRespectsSmallestSnapshot 是同一件事的两端：那条测的是
// compact 包内部的丢弃规则，这条测的是**丢弃上界真的来自 DB 层登记的存活快照**。
// 如果 GetSnapshot 忘了登记，这里读到的会是覆盖之后的新值（或者直接 ErrNotFound）——
// 而那是静默的数据损坏，不报任何错。
func TestSnapshotPinsOldVersionsAcrossCompaction(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 16 << 10
		o.L0CompactionTrigger = 2
		o.LevelBaseSize = 64 << 10
	})
	defer db.Close()

	if err := db.Put([]byte("pinned"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	snap := db.GetSnapshot()
	if got := db.Stats().LiveSnapshots; got != 1 {
		t.Fatalf("LiveSnapshots = %d, want 1", got)
	}

	// 一批覆盖写，把 v1 压进 SST 并逼出多轮 Flush 与 Compaction。
	filler := bytes.Repeat([]byte("x"), 200)
	for i := 0; i < 4000; i++ {
		if i%7 == 0 {
			if err := db.Put([]byte("pinned"), []byte("v2")); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := db.Put(benchKey(i), filler); err != nil {
			t.Fatal(err)
		}
	}
	waitForStats(t, db, "至少跑过一轮 Compaction", func(s Stats) bool {
		return s.Compaction.Count > 0
	})

	v, err := snap.Get([]byte("pinned"))
	if err != nil {
		t.Fatalf("快照读 pinned 失败: %v（旧版本被 Compaction 丢掉了）", err)
	}
	if string(v) != "v1" {
		t.Fatalf("快照读到 %q, want v1", v)
	}

	snap.Release()
	if got := db.Stats().LiveSnapshots; got != 0 {
		t.Fatalf("Release 之后 LiveSnapshots = %d, want 0", got)
	}
	// Release 之后同一句读到的就是最新值 —— 顺带证明 Release 没有把数据读坏。
	if got := mustGet(t, db, "pinned"); got != "v2" {
		t.Fatalf("Release 之后 Get(pinned) = %q, want v2", got)
	}
}

// ── 失败语义 ──────────────────────────────────────────────────────

// TestWALFailureStopsTheDatabase 检验"日志写不进去"时整个库会停下来。
//
// 这是组提交引入的一个必须显式规定的语义。一条批次是原子的，但一个**提交组**
// 不是：组里靠前的批次可能已经进了 WAL，靠后的没有。如果这时候只把错误返回给
// 那几个失败的写者、然后继续服务，就会出现"返回了错误但其实写成功了"——
// 调用方据此重试会重复写、据此放弃会丢数据，而且不报任何错。
//
// 所以规定是：组里任何一步出错，整组一起失败，并且整个库停下（bgErr），
// 后续写入一律返回同一个错误。重开数据库即可，WAL 仍然是唯一的事实来源。
//
// 这里用"把日志句柄关掉"来制造一次真实的写失败：它走的正是 appendWriteGroup
// 里那条磁盘故障分支。
func TestWALFailureStopsTheDatabase(t *testing.T) {
	db := openTestDB(t, nil)
	// Close 会因为日志已经被我们关掉而报错，这不是本用例要断言的东西。
	defer func() { _ = db.Close() }()

	const victim = "written-before-failure"
	if err := db.Put([]byte(victim), []byte("v")); err != nil {
		t.Fatalf("Put before failure: %v", err)
	}

	// 直接关掉当前的日志句柄，模拟磁盘/文件系统层面的写失败。
	if err := db.log.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	err := db.Put([]byte("doomed"), []byte("v"))
	if err == nil {
		t.Fatal("日志已不可写，Put 却返回了 nil")
	}
	if errors.Is(err, ErrClosed) {
		t.Fatalf("这里应当是磁盘写失败而非关库：%v", err)
	}

	// 失败的这一批不能被读出来 —— 它没有进 MemTable。
	if _, gerr := db.Get([]byte("doomed")); !errors.Is(gerr, ErrNotFound) {
		t.Fatalf("失败的写入却读到了值：%v", gerr)
	}
	// 之前成功的写入不受影响。
	if got := mustGet(t, db, victim); got != "v" {
		t.Fatalf("Get(%q) = %q, want v", victim, got)
	}

	// 库已经停下：后续写入必须立刻拿到同一个错误，而不是各写各的。
	// 用同一个后台错误做判据（错误值本身可以是包装过的）。
	first := err.Error()
	if err := db.Put([]byte("after-failure"), []byte("v")); err == nil {
		t.Fatal("库已经停下，Put 却返回了 nil")
	} else if err.Error() != first {
		t.Fatalf("后续写入拿到的错误变了：%q vs %q", err, first)
	}
	// 停库也意味着读仍然可用（只读不写是安全的）。
	if got := mustGet(t, db, victim); got != "v" {
		t.Fatalf("停库后读 %q = %q, want v", victim, got)
	}
}

// ── 生命周期竞态 ──────────────────────────────────────────────────

// TestCloseWhileWritersRunning 检验关库与并发写不会互相等待，也不会写坏状态。
//
// 关键是不能挂：Close 要先排空写队列（否则它会在 db.mu 上与提交队长互相等待），
// 而排空之后每个写者都必须拿到一个确定的结论（成功或 ErrClosed），不能有人被
// 落在队列里永远等下去。
func TestCloseWhileWritersRunning(t *testing.T) {
	for round := 0; round < 5; round++ {
		db := openTestDB(t, nil)

		stop := make(chan struct{})
		var wg sync.WaitGroup
		var mu sync.Mutex
		var unexpected []error

		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					err := db.Put([]byte(fmt.Sprintf("c%d-%d-%d", round, w, i)), []byte("v"))
					if err == nil {
						continue
					}
					if !errors.Is(err, ErrClosed) {
						mu.Lock()
						unexpected = append(unexpected, err)
						mu.Unlock()
					}
					return
				}
			}(w)
		}

		time.Sleep(2 * time.Millisecond)
		close(stop)
		if err := db.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
		wg.Wait()

		mu.Lock()
		for _, err := range unexpected {
			mu.Unlock()
			t.Fatalf("关库期间写入返回了意外错误: %v", err)
		}
		mu.Unlock()

		// 关库之后的写入必须立刻拿到 ErrClosed，而不是挂住。
		if err := db.Put([]byte("after-close"), []byte("v")); !errors.Is(err, ErrClosed) {
			t.Fatalf("关库后 Put = %v, want ErrClosed", err)
		}
	}
}

// TestConcurrentMixedWorkload 是一轮"什么都在跑"的压测：写入、点查、快照读、
// 范围扫描、后台 Flush 与 Compaction 同时进行。
//
// 它不检查某一个具体值（那需要事先约定写入顺序），而是检查几条在任何交错下
// 都必须成立的不变式。真正的价值在于 -race 下跑它 —— M4 的验收标准就是它。
func TestConcurrentMixedWorkload(t *testing.T) {
	const (
		writers    = 4
		keysPerW   = 300
		readers    = 4
		iterations = 200
	)

	db := openTestDB(t, func(o *Options) {
		// 压低 MemTable 阈值，让这条测试同时压到 Flush 与 Compaction 的路径。
		o.MemTableSize = 32 << 10
		o.L0CompactionTrigger = 2
		o.LevelBaseSize = 64 << 10
	})
	defer db.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	fail := make(chan error, writers+readers)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			value := bytes.Repeat([]byte(strconv.Itoa(w)), 120)
			for i := 0; i < keysPerW; i++ {
				k := benchKey(w*keysPerW + i)
				if err := db.Put(k, value); err != nil {
					fail <- err
					return
				}
			}
			// 再覆盖写一遍，制造同 key 的多版本（Compaction 的丢弃逻辑才会被走到）。
			for i := 0; i < keysPerW; i++ {
				k := benchKey(w*keysPerW + i)
				if err := db.Put(k, value); err != nil {
					fail <- err
					return
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				select {
				case <-stop:
					return
				default:
				}

				// ① 点查：值要么不存在，要么是某个写者那种"重复字节"的形态。
				if v, err := db.Get(benchKey(i % (writers * keysPerW))); err != nil {
					if !errors.Is(err, ErrNotFound) {
						fail <- err
						return
					}
				} else if len(v) != 120 || !bytes.Equal(v, bytes.Repeat(v[:1], 120)) {
					fail <- fmt.Errorf("点查读到畸形值 %d 字节", len(v))
					return
				}

				// ② 快照 + 范围扫描：只要不报错、且顺序严格递增，就算通过。
				//    扫描期间写者一直在插数据，所以条数不做断言。
				snap := db.GetSnapshot()
				it := snap.NewIterator(&IteratorOptions{
					LowerBound: benchKey(0),
					UpperBound: benchKey(writers*keysPerW - 1),
				})
				var prev []byte
				for it.SeekToFirst(); it.Valid(); it.Next() {
					k := it.Key()
					if prev != nil && bytes.Compare(prev, k) >= 0 {
						fail <- fmt.Errorf("迭代器顺序错误：%q 之后是 %q", prev, k)
						it.Close()
						snap.Release()
						return
					}
					prev = append(prev[:0], k...)
				}
				if err := it.Error(); err != nil {
					fail <- err
					it.Close()
					snap.Release()
					return
				}
				it.Close()
				snap.Release()
			}
		}(r)
	}

	go func() {
		waitForStatsQuiet(db, writers*keysPerW)
		close(stop)
	}()

	wg.Wait()
	close(fail)
	for err := range fail {
		t.Fatalf("混合负载下出现不一致: %v", err)
	}

	// 收尾：等后台收敛，再把每个 key 都读一遍，确认一条不丢。
	waitConverged(t, db)
	for w := 0; w < writers; w++ {
		want := bytes.Repeat([]byte(strconv.Itoa(w)), 120)
		for i := 0; i < keysPerW; i++ {
			k := benchKey(w*keysPerW + i)
			got, err := db.Get(k)
			if err != nil {
				t.Fatalf("收尾读取 %q 失败: %v", k, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("收尾读取 %q 的值不对：%d 字节", k, len(got))
			}
		}
	}
}

// waitForStatsQuiet 等数据写完（不再有在途的 Flush），用于给混合负载一个停止点。
// 它不失败：混合负载的读侧本来就不依赖"是否已落盘"。
func waitForStatsQuiet(db *DB, wantKeys int) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		s := db.Stats()
		if !s.HasImmutable && uint64(wantKeys*2) <= s.LastSequence {
			// 再等一小会儿，让最后一轮 Compaction 也走完。
			time.Sleep(20 * time.Millisecond)
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}
