package kvdb

import (
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/xiatianliang1024gm/kvdb/internal/compact"
	"github.com/xiatianliang1024gm/kvdb/internal/key"
	"github.com/xiatianliang1024gm/kvdb/internal/memdb"
	"github.com/xiatianliang1024gm/kvdb/internal/sst"
	"github.com/xiatianliang1024gm/kvdb/internal/version"
)

// errClosing 用于在关库过程中把后台任务的中断与真实的失败区分开。
//
// 两者必须分开：真实的失败要记进 bgErr 让前台写入停下来，
// 而"库正在关"什么都不用记 —— 它是预期路径。
var errClosing = errors.New("kvdb: database is closing")

// compactLoop 是后台 Compaction 协程。
//
// 它和 Flush 是两条独立的流水线：Flush 只负责把 Immutable 变成 L0 文件，
// Compaction 负责把 L0 收敛成 L1 以下的分层布局。合成一条会互相堵：
// 一次大 Compaction 要跑几百毫秒，期间 MemTable 写满就没法落盘，前台写入全部停摆。
func (db *DB) compactLoop() {
	defer db.bgWG.Done()
	for {
		select {
		case <-db.closeCh:
			return
		case <-db.compactCh:
		}
		if err := db.runCompactions(); err != nil {
			if errors.Is(err, errClosing) {
				return
			}
			db.setBgErr(err)
			return
		}
	}
}

// runCompactions 反复挑选并执行 Compaction，直到没有需要做的。
//
// 每轮都重新从"当前版本"出发挑一次，而不是一次挑完排队执行：
// 上一轮的结果会改变各层的规模，也改变了下一轮该挑谁。
func (db *DB) runCompactions() error {
	// 手动 CompactRange（M10）可能正在跑：两边都从"当前版本"选输入、
	// 都经 commitCompaction 提交，并发跑会选到重叠的输入、提交互相踩脚。
	// compactMu 把手动与后台串成单线程；拿锁之前不持 db.mu，无锁序问题。
	db.compactMu.Lock()
	defer db.compactMu.Unlock()
	for {
		v, snapshot, err := db.compactionPlan()
		if err != nil {
			return err
		}

		c := compact.Pick(v, db.levelConfig())
		if c == nil {
			v.Unref()
			return nil
		}

		// 整轮 Compaction 期间都持有这个版本引用：输入文件因此不会被
		// garbage collection 收走 —— 归并读到一半文件消失，是这类系统里
		// 最难复现的一类崩溃。
		res, err := compact.Run(c, v, db.compactionEnv(v, c, snapshot))
		v.Unref()
		if err != nil {
			if errors.Is(err, errClosing) {
				return err
			}
			db.logErrorf("compaction failed: %s: %v", c, err)
			return fmt.Errorf("kvdb: compaction (%s): %w", c, err)
		}

		db.recordCompaction(res)
		db.logInfof("compaction done: %s inputs=%d(%s) outputs=%d(%s) dropped=%d",
			c, res.InputFiles, humanBytes(res.InputBytes), res.OutputFiles, humanBytes(res.OutputBytes), res.DroppedRecords)
		db.collectGarbage()
	}
}

// compactionEnv 构造一次 Compaction 的执行环境。后台自动 Compaction 与
// 手动 CompactRange 用的是同一份 —— 归并语义不该因为"谁触发的"而不同。
//
// v 必须传调用方持有引用的那个版本而不是 db.v：范围墓碑表随版本走，
// 并发提交随时可能把 db.v 换掉，锁外读 db.v 是数据竞争。
func (db *DB) compactionEnv(v *version.Version, c *compact.Compaction, snapshot uint64) compact.Env {
	return compact.Env{
		Dir:              db.opts.Dir,
		ICmp:             db.icmp,
		BlockSize:        db.opts.BlockSize,
		BloomBitsPerKey:  db.opts.BloomBitsPerKey,
		Compression:      db.opts.Compression.toType(),
		RateLimiter:      db.rateLimiter,
		TargetFileSize:   db.opts.targetFileSize(c.OutputLevel),
		SmallestSnapshot: snapshot,
		Filter:           db.opts.CompactionFilter,
		Merge:            db.opts.MergeOperator,
		RangeDeletions:   v.RangeDeletions(),
		AllocFileNum:     db.vset.AllocFileNum,
		Reader:           db.readerForMaintenance,
		Commit:           db.commitCompaction,
	}
}

// CompactRange 把 [start, end) 区间内的数据一路向下压实（M10，EXTENSIONS.md §5.4）。
//
// start / end 为 nil 表示该侧不设界；end <= start 是空区间，直接返回 nil。
//
// 做三件事：
//
//  1. 先 Flush —— MemTable 里的数据不落盘就不参与 Compaction，
//     "刚写完就清表"的场景会漏掉整整一张表；
//  2. 从 L0 起逐层把与区间重叠的文件压到下一层，循环到区间内的数据
//     全部落在最底层（或区间里已经没有文件）；
//  3. 每次提交后 retireRangeTombstonesLocked 照常判定墓碑退休。
//
// 为什么需要它：自动 Compaction 是分数驱动的（L0 文件数 / 层容量），清表
// 之后剩下的范围墓碑和零星文件可能永远凑不够触发条件 —— 空间就永远
// 回收不了。CompactRange 是"不等分数、现在就搬"的手动通道，范围删除
// 之后的空间回收（EXTENSIONS.md §4.1 的"回收时机由 Compaction 决定"）
// 就落在它这一路。
//
// 代价必须写明：区间数据每经过一层就被完整重写一次，写放大 ≈ 层数。
// 这是"手动、低频"的操作定位 —— 别当日常维护用。
//
// 并发：与后台 Compaction 互斥（compactMu），与读写完全并发 —— 版本引用
// 保证输入文件在归并期间不被回收，语义与后台 Compaction 一致。
func (db *DB) CompactRange(start, end []byte) error {
	if start != nil && end != nil && db.cmp.Compare(start, end) >= 0 {
		return nil
	}
	// 先 Flush 再拿 compactMu：Flush 会等 flushLoop（它不碰 compactMu），
	// 顺序反了就是"拿着 compactMu 等一个不需要 compactMu 的后台任务"，白等。
	if err := db.Flush(); err != nil {
		return err
	}
	db.compactMu.Lock()
	defer db.compactMu.Unlock()
	for {
		v, snapshot, err := db.compactionPlan()
		if err != nil {
			if errors.Is(err, errClosing) {
				return ErrClosed
			}
			return err
		}
		c := pickRangeCompaction(v, db.cmp, start, end)
		if c == nil {
			// 区间内的数据已经全部在最底层（或区间里没有数据）：
			// 再往下没有层可去，区间维度的压实收敛。
			v.Unref()
			return nil
		}
		db.logInfof("manual compaction start: %s", c)
		res, err := compact.Run(c, v, db.compactionEnv(v, c, snapshot))
		v.Unref()
		if err != nil {
			if errors.Is(err, errClosing) {
				return ErrClosed
			}
			db.logErrorf("manual compaction failed: %s: %v", c, err)
			return fmt.Errorf("kvdb: compact range: %w", err)
		}
		db.recordCompaction(res)
		db.logInfof("manual compaction done: %s inputs=%d(%s) outputs=%d(%s) dropped=%d",
			c, res.InputFiles, humanBytes(res.InputBytes), res.OutputFiles, humanBytes(res.OutputBytes), res.DroppedRecords)
		db.collectGarbage()
	}
}

// pickRangeCompaction 从 L0 向下找第一层"有与 [start, end) 重叠的文件、且
// 下面还有一层可去"的层，照 compact.Pick 的构造规则配好输入集合；找不到
// 返回 nil。start / end 为 nil 表示该侧不设界。
//
// 与 Pick 的分工是刻意的：Pick 回答"分数要求我现在搬谁"，这里回答
// "这个区间还有谁没到底"。前者的输入集合选择（最老文件、最左文件）是
// 为分摊服务的设计，手动压实要的是"区间内一个不留"，直接取全部重叠文件。
func pickRangeCompaction(v *version.Version, cmp key.Comparer, start, end []byte) *compact.Compaction {
	for level := 0; level < v.NumLevels()-1; level++ {
		var inputs []*version.FileMeta
		for _, f := range v.Files(level) {
			if overlapsRange(cmp, key.UserKey(f.Smallest), key.UserKey(f.Largest), start, end) {
				inputs = append(inputs, f)
			}
		}
		if len(inputs) == 0 {
			continue
		}
		// 输入的区间用 internal key 求（与 pickL0 的 span 同构）：
		// Overlapping 的另一半必须一起归并，否则"同层不重叠"当场破掉。
		icmp := v.Comparer()
		var smallest, largest []byte
		for _, f := range inputs {
			if smallest == nil || icmp.Compare(f.Smallest, smallest) < 0 {
				smallest = f.Smallest
			}
			if largest == nil || icmp.Compare(f.Largest, largest) > 0 {
				largest = f.Largest
			}
		}
		return &compact.Compaction{
			Level:       level,
			OutputLevel: level + 1,
			Inputs: [2][]*version.FileMeta{
				inputs,
				v.Overlapping(level+1, smallest, largest),
			},
		}
	}
	return nil
}

// overlapsRange 判定文件区间 [fileSmallest, fileLargest] 与查询区间
// [start, end) 是否相交（user key 维度，半开；start/end 为 nil 表示不设界）。
// 判据与 rangeHasRecordsLocked 一致：fileSmallest < end 且 fileLargest >= start。
func overlapsRange(cmp key.Comparer, fileSmallest, fileLargest, start, end []byte) bool {
	if start != nil && cmp.Compare(fileLargest, start) < 0 {
		return false
	}
	if end != nil && cmp.Compare(fileSmallest, end) >= 0 {
		return false
	}
	return true
}

// compactionPlan 取出"用哪个版本挑 Compaction、可以丢到哪个序列号为止"。
//
// 返回的版本已经 Ref 过一次，调用方必须 Unref。
func (db *DB) compactionPlan() (*version.Version, uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, 0, errClosing
	}
	if db.bgErr != nil {
		return nil, 0, db.bgErr
	}
	v := db.v
	v.Ref()
	return v, db.smallestSnapshotLocked(), nil
}

// levelConfig 把 Options 里的分层参数转成 compact 包看得懂的形式。
func (db *DB) levelConfig() compact.LevelConfig {
	return compact.LevelConfig{
		L0CompactionTrigger: db.opts.L0CompactionTrigger,
		MaxLevels:           db.opts.MaxLevels,
		LevelMaxBytes:       db.opts.levelMaxBytes,
		TargetFileSize:      db.opts.targetFileSize,
	}
}

// readerForMaintenance 给 Compaction 提供输入文件的读取器。
//
// 归并在锁外跑，而 collectGarbage 会增删读取器表，所以这里要短暂拿一次读锁。
// 一次 Compaction 只会调用它"输入文件数"次（不是每条记录一次），可以忽略。
func (db *DB) readerForMaintenance(num uint64) (*sst.Reader, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.readerFor(num)
}

// commitCompaction 把 Compaction 的输出安装进版本树，并删掉输入文件。
//
// 三步顺序不能乱：
//
//  1. **先打开输出文件的读取器**（锁外）—— 用不是零成本的操作占着写锁会堵住所有读；
//  2. **再登记读取器**（锁内），保证新版本一旦生效，它引用的每个文件都已经可读；
//  3. **最后提交版本变更**，由 Manifest 落盘保证原子性。
//
// 反过来的话，会出现"版本已经指向新文件、但读取器表里还没有它"的窗口，
// 那一刻的读就会报"没有登记的读取器"而不是拿到数据。
func (db *DB) commitCompaction(c *compact.Compaction, outputs []*version.FileMeta) error {
	opened := make(map[uint64]*sst.Reader, len(outputs))
	for _, f := range outputs {
		r, err := db.openReader(f.Num)
		if err != nil {
			closeReaders(opened)
			removeSSTFiles(db.opts.Dir, outputs)
			return err
		}
		opened[f.Num] = r
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bgErr != nil {
		closeReaders(opened)
		removeSSTFiles(db.opts.Dir, outputs)
		return db.bgErr
	}
	for num, r := range opened {
		db.readers[num] = r
	}

	edit := db.baseEdit()
	for _, f := range outputs {
		edit.Added = append(edit.Added, f.Edit(c.OutputLevel))
	}
	for _, f := range c.Inputs[0] {
		edit.Deleted = append(edit.Deleted, version.FileEdit{Level: c.Level, Num: f.Num})
	}
	for _, f := range c.Inputs[1] {
		edit.Deleted = append(edit.Deleted, version.FileEdit{Level: c.OutputLevel, Num: f.Num})
	}
	if err := db.logAndApplyLocked(edit); err != nil {
		for num := range opened {
			delete(db.readers, num)
		}
		closeReaders(opened)
		removeSSTFiles(db.opts.Dir, outputs)
		return fmt.Errorf("kvdb: commit compaction: %w", err)
	}
	// 提交之后顺势判定一次范围墓碑的退休（M7）：这次 Compaction 可能刚好把
	// 某个区间里的最后一批数据物理清掉了。退休 = 把墓碑从 Version 的全局表
	// 里摘掉，同样要走 Manifest，但只有真的有墓碑可退时才会发生。
	db.retireRangeTombstonesLocked()
	return nil
}

// retireRangeTombstonesLocked 把"区间内已经没有任何数据"的范围墓碑从版本里摘掉。
// 调用方必须持有 db.mu 的写锁。
//
// 一条墓碑 T 可以退休，当且仅当以下两条同时成立：
//
//  1. **没有存活快照需要它**：T.Seq <= 最小存活快照。快照 seq 更小的读者
//     还要靠 T 区分"删了"和"还没删"；
//  2. **区间内不再有任何 seq < T.Seq 的记录**，任何地方：
//     所有层的 SST（按 user key 区间与 [T.Start, T.End) 求交）、以及
//     MemTable / Immutable。少了这一半会出正确性问题——墓碑摘掉之后，
//     一条还躺在某处的被遮蔽记录会"复活"。SST 侧按文件区间判定是保守的
//     （文件里可能只有 seq > T.Seq 的新记录），代价只是退休得晚一点。
//
// 判定不到退休条件就什么都不做：多留一条墓碑只付 O(范围数) 的查找开销，
// 提前退休则丢数据，两个方向的代价完全不对称。
func (db *DB) retireRangeTombstonesLocked() {
	snapshot := db.smallestSnapshotLocked()
	var retired []key.RangeDeletion
	for _, t := range db.v.RangeDeletions() {
		if t.Seq > snapshot || db.rangeHasRecordsLocked(t) {
			continue
		}
		retired = append(retired, t)
	}
	if len(retired) == 0 {
		return
	}
	edit := db.baseEdit()
	edit.RetiredTombstones = retired
	if err := db.logAndApplyLocked(edit); err != nil {
		// 退休失败不另立故障：Manifest 追加失败意味着磁盘不可信，
		// 后续的 Flush / Compaction 会拿到同一个错误并停库。
		db.logErrorf("retire range tombstones: %v", err)
	}
}

// rangeHasRecordsLocked 判断区间 [t.Start, t.End) 内是否还有任何记录
// （SST 各层 + MemTable + Immutable）。调用方必须持有 db.mu。
func (db *DB) rangeHasRecordsLocked(t key.RangeDeletion) bool {
	// SST：文件元信息里存的是 internal key，user 部分即文件的 key 区间。
	// 相交判定（半开区间）：ukSmallest < t.End 且 ukLargest >= t.Start。
	for level := 0; level < db.v.NumLevels(); level++ {
		for _, f := range db.v.Files(level) {
			if db.cmp.Compare(key.UserKey(f.Smallest), t.End) < 0 &&
				db.cmp.Compare(key.UserKey(f.Largest), t.Start) >= 0 {
				return true
			}
		}
	}
	// MemTable / Immutable：从 t.Start 起顺序扫到 t.End，看有没有
	// seq < t.Seq 的记录。范围删除是低频操作，这段扫描的代价可以接受。
	for _, m := range []*memdb.MemTable{db.mem, db.imm} {
		if m == nil || m.Empty() {
			continue
		}
		it := m.Seek(key.MaxSeqNum, t.Start)
		for ; it.Valid(); it.Next() {
			ik := it.Key()
			uk := key.UserKey(ik)
			if db.cmp.Compare(uk, t.End) >= 0 {
				break
			}
			if key.SeqNum(ik) < t.Seq {
				return true
			}
		}
	}
	return false
}

// recordCompaction 累计 Compaction 的规模，供 Stats 与压测输出使用。
func (db *DB) recordCompaction(res compact.Result) {
	db.counters.compactions.Add(1)
	db.counters.compactionInputFiles.Add(int64(res.InputFiles))
	db.counters.compactionOutputFiles.Add(int64(res.OutputFiles))
	db.counters.compactionInputBytes.Add(res.InputBytes)
	db.counters.compactionOutputBytes.Add(res.OutputBytes)
	db.counters.compactionDropped.Add(int64(res.DroppedRecords))
}

// setBgErr 记录后台任务的失败并唤醒等待中的写者。
func (db *DB) setBgErr(err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.failLocked(err)
}

// failLocked 记下让整个库停止服务的错误。调用方必须持有 db.mu 的写锁。
//
// "停下来"而不是"继续试"是刻意的：会走到这里的错误（日志写失败、MemTable 冻结失败、
// 后台落盘或合并失败）都意味着内存与磁盘已经开始不一致，继续接受写入只会把不一致放大。
// 停库比静默地写坏数据好 —— 重开数据库即可，WAL 仍然是唯一的事实来源。
func (db *DB) failLocked(err error) {
	if err == nil {
		return
	}
	if db.bgErr == nil {
		db.bgErr = err
	}
	db.cond.Broadcast() // 唤醒等在 Immutable 落盘上的写者，让它拿到这个错误
}

// closeReaders 关闭一组读取器。
func closeReaders(readers map[uint64]*sst.Reader) {
	for _, r := range readers {
		_ = r.Close()
	}
}

// removeSSTFiles 删除一组 SST 文件。
func removeSSTFiles(dir string, files []*version.FileMeta) {
	for _, f := range files {
		os.Remove(sst.FilePath(dir, f.Num))
	}
}

// ── 快照登记 ──────────────────────────────────────────────────────
//
// Compaction 要决定"旧版本能丢到哪个序列号为止"。这个上界必须由**存活的快照**
// 决定：丢掉任何一个存活快照还需要读的版本都是静默的数据损坏（读到新值，
// 或者本该读到的值变成"不存在"），而且不会报任何错。
//
// 所以 M2 里"Release 只是标记失效"的语义在 M3 变成了必须的动作：
// 忘记 Release 不会出错，只会让 Compaction 一直保守下去（旧版本清不掉）。

// registerSnapshotLocked 登记一个存活的快照。
func (db *DB) registerSnapshotLocked(seq uint64) {
	db.snapshots[seq]++
}

// releaseSnapshot 注销一个快照。
//
// 它可能在 DB 已经关闭之后才被调用（用户忘了 Release，程序退出前统一清理），
// 所以不检查 closed：这里只是维护一个 map，与文件句柄无关。
func (db *DB) releaseSnapshot(seq uint64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if n := db.snapshots[seq]; n > 1 {
		db.snapshots[seq] = n - 1
	} else {
		delete(db.snapshots, seq)
	}
}

// smallestSnapshotLocked 返回"还可能有读者"的最小序列号。
//
// 没有存活快照时返回当前序列号：任何新读者看到的都是最新版本，
// 所以比它更旧的版本不可能被读到。
func (db *DB) smallestSnapshotLocked() uint64 {
	if len(db.snapshots) == 0 {
		return db.lastSeq
	}
	min := uint64(math.MaxUint64)
	for seq := range db.snapshots {
		if seq < min {
			min = seq
		}
	}
	return min
}

// releaseVersion 释放一个版本引用；归零时顺手做一次垃圾回收。
//
// 回收的时机很关键：迭代器关闭是最常见的"最后一个引用消失"的时刻，
// 等到下次 Flush 才回收的话，被合并掉的文件会在磁盘上多躺很久。
func (db *DB) releaseVersion(v *version.Version) {
	if v == nil {
		return
	}
	v.Unref()
	if v.RefCount() == 0 {
		db.collectGarbage()
	}
}
