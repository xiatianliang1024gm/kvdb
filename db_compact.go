package kvdb

import (
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/xiatianliang1024gm/kvdb/internal/compact"
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
		res, err := compact.Run(c, v, compact.Env{
			Dir:              db.opts.Dir,
			ICmp:             db.icmp,
			BlockSize:        db.opts.BlockSize,
			BloomBitsPerKey:  db.opts.BloomBitsPerKey,
			Compression:      db.opts.Compression.toType(),
			RateLimiter:      db.rateLimiter,
			TargetFileSize:   db.opts.targetFileSize(c.OutputLevel),
			SmallestSnapshot: snapshot,
			Filter:           db.opts.CompactionFilter,
			AllocFileNum:     db.vset.AllocFileNum,
			Reader:           db.readerForMaintenance,
			Commit:           db.commitCompaction,
		})
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
	return nil
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
