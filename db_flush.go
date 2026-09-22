package kvdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/xiatianliang1024gm/kvdb/internal/cache"
	"github.com/xiatianliang1024gm/kvdb/internal/key"
	"github.com/xiatianliang1024gm/kvdb/internal/memdb"
	"github.com/xiatianliang1024gm/kvdb/internal/sst"
	"github.com/xiatianliang1024gm/kvdb/internal/version"
	"github.com/xiatianliang1024gm/kvdb/internal/wal"
)

// recover 完成打开数据库时的恢复。
//
// 顺序是刻意的，每一步都为下一步准备了前提：
//
//  1. 从 CURRENT/Manifest 重建"当前有哪些文件、各自在第几层"；
//     目录里没有 Manifest 时退化成一次目录扫描（M2 及更早的目录迁移）；
//  2. 清掉没有任何版本引用的 .sst；
//  3. 重放 WAL，得到内存里的那张表；
//  4. **先把重放结果落成 SST，再删旧日志** —— 反过来会丢掉日志承载的数据。
//
// 第 1 步与第 4 步合起来就是 M3 相对 M2 最大的变化：M2 打开时看到的文件列表
// 完全来自目录扫描，因此"已经提交"和"做了一半"在磁盘上长得一模一样；
// M3 之后这两者由 Manifest 明确区分 —— 提交过的文件才会出现在版本里，
// 没提交过的连打开校验都不需要，直接按孤儿删掉。
func (db *DB) recover() error {
	hasManifest, truncated, err := db.vset.Recover()
	if err != nil {
		return err
	}
	db.report.TruncatedManifest = truncated

	if hasManifest {
		db.setCurrentVersionLocked()
		if err := db.openCommittedFilesLocked(); err != nil {
			return err
		}
	} else if err := db.migrateFromScanLocked(); err != nil {
		return err
	}

	// 到这里"当前有哪些文件"已经完全确定，剩下的 .sst 一律是孤儿。
	if err := db.removeObsoleteFilesLocked(); err != nil {
		return err
	}
	// 把历史积累下来的增量编辑收敛成一份新 Manifest。
	// 每次打开都做一次，代价只有"当前文件数 × 每条几十字节"，换来的是
	// 打开时间不随使用年限增长。
	if err := db.vset.NewManifest(); err != nil {
		return err
	}

	// ── 重放 WAL ─────────────────────────────────────────────────
	logs, err := wal.ListLogs(db.opts.Dir)
	if err != nil {
		return err
	}
	db.mem = memdb.New(db.cmp, 0)
	for _, num := range logs {
		res, err := wal.Replay(db.opts.Dir, num, db.applyBatch)
		if err != nil {
			return err
		}
		db.report.ReplayedRecords += res.Records
		if res.Corrupt != nil {
			db.report.TornLogs = append(db.report.TornLogs, num)
		}
	}
	// Manifest 里的 lastSeq 与重放出来的取大者：
	// 前者覆盖"日志已被清理"的部分，后者覆盖"还没进 Manifest 的最新写入"。
	db.vset.SetLastSeq(db.lastSeq)

	// 迁移自 M2 的目录有一个坑：那批 SST 里的序列号可能没有任何日志记录能证明。
	// （M2 的 lastSeq 只靠重放 WAL 恢复，而"所有 MemTable 都落盘、当前日志是空的"
	// 这个正常时刻一旦关闭，日志里就没有记录了。）此时 lastSeq 会是 0，
	// 新写入拿到的序列号与 SST 里的老记录撞号，而撞号之后按 (seq 降序) 排序
	// 根本分不出谁更新 —— 是静默的数据损坏。所以这里退化成扫一遍文件取真实上界。
	if db.lastSeq == 0 && db.v.FileCount() > 0 {
		seq, err := db.maxSequenceInVersion()
		if err != nil {
			return err
		}
		db.lastSeq = seq
		db.vset.SetLastSeq(seq)
	}

	// 重放出来的内容先落盘：只有它变成 SST，旧日志才能安全删除。
	if !db.mem.Empty() {
		if err := db.flushRecoveredLocked(); err != nil {
			return err
		}
	}

	// 换上新的空表与新的日志继续服务，编号一定大于目录里的所有文件。
	logNum := db.vset.AllocFileNum()
	newLog, err := wal.Create(db.opts.Dir, logNum)
	if err != nil {
		return err
	}
	db.log = newLog
	db.mem = memdb.New(db.cmp, logNum)
	db.vset.SetLogNumber(logNum)

	// 此刻所有编号 < logNum 的日志都已经被上面的 Flush 或更早的 SST 覆盖。
	return db.removeObsoleteLogsLocked()
}

// migrateFromScanLocked 处理"目录里没有 Manifest"的情形：扫描目录重建文件列表，
// 结果全部放进 L0，并把它固化成第一份 Manifest。
//
// 为什么全部放 L0 而不是猜层号：L0 的规则（区间可以重叠、按编号从新到旧查找）
// 对任何输入都成立，而猜错层号会直接破坏"同层不重叠"这个二分查找的前提。
// 至于这些文件之间到底谁新谁旧，编号已经说明了 —— 编号大 = 更新。
func (db *DB) migrateFromScanLocked() error {
	scanned, discarded, maxNum, err := scanSSTFiles(db.opts.Dir, db.cmp, db.blockCache)
	if err != nil {
		return err
	}
	db.report.DiscardedFiles = discarded
	db.report.RecoveredFromScan = true

	metas := make([]*version.FileMeta, 0, len(scanned))
	for _, s := range scanned {
		metas = append(metas, s.meta)
		db.readers[s.meta.Num] = s.reader
	}
	// 日志与 SST 共享同一个编号空间，水位要取两者的最大值。
	logs, err := wal.ListLogs(db.opts.Dir)
	if err != nil {
		return err
	}
	if n := len(logs); n > 0 && logs[n-1] > maxNum {
		maxNum = logs[n-1]
	}
	if err := db.vset.SetFromScan(metas, maxNum); err != nil {
		return err
	}
	db.setCurrentVersionLocked()
	return nil
}

// openCommittedFilesLocked 打开当前版本引用的全部 SST 读取器。
//
// 这里打开失败是**不可容忍**的：Manifest 说这些文件已经提交，它们必然是被完整
// 写完并 fsync 过才可能进 Manifest。打不开说明磁盘上的数据真的坏了，
// 必须报错，而不是像 M2 那样把文件当成"崩溃的半成品"删掉 —— 那会丢掉已提交的数据。
func (db *DB) openCommittedFilesLocked() error {
	for level := 0; level < db.v.NumLevels(); level++ {
		for _, f := range db.v.Files(level) {
			r, err := db.openReader(f.Num)
			if err != nil {
				return fmt.Errorf("kvdb: open the committed sst %s (L%d): %w", sst.FileName(f.Num), level, err)
			}
			db.readers[f.Num] = r
		}
	}
	return nil
}

// applyBatch 把一条 WAL 记录（一个编码后的 WriteBatch）应用到 MemTable。
//
// 序列号直接采用批次头部里的值，绝不重新分配：否则同一个目录在每次恢复后
// 都会得到不同的版本顺序，快照语义随之崩坏。
func (db *DB) applyBatch(record []byte) error {
	b, err := decodeBatch(record)
	if err != nil {
		return err
	}
	if b.Len() == 0 {
		return nil
	}
	seq := b.Sequence()
	if err := b.rangeRecords(seq, func(seq uint64, kind key.Kind, userKey, value []byte) bool {
		db.mem.Add(seq, kind, userKey, value)
		return true
	}); err != nil {
		return err
	}
	if last := seq + uint64(b.Len()) - 1; last > db.lastSeq {
		db.lastSeq = last
	}
	return nil
}

// maxSequenceInVersion 扫描当前版本里的全部记录，返回最大的序列号。
//
// 只在"迁移自 M2 的目录且重放不出任何记录"时才会被调用，属于一次性的兜底扫描：
// 正确性远比这一次全量扫描的代价重要。
func (db *DB) maxSequenceInVersion() (uint64, error) {
	var max uint64
	for level := 0; level < db.v.NumLevels(); level++ {
		for _, f := range db.v.Files(level) {
			r := db.readers[f.Num]
			if r == nil {
				return 0, fmt.Errorf("kvdb: internal error: no open reader for sst file %d", f.Num)
			}
			if err := r.Iterate(func(ik, _ []byte) bool {
				if seq := key.SeqNum(ik); seq > max {
					max = seq
				}
				return true
			}); err != nil {
				return 0, err
			}
		}
	}
	return max, nil
}

// scanSSTFiles 扫描目录里的 SST 文件（迁移旧目录时用）。
//
// 返回按编号升序排列的文件、被丢弃的文件名，以及目录里的最大编号。
//
// 这里保留了 M2 的损坏处置策略，因为迁移路径面对的就是 M2 的目录形态：
// **可丢弃的只有"没写完"和"内容损坏"两类**（数据仍完整地躺在 WAL 里，
// 稍后会随重放重新落盘）；格式版本不匹配必须报错，静默删除一个旧格式的
// 数据目录是真实的数据损失。
func scanSSTFiles(dir string, cmp key.Comparer, c *cache.Cache) (files []scannedSST, discarded []string, maxNum uint64, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("kvdb: read dir %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), sst.Suffix) {
			continue
		}
		num, perr := strconv.ParseUint(strings.TrimSuffix(e.Name(), sst.Suffix), 10, 64)
		if perr != nil {
			continue // 不符合命名规则的文件不属于本引擎
		}
		if num > maxNum {
			maxNum = num
		}

		path := filepath.Join(dir, e.Name())
		r, oerr := sst.Open(path, sst.OpenOptions{Comparer: cmp, Cache: c, FileNum: num})
		if oerr != nil {
			if isDiscardableSSTError(oerr) {
				discarded = append(discarded, e.Name())
				_ = os.Remove(path)
				continue
			}
			return nil, nil, 0, fmt.Errorf("kvdb: open sst %s: %w", e.Name(), oerr)
		}
		meta, merr := metaFromReader(r, num)
		if merr != nil {
			if isDiscardableSSTError(merr) {
				r.Close()
				discarded = append(discarded, e.Name())
				_ = os.Remove(path)
				continue
			}
			r.Close()
			return nil, nil, 0, fmt.Errorf("kvdb: read sst %s: %w", e.Name(), merr)
		}
		files = append(files, scannedSST{meta: meta, reader: r})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].meta.Num < files[j].meta.Num })
	return files, discarded, maxNum, nil
}

// scannedSST 是目录扫描的结果：文件的元信息 + 已经打开的读取器。
type scannedSST struct {
	meta   *version.FileMeta
	reader *sst.Reader
}

// metaFromReader 现算一个已打开文件的 key 区间。
//
// 正常路径上不需要做这件事（区间由 Manifest 记着），只有迁移旧目录时要现算：
// 那些目录里没有任何版本元数据，区间只能从文件本身读回来。
func metaFromReader(r *sst.Reader, num uint64) (*version.FileMeta, error) {
	smallest, err := r.SmallestKey()
	if err != nil {
		return nil, err
	}
	largest := r.LargestKey()
	if smallest == nil || largest == nil {
		// 空文件：理论上有 Footer 就至少有 Filter/MetaIndex/Index 三个块，
		// 但索引里可能一条数据块条目都没有。这种文件没有任何记录，
		// 放不进版本（区间为空无法参与查找），直接丢弃。
		return nil, fmt.Errorf("kvdb: sst %s contains no records", r.Path())
	}
	return &version.FileMeta{
		Num:      num,
		Size:     uint64(r.Size()),
		Smallest: smallest,
		Largest:  append([]byte(nil), largest...),
	}, nil
}

// isDiscardableSSTError 判断一个打不开的 SST 是否属于"可以安全丢弃"的情形。
//
//   - ErrBadFooter：Footer 缺失或不可识别。崩溃时写到一半的 Flush 就是这种形态，
//     它的数据仍然完整地躺在 WAL 里，随重放重新落盘，所以丢弃是安全的。
//   - ErrCorruptBlock：块的 CRC 校验失败或索引结构非法。磁盘损坏或写入中断都可能是
//     这个形态，数据已不可信，只能丢弃并如实上报。
//
// 其余错误（尤其是 ErrLegacyFormat）一律向上抛：静默删除一个旧格式的数据目录
// 是真实的数据损失，必须让使用者自己决定怎么处理。
func isDiscardableSSTError(err error) bool {
	return errors.Is(err, sst.ErrBadFooter) || errors.Is(err, sst.ErrCorruptBlock)
}

// flushLoop 是后台落盘协程。
//
// 它和前台写入之间只有 db.mu 与一个容量为 1 的通知通道：写入侧把 Immutable
// 挂好之后通知一次，这里就把它落盘。Flush 本身不持锁，因此耗时的磁盘 IO
// 不会挡住读。
func (db *DB) flushLoop() {
	defer db.bgWG.Done()
	for {
		select {
		case <-db.closeCh:
			return
		case <-db.flushCh:
		}

		for {
			db.mu.Lock()
			imm := db.imm
			if imm == nil {
				db.mu.Unlock()
				break
			}
			num := db.vset.AllocFileNum()
			db.mu.Unlock()

			// ── 锁外：写文件 + 打开读取器 ──
			meta, err := db.flushMemTable(imm, num)
			var reader *sst.Reader
			if err == nil && meta != nil {
				if reader, err = db.openReader(meta.Num); err != nil {
					removeSSTFile(db.opts.Dir, meta.Num)
					meta = nil
				}
			}

			// ── 锁内：登记 + 提交版本 ──
			db.mu.Lock()
			switch {
			case err != nil:
				// 落盘失败：保留 Immutable 不放，让写者拿到同一个错误并停止写入。
				db.bgErr = fmt.Errorf("kvdb: flush memtable: %w", err)
			case meta != nil:
				db.readers[meta.Num] = reader
				edit := db.baseEdit()
				edit.Added = append(edit.Added, meta.Edit(0))
				if aerr := db.logAndApplyLocked(edit); aerr != nil {
					delete(db.readers, meta.Num)
					_ = reader.Close()
					removeSSTFile(db.opts.Dir, meta.Num)
					err = fmt.Errorf("kvdb: commit flushed sst %s: %w", sst.FileName(meta.Num), aerr)
					db.bgErr = err
				} else {
					db.counters.flushFiles.Add(1)
					db.counters.flushBytes.Add(meta.Size)
					db.logInfof("flush done: sst=%s level=0 entries_from_log bytes=%s",
						sst.FileName(meta.Num), humanBytes(meta.Size))
				}
			}
			if err == nil && db.imm == imm {
				db.imm = nil
			}
			if err == nil {
				if rerr := db.removeObsoleteLogsLocked(); rerr != nil {
					err = rerr
					db.bgErr = rerr
				}
			}
			db.cond.Broadcast()
			db.mu.Unlock()

			// 新的 L0 文件可能就是压垮骆驼的那根稻草，提醒一次后台 Compaction。
			db.collectGarbage()
			db.notifyCompact()

			if err != nil {
				return
			}
		}
	}
}

// flushRecoveredLocked 把重放出来的 MemTable 落成 SST 并提交。
//
// 恢复期它必须同步完成（不像常规 Flush 那样交给后台）：它落盘之后，承载这批数据的
// 旧日志才能被删除，而"删日志"是这一步的直接目的。
func (db *DB) flushRecoveredLocked() error {
	num := db.vset.AllocFileNum()
	meta, err := db.flushMemTable(db.mem, num)
	if err != nil {
		return err
	}
	if meta == nil {
		return nil
	}
	reader, err := db.openReader(meta.Num)
	if err != nil {
		removeSSTFile(db.opts.Dir, meta.Num)
		return err
	}
	db.readers[meta.Num] = reader
	edit := db.baseEdit()
	edit.Added = append(edit.Added, meta.Edit(0))
	if err := db.logAndApplyLocked(edit); err != nil {
		delete(db.readers, meta.Num)
		_ = reader.Close()
		removeSSTFile(db.opts.Dir, meta.Num)
		return err
	}
	db.counters.flushFiles.Add(1)
	db.counters.flushBytes.Add(meta.Size)
	return nil
}

// flushMemTable 把一张 MemTable 写成 SST 并返回它的元信息。
//
// 它不修改任何共享状态，因此可以在不持锁的情况下慢慢跑；
// 文件"写完"的标志就是这里返回了非 nil 的 meta。
func (db *DB) flushMemTable(m *memdb.MemTable, num uint64) (*version.FileMeta, error) {
	if m.Empty() {
		return nil, nil
	}
	path := sst.FilePath(db.opts.Dir, num)
	w, err := sst.NewWriter(path, db.cmp, sst.WriterOptions{
		BlockSize:       db.opts.BlockSize,
		BloomBitsPerKey: db.opts.BloomBitsPerKey,
		Compression:     db.opts.Compression.toType(),
		BlockStats:      db.blockStats,
	})
	if err != nil {
		return nil, err
	}
	var first, last []byte
	it := m.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		ik := it.Key()
		if first == nil {
			first = append([]byte(nil), ik...)
		}
		last = append(last[:0], ik...)
		if err := w.Add(ik, it.Value()); err != nil {
			w.Abandon()
			return nil, err
		}
	}
	if err := it.Error(); err != nil {
		w.Abandon()
		return nil, err
	}
	if err := w.Finish(); err != nil {
		w.Abandon()
		return nil, err
	}
	return &version.FileMeta{
		Num:      num,
		Size:     uint64(w.Size()),
		Smallest: first,
		Largest:  append([]byte(nil), last...),
	}, nil
}

// collectGarbage 删除不再被任何存活版本引用的 SST。
//
// 判定完全来自版本信息，不依赖"文件能不能打开"：
//
//	所有存活版本引用到的文件 = 还有人可能读的文件
//
// 只有既不属于当前版本、也没有任何迭代器/快照握着旧版本的 .sst 才会走到这里。
// 关闭句柄与删文件放在锁外做 —— 删文件是 IO，不该堵住读。
func (db *DB) collectGarbage() { db.removeUnreferencedSSTs(false) }

// collectGarbageFinal 是 Close 之后补做的最后一次清理。
//
// **它必须存在，而且是这类系统里很容易漏掉的一步**：后台的 Compaction 可能正好在
// "提交了新版本"和"回收输入文件"之间被关闭打断 —— 提交那一刻输入文件就失去了
// 所有引用，而紧随其后的那次回收被 db.closed 挡掉了。结果就是目录里躺着一个
// 谁也不引用、也没有人记得该删的 .sst，要等到下次打开才被当孤儿清掉。
//
// 这个漏洞是靠一条"关闭之后目录里不该有多余文件"的断言抓出来的：
// 关库前的最后一次 Compaction 每次都会留下一个孤儿，稳定复现。
func (db *DB) collectGarbageFinal() { db.removeUnreferencedSSTs(true) }

// removeUnreferencedSSTs 是上面两者的共同实现；allowClosed 表示"库已经关了也照清"。
func (db *DB) removeUnreferencedSSTs(allowClosed bool) {
	db.mu.Lock()
	if db.closed && !allowClosed {
		db.mu.Unlock()
		return
	}
	live := db.vset.LiveFileNums()
	var nums []uint64
	var readers []*sst.Reader
	for num, r := range db.readers {
		if live[num] {
			continue
		}
		nums = append(nums, num)
		readers = append(readers, r)
	}
	db.mu.Unlock()

	for i, r := range readers {
		// 先关句柄再删文件：Windows 上打开着的文件删不掉。
		_ = r.Close()
		if err := os.Remove(sst.FilePath(db.opts.Dir, nums[i])); err != nil && !os.IsNotExist(err) {
			// 删不掉（文件被别的进程占着之类）就先留着登记项，下次再试。
			// 这里绝对不能把登记项摘掉：摘掉之后再也没有人知道这个文件该删了，
			// 它会一直躺在目录里，变成一个谁也解释不清的孤儿。
			continue
		}
		db.mu.Lock()
		delete(db.readers, nums[i])
		db.mu.Unlock()
	}
}

// removeObsoleteFilesLocked 删除目录里没有被任何版本引用的 .sst，并分类记进 RecoveryReport。
//
// 判据只有一条：**不在任何存活版本里 = 已经没有人需要它**。
//
// M3 之后这条判据是完备的：一个文件要么在某次 LogAndApply 里被写进 Manifest
// （于是它出现在版本里），要么那次提交没成功（于是它不在版本里，而它的数据另有归属
// —— Flush 的数据还在 WAL 里，Compaction 的输入还在版本里）。所以孤儿文件一律可以安全删除，
// 不需要像 M2 那样"先打开看看能不能读"才敢动手。
//
// 删除归删除，上报时要分成两类，因为成因不同、用户该采取的行动也不同：
//
//   - **能正常打开**的记进 ObsoleteFiles：一次写完并 fsync 了、但 Manifest 里的
//     提交还没落盘就崩溃的 Flush / Compaction 输出。它们本身是完整的，只是"没被确认过"。
//   - **打不开**的记进 DiscardedFiles：连 footer 都没写完的残片，那是写到一半被打断的写入。
//     M2 的 TestRecoveryDiscardsUnfinishedFlush 覆盖的正是这一形态，M3 必须继续如实上报。
//
// 还有第三类：打不开、但错误也不是"可识别的损坏"（典型是 M1 的线性布局）。
// 这类宁可让 Open 失败也不能静默删除，理由见 isDiscardableSSTError 的说明。
// 打开孤儿文件只为分类，而孤儿在正常情况下一个都不会有，这点 IO 不值得省。
func (db *DB) removeObsoleteFilesLocked() error {
	live := db.vset.LiveFileNums()
	entries, err := os.ReadDir(db.opts.Dir)
	if err != nil {
		return fmt.Errorf("kvdb: read dir %s: %w", db.opts.Dir, err)
	}
	var obsolete, discarded []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), sst.Suffix) {
			continue
		}
		num, perr := strconv.ParseUint(strings.TrimSuffix(e.Name(), sst.Suffix), 10, 64)
		if perr != nil {
			continue // 不符合命名规则的文件不属于本引擎
		}
		if live[num] {
			continue
		}
		if r := db.readers[num]; r != nil {
			_ = r.Close()
			delete(db.readers, num)
		}
		path := filepath.Join(db.opts.Dir, e.Name())
		switch r, oerr := db.openReader(num); {
		case oerr == nil:
			_ = r.Close()
			obsolete = append(obsolete, e.Name())
		case isDiscardableSSTError(oerr):
			discarded = append(discarded, e.Name())
		default:
			return fmt.Errorf("kvdb: %s is on disk but no version references it, and it cannot be interpreted: %w", e.Name(), oerr)
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("kvdb: remove obsolete sst %s: %w", e.Name(), err)
		}
	}
	sort.Strings(obsolete)
	sort.Strings(discarded)
	db.report.ObsoleteFiles = append(db.report.ObsoleteFiles, obsolete...)
	db.report.DiscardedFiles = append(db.report.DiscardedFiles, discarded...)
	return nil
}

// removeSSTFile 删除一个已经被判定为无用的 SST 文件。
func removeSSTFile(dir string, num uint64) {
	_ = os.Remove(sst.FilePath(dir, num))
}

// removeObsoleteLogsLocked 删除不再被任何 MemTable 需要的日志。
//
// 判据是 LevelDB 的那一条：编号小于 min(当前 MemTable 的日志编号,
// Immutable 的日志编号) 的日志，其内容一定已经落进 SST，可以安全删除。
//
// 删除失败**不算致命**：判据是单调的（编号只增），这次删不掉下次 Flush 还会再试，
// 多留一会儿的唯一代价是磁盘占用。而把它当成停库理由是灾难 —— 杀毒软件、
// 索引服务、备份程序都会短暂占用刚被读过的文件，Windows 上尤其常见，
// 因为这点"可能 transient 的删除失败"把整个引擎停下完全不成比例。
func (db *DB) removeObsoleteLogsLocked() error {
	minNum := db.mem.LogNumber()
	if db.imm != nil && db.imm.LogNumber() < minNum {
		minNum = db.imm.LogNumber()
	}
	logs, err := wal.ListLogs(db.opts.Dir)
	if err != nil {
		return err
	}
	for _, num := range logs {
		if num >= minNum {
			continue
		}
		if err := wal.RemoveLog(db.opts.Dir, num); err != nil {
			if !os.IsNotExist(err) {
				db.logWarnf("could not remove obsolete log %06d.log (will retry on the next flush): %v", num, err)
			}
			continue
		}
	}
	return nil
}
