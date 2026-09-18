package kvdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"kvdb/internal/cache"
	"kvdb/internal/key"
	"kvdb/internal/memdb"
	"kvdb/internal/sst"
	"kvdb/internal/wal"
)

// sstSuffix 是 SST 文件的扩展名。
const sstSuffix = ".sst"

// sstName 返回编号为 num 的 SST 文件名，与日志共用编号空间，
// 因此"编号大 = 更新"这一条规则对两种文件同时成立。
func sstName(num uint64) string {
	return fmt.Sprintf("%06d%s", num, sstSuffix)
}

// recover 完成打开数据库时的恢复：扫描目录、重放 WAL、把结果落成 SST。
//
// M1 没有 Manifest，文件列表直接由目录扫描得到，规则是：
//   - footer 完整的 .sst 是已提交的文件，加入读集合；
//   - footer 不全的 .sst 是崩溃时中断的一次 Flush，直接删除——
//     它的数据仍然完整地躺在 WAL 里，稍后会随重放重新落盘。
//
// 重放结束后必须把结果落成 SST 再删旧日志，否则会丢掉日志承载的数据。
// 这件事在 M3 会交给 Manifest / VersionSet 正式接管。
func (db *DB) recover() error {
	files, discarded, maxNum, err := scanSSTFiles(db.opts.Dir, db.cmp, db.blockCache)
	if err != nil {
		return err
	}
	db.files = files
	db.report.DiscardedFiles = discarded

	logs, err := wal.ListLogs(db.opts.Dir)
	if err != nil {
		return err
	}
	if n := len(logs); n > 0 && logs[n-1] > maxNum {
		maxNum = logs[n-1]
	}
	db.nextFileNum = maxNum + 1

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

	// 重放出来的内容先落盘：只有它变成 SST，旧日志才能安全删除。
	if !db.mem.Empty() {
		meta, err := db.flushMemTable(db.mem, db.allocFileNum())
		if err != nil {
			return err
		}
		if meta != nil {
			db.files = append(db.files, meta)
		}
	}

	// 换上新的空表与新的日志继续服务，编号一定大于目录里的所有文件。
	logNum := db.allocFileNum()
	newLog, err := wal.Create(db.opts.Dir, logNum)
	if err != nil {
		return err
	}
	db.log = newLog
	db.mem = memdb.New(db.cmp, logNum)

	// 此刻所有编号 < logNum 的日志都已经被上面的 Flush 或更早的 SST 覆盖。
	return db.removeObsoleteLogsLocked()
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
	if err := b.Range(seq, func(seq uint64, kind key.Kind, userKey, value []byte) bool {
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

// allocFileNum 分配一个新的文件编号。只在打开与冻结路径上调用。
func (db *DB) allocFileNum() uint64 {
	n := db.nextFileNum
	db.nextFileNum++
	return n
}

// scanSSTFiles 扫描目录里的 SST 文件。
//
// 返回按编号升序排列的文件列表、被丢弃的文件名，以及目录里的最大编号。
//
// 损坏文件的处置策略沿用 M1，但把"哪些错误可以丢"讲清楚了（见 isDiscardableSSTError）：
// M2 的文件多了索引与块校验，能暴露出的损坏形态比 M1 多，但**可丢弃的只有"没写完"
// 和"内容损坏"两类**——格式版本不匹配（M1 目录）必须报错，不能被当成损坏文件删掉。
func scanSSTFiles(dir string, cmp key.Comparer, c *cache.Cache) (files []*fileMeta, discarded []string, maxNum uint64, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("kvdb: read dir %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), sstSuffix) {
			continue
		}
		num, perr := strconv.ParseUint(strings.TrimSuffix(e.Name(), sstSuffix), 10, 64)
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
		files = append(files, &fileMeta{num: num, reader: r})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].num < files[j].num })
	return files, discarded, maxNum, nil
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
	defer close(db.flushDone)
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
			num := db.allocFileNum()
			db.mu.Unlock()

			meta, err := db.flushMemTable(imm, num)

			db.mu.Lock()
			switch {
			case err != nil:
				// 落盘失败：保留 Immutable 不放，让写者拿到同一个错误并停止写入。
				db.bgErr = fmt.Errorf("kvdb: flush memtable: %w", err)
			case meta != nil:
				db.files = append(db.files, meta)
			}
			if err == nil && db.imm == imm {
				db.imm = nil
			}
			if err == nil {
				if rerr := db.removeObsoleteLogsLocked(); rerr != nil {
					db.bgErr = rerr
				}
			}
			db.cond.Broadcast()
			db.mu.Unlock()

			if err != nil {
				return
			}
		}
	}
}

// flushMemTable 把一张 MemTable 写成 SST 并返回它的元信息。
//
// 它不修改任何共享状态，因此可以在不持锁的情况下慢慢跑；
// 文件"提交"的标志就是这里返回了非 nil 的 meta。
func (db *DB) flushMemTable(m *memdb.MemTable, num uint64) (*fileMeta, error) {
	if m.Empty() {
		return nil, nil
	}
	path := filepath.Join(db.opts.Dir, sstName(num))
	w, err := sst.NewWriter(path, db.cmp, sst.WriterOptions{
		BlockSize:       db.opts.BlockSize,
		BloomBitsPerKey: db.opts.BloomBitsPerKey,
	})
	if err != nil {
		return nil, err
	}
	it := m.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if err := w.Add(it.Key(), it.Value()); err != nil {
			w.Abandon()
			return nil, err
		}
	}
	if err := w.Finish(); err != nil {
		w.Abandon()
		return nil, err
	}
	r, err := sst.Open(path, sst.OpenOptions{Comparer: db.cmp, Cache: db.blockCache, FileNum: num})
	if err != nil {
		return nil, err
	}
	return &fileMeta{num: num, reader: r}, nil
}

// removeObsoleteLogsLocked 删除不再被任何 MemTable 需要的日志。
//
// 判据是 LevelDB 的那一条：编号小于 min(当前 MemTable 的日志编号,
// Immutable 的日志编号) 的日志，其内容一定已经落进 SST，可以安全删除。
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
		if num < minNum {
			if err := wal.RemoveLog(db.opts.Dir, num); err != nil {
				return err
			}
		}
	}
	return nil
}
