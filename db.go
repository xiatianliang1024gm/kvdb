package kvdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"kvdb/internal/cache"
	"kvdb/internal/key"
	"kvdb/internal/memdb"
	"kvdb/internal/sst"
	"kvdb/internal/wal"
)

var (
	// ErrNotFound 表示 key 不存在，或者已经被删除。
	ErrNotFound = errors.New("kvdb: key not found")
	// ErrClosed 表示数据库已经关闭。
	ErrClosed = errors.New("kvdb: database is closed")
	// ErrLocked 表示数据目录已被另一个进程（或本进程的另一个实例）打开。
	ErrLocked = errors.New("kvdb: database directory is locked by another process")
)

// lockFileName 是目录锁的文件名。锁在文件上而不在文件的存在性上，
// 所以这个文件平时一直躺在目录里是正常的。
const lockFileName = "LOCK"

// batchPool 复用 WriteBatch，避免 Put/Delete 每次都分配新对象。
var batchPool = sync.Pool{New: func() any { return NewWriteBatch() }}

// fileMeta 描述一个已落盘、可读取的 SST 文件。
//
// reader 常驻持有文件句柄，并在打开时把 Index / Filter 读进内存（各几 KB）。
// 这样"点查"就不需要任何额外的元数据 IO；数据块由 reader 内部走块缓存。
type fileMeta struct {
	num    uint64
	reader *sst.Reader
}

// DB 是一个嵌入式 KV 存储引擎实例。所有导出方法都可并发调用。
//
// 写路径：WriteBatch 编码成一条 WAL 记录 → fsync → 写入 MemTable。
// 读路径：MemTable → Immutable MemTable → SST 文件（从新到旧，每层内部走
// 索引二分 + Bloom 过滤 + 块缓存）。
// 两者的公共部分只有一把读写锁：读之间不互斥，写之间串行。
type DB struct {
	opts Options
	cmp  key.Comparer
	icmp key.InternalComparer

	lock *os.File // 目录锁，Close 时释放

	// blockCache 缓存解压后的数据块；opts 显式关闭时为 nil（nil 缓存是安全的空操作）。
	blockCache *cache.Cache

	mu    sync.RWMutex
	cond  *sync.Cond // 写者在此等待 Immutable 落盘
	mem   *memdb.MemTable
	imm   *memdb.MemTable // 只读，等待后台落盘；nil 表示没有
	files []*fileMeta     // 按 num 升序，num 越大越新
	log   *wal.Log

	nextFileNum uint64
	lastSeq     uint64
	bgErr       error
	closed      bool

	report RecoveryReport

	flushCh   chan struct{}
	closeCh   chan struct{}
	flushDone chan struct{}
}

// RecoveryReport 汇报打开数据库时发现的可容忍损坏。
//
// 崩溃时写了一半的 WAL 记录与 SST 文件都会被安全丢弃（它们的内容没有被确认过），
// 但用户有权知道发生了什么，所以这里如实记录。
type RecoveryReport struct {
	// TornLogs 列出尾部有损坏字节、被截断恢复的日志编号。
	TornLogs []uint64
	// DiscardedFiles 列出 footer 不完整、被丢弃的 SST 文件名（中断的 Flush）。
	DiscardedFiles []string
	// ReplayedRecords 是重放 WAL 得到的记录条数。
	ReplayedRecords int
}

// RecoveryReport 返回最近一次打开数据库时的恢复报告。
func (db *DB) RecoveryReport() RecoveryReport {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.report
}

// Stats 是引擎当前的规模快照，用于诊断与压测输出。
type Stats struct {
	// Files 是当前参与读取的 SST 文件数。
	Files int
	// MemTableSize 是当前可变 MemTable 的近似内存占用。
	MemTableSize int64
	// HasImmutable 表示是否有一张正在等待落盘的 Immutable MemTable。
	HasImmutable bool
	// LastSequence 是已提交的最大序列号。
	LastSequence uint64
	// ObsoleteLogs 是目录里尚未删除的日志文件数。
	ObsoleteLogs int
	// CacheHits / CacheMisses 是块缓存的累计命中与未命中次数；缓存关闭时恒为 0。
	//
	// 命中率是 M2 最直接的读路径指标：点查延迟能不能压住，几乎全看它。
	CacheHits   int64
	CacheMisses int64
	// CacheBytes 是块缓存当前占用（含条目开销），CacheItems 是条目数。
	CacheBytes int64
	CacheItems int64
}

// Stats 返回当前规模快照。
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	logs, _ := wal.ListLogs(db.opts.Dir)
	cs := db.blockCache.Stats()
	return Stats{
		Files:        len(db.files),
		MemTableSize: db.mem.ApproximateSize(),
		HasImmutable: db.imm != nil,
		LastSequence: db.lastSeq,
		ObsoleteLogs: len(logs),
		CacheHits:    cs.Hits,
		CacheMisses:  cs.Misses,
		CacheBytes:   cs.Bytes,
		CacheItems:   cs.Count,
	}
}

// Open 打开（必要时创建）opts.Dir 处的数据库。
//
// 同一目录同时只允许一个实例打开：目录锁由内核持有，进程崩溃后会自动释放，
// 因此被 kill -9 打断的进程不会导致数据目录无法再次打开。
func Open(opts Options) (*DB, error) {
	if err := opts.prepare(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("kvdb: create directory %s: %w", opts.Dir, err)
	}
	lock, err := acquireDirLock(opts.Dir)
	if err != nil {
		return nil, err
	}

	db := &DB{
		opts:      opts,
		cmp:       opts.Comparer,
		icmp:      opts.internalKeyComparer(),
		lock:      lock,
		flushCh:   make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
		flushDone: make(chan struct{}),
	}
	// BlockCacheSize > 0 才建缓存；显式关闭（归一化后为 0）时保持 nil，
	// 读路径会自动退化成"每读一块分配一次"。
	db.blockCache = cache.New(opts.BlockCacheSize)
	db.cond = sync.NewCond(&db.mu)

	if err := db.recover(); err != nil {
		db.releaseLock()
		return nil, err
	}
	go db.flushLoop()
	return db, nil
}

// Close 停止后台任务、把 WAL 落盘并释放所有资源。
//
// 它**不**把 MemTable 刷成 SST：内存里的数据本来就已经在 WAL 里了，
// 下次 Open 时会重放并落盘。这样 Close 又快又简单，而且顺带把恢复路径
// 变成每次都必然走一遍的常规路径。
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	close(db.closeCh)
	db.cond.Broadcast() // 唤醒可能正在等 Flush 的写者，让它拿 ErrClosed 退出
	db.mu.Unlock()

	<-db.flushDone // 等后台 Flush 彻底结束，之后才敢关文件

	db.mu.Lock()
	defer db.mu.Unlock()

	var errs []error
	if db.log != nil {
		if err := db.log.Sync(); err != nil {
			errs = append(errs, err)
		}
		if err := db.log.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, f := range db.files {
		if err := f.reader.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := db.releaseLock(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// releaseLock 释放目录锁。
func (db *DB) releaseLock() error {
	if db.lock == nil {
		return nil
	}
	unlockErr := unlockDirFile(db.lock)
	closeErr := db.lock.Close()
	db.lock = nil
	return errors.Join(unlockErr, closeErr)
}

// acquireDirLock 打开并锁定数据目录里的 LOCK 文件。
func acquireDirLock(dir string) (*os.File, error) {
	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("kvdb: open lock file %s: %w", path, err)
	}
	if err := lockDirFile(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("%w: %s", ErrLocked, err)
	}
	return f, nil
}

// Write 原子地写入一个批次，是引擎唯一的写入口。
//
// Put / Delete 都只是它的语法糖。原子性来自"整个批次编码成一条 WAL 记录"：
// 恢复时要么整条重放成功，要么整条被丢弃，不存在只应用一半的情况。
func (db *DB) Write(b *WriteBatch) error {
	if b == nil || b.Len() == 0 {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.bgErr != nil {
		// 后台 Flush 失败说明磁盘已经不可信，继续写只会让情况更糟。
		return db.bgErr
	}

	seq := db.lastSeq + 1
	b.SetSequence(seq)

	// 先写日志再动内存：fsync 成功才算这条写被确认。
	if err := db.log.Append(b.Encode()); err != nil {
		return err
	}
	if db.opts.SyncWrites {
		if err := db.log.Sync(); err != nil {
			return err
		}
	}
	if err := b.Range(seq, func(seq uint64, kind key.Kind, userKey, value []byte) bool {
		db.mem.Add(seq, kind, userKey, value)
		return true
	}); err != nil {
		return err
	}
	db.lastSeq = seq + uint64(b.Len()) - 1

	if db.mem.ApproximateSize() >= int64(db.opts.MemTableSize) {
		return db.freezeLocked()
	}
	return nil
}

// Put 写入一个键值对。value 允许为空。
func (db *DB) Put(userKey, value []byte) error {
	b := batchPool.Get().(*WriteBatch)
	defer putBatch(b)
	if err := b.Put(userKey, value); err != nil {
		return err
	}
	return db.Write(b)
}

// Delete 删除一个键。它写入墓碑，物理删除留给后续 Compaction。
func (db *DB) Delete(userKey []byte) error {
	b := batchPool.Get().(*WriteBatch)
	defer putBatch(b)
	if err := b.Delete(userKey); err != nil {
		return err
	}
	return db.Write(b)
}

func putBatch(b *WriteBatch) {
	b.Reset()
	batchPool.Put(b)
}

// Get 读取 userKey 的最新可见版本，key 不存在或已被删除时返回 ErrNotFound。
//
// 返回的切片是一份独立的数据：内部实现可能返回指向块缓存或跳表节点的切片，
// 在包的边界上统一复制一次，避免调用方无意中改到共享内存。
func (db *DB) Get(userKey []byte) ([]byte, error) {
	if len(userKey) == 0 {
		return nil, ErrEmptyKey
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	// 读到的快照就是"当前已提交的最后一个序列号"，需要固定视图请用 GetSnapshot。
	v, err := db.getLocked(db.lastSeq, userKey)
	if err != nil {
		return nil, err
	}
	return copyValue(v), nil
}

// getLocked 在指定快照下按"从新到旧"的顺序查找。
//
// 顺序不能乱：MemTable 里的数据一定比 Immutable 新，Immutable 一定比任何
// SST 新；SST 之间按文件编号从大到小。第一个命中的层次（哪怕是墓碑）就是答案。
//
// 返回的切片由内部缓冲区持有，调用方不得修改。
func (db *DB) getLocked(snapshot uint64, userKey []byte) ([]byte, error) {
	if v, kind, found := db.mem.Get(snapshot, userKey); found {
		return valueOf(v, kind)
	}
	if db.imm != nil {
		if v, kind, found := db.imm.Get(snapshot, userKey); found {
			return valueOf(v, kind)
		}
	}
	for i := len(db.files) - 1; i >= 0; i-- {
		v, kind, found, err := db.files[i].reader.Get(snapshot, userKey)
		if err != nil {
			return nil, err
		}
		if found {
			return valueOf(v, kind)
		}
	}
	return nil, ErrNotFound
}

// valueOf 把"命中的记录"翻译成对外语义：墓碑与不存在都表现为 ErrNotFound。
func valueOf(value []byte, kind key.Kind) ([]byte, error) {
	if kind == key.TypeDeletion {
		return nil, ErrNotFound
	}
	return value, nil
}

// freezeLocked 把 MemTable 冻结成 Immutable，换上新的空表与新的 WAL。
//
// 调用时必须持有写锁。同一时刻只允许存在一张 Immutable：如果上一张还在后台
// 落盘，写者要在这里等它完成。这是 Immutable 层的代价，换来的是
// "Flush 期间前台写入只被短暂阻塞"，而不是"全程停写"。
func (db *DB) freezeLocked() error {
	for db.imm != nil && db.bgErr == nil && !db.closed {
		db.cond.Wait()
	}
	if db.bgErr != nil {
		return db.bgErr
	}
	if db.closed {
		return ErrClosed
	}

	logNum := db.nextFileNum
	db.nextFileNum++
	newLog, err := wal.Create(db.opts.Dir, logNum)
	if err != nil {
		return err
	}

	old := db.log
	db.log = newLog
	db.imm = db.mem
	db.mem = memdb.New(db.cmp, logNum)

	// 旧日志句柄不再写入，但它承载着 Immutable 的数据，只能等落盘后再删文件。
	if old != nil {
		_ = old.Close()
	}
	db.notifyFlush()
	return nil
}

// notifyFlush 非阻塞地唤醒后台 Flush。
func (db *DB) notifyFlush() {
	select {
	case db.flushCh <- struct{}{}:
	default:
	}
}
