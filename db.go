package kvdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"kvdb/internal/cache"
	"kvdb/internal/key"
	"kvdb/internal/memdb"
	"kvdb/internal/sst"
	"kvdb/internal/version"
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

// DB 是一个嵌入式 KV 存储引擎实例。所有导出方法都可并发调用。
//
// 写路径：WriteBatch 编码成一条 WAL 记录 → fsync → 写入 MemTable。
// 读路径：MemTable → Immutable MemTable → 当前版本里的 SST
// （L0 从新到旧逐个试，L1 以下每层二分定位一次；每层内部走索引二分 + Bloom + 块缓存）。
//
// 并发模型（M3 之后）：
//
//	写者                持 db.mu 写锁，串行
//	读者                持 db.mu 读锁，互不阻塞
//	后台 Flush 协程      只在"取任务"和"提交"时短暂持锁，落盘全程不持锁
//	后台 Compaction 协程 同上；它额外持有一个 Version 引用，保证输入文件在归并期间不被删除
//
// 真正的版本状态（哪些文件在第几层）由 Version + VersionSet 承载，它不可变、
// 带引用计数，因此"读一个一致的文件集合"不需要在读路径上加任何锁。
type DB struct {
	opts Options
	cmp  key.Comparer
	icmp key.InternalComparer

	lock *os.File // 目录锁，Close 时释放

	// blockCache 缓存解压后的数据块；opts 显式关闭时为 nil（nil 缓存是安全的空操作）。
	blockCache *cache.Cache

	// vset 是版本元数据的唯一所有者；v 是当前版本，DB 自己持有它的一个引用。
	//
	// 为什么 DB 要自己持引用而不是每次都从 vset 取：读路径（Get / NewIterator）
	// 已经在 db.mu 读锁里了，再进 vset.mu 只是为了拿一个指针并不划算，
	// 而"DB 持有的这一个引用"恰好保证了当前版本在任何时刻都不会被回收。
	vset *version.VersionSet
	v    *version.Version

	// readers 是已打开的 SST 读取器（文件编号 → 句柄）。
	//
	// 一个编号只开一次：Reader 在打开时就把 Index 与 Filter 读进内存，
	// 反复打开等于反复做同样的 IO。表的增删只发生在提交/回收这两个点上。
	readers map[uint64]*sst.Reader

	mu   sync.RWMutex
	cond *sync.Cond // 写者在此等待 Immutable 落盘
	mem  *memdb.MemTable
	imm  *memdb.MemTable // 只读，等待后台落盘；nil 表示没有
	log  *wal.Log

	lastSeq uint64
	bgErr   error
	closed  bool

	// snapshots 记录存活快照的序列号（值是同一个序列号的快照个数）。
	//
	// Compaction 需要它来决定"旧版本能丢到哪个序列号为止"：丢掉任何一个存活快照
	// 还需要读的版本都是静默的数据损坏。记录 Release 让这件事可判定 ——
	// 代价是"忘记 Release"会让 Compaction 变保守（旧版本清不掉），而不是出错。
	snapshots map[uint64]int

	report RecoveryReport

	// counters 是全引擎的累计指标。用原子量而不是锁保护：读路径上的每一个计数器
	// 都值得避免再加一次锁竞争，而 Stats 读到的值本来就是近似快照。
	counters counters

	flushCh   chan struct{}
	compactCh chan struct{}
	closeCh   chan struct{}
	bgWG      sync.WaitGroup
}

// counters 汇总累计指标。
type counters struct {
	walBytes   atomic.Uint64 // 写进 WAL 的负载字节数
	flushFiles atomic.Int64
	flushBytes atomic.Uint64

	gets       atomic.Int64 // 点查次数
	readProbes atomic.Int64 // 点查时"向某个 SST 发起查找"的次数，读放大 = 它 / gets

	compactions           atomic.Int64
	compactionInputFiles  atomic.Int64
	compactionOutputFiles atomic.Int64
	compactionInputBytes  atomic.Uint64
	compactionOutputBytes atomic.Uint64
	compactionDropped     atomic.Int64
}

// RecoveryReport 汇报打开数据库时发现的可容忍损坏。
//
// 崩溃时写了一半的 WAL 记录与 SST 文件都会被安全丢弃（它们的内容没有被确认过），
// 但用户有权知道发生了什么，所以这里如实记录。
type RecoveryReport struct {
	// TornLogs 列出尾部有损坏字节、被截断恢复的日志编号。
	TornLogs []uint64
	// DiscardedFiles 列出被丢弃的、**残缺的** SST 文件名：footer 不全或块校验失败，
	// 是崩溃时被中断的一次写入。两条路径都会产生它们：
	// 迁移旧目录（目录里没有 Manifest，靠扫描重建文件列表）时扫描发现，
	// 或者版本已确定之后发现一个"不在版本里、又打不开"的孤儿文件。
	DiscardedFiles []string
	// ObsoleteFiles 列出没有任何版本引用的 SST 文件名，且它们本身是完整的。
	//
	// 典型来源是"文件已经写完并 fsync，但 Manifest 里的那次提交还没落盘就崩溃了"。
	// 这类文件的内容没有被确认过（Flush 的数据还在 WAL 里，Compaction 的输入还在版本里），
	// 安全删除。它与 DiscardedFiles 的区别是：那些文件是**根本不该存在**的残片，
	// 这些文件是**曾经想提交但没提交成功**的完整文件。
	ObsoleteFiles []string
	// TruncatedManifest 表示 Manifest 尾部有一截没写完的追加被丢弃。
	TruncatedManifest bool
	// RecoveredFromScan 表示目录里没有 Manifest，文件列表来自目录扫描
	// （M2 及更早的目录第一次被打开时的迁移路径）。
	RecoveredFromScan bool
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
	// Files 是当前参与读取的 SST 文件数（所有层之和）。
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

	// Levels 是各层的文件数与字节数，索引即层号。
	//
	// 它是 M3 最直观的验收指标：L0 的文件数应当稳定在 L0CompactionTrigger 附近
	// 而不是一直涨，L1 以下的字节数应当按 LevelSizeMultiplier 阶梯式增长。
	Levels []LevelStats
	// Compaction 是 Compaction 的累计规模；FlushBytes 是 Flush 写出的字节数；
	// WALBytes 是写进预写日志的负载字节数。
	Compaction CompactionStats
	FlushBytes uint64
	WALBytes   uint64
	// Gets 是点查次数，ReadProbes 是点查向 SST 发起查找的累计次数。
	//
	// 读放大 = ReadProbes / Gets，含义是"平均一次点查要碰几个文件"。
	// 它是 Bloom Filter 挡不掉的成本 —— Bloom 只让"碰"变得更便宜，
	// 真正让次数下降的是 Compaction 把 L0 收敛成 L1 以下的分层结构。
	Gets       int64
	ReadProbes int64
}

// LevelStats 是单层的规模。
type LevelStats struct {
	Files int
	Bytes uint64
}

// CompactionStats 汇总 Compaction 的累计规模。
type CompactionStats struct {
	// Count 是执行次数，InputFiles / OutputFiles 是文件数的累计。
	Count       int64
	InputFiles  int64
	OutputFiles int64
	// InputBytes / OutputBytes 是字节数的累计，写放大的分子就来自它们。
	InputBytes  uint64
	OutputBytes uint64
	// DroppedRecords 是被丢弃的记录数（被覆盖的旧版本 + 可以退休的墓碑）。
	DroppedRecords int64
}

// Stats 返回当前规模快照。
func (db *DB) Stats() Stats {
	db.mu.RLock()
	v := db.v
	levels := make([]LevelStats, 0, v.NumLevels())
	for level := 0; level < v.NumLevels(); level++ {
		levels = append(levels, LevelStats{Files: len(v.Files(level)), Bytes: v.LevelBytes(level)})
	}
	memSize := int64(0)
	if db.mem != nil {
		memSize = db.mem.ApproximateSize()
	}
	s := Stats{
		Files:        v.FileCount(),
		MemTableSize: memSize,
		HasImmutable: db.imm != nil,
		LastSequence: db.lastSeq,
		Levels:       levels,
		Compaction: CompactionStats{
			Count:          db.counters.compactions.Load(),
			InputFiles:     db.counters.compactionInputFiles.Load(),
			OutputFiles:    db.counters.compactionOutputFiles.Load(),
			InputBytes:     db.counters.compactionInputBytes.Load(),
			OutputBytes:    db.counters.compactionOutputBytes.Load(),
			DroppedRecords: db.counters.compactionDropped.Load(),
		},
		FlushBytes: db.counters.flushBytes.Load(),
		WALBytes:   db.counters.walBytes.Load(),
		Gets:       db.counters.gets.Load(),
		ReadProbes: db.counters.readProbes.Load(),
	}
	db.mu.RUnlock()

	// 目录扫描是真正的 IO，放在锁外做。
	if logs, err := wal.ListLogs(db.opts.Dir); err == nil {
		s.ObsoleteLogs = len(logs)
	}
	cs := db.blockCache.Stats()
	s.CacheHits, s.CacheMisses = cs.Hits, cs.Misses
	s.CacheBytes, s.CacheItems = cs.Bytes, cs.Count
	return s
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
		vset:      version.New(version.Config{Dir: opts.Dir, Comparer: opts.Comparer, MaxLevels: opts.MaxLevels}),
		readers:   make(map[uint64]*sst.Reader),
		snapshots: make(map[uint64]int),
		flushCh:   make(chan struct{}, 1),
		compactCh: make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
	}
	// BlockCacheSize > 0 才建缓存；显式关闭（归一化后为 0）时保持 nil，
	// 读路径会自动退化成"每读一块分配一次"。
	db.blockCache = cache.New(opts.BlockCacheSize)
	db.cond = sync.NewCond(&db.mu)
	db.v = db.vset.Current()

	if err := db.recover(); err != nil {
		db.releaseLock()
		return nil, err
	}

	db.bgWG.Add(2)
	go db.flushLoop()
	go db.compactLoop()
	// 恢复出来的版本本身可能就需要整理（例如刚从 M2 目录迁移过来，
	// 几十个文件全堆在 L0），所以开局先提醒一次后台。
	db.notifyCompact()
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

	db.bgWG.Wait() // 等后台两个协程彻底结束，之后才敢关文件

	// 后台停下之后再清一次：关库那一刻正在收尾的 Compaction 可能刚提交完新版本，
	// 还没来得及回收被它消耗掉的输入文件。这一步补上，目录里才不会留下孤儿。
	db.collectGarbageFinal()

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
	for num, r := range db.readers {
		if err := r.Close(); err != nil {
			errs = append(errs, err)
		}
		delete(db.readers, num)
	}
	if db.v != nil {
		db.v.Unref()
		db.v = nil
	}
	if err := db.vset.Close(); err != nil {
		errs = append(errs, err)
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

	record := b.Encode()
	// 先写日志再动内存：fsync 成功才算这条写被确认。
	if err := db.log.Append(record); err != nil {
		return err
	}
	if db.opts.SyncWrites {
		if err := db.log.Sync(); err != nil {
			return err
		}
	}
	db.counters.walBytes.Add(uint64(len(record)))
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
	db.counters.gets.Add(1)
	v, err := db.getLocked(db.lastSeq, userKey)
	if err != nil {
		return nil, err
	}
	return copyValue(v), nil
}

// getLocked 在指定快照下按"从新到旧"的顺序查找。
//
// 顺序不能乱：MemTable 里的数据一定比 Immutable 新，Immutable 一定比任何
// SST 新；SST 之间先看 L0（从新到旧），再看 L1 以下（每层最多一个候选）。
// 第一个命中的层次（哪怕是墓碑）就是答案。
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

	v := db.v
	// L0 的文件区间互相重叠，只能从新到旧逐个试。
	l0 := v.Files(0)
	for i := len(l0) - 1; i >= 0; i-- {
		val, kind, found, err := db.probeFile(l0[i].Num, snapshot, userKey)
		if err != nil {
			return nil, err
		}
		if found {
			return valueOf(val, kind)
		}
	}
	// L1 以下同层不重叠，每层二分一次就能确定"是不是这个文件"。
	// 这一次二分就是 M3 带来的读放大下降：L0 少一个文件，这里就少一次探测。
	if v.NumLevels() > 1 {
		target := key.SeekKey(userKey, snapshot)
		for level := 1; level < v.NumLevels(); level++ {
			f := v.FindFile(level, userKey, target)
			if f == nil {
				continue
			}
			val, kind, found, err := db.probeFile(f.Num, snapshot, userKey)
			if err != nil {
				return nil, err
			}
			if found {
				return valueOf(val, kind)
			}
		}
	}
	return nil, ErrNotFound
}

// probeFile 在编号为 num 的 SST 里查一次，并记录读放大。
func (db *DB) probeFile(num, snapshot uint64, userKey []byte) (value []byte, kind key.Kind, found bool, err error) {
	r, err := db.readerFor(num)
	if err != nil {
		return nil, 0, false, err
	}
	db.counters.readProbes.Add(1)
	return r.Get(snapshot, userKey)
}

// readerFor 返回编号对应的读取器。调用方必须持有 db.mu（读或写）。
//
// 取不到只有一个原因：某个版本引用了一个没有登记过的文件编号。这属于程序缺陷，
// 所以报错而不是返回"没找到"—— 后者会伪装成数据丢失，查起来代价极大。
func (db *DB) readerFor(num uint64) (*sst.Reader, error) {
	if r := db.readers[num]; r != nil {
		return r, nil
	}
	return nil, fmt.Errorf("kvdb: internal error: no open reader for sst file %d", num)
}

// openReader 打开一个 SST 的读取器。它不碰任何共享状态，因此可以在锁外调用。
func (db *DB) openReader(num uint64) (*sst.Reader, error) {
	return sst.Open(sst.FilePath(db.opts.Dir, num), sst.OpenOptions{
		Comparer: db.cmp,
		Cache:    db.blockCache,
		FileNum:  num,
	})
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

	logNum := db.vset.AllocFileNum()
	newLog, err := wal.Create(db.opts.Dir, logNum)
	if err != nil {
		return err
	}
	db.vset.SetLogNumber(logNum)

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

// notifyCompact 非阻塞地唤醒后台 Compaction。
func (db *DB) notifyCompact() {
	select {
	case db.compactCh <- struct{}{}:
	default:
	}
}

// setCurrentVersion 把 DB 手里的当前版本引用换成 vset 的最新版本。
// 调用方必须持有 db.mu 的写锁。
func (db *DB) setCurrentVersionLocked() {
	nv := db.vset.Current()
	if db.v != nil {
		db.v.Unref()
	}
	db.v = nv
}

// logAndApplyLocked 提交一次版本变更并把 DB 的当前版本引用换过去。
// 调用方必须持有 db.mu 的写锁。
//
// 锁序：db.mu → VersionSet.mu。VersionSet 从不回调 DB，所以这个方向是安全的。
func (db *DB) logAndApplyLocked(edit *version.VersionEdit) error {
	if edit.NextFileNum == 0 {
		edit.NextFileNum = db.vset.NextFileNum()
	}
	if err := db.vset.LogAndApply(edit); err != nil {
		return err
	}
	db.setCurrentVersionLocked()
	return nil
}

// baseEdit 构造一次版本变更的公共部分：文件编号水位与序列号水位。
//
// 把 NextFileNum 写进每一条记录不是可有可无的：编号水位只在 Manifest 里，
// 重启后如果不比目录里出现过的编号更大，新分配的文件号就可能与"上次崩溃时
// 已经创建但没提交"的文件重号 —— 而 (文件编号, 块偏移) 正是块缓存的键。
func (db *DB) baseEdit() *version.VersionEdit {
	edit := &version.VersionEdit{
		NextFileNum: db.vset.NextFileNum(),
		LastSeq:     db.lastSeq,
	}
	if db.mem != nil {
		edit.LogNumber = db.mem.LogNumber()
	}
	return edit
}
