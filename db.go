package kvdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/xiatianliang1024gm/kvdb/internal/cache"
	"github.com/xiatianliang1024gm/kvdb/internal/key"
	"github.com/xiatianliang1024gm/kvdb/internal/memdb"
	"github.com/xiatianliang1024gm/kvdb/internal/rate"
	"github.com/xiatianliang1024gm/kvdb/internal/sst"
	"github.com/xiatianliang1024gm/kvdb/internal/version"
	"github.com/xiatianliang1024gm/kvdb/internal/wal"
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
// 写路径（M4 起是组提交）：并发写者排进一条写队列 → 队首那个写者替全组做一次
// WAL 追加 + 一次 fsync → 整组一起落进 MemTable。细节见 db_write.go。
// 读路径：MemTable → Immutable MemTable → 当前版本里的 SST
// （L0 从新到旧逐个试，L1 以下每层二分定位一次；每层内部走索引二分 + Bloom + 块缓存）。
//
// 并发模型（M4 之后）：
//
//	写者                在写队列里排队，队长串行提交；两个临界区各持一次 db.mu
//	读者                持 db.mu 读锁，互不阻塞；**不被 fsync 阻塞**
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

	// ── 写队列（Group Commit，M4）────────────────────────────────
	//
	// wmu 与 db.mu 是分工关系，不是同一个东西：db.mu 保护全局状态
	// （MemTable、版本、序列号水位），wmu 只保护写队列本身。
	//
	// 分开的理由是 fsync：如果写者拿 db.mu 去做 fsync，读者会被一次磁盘
	// 等待堵住。分开之后队长做 fsync 时既不持 db.mu（读者照常读），也只是
	// 让后来的写者排在队尾而不是空等 —— 队列因此自动攒成一组，
	// "多少个写者合并进一次 fsync"完全由并发度决定。
	wmu      sync.Mutex
	wcond    *sync.Cond // 跟随者在此等待队长发结论
	wqueue   []*writeRequest
	wleader  bool // 已有队长在提交
	wclosing bool // Close 已经开始，不再接受新写者

	lastSeq uint64
	bgErr   error
	closed  bool

	// snapshots 记录存活快照的序列号（值是同一个序列号的快照个数）。
	//
	// Compaction 需要它来决定"旧版本能丢到哪个序列号为止"：丢掉任何一个存活快照
	// 还需要读的版本都是静默的数据损坏。记录 Release 让这件事可判定 ——
	// 代价是"忘记 Release"会让 Compaction 变保守（旧版本清不掉），而不是出错。
	snapshots map[uint64]int

	// replayRanges 暂存 WAL 重放期间收集到的范围墓碑（M7）。
	// 只在 recover() 内部使用：重放发生在 NewManifest 之后，墓碑要等
	// Manifest 可追加后才能提交到 Version，见 db_flush.go 的 applyBatch。
	replayRanges []key.RangeDeletion

	report RecoveryReport

	// blockStats 累计块压缩在读写两侧的规模，Flush / Compaction 的 Writer 与
	// 全部 Reader 共享同一份（内部是原子量，无需加锁）。
	blockStats *sst.BlockStats
	// rateLimiter 限制后台 Compaction 的读写带宽；不限流时为 nil（nil 是合法的空操作）。
	rateLimiter *rate.Limiter
	// eventLog 接收运维事件。Open 一定把它填好：要么是用户给的 Logger，
	// 要么是数据目录下的文件日志，要么是丢弃型的空实现（禁用文件日志时）。
	eventLog Logger
	// closeEventLog 关闭事件日志（用户提供的 Logger 不会被关）。
	closeEventLog func() error

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

	// 组提交的规模：写批次被合并成多少个提交组、以及实际调用了多少次 fsync。
	// 合并率 = writeBatches / writeGroups，它就是并发写吞吐相对"每条写一次 fsync"
	// 的倍数；walSyncs 是这件事最直接的证据（它 ≪ writeBatches）。
	writeGroups   atomic.Int64
	writeBatches  atomic.Int64
	maxWriteGroup atomic.Int64
	walSyncs      atomic.Int64

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

	// ── M4：组提交规模 ──────────────────────────────────────────────
	//
	// WriteGroups 是提交组数，WriteBatches 是参与组提交的批次数（≈ Write 调用次数），
	// MaxWriteGroup 是观察到的最大组大小，WALFsyncs 是实际执行的 WAL fsync 次数。
	//
	// 这四个数把组提交的效果变得可量化：
	//
	//	合并率   = WriteBatches / WriteGroups   "平均多少个写者共用一次 fsync"
	//	fsync 数 = WALFsyncs                    SyncWrites 为假时恒为 0
	//
	// SyncWrites 为真时 WALFsyncs 是吞吐的硬上限来源：它比 WriteBatches 小多少倍，
	// 吞吐就有多少倍的余量。
	WriteGroups   int64
	WriteBatches  int64
	MaxWriteGroup int64
	WALFsyncs     int64

	// LiveSnapshots 是尚未 Release 的快照数量。
	//
	// 它同时是"旧版本能不能被丢弃"的依据：只要有一个存活快照，Compaction 就必须
	// 保留它还需要读的那些版本。这个数长期不归零，说明有快照忘了 Release ——
	// 不会读错数据，但旧版本会一直留在磁盘上。
	LiveSnapshots int

	// RangeTombstones 是当前版本上还挂着的范围墓碑条数（M7）。
	//
	// 一条范围墓碑在"它遮蔽的区间内不再有任何数据"时由 Compaction 自动退休，
	// 这个数随之归零。长期不为零通常意味着区间内还有更深层的旧数据没被
	// Compaction 触及，而不是出了问题。
	RangeTombstones int

	// ── M5：块压缩与限流 ────────────────────────────────────────────
	//
	// Compression 把"写出去多少、省下来多少"变成可验证的数字：
	// 压缩比 = RawBytes / StoredBytes，没有压缩时两项相等。
	Compression CompressionStats
	// RateLimit 是后台 Compaction 限流器的累计状态；不限流时 Waits 恒为 0。
	RateLimit RateLimitStats
}

// CompressionStats 汇总块压缩在读写两侧的累计规模。
type CompressionStats struct {
	// BlocksWritten 是写出的块总数，CompressedBlocks 是其中真正压缩落盘的数量。
	// 两者的差值是"压了不划算"的块（过滤器位图、小索引块、高熵数据）。
	BlocksWritten    int64
	CompressedBlocks int64
	// RawBytes 是写出块的原始字节总数，StoredBytes 是实际落盘字节总数。
	// 压缩比 = RawBytes / StoredBytes（StoredBytes 为 0 时未写过块）。
	RawBytes    uint64
	StoredBytes uint64
	// Decompressions 是读取时实际解压的次数；CompressedBytesRead 是解压前读入的字节数。
	// 它与块缓存命中率是一对：命中率越高，这个数越低。
	Decompressions      int64
	CompressedBytesRead uint64
}

// RateLimitStats 是限流器的累计状态。
type RateLimitStats struct {
	// Bytes 是累计放行的字节数；Waits 是实际阻塞的次数，WaitNanos 是累计阻塞时长。
	// Waits 为 0 说明 Compaction 的带宽从未打满配额。
	Bytes     uint64
	Waits     int64
	WaitNanos int64
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
		Files:           v.FileCount(),
		MemTableSize:    memSize,
		HasImmutable:    db.imm != nil,
		LastSequence:    db.lastSeq,
		LiveSnapshots:   len(db.snapshots),
		RangeTombstones: v.RangeDeletionCount(),
		Levels:          levels,
		Compaction: CompactionStats{
			Count:          db.counters.compactions.Load(),
			InputFiles:     db.counters.compactionInputFiles.Load(),
			OutputFiles:    db.counters.compactionOutputFiles.Load(),
			InputBytes:     db.counters.compactionInputBytes.Load(),
			OutputBytes:    db.counters.compactionOutputBytes.Load(),
			DroppedRecords: db.counters.compactionDropped.Load(),
		},
		FlushBytes:    db.counters.flushBytes.Load(),
		WALBytes:      db.counters.walBytes.Load(),
		Gets:          db.counters.gets.Load(),
		ReadProbes:    db.counters.readProbes.Load(),
		WriteGroups:   db.counters.writeGroups.Load(),
		WriteBatches:  db.counters.writeBatches.Load(),
		MaxWriteGroup: db.counters.maxWriteGroup.Load(),
		WALFsyncs:     db.counters.walSyncs.Load(),
	}
	db.mu.RUnlock()

	// 目录扫描是真正的 IO，放在锁外做。
	if logs, err := wal.ListLogs(db.opts.Dir); err == nil {
		s.ObsoleteLogs = len(logs)
	}
	cs := db.blockCache.Stats()
	s.CacheHits, s.CacheMisses = cs.Hits, cs.Misses
	s.CacheBytes, s.CacheItems = cs.Bytes, cs.Count

	if snap := db.blockStats.Snapshot(); snap.Blocks > 0 || snap.Decompressions > 0 {
		s.Compression = CompressionStats{
			BlocksWritten:       snap.Blocks,
			CompressedBlocks:    snap.Compressed,
			RawBytes:            snap.RawBytes,
			StoredBytes:         snap.StoredBytes,
			Decompressions:      snap.Decompressions,
			CompressedBytesRead: snap.CompressedRead,
		}
	}
	if st := db.rateLimiter.Stats(); st.Bytes > 0 || st.Waits > 0 {
		s.RateLimit = RateLimitStats{Bytes: st.Bytes, Waits: st.Waits, WaitNanos: st.WaitNanos}
	}
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
		opts:        opts,
		cmp:         opts.Comparer,
		icmp:        opts.internalKeyComparer(),
		lock:        lock,
		vset: version.New(version.Config{
			Dir:        opts.Dir,
			Comparer:   opts.Comparer,
			MaxLevels:  opts.MaxLevels,
			FilterName: compactionFilterName(opts.CompactionFilter),
			MergeName:  mergeOperatorName(opts.MergeOperator),
		}),
		readers:     make(map[uint64]*sst.Reader),
		snapshots:   make(map[uint64]int),
		blockStats:  &sst.BlockStats{},
		rateLimiter: rate.New(opts.CompactionRateLimit),
		flushCh:     make(chan struct{}, 1),
		compactCh:   make(chan struct{}, 1),
		closeCh:     make(chan struct{}),
	}
	eventLog, closeEventLog, logErr := setupEventLog(&opts)
	db.eventLog = eventLog
	db.closeEventLog = closeEventLog
	// BlockCacheSize > 0 才建缓存；显式关闭（归一化后为 0）时保持 nil，
	// 读路径会自动退化成"每读一块分配一次"。
	db.blockCache = cache.New(opts.BlockCacheSize)
	db.cond = sync.NewCond(&db.mu)
	db.wcond = sync.NewCond(&db.wmu)
	db.v = db.vset.Current()

	if err := db.recover(); err != nil {
		db.releaseLock()
		closeEventLog()
		return nil, err
	}

	// 事件日志在这里才用得上：恢复过程中发生的丢弃/截断马上就要写进 LOG。
	if logErr != nil {
		db.logWarnf("falling back to no-op event log: %v", logErr)
	}
	db.logInfof("open db: dir=%s compression=%s rate_limit=%d memtable=%dMB block_cache=%dMB bloom=%db/key sync_writes=%v",
		opts.Dir, opts.Compression, opts.CompactionRateLimit, opts.MemTableSize>>20,
		opts.BlockCacheSize>>20, opts.BloomBitsPerKey, opts.SyncWrites)
	if db.report.TruncatedManifest {
		db.logWarnf("manifest tail was truncated during recovery")
	}
	for _, name := range db.report.DiscardedFiles {
		db.logWarnf("discarded corrupt file %s (its data is still in the WAL)", name)
	}
	for _, name := range db.report.ObsoleteFiles {
		db.logInfof("removed unreferenced complete file %s", name)
	}
	if db.report.RecoveredFromScan {
		db.logWarnf("no manifest found: file list rebuilt from directory scan")
	}
	if db.rateLimiter != nil {
		db.logInfof("compaction rate limit enabled: %d bytes/sec", opts.CompactionRateLimit)
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
//
// 顺序上有一步是 M4 新增且必须放在最前面的：**先把写队列排空**。
// 提交队长在后半段要拿 db.mu，而 Close 一旦先拿住 db.mu 再等队长，两边就是互相等待。
func (db *DB) Close() error {
	// 等正在提交的那一组做完，并从此拒绝新的写者。做完之后队列一定是空的。
	db.drainWrites()

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

	// 限流器在这一刻已经没有任务在等了（后台协程都已退出），Close 只是防御性的。
	db.rateLimiter.Close()

	// 汇总一次关库前的规模。放在这里：最后一段清理不改变这些累计量，
	// 而 logInfof 只碰 eventLog 字段，不需要 db.mu。
	db.mu.RLock()
	lastSeq, fileCount := db.lastSeq, db.v.FileCount()
	db.mu.RUnlock()
	snap := db.blockStats.Snapshot()
	db.logInfof("close db: files=%d last_seq=%d blocks=%d compressed=%d raw=%d stored=%d decompressions=%d",
		fileCount, lastSeq, snap.Blocks, snap.Compressed, snap.RawBytes, snap.StoredBytes, snap.Decompressions)

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
	// 用户提供的 Logger 不归我们关；closeEventLog 对那种情形是空操作。
	// 置成丢弃实现是为了防"Close 之后还有 goroutine 迟到地调日志"。
	db.eventLog = nopLogger{}
	if err := db.closeEventLog(); err != nil {
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

// DeleteRange 删除半开区间 [start, end) 内的所有键（M7）。
//
// 一条范围墓碑覆盖任意大的区间，成本与区间内的键数无关——SQL 的
// DROP TABLE / TRUNCATE 和 Redis 的 DEL 大 key 都建立在这上面。
//
// 语义与单键 Delete 的差别只在物理回收的时机：可见性立即生效（返回后
// 范围内的键读不到、迭代器扫不到，之前的快照不受影响），但空间回收要等
// 后续 Compaction 把被遮蔽的记录真正重写掉；Compaction 还会在"区间内
// 再无任何数据"时自动退休这条墓碑（Stats.RangeTombstones 归零）。
// end <= start 返回 ErrInvalidRange。
func (db *DB) DeleteRange(start, end []byte) error {
	b := batchPool.Get().(*WriteBatch)
	defer putBatch(b)
	if err := b.DeleteRange(start, end); err != nil {
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

// getLocked 在指定快照下读取 userKey 的可见值。
//
// 先定位最新可见版本（seekVisibleLocked，快速路径不付任何额外代价），
// 命中 TypeMerge 时再向下收集全部 operand 并折叠（M8）。
func (db *DB) getLocked(snapshot uint64, userKey []byte) ([]byte, error) {
	val, kind, seq, err := db.seekVisibleLocked(snapshot, userKey)
	if err != nil {
		return nil, err
	}
	// 命中之后还有最后一道判据（M7）：**范围墓碑遮蔽**。命中的版本若落在某条
	// 范围删除的区间内、且比它旧（seq < T.Seq <= snapshot），整个 key 对这个
	// 快照不可见——同 key 更旧的版本必然也被遮蔽，不用继续下探，直接 NotFound。
	if db.v.RangeCovers(userKey, seq, snapshot) {
		return nil, ErrNotFound
	}
	if kind == key.TypeMerge {
		return db.foldMergeLocked(snapshot, userKey, seq, val)
	}
	return valueOf(val, kind)
}

// seekVisibleLocked 在指定快照下按"从新到旧"的顺序查找最新可见版本。
//
// 顺序不能乱：MemTable 里的数据一定比 Immutable 新，Immutable 一定比任何
// SST 新；SST 之间先看 L0（从新到旧），再看 L1 以下（每层最多一个候选）。
// 第一个命中的层次（哪怕是墓碑或 merge 记录）就是答案。
//
// 它不做范围遮蔽判定、不处理 Merge 折叠——那是 getLocked 的事。返回的
// 切片由内部缓冲区持有，调用方不得修改。
func (db *DB) seekVisibleLocked(snapshot uint64, userKey []byte) (value []byte, kind key.Kind, seq uint64, err error) {
	if val, kind, seq, found := db.mem.Get(snapshot, userKey); found {
		return val, kind, seq, nil
	}
	if db.imm != nil {
		if val, kind, seq, found := db.imm.Get(snapshot, userKey); found {
			return val, kind, seq, nil
		}
	}

	v := db.v
	// L0 的文件区间互相重叠，只能从新到旧逐个试。
	l0 := v.Files(0)
	for i := len(l0) - 1; i >= 0; i-- {
		val, kind, seq, found, err := db.probeFile(l0[i].Num, snapshot, userKey)
		if err != nil {
			return nil, 0, 0, err
		}
		if found {
			return val, kind, seq, nil
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
			val, kind, seq, found, err := db.probeFile(f.Num, snapshot, userKey)
			if err != nil {
				return nil, 0, 0, err
			}
			if found {
				return val, kind, seq, nil
			}
		}
	}
	return nil, 0, 0, ErrNotFound
}

// foldMergeLocked 收集并折叠 userKey 的 merge operand 链（M8）。
//
// hitSeq / hitOperand 是已经通过可见性与范围遮蔽检查的最新 operand；收集从
// 它以下继续：按"MemTable → Immutable → L0（从新到旧）→ L1 以下"的顺序逐
// 来源下探，把 TypeMerge 的 operand 收进列表，直到遇到可见的 TypeValue
// （作为 base）或可见的 TypeDeletion（base 为 nil，更旧的记录全部作废）。
//
// 两个逐条判定的细节：
//
//   - 被范围墓碑遮蔽的记录**跳过**而不是终止：遮蔽按记录逐条判定，一条被
//     盖住的 Value 对这个快照等于不存在，它下面的 operand 链要继续收；
//   - 收集从 seq < hitSeq 开始：命中来源里比命中记录新的版本要么不存在、
//     要么 seq > snapshot（对快照不可见），统一用这个条件排除。
//
// 收集完毕后按 seq 从旧到新调 FullMerge。算子返回 error ⇒ 读失败——
// 折叠不出"这个 key 现在的值"时，静默返回旧值等于给错误的数据。
func (db *DB) foldMergeLocked(snapshot uint64, userKey []byte, hitSeq uint64, hitOperand []byte) ([]byte, error) {
	op := db.opts.MergeOperator
	if op == nil {
		return nil, ErrNoMergeOperator
	}
	// hitOperand 可能指向 SST 块缓存里的字节；折叠要等整条链收集完才发生，
	// 先复制一份（memdb 的值虽然稳定，统一复制省得区分来源）。
	hitOperand = append([]byte(nil), hitOperand...)
	// operands 的收集顺序是从新到旧；FullMerge 要求从旧到新，最后反转。
	operands := [][]byte{hitOperand}
	var base []byte
	hasBase := false

	// visit 处理一条候选记录；返回 false 表示收集终止（遇到了终局）。
	// value 一律复制：SST 迭代器的 value 指向块缓存，链要跨多条记录收集。
	visit := func(seq uint64, kind key.Kind, value []byte) bool {
		if db.v.RangeCovers(userKey, seq, snapshot) {
			return true // 被遮蔽：对这个快照不可见，跳过继续收集
		}
		switch kind {
		case key.TypeMerge:
			operands = append(operands, append([]byte(nil), value...))
			return true
		case key.TypeValue:
			base, hasBase = append([]byte(nil), value...), true
			return false
		case key.TypeDeletion:
			// key 在此被删除：base 为 nil，更旧的记录对任何快照都作废。
			hasBase = true
			return false
		default:
			return true
		}
	}

	// walkMem 下探一张 MemTable：定位到最新可见版本后逐个更旧版本走。
	walkMem := func(m *memdb.MemTable) {
		if hasBase || m == nil {
			return
		}
		it := m.Seek(snapshot, userKey)
		for it.Valid() {
			ik := it.Key()
			if db.cmp.Compare(key.UserKey(ik), userKey) != 0 {
				return
			}
			if seq := key.SeqNum(ik); seq < hitSeq {
				if !visit(seq, key.KindOf(ik), it.Value()) {
					return
				}
			}
			it.Next()
		}
	}

	// walkFile 下探一个 SST：与 walkMem 同构，定位用 internal key seek。
	walkFile := func(num uint64) error {
		if hasBase {
			return nil
		}
		r, err := db.readerFor(num)
		if err != nil {
			return err
		}
		it := r.NewIterator()
		for it.Seek(key.SeekKey(userKey, snapshot)); it.Valid(); it.Next() {
			ik := it.Key()
			if db.icmp.CompareUser(key.UserKey(ik), userKey) != 0 {
				return nil
			}
			if seq := key.SeqNum(ik); seq < hitSeq {
				if !visit(seq, key.KindOf(ik), it.Value()) {
					return nil
				}
			}
		}
		return it.Error()
	}

	walkMem(db.mem)
	walkMem(db.imm)
	v := db.v
	l0 := v.Files(0)
	for i := len(l0) - 1; i >= 0 && !hasBase; i-- {
		if err := walkFile(l0[i].Num); err != nil {
			return nil, err
		}
	}
	if v.NumLevels() > 1 && !hasBase {
		target := key.SeekKey(userKey, snapshot)
		for level := 1; level < v.NumLevels() && !hasBase; level++ {
			f := v.FindFile(level, userKey, target)
			if f == nil {
				continue
			}
			if err := walkFile(f.Num); err != nil {
				return nil, err
			}
		}
	}

	for i, j := 0, len(operands)-1; i < j; i, j = i+1, j-1 {
		operands[i], operands[j] = operands[j], operands[i]
	}
	merged, err := op.FullMerge(userKey, base, operands)
	if err != nil {
		return nil, fmt.Errorf("kvdb: merge operator %q: %w", op.Name(), err)
	}
	return merged, nil
}

// probeFile 在编号为 num 的 SST 里查一次，并记录读放大。
func (db *DB) probeFile(num, snapshot uint64, userKey []byte) (value []byte, kind key.Kind, seq uint64, found bool, err error) {
	r, err := db.readerFor(num)
	if err != nil {
		return nil, 0, 0, false, err
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
		Comparer:   db.cmp,
		Cache:      db.blockCache,
		FileNum:    num,
		BlockStats: db.blockStats,
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
