package kvdb

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/xiatianliang1024gm/kvdb/internal/compress"
	"github.com/xiatianliang1024gm/kvdb/internal/key"
	"github.com/xiatianliang1024gm/kvdb/internal/logger"
)

// 默认配置，数值取自 docs/DESIGN.md 附录 A 的 Options 草图。
const (
	// DefaultMemTableSize 是 MemTable 的默认容量阈值，写满即冻结为 Immutable MemTable。
	DefaultMemTableSize = 64 << 20 // 64MB
	// DefaultBlockSize 是 SSTable 中 Data Block 的默认大小。
	DefaultBlockSize = 4 << 10 // 4KB
	// DefaultBlockCacheSize 是解压后数据块的默认缓存容量。
	DefaultBlockCacheSize = 8 << 20 // 8MB
	// DefaultBloomBitsPerKey 是 Bloom Filter 每个 key 占用的默认位数。
	DefaultBloomBitsPerKey = 10
	// DefaultL0CompactionTrigger 是触发 L0 Compaction 的默认文件数阈值。
	DefaultL0CompactionTrigger = 4
	// DefaultLevelBaseSize 是 L1 的默认容量，更深层按 DefaultLevelSizeMultiplier 递增。
	DefaultLevelBaseSize = 256 << 20 // 256MB
	// DefaultLevelSizeMultiplier 是相邻两层容量倍数的默认值。
	DefaultLevelSizeMultiplier = 10
	// DefaultMaxLevels 是默认的最大层数（含 L0）。
	DefaultMaxLevels = 7
	// DefaultLogMaxSize 是数据目录下 LOG 文件的默认轮转阈值。
	DefaultLogMaxSize = 1 << 20 // 1MB
)

// Compression 选择 SSTable 数据块的压缩算法。
//
// 数值沿用本引擎"0 = 默认值"的约定：零值 CompressionDefault 表示启用默认算法
// （Snappy），负数 CompressionNone 表示显式关闭。
type Compression int8

const (
	// CompressionDefault 是零值：采用默认算法（Snappy）。
	CompressionDefault Compression = 0
	// CompressionNone 显式关闭块压缩。
	CompressionNone Compression = -1
	// CompressionSnappy 是 Snappy（default），解压速度优先。
	CompressionSnappy Compression = 1
	// CompressionZlib 是 stdlib flate（BestSpeed），压缩率更高、CPU 更贵。
	CompressionZlib Compression = 2
)

// String 返回算法名，用于日志与压测报告。
func (c Compression) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionSnappy, CompressionDefault:
		return "snappy"
	case CompressionZlib:
		return "zlib"
	default:
		return fmt.Sprintf("unknown(%d)", int8(c))
	}
}

// toType 映射到 internal/compress 的类型字节。ensureDefaults 之后的值一定落在已知项里。
func (c Compression) toType() compress.Type {
	switch c {
	case CompressionNone:
		return compress.TypeNone
	case CompressionZlib:
		return compress.TypeZlib
	default:
		return compress.TypeSnappy
	}
}

// Logger 接收引擎的事件日志（打开、Flush、Compaction、故障……）。
//
// 为 nil 时引擎把事件写进数据目录下的 LOG 文件（按大小轮转到 LOG.old）。
// 想把日志接进自己的体系就实现这个接口传进来；要复用 internal 包自带的
// 文件实现，用 NewFileLogger 构造即可。
type Logger interface {
	// Infof 记录正常运维事件。
	Infof(format string, args ...any)
	// Warnf 记录"能继续跑，但需要人看一眼"的事件。
	Warnf(format string, args ...any)
	// Errorf 记录故障。
	Errorf(format string, args ...any)
}

// fileLogger 把根包的 Logger 接口适配到 internal/logger 的文件实现。
// 做成独立类型而不是让 *logger.Logger 直接实现，是为了不让根包的接口
// 暴露 internal 包的具体方法集。
type fileLogger struct{ l *logger.Logger }

func (f fileLogger) Infof(format string, args ...any)  { f.l.Logf(logger.LevelInfo, format, args...) }
func (f fileLogger) Warnf(format string, args ...any)  { f.l.Logf(logger.LevelWarning, format, args...) }
func (f fileLogger) Errorf(format string, args ...any) { f.l.Logf(logger.LevelError, format, args...) }

// NewFileLogger 创建一个写到 dir 目录下 LOG 文件的 Logger，超过 maxSize 字节
// 轮转为 LOG.old。maxSize <= 0 时用 DefaultLogMaxSize。
//
// 数据库打开期间会持有这个实例；用完（关闭数据库之后）记得调用返回值的 Close。
func NewFileLogger(dir string, maxSize int) (Logger, func() error, error) {
	if maxSize <= 0 {
		maxSize = DefaultLogMaxSize
	}
	l, err := logger.NewFile(dir, logger.DefaultName, maxSize)
	if err != nil {
		return nil, nil, err
	}
	return fileLogger{l}, l.Close, nil
}

// Comparer 定义 user key 之间的全序关系，允许替换以支持自定义 key 编码（如倒序时间戳）。
//
// 实现必须满足三个前提，否则 SSTable 的排序与二分查找前提会被破坏：
//
//   - 反对称：Compare(a, b) == -Compare(b, a)；
//   - 可传递；
//   - 跨进程、跨重启稳定——同一份数据任何时候比较结果必须一致，
//     所以 Name 会被写入 Manifest 用于校验目录与配置是否匹配。
type Comparer interface {
	// Compare 返回 -1 / 0 / +1，分别表示 a < b、a == b、a > b。
	Compare(a, b []byte) int
	// Name 返回该比较器的稳定标识。
	Name() string
}

// BytewiseComparer 按字节序比较，等价于 bytes.Compare，是默认比较器。
type BytewiseComparer struct{}

// Compare 实现 Comparer。
func (BytewiseComparer) Compare(a, b []byte) int { return bytes.Compare(a, b) }

// Name 实现 Comparer。
func (BytewiseComparer) Name() string { return "kvdb.BytewiseComparer" }

// defaultComparer 是 Options.Comparer 为空时使用的兜底比较器。
var defaultComparer Comparer = BytewiseComparer{}

// internalComparer 在 user key 比较器之上叠加 internal key 的排序规则：
// user_key 升序，user_key 相同时尾缀降序（新版本在前）。
//
// 实现在 internal/key 中，这里保留别名是为了让根包与内部包共用同一份逻辑。
type internalComparer = key.InternalComparer

// Options 汇总引擎的可调参数。零值不保证可用，请用 DefaultOptions 构造，
// 或在打开数据库前依赖内部的 ensureDefaults 补齐缺省字段。
type Options struct {
	// Dir 是数据目录，必须存在且可写。同一目录同时只允许一个进程打开。
	Dir string

	// Comparer 决定 user key 的顺序，nil 时使用 BytewiseComparer。
	Comparer Comparer

	// MemTableSize 是 MemTable 的容量阈值，单位字节，写满即冻结。
	MemTableSize int
	// BlockSize 是 SSTable 中单个 Data Block 的目标大小，单位字节。
	BlockSize int
	// BlockCacheSize 是分片 LRU 块缓存的容量，单位字节，0 表示关闭缓存。
	BlockCacheSize int
	// BloomBitsPerKey 是 Bloom Filter 每个 key 占用的位数，0 表示不建过滤器。
	BloomBitsPerKey int
	// L0CompactionTrigger 是 L0 文件数达到即触发 Compaction 的阈值。
	L0CompactionTrigger int
	// LevelBaseSize 是 L1 的容量上限，单位字节；更深层按倍数递增。
	LevelBaseSize int
	// LevelSizeMultiplier 是相邻两层容量的倍数。
	LevelSizeMultiplier int
	// MaxLevels 是最大层数（含 L0），必须大于等于 2。
	MaxLevels int

	// SyncWrites 为真时每条写都等待 WAL fsync 落盘后才返回。
	//
	// 置为 false 则返回更快，但进程崩溃可能丢掉最近若干条写——
	// 只适合可重建的缓存类数据。
	SyncWrites bool

	// Compression 选择 SSTable 数据块的块级压缩算法。
	//
	// 压缩发生在"一个 Data Block 写满"的时刻：压完若省不下 1/8 就原样存储。
	// 读侧在块缓存未命中时解压一次。默认 Snappy。
	Compression Compression

	// CompactionRateLimit 是后台 Compaction 的读写带宽上限，单位字节/秒。
	//
	// 0 = 不限流。Compaction 与前台读写共享磁盘，磁盘抖动最明显的症状是
	// 点查长尾突然变高 —— 给它限一个配额，就是把长尾换成交付时间。
	CompactionRateLimit int

	// CompactionFilter 在后台 Compaction 时对每个 user key 的"最新可见版本"
	// 调用一次，判为 Drop 的记录按墓碑语义处理（物理清除发生在这一步）。
	// nil = 不过滤。
	//
	// 调用发生在后台线程：不保证顺序、同一条记录会随多次 Compaction 被多次
	// 判定，实现必须并发安全。Filter 返回 error 会让本次 Compaction 失败并
	// 停库 —— 过滤器的内部故障被当作引擎故障，而不是静默跳过。
	//
	// Name() 会被写进 Manifest 用于校验目录与配置是否匹配：目录一旦用某个
	// 过滤器的名字落过盘，之后换名字（或不再配置过滤器）打开会报错，
	// 级别同 Comparer.Name —— 换一套过滤语义读老目录，会造成
	// "该丢的没丢、不该丢的丢了"。
	CompactionFilter CompactionFilter

	// FilterOnFlush 为真时 Flush 也执行过滤（默认 false，只在 Compaction 生效）。
	//
	// 默认不开的理由：Flush 在前台关键路径上（决定 MemTable 能否释放），
	// 把用户回调插在那里等于把引擎的延迟暴露给用户代码。打开它适合
	// "写完就想尽快清掉过期数据"的负载；注意两点：Filter 的 error 会让
	// Flush 失败并停库；崩溃后 WAL 重放可能让被过滤的数据临时回来一次
	// （过滤是幂等的，重放后再 Flush 会再次清掉）。
	FilterOnFlush bool

	// MergeOperator 定义 merge operand 的折叠方式（M8）。nil = 不支持 Merge，
	// 此时调用 DB.Merge / WriteBatch.Merge 会返回 ErrNoMergeOperator。
	//
	// 读路径命中 merge 记录后要把同 key 的 operand 链收集齐交给 FullMerge，
	// 所以"读变贵"是 Merge 的固有代价；稳态下 Compaction 把每个 key 折叠回
	// 一条 Value，代价由后台抵消。
	//
	// Name() 会被写进 Manifest 校验，语义同 Comparer：写过 merge 记录的目录
	// 之后只能用同名算子打开——换一套折叠语义读老目录，未折叠的 operand 会
	// 被折出错误的值。
	MergeOperator MergeOperator

	// LogMaxSize 是数据目录下 LOG 文件的轮转阈值，单位字节。
	//
	// 0 = 用 DefaultLogMaxSize（1MB）；负数 = 不写文件日志（Logger 为 nil 时
	// 引擎也没有任何事件输出）。事件本身始终可通过 Options.Logger 接走。
	LogMaxSize int

	// Logger 接收引擎的事件日志；nil 时写入数据目录下的 LOG 文件。
	//
	// 注意：引擎不会关闭用户提供的 Logger，Close 时只关闭自己创建的文件日志。
	Logger Logger
}

// DefaultOptions 返回字段全部填好的默认配置。
func DefaultOptions(dir string) Options {
	return Options{
		Dir:                 dir,
		Comparer:            defaultComparer,
		MemTableSize:        DefaultMemTableSize,
		BlockSize:           DefaultBlockSize,
		BlockCacheSize:      DefaultBlockCacheSize,
		BloomBitsPerKey:     DefaultBloomBitsPerKey,
		L0CompactionTrigger: DefaultL0CompactionTrigger,
		LevelBaseSize:       DefaultLevelBaseSize,
		LevelSizeMultiplier: DefaultLevelSizeMultiplier,
		MaxLevels:           DefaultMaxLevels,
		SyncWrites:          true,
		Compression:         CompressionDefault,
	}
}

// ensureDefaults 把零值字段补成默认值，使 `&Options{Dir: dir}` 这类写法也能安全使用。
//
// 数值字段统一遵循"0 = 沿用默认值"的规则；对 BlockCacheSize / BloomBitsPerKey
// 这两个可关闭的特性，另用负数表示显式关闭（归一化后为 0）。
func (o *Options) ensureDefaults() {
	if o.Comparer == nil {
		o.Comparer = defaultComparer
	}
	if o.MemTableSize <= 0 {
		o.MemTableSize = DefaultMemTableSize
	}
	if o.BlockSize <= 0 {
		o.BlockSize = DefaultBlockSize
	}
	switch {
	case o.BlockCacheSize < 0:
		o.BlockCacheSize = 0 // 显式关闭缓存
	case o.BlockCacheSize == 0:
		o.BlockCacheSize = DefaultBlockCacheSize
	}
	switch {
	case o.BloomBitsPerKey < 0:
		o.BloomBitsPerKey = 0 // 显式关闭 Bloom Filter
	case o.BloomBitsPerKey == 0:
		o.BloomBitsPerKey = DefaultBloomBitsPerKey
	}
	if o.L0CompactionTrigger <= 0 {
		o.L0CompactionTrigger = DefaultL0CompactionTrigger
	}
	if o.LevelBaseSize <= 0 {
		o.LevelBaseSize = DefaultLevelBaseSize
	}
	if o.LevelSizeMultiplier <= 0 {
		o.LevelSizeMultiplier = DefaultLevelSizeMultiplier
	}
	if o.MaxLevels <= 0 {
		o.MaxLevels = DefaultMaxLevels
	}
	// Compression 的零值是"默认算法"而不是"不压缩"，与数值字段不同：
	// 想明确关掉必须写 CompressionNone。这不破坏约定 —— 0 仍然等于"给最常见的默认"。
	if o.Compression == CompressionDefault {
		o.Compression = CompressionSnappy
	}
}

// Validate 校验配置是否自洽。
//
// 它只做检查、不填默认值，因此必须先调用 ensureDefaults 再调用 Validate；
// 打开数据库时请使用 prepare。BlockCacheSize 与 BloomBitsPerKey 的负值
// 不视为错误——它们表示"显式关闭该特性"，由 ensureDefaults 归一化为 0。
func (o *Options) Validate() error {
	if o.Dir == "" {
		return errors.New("kvdb: Options.Dir must not be empty")
	}
	if o.Comparer == nil {
		return errors.New("kvdb: Options.Comparer must not be nil")
	}
	if o.MemTableSize <= 0 {
		return fmt.Errorf("kvdb: Options.MemTableSize must be positive, got %d", o.MemTableSize)
	}
	if o.BlockSize <= 0 {
		return fmt.Errorf("kvdb: Options.BlockSize must be positive, got %d", o.BlockSize)
	}
	if o.L0CompactionTrigger < 2 {
		return fmt.Errorf("kvdb: Options.L0CompactionTrigger must be at least 2, got %d", o.L0CompactionTrigger)
	}
	if o.LevelBaseSize <= 0 {
		return fmt.Errorf("kvdb: Options.LevelBaseSize must be positive, got %d", o.LevelBaseSize)
	}
	if o.LevelSizeMultiplier < 2 {
		return fmt.Errorf("kvdb: Options.LevelSizeMultiplier must be at least 2, got %d", o.LevelSizeMultiplier)
	}
	if o.MaxLevels < 2 {
		return fmt.Errorf("kvdb: Options.MaxLevels must be at least 2, got %d", o.MaxLevels)
	}
	// 零值 CompressionDefault 是合法输入（表示"用默认算法"），ensureDefaults
	// 会把它归一成 Snappy；走到这里还是未知值才是调用方传错了。
	switch o.Compression {
	case CompressionDefault, CompressionNone, CompressionSnappy, CompressionZlib:
	default:
		return fmt.Errorf("kvdb: unknown Options.Compression %d", int8(o.Compression))
	}
	return nil
}

// prepare 补齐默认值并校验配置，是打开数据库前对 Options 的标准化入口。
func (o *Options) prepare() error {
	o.ensureDefaults()
	return o.Validate()
}

// internalKeyComparer 返回叠加了 internal key 排序规则的比较器。
func (o Options) internalKeyComparer() internalComparer {
	return internalComparer{User: o.Comparer}
}

// levelMaxBytes 返回第 level 层的容量上限（字节）；L0 没有容量上限，返回 0。
//
// L0 按文件数触发、L1 以下按容量触发，这个函数就是后者的判据。
// L1 的容量是 LevelBaseSize，之后每层乘 LevelSizeMultiplier —— 这个 10 倍关系
// 不是随手定的：它让每层的容量与它的读写代价匹配，点查每层最多碰一个文件，
// 于是"层数"决定了读放大，而层数只有 log_10(总数据量 / LevelBaseSize) 级别。
func (o Options) levelMaxBytes(level int) uint64 {
	if level == 0 {
		return 0
	}
	size := uint64(o.LevelBaseSize)
	for i := 1; i < level; i++ {
		size *= uint64(o.LevelSizeMultiplier)
	}
	return size
}

// targetFileSize 返回写进第 level 层的单个输出文件的目标字节数。
//
// 取该层容量的 1/100，并夹在 [256KB, 64MB] 之间：
//
//   - 跟着容量走：下层容量大，文件也应当大，否则 L2 会堆出上万个几 KB 的小文件，
//     光是打开它们的索引就要吃掉可观的启动时间；
//   - 上限 64MB：一次 Compaction 的产物若太大，下次搬运它就要把这么多字节重写一遍，
//     写放大会跟着变大；
//   - 下限 256KB：让"一次 Compaction 输出一个文件"这个常见情形成立。
//     测试里把 LevelBaseSize 调小就能观察到切分行为。
func (o Options) targetFileSize(level int) uint64 {
	size := o.levelMaxBytes(level) / 100
	switch {
	case size < 256<<10:
		size = 256 << 10
	case size > 64<<20:
		size = 64 << 20
	}
	return size
}
