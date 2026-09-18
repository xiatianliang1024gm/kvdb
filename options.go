package kvdb

import (
	"bytes"
	"errors"
	"fmt"

	"kvdb/internal/key"
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
)

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
