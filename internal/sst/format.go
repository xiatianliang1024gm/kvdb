package sst

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/xiatianliang1024gm/kvdb/internal/compress"
	"github.com/xiatianliang1024gm/kvdb/internal/filter"
	"github.com/xiatianliang1024gm/kvdb/internal/key"
)

// Suffix 是 SSTable 文件的扩展名。
const Suffix = ".sst"

// FileName 返回编号为 num 的 SSTable 文件名。
//
// 编号补零到 6 位，好让"文件名排序 = 编号排序"也成立；编号本身在 SST 与 WAL
// 之间共享同一个空间，"编号大 = 更新"这条规则因此对两种文件同时成立。
func FileName(num uint64) string {
	return fmt.Sprintf("%06d%s", num, Suffix)
}

// FilePath 返回编号为 num 的 SSTable 完整路径。
func FilePath(dir string, num uint64) string {
	return filepath.Join(dir, FileName(num))
}

// 文件格式常量与 Footer / BlockHandle 的编解码。
//
// 与 M1 的线性布局相比，M2 的文件是"多块 + 三级索引"：
//
//	┌──────────────┬──────────────┬─────┬──────────────┬────────────┬─────────────┬───────────────┐
//	│ Data Block 0 │ Data Block 1 │ ... │ Filter Block │ MetaIndex  │ Index Block │ Footer (48B)  │
//	└──────────────┴──────────────┴─────┴──────────────┴────────────┴─────────────┴───────────────┘
//
// 每个块的物理形态（含最后那个）：
//
//	块内容 | 压缩类型(1B) | CRC32C(4B)      校验范围 = 块内容 + 类型字节
//
// Footer 是文件入口，定长 48 字节：
//
//	MetaIndex handle | Index handle | 补零到 40 字节 | 魔数(8B)
//
// handle = uvarint(块偏移) | uvarint(块内容长度)，长度不含 5 字节的块尾部。
const (
	// FooterLen 是文件末尾定长 Footer 的字节数。
	FooterLen = 48

	// footerHandleArea 是 Footer 里放两个 handle 的区域长度：两个 handle 各最多 20 字节。
	footerHandleArea = 40

	// magic 识别"这确实是一个 kvdb SSTable"，数值是 ASCII 的 "kvdb0002"。
	//
	// 它在 M2 被换掉了：M1 的线性布局用的是 "kvdb0001"。用不同魔数而不是复用旧值，
	// 是为了让老文件被明确拒绝（ErrLegacyFormat），而不是被新读取器按新布局误读成
	// 一堆看似合法、实际乱序的块。
	magic uint64 = 0x6b76646230303032
	// magicM1 是 M1 线性布局的魔数，只用于给出可读的报错。
	magicM1 uint64 = 0x6b76646230303031

	// BlockTrailerLen 是每个块尾部的字节数：压缩类型 1 字节 + CRC32C 4 字节。
	BlockTrailerLen = 5

	// blockCompressionNone 表示块未压缩。它在 M2 就占好了位，因此 M5 引入压缩
	// **不需要改文件格式**，M2 写出的文件（类型 0）也照样能读。
	blockCompressionNone byte = byte(compress.TypeNone)

	// restartInterval 是数据块内重启点的间隔：每 16 条记录一个重启点。
	//
	// 重启点之间用前缀压缩，间隔越大块越小、但块内线性扫描越长。16 是 LevelDB 的默认值。
	restartInterval = 16

	// DefaultBlockSize 是 Data Block 的目标大小。
	DefaultBlockSize = 4 << 10

	// indexRestartInterval 是索引块的重启点间隔：1 表示每条都是重启点。
	//
	// 索引块很小（每条几十字节），追求的是"二分时不必先线性扫描"，所以不做前缀压缩。
	indexRestartInterval = 1

	// filterMetaPrefix 是 MetaIndex 里过滤器条目的 key 前缀。
	filterMetaPrefix = "filter."

	// compressionBreakEven 是"压缩划不划算"的判据：压缩后至少要省下 1/8。
	//
	// 沿用 LevelDB 的取值。它与块内的数据形态强相关 —— 过滤器位图与小块索引
	// 压缩后往往更大，所以每个块都要独立判断，不能按文件甚至按库一刀切。
	// 不划算时就原样存储（类型字节回到 0），读路径也能少一次解压。
	compressionBreakEven = 8
)

var (
	// ErrBadFooter 表示 Footer 缺失或不可识别。
	//
	// 它是**可容忍**的：崩溃时写到一半的 Flush 没有 Footer，或者 Footer 只写了一半，
	// 都会落到这个错误上，调用方应当丢弃该文件（数据仍在 WAL 里）。
	ErrBadFooter = errors.New("github.com/xiatianliang1024gm/kvdb/sst: bad footer")

	// ErrLegacyFormat 表示文件是 M1 的线性布局，与 M2 不兼容。
	//
	// 它**不可容忍**：这说明数据目录来自旧版本，静默丢弃会造成真实的数据损失，
	// 必须让调用方明确报错而不是当损坏文件删掉。
	ErrLegacyFormat = errors.New("github.com/xiatianliang1024gm/kvdb/sst: unsupported sst format (M1 linear layout)")

	// ErrCorruptBlock 表示块 CRC 校验失败，或块/索引结构非法。
	ErrCorruptBlock = errors.New("github.com/xiatianliang1024gm/kvdb/sst: corrupt block")

	// ErrUnsupportedCompression 表示块尾的类型字节本引擎不认识。
	//
	// 它**不可容忍**：把它当"未压缩"处理会把一段压缩流交给上层解析，
	// 得到一堆看似合法、实际全是乱码的 key —— 静默的数据损坏。
	// 这个错误通常意味着"文件来自更新的版本"，用户需要知道这一点。
	ErrUnsupportedCompression = errors.New("github.com/xiatianliang1024gm/kvdb/sst: unsupported block compression type")
)

// BlockStats 汇总块压缩在读写两侧的规模。
//
// 它是**原子量的集合**，因此可以被多个 Writer / Reader 并发累加：DB 全程只持有
// 一份，把它通过 WriterOptions / OpenOptions 交给每条流水线，最后读出来就是全库
// 的压缩效果 —— 不需要在各个写入点做汇总，也不会因为文件被删掉而丢统计。
//
// 它不能按值复制（含 atomic），必须始终以指针使用。
type BlockStats struct {
	// BlocksWritten 是写出的块总数（数据块 + 过滤器 / 元索引 / 索引块）。
	BlocksWritten atomic.Int64
	// CompressedWritten 是其中真正以压缩形态落盘的块数。
	//
	// 它与 BlocksWritten 的差值里既有"不划算所以没压"的块（过滤器位图、小索引块），
	// 也有"压了反而更大"的块 —— 两者都正确地落在了不压缩这一侧。
	CompressedWritten atomic.Int64
	// RawBytesWritten 是这些块的原始（未压缩）字节数之和。
	RawBytesWritten atomic.Uint64
	// StoredBytesWritten 是它们实际落盘的字节数之和（含未压缩的块）。
	//
	// 压缩比 = RawBytesWritten / StoredBytesWritten。
	StoredBytesWritten atomic.Uint64
	// Decompressions 是读取时实际发生解压的次数。
	//
	// 它只在块缓存未命中、且那一块确实是压缩存储时才会增加 ——
	// 于是这个数与缓存命中率是一对：命中率高时它自然低。
	Decompressions atomic.Int64
	// CompressedBytesRead 是读进来待解压的字节数之和，用于估算解压吞吐。
	CompressedBytesRead atomic.Uint64
}

// Snapshot 返回一份可读的统计副本。
func (s *BlockStats) Snapshot() BlockStatsSnapshot {
	if s == nil {
		return BlockStatsSnapshot{}
	}
	return BlockStatsSnapshot{
		Blocks:         s.BlocksWritten.Load(),
		Compressed:     s.CompressedWritten.Load(),
		RawBytes:       s.RawBytesWritten.Load(),
		StoredBytes:    s.StoredBytesWritten.Load(),
		Decompressions: s.Decompressions.Load(),
		CompressedRead: s.CompressedBytesRead.Load(),
	}
}

// BlockStatsSnapshot 是 BlockStats 的一次性读数。
type BlockStatsSnapshot struct {
	Blocks         int64
	Compressed     int64
	RawBytes       uint64
	StoredBytes    uint64
	Decompressions int64
	CompressedRead uint64
}

// Ratio 返回压缩比（原始字节 / 落盘字节）。没有写过块时返回 0。
func (s BlockStatsSnapshot) Ratio() float64 {
	if s.StoredBytes == 0 {
		return 0
	}
	return float64(s.RawBytes) / float64(s.StoredBytes)
}

// SavedPercent 返回省下的字节占比，供日志直接打印。
func (s BlockStatsSnapshot) SavedPercent() float64 {
	if s.RawBytes == 0 {
		return 0
	}
	return 100 * (1 - float64(s.StoredBytes)/float64(s.RawBytes))
}

// blockHandle 指向文件里的一个块。
type blockHandle struct {
	offset uint64 // 块内容起始偏移
	size   uint64 // 块内容字节数，不含 5 字节尾部
}

// encode 把 handle 追加到 dst 末尾并返回扩展后的切片。
func (h blockHandle) encode(dst []byte) []byte {
	dst = key.PutUvarint(dst, h.offset)
	return key.PutUvarint(dst, h.size)
}

// decodeBlockHandle 解析 buf 开头的一个 handle，同时返回消耗的字节数。
func decodeBlockHandle(buf []byte) (blockHandle, int, error) {
	off, n1, err := key.Uvarint(buf)
	if err != nil {
		return blockHandle{}, 0, fmt.Errorf("%w: block handle offset: %v", ErrCorruptBlock, err)
	}
	size, n2, err := key.Uvarint(buf[n1:])
	if err != nil {
		return blockHandle{}, 0, fmt.Errorf("%w: block handle size: %v", ErrCorruptBlock, err)
	}
	return blockHandle{offset: off, size: size}, n1 + n2, nil
}

// encodeFooter 生成定长 Footer。
func encodeFooter(meta, index blockHandle) [FooterLen]byte {
	var buf [FooterLen]byte
	area := meta.encode(nil)
	area = index.encode(area)
	if len(area) > footerHandleArea {
		// 两个 handle 各最多 20 字节，40 字节是硬上界，越界只可能是编码逻辑写错。
		panic(fmt.Sprintf("github.com/xiatianliang1024gm/kvdb/sst: footer handle area overflow (%d bytes)", len(area)))
	}
	copy(buf[:], area)
	binary.BigEndian.PutUint64(buf[footerHandleArea:], magic)
	return buf
}

// decodeFooter 解析 Footer，返回 MetaIndex 与 Index 两个 handle。
func decodeFooter(footer []byte) (meta, index blockHandle, err error) {
	if len(footer) < FooterLen {
		return meta, index, fmt.Errorf("%w: footer needs %d bytes, got %d", ErrBadFooter, FooterLen, len(footer))
	}
	switch got := binary.BigEndian.Uint64(footer[footerHandleArea:FooterLen]); got {
	case magic:
	case magicM1:
		return meta, index, fmt.Errorf("%w: 该文件是 M1 的线性布局，请使用新目录重新写入", ErrLegacyFormat)
	default:
		return meta, index, fmt.Errorf("%w: magic = %#x, want %#x", ErrBadFooter, got, magic)
	}

	area := footer[:footerHandleArea]
	meta, n, err := decodeBlockHandle(area)
	if err != nil {
		return meta, index, err
	}
	if n >= footerHandleArea {
		return meta, index, fmt.Errorf("%w: footer 里没有放 index handle 的位置", ErrBadFooter)
	}
	index, n2, err := decodeBlockHandle(area[n:])
	if err != nil {
		return meta, index, err
	}
	for _, b := range area[n+n2:] { // 补零区必须真的是零
		if b != 0 {
			return meta, index, fmt.Errorf("%w: footer padding is not zero", ErrBadFooter)
		}
	}
	return meta, index, nil
}

// filterMetaKey 返回 MetaIndex 里过滤器条目的 key。
func filterMetaKey() []byte {
	return []byte(filterMetaPrefix + filter.PolicyName)
}
