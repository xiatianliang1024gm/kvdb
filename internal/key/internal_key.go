package key

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// TrailerLen 是 internal key 尾缀（trailer）的固定字节数。
const TrailerLen = 8

// MaxSeqNum 是 56 位序列号能表示的最大值。
//
// 序列号占用尾缀的高 56 位，低 8 位留给记录类型，因此 56 位是硬上限。
const MaxSeqNum uint64 = (1 << 56) - 1

// ErrCorruptInternalKey 表示 internal key 的尾缀缺失或类型字段非法。
var ErrCorruptInternalKey = errors.New("github.com/xiatianliang1024gm/kvdb/key: corrupt internal key")

// Kind 是 internal key 尾缀里的记录类型，占 1 字节。
type Kind uint8

const (
	// TypeDeletion 是墓碑（tombstone）：key 已被删除，物理删除留给 Compaction。
	TypeDeletion Kind = 0
	// TypeValue 是一条正常的键值写入。
	TypeValue Kind = 1
	// TypeMerge 预留给 M8 的 Merge 算子（docs/EXTENSIONS.md §4.3）。
	//
	// Kind 编号一旦写进磁盘就冻结，所以它与 TypeRangeDeletion 的编号必须
	// 一次定死：无论两个特性谁先实现，都按同一张表占位。
	TypeMerge Kind = 2
	// TypeRangeDeletion 是范围墓碑（M7，docs/EXTENSIONS.md §4.1）：
	// 半开区间 [Start, End) 内、seq 更小的记录全部不可见。
	//
	// 它**永不落进 SST**：范围墓碑不进 MemTable，而是随写入直接提交到
	// Version 的全局有序表上（Manifest 持久化）。这里放进解码白名单是为了
	// 让"记录类型"这一层对它自洽——批次的编解码、internal key 的完整性
	// 校验都认识它。
	TypeRangeDeletion Kind = 3
)

// String 返回类型的可读名，便于日志与测试输出。
func (k Kind) String() string {
	switch k {
	case TypeDeletion:
		return "Deletion"
	case TypeValue:
		return "Value"
	case TypeMerge:
		return "Merge"
	case TypeRangeDeletion:
		return "RangeDeletion"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

// RangeDeletion 是一条范围墓碑：半开区间 [Start, End) 内、
// 序列号小于 Seq 的记录全部被它遮蔽（不可见）。
//
// 它挂在 Version 上做成一张全局有序表，不按文件存——范围数等于
// "执行过的范围删除次数"，通常是几十量级，O(n) 线性扫的常数开销
// 换掉一整层 per-file 状态，是划算的取舍（docs/EXTENSIONS.md 附录 C）。
type RangeDeletion struct {
	Start []byte
	End   []byte
	Seq   uint64
}

// CoveredByRange 判断 (userKey, seq) 是否被某条范围墓碑遮蔽：
//
//	存在 T 使 Start <= userKey < End，且 seq < T.Seq <= snapshot。
//
// snapshot 上界是快照隔离的一半：T.Seq > snapshot 的墓碑对该快照不可见，
// 它要删的数据快照必须还能读到。返回 true 表示该版本对 snapshot 不可见，
// 且同 key 更旧的版本必然也被遮蔽（遮蔽条件随 seq 减小单调成立）。
//
// 线性扫描是刻意的：范围数是"执行过的范围删除次数"，几十量级，
// 一次线性扫的代价远低于为此维护区间树的复杂度。
func CoveredByRange(ranges []RangeDeletion, ucmp func(a, b []byte) int, userKey []byte, seq, snapshot uint64) bool {
	for i := range ranges {
		t := &ranges[i]
		if t.Seq <= seq || t.Seq > snapshot {
			continue
		}
		if ucmp(t.Start, userKey) <= 0 && ucmp(userKey, t.End) < 0 {
			return true
		}
	}
	return false
}

// ParsedInternalKey 是解码后的 internal key。
type ParsedInternalKey struct {
	UserKey []byte
	Seq     uint64
	Kind    Kind
}

// String 按 LevelDB 风格输出，例如 `foo#12,Value`。
func (p ParsedInternalKey) String() string {
	return fmt.Sprintf("%s#%d,%s", p.UserKey, p.Seq, p.Kind)
}

// MakeTrailer 把 (seq, kind) 打包成尾缀的数值形式：seq<<8 | kind。
//
// seq 超过 MaxSeqNum 属于调用方的编程错误，直接 panic——
// 静默截断会破坏排序前提，进而产生难以定位的数据错乱。
func MakeTrailer(seq uint64, kind Kind) uint64 {
	if seq > MaxSeqNum {
		panic(fmt.Sprintf("github.com/xiatianliang1024gm/kvdb/key: sequence number %d exceeds MaxSeqNum %d", seq, MaxSeqNum))
	}
	return seq<<8 | uint64(kind)
}

// AppendInternalKey 把 userKey 与其 8 字节大端尾缀追加到 dst 末尾并返回扩展后的切片。
func AppendInternalKey(dst, userKey []byte, seq uint64, kind Kind) []byte {
	dst = append(dst, userKey...)
	var trailer [TrailerLen]byte
	binary.BigEndian.PutUint64(trailer[:], MakeTrailer(seq, kind))
	return append(dst, trailer[:]...)
}

// EncodeInternalKey 分配一段新内存并返回 `userKey + trailer` 的编码结果。
func EncodeInternalKey(userKey []byte, seq uint64, kind Kind) []byte {
	dst := make([]byte, 0, len(userKey)+TrailerLen)
	return AppendInternalKey(dst, userKey, seq, kind)
}

// splitInternalKey 切出 user_key 与尾缀；输入过短时返回 (nil, 0)。
func splitInternalKey(ik []byte) (userKey []byte, trailer uint64) {
	if len(ik) < TrailerLen {
		return nil, 0
	}
	return ik[:len(ik)-TrailerLen], binary.BigEndian.Uint64(ik[len(ik)-TrailerLen:])
}

// UserKey 返回 ik 的 user_key 部分，与 ik 共享底层数组；ik 过短时返回 nil。
func UserKey(ik []byte) []byte {
	userKey, _ := splitInternalKey(ik)
	return userKey
}

// Trailer 返回 ik 尾缀的数值形式 (seq<<8|kind)；ik 过短时返回 0。
func Trailer(ik []byte) uint64 {
	_, trailer := splitInternalKey(ik)
	return trailer
}

// SeqNum 返回 ik 的序列号；ik 过短时返回 0。
func SeqNum(ik []byte) uint64 { return Trailer(ik) >> 8 }

// KindOf 返回 ik 的记录类型；ik 过短时返回 TypeDeletion（数值 0）。
func KindOf(ik []byte) Kind { return Kind(Trailer(ik) & 0xff) }

// DecodeInternalKey 解析 ik，返回 user_key（与 ik 共享底层数组）、序列号与类型。
//
// 相比 SeqNum/KindOf 这类"尽力而为"的访问器，本函数会校验长度与类型字段，
// 适用于从磁盘读回数据后的完整性检查。
func DecodeInternalKey(ik []byte) (userKey []byte, seq uint64, kind Kind, err error) {
	if len(ik) < TrailerLen {
		return nil, 0, 0, fmt.Errorf("%w: needs at least %d bytes, got %d", ErrCorruptInternalKey, TrailerLen, len(ik))
	}
	trailer := binary.BigEndian.Uint64(ik[len(ik)-TrailerLen:])
	seq, kind = trailer>>8, Kind(trailer&0xff)
	switch kind {
	case TypeDeletion, TypeValue, TypeMerge, TypeRangeDeletion:
	default:
		return nil, 0, 0, fmt.Errorf("%w: unknown kind %d", ErrCorruptInternalKey, uint8(kind))
	}
	return ik[:len(ik)-TrailerLen], seq, kind, nil
}

// ParseInternalKey 是 DecodeInternalKey 的 struct 版本，便于在迭代器里传递。
func ParseInternalKey(ik []byte) (ParsedInternalKey, error) {
	userKey, seq, kind, err := DecodeInternalKey(ik)
	if err != nil {
		return ParsedInternalKey{}, err
	}
	return ParsedInternalKey{UserKey: userKey, Seq: seq, Kind: kind}, nil
}

// seekTrailerKind 是定位目标（SeekKey）里 kind 字段的取值：Kind 字段的上界 0xFF。
//
// 定位目标必须是"seq <= snapshot 的全部记录"里**最大**的尾缀，这样 Seek 的
// 落点（尾缀降序下第一个 >= 目标的记录）才是最新的可见版本。旧实现用
// TypeValue，在只有 Value / Deletion 两种类型时成立；M8 引入 TypeMerge
// （2 > 1）之后，与定位点**同序列号**的 merge 记录尾缀更大、排在目标之前，
// 会被 Seek 静默跳过——读到的就是旧版本。取上界 0xFF 对任何未来的新类型
// 都成立。（RocksDB 为同一个原因把 kValueTypeForSeek 定成 kTypeMerge。）
const seekTrailerKind = Kind(0xFF)

// SeekKey 返回"在 snapshot 序列号下查找 key"的定位 key：user_key + (snapshot<<8 | 0xFF)。
//
// 尾缀按降序排列，而 (snapshot, 0xFF) 是"seq <= snapshot 的全部版本"可能取到的
// 最大尾缀，因此 seek 的落点恰好是该 key 在 snapshot 下最新的可见记录；
// 若该记录是墓碑，则说明 key 在 snapshot 时刻已被删除。
func SeekKey(userKey []byte, snapshot uint64) []byte {
	return EncodeInternalKey(userKey, snapshot, seekTrailerKind)
}

// InternalKeyCompare 按 internal key 的规则比较 a 与 b：
//
//  1. 先按 user_key 升序，顺序由 userCmp 决定；
//  2. user_key 相同时，按 8 字节尾缀**降序**——保证 (seq, kind) 更大的新版本排在前面。
//
// 第 2 条是整个 MVCC 的基础。userCmp 为 nil 时退化为 bytes.Compare。
func InternalKeyCompare(a, b []byte, userCmp func(a, b []byte) int) int {
	if userCmp == nil {
		userCmp = bytes.Compare
	}
	aUser, aTrailer := splitInternalKey(a)
	bUser, bTrailer := splitInternalKey(b)
	if c := userCmp(aUser, bUser); c != 0 {
		return c
	}
	switch {
	case aTrailer > bTrailer:
		return -1
	case aTrailer < bTrailer:
		return 1
	default:
		return 0
	}
}
