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
)

// String 返回类型的可读名，便于日志与测试输出。
func (k Kind) String() string {
	switch k {
	case TypeDeletion:
		return "Deletion"
	case TypeValue:
		return "Value"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
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
	case TypeDeletion, TypeValue:
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

// SeekKey 返回"在 snapshot 序列号下查找 key"的定位 key：user_key + (snapshot<<8 | TypeValue)。
//
// 由于尾缀按降序排列，尾缀 (snapshot, TypeValue) 恰好是"seq <= snapshot 的全部版本"中最大的那个，
// 因此它在有序序列里排在所有可见版本之前。用它做 seek 目标，落点就是该 key 在 snapshot 下
// 最新的可见记录；若该记录是墓碑，则说明 key 在 snapshot 时刻已被删除。
func SeekKey(userKey []byte, snapshot uint64) []byte {
	return EncodeInternalKey(userKey, snapshot, TypeValue)
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
