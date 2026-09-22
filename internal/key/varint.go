package key

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// MaxVarintLen32 是 32 位无符号整数 varint 编码后的最大字节数。
	MaxVarintLen32 = 5
	// MaxVarintLen64 是 64 位无符号整数 varint 编码后的最大字节数。
	MaxVarintLen64 = 10

	// Fixed32Len 是定长 32 位整数的字节数。
	Fixed32Len = 4
	// Fixed64Len 是定长 64 位整数的字节数。
	Fixed64Len = 8
)

var (
	// ErrVarintTruncated 表示缓冲区在 varint 编码结束前就已耗尽。
	ErrVarintTruncated = errors.New("github.com/xiatianliang1024gm/kvdb/key: truncated varint")
	// ErrVarintOverflow 表示 varint 编码超过 64 位，无法用 uint64 表示。
	ErrVarintOverflow = errors.New("github.com/xiatianliang1024gm/kvdb/key: varint overflows 64 bits")
	// ErrBufferTooSmall 表示读取定长编码时缓冲区长度不足。
	ErrBufferTooSmall = errors.New("github.com/xiatianliang1024gm/kvdb/key: buffer too small")
)

// PutUvarint 将 x 的 uvarint 编码追加到 dst 末尾并返回扩展后的切片。
//
// 编码方式为 7 位一组、小端排列，每字节最高位表示"后面还有字节"。
// 输出与 encoding/binary.PutUvarint 逐字节一致。
func PutUvarint(dst []byte, x uint64) []byte {
	for x >= 0x80 {
		dst = append(dst, byte(x)|0x80)
		x >>= 7
	}
	return append(dst, byte(x))
}

// Uvarint 从 buf 起始处解码一个 uvarint，返回解码值与消耗的字节数。
//
// 出错时返回值与字节数均为 0，可能的错误为 ErrVarintTruncated 或 ErrVarintOverflow。
// 注意 buf 可以长于编码本身，多余的字节不会被读取。
func Uvarint(buf []byte) (value uint64, n int, err error) {
	var x uint64
	var s uint
	for i, b := range buf {
		if i == MaxVarintLen64 {
			return 0, 0, ErrVarintOverflow
		}
		if b < 0x80 {
			if i == MaxVarintLen64-1 && b > 1 {
				return 0, 0, ErrVarintOverflow
			}
			return x | uint64(b)<<s, i + 1, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0, ErrVarintTruncated
}

// UvarintLen 返回 x 的 uvarint 编码长度，不实际分配内存。
func UvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// PutVarint 将 x 的 zigzag varint 编码追加到 dst 末尾并返回扩展后的切片。
//
// 先用 zigzag 把有符号数映射为非负数（绝对值小的负数编码后同样很短），
// 再按 uvarint 编码。输出与 encoding/binary.PutVarint 逐字节一致。
func PutVarint(dst []byte, x int64) []byte {
	return PutUvarint(dst, zigzagEncode(x))
}

// Varint 从 buf 起始处解码一个 zigzag varint，返回解码值与消耗的字节数。
func Varint(buf []byte) (value int64, n int, err error) {
	ux, n, err := Uvarint(buf)
	if err != nil {
		return 0, 0, err
	}
	return zigzagDecode(ux), n, nil
}

// VarintLen 返回 x 的 zigzag varint 编码长度。
func VarintLen(x int64) int {
	return UvarintLen(zigzagEncode(x))
}

func zigzagEncode(x int64) uint64 {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return ux
}

func zigzagDecode(ux uint64) int64 {
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x
}

// PutFixed32 将 x 以 4 字节大端序追加到 dst 末尾并返回扩展后的切片。
func PutFixed32(dst []byte, x uint32) []byte {
	var buf [Fixed32Len]byte
	binary.BigEndian.PutUint32(buf[:], x)
	return append(dst, buf[:]...)
}

// Fixed32 从 buf 起始处读取一个大端序 uint32。
func Fixed32(buf []byte) (uint32, error) {
	if len(buf) < Fixed32Len {
		return 0, fmt.Errorf("%w: Fixed32 needs %d bytes, got %d", ErrBufferTooSmall, Fixed32Len, len(buf))
	}
	return binary.BigEndian.Uint32(buf), nil
}

// PutFixed64 将 x 以 8 字节大端序追加到 dst 末尾并返回扩展后的切片。
func PutFixed64(dst []byte, x uint64) []byte {
	var buf [Fixed64Len]byte
	binary.BigEndian.PutUint64(buf[:], x)
	return append(dst, buf[:]...)
}

// Fixed64 从 buf 起始处读取一个大端序 uint64。
func Fixed64(buf []byte) (uint64, error) {
	if len(buf) < Fixed64Len {
		return 0, fmt.Errorf("%w: Fixed64 needs %d bytes, got %d", ErrBufferTooSmall, Fixed64Len, len(buf))
	}
	return binary.BigEndian.Uint64(buf), nil
}
