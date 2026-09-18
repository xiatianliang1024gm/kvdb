package wal

import (
	"encoding/binary"
	"io"

	"kvdb/internal/crc"
)

// 分块日志的物理布局（沿用 LevelDB 的 log 格式，只把整数字节序统一成大端）：
//
//	┌──────────────┬──────────────┬────────────┬─────────────────┐
//	│ crc32c (4B)  │ 长度 (2B)    │ 类型 (1B)  │ 记录负载         │
//	 └──────────────┴──────────────┴────────────┴─────────────────┘
//
// 文件按 BlockSize 切成块，一条记录可以跨块，跨块时用 FIRST/MIDDLE/LAST 标记拆分，
// 单块装得下则是 FULL。块尾剩余空间不足一个头时补零。
//
// 校验范围是"类型字节 + 负载"，因此类型字段本身的翻转也能被发现。
const (
	// BlockSize 是日志文件的块大小。
	BlockSize = 32 << 10 // 32KB
	// HeaderSize 是块内每条记录的固定头长度。
	HeaderSize = 4 + 2 + 1
)

// recordType 标记一条记录在块内的拆分角色。
type recordType byte

const (
	// typeFull 表示一条不跨块的完整记录。
	typeFull recordType = 1
	// typeFirst 表示跨块记录的第一段。
	typeFirst recordType = 2
	// typeMiddle 表示跨块记录的中间段。
	typeMiddle recordType = 3
	// typeLast 表示跨块记录的最后一段。
	typeLast recordType = 4

	// typeZero 出现在预分配（全零）的块上，解析时直接跳过。
	typeZero recordType = 0
)

// typeCRCs 缓存"类型字节"的 CRC，作为整条记录校验的起点。
var typeCRCs = func() [5]uint32 {
	var t [5]uint32
	for i := range t {
		t[i] = crc.Update(0, []byte{byte(i)})
	}
	return t
}()

// recordCRC 计算一条物理记录的校验值。
//
// 掩码与 CRC32C 表都来自 internal/crc：SST 的块校验用的是同一套算法，
// 存储层只有一份校验实现，"写的时候能过、读的时候报损坏"这类不一致就不可能发生。
func recordCRC(t recordType, payload []byte) uint32 {
	return crc.Mask(crc.Update(typeCRCs[t], payload))
}

// Writer 把记录切成块写入底层 io.Writer。
//
// 它不持有锁，也不是并发安全的：调用方（Log）负责串行化与 fsync。
type Writer struct {
	dst         io.Writer
	blockOffset int   // 当前块内已写入的字节数
	written     int64 // 累计写入的字节数（含头与补零）
}

// NewWriter 创建一个块对齐的记录写入器。
func NewWriter(dst io.Writer) *Writer {
	return &Writer{dst: dst}
}

// Written 返回累计写入的字节数。
func (w *Writer) Written() int64 { return w.written }

// addRecord 把 record 按需拆成若干物理记录追加写入。
func (w *Writer) addRecord(record []byte) error {
	left := record
	begin := true
	for {
		leftover := BlockSize - w.blockOffset
		if leftover < HeaderSize {
			// 块尾放不下一个头：补零跳过。leftover 为 0 时无需补。
			if leftover > 0 {
				if err := w.writeZeros(leftover); err != nil {
					return err
				}
			}
			w.blockOffset = 0
		}

		avail := BlockSize - w.blockOffset - HeaderSize
		fragLen := avail
		if len(left) < avail {
			fragLen = len(left)
		}
		end := fragLen == len(left)

		var t recordType
		switch {
		case begin && end:
			t = typeFull
		case begin:
			t = typeFirst
		case end:
			t = typeLast
		default:
			t = typeMiddle
		}
		if err := w.emitPhysicalRecord(t, left[:fragLen]); err != nil {
			return err
		}
		left = left[fragLen:]
		begin = false
		if end {
			return nil
		}
	}
}

// emitPhysicalRecord 写入"头 + 负载"，不涉及拆分逻辑。
func (w *Writer) emitPhysicalRecord(t recordType, payload []byte) error {
	var header [HeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], recordCRC(t, payload))
	binary.BigEndian.PutUint16(header[4:6], uint16(len(payload)))
	header[6] = byte(t)

	if _, err := w.dst.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.dst.Write(payload); err != nil {
		return err
	}
	w.blockOffset += HeaderSize + len(payload)
	w.written += int64(HeaderSize + len(payload))
	return nil
}

// writeZeros 写入 n 个 0 字节，用于填充块尾。
func (w *Writer) writeZeros(n int) error {
	var zeros [HeaderSize]byte
	if _, err := w.dst.Write(zeros[:n]); err != nil {
		return err
	}
	w.blockOffset += n
	w.written += int64(n)
	return nil
}
