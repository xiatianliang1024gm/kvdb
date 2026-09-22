package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ErrCorruptRecord 表示日志中出现无法解释的字节。
//
// 崩溃恢复时它有两种含义，由调用方结合上下文判断：
//   - 出现在文件末尾：几乎一定是崩溃时写了一半的记录，应当截断丢弃并接受丢尾；
//   - 出现在文件中间：说明后面的数据已经不可信，必须报错而不是继续重放。
type ErrCorruptRecord struct {
	Offset int64
	Reason string
}

func (e *ErrCorruptRecord) Error() string {
	return fmt.Sprintf("github.com/xiatianliang1024gm/kvdb/wal: corrupt record at offset %d: %s", e.Offset, e.Reason)
}

// Reader 按记录边界读回日志。
//
// 它以块为单位向底层 io.Reader 取数，块内顺序解析物理记录，并把
// FIRST/MIDDLE/LAST 片段重新拼成完整记录。
type Reader struct {
	src io.Reader

	block    []byte // 当前块的有效内容
	pos      int    // 当前块内的解析位置
	offset   int64  // 已从 src 读入的字节数，用于错误定位
	eof      bool   // 底层已到文件末尾
	fragment []byte // 正在拼装的跨块记录
	inFrag   bool   // 是否处于跨块记录拼装中
}

// NewReader 创建一个日志读取器。
func NewReader(src io.Reader) *Reader {
	return &Reader{src: src}
}

// ReadRecord 返回下一条完整记录。
//
// 干净地读到文件末尾时返回 io.EOF；遇到损坏字节返回 *ErrCorruptRecord。
func (r *Reader) ReadRecord() ([]byte, error) {
	for {
		t, payload, err := r.nextFragment()
		if err != nil {
			if errors.Is(err, io.EOF) && r.inFrag {
				// 记录还没拼完就到文件末尾：崩溃时写了一半。
				return nil, &ErrCorruptRecord{Offset: r.offset, Reason: "unexpected EOF in the middle of a fragmented record"}
			}
			return nil, err
		}

		switch t {
		case typeFull:
			if r.inFrag {
				return nil, &ErrCorruptRecord{Offset: r.offset, Reason: "full record inside a fragmented record"}
			}
			return payload, nil
		case typeFirst:
			if r.inFrag {
				return nil, &ErrCorruptRecord{Offset: r.offset, Reason: "first fragment inside a fragmented record"}
			}
			r.inFrag = true
			r.fragment = append(r.fragment[:0], payload...)
		case typeMiddle:
			if !r.inFrag {
				return nil, &ErrCorruptRecord{Offset: r.offset, Reason: "middle fragment without a preceding first fragment"}
			}
			r.fragment = append(r.fragment, payload...)
		case typeLast:
			if !r.inFrag {
				return nil, &ErrCorruptRecord{Offset: r.offset, Reason: "last fragment without a preceding first fragment"}
			}
			r.fragment = append(r.fragment, payload...)
			r.inFrag = false
			return r.fragment, nil
		default:
			return nil, &ErrCorruptRecord{Offset: r.offset, Reason: fmt.Sprintf("unknown record type %d", t)}
		}
	}
}

// nextFragment 取出下一个物理记录的负载。
func (r *Reader) nextFragment() (recordType, []byte, error) {
	for {
		if r.block == nil || r.pos == len(r.block) {
			if err := r.fillBlock(); err != nil {
				return 0, nil, err
			}
		}

		left := len(r.block) - r.pos
		if left < HeaderSize {
			// 块尾不足一个头：是补零区或块被写坏，直接跳到下一块。
			if r.block[r.pos] != 0 {
				return 0, nil, &ErrCorruptRecord{Offset: r.offset - int64(left), Reason: "truncated record header at the end of a block"}
			}
			r.pos = len(r.block)
			continue
		}

		header := r.block[r.pos : r.pos+HeaderSize]
		want := binary.BigEndian.Uint32(header[0:4])
		length := int(binary.BigEndian.Uint16(header[4:6]))
		t := recordType(header[6])

		if t == typeZero && length == 0 && want == 0 {
			// 预分配块或块尾补零，跳过整块。
			r.pos = len(r.block)
			continue
		}
		if t < typeFull || t > typeLast {
			return 0, nil, &ErrCorruptRecord{Offset: r.offset - int64(left), Reason: fmt.Sprintf("unknown record type %d", t)}
		}
		if length > left-HeaderSize {
			return 0, nil, &ErrCorruptRecord{Offset: r.offset - int64(left), Reason: fmt.Sprintf("record claims %d bytes but only %d left in block", length, left-HeaderSize)}
		}

		payload := r.block[r.pos+HeaderSize : r.pos+HeaderSize+length]
		if got := recordCRC(t, payload); got != want {
			return 0, nil, &ErrCorruptRecord{Offset: r.offset - int64(left), Reason: fmt.Sprintf("checksum mismatch: got %#x, want %#x", got, want)}
		}
		r.pos += HeaderSize + length
		return t, payload, nil
	}
}

// fillBlock 从底层读入下一块。读到干净的文件末尾返回 io.EOF。
func (r *Reader) fillBlock() error {
	if r.eof {
		return io.EOF
	}
	buf := make([]byte, BlockSize)
	n, err := io.ReadFull(r.src, buf)
	switch {
	case err == nil:
	case errors.Is(err, io.ErrUnexpectedEOF):
		// 文件长度不是块大小的整数倍：最后一块是短块。
		r.eof = true
	case errors.Is(err, io.EOF):
		r.eof = true
		if r.block == nil && n == 0 {
			return io.EOF
		}
	default:
		return err
	}
	if n == 0 && r.block == nil {
		return io.EOF
	}
	r.offset += int64(n)
	r.block = buf[:n]
	r.pos = 0
	return nil
}
