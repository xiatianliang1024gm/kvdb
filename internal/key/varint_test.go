package key

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

var uvarintGolden = []uint64{
	0, 1, 2, 0x7f, 0x80, 0x81, 0x3fff, 0x4000, 0xffff,
	1 << 20, 1 << 32, math.MaxUint32, 1<<56 - 1, math.MaxUint64,
}

func TestPutUvarintMatchesStdlib(t *testing.T) {
	for _, v := range uvarintGolden {
		got := PutUvarint(nil, v)
		var want [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(want[:], v)
		if !bytes.Equal(got, want[:n]) {
			t.Errorf("PutUvarint(%d) = %v, want %v", v, got, want[:n])
		}
		if len(got) != UvarintLen(v) {
			t.Errorf("UvarintLen(%d) = %d, but encoding occupies %d bytes", v, UvarintLen(v), len(got))
		}
	}
}

func TestUvarintRoundTrip(t *testing.T) {
	for _, v := range uvarintGolden {
		buf := PutUvarint(nil, v)
		got, n, err := Uvarint(buf)
		if err != nil {
			t.Fatalf("Uvarint(%v): unexpected error: %v", buf, err)
		}
		if got != v {
			t.Errorf("Uvarint round trip of %d = %d", v, got)
		}
		if n != len(buf) {
			t.Errorf("Uvarint(%d) consumed %d bytes, want %d", v, n, len(buf))
		}
	}
}

// 编码追加在已有数据之后时，解码必须从 buf 起始处开始、且不多读后面的字节。
func TestUvarintAppendAndOffsetDecoding(t *testing.T) {
	prefix := []byte{0xff, 0xfe}
	buf := PutUvarint(append([]byte(nil), prefix...), 300)
	if !bytes.Equal(buf[:len(prefix)], prefix) {
		t.Fatalf("PutUvarint clobbered the destination prefix: %v", buf)
	}

	got, n, err := Uvarint(buf[len(prefix):])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 300 || n != len(buf)-len(prefix) {
		t.Fatalf("Uvarint = (%d, %d), want (300, %d)", got, n, len(buf)-len(prefix))
	}

	// 尾部多余字节不应被消耗。
	trailing := append(append([]byte(nil), buf...), 0x01, 0x02)
	if _, n, err := Uvarint(trailing[2:]); err != nil || n != 2 {
		t.Fatalf("Uvarint consumed %d bytes (err=%v), want 2", n, err)
	}
}

func TestUvarintErrors(t *testing.T) {
	tests := []struct {
		name string
		buf  []byte
		want error
	}{
		{"empty", nil, ErrVarintTruncated},
		{"single continuation byte", []byte{0x80}, ErrVarintTruncated},
		{"all continuation bytes", []byte{0x80, 0x80, 0x80}, ErrVarintTruncated},
		{"eleven bytes", bytes.Repeat([]byte{0x80}, 11), ErrVarintOverflow},
		{"tenth byte too large", append(bytes.Repeat([]byte{0x80}, 9), 0x02), ErrVarintOverflow},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, n, err := Uvarint(tt.buf)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Uvarint(%v) error = %v, want %v", tt.buf, err, tt.want)
			}
			if got != 0 || n != 0 {
				t.Fatalf("Uvarint(%v) = (%d, %d), want (0, 0) on error", tt.buf, got, n)
			}
		})
	}
}

// 10 字节编码的边界：最后一字节为 1 时恰好是 1<<63，属于合法值。
func TestUvarintMaxEncodingBoundary(t *testing.T) {
	buf := append(bytes.Repeat([]byte{0x80}, 9), 0x01)
	got, n, err := Uvarint(buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 1<<63 || n != 10 {
		t.Fatalf("Uvarint = (%d, %d), want (%d, 10)", got, n, uint64(1)<<63)
	}
}

var varintGolden = []int64{
	0, 1, -1, 2, -2, 63, -64, 64, -65,
	1 << 31, -(1 << 31), math.MaxInt32, math.MinInt32, math.MaxInt64, math.MinInt64,
}

func TestPutVarintMatchesStdlib(t *testing.T) {
	for _, v := range varintGolden {
		got := PutVarint(nil, v)
		var want [binary.MaxVarintLen64]byte
		n := binary.PutVarint(want[:], v)
		if !bytes.Equal(got, want[:n]) {
			t.Errorf("PutVarint(%d) = %v, want %v", v, got, want[:n])
		}
		if len(got) != VarintLen(v) {
			t.Errorf("VarintLen(%d) = %d, but encoding occupies %d bytes", v, VarintLen(v), len(got))
		}
	}
}

func TestVarintRoundTrip(t *testing.T) {
	for _, v := range varintGolden {
		buf := PutVarint(nil, v)
		got, n, err := Varint(buf)
		if err != nil {
			t.Fatalf("Varint(%v): unexpected error: %v", buf, err)
		}
		if got != v {
			t.Errorf("Varint round trip of %d = %d", v, got)
		}
		if n != len(buf) {
			t.Errorf("Varint(%d) consumed %d bytes, want %d", v, n, len(buf))
		}
	}
}

func TestVarintPropagatesUvarintErrors(t *testing.T) {
	if _, _, err := Varint([]byte{0x80}); !errors.Is(err, ErrVarintTruncated) {
		t.Fatalf("Varint error = %v, want ErrVarintTruncated", err)
	}
}

func TestFixedEncodingIsBigEndian(t *testing.T) {
	if got := PutFixed32(nil, 0x01020304); !bytes.Equal(got, []byte{0x01, 0x02, 0x03, 0x04}) {
		t.Errorf("PutFixed32 = %v, want [1 2 3 4]", got)
	}
	want64 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	if got := PutFixed64(nil, 0x0102030405060708); !bytes.Equal(got, want64) {
		t.Errorf("PutFixed64 = %v, want %v", got, want64)
	}
}

func TestFixedRoundTrip(t *testing.T) {
	for _, v := range []uint32{0, 1, 0xff, 0xffff, math.MaxUint32} {
		got, err := Fixed32(PutFixed32(nil, v))
		if err != nil {
			t.Fatalf("Fixed32(%d): unexpected error: %v", v, err)
		}
		if got != v {
			t.Errorf("Fixed32 round trip of %d = %d", v, got)
		}
	}
	for _, v := range []uint64{0, 1, 1 << 32, math.MaxUint64} {
		got, err := Fixed64(PutFixed64(nil, v))
		if err != nil {
			t.Fatalf("Fixed64(%d): unexpected error: %v", v, err)
		}
		if got != v {
			t.Errorf("Fixed64 round trip of %d = %d", v, got)
		}
	}
}

func TestFixedTooSmall(t *testing.T) {
	short := make([]byte, Fixed32Len-1)
	if _, err := Fixed32(short); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("Fixed32 error = %v, want ErrBufferTooSmall", err)
	}
	if _, err := Fixed64(short); !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("Fixed64 error = %v, want ErrBufferTooSmall", err)
	}
}
