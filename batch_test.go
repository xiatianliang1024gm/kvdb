package kvdb

import (
	"bytes"
	"errors"
	"testing"

	"kvdb/internal/key"
)

func TestWriteBatchEncodeLayout(t *testing.T) {
	b := NewWriteBatch()
	if err := b.Put([]byte("ab"), []byte("cd")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("e")); err != nil {
		t.Fatal(err)
	}
	b.SetSequence(0x0102030405060708)

	if b.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", b.Len())
	}
	if b.Sequence() != 0x0102030405060708 {
		t.Fatalf("Sequence() = %#x", b.Sequence())
	}

	got := b.Encode()
	want := []byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // seq，大端
		0x00, 0x00, 0x00, 0x02, // count，大端
		byte(key.TypeValue), 0x02, 'a', 'b', 0x02, 'c', 'd',
		byte(key.TypeDeletion), 0x01, 'e',
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Encode() = % x\nwant       % x", got, want)
	}

	// EncodeTo 必须与 Encode 得到同样的字节，只是省掉一次分配。
	prefix := []byte("head")
	if out := b.EncodeTo(append([]byte(nil), prefix...)); !bytes.Equal(out, append(prefix, want...)) {
		t.Fatalf("EncodeTo() 追加结果不对: % x", out)
	}
}

func TestWriteBatchDecodeRoundTrip(t *testing.T) {
	b := NewWriteBatch()
	b.SetSequence(42)
	if err := b.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put([]byte("k2"), nil); err != nil { // 空 value
		t.Fatal(err)
	}
	if err := b.Delete([]byte("k3")); err != nil {
		t.Fatal(err)
	}

	decoded, err := decodeBatch(b.Encode())
	if err != nil {
		t.Fatalf("decodeBatch failed: %v", err)
	}
	if decoded.Len() != 3 || decoded.Sequence() != 42 {
		t.Fatalf("解码结果 = (%d 条, seq %d), want (3, 42)", decoded.Len(), decoded.Sequence())
	}

	type rec struct {
		seq  uint64
		kind key.Kind
		k, v string
	}
	var got []rec
	if err := decoded.rangeRecords(decoded.Sequence(), func(seq uint64, kind key.Kind, k, v []byte) bool {
		got = append(got, rec{seq, kind, string(k), string(v)})
		return true
	}); err != nil {
		t.Fatalf("rangeRecords failed: %v", err)
	}
	want := []rec{
		{42, key.TypeValue, "k1", "v1"},
		{43, key.TypeValue, "k2", ""},
		{44, key.TypeDeletion, "k3", ""},
	}
	if len(got) != len(want) {
		t.Fatalf("记录数 = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("记录 #%d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// Range 只读不写：遍历过程中批次必须保持可用。
func TestWriteBatchRangeDoesNotMutate(t *testing.T) {
	b := NewWriteBatch()
	b.SetSequence(1)
	for i := 0; i < 3; i++ {
		if err := b.Put([]byte{'a' + byte(i)}, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	count := func() int {
		n := 0
		if err := b.rangeRecords(1, func(uint64, key.Kind, []byte, []byte) bool { n++; return true }); err != nil {
			t.Fatalf("rangeRecords failed: %v", err)
		}
		return n
	}
	if a, c := count(), count(); a != 3 || c != 3 {
		t.Fatalf("两次遍历结果 = %d, %d; want 3, 3", a, c)
	}
	if b.Len() != 3 {
		t.Fatalf("遍历之后 Len() = %d, want 3", b.Len())
	}
}

func TestWriteBatchRangeEarlyStop(t *testing.T) {
	b := NewWriteBatch()
	for i := 0; i < 5; i++ {
		if err := b.Put([]byte{byte('a' + i)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	if err := b.rangeRecords(1, func(uint64, key.Kind, []byte, []byte) bool { n++; return false }); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("提前终止生效前遍历了 %d 条, want 1", n)
	}
}

func TestWriteBatchReset(t *testing.T) {
	b := NewWriteBatch()
	if err := b.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	capacity := cap(b.data)
	b.Reset()
	if b.Len() != 0 || b.Sequence() != 0 {
		t.Fatalf("Reset 之后 = (%d 条, seq %d), want (0, 0)", b.Len(), b.Sequence())
	}
	if cap(b.data) != capacity {
		t.Errorf("Reset 丢掉了底层缓冲: cap %d -> %d", capacity, cap(b.data))
	}
	if err := b.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if b.Len() != 1 {
		t.Fatalf("复用之后 Len() = %d, want 1", b.Len())
	}
}

func TestWriteBatchRejectsEmptyKey(t *testing.T) {
	b := NewWriteBatch()
	if err := b.Put(nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Put(nil) error = %v, want ErrEmptyKey", err)
	}
	if err := b.Delete([]byte{}); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Delete(empty) error = %v, want ErrEmptyKey", err)
	}
	if b.Len() != 0 {
		t.Fatalf("被拒绝的记录不应计入 Len(): %d", b.Len())
	}
}

// 日志里的字节不可信：头部与记录区不自洽时必须报错，而不是越界读。
func TestDecodeBatchRejectsCorruptInput(t *testing.T) {
	valid := NewWriteBatch()
	valid.SetSequence(1)
	if err := valid.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	rep := valid.Encode()

	tests := []struct {
		name string
		data []byte
	}{
		{"太短", rep[:batchHeaderLen-1]},
		{"记录区被截断", rep[:len(rep)-2]},
		{"记录之后有多余字节", append(append([]byte(nil), rep...), 0x00)},
		{"类型非法", func() []byte {
			d := append([]byte(nil), rep...)
			d[batchHeaderLen] = 9
			return d
		}()},
		{"key 长度越界", func() []byte {
			d := append([]byte(nil), rep...)
			d[batchHeaderLen+1] = 0x7f
			return d
		}()},
		{"count 大于实际记录数", func() []byte {
			d := append([]byte(nil), rep...)
			d[11] = 5 // count = 5
			return d
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeBatch(tt.data); !errors.Is(err, ErrBatchCorrupt) {
				t.Fatalf("decodeBatch error = %v, want ErrBatchCorrupt", err)
			}
		})
	}
}
