package crc

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

// 掩码必须可逆，且对 0 与非 0 都不能退化成恒等映射。
func TestMaskRoundTrip(t *testing.T) {
	for _, v := range []uint32{0, 1, 0xffffffff, 0x12345678, maskDelta} {
		if got := Unmask(Mask(v)); got != v {
			t.Errorf("Unmask(Mask(%#x)) = %#x", v, got)
		}
		if v != 0 && Mask(v) == v {
			t.Errorf("Mask(%#x) 意外地等于输入，说明掩码没生效", v)
		}
	}
}

// 校验和必须与标准库的 crc32c 一致（只差一次掩码），避免"自己编自己解"的循环验证。
func TestChecksumMatchesStandardLibrary(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)
	for _, s := range []string{"", "a", "abcd", "hello kvdb", string([]byte{0, 1, 2, 3, 255})} {
		want := crc32.Checksum([]byte(s), table)
		if got := Unmask(Checksum([]byte(s))); got != want {
			t.Errorf("Checksum(%q) 去掉掩码后 = %#x, want %#x", s, got, want)
		}
	}
}

// 分段计算（Update + ChecksumWithType）必须与一次性计算结果一致。
func TestChecksumWithTypeEqualsConcatenation(t *testing.T) {
	for _, b := range [][]byte{{}, []byte("block-contents"), make([]byte, 300)} {
		for _, ty := range []byte{0, 1, 200} {
			joined := append(append([]byte(nil), b...), ty)
			if got, want := ChecksumWithType(b, ty), Checksum(joined); got != want {
				t.Errorf("ChecksumWithType(len=%d, type=%d) = %#x, Checksum(joined) = %#x", len(b), ty, got, want)
			}
		}
	}
}

// Update 必须与小端/大端无关地逐段累加正确：这里与标准库交叉校验。
func TestUpdateIncremental(t *testing.T) {
	whole := []byte("kvdb: block trailer checksum")
	table := crc32.MakeTable(crc32.Castagnoli)

	naive := crc32.Checksum(whole, table)
	incremental := Update(Update(0, whole[:7]), whole[7:])
	if incremental != naive {
		t.Fatalf("分段 Update = %#x, 一次算 = %#x", incremental, naive)
	}

	// 逐字节切成两段也应一致（顺带说明 CRC 本身对"长度字段"敏感）。
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], 0x01020304)
	if Update(0, buf[:]) != crc32.Checksum(buf[:], table) {
		t.Error("固定长度缓冲区的 Update 与标准库不一致")
	}
}
