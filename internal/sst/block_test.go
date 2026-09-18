package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// blockKeys 生成一组递增的块内 key。
func blockKeys(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte(fmt.Sprintf("key-%04d", i))
	}
	return out
}

// 前缀压缩必须真的生效：连续的递增 key 只应写"变化的那几个字节"。
func TestBlockBuilderPrefixCompression(t *testing.T) {
	keys := [][]byte{
		[]byte("prefix-0001"),
		[]byte("prefix-0002"),
		[]byte("prefix-0003"),
		[]byte("prefix-0004"),
	}
	b := newBlockBuilder(16)
	raw := 0
	for _, k := range keys {
		b.add(k, []byte("v"))
		raw += len(k) + 1
	}
	data := b.finish()
	// 4 条记录共享 9 字节前缀（"prefix-000"），所以整块应当明显小于原始 key 总和。
	if len(data) >= raw {
		t.Fatalf("块大小 %d 不小于原始 key 总和 %d，前缀压缩没有生效", len(data), raw)
	}

	// 解出来的 key/value 必须与写入的完全一致。
	it, err := newBlockIter(data, bytes.Compare)
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if !bytes.Equal(it.Key(), keys[i]) {
			t.Fatalf("第 %d 条 key = %q, want %q", i, it.Key(), keys[i])
		}
		if string(it.Value()) != "v" {
			t.Fatalf("第 %d 条 value = %q, want v", i, it.Value())
		}
		i++
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if i != len(keys) {
		t.Fatalf("块内记录数 = %d, want %d", i, len(keys))
	}
}

// 重启点间隔决定"每多少条写一次完整 key"：间隔越小块越大、随机定位越快。
func TestBlockBuilderRestartPoints(t *testing.T) {
	keys := blockKeys(5)
	b := newBlockBuilder(2)
	for _, k := range keys {
		b.add(k, nil)
	}
	it, err := newBlockIter(b.finish(), bytes.Compare)
	if err != nil {
		t.Fatal(err)
	}
	// 5 条、间隔 2 → 重启点在 0、2、4，共 3 个。
	if it.num != 3 {
		t.Fatalf("重启点数 = %d, want 3", it.num)
	}
	for i := 0; i < it.num; i++ {
		// 重启点必须是完整 key（shared == 0），否则二分找不到正确的落点。
		if _, err := it.restartKey(i); err != nil {
			t.Fatalf("重启点 %d 不是完整 key: %v", i, err)
		}
	}
	// 间隔为 1 时每条都是重启点。
	b1 := newBlockBuilder(1)
	for _, k := range keys {
		b1.add(k, nil)
	}
	it1, err := newBlockIter(b1.finish(), bytes.Compare)
	if err != nil {
		t.Fatal(err)
	}
	if it1.num != len(keys) {
		t.Fatalf("间隔 1 时重启点数 = %d, want %d", it1.num, len(keys))
	}
}

// Seek 必须落在第一个 >= target 的位置上，覆盖"命中重启点""落在重启点之间"
// "小于最小 key""大于最大 key"四种情况。
func TestBlockIterSeek(t *testing.T) {
	keys := blockKeys(37) // 37 条、间隔 4 → 大量目标落在重启点之间
	b := newBlockBuilder(4)
	for _, k := range keys {
		b.add(k, append([]byte("v:"), k...))
	}
	data := b.finish()

	cases := []struct {
		name   string
		target string
		want   string // "" 表示期望失效
	}{
		{"精确命中", "key-0000", "key-0000"},
		{"大于最小 key", "key-0000a", "key-0001"},
		{"命中重启点", "key-0004", "key-0004"},
		{"落在重启点之间", "key-0015a", "key-0016"},
		{"介于两条之间", "key-0010x", "key-0011"},
		{"小于最小 key", "", "key-0000"},
		{"大于最大 key", "zzz", ""},
		{"等于最大 key", "key-0036", "key-0036"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			it, err := newBlockIter(data, bytes.Compare)
			if err != nil {
				t.Fatal(err)
			}
			it.Seek([]byte(c.target))
			if c.want == "" {
				if it.Valid() {
					t.Fatalf("Seek(%q) 应当失效，却停在 %q", c.target, it.Key())
				}
				return
			}
			if !it.Valid() {
				t.Fatalf("Seek(%q) 失效了，want %q", c.target, c.want)
			}
			if string(it.Key()) != c.want {
				t.Fatalf("Seek(%q) 停在 %q, want %q", c.target, it.Key(), c.want)
			}
			if want := "v:" + c.want; string(it.Value()) != want {
				t.Fatalf("Seek(%q) 的 value = %q, want %q", c.target, it.Value(), want)
			}
		})
	}
}

// SeekToFirst + Next 必须把块内记录按序走完。
func TestBlockIterIterateAll(t *testing.T) {
	keys := blockKeys(20)
	b := newBlockBuilder(3)
	for _, k := range keys {
		b.add(k, nil)
	}
	it, err := newBlockIter(b.finish(), bytes.Compare)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(keys) {
		t.Fatalf("遍历出 %d 条, want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i] != string(keys[i]) {
			t.Fatalf("第 %d 条 = %q, want %q", i, got[i], keys[i])
		}
	}
}

// 结构损坏必须变成错误，而不是 panic、静默截断或死循环。
func TestBlockIterRejectsCorruption(t *testing.T) {
	good := func() []byte {
		b := newBlockBuilder(2)
		for _, k := range blockKeys(6) {
			b.add(k, []byte("v"))
		}
		return b.finish()
	}
	restartCountAt := func(d []byte) int { return len(d) - 4 }

	cases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"重启点数为 0", func(d []byte) []byte {
			binary.BigEndian.PutUint32(d[restartCountAt(d):], 0)
			return d
		}},
		{"重启点数过大", func(d []byte) []byte {
			binary.BigEndian.PutUint32(d[restartCountAt(d):], 1<<20)
			return d
		}},
		{"首个重启点不在 0", func(d []byte) []byte {
			n := int(binary.BigEndian.Uint32(d[restartCountAt(d):]))
			base := len(d) - (n*4 + 4)
			binary.BigEndian.PutUint32(d[base:], 5)
			return d
		}},
		{"记录头被写坏", func(d []byte) []byte {
			d[1] = 0xff // nonShared 变成多字节 varint，长度超出记录区
			return d
		}},
		{"被截断", func(d []byte) []byte { return d[:3] }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data := c.mutate(append([]byte(nil), good()...))

			detected := false
			it, err := newBlockIter(data, bytes.Compare)
			if err != nil {
				detected = true
			} else {
				// 两种驱动方式都要能发现损坏：从中间 Seek（会二分重启点）与从头遍历。
				it.Seek([]byte("key-0003"))
				detected = it.Error() != nil
				if !detected {
					for it.SeekToFirst(); it.Valid(); it.Next() {
					}
					detected = it.Error() != nil
				}
			}
			if !detected {
				t.Fatal("损坏的块没有报错")
			}
		})
	}
}

// 前缀压缩与重启点的字节布局必须可逐字节核对，便于对照 LevelDB 的 table_format 文档。
func TestBlockEntryLayout(t *testing.T) {
	b := newBlockBuilder(16)
	b.add([]byte("abcd"), []byte("v"))
	b.add([]byte("abce"), nil)
	data := b.finish()

	// 第一条：shared=0, nonShared=4, valueLen=1, "abcd", "v"
	if !bytes.Equal(data[0:3], []byte{0x00, 0x04, 0x01}) {
		t.Fatalf("第一条记录的头 = % x, want 00 04 01", data[0:3])
	}
	if string(data[3:8]) != "abcdv" {
		t.Fatalf("第一条记录的内容 = %q, want abcdv", data[3:8])
	}
	// 第二条：与上一条共享 3 字节前缀，只写 "e"，value 为空
	if !bytes.Equal(data[8:11], []byte{0x03, 0x01, 0x00}) {
		t.Fatalf("第二条记录的头 = % x, want 03 01 00", data[8:11])
	}
	if string(data[11:12]) != "e" {
		t.Fatalf("第二条记录只应写 1 字节 key 增量，实得 %q", data[11:12])
	}
	// 尾部：一个重启点（偏移 0） + 重启点个数 1
	if !bytes.Equal(data[len(data)-8:], []byte{0, 0, 0, 0, 0, 0, 0, 1}) {
		t.Fatalf("重启数组 = % x, want 00000000 00000001", data[len(data)-8:])
	}
}
