package compress

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
)

// allTypes 是被测的全部算法。新增算法时只要往这里加一项，
// 下面所有往返用例都会自动覆盖到它。
var allTypes = []Type{TypeNone, TypeSnappy, TypeZlib}

// TestRoundTrip 验证每种算法都能"压了能解回原样"，并覆盖几个边界形态。
func TestRoundTrip(t *testing.T) {
	cases := map[string][]byte{
		"empty":       {},
		"one byte":    {0x42},
		"short":       []byte("hello"),
		"prefix":      bytes.Repeat([]byte("kvdb-block-"), 200),
		"high entro":  randBytes(4096, 1),
		"binary zero": make([]byte, 8192),
	}
	for _, typ := range allTypes {
		c, err := ByType(typ)
		if err != nil {
			t.Fatalf("ByType(%v): %v", typ, err)
		}
		for name, src := range cases {
			comp := c.Compress(nil, src)
			got, err := c.Decompress(nil, comp)
			if err != nil {
				t.Fatalf("%s/%s: decompress: %v", typ, name, err)
			}
			if !bytes.Equal(got, src) {
				t.Fatalf("%s/%s: round trip mismatch (%d bytes in, %d out)", typ, name, len(src), len(got))
			}
			if c.Type() != typ {
				t.Fatalf("%s: Type() = %v", typ, c.Type())
			}
		}
	}
}

// TestSnappyAndZlibActuallyCompress 确认这两个算法不是"原样返回"的伪装：
// 对这种高度重复的输入必须有明显收益，否则压测报告里的"压缩比"就无从谈起。
func TestSnappyAndZlibActuallyCompress(t *testing.T) {
	src := bytes.Repeat([]byte("value-0000000000"), 400) // 6.8KB 的重复内容
	for _, typ := range []Type{TypeSnappy, TypeZlib} {
		c, _ := ByType(typ)
		comp := c.Compress(nil, src)
		if len(comp) >= len(src)/2 {
			t.Errorf("%s: 压缩后 %d 字节，原文 %d 字节，收益过低", typ, len(comp), len(src))
		}
	}
}

// TestDecompressReusesBuffer 验证 Decompress 允许复用调用方给的缓冲区：
// 读路径每次解压都分配一块新内存会明显推高 GC 压力。
func TestDecompressReusesBuffer(t *testing.T) {
	src := bytes.Repeat([]byte("reuse-me"), 100)
	for _, typ := range allTypes {
		c, _ := ByType(typ)
		comp := c.Compress(nil, src)
		buf := make([]byte, 0, len(src)*2)
		got, err := c.Decompress(buf, comp)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, src) {
			t.Fatalf("%v: round trip mismatch", typ)
		}
	}
}

// TestDecompressRejectsCorruption 验证损坏输入被拒绝而不是被静默当成原文。
//
// 这条很重要：读路径是先校验 CRC 再解压的，所以走到这里的一定是"类型字节写错"
// 或"CRC 恰好对上"之类的极端情形，此时报错远比返回乱码安全。
func TestDecompressRejectsCorruption(t *testing.T) {
	src := bytes.Repeat([]byte("0123456789abcdef"), 64)
	for _, typ := range allTypes {
		c, _ := ByType(typ)
		comp := c.Compress(nil, src)

		// 截断到一半。
		if len(comp) > 2 {
			if _, err := c.Decompress(nil, comp[:len(comp)/2]); err != nil {
				continue // 报错是预期结果
			}
			if typ != TypeNone {
				t.Errorf("%v: 截断的压缩流没有被拒绝", typ)
			}
		}
	}
}

// TestDecompressRejectsOversizedClaim 验证"声称超大长度"的输入不会导致巨额分配。
//
// snappy 的流头自带原始长度，照着它分配内存就能让 5 个字节的输入申请 4GB ——
// 这正是 decompressedLimit 存在的理由。
func TestDecompressRejectsOversizedClaim(t *testing.T) {
	sn, _ := ByType(TypeSnappy)
	// 手工拼一个合法的 snappy 流头：声称原始长度是 3GB，接着 4 个零字节的负载。
	var src []byte
	src = appendUvarint(src, 3<<30)
	src = append(src, 0, 0, 0, 0)

	if _, err := sn.Decompress(nil, src); err == nil {
		t.Fatal("snappy: 声称 3GB 的块应当被拒绝")
	} else if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("snappy: 错误应当是 ErrCorrupt，实际 %v", err)
	}
}

// TestByTypeUnknown 验证未知类型返回错误而不是回退到不压缩。
//
// 回退是最危险的选择：那会把一段压缩流当原文交给上层解析，
// 得到的是"看起来合法"的乱码 —— 静默的数据损坏。
func TestByTypeUnknown(t *testing.T) {
	for _, typ := range []Type{3, 7, 200, 255} {
		if _, err := ByType(typ); err == nil {
			t.Errorf("ByType(%d): 期望报错", typ)
		} else if !errors.Is(err, ErrCorrupt) {
			t.Errorf("ByType(%d): 错误应当是 ErrCorrupt，实际 %v", typ, err)
		}
	}
}

// TestByName 覆盖配置解析：名字 → 类型必须与落盘的类型字节一致。
func TestByName(t *testing.T) {
	cases := map[string]Type{
		"":       TypeSnappy,
		"none":   TypeNone,
		"None":   TypeNone,
		"snappy": TypeSnappy,
		"SNAPPY": TypeSnappy,
		"zlib":   TypeZlib,
		"flate":  TypeZlib,
	}
	for name, want := range cases {
		c, err := ByName(name)
		if err != nil {
			t.Fatalf("ByName(%q): %v", name, err)
		}
		if c.Type() != want {
			t.Errorf("ByName(%q) = %v, want %v", name, c.Type(), want)
		}
	}
	if _, err := ByName("lz4"); err == nil {
		t.Error("ByName(\"lz4\") 应当报错（未实装）")
	}
}

// TestTypeNameAndString 锁定日志里出现的名字，避免报告与日志对不上。
func TestTypeNameAndString(t *testing.T) {
	for typ, want := range map[Type]string{
		TypeNone: "none", TypeSnappy: "snappy", TypeZlib: "zlib", Type(9): "unknown(9)",
	} {
		if got := typ.Name(); got != want {
			t.Errorf("Type(%d).Name() = %q, want %q", byte(typ), got, want)
		}
		if got := typ.String(); got != want {
			t.Errorf("Type(%d).String() = %q, want %q", byte(typ), got, want)
		}
	}
}

// TestNamesCoversRegistry 防止"加了算法却忘了更新帮助信息"。
func TestNamesCoversRegistry(t *testing.T) {
	for _, name := range Names() {
		c, err := ByName(name)
		if err != nil {
			t.Fatalf("Names() 里的 %q 解不出来: %v", name, err)
		}
		if _, ok := byType[c.Type()]; !ok {
			t.Errorf("%q 映射到 %v，但它不在路由表里", name, c.Type())
		}
	}
}

// TestConcurrentCompress 验证压缩器可以并发共用。
//
// 后台 Flush 与后台 Compaction 会同时写各自的文件，共用同一个压缩器实例；
// zlib 的编码器状态不能共享，所以它必须走池 —— 这条用例就是来抓那个池写错的。
func TestConcurrentCompress(t *testing.T) {
	for _, typ := range []Type{TypeSnappy, TypeZlib} {
		c, _ := ByType(typ)
		var wg sync.WaitGroup
		errs := make(chan error, 32)
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(int64(g)))
				for i := 0; i < 200; i++ {
					src := randBytes(1<<10, rnd.Int63())
					comp := c.Compress(nil, src)
					got, err := c.Decompress(nil, comp)
					if err != nil {
						errs <- err
						return
					}
					if !bytes.Equal(got, src) {
						errs <- fmt.Errorf("goroutine %d: round trip mismatch at %d", g, i)
						return
					}
				}
			}(g)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("%v: %v", typ, err)
		}
	}
}

// TestDecompressedLimitIsSane 把上限当常量锁定，提醒改动它的人注意影响面。
func TestDecompressedLimitIsSane(t *testing.T) {
	if decompressedLimit < 64<<20 {
		t.Fatalf("decompressedLimit = %d，对正常的大块来说太小了", decompressedLimit)
	}
}

// ── 小工具 ────────────────────────────────────────────────────────

func randBytes(n int, seed int64) []byte {
	rnd := rand.New(rand.NewSource(seed))
	out := make([]byte, n)
	rnd.Read(out)
	return out
}

// appendUvarint 手写一份 uvarint，避免为了造假输入去 import 本项目的 key 包。
func appendUvarint(dst []byte, x int) []byte {
	for x >= 0x80 {
		dst = append(dst, byte(x)|0x80)
		x >>= 7
	}
	return append(dst, byte(x))
}

// TestNoCompressKeepsBytesVerbatim 确认 TypeNone 是逐字节的恒等映射。
//
// 它是"压缩不划算就原样存"这条策略的兜底，必须绝对安全。
func TestNoCompressKeepsBytesVerbatim(t *testing.T) {
	c, _ := ByType(TypeNone)
	src := randBytes(1024, 7)
	comp := c.Compress(nil, src)
	if !bytes.Equal(comp, src) {
		t.Fatal("TypeNone 的 Compress 不是恒等映射")
	}
	got, err := c.Decompress(nil, comp)
	if err != nil || !bytes.Equal(got, src) {
		t.Fatalf("TypeNone 的解压结果不对: %v", err)
	}
	if strings.Contains(TypeSnappy.Name(), "unknown") {
		t.Fatal("类型名不该落在 unknown 分支")
	}
}
