// Package compress 提供 SSTable 数据块的**块级压缩**。
//
// 为什么压缩做在"块"这一层而不是整个文件：SSTable 的读取单位本来就是块
// （一次点查只读一个数据块），压缩只有落在读取单位上才能在读的时候省下 IO。
// 压缩整个文件反而会让"读一个 4KB 的块"变成"解压一个几十 MB 的文件"。
//
// 块尾（trailer）里那 1 个字节的"压缩类型"就是本包的路由表入口：
//
//	Data Block | 压缩类型(1B) | CRC32C(4B)
//
// 类型字节从 M2 起就留在格式里（当时恒为 0），所以 M5 引入压缩**不需要改文件格式**，
// M2 写出的文件（类型 0）也照样能读 —— 这是当初先把长度留出来的直接回报。
//
// 三个容易踩的点：
//
//  1. **CRC 校验的范围包含类型字节本身**，而且校验的是**落盘的那份字节**
//     （压缩后的），不是压缩前的原文。所以读路径必须先校验、再解压：
//     校验通过才敢相信"这串字节确实是我写进去的"，此时再解压就不会把损坏的
//     位流喂给解压器（那既可能报出误导性的错误，也可能放大成一次巨额内存分配）。
//  2. **压缩不划算时不压缩**。过滤器位图、小索引块压缩后往往更大，
//     所以每个块独立判断（见 sst 包里的 worthCompressing）。
//  3. **解压后的长度要设硬上限**。压缩流自己声称的长度可以是一个天文数字，
//     照着它分配内存等于把一次损坏变成一次 OOM。见 decompressedLimit。
package compress

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/golang/snappy"
)

// Type 是块尾那 1 个字节的取值，也是本包的路由键。
//
// **数值一旦写进磁盘就不能再改**：它必须与历史文件保持兼容。
type Type byte

const (
	// TypeNone 表示块原样存储。
	TypeNone Type = 0
	// TypeSnappy 是 Google Snappy：速度优先，压缩比一般，是默认算法。
	TypeSnappy Type = 1
	// TypeZlib 是 DEFLATE（stdlib compress/flate）：压缩比更好，速度更慢。
	TypeZlib Type = 2
)

// Name 返回类型的可读名称，用于日志与压测输出。
func (t Type) Name() string {
	switch t {
	case TypeNone:
		return "none"
	case TypeSnappy:
		return "snappy"
	case TypeZlib:
		return "zlib"
	default:
		return fmt.Sprintf("unknown(%d)", byte(t))
	}
}

// String 实现 fmt.Stringer。
func (t Type) String() string { return t.Name() }

// ErrCorrupt 表示压缩块无法解压。
//
// 它与 sst.ErrCorruptBlock 是同一类故障的不同环节：CRC 说明"字节变了"，
// 这个错误说明"字节没变，但它不是一个合法的压缩流"（例如换过算法却忘了改类型字节）。
var ErrCorrupt = errors.New("kvdb/compress: corrupt block")

// decompressedLimit 是单个块解压后的字节上限。
//
// 它的作用不是省内存，而是把"损坏或恶意构造的压缩流"变成一次明确的错误：
// snappy 的流头里带着原始长度，照着它 make([]byte) 就能让一个 5 字节的输入
// 申请 4GB。正常块是 4KB 级别，1GB 已经宽得不可能误伤。
const decompressedLimit = 1 << 30

// Compressor 是一种块级压缩算法。
//
// 实现必须是无状态的：同一个实例会被并发的 Writer 共用（后台 Flush 与
// 后台 Compaction 各写各的文件），内部需要缓冲的实现请自己用 sync.Pool 托管。
type Compressor interface {
	// Type 返回落盘时写进块尾的类型字节。
	Type() Type
	// Compress 把 src 压缩后追加到 dst 末尾。它只保证结果正确，
	// **不判断"划不划算"** —— 是否采用压缩结果由调用方决定。
	Compress(dst, src []byte) []byte
	// Decompress 解压，返回的切片可能复用 dst。
	//
	// 输入不合法时返回 ErrCorrupt 包裹的错误；解压结果超过 decompressedLimit
	// 时同样报错而不是继续分配内存。
	Decompress(dst, src []byte) ([]byte, error)
}

// noneCompressor 是"不压缩"的占位实现，让写路径不必到处判空。
type noneCompressor struct{}

func (noneCompressor) Type() Type { return TypeNone }

func (noneCompressor) Compress(dst, src []byte) []byte { return append(dst, src...) }

func (noneCompressor) Decompress(dst, src []byte) ([]byte, error) { return append(dst, src...), nil }

// snappyCompressor 直接用 golang/snappy。它是纯 Go、无依赖、无状态的。
type snappyCompressor struct{}

func (snappyCompressor) Type() Type { return TypeSnappy }

func (snappyCompressor) Compress(dst, src []byte) []byte { return snappy.Encode(dst, src) }

func (snappyCompressor) Decompress(dst, src []byte) ([]byte, error) {
	// 先读长度再分配：DecodedLen 只解析流头，不碰负载。
	n, err := snappy.DecodedLen(src)
	if err != nil {
		return nil, fmt.Errorf("%w: snappy length: %v", ErrCorrupt, err)
	}
	if n > decompressedLimit {
		return nil, fmt.Errorf("%w: snappy block claims %d bytes", ErrCorrupt, n)
	}
	out, err := snappy.Decode(dst, src)
	if err != nil {
		return nil, fmt.Errorf("%w: snappy decode: %v", ErrCorrupt, err)
	}
	return out, nil
}

// zlibCompressor 用 stdlib 的 DEFLATE。
//
// 之所以要池化 writer：flate 的编码器状态是几十 KB 量级，而一个块只有 4KB，
// 每块 NewWriter 一次的话，分配开销会比压缩本身还大。flate.Writer 支持 Reset，
// 所以池化是安全且划算的。
//
// 解压侧没法池化（flate.NewReader 没有 Reset），但解压只在缓存未命中时发生，
// 而且那一次本来就要读磁盘，多一次分配可以接受。
type zlibCompressor struct {
	level int
	pool  sync.Pool
}

// newZlibCompressor 构造一个带 writer 池的 DEFLATE 压缩器。
func newZlibCompressor(level int) *zlibCompressor {
	c := &zlibCompressor{level: level}
	c.pool.New = func() any {
		fw, err := flate.NewWriter(io.Discard, level)
		if err != nil {
			// level 来自本包内部的常量，不合法只可能是代码写错。
			panic(fmt.Sprintf("kvdb/compress: flate.NewWriter(%d): %v", level, err))
		}
		return fw
	}
	return c
}

func (c *zlibCompressor) Type() Type { return TypeZlib }

func (c *zlibCompressor) Compress(dst, src []byte) []byte {
	fw := c.pool.Get().(*flate.Writer)
	// sliceWriter 直接把字节追加到 dst 上，避免再引入一块中间缓冲。
	sw := sliceWriter{buf: dst}
	fw.Reset(&sw)
	if _, err := fw.Write(src); err != nil {
		// bytes.Buffer 语义的写入器不会失败，真出错说明实现有缺陷，
		// 此时宁可把这一块原样返回（TypeNone 的路径会兜住）也不要 panic。
		c.pool.Put(fw)
		return append(dst, src...)
	}
	if err := fw.Close(); err != nil {
		c.pool.Put(fw)
		return append(dst, src...)
	}
	c.pool.Put(fw)
	return sw.buf
}

func (c *zlibCompressor) Decompress(dst, src []byte) ([]byte, error) {
	fr := flate.NewReader(bytes.NewReader(src))
	defer fr.Close()

	// 多读一个字节：读出 limit+1 就说明越界了，不用真的把整个流解完。
	// 直接往 dst 的容量上追加，省掉一层中间缓冲。
	sw := sliceWriter{buf: dst[:0]}
	n, err := io.Copy(&sw, io.LimitReader(fr, decompressedLimit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: flate: %v", ErrCorrupt, err)
	}
	if n > decompressedLimit {
		return nil, fmt.Errorf("%w: flate block expands beyond %d bytes", ErrCorrupt, decompressedLimit)
	}
	return sw.buf, nil
}

// sliceWriter 是一个把字节追加到切片上的 io.Writer。
type sliceWriter struct{ buf []byte }

func (w *sliceWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// byType 是类型字节到实现的静态路由表。
//
// 表里每个值都是无状态的（zlib 的状态在它自己的池里），因此可以被并发共用。
var byType = map[Type]Compressor{
	TypeNone:   noneCompressor{},
	TypeSnappy: snappyCompressor{},
	TypeZlib:   newZlibCompressor(flate.BestSpeed),
}

// ByType 返回类型字节对应的压缩器。
//
// 遇到不认识的类型必须报错而不是回退到"不压缩"：那样会把一段压缩流当原文
// 交给上层，解析出来的是一堆看起来合法的乱码 —— 静默的数据损坏。
// 未知类型通常意味着"文件来自更新的版本"，用户需要知道这一点。
func ByType(t Type) (Compressor, error) {
	c, ok := byType[t]
	if !ok {
		return nil, fmt.Errorf("%w: unsupported block compression type %d", ErrCorrupt, byte(t))
	}
	return c, nil
}

// ByName 按名字取压缩器，供配置解析使用。
func ByName(name string) (Compressor, error) {
	switch name {
	case "none", "None", "NONE":
		return noneCompressor{}, nil
	case "snappy", "Snappy", "SNAPPY", "":
		return snappyCompressor{}, nil
	case "zlib", "Zlib", "ZLIB", "flate":
		return newZlibCompressor(flate.BestSpeed), nil
	default:
		return nil, fmt.Errorf("kvdb/compress: unknown compression %q (want none|snappy|zlib)", name)
	}
}

// Names 返回内置算法的名字，按"速度优先 → 压缩比优先"排列，供帮助信息使用。
func Names() []string { return []string{"none", "snappy", "zlib"} }
