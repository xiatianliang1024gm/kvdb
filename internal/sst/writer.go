// Package sst 实现 SSTable 的读写。
//
// M2 的文件结构是完整的五段式（见 format.go 的示意图）：Data Block 多个、
// Filter Block、MetaIndex Block、Index Block、定长 Footer。一次点查的链路是：
//
//	读 Footer → 读 Index（打开文件时常驻内存）→ 二分 Index 定位 Data Block
//	→ 查 Filter 判断 key 是否可能存在 → 命中则读块（先查 Block Cache）→ 块内二分
//
// M5 给每个块加上了可选的压缩（见 internal/compress）。它**没有改文件格式**：
// 块尾那 1 个字节的"压缩类型"从 M2 起就在那里（当时恒为 0），
// 于是 M2 写出的文件照样能读，压缩也只是"把这一字节从 0 换成 1 或 2"。
//
// 块内的查找依赖一个关键约定（见 Writer.Add 的注释）：
// **同一个 user key 的所有版本必须落在同一个 Data Block 里**。
// 有了它，"按索引定位到唯一一块、块内 seek 一次"就足以得到正确结果，
// 不需要跨块回溯。
package sst

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"kvdb/internal/compress"
	"kvdb/internal/crc"
	"kvdb/internal/filter"
	"kvdb/internal/key"
)

// WriterOptions 是写 SSTable 时的可选参数。
type WriterOptions struct {
	// BlockSize 是 Data Block 的目标大小（字节）；<= 0 时用 DefaultBlockSize。
	BlockSize int
	// BloomBitsPerKey 是 Bloom Filter 每个 key 占用的位数；<= 0 表示不建过滤器。
	//
	// 建过滤器会让文件变大（10 位/key 约 1.25 字节/key），换来的是读路径上
	// 大量"本来要读块、结果发现 key 不在"的无效 IO 被挡掉。
	BloomBitsPerKey int
	// Compression 是块级压缩算法；零值 compress.TypeNone 表示不压缩。
	//
	// 它只影响**写入**：读取时会按块尾的类型字节路由到对应算法，
	// 因此"关掉压缩"不会让已有的压缩文件变成不可读。
	Compression compress.Type
	// BlockStats 非 nil 时累计块压缩的规模；多个 Writer 可以共享同一份。
	BlockStats *BlockStats
}

// Writer 把按 internal key 严格升序排列的记录写成一个 SSTable。
//
// 调用方必须保证 key 递增：索引、二分与块内重启点全都建立在这个前提上。
type Writer struct {
	path string
	f    *os.File
	buf  *bufio.Writer
	opts WriterOptions
	icmp key.InternalComparer

	// compressor 由 opts.Compression 解析而来，nil 表示不压缩。
	compressor compress.Compressor

	offset int64 // 已写入的字节数（含每块的 5 字节尾部）

	data   *blockBuilder
	index  *blockBuilder
	filter *filter.BlockBuilder // nil 表示不建过滤器

	lastKey []byte // 已写入的最后一条 internal key，用于校验有序性与登记索引
	count   int

	scratch []byte
	// compressed 是压缩输出的复用缓冲，避免每个块都新分配一块内存。
	compressed []byte

	finished bool
}

// NewWriter 创建（或覆盖）path 处的 SSTable。
func NewWriter(path string, cmp key.Comparer, opts WriterOptions) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("kvdb/sst: create %s: %w", path, err)
	}
	if opts.BlockSize <= 0 {
		opts.BlockSize = DefaultBlockSize
	}
	w := &Writer{
		path:  path,
		f:     f,
		buf:   bufio.NewWriterSize(f, 64<<10),
		opts:  opts,
		icmp:  key.InternalComparer{User: cmp},
		data:  newBlockBuilder(restartInterval),
		index: newBlockBuilder(indexRestartInterval),
	}
	if opts.Compression != compress.TypeNone {
		c, cerr := compress.ByType(opts.Compression)
		if cerr != nil {
			f.Close()
			os.Remove(path)
			return nil, cerr
		}
		w.compressor = c
	}
	if opts.BloomBitsPerKey > 0 {
		w.filter = filter.NewBlockBuilder(opts.BloomBitsPerKey)
	}
	return w, nil
}

// Add 追加一条记录。value 可以为空（墓碑）。
func (w *Writer) Add(ik, value []byte) error {
	if w.finished {
		return errors.New("kvdb/sst: add to a finished writer")
	}
	if _, _, _, err := key.DecodeInternalKey(ik); err != nil {
		return err
	}
	if w.count > 0 {
		if c := w.icmp.Compare(ik, w.lastKey); c <= 0 {
			return fmt.Errorf("kvdb/sst: keys must be added in strictly increasing order, got %s after %s",
				describeKey(ik), describeKey(w.lastKey))
		}
		// 切块的唯一时机：块已经够大，且下一条记录换了 user key。
		//
		// "换了 user key 才允许切"是刻意的：同一个 user key 的多个版本一旦被切开，
		// 快照读就有可能"定位到的那一块里只有更新/更旧的版本"而漏掉可见版本，
		// 读取方就得跨块回溯。用一点块大小的不均匀换掉一整类边界 bug，很划算。
		if w.data.sizeEstimate() >= w.opts.BlockSize && w.icmp.UserCompare(ik, w.lastKey) != 0 {
			if err := w.flushBlock(); err != nil {
				return err
			}
		}
	}

	if w.filter != nil {
		// 过滤器按 user key 建：seek key 的尾缀带快照序列号，与任何真实写入的
		// internal key 都不相同，若按 internal key 建会直接把可见版本漏判掉。
		w.filter.AddKey(key.UserKey(ik))
	}
	w.data.add(ik, value)
	w.lastKey = append(w.lastKey[:0], ik...)
	w.count++
	return nil
}

// Count 返回已写入的记录数。
func (w *Writer) Count() int { return w.count }

// Size 返回当前已写入的字节数。
func (w *Writer) Size() int64 { return w.offset }

// flushBlock 把当前数据块落盘并登记索引。
//
// 索引项用的是**这一块的最大 key**（也就是刚写进去的最后一条），不做"最短分隔符"
// 压缩。好处是写完一块就能立刻登记索引，不需要 LevelDB 那种"等下一个 key 来了
// 才能算出分隔符"的 pending 状态；代价是索引 key 略长，而索引块本来就只有几 KB。
func (w *Writer) flushBlock() error {
	if w.data.empty() {
		return nil
	}
	contents := w.data.finish()
	h, err := w.writeRawBlock(contents)
	if err != nil {
		return err
	}
	w.data.reset()

	w.index.add(w.lastKey, h.encode(w.scratch[:0]))
	if w.filter != nil {
		// 新块的起点偏移决定它的 key 归入哪个 Bloom 位图。
		w.filter.StartBlock(uint64(w.offset))
	}
	return nil
}

// writeRawBlock 写出"块内容 + 5 字节尾部"，返回块的 handle。
//
// 压缩就发生在这里，而且**每个块独立判断**：压缩后至少省下 1/8 才采用，
// 否则原样存储并把类型字节写回 0。这样做的收益是双向的 ——
// 写的时候不用把没收益的字节也压一遍，读的时候也不用为一个
// "压了反而更大" 的块付一次解压代价。
func (w *Writer) writeRawBlock(contents []byte) (blockHandle, error) {
	data, ctype := contents, compress.TypeNone
	if w.compressor != nil {
		// 复用出口缓冲：一个 4KB 的块压缩一次，如果每次都新分配一块内存，
		// 后台 Compaction 写几十万个块就是几十万次分配。
		//
		// 注意只有在**真的采用**压缩结果时才接管这块缓冲。反过来写的话，
		// "不划算"这条路径会把 w.compressed 指向调用方的 contents，
		// 而 contents 往往就是块构造器自己的复用缓冲 —— 下一次压缩就变成
		// 源和目标同一块内存，编码器会在读之前把它覆盖掉。
		out := w.compressor.Compress(w.compressed[:0], contents)
		if worthCompressing(len(contents), len(out)) {
			data, ctype = out, w.compressor.Type()
			w.compressed = out[:0]
		}
	}

	h := blockHandle{offset: uint64(w.offset), size: uint64(len(data))}

	var trailer [BlockTrailerLen]byte
	trailer[0] = byte(ctype)
	// 校验范围包含类型字节本身，且校验的是**落盘的那份字节**（压缩后的）：
	// 类型字段被翻转、或压缩流被改坏，都会在这里被发现。
	// 读路径因此可以放心地"先校验、再解压"。
	binary.BigEndian.PutUint32(trailer[1:], crc.ChecksumWithType(data, byte(ctype)))

	if _, err := w.buf.Write(data); err != nil {
		return blockHandle{}, fmt.Errorf("kvdb/sst: write block to %s: %w", w.path, err)
	}
	if _, err := w.buf.Write(trailer[:]); err != nil {
		return blockHandle{}, fmt.Errorf("kvdb/sst: write block trailer to %s: %w", w.path, err)
	}
	w.offset += int64(len(data)) + BlockTrailerLen

	if bs := w.opts.BlockStats; bs != nil {
		bs.BlocksWritten.Add(1)
		bs.RawBytesWritten.Add(uint64(len(contents)))
		bs.StoredBytesWritten.Add(uint64(len(data)))
		if ctype != compress.TypeNone {
			bs.CompressedWritten.Add(1)
		}
	}
	return h, nil
}

// worthCompressing 判断"压缩这笔买卖划不划算"。
//
// 判据是"至少省下 1/8"。它挡掉的主要是两类块：过滤器位图（接近随机分布，
// 压不动）与很小的索引块（压缩流的头部开销就占掉了收益）。
// 对这两类块强行压缩只会既让文件变大、又让每次读多付出一次解压。
func worthCompressing(raw, compressed int) bool {
	return compressed < raw-raw/compressionBreakEven
}

// Finish 写出 Filter / MetaIndex / Index 与 Footer，fsync 并关闭文件。返回后文件已可读。
func (w *Writer) Finish() error {
	if w.finished {
		return nil
	}
	w.finished = true

	if err := w.flushBlock(); err != nil {
		return err
	}

	var filterHandle blockHandle
	hasFilter := false
	if w.filter != nil {
		h, err := w.writeRawBlock(w.filter.Finish())
		if err != nil {
			return err
		}
		filterHandle, hasFilter = h, true
	}

	// MetaIndex 目前只放过滤器一项，但这一层保留了"按策略名查找"的扩展点：
	// 将来加 properties、压缩字典等元数据时不需要改文件结构。
	meta := newBlockBuilder(indexRestartInterval)
	if hasFilter {
		meta.add(filterMetaKey(), filterHandle.encode(w.scratch[:0]))
	}
	metaHandle, err := w.writeRawBlock(meta.finish())
	if err != nil {
		return err
	}

	indexHandle, err := w.writeRawBlock(w.index.finish())
	if err != nil {
		return err
	}

	footer := encodeFooter(metaHandle, indexHandle)
	if _, err := w.buf.Write(footer[:]); err != nil {
		return fmt.Errorf("kvdb/sst: write footer to %s: %w", w.path, err)
	}
	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("kvdb/sst: flush %s: %w", w.path, err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("kvdb/sst: sync %s: %w", w.path, err)
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("kvdb/sst: close %s: %w", w.path, err)
	}
	return nil
}

// Abandon 关闭并删除尚未写完的文件，用于中途失败时避免留下半截 SST。
func (w *Writer) Abandon() {
	w.finished = true
	_ = w.buf.Flush()
	_ = w.f.Close()
	_ = os.Remove(w.path)
}

// describeKey 把 internal key 渲染成 `user#seq,Kind` 形式，便于定位"哪条记录插错了"。
func describeKey(ik []byte) string {
	p, err := key.ParseInternalKey(ik)
	if err != nil {
		return fmt.Sprintf("%q", ik)
	}
	return p.String()
}
