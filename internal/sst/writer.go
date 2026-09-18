// Package sst 实现 SSTable 的读写。
//
// M2 的文件结构是完整的五段式（见 format.go 的示意图）：Data Block 多个、
// Filter Block、MetaIndex Block、Index Block、定长 Footer。一次点查的链路是：
//
//	读 Footer → 读 Index（打开文件时常驻内存）→ 二分 Index 定位 Data Block
//	→ 查 Filter 判断 key 是否可能存在 → 命中则读块（先查 Block Cache）→ 块内二分
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

	offset int64 // 已写入的字节数（含每块的 5 字节尾部）

	data   *blockBuilder
	index  *blockBuilder
	filter *filter.BlockBuilder // nil 表示不建过滤器

	lastKey []byte // 已写入的最后一条 internal key，用于校验有序性与登记索引
	count   int

	scratch  []byte
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
func (w *Writer) writeRawBlock(contents []byte) (blockHandle, error) {
	h := blockHandle{offset: uint64(w.offset), size: uint64(len(contents))}

	var trailer [BlockTrailerLen]byte
	trailer[0] = blockCompressionNone
	// 校验范围包含类型字节本身：否则类型字段被翻转不会被发现。
	binary.BigEndian.PutUint32(trailer[1:], crc.ChecksumWithType(contents, blockCompressionNone))

	if _, err := w.buf.Write(contents); err != nil {
		return blockHandle{}, fmt.Errorf("kvdb/sst: write block to %s: %w", w.path, err)
	}
	if _, err := w.buf.Write(trailer[:]); err != nil {
		return blockHandle{}, fmt.Errorf("kvdb/sst: write block trailer to %s: %w", w.path, err)
	}
	w.offset += int64(len(contents)) + BlockTrailerLen
	return h, nil
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
