package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/xiatianliang1024gm/kvdb/internal/cache"
	"github.com/xiatianliang1024gm/kvdb/internal/compress"
	"github.com/xiatianliang1024gm/kvdb/internal/crc"
	"github.com/xiatianliang1024gm/kvdb/internal/filter"
	"github.com/xiatianliang1024gm/kvdb/internal/key"
)

// OpenOptions 收集打开一个 SSTable 时需要的读取侧依赖。
type OpenOptions struct {
	// Comparer 必须与写入时使用的一致，否则块内的顺序假设不成立。
	Comparer key.Comparer
	// Cache 是块缓存，nil 表示不缓存（每读一块就分配一次）。
	Cache *cache.Cache
	// FileNum 参与块缓存的键：同一个文件必须始终用同一个编号打开。
	//
	// 文件编号在本引擎里单调递增且永不复用，所以 (编号, 块偏移) 能唯一标识一块内容，
	// 缓存条目不需要引用计数，也不需要担心"文件被删了但缓存里还有旧数据"。
	FileNum uint64
	// BlockStats 非 nil 时累计解压次数；多个 Reader 可以共享同一份计数。
	//
	// 这里**没有**"压缩开关"：读路径永远按块尾的类型字节路由到对应算法，
	// 与打开时的配置无关。否则"把 Compression 改成 none 再打开老目录"就会让
	// 所有压缩块变成不可读 —— 配置项的改动不该能损坏既有数据。
	BlockStats *BlockStats
}

// Reader 读取一个 SSTable。
//
// 它在打开时就把 Index Block 与 Filter Block 读进内存（两者都很小，通常各几 KB），
// 之后每个 blockIter 都在同一份只读字节上构造，因此 Reader 可以被多个读协程并发使用。
//
// 返回的 value 可能直接指向块缓存里的字节，**调用方不得修改**；需要长期持有时自行复制。
type Reader struct {
	path    string
	f       *os.File
	size    int64
	fileNum uint64
	cache   *cache.Cache
	icmp    key.InternalComparer
	blocks  *BlockStats

	index     []byte
	numBlocks int
	filter    *filter.BlockReader

	// largestKey 是文件里最大的 internal key，在 validateIndex 顺势记下（索引项的
	// 最后一条就是它）。它让"打开一个文件就知道它的 key 上界"不必额外付出一次 IO。
	largestKey []byte
}

// Open 打开 path 处的 SSTable：校验 Footer、读入 Index 与 Filter、逐条校验索引项。
//
// 打开时会完整校验索引（每条索引项都能解码、handle 落在文件内、key 严格递增），
// 因此"索引损坏"在 Open 阶段就会暴露，而不是等到某次读才炸。
func Open(path string, o OpenOptions) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("github.com/xiatianliang1024gm/kvdb/sst: open %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("github.com/xiatianliang1024gm/kvdb/sst: stat %s: %w", path, err)
	}
	size := info.Size()
	if size < FooterLen {
		f.Close()
		return nil, fmt.Errorf("%w: %s is only %d bytes", ErrBadFooter, path, size)
	}
	var footer [FooterLen]byte
	if _, err := f.ReadAt(footer[:], size-FooterLen); err != nil {
		f.Close()
		return nil, fmt.Errorf("github.com/xiatianliang1024gm/kvdb/sst: read footer of %s: %w", path, err)
	}
	metaH, indexH, err := decodeFooter(footer[:])
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	r := &Reader{
		path:    path,
		f:       f,
		size:    size,
		fileNum: o.FileNum,
		cache:   o.Cache,
		icmp:    key.InternalComparer{User: o.Comparer},
		blocks:  o.BlockStats,
	}
	fail := func(err error) (*Reader, error) {
		f.Close()
		return nil, err
	}

	meta, err := r.readBlock(metaH)
	if err != nil {
		return fail(err)
	}
	index, err := r.readBlock(indexH)
	if err != nil {
		return fail(err)
	}
	n, err := r.validateIndex(index)
	if err != nil {
		return fail(err)
	}
	r.index, r.numBlocks = index, n

	// 过滤器是可选的（写入时 BloomBitsPerKey <= 0 就没有）：查不到就说明这个文件
	// 没建过滤器，读路径自动退化成"每块都读"。
	if fh, ok, err := findFilterHandle(meta); err != nil {
		return fail(err)
	} else if ok {
		fb, err := r.readBlock(fh)
		if err != nil {
			return fail(err)
		}
		r.filter = filter.NewBlockReader(fb)
	}
	return r, nil
}

// Path 返回文件路径。
func (r *Reader) Path() string { return r.path }

// Size 返回文件总大小（含 Footer）。
func (r *Reader) Size() int64 { return r.size }

// NumBlocks 返回 Data Block 的个数。
func (r *Reader) NumBlocks() int { return r.numBlocks }

// FilterEnabled 表示这个文件是否带 Bloom Filter。
func (r *Reader) FilterEnabled() bool { return r.filter != nil }

// Close 关闭文件句柄。
func (r *Reader) Close() error {
	if err := r.f.Close(); err != nil {
		return fmt.Errorf("github.com/xiatianliang1024gm/kvdb/sst: close %s: %w", r.path, err)
	}
	return nil
}

// findFilterHandle 在 MetaIndex 块里查找过滤器条目的 handle。
func findFilterHandle(meta []byte) (blockHandle, bool, error) {
	it, err := newBlockIter(meta, compareBytes)
	if err != nil {
		return blockHandle{}, false, fmt.Errorf("github.com/xiatianliang1024gm/kvdb/sst: invalid metaindex block: %w", err)
	}
	want := filterMetaKey()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if compareBytes(it.Key(), want) != 0 {
			continue
		}
		h, _, err := decodeBlockHandle(it.Value())
		if err != nil {
			return blockHandle{}, false, err
		}
		return h, true, nil
	}
	return blockHandle{}, false, it.Error()
}

// validateIndex 完整走一遍索引块，返回 Data Block 的个数。
func (r *Reader) validateIndex(index []byte) (int, error) {
	it, err := newBlockIter(index, r.icmp.Compare)
	if err != nil {
		return 0, fmt.Errorf("%w: %s: invalid index block: %v", ErrCorruptBlock, r.path, err)
	}
	n := 0
	var prev []byte
	for it.SeekToFirst(); it.Valid(); it.Next() {
		h, _, err := decodeBlockHandle(it.Value())
		if err != nil {
			return 0, fmt.Errorf("%s: index entry %d: %w", r.path, n, err)
		}
		if err := r.checkHandle(h); err != nil {
			return 0, fmt.Errorf("%s: index entry %d: %w", r.path, n, err)
		}
		if prev != nil && r.icmp.Compare(prev, it.Key()) >= 0 {
			return 0, fmt.Errorf("%w: %s: index keys are not strictly increasing at entry %d", ErrCorruptBlock, r.path, n)
		}
		prev = append(prev[:0], it.Key()...)
		n++
	}
	if err := it.Error(); err != nil {
		return 0, fmt.Errorf("%s: %w", r.path, err)
	}
	// 索引项的 key 是"数据块的最大 key"，所以最后一条索引项就是整个文件的最大 key。
	r.largestKey = append(r.largestKey[:0], prev...)
	return n, nil
}

// LargestKey 返回文件里最大的 internal key；空文件返回 nil。
//
// 它只在"迁移旧目录"时被用到：那种场合下文件的 key 区间必须现算，
// 因为当时的版本元数据还不存在。正常路径上路过的每个文件，区间都由 Manifest 记着。
func (r *Reader) LargestKey() []byte { return r.largestKey }

// SmallestKey 返回文件里最小的 internal key；空文件返回 nil。
//
// 索引项记的是每块的最大 key，所以第一个数据块的最大 key 并不是文件的下界，
// 必须真的把第一块读进来取它的首条记录。这次 IO 由调用方按需触发，
// 不放进 Open —— 正常打开一个文件不该多看一个数据块。
func (r *Reader) SmallestKey() ([]byte, error) {
	if r.numBlocks == 0 {
		return nil, nil
	}
	it, err := newBlockIter(r.index, r.icmp.Compare)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", r.path, err)
	}
	it.SeekToFirst()
	if !it.Valid() {
		return nil, it.Error()
	}
	h, _, err := decodeBlockHandle(it.Value())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", r.path, err)
	}
	block, err := r.readBlock(h)
	if err != nil {
		return nil, err
	}
	bi, err := newBlockIter(block, r.icmp.Compare)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", r.path, err)
	}
	bi.SeekToFirst()
	if !bi.Valid() {
		return nil, bi.Error()
	}
	return append([]byte(nil), bi.Key()...), nil
}

// checkHandle 校验块的位置确实落在数据区之内。
//
// 索引损坏时 handle 可能指向文件外或声称一个巨大的长度，直接按它分配内存
// 会变成一次 OOM。读块之前先做边界检查，把这类损坏变成明确的错误。
func (r *Reader) checkHandle(h blockHandle) error {
	if h.size == 0 {
		return fmt.Errorf("%w: block at offset %d is empty", ErrCorruptBlock, h.offset)
	}
	if h.offset+h.size+BlockTrailerLen > uint64(r.size-FooterLen) {
		return fmt.Errorf("%w: block [%d, %d) is outside the data area (%d bytes)", ErrCorruptBlock, h.offset, h.offset+h.size, r.size-FooterLen)
	}
	return nil
}

// readBlock 读入一个块，校验 CRC，必要时解压。命中缓存时直接返回缓存里的字节。
//
// 三步的顺序不能换：
//
//  1. **先校验 CRC**，而且校验的是从磁盘读回来的那份字节（压缩形态）。
//     CRC 过了才说明"这串字节确实是我写进去的"，此时再解压就不会把损坏的
//     位流喂给解压器 —— 那既可能报出误导性的错误，也可能放大成一次巨额内存分配。
//  2. **再解压**（未压缩块直接跳过）。
//  3. **缓存解压后的内容**。这一点很关键：缓存里存压缩流的话，每一次命中
//     都要重新解压一遍，块缓存就只省下了磁盘 IO、省不掉 CPU。存解压后的字节，
//     命中即可以直接用。
//
// 代价是缓存按解压后的大小计费 —— 这是对的，因为内存占用本来就是按解压后的算。
func (r *Reader) readBlock(h blockHandle) ([]byte, error) {
	if b, ok := r.cache.Get(r.fileNum, h.offset); ok {
		return b, nil
	}
	if err := r.checkHandle(h); err != nil {
		return nil, fmt.Errorf("%s: %w", r.path, err)
	}

	buf := make([]byte, h.size+BlockTrailerLen)
	if _, err := r.f.ReadAt(buf, int64(h.offset)); err != nil {
		return nil, fmt.Errorf("github.com/xiatianliang1024gm/kvdb/sst: read block at %d of %s: %w", h.offset, r.path, err)
	}
	trailer := buf[h.size:]
	ctype := compress.Type(trailer[0])
	if want := binary.BigEndian.Uint32(trailer[1:]); crc.ChecksumWithType(buf[:h.size], trailer[0]) != want {
		return nil, fmt.Errorf("%w: %s: block at offset %d failed the checksum", ErrCorruptBlock, r.path, h.offset)
	}

	data := buf[:h.size]
	if ctype != compress.Type(blockCompressionNone) {
		c, err := compress.ByType(ctype)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: block at offset %d: %v", ErrUnsupportedCompression, r.path, h.offset, err)
		}
		out, err := c.Decompress(nil, data)
		if err != nil {
			// 到这里说明块的 CRC 是对的、但内容不是合法的压缩流。
			// 归到 ErrCorruptBlock：对上层来说它与"块坏了"是同一件事。
			return nil, fmt.Errorf("%w: %s: block at offset %d: %v", ErrCorruptBlock, r.path, h.offset, err)
		}
		data = out
		if r.blocks != nil {
			r.blocks.Decompressions.Add(1)
			r.blocks.CompressedBytesRead.Add(uint64(h.size))
		}
	}

	r.cache.Put(r.fileNum, h.offset, data)
	return data, nil
}

// seekIndex 在索引块里定位"最后一个 key >= target"的那一块。
//
// 索引项存的是每块的最大 key，所以"第一个索引 key >= target"的那一块，
// 就是唯一可能含有 target 的块——若 target 存在，它一定落在这块里。
func (r *Reader) seekIndex(target []byte) (blockHandle, bool, error) {
	it, err := newBlockIter(r.index, r.icmp.Compare)
	if err != nil {
		return blockHandle{}, false, fmt.Errorf("%s: %w", r.path, err)
	}
	it.Seek(target)
	if !it.Valid() {
		return blockHandle{}, false, it.Error()
	}
	h, _, err := decodeBlockHandle(it.Value())
	if err != nil {
		return blockHandle{}, false, fmt.Errorf("%s: %w", r.path, err)
	}
	return h, true, nil
}

// Get 在 snapshot 序列号下查找 userKey。
//
// 返回的 value 与 kind 语义与 M1 一致：found 为 false 表示"这个文件里没有该 key
// 在 snapshot 下的可见版本"（可能是没有这个 key，也可能是它的所有版本都更新），
// 调用方应当继续查更旧的文件。
func (r *Reader) Get(snapshot uint64, userKey []byte) (value []byte, kind key.Kind, found bool, err error) {
	if r.numBlocks == 0 {
		return nil, 0, false, nil
	}
	// 定位用的 key 带 (snapshot, TypeValue) 尾缀：尾缀降序排列下，所有比快照更新的
	// 版本都排在它前面，因此 seek 的落点恰好是该快照下最新的可见版本。
	target := key.SeekKey(userKey, snapshot)

	h, ok, err := r.seekIndex(target)
	if err != nil || !ok {
		return nil, 0, false, err
	}
	// 先问 Bloom，再读块：这一步挡掉的就是"根本不存在这个 key"的无效 IO。
	if !r.filter.KeyMayMatch(h.offset, userKey) {
		return nil, 0, false, nil
	}

	block, err := r.readBlock(h)
	if err != nil {
		return nil, 0, false, err
	}
	it, err := newBlockIter(block, r.icmp.Compare)
	if err != nil {
		return nil, 0, false, fmt.Errorf("%s: %w", r.path, err)
	}
	it.Seek(target)
	if !it.Valid() {
		return nil, 0, false, it.Error()
	}
	// 落点换了 user key，说明目标 key 在这个文件里没有可见版本。
	if r.icmp.UserCompare(it.Key(), target) != 0 {
		return nil, 0, false, nil
	}
	return it.Value(), key.KindOf(it.Key()), true, nil
}

// Iterate 顺序遍历全部记录，fn 返回 false 时提前结束。
func (r *Reader) Iterate(fn func(internalKey, value []byte) bool) error {
	it := r.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if !fn(it.Key(), it.Value()) {
			return it.Error()
		}
	}
	return it.Error()
}

// Count 统计记录条数，主要供测试与诊断使用。
func (r *Reader) Count() (int, error) {
	n := 0
	err := r.Iterate(func(_, _ []byte) bool { n++; return true })
	return n, err
}

// compareBytes 是按字节比较，用于 MetaIndex 这类 key 不是 internal key 的块
// （它的条目是 "filter.kvdb.BloomFilter" 这样的普通字符串）。
func compareBytes(a, b []byte) int { return bytes.Compare(a, b) }
