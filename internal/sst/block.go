package sst

import (
	"encoding/binary"
	"fmt"

	"github.com/xiatianliang1024gm/kvdb/internal/key"
)

// Data Block 与 Index Block 用的是同一种块编码（LevelDB 的 block 格式）：
//
//	entry  = uvarint(shared) | uvarint(nonShared) | uvarint(valueLen) | key[shared:] | value
//	block  = entry * | restart 数组(fixed32 * N) | fixed32(N)
//
// 前缀压缩：每条记录的 key 只写"与上一条 key 的公共前缀之后"的部分，公共前缀长度放在
// shared 里。LSM 里的 key 常常是连续递增的（比如同一 user key 的多个版本只差最后几个
// 字节），这一层能省掉相当可观的体积。
//
// 代价是"块内不能随机访问第 i 条"——必须从上一条解出来。所以每隔 restartInterval 条
// 会强制写一条完整 key（shared = 0），这个位置叫重启点。二分查找先二分重启点，
// 再在最后一个重启点之后线性扫描，把"随机访问"和"前缀压缩"这两个目标调和起来。

// blockBuilder 构造一个块。写入的 key 必须严格递增（调用方负责）。
type blockBuilder struct {
	restartInterval int

	buf      []byte
	restarts []uint32
	counter  int    // 距下一个重启点还有多少条
	lastKey  []byte // 上一条完整 key，用于算公共前缀
}

// newBlockBuilder 创建一个块构造器。restartInterval 必须 ≥ 1。
func newBlockBuilder(restartInterval int) *blockBuilder {
	b := &blockBuilder{restartInterval: restartInterval}
	b.reset()
	return b
}

// reset 清空内容，准备构造下一个块。
func (b *blockBuilder) reset() {
	b.buf = b.buf[:0]
	b.restarts = append(b.restarts[:0], 0) // 第一个重启点固定在偏移 0
	b.counter = 0
	b.lastKey = b.lastKey[:0]
}

// empty 表示还没有写入任何记录。
func (b *blockBuilder) empty() bool { return len(b.buf) == 0 }

// sizeEstimate 估算当前块大小（含重启数组），用于判断是否该切块。
func (b *blockBuilder) sizeEstimate() int { return len(b.buf) + len(b.restarts)*key.Fixed32Len }

// add 追加一条记录。key 只在本次调用内被读取。
func (b *blockBuilder) add(k, value []byte) {
	shared := 0
	if b.counter < b.restartInterval {
		n := len(b.lastKey)
		if len(k) < n {
			n = len(k)
		}
		for shared < n && b.lastKey[shared] == k[shared] {
			shared++
		}
	} else {
		// 到达间隔：本条成为新的重启点，写完整 key。
		b.restarts = append(b.restarts, uint32(len(b.buf)))
		b.counter = 0
	}

	b.buf = key.PutUvarint(b.buf, uint64(shared))
	b.buf = key.PutUvarint(b.buf, uint64(len(k)-shared))
	b.buf = key.PutUvarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, k[shared:]...)
	b.buf = append(b.buf, value...)

	// lastKey = shared 前缀 + 本次写入的后半段。append 与 copy 一样能正确处理
	// "源与目标重叠"的情况，所以即便 k 恰好指向 b.lastKey 也是安全的。
	b.lastKey = append(b.lastKey[:shared], k[shared:]...)
	b.counter++
}

// finish 追加重启数组并返回完整块内容（不含 5 字节块尾部）。
//
// 返回值与内部缓冲区共享内存，只在下一次 add / reset 之前有效。
func (b *blockBuilder) finish() []byte {
	for _, r := range b.restarts {
		b.buf = key.PutFixed32(b.buf, r)
	}
	return key.PutFixed32(b.buf, uint32(len(b.restarts)))
}

// blockIter 在一个块上做二分 + 前向扫描。
//
// 它持有"重建出来的当前 key"（前缀压缩使 key 不能直接切片得到），因此
// Key() 返回的切片在下一次移动之前有效。
//
// 同一个块可以被多个 blockIter 并发遍历：迭代状态全在迭代器自己身上，
// 块内容只读。
type blockIter struct {
	data     []byte // 整个块内容（不含块尾部）
	restarts []byte // 指向 data 尾部的重启数组
	entryEnd int    // 记录区结束位置 = 重启数组起点
	num      int    // 重启点个数

	nextOffset int // 下一条待解析记录的偏移
	keyBuf     []byte
	value      []byte
	valid      bool
	empty      bool // 空块：没有任何记录，任何定位都直接失效
	err        error

	cmp func(a, b []byte) int
}

// newBlockIter 在块内容上创建迭代器，cmp 决定 key 的比较方式。
//
// 数据块与索引块传 internal key 比较器；MetaIndex 块的 key 不是 internal key，
// 传 bytes.Compare。
func newBlockIter(data []byte, cmp func(a, b []byte) int) (*blockIter, error) {
	it := &blockIter{data: data, cmp: cmp}
	if err := it.init(); err != nil {
		return nil, err
	}
	return it, nil
}

func (it *blockIter) init() error {
	if len(it.data) < key.Fixed32Len+1 {
		return fmt.Errorf("%w: block is only %d bytes", ErrCorruptBlock, len(it.data))
	}
	n := int(binary.BigEndian.Uint32(it.data[len(it.data)-key.Fixed32Len:]))
	if n < 1 {
		return fmt.Errorf("%w: restart count is %d", ErrCorruptBlock, n)
	}
	size := n*key.Fixed32Len + key.Fixed32Len
	if size > len(it.data) {
		return fmt.Errorf("%w: restart array of %d entries does not fit in %d bytes", ErrCorruptBlock, n, len(it.data))
	}
	it.num = n
	it.entryEnd = len(it.data) - size
	it.restarts = it.data[it.entryEnd : it.entryEnd+n*key.Fixed32Len]
	if it.entryEnd == 0 {
		// 空块（只有重启数组）是合法的：MetaIndex 在没建过滤器时就是空的，
		// 空文件也没有索引项。它只是"没有任何记录"，迭代器立刻失效即可。
		it.empty = true
	}
	// 第一个重启点必须在偏移 0，否则二分查找会从错误的位置开始。
	if first := binary.BigEndian.Uint32(it.restarts); first != 0 {
		return fmt.Errorf("%w: first restart point at %d, want 0", ErrCorruptBlock, first)
	}
	return nil
}

// restartOffset 返回第 i 个重启点在块内的偏移。
func (it *blockIter) restartOffset(i int) int {
	return int(binary.BigEndian.Uint32(it.restarts[i*key.Fixed32Len:]))
}

// fail 记录第一个错误并让迭代器失效。
func (it *blockIter) fail(format string, args ...any) {
	if it.err == nil {
		it.err = fmt.Errorf("%w: %s", ErrCorruptBlock, fmt.Sprintf(format, args...))
	}
	it.valid = false
}

// parseNextKey 从 nextOffset 解析下一条记录；解析不出来就失效（正常到块尾）。
func (it *blockIter) parseNextKey() {
	if it.err != nil {
		it.valid = false
		return
	}
	if it.nextOffset >= it.entryEnd {
		it.valid = false
		it.keyBuf = it.keyBuf[:0]
		return
	}
	s := it.data[it.nextOffset:]

	shared, n1, err := key.Uvarint(s)
	if err != nil {
		it.fail("entry at %d: %v", it.nextOffset, err)
		return
	}
	nonShared, n2, err := key.Uvarint(s[n1:])
	if err != nil {
		it.fail("entry at %d: %v", it.nextOffset, err)
		return
	}
	valueLen, n3, err := key.Uvarint(s[n1+n2:])
	if err != nil {
		it.fail("entry at %d: %v", it.nextOffset, err)
		return
	}
	header := n1 + n2 + n3
	need := header + int(nonShared) + int(valueLen)
	if need > len(s) || need > it.entryEnd-it.nextOffset {
		it.fail("entry at %d claims %d bytes but only %d left", it.nextOffset, need, it.entryEnd-it.nextOffset)
		return
	}
	if shared > uint64(len(it.keyBuf)) {
		it.fail("entry at %d reuses %d shared bytes but the previous key is only %d bytes", it.nextOffset, shared, len(it.keyBuf))
		return
	}

	it.keyBuf = append(it.keyBuf[:shared], s[header:header+int(nonShared)]...)
	it.value = s[header+int(nonShared) : need]
	it.nextOffset += need
	it.valid = true
}

// seekToRestartPoint 把迭代器定位到第 i 个重启点之前，下一步 parseNextKey 会读出它。
func (it *blockIter) seekToRestartPoint(i int) {
	if it.empty {
		it.valid = false
		return
	}
	off := it.restartOffset(i)
	if off < 0 || off >= it.entryEnd {
		it.fail("restart point %d points to offset %d, outside the entry area", i, off)
		return
	}
	it.nextOffset = off
	// 重启点的记录是完整 key（shared = 0），清空 keyBuf 让解析不依赖上一条。
	it.keyBuf = it.keyBuf[:0]
	it.valid = false
}

// SeekToFirst 定位到块内第一条记录。
func (it *blockIter) SeekToFirst() {
	if it.err != nil {
		return
	}
	it.seekToRestartPoint(0)
	it.parseNextKey()
}

// Seek 定位到第一个 key >= target 的记录；不存在时迭代器失效。
func (it *blockIter) Seek(target []byte) {
	if it.err != nil {
		return
	}
	// 二分找"最后一个 key < target 的重启点"，再从那里线性扫描。
	// 注意不能直接二分到第一个 >= target 的重启点：目标可能落在前一个重启点
	// 之后的若干条记录里。
	left, right := 0, it.num-1
	for left < right {
		mid := (left + right + 1) / 2
		midKey, err := it.restartKey(mid)
		if err != nil {
			it.fail("%v", err)
			return
		}
		if it.cmp(midKey, target) < 0 {
			left = mid
		} else {
			right = mid - 1
		}
	}
	it.seekToRestartPoint(left)
	if it.err != nil {
		return
	}
	for {
		it.parseNextKey()
		if !it.valid || it.cmp(it.keyBuf, target) >= 0 {
			return
		}
	}
}

// restartKey 读出第 i 个重启点的 key（重启点一定以完整 key 存放）。
func (it *blockIter) restartKey(i int) ([]byte, error) {
	off := it.restartOffset(i)
	if off < 0 || off >= it.entryEnd {
		return nil, fmt.Errorf("restart point %d points to offset %d, outside the entry area", i, off)
	}
	s := it.data[off:]
	shared, n1, err := key.Uvarint(s)
	if err != nil {
		return nil, err
	}
	if shared != 0 {
		return nil, fmt.Errorf("restart point %d does not start with a full key (shared = %d)", i, shared)
	}
	nonShared, n2, err := key.Uvarint(s[n1:])
	if err != nil {
		return nil, err
	}
	_, n3, err := key.Uvarint(s[n1+n2:])
	if err != nil {
		return nil, err
	}
	start := n1 + n2 + n3
	if start+int(nonShared) > it.entryEnd-off {
		return nil, fmt.Errorf("restart point %d claims a %d byte key, beyond the entry area", i, nonShared)
	}
	return s[start : start+int(nonShared)], nil
}

// Valid 表示迭代器是否指向一条有效记录。
func (it *blockIter) Valid() bool { return it.valid && it.err == nil }

// Key 返回当前记录的完整 key。
func (it *blockIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.keyBuf
}

// Value 返回当前记录的 value。
func (it *blockIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.value
}

// Next 前进一条记录。
func (it *blockIter) Next() {
	if !it.valid {
		return
	}
	it.parseNextKey()
}

// Error 返回块结构损坏的错误。
func (it *blockIter) Error() error { return it.err }
