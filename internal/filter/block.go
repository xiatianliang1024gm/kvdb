package filter

import (
	"encoding/binary"

	"kvdb/internal/key"
)

// Filter Block 的字节布局（沿用 LevelDB 的设计，只把 fixed32 换成大端）：
//
//	┌────────────┬────────────┬─────┬──────────────────┬───────────────┬────────┐
//	│ 位图 0      │ 位图 1      │ ... │ 位图 N-1          │ 偏移数组       │ baseLg │
//	└────────────┴────────────┴─────┴──────────────────┴───────────────┴────────┘
//
// 偏移区：共 N+1 个 fixed32（N 个位图起点 + 数组自身起点的副本）。第 i 项是位图 i
// 的起点，第 i+1 项充当位图 i 的终点——最后一段的终点正好落在末尾那个副本上，
// 所以读取任意一段都不需要边界判断（LevelDB 的技巧）。数组起点由"倒数第 5 字节"
// 里的 fixed32 给出，最后一字节是 baseLg。
//
// 分段（region）的规则是：数据区里偏移落在 [i*2^baseLg, (i+1)*2^baseLg) 的
// 数据块，其 key 全部写进位图 i。这样一个位图覆盖 2KB 数据，而不是"一个块一个位图"，
// 位图数量与数据块的切分方式解耦，读块时用块偏移右移 baseLg 就能定位位图。
//
// 与 LevelDB 的唯一差异：所有 fixed32 用**大端**，与项目其余部分（varint 之外的
// 定长编码一律大端）保持一致。这是我们自己的文件格式，不对外交换。

// BlockBuilder 生成 Filter Block。
//
// 用法：每写完一个数据块调用 StartBlock(新块偏移)，每个 entry 调用 AddKey(userKey)，
// 全部写完后调用 Finish。AddKey 传的是 **user key**，不是 internal key：
// 过滤器只需要回答"这个 key 在不在这段数据里"，把 8 字节尾缀也哈希进去会白白
// 降低精度，而且快照读时构造的 seek key 的尾缀与任何真实写入都不相同，会直接漏判。
type BlockBuilder struct {
	bitsPerKey int

	keys    []byte // 平铺的 key 字节，避免每个 key 一次分配
	starts  []int  // 每个 key 在 keys 里的起点
	views   [][]byte
	result  []byte   // 已生成的位图
	offsets []uint32 // 每个位图的起点
}

// NewBlockBuilder 创建一个 bitsPerKey 位/key 的过滤器构造器。
func NewBlockBuilder(bitsPerKey int) *BlockBuilder {
	return &BlockBuilder{bitsPerKey: bitsPerKey}
}

// AddKey 记录一个 user key。它只缓冲，不立刻建位图。
func (b *BlockBuilder) AddKey(userKey []byte) {
	b.starts = append(b.starts, len(b.keys))
	b.keys = append(b.keys, userKey...)
}

// StartBlock 告知"从 blockOffset 起开始写入一个新的数据块"。
//
// 它会把此前缓冲的 key 归入当前位图，并按需补齐中间的位图（空缺的位图是空位图，
// 查询时直接返回"一定不存在"，这也是正确的：那一段偏移没有 key）。
func (b *BlockBuilder) StartBlock(blockOffset uint64) {
	index := int(blockOffset >> BaseLg)
	for index > len(b.offsets) {
		b.generate()
	}
}

// generate 结束当前位图，直到凑够 StartBlock 要求的数量。
func (b *BlockBuilder) generate() {
	if len(b.starts) == 0 {
		// 这一段没有任何 key：登记一个空位图（起点与终点相同）。
		b.offsets = append(b.offsets, uint32(len(b.result)))
		return
	}

	n := len(b.starts)
	if cap(b.views) < n {
		b.views = make([][]byte, n)
	}
	b.views = b.views[:n]
	for i := 0; i < n; i++ {
		end := len(b.keys)
		if i+1 < n {
			end = b.starts[i+1]
		}
		b.views[i] = b.keys[b.starts[i]:end]
	}

	b.offsets = append(b.offsets, uint32(len(b.result)))
	b.result = appendBloom(b.views, b.bitsPerKey, b.result)

	b.keys = b.keys[:0]
	b.starts = b.starts[:0]
}

// Finish 收尾并返回完整的 Filter Block 内容。返回值在下次调用前有效。
func (b *BlockBuilder) Finish() []byte {
	if len(b.starts) > 0 {
		b.generate()
	}
	arrayOffset := uint32(len(b.result))
	for _, off := range b.offsets {
		b.result = key.PutFixed32(b.result, off)
	}
	// 末尾再放一次数组起点：读取第 i 段的终点时就是读第 i+1 项，
	// 最后一段的终点正好落在这一项上，不需要额外分支。
	b.result = key.PutFixed32(b.result, arrayOffset)
	b.result = append(b.result, BaseLg)
	return b.result
}

// Size 返回当前已生成的字节数（不含尚未 Finish 的偏移数组）。
func (b *BlockBuilder) Size() int { return len(b.result) }

// BlockReader 在已生成的 Filter Block 上回答问题。
//
// 它是只读的、构造之后不可变，因此可以被多个读协程并发使用。
type BlockReader struct {
	data    []byte // 整个 Filter Block 内容
	offsets []byte // 偏移数组
	num     int    // 位图（段）数量
	baseLg  uint
	ok      bool
}

// NewBlockReader 解析一段 Filter Block 内容；结构不合法时返回一个"永远回答可能存在"
// 的读取器——过滤器只能带来性能，绝不能因为过滤器损坏而影响正确性。
func NewBlockReader(contents []byte) *BlockReader {
	r := &BlockReader{}
	n := len(contents)
	if n < 5 {
		return r
	}
	arrayOffset, err := key.Fixed32(contents[n-5:])
	if err != nil || int(arrayOffset) > n-5 {
		return r
	}
	baseLg := uint(contents[n-1])
	if baseLg > 30 { // 位移量过大没有意义，按损坏处理
		return r
	}
	// 视图从偏移数组起点一直到 baseLg 字节之前，因此末尾那个"数组自身起点"的副本
	// （哨兵）也在视图内：读取第 i 段的终点即第 i+1 项，最后一段落在哨兵上，
	// 不越界也不需要额外分支。段数 = 项数 - 1（减掉哨兵）。
	offsets := contents[arrayOffset : n-1]
	if len(offsets) < 4 || len(offsets)%4 != 0 {
		return r
	}
	r.data = contents
	r.offsets = offsets
	r.num = len(offsets)/4 - 1
	r.baseLg = baseLg
	r.ok = true
	return r
}

// NumRegions 返回位图（段）数量，供测试与诊断使用。
func (r *BlockReader) NumRegions() int {
	if !r.ok {
		return 0
	}
	return r.num
}

// KeyMayMatch 判断 userKey 是否可能出现在 blockOffset 开始的那个数据块里。
//
// 返回 false 是可靠结论，调用方可以跳过读块；返回 true 只表示"需要读块确认"。
// 接收者为 nil（即该文件没建过滤器）或过滤器结构异常时一律返回 true。
func (r *BlockReader) KeyMayMatch(blockOffset uint64, userKey []byte) bool {
	if r == nil || !r.ok {
		return true
	}
	index := blockOffset >> r.baseLg
	if index >= uint64(r.num) {
		return true
	}
	start := binary.BigEndian.Uint32(r.offsets[index*4:])
	limit := binary.BigEndian.Uint32(r.offsets[index*4+4:])
	if start > limit || int(limit) > len(r.data) {
		return true
	}
	return mayMatch(userKey, r.data[start:limit])
}
