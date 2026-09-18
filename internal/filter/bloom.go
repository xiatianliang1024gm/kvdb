// Package filter 实现 SSTable 的 Filter Block：按数据区偏移分段的 Bloom 位图。
//
// 定位：读路径最贵的是"读一个根本不含目标 key 的数据块"。Bloom Filter 能在
// 读块之前就判定"这个 key 一定不在这一段数据里"，从而省掉绝大部分无效 IO。
//
// 两个必须记住的性质：
//   - **不存在假阴性**：真的写进去的 key，查询一定返回"可能存在"。
//     这是 SStable 正确性的底线——漏判会直接变成读不到数据。
//   - 存在假阳性：key 不在时也可能返回"可能存在"，只是多读一次块。
//     用 bits/key 控制它的概率：10 位/key 时约 1%，20 位时约 0.01%。
package filter

// PolicyName 是 Bloom Filter 的策略名，会作为 MetaIndex Block 里的 key 出现
// （形如 "filter.kvdb.BloomFilter"）。读取方按它查找，因此改名等于改文件格式。
const PolicyName = "kvdb.BloomFilter"

const (
	// BaseLg 决定一个 Bloom 位图覆盖数据区的范围：2^BaseLg 字节。
	//
	// 取 11（2KB）是 LevelDB 的默认值：位图太小则假阳性率高、太大则淘汰粒度粗。
	// 它只影响精度，不影响正确性——一个位图里混进更多 key 只会更"容易假阳性"。
	BaseLg = 11
	// BaseSize 是一个 Bloom 位图覆盖的数据区字节数。
	BaseSize = 1 << BaseLg
)

// Hash 是 LevelDB 的 Bloom 哈希：4 字节一组累加后相乘再异或，尾部 1~3 字节单独处理。
//
// 它按**小端**取值成 32 位字，这是该算法自身的约定（与项目"二进制编码一律大端"
// 的规则无关，位图内容不对外交换）。
func Hash(key []byte) uint32 {
	const (
		seed = uint32(0xbc9f1d34)
		m    = uint32(0xc6a4a793)
		r    = 24
	)
	h := seed ^ uint32(len(key))*m
	for len(key) >= 4 {
		h += uint32(key[0]) | uint32(key[1])<<8 | uint32(key[2])<<16 | uint32(key[3])<<24
		key = key[4:]
		h *= m
		h ^= h >> 16
	}
	switch len(key) {
	case 3:
		h += uint32(key[2]) << 16
		fallthrough
	case 2:
		h += uint32(key[1]) << 8
		fallthrough
	case 1:
		h += uint32(key[0])
		h *= m
		h ^= h >> r
	}
	return h
}

// probeCount 返回最优探测次数：ln2 ≈ 0.69，上下限沿用 LevelDB。
func probeCount(bitsPerKey int) int {
	k := int(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}

// appendBloom 把 keys 的 Bloom 位图追加到 dst 末尾并返回扩展后的切片。
//
// 编码是自描述的：位图字节 + 1 字节探测次数，因此判断时不需要回看配置。
// 最后一个字节大于 30 被视为"未来换过编码"，此时一律回答"可能存在"（保守但正确）。
func appendBloom(keys [][]byte, bitsPerKey int, dst []byte) []byte {
	if len(keys) == 0 {
		return dst
	}
	bits := len(keys) * bitsPerKey
	if bits < 64 { // 位图至少 8 字节，避免位数过少导致全 1
		bits = 64
	}
	nbytes := (bits + 7) / 8
	bits = nbytes * 8
	k := probeCount(bitsPerKey)

	base := len(dst)
	dst = append(dst, make([]byte, nbytes+1)...)
	array := dst[base : base+nbytes]
	dst[base+nbytes] = byte(k)

	for _, key := range keys {
		h := Hash(key)
		delta := (h >> 17) | (h << 15) // 循环右移 17 位，用于生成 k 个互不相关的探测位
		for j := 0; j < k; j++ {
			bit := h % uint32(bits)
			array[bit/8] |= 1 << (bit % 8)
			h += delta
		}
	}
	return dst
}

// mayMatch 判断 key 的 k 个探测位是否全部为 1。
//
// 返回 false 表示 **key 一定不在** 这个位图对应的 key 集合里（可以安全跳过读块）；
// 返回 true 表示可能存在，需要真的去读块确认。
func mayMatch(key, bloom []byte) bool {
	if len(bloom) < 2 {
		return false // 空位图：里面没有任何 key
	}
	bits := uint32((len(bloom) - 1) * 8)
	k := int(bloom[len(bloom)-1])
	if k > 30 {
		return true // 保留编码：无法解释就保守回答
	}
	array := bloom[:len(bloom)-1]

	h := Hash(key)
	delta := (h >> 17) | (h << 15)
	for j := 0; j < k; j++ {
		bit := h % bits
		if array[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}
