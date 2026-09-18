package filter

import (
	"bytes"
	"fmt"
	"testing"
)

// Bloom 的最低保证：写进去的 key 一定不会漏判（假阴性会直接变成读不到数据）。
func TestBloomNeverHasFalseNegatives(t *testing.T) {
	for _, bitsPerKey := range []int{6, 10, 20} {
		var keys [][]byte
		for i := 0; i < 2000; i++ {
			keys = append(keys, []byte(fmt.Sprintf("key-%06d", i)))
		}
		bloom := appendBloom(keys, bitsPerKey, nil)
		for _, k := range keys {
			if !mayMatch(k, bloom) {
				t.Fatalf("bitsPerKey=%d: key %q 被漏判（假阴性）", bitsPerKey, k)
			}
		}
	}
}

// 假阳性率要落在理论值附近：10 位/key 大约 1%，20 位/key 大约 0.01%。
// 这里给足余量，只用来发现"位图算错"这类量级错误。
func TestBloomFalsePositiveRate(t *testing.T) {
	const n = 5000
	var keys [][]byte
	for i := 0; i < n; i++ {
		keys = append(keys, []byte(fmt.Sprintf("present-%06d", i)))
	}

	cases := []struct {
		bitsPerKey int
		maxRate    float64
	}{
		{10, 0.05},
		{20, 0.01},
	}
	for _, c := range cases {
		bloom := appendBloom(keys, c.bitsPerKey, nil)
		fp := 0
		const probes = 20000
		for i := 0; i < probes; i++ {
			if mayMatch([]byte(fmt.Sprintf("absent-%06d", i)), bloom) {
				fp++
			}
		}
		rate := float64(fp) / probes
		if rate > c.maxRate {
			t.Errorf("bitsPerKey=%d 的假阳性率 %.4f 超过上限 %.4f", c.bitsPerKey, rate, c.maxRate)
		}
		if testing.Verbose() {
			t.Logf("bitsPerKey=%d: 位图 %d 字节, 假阳性率 %.4f", c.bitsPerKey, len(bloom), rate)
		}
	}
}

// 哈希必须稳定且分布可用（同一输入同一结果，不同输入不大量碰撞）。
func TestHashIsStableAndSpread(t *testing.T) {
	if Hash(nil) != Hash([]byte{}) {
		t.Error("nil 与空切片应当得到同一哈希")
	}
	seen := make(map[uint32]int, 10000)
	collisions := 0
	for i := 0; i < 10000; i++ {
		h := Hash([]byte(fmt.Sprintf("k%d", i)))
		if _, dup := seen[h]; dup {
			collisions++
		}
		seen[h] = i
	}
	if collisions > 5 {
		t.Errorf("10000 个 key 里有 %d 次哈希碰撞，分布异常", collisions)
	}
}

// 空集合与超短位图不能被误当作"可能存在"之外的任何结论。
func TestMayMatchEdgeCases(t *testing.T) {
	if mayMatch([]byte("a"), nil) {
		t.Error("空位图应当回答「一定不存在」")
	}
	if mayMatch([]byte("a"), []byte{1}) {
		t.Error("只有 1 字节的位图是非法编码，应当回答不存在")
	}
	// 保留编码：最后一字节 > 30 时无法解释，必须保守回答"可能存在"。
	if !mayMatch([]byte("a"), []byte{0xff, 0xff, 31}) {
		t.Error("未知编码应当保守回答可能存在")
	}
}

// Filter Block 的分段语义：位图按数据区偏移分段，查询用块偏移定位。
func TestBlockBuilderSegments(t *testing.T) {
	b := NewBlockBuilder(10)
	b.StartBlock(0)
	b.AddKey([]byte("a"))
	b.AddKey([]byte("b"))

	// 跳到第 2 段：中间的第 1 段没有 key，应当生成一个空位图。
	b.StartBlock(2 * BaseSize)
	b.AddKey([]byte("c"))
	b.StartBlock(3 * BaseSize)

	contents := b.Finish()
	r := NewBlockReader(contents)
	if got := r.NumRegions(); got != 3 {
		t.Fatalf("段数 = %d, want 3", got)
	}
	if !r.KeyMayMatch(0, []byte("a")) || !r.KeyMayMatch(0, []byte("b")) {
		t.Error("第 0 段应当包含 a、b")
	}
	// 第 1 段（偏移 [2048, 4096)）没有任何 key：任何查询都必须被否掉。
	for _, k := range []string{"a", "b", "c", "whatever"} {
		if r.KeyMayMatch(BaseSize, []byte(k)) {
			t.Errorf("空段对 %q 回答了可能存在，会白白多读一次块", k)
		}
	}
	if !r.KeyMayMatch(2*BaseSize, []byte("c")) {
		t.Error("第 2 段应当包含 c")
	}
	// 绝对不存在的 key 应该被否掉（确定性哈希下结果稳定）。
	if r.KeyMayMatch(0, []byte("zzz-not-present")) {
		t.Error("不存在的 key 被误判为可能存在")
	}
}

// 结构损坏的 Filter Block 不能影响正确性：只能退化成"全部可能存在"。
func TestBlockReaderToleratesCorruption(t *testing.T) {
	b := NewBlockBuilder(10)
	b.StartBlock(0)
	b.AddKey([]byte("a"))
	contents := b.Finish()

	for _, n := range []int{0, 1, 4} {
		r := NewBlockReader(contents[:n])
		if !r.KeyMayMatch(0, []byte("definitely-absent")) {
			t.Errorf("截断到 %d 字节后仍给出了否定的结论", n)
		}
	}

	// 偏移数组指向文件外：同样必须保守回答。
	bad := append([]byte(nil), contents...)
	bad[len(bad)-5] = 0xff
	if r := NewBlockReader(bad); !r.KeyMayMatch(0, []byte("definitely-absent")) {
		t.Error("偏移数组越界时应当保守回答可能存在")
	}
}

// 位图内容必须可重复生成（同一批 key 两次构造结果一致）。
func TestBlockBuilderDeterministic(t *testing.T) {
	build := func() []byte {
		b := NewBlockBuilder(10)
		b.StartBlock(0)
		for _, k := range []string{"a", "b", "c"} {
			b.AddKey([]byte(k))
		}
		return b.Finish()
	}
	if !bytes.Equal(build(), build()) {
		t.Error("同一批 key 两次构造出的 Filter Block 不一致")
	}
}

// 没有任何 key 时生成的 Filter Block 必须仍能被安全解析。
func TestBlockBuilderEmpty(t *testing.T) {
	contents := NewBlockBuilder(10).Finish()
	r := NewBlockReader(contents)
	if r.NumRegions() != 0 {
		t.Errorf("空过滤器的段数 = %d, want 0", r.NumRegions())
	}
	if !r.KeyMayMatch(0, []byte("a")) {
		t.Error("空过滤器的段数为 0，任何偏移都落在未知区间，应当回答可能存在")
	}
}
