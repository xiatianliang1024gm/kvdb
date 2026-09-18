package cache

import (
	"sync"
	"testing"
)

// nil 缓存（容量为 0）必须完全无害：调用方不需要到处判空。
func TestNilCacheIsSafe(t *testing.T) {
	c := New(0)
	if c != nil {
		t.Fatal("New(0) 应当返回 nil 表示不缓存")
	}
	if _, ok := c.Get(1, 0); ok {
		t.Error("nil 缓存的 Get 应当返回未命中")
	}
	c.Put(1, 0, []byte("block")) // 不应 panic
	if s := c.Stats(); s != (Stats{}) {
		t.Errorf("nil 缓存的 Stats = %+v, want 零值", s)
	}
}

func TestGetPutHitMiss(t *testing.T) {
	c := New(1 << 20)
	if _, ok := c.Get(7, 4096); ok {
		t.Fatal("空缓存不应当命中")
	}
	block := []byte("data block contents")
	c.Put(7, 4096, block)

	got, ok := c.Get(7, 4096)
	if !ok {
		t.Fatal("写入之后应当命中")
	}
	if string(got) != string(block) {
		t.Fatalf("命中内容 = %q, want %q", got, block)
	}
	// 同一个文件的不同偏移必须是不同的条目。
	if _, ok := c.Get(7, 8192); ok {
		t.Error("不同偏移被当成同一个块")
	}
	// 不同文件的相同偏移也必须是不同的条目。
	if _, ok := c.Get(8, 4096); ok {
		t.Error("不同文件的相同偏移被当成同一个块")
	}

	st := c.Stats()
	if st.Hits != 1 || st.Misses != 3 {
		t.Errorf("Hits/Misses = %d/%d, want 1/3", st.Hits, st.Misses)
	}
	if st.Count != 1 || st.Bytes <= 0 {
		t.Errorf("Count=%d Bytes=%d, want Count=1 Bytes>0", st.Count, st.Bytes)
	}
}

// 容量上限必须被守住：无论放多少块，占用都不超过配置值。
func TestCapacityIsEnforced(t *testing.T) {
	const blockSize = 1024
	c := New(16 * blockSize * 4)
	block := make([]byte, blockSize)
	for i := 0; i < 2000; i++ {
		block[0] = byte(i)
		c.Put(1, uint64(i)*4096, block)
	}
	st := c.Stats()
	if st.Bytes > 16*blockSize*4 {
		t.Errorf("占用 %d 字节超过容量 %d", st.Bytes, 16*blockSize*4)
	}
	if st.Evictions == 0 {
		t.Error("写入远超容量的数据后应当发生淘汰")
	}
	// 最后写入的一批必须还在（LRU 淘汰的是最久未使用的）。
	if _, ok := c.Get(1, 1999*4096); !ok {
		t.Error("最近写入的块被淘汰了")
	}
}

// LRU 顺序：被访问过的条目要排在"未被淘汰"的一侧。
// 这里直接测分片，避免哈希选片把不同 key 分到不同片上导致断言不稳定。
func TestShardLRUOrder(t *testing.T) {
	s := &shard{capacity: 200, items: make(map[blockKey]*entry)}
	put := func(n uint64) *entry {
		k := blockKey{fileNum: 1, offset: n}
		e := &entry{key: k, value: []byte("v"), charge: 100}
		s.items[k] = e
		s.pushFront(e)
		s.used += e.charge
		return e
	}

	a, b := put(1), put(2)
	if s.head != b || s.tail != a {
		t.Fatal("新条目应当放在链表头部")
	}

	// 访问 a 之后，最久未使用的应当变成 b。
	s.moveToFront(a)
	if s.head != a || s.tail != b {
		t.Fatal("moveToFront 没有把 a 挪到头部")
	}

	// 再放一个 c，超容量后应当淘汰尾部的 b。
	put(3)
	for s.used > s.capacity {
		s.removeTail()
		s.evictions++
	}
	if _, ok := s.items[blockKey{1, 2}]; ok {
		t.Error("尾部条目应当被淘汰")
	}
	if _, ok := s.items[blockKey{1, 1}]; !ok {
		t.Error("刚访问过的条目不应被淘汰")
	}
	if s.used != 200 {
		t.Errorf("淘汰后 used = %d, want 200", s.used)
	}
}

// 重复写入同一个键不能导致容量重复计数。
func TestPutExistingKeyDoesNotDoubleCount(t *testing.T) {
	c := New(1 << 20)
	block := make([]byte, 100)
	c.Put(1, 10, block)
	first := c.Stats()
	c.Put(1, 10, block)
	second := c.Stats()
	if second.Count != first.Count || second.Bytes != first.Bytes {
		t.Errorf("重复写入后 Count/Bytes 从 %d/%d 变成 %d/%d", first.Count, first.Bytes, second.Count, second.Bytes)
	}
}

// 并发读写：命中与否都必须返回自洽的结果（配合 -race 使用）。
func TestConcurrentAccess(t *testing.T) {
	c := New(1 << 18)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				off := uint64(i % 64 * 4096)
				if b, ok := c.Get(uint64(g%2), off); ok {
					if len(b) != 32 {
						t.Errorf("命中块的意外长度 %d", len(b))
						return
					}
					continue
				}
				c.Put(uint64(g%2), off, make([]byte, 32))
			}
		}(g)
	}
	wg.Wait()
	if st := c.Stats(); st.Count == 0 {
		t.Error("并发跑完之后缓存里应当有内容")
	}
}

// 分片必须把相近的偏移打散，否则高并发的读会集中到同一把锁上。
func TestShardSelectionSpreadsAlignedOffsets(t *testing.T) {
	c := New(1 << 20)
	used := map[*shard]int{}
	for i := 0; i < 256; i++ {
		used[c.shardFor(blockKey{fileNum: 1, offset: uint64(i) * 4096})]++
	}
	if len(used) < NumShards/2 {
		t.Errorf("256 个 4KB 对齐的偏移只落在 %d 个分片上，哈希分布的散列性不足", len(used))
	}
	if testing.Verbose() {
		t.Logf("256 个偏移落在 %d/%d 个分片上", len(used), NumShards)
	}
}
