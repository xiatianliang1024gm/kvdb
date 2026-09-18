package cache

import "sync"

// NumShards 是分片数。取 16 是 LevelDB 的做法：读路径会并发命中缓存，
// 单片锁会成为瓶颈；分片太多又会让每片容量过小、淘汰过于频繁。
const NumShards = 16

// entryOverhead 是每个条目除 block 字节之外的开销估计（结构体 + map 桶摊销），
// 计入容量以便"8MB 缓存"不会因为条目过多而实际占用远超 8MB。
const entryOverhead = 64

// blockKey 是缓存的键：文件编号 + 块在文件里的偏移。
//
// 用 (编号, 偏移) 而不是路径，是因为文件编号在本引擎里单调递增且永不复用
// （见 DESIGN.md 9.9），所以块内容一旦写成就再也不会变——不需要引用计数、
// 不需要"条目在被使用时不得释放"的复杂处理，这是 M2 能把缓存做得很简单的前提。
type blockKey struct {
	fileNum uint64
	offset  uint64
}

type entry struct {
	key        blockKey
	value      []byte
	charge     int64
	prev, next *entry
}

// shard 是一把锁保护下的一段 LRU 链表。
type shard struct {
	mu       sync.Mutex
	capacity int64 // 本片容量上限（字节）
	used     int64
	items    map[blockKey]*entry
	head     *entry // 最近使用
	tail     *entry // 最久未使用

	hits, misses, evictions int64
}

// Cache 是分片 LRU 块缓存。零值不可用，请用 New 构造。
//
// 缓存里存的是**校验通过、已去掉块尾部**的数据块内容（未解压原文，压缩是 M5）。
// 由于块内容不可变，Get 直接返回内部切片，调用方不得修改。
type Cache struct {
	shards [NumShards]shard
}

// Stats 是缓存的累计统计。
type Stats struct {
	// Hits / Misses 是累计命中与未命中次数。
	Hits, Misses int64
	// Evictions 是被淘汰的条目数，容量过小时它会快速上涨。
	Evictions int64
	// Bytes 是当前占用的字节数（含条目开销），Count 是条目数。
	Bytes, Count int64
}

// New 创建一个总容量为 size 字节的缓存。
//
// size <= 0 时返回 nil，表示"不缓存"：Get/Put 在 nil 接收者上是安全的空操作，
// 因此调用方不需要到处判空。
func New(size int) *Cache {
	if size <= 0 {
		return nil
	}
	c := &Cache{}
	per := int64(size) / NumShards
	if per < 1 {
		per = 1
	}
	for i := range c.shards {
		c.shards[i].capacity = per
		c.shards[i].items = make(map[blockKey]*entry)
	}
	return c
}

// shardFor 按键的哈希选片。
//
// 用高位而不是低位取模：块偏移常常是 4KB 对齐的，低位几乎全是 0，
// 直接取模会让所有块挤在少数几片里。这里用 splitmix64 的 fmix 做雪崩，
// 保证低位也被充分打散。
func (c *Cache) shardFor(k blockKey) *shard {
	h := k.fileNum*0x9e3779b97f4a7c15 ^ (k.offset + 0x165667b19e3779f9)
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return &c.shards[h%NumShards]
}

// Get 返回缓存的块内容。第二个返回值为 false 表示未命中。
func (c *Cache) Get(fileNum, offset uint64) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	k := blockKey{fileNum: fileNum, offset: offset}
	s := c.shardFor(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.items[k]
	if !ok {
		s.misses++
		return nil, false
	}
	s.hits++
	s.moveToFront(e)
	return e.value, true
}

// Put 放入一个块。同一个键重复放入时按覆盖处理（正常情况下不会发生）。
func (c *Cache) Put(fileNum, offset uint64, block []byte) {
	if c == nil || len(block) == 0 {
		return
	}
	k := blockKey{fileNum: fileNum, offset: offset}
	s := c.shardFor(k)
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[k]; ok {
		s.moveToFront(e)
		return
	}
	e := &entry{key: k, value: block, charge: int64(len(block)) + entryOverhead}
	s.items[k] = e
	s.pushFront(e)
	s.used += e.charge

	for s.used > s.capacity && s.tail != nil {
		s.removeTail()
		s.evictions++
	}
}

// Stats 汇总所有分片的统计。
func (c *Cache) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	var st Stats
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		st.Hits += s.hits
		st.Misses += s.misses
		st.Evictions += s.evictions
		st.Bytes += s.used
		st.Count += int64(len(s.items))
		s.mu.Unlock()
	}
	return st
}

// 以下都是"调用方已持锁"的内部链表操作。

func (s *shard) pushFront(e *entry) {
	e.prev = nil
	e.next = s.head
	if s.head != nil {
		s.head.prev = e
	}
	s.head = e
	if s.tail == nil {
		s.tail = e
	}
}

func (s *shard) unlink(e *entry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		s.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		s.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (s *shard) moveToFront(e *entry) {
	if s.head == e {
		return
	}
	s.unlink(e)
	s.pushFront(e)
}

func (s *shard) removeTail() {
	e := s.tail
	if e == nil {
		return
	}
	s.unlink(e)
	delete(s.items, e.key)
	s.used -= e.charge
}
