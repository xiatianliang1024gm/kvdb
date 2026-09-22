package memdb

import (
	"fmt"
	"math/rand"
	"sync/atomic"

	"github.com/xiatianliang1024gm/kvdb/internal/key"
)

// MaxHeight 是跳表的最大层数。
//
// 层高按几何分布生成，期望值是 1/(1-p)。取 p = 1/4 时，
// 12 层足以支撑 4^11 ≈ 4 百万个节点的索引，对单个 MemTable 足够。
const MaxHeight = 12

// branching 是层高的分支因子：层高 ≥ h 的概率是 1/branching^(h-1)。
const branching = 4

// node 是跳表节点。key 是完整的 internal key，value 是记录内容
// （TypeDeletion 时为空）。
//
// next 的长度即节点层高，用原子指针保存，使并发读者无需加锁即可遍历。
type node struct {
	key   []byte
	value []byte
	next  []atomic.Pointer[node]
}

func (n *node) loadNext(level int) *node { return n.next[level].Load() }

func (n *node) storeNext(level int, x *node) { n.next[level].Store(x) }

func newNodeInternal(k, v []byte, height int) *node {
	return &node{key: k, value: v, next: make([]atomic.Pointer[node], height)}
}

// Skiplist 是 MemTable 的底层有序容器，采用 LevelDB 的并发模型：
// **单写者 + 多读者，读路径完全不持锁**。
//
// 写者必须串行调用（由 MemTable 之外的写锁保证），读者可以任意并发：
// 新节点先把自己的全部后继指针写好，再逐层挂到链上；读者沿原子指针遍历时
// 要么看不到新节点，要么看到的节点已经完整。顺序号天然唯一，因此
// 同一个 internal key 不会被插入两次，写者不需要原地修改任何已有节点，
// 这是"无锁读"成立的前提。
type Skiplist struct {
	cmp    key.Comparer
	head   *node
	height atomic.Int32
	count  atomic.Int64

	// rnd 只被写者使用，不需要同步；固定种子让层高序列可复现，便于排查问题。
	rnd *rand.Rand
}

// NewSkiplist 创建一个使用 cmp 比较 internal key 的空跳表。
func NewSkiplist(cmp key.Comparer) *Skiplist {
	s := &Skiplist{
		cmp:  cmp,
		head: newNodeInternal(nil, nil, MaxHeight),
		rnd:  rand.New(rand.NewSource(0x5eed)),
	}
	s.height.Store(1)
	return s
}

// Len 返回已插入的节点数。
func (s *Skiplist) Len() int64 { return s.count.Load() }

// Height 返回当前有效层数，主要用于测试。
func (s *Skiplist) Height() int { return int(s.height.Load()) }

// randomHeight 按几何分布生成层高，返回 [1, MaxHeight]。
func (s *Skiplist) randomHeight() int {
	h := 1
	for h < MaxHeight && s.rnd.Intn(branching) == 0 {
		h++
	}
	return h
}

// findGreaterOrEqual 返回第一个 key >= ik 的节点。
//
// 当 prev 非 nil 时，把每一层"最后一个 key < ik 的节点"写进 prev，
// 供插入时定位各层的前驱。未被搜索覆盖的高层由调用方预先填成 head。
func (s *Skiplist) findGreaterOrEqual(ik []byte, prev *[MaxHeight]*node) *node {
	x := s.head
	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		next := x.loadNext(level)
		for next != nil && s.cmp.Compare(next.key, ik) < 0 {
			x = next
			next = x.loadNext(level)
		}
		if prev != nil {
			prev[level] = x
		}
	}
	return x.loadNext(0)
}

// Insert 插入一条记录。key 必须是完整的 internal key。
//
// 调用方必须保证 ik 是新的（序列号唯一），重复插入会 panic——
// 这属于编程错误，静默覆盖会让读者看到撕裂的数据。
func (s *Skiplist) Insert(ik, value []byte) {
	var prev [MaxHeight]*node
	for i := range prev {
		prev[i] = s.head
	}
	if x := s.findGreaterOrEqual(ik, &prev); x != nil && s.cmp.Compare(x.key, ik) == 0 {
		panic(fmt.Sprintf("github.com/xiatianliang1024gm/kvdb/memdb: duplicate internal key %q", ik))
	}

	height := s.randomHeight()
	if height > int(s.height.Load()) {
		s.height.Store(int32(height))
	}
	n := newNodeInternal(ik, value, height)

	// 第一轮：把新节点在每一层的后继接好。
	for level := 0; level < height; level++ {
		n.storeNext(level, prev[level].loadNext(level))
	}
	// 第二轮：逐层发布。任一层发布出去时，n 的所有后继都已就绪。
	for level := 0; level < height; level++ {
		prev[level].storeNext(level, n)
	}
	s.count.Add(1)
}

// Iterator 是 Skiplist 的前向迭代器。它只持有跳表指针与当前节点，
// 因此 Seek 可以直接从头部重新下降定位。
type Iterator struct {
	s *Skiplist
	n *node
}

// NewIterator 返回一个尚未定位的迭代器，用法是 SeekToFirst 或 Seek。
func (s *Skiplist) NewIterator() *Iterator {
	return &Iterator{s: s}
}

// SeekToFirst 定位到最小的节点。
func (it *Iterator) SeekToFirst() {
	it.n = it.s.head.loadNext(0)
}

// Seek 定位到第一个 key >= ik 的节点；不存在时迭代器失效。
func (it *Iterator) Seek(ik []byte) {
	it.n = it.s.findGreaterOrEqual(ik, nil)
}

// Valid 表示迭代器是否指向有效节点。
func (it *Iterator) Valid() bool { return it.n != nil }

// Key 返回当前节点的 internal key。
func (it *Iterator) Key() []byte { return it.n.key }

// Value 返回当前节点的 value；墓碑的 value 为空切片。
func (it *Iterator) Value() []byte { return it.n.value }

// Next 前进一个节点。
func (it *Iterator) Next() {
	if it.n != nil {
		it.n = it.n.loadNext(0)
	}
}

// Error 永远返回 nil：跳表的数据全在内存里，遍历不可能出错。
//
// 它存在的意义是让 memdb.Iterator 与 sst.Iterator 拥有同一组方法，
// 从而能被 MergingIterator 归并到一起，而不需要为两边各写一份适配层。
func (it *Iterator) Error() error { return nil }
