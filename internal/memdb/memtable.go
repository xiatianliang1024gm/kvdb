package memdb

import (
	"sync/atomic"

	"kvdb/internal/key"
)

// nodeOverhead 是每个跳表节点除 key/value 之外的粗略开销估计：
// node 结构体（两个切片头 + 一个切片头）加上层高数组里的指针。
const nodeOverhead = 64

// MemTable 是内存中的有序表，底层是跳表。
//
// 它保存的是完整的 internal key，因此天然带版本信息：同一个 user key 的多个版本
// 按 (seq, kind) 降序排列，读取时"第一个 seq <= snapshot 的记录"就是可见版本。
//
// 并发模型：读无锁，写由调用方串行化（db 层的写锁）。一旦被冻结为
// Immutable MemTable，就不应再有新的写入。
type MemTable struct {
	cmp key.Comparer
	skl *Skiplist

	mem    atomic.Int64 // 近似内存占用，单位字节
	logNum uint64       // 支撑这张表的 WAL 编号
}

// New 创建一张空 MemTable。logNum 是当前正在追加的 WAL 编号，用于 Flush 完成后
// 判断哪些日志可以安全删除。
func New(cmp key.Comparer, logNum uint64) *MemTable {
	return &MemTable{
		cmp:    cmp,
		skl:    NewSkiplist(key.InternalComparer{User: cmp}),
		logNum: logNum,
	}
}

// LogNumber 返回支撑这张 MemTable 的 WAL 编号。
func (m *MemTable) LogNumber() uint64 { return m.logNum }

// SetLogNumber 用于恢复期把重放出的 MemTable 关联到正确的 WAL 编号。
func (m *MemTable) SetLogNumber(n uint64) { m.logNum = n }

// ApproximateSize 返回近似内存占用，单位字节。
func (m *MemTable) ApproximateSize() int64 { return m.mem.Load() }

// Empty 表示这张表是否还没有任何记录。
func (m *MemTable) Empty() bool { return m.skl.Len() == 0 }

// Len 返回记录条数（含墓碑）。
func (m *MemTable) Len() int64 { return m.skl.Len() }

// Add 插入一条记录。key 与 value 会被复制，调用方可以随意复用缓冲区。
func (m *MemTable) Add(seq uint64, kind key.Kind, userKey, value []byte) {
	ik := key.EncodeInternalKey(userKey, seq, kind)
	var v []byte
	if len(value) > 0 {
		v = append([]byte(nil), value...)
	}
	m.skl.Insert(ik, v)
	m.mem.Add(int64(len(ik)+len(v)) + nodeOverhead)
}

// Get 在 snapshot 序列号下查找 userKey。
//
// 返回值语义：
//   - found 为 false：这张表里没有该 key 在 snapshot 下的可见版本，调用方应继续查下层；
//   - found 为 true 且 kind 为 TypeDeletion：命中墓碑，key 已被删除，调用方应停止下探；
//   - found 为 true 且 kind 为 TypeValue：命中数据。
func (m *MemTable) Get(snapshot uint64, userKey []byte) (value []byte, kind key.Kind, found bool) {
	// SeekKey 的尾缀是 (snapshot, TypeValue)，尾缀降序下它排在
	// "seq <= snapshot 的全部版本"之前，因此 seek 落点就是最新可见版本。
	it := m.skl.NewIterator()
	it.Seek(key.SeekKey(userKey, snapshot))
	if !it.Valid() {
		return nil, 0, false
	}
	ik := it.Key()
	if m.cmp.Compare(key.UserKey(ik), userKey) != 0 {
		// 该 user key 在 snapshot 下没有可见版本，落点已经跑到下一个 key 上了。
		return nil, 0, false
	}
	return it.Value(), key.KindOf(ik), true
}

// NewIterator 返回遍历全部 internal key 的前向迭代器，供 Flush 落盘使用。
func (m *MemTable) NewIterator() *Iterator { return m.skl.NewIterator() }

// Seek 在表中定位到第一个 user key >= target 的位置（按 snapshot 可见性）。
//
// 与 Get 不同，它不会因为遇到墓碑而停止，用于范围扫描的起点定位（M2 的迭代器
// 会在此基础上做版本归并）。
func (m *MemTable) Seek(snapshot uint64, userKey []byte) *Iterator {
	it := m.skl.NewIterator()
	it.Seek(key.SeekKey(userKey, snapshot))
	return it
}
