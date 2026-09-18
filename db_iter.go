package kvdb

import (
	"errors"

	"kvdb/internal/iterator"
)

// ErrSnapshotReleased 表示快照已经被 Release 之后又被使用。
var ErrSnapshotReleased = errors.New("kvdb: snapshot has been released")

// IteratorOptions 控制迭代器的扫描范围。
type IteratorOptions struct {
	// LowerBound 是扫描下界（含）。nil 表示从最小的 key 开始。
	LowerBound []byte
	// UpperBound 是扫描上界（含）。nil 表示一直扫到最大的 key。
	//
	// 注意两个边界都是**闭区间**，与 LevelDB 的 ReadOptions 一致。
	UpperBound []byte
}

// Iterator 是面向 user key 的只读有序迭代器，只支持前向遍历。
//
// 它呈现的是"某个序列号下的可见视图"：同一个 key 只出现一次，已删除的 key
// 不出现，比快照更新的版本被隐藏。MemTable、Immutable MemTable 与所有 SST
// 在它内部被归并成一条有序流，调用方不需要关心数据在哪一层。
//
// 生命周期约定：
//
//   - Key() / Value() 返回的切片只在下一次 Seek / Next / Close 之前有效，
//     并且可能直接指向块缓存的内部字节，**调用方不得修改**；需要长期持有请复制；
//   - 迭代器持有构造那一刻的 MemTable 与 SST 引用，**不得在 DB.Close 之后继续使用**；
//   - Close 释放迭代器持有的引用。M2 还没有需要显式释放的资源，它只是把迭代器
//     置为失效（之后 Valid 恒为 false）；M3 引入版本引用计数后会变成必需调用，
//     所以现在就把它放进接口，避免将来改接口。
type Iterator interface {
	// SeekToFirst 定位到最小的可见 key。
	SeekToFirst()
	// Seek 定位到第一个 >= target 的可见 key。
	Seek(target []byte)
	// Valid 表示当前是否指向一个可见 key。
	Valid() bool
	// Key 返回当前 user key；无效时返回 nil。
	Key() []byte
	// Value 返回当前 key 的值；无效时返回 nil。
	Value() []byte
	// Next 前进到下一个可见 key。
	Next()
	// Error 返回迭代过程中遇到的首个错误。
	Error() error
	// Close 释放迭代器。
	Close() error
}

// NewIterator 返回一个遍历当前最新视图的迭代器。
//
// 构造时把 MemTable、Immutable MemTable 与当前 SST 列表（从新到旧）一次性挂进
// 归并迭代器，之后不再持有数据库锁，因此长时间扫描不会挡住写入。
//
// 这个"挂引用而不是挂锁"的做法在本项目里是安全的，因为 MemTable 只会被追加
// （冻结之后不再修改），SST 文件一旦写成就不再改动。M3 的 Compaction 会删除文件，
// 届时需要给版本加引用计数，否则迭代器可能读到已被删除的文件。
func (db *DB) NewIterator(opt *IteratorOptions) Iterator {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return errIterator{err: ErrClosed}
	}
	return db.newIteratorLocked(db.lastSeq, opt)
}

// newIteratorLocked 在指定快照序列号下构造迭代器。调用方必须持有 db.mu 的读锁。
func (db *DB) newIteratorLocked(snapshot uint64, opt *IteratorOptions) Iterator {
	children := make([]iterator.Iterator, 0, 2+len(db.files))
	// 顺序即新旧顺序：MemTable 最新，其次是 Immutable，最后是按编号从大到小的 SST。
	children = append(children, db.mem.NewIterator())
	if db.imm != nil {
		children = append(children, db.imm.NewIterator())
	}
	for i := len(db.files) - 1; i >= 0; i-- {
		children = append(children, db.files[i].reader.NewIterator())
	}

	var lower, upper []byte
	if opt != nil {
		lower, upper = opt.LowerBound, opt.UpperBound
	}
	return iterator.NewDBIter(db.icmp, iterator.NewMerging(db.icmp, children...), snapshot, lower, upper)
}

// Snapshot 是一个固定的序列号视图：用它读到的永远是"取快照那一刻"的数据，
// 之后写入的新数据对它不可见。
//
// M2 只提供读侧能力。释放序列号（让 Compaction 能安全地丢掉更旧的版本）
// 属于 M4 的工作，所以 Release 目前只是把快照标记为失效。
type Snapshot struct {
	db       *DB
	seq      uint64
	released bool
}

// GetSnapshot 记录当前已提交的最大序列号并返回一个快照。
//
// 取快照本身几乎零成本：它只记下一个序列号，不复制任何数据，也不阻塞写入。
func (db *DB) GetSnapshot() *Snapshot {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return &Snapshot{db: db, seq: db.lastSeq}
}

// Seq 返回快照固定的序列号。
func (s *Snapshot) Seq() uint64 { return s.seq }

// Get 读取快照时刻的可见版本；key 不存在或当时已被删除时返回 ErrNotFound。
func (s *Snapshot) Get(userKey []byte) ([]byte, error) {
	if len(userKey) == 0 {
		return nil, ErrEmptyKey
	}
	db := s.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if s.released {
		return nil, ErrSnapshotReleased
	}
	v, err := db.getLocked(s.seq, userKey)
	if err != nil {
		return nil, err
	}
	return copyValue(v), nil
}

// NewIterator 返回一个遍历快照视图的迭代器。
func (s *Snapshot) NewIterator(opt *IteratorOptions) Iterator {
	db := s.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return errIterator{err: ErrClosed}
	}
	if s.released {
		return errIterator{err: ErrSnapshotReleased}
	}
	return db.newIteratorLocked(s.seq, opt)
}

// Release 使快照失效。重复调用是安全的。
//
// 快照不影响写入，也不占用内存，所以"忘记 Release"目前不会造成任何泄漏；
// 一旦 M4 开始回收序列号，忘记 Release 会推迟旧版本的清理，那时它才会变成必需动作。
func (s *Snapshot) Release() { s.released = true }

// copyValue 复制一份值，把"指向块缓存/跳表节点内部的切片"挡在包的边界之内。
//
// Internal 实现返回的切片生命周期都很短，而 DB.Get 的调用方会理所当然地认为
// 拿到的是一份自己的数据，所以在出口处统一复制一次。
func copyValue(v []byte) []byte {
	if v == nil {
		return nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

// errIterator 是一个立即报错的迭代器：NewIterator 在数据库已关闭、快照已释放等
// 情况下返回它，让调用方通过 Error() 拿到原因，而不是拿到一个 panic 或者空结果。
type errIterator struct{ err error }

func (e errIterator) SeekToFirst()  {}
func (e errIterator) Seek([]byte)   {}
func (e errIterator) Valid() bool   { return false }
func (e errIterator) Key() []byte   { return nil }
func (e errIterator) Value() []byte { return nil }
func (e errIterator) Next()         {}
func (e errIterator) Error() error  { return e.err }
func (e errIterator) Close() error  { return nil }
