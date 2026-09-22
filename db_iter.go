package kvdb

import (
	"errors"
	"sync/atomic"

	"github.com/xiatianliang1024gm/kvdb/internal/iterator"
	"github.com/xiatianliang1024gm/kvdb/internal/version"
)

// ErrSnapshotReleased 表示快照已经被 Release 之后又被使用。
var ErrSnapshotReleased = errors.New("kvdb: snapshot has been released")

// ErrInvalidIteratorOptions 表示 IteratorOptions 的字段组合不合法：
// Prefix 与 LowerBound / UpperBound 互斥，同时设置时报这个错。
var ErrInvalidIteratorOptions = errors.New("kvdb: IteratorOptions.Prefix conflicts with LowerBound/UpperBound")

// IteratorOptions 控制迭代器的扫描范围。
//
// 区间语义（M7 起统一为半开，与范围删除 [Start, End) 一致）：
type IteratorOptions struct {
	// Prefix 按前缀扫描：等价于 LowerBound = Prefix、
	// UpperBound = Prefix 的后继（每个字节 +1 进位，全 0xFF 时无上界）。
	//
	// 它与 LowerBound / UpperBound 互斥，同时设置时 NewIterator 返回的
	// 迭代器 Error() 为 ErrInvalidIteratorOptions。
	Prefix []byte
	// LowerBound 是扫描下界（含）。nil 表示从最小的 key 开始。
	LowerBound []byte
	// UpperBound 是扫描上界（**不含**）。nil 表示一直扫到最大的 key。
	//
	// M7 起从闭区间改为半开：范围删除本身就是半开区间（"从前缀 P 到
	// P 的后继"），两套区间语义并存是 bug 温床，一次统一。
	UpperBound []byte
}

// bounds 把选项翻译成内部迭代器的 (lower, upper)；返回 error 表示选项组合非法。
func (opt *IteratorOptions) bounds() (lower, upper []byte, err error) {
	if len(opt.Prefix) > 0 {
		if opt.LowerBound != nil || opt.UpperBound != nil {
			return nil, nil, ErrInvalidIteratorOptions
		}
		return opt.Prefix, prefixUpperBound(opt.Prefix), nil
	}
	return opt.LowerBound, opt.UpperBound, nil
}

// prefixUpperBound 返回前缀 p 的后继：把最后一个非 0xFF 字节 +1、其后清零。
// 全 0xFF 时不存在后继，返回 nil 表示"扫到最大的 key 为止"。
func prefixUpperBound(p []byte) []byte {
	out := append([]byte(nil), p...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

// Iterator 是面向 user key 的只读有序迭代器，只支持前向遍历。
//
// 它呈现的是"某个序列号下的可见视图"：同一个 key 只出现一次，已删除的 key
// 不出现，比快照更新的版本被隐藏。MemTable、Immutable MemTable 与当前版本里的
// 所有 SST 在它内部被归并成一条有序流，调用方不需要关心数据在哪一层。
//
// 生命周期约定：
//
//   - Key() / Value() 返回的切片只在下一次 Seek / Next / Close 之前有效，
//     并且可能直接指向块缓存的内部字节，**调用方不得修改**；需要长期持有请复制；
//   - 迭代器持有构造那一刻的**版本引用**，因此它读的文件在整个生命周期内都不会
//     被 Compaction 删掉 —— 这也是 Close 从 M3 起变成必需调用的原因：
//     不 Close 就等于一直拦着那次 Compaction 的输出文件被回收；
//   - 迭代器仍然不得在 DB.Close 之后继续使用。
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
	// Close 释放迭代器持有的版本引用。重复调用是安全的。
	Close() error
}

// NewIterator 返回一个遍历当前最新视图的迭代器。
//
// 构造时把 MemTable、Immutable MemTable 与当前版本里的 SST（按新旧顺序）一次性
// 挂进归并迭代器，之后不再持有数据库锁，因此长时间扫描不会挡住写入。
//
// "不持锁"能成立靠的是两件事：MemTable 只会被追加（冻结之后不再修改），
// 以及迭代器持有的版本引用让它在读的这批文件不会被删掉。
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
	v := db.v
	v.Ref() // 交给迭代器持有，Close 时释放

	children := make([]iterator.Iterator, 0, 2+v.FileCount())
	// 顺序即新旧顺序：MemTable 最新，其次是 Immutable，然后是 L0（从新到旧），
	// 最后是 L1 以下的各层。归并迭代器在两条记录的 internal key 完全相同时
	// 按下标决胜，所以这个顺序必须严格成立。
	children = append(children, db.mem.NewIterator())
	if db.imm != nil {
		children = append(children, db.imm.NewIterator())
	}
	for level := 0; level < v.NumLevels(); level++ {
		files := v.Files(level)
		if level == 0 {
			for i := len(files) - 1; i >= 0; i-- {
				children = append(children, db.iteratorForFileLocked(files[i].Num))
			}
			continue
		}
		for _, f := range files {
			children = append(children, db.iteratorForFileLocked(f.Num))
		}
	}

	var lower, upper []byte
	if opt != nil {
		var err error
		lower, upper, err = opt.bounds()
		if err != nil {
			return errIterator{err: err}
		}
	}
	return &versionedIterator{
		DBIter: iterator.NewDBIter(db.icmp, iterator.NewMerging(db.icmp, children...), snapshot, lower, upper, v.RangeDeletions(), db.opts.MergeOperator),
		db:     db,
		v:      v,
	}
}

// iteratorForFileLocked 返回某个 SST 的迭代器。
//
// 读取器缺失时返回一个立即报错的迭代器（而不是 panic）：这样"版本引用了没登记的文件"
// 这个内部缺陷会以 Error() 的形式浮出来，而不是把整个进程带走。
func (db *DB) iteratorForFileLocked(num uint64) iterator.Iterator {
	r, err := db.readerFor(num)
	if err != nil {
		return errorSource{err: err}
	}
	return r.NewIterator()
}

// versionedIterator 让迭代器在关闭时释放它持有的版本引用。
//
// M2 时迭代器挂的只是一堆读取器指针，Close 没有实际作用；M3 引入 Compaction 之后，
// 不释放引用就意味着"被合并掉的文件永远删不掉"。接口里早就有 Close，
// 到这一步它才真正变成必需调用 —— 这正是当初把它放进接口的理由。
type versionedIterator struct {
	// 内嵌具体类型而不是 iterator.Iterator 接口：Close 只存在于 DBIter 上，
	// 不在那个接口里，内嵌接口的话就没法调用它。
	*iterator.DBIter

	db     *DB
	v      *version.Version
	closed bool
}

// Close 释放迭代器。重复调用是安全的。
func (it *versionedIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	err := it.DBIter.Close()
	it.db.releaseVersion(it.v)
	it.v = nil
	return err
}

// Snapshot 是一个固定的序列号视图：用它读到的永远是"取快照那一刻"的数据，
// 之后写入的新数据对它不可见。
//
// 它定义成接口而不是结构体，理由只有一个：结构体版本的字段全是非导出的，
// 包外造不出一个 *Snapshot，于是任何想替换 kvdb 实现的调用方（测试里的假库、
// 包一层缓存/加密的装饰器）走到 GetSnapshot 就断了。换成接口之后，
// "读最新视图"的 *DB 与"读固定视图"的快照在调用方眼里是同一种东西 ——
// 两者都满足 Reader。
//
// 快照本身不复制任何数据，但它会让 Compaction 保留它还需要读的那些旧版本，
// 所以它必须被 Release。忘记 Release 不会读出错数据，只会让磁盘上的旧版本
// 清理得晚一些。
type Snapshot interface {
	// Seq 返回快照固定的序列号。
	Seq() uint64
	// Get 读取快照时刻的可见版本；key 不存在或当时已被删除时返回 ErrNotFound。
	Get(userKey []byte) ([]byte, error)
	// NewIterator 返回一个遍历快照视图的迭代器。
	NewIterator(opt *IteratorOptions) Iterator
	// Release 使快照失效，并把它从"存活快照"里注销。重复调用是安全的。
	//
	// 注销之后 Compaction 才敢丢掉这个序列号之前的旧版本。不调用也不会读出错数据，
	// 只是旧版本会一直留在磁盘上（直到进程退出）。
	Release()
}

// snapshot 是 Snapshot 的唯一实现。
type snapshot struct {
	db       *DB
	seq      uint64
	released atomic.Bool
}

// GetSnapshot 记录当前已提交的最大序列号并返回一个快照。
//
// 取快照本身几乎零成本：它只记下一个序列号，不复制任何数据，也不阻塞写入。
func (db *DB) GetSnapshot() Snapshot {
	db.mu.Lock()
	defer db.mu.Unlock()
	s := &snapshot{db: db, seq: db.lastSeq}
	if !db.closed {
		db.registerSnapshotLocked(db.lastSeq)
	}
	return s
}

// Seq 返回快照固定的序列号。
func (s *snapshot) Seq() uint64 { return s.seq }

// Get 读取快照时刻的可见版本；key 不存在或当时已被删除时返回 ErrNotFound。
func (s *snapshot) Get(userKey []byte) ([]byte, error) {
	if len(userKey) == 0 {
		return nil, ErrEmptyKey
	}
	db := s.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if s.released.Load() {
		return nil, ErrSnapshotReleased
	}
	v, err := db.getLocked(s.seq, userKey)
	if err != nil {
		return nil, err
	}
	return copyValue(v), nil
}

// NewIterator 返回一个遍历快照视图的迭代器。
func (s *snapshot) NewIterator(opt *IteratorOptions) Iterator {
	db := s.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return errIterator{err: ErrClosed}
	}
	if s.released.Load() {
		return errIterator{err: ErrSnapshotReleased}
	}
	return db.newIteratorLocked(s.seq, opt)
}

// Release 使快照失效，并把它从"存活快照"里注销。重复调用是安全的。
func (s *snapshot) Release() {
	if s.released.CompareAndSwap(false, true) {
		s.db.releaseSnapshot(s.seq)
	}
}

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

// errorSource 是一个"一条记录都没有、但带着错误"的 internal 迭代器，
// 供 MergingIterator 把内部缺陷传播到上层。
type errorSource struct{ err error }

func (e errorSource) SeekToFirst()  {}
func (e errorSource) Seek([]byte)   {}
func (e errorSource) Valid() bool   { return false }
func (e errorSource) Key() []byte   { return nil }
func (e errorSource) Value() []byte { return nil }
func (e errorSource) Next()         {}
func (e errorSource) Error() error  { return e.err }
