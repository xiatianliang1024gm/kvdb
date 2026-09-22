package iterator

import "github.com/xiatianliang1024gm/kvdb/internal/key"

// DBIter 在归并流之上呈现 user key 级别的有序视图。
//
// 它负责四件归并流不该管的事：
//
//  1. **快照可见性**：seq > snapshot 的版本直接丢弃；
//  2. **同 key 去重**：同一个 user key 只输出该快照下最新的那个版本；
//  3. **墓碑遮蔽**：最新可见版本是墓碑时，这个 key 整个不输出；
//  4. **范围墓碑遮蔽**（M7）：最新可见版本落在某条范围删除的区间内且比它旧时，
//     同样整个不输出。
//
// 这四件事能做得这么轻，是因为归并流保证"同一个 user key 的版本按 seq 降序相邻排列"：
// 只要顺着扫，遇到的第一个 seq <= snapshot 的记录就是可见版本。
type DBIter struct {
	mi   Iterator
	icmp key.InternalComparer
	ucmp func(a, b []byte) int

	snapshot     uint64
	lower, upper []byte
	// ranges 是范围墓碑表（M7）：命中一条可见记录后还要判一次
	// "是否被某条范围删除遮蔽"。它由构造方（DB）从当前版本取来，
	// 与归并流里的 SST 同属一个版本快照，读的过程中不变。
	ranges []key.RangeDeletion

	// savedKey 是"当前输出的 user key"的副本。
	//
	// 必须复制：归并流的 Key() 指向子迭代器的内部缓冲区，一旦前进就会被覆盖，
	// 而跳过同 key 旧版本、判断是否越过上界都需要一个稳定的用户 key。
	savedKey []byte

	valid  bool
	closed bool
}

// NewDBIter 在 mi 之上构造 user key 级迭代器。mi 的每个子迭代器必须按"新的在前"排列。
// ranges 是当前版本的范围墓碑表（没有范围删除时传 nil）。
func NewDBIter(icmp key.InternalComparer, mi Iterator, snapshot uint64, lower, upper []byte, ranges []key.RangeDeletion) *DBIter {
	return &DBIter{
		mi:       mi,
		icmp:     icmp,
		ucmp:     userCmp(icmp),
		snapshot: snapshot,
		lower:    lower,
		upper:    upper,
		ranges:   ranges,
	}
}

// Valid 表示迭代器是否指向一个可见的 user key。
func (it *DBIter) Valid() bool { return !it.closed && it.valid }

// Key 返回当前 user key。返回值在下一次移动之前有效。
func (it *DBIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.savedKey
}

// Value 返回当前 user key 在快照下的值。
//
// 它可能直接指向块缓存里的字节，调用方不得修改。
func (it *DBIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.mi.Value()
}

// Error 返回迭代过程中遇到的首个错误。
func (it *DBIter) Error() error {
	if it.closed || it.mi == nil {
		return nil
	}
	return it.mi.Error()
}

// Close 释放迭代器。重复调用是安全的。
func (it *DBIter) Close() error {
	it.closed = true
	it.valid = false
	it.mi = nil
	it.savedKey = nil
	return nil
}

// SeekToFirst 定位到最小的可见 user key（受下界约束）。
func (it *DBIter) SeekToFirst() {
	if it.closed {
		return
	}
	if it.lower != nil {
		it.mi.Seek(key.SeekKey(it.lower, it.snapshot))
	} else {
		it.mi.SeekToFirst()
	}
	it.findNextUserEntry(false)
}

// Seek 定位到第一个 >= target 的可见 user key。
func (it *DBIter) Seek(target []byte) {
	if it.closed {
		return
	}
	if it.lower != nil && it.ucmp(target, it.lower) < 0 {
		target = it.lower
	}
	// 上界是**半开**的（不含）：与范围墓碑 [Start, End) 的区间语义统一。
	// target >= upper 时整个扫描区间为空，直接失效。
	if it.upper != nil && it.ucmp(target, it.upper) >= 0 {
		it.valid = false
		return
	}
	// 定位 key 带 (snapshot, TypeValue) 尾缀：比快照更新的版本全都排在它前面，
	// 因此落点要么是该 key 在快照下的可见版本，要么是下一个更大的 user key。
	it.mi.Seek(key.SeekKey(target, it.snapshot))
	it.findNextUserEntry(false)
}

// Next 前进到下一个可见 user key。
func (it *DBIter) Next() {
	if it.closed || !it.valid {
		return
	}
	it.mi.Next()
	// skipping = true：savedKey 里存着刚刚输出过的 user key，
	// 它剩下的旧版本全部要跳过。
	it.findNextUserEntry(true)
}

// findNextUserEntry 从当前位置开始前进，停在第一个"对快照可见且未被墓碑遮蔽"的
// user key 上。skipping 为真时，与 savedKey 相同（或更小）的 user key 一律跳过。
//
// 逻辑与 LevelDB 的 DBIter::FindNextUserEntry 一致，因为它把"版本降序排列"这个
// 前提用到了极致：遇到墓碑就记下这个 key 并转入跳过模式，遇到同名旧版本直接丢弃。
func (it *DBIter) findNextUserEntry(skipping bool) {
	it.valid = false
	if !skipping {
		// 非跳过模式下 savedKey 只作为"输出缓冲"，先清掉它（长度 0 不会参与比较，
		// 因为比较只发生在 skipping 为真的分支里）。
		it.savedKey = it.savedKey[:0]
	}
	for it.mi.Valid() {
		ik := it.mi.Key()
		uk := key.UserKey(ik)

		// 越上半开上界，本次扫描结束。
		if it.upper != nil && it.ucmp(uk, it.upper) >= 0 {
			return
		}

		if key.SeqNum(ik) <= it.snapshot {
			switch key.KindOf(ik) {
			case key.TypeValue:
				// 被范围墓碑遮蔽与被单键墓碑删除同构：这条记录不可见，
				// 且同 key 更旧的版本必然也被遮蔽（遮蔽条件随 seq 减小单调成立），
				// 所以整段跳过。
				if key.CoveredByRange(it.ranges, it.ucmp, uk, key.SeqNum(ik), it.snapshot) {
					it.savedKey = append(it.savedKey[:0], uk...)
					skipping = true
					break
				}
				if !skipping || it.ucmp(uk, it.savedKey) > 0 {
					it.savedKey = append(it.savedKey[:0], uk...)
					it.valid = true
					return
				}
			case key.TypeDeletion:
				// 该 key 在快照下已被删除：记下它，跳过它剩下的全部版本。
				it.savedKey = append(it.savedKey[:0], uk...)
				skipping = true
			}
		}
		it.mi.Next()
	}
}
