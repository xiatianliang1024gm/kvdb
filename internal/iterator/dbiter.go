package iterator

import (
	"errors"
	"fmt"

	"github.com/xiatianliang1024gm/kvdb/internal/key"
)

// errNoMergeOperator 是读路径的运行时防线：归并流里出现了 merge 记录，
// 但构造 DBIter 时没有给算子。正常情况下 Manifest 校验会拦住这种配置，
// 到不了这里；宁可报错也不静默把 operand 当值返回。
var errNoMergeOperator = errors.New("kvdb/iterator: merge record found but no merge operator configured")

// MergeFold 是 DBIter 折叠 merge operand 链所需的最小算子能力。
//
// 单独定义这个单方法接口而不是 import compact 包的 MergeOperator：
// compact 依赖本包（Compaction 的归并循环用 MergingIterator），反向 import
// 会成环。compact.MergeOperator 的方法集是它的超集，天然满足本接口。
type MergeFold interface {
	// FullMerge 把 base（可为 nil）与一串按 seq **从旧到新**排列的 operand
	// 折叠成一个值。语义与 compact.MergeOperator.FullMerge 一致。
	FullMerge(userKey, base []byte, operands [][]byte) ([]byte, error)
}

// DBIter 在归并流之上呈现 user key 级别的有序视图。
//
// 它负责五件归并流不该管的事：
//
//  1. **快照可见性**：seq > snapshot 的版本直接丢弃；
//  2. **同 key 去重**：同一个 user key 只输出该快照下最新的那个版本；
//  3. **墓碑遮蔽**：最新可见版本是墓碑时，这个 key 整个不输出；
//  4. **范围墓碑遮蔽**（M7）：最新可见版本落在某条范围删除的区间内且比它旧时，
//     同样整个不输出；
//  5. **Merge 折叠**（M8）：最新可见版本是 merge 记录时，向下收集同 key 的
//     全部 operand（直到可见的 Value / Deletion 或数据尽头），折叠成一个值输出。
//
// 这五件事能做得这么轻，是因为归并流保证"同一个 user key 的版本按 seq 降序
// 相邻排列"：只要顺着扫，遇到的第一个 seq <= snapshot 的记录就是可见版本。
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
	// merge 非 nil 时支持折叠 merge operand 链（M8）。nil 时遇到 merge
	// 记录会让迭代器进入错误状态——比静默返回错误的值安全。
	merge MergeFold

	// savedKey 是"当前输出的 user key"的副本。
	//
	// 必须复制：归并流的 Key() 指向子迭代器的内部缓冲区，一旦前进就会被覆盖，
	// 而跳过同 key 旧版本、判断是否越过上界都需要一个稳定的用户 key。
	savedKey []byte

	// folded / foldedSet 是 merge 折叠的产物。折叠会把归并流推进过整个
	// operand 链，Value() 不能再回读 mi，只能用这里存的值。
	folded     []byte
	foldedSet  bool
	// consumedKey 表示"mi 已经被折叠推进到下一个 user key（或尽头）"：
	// 下一次 Next() 不能再前进一步，否则会跳过下一个 key 的第一条记录。
	consumedKey bool

	valid  bool
	closed bool
	err    error
}

// NewDBIter 在 mi 之上构造 user key 级迭代器。mi 的每个子迭代器必须按"新的在前"排列。
// ranges 是当前版本的范围墓碑表（没有范围删除时传 nil）；
// merge 是折叠算子（不支持 Merge 时传 nil）。
func NewDBIter(icmp key.InternalComparer, mi Iterator, snapshot uint64, lower, upper []byte, ranges []key.RangeDeletion, merge MergeFold) *DBIter {
	return &DBIter{
		mi:       mi,
		icmp:     icmp,
		ucmp:     userCmp(icmp),
		snapshot: snapshot,
		lower:    lower,
		upper:    upper,
		ranges:   ranges,
		merge:    merge,
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
// 对 merge 折叠出来的 key，返回的是折叠结果（迭代器自持，直到下一次移动）；
// 其余情况可能直接指向块缓存里的字节，调用方不得修改。
func (it *DBIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	if it.foldedSet {
		return it.folded
	}
	return it.mi.Value()
}

// Error 返回迭代过程中遇到的首个错误。
func (it *DBIter) Error() error {
	if it.err != nil {
		return it.err
	}
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
	it.folded = nil
	it.foldedSet = false
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
	if it.consumedKey {
		// merge 折叠已经把归并流推进到下一个 user key（或尽头）。
		// 这里不能再前进一步，否则会跳过下一个 key 的第一条记录。
		it.consumedKey = false
		it.findNextUserEntry(true)
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
	it.foldedSet = false
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
				// 它下面的 merge operand 链也随之作废（skipping 模式不会再折叠）。
				it.savedKey = append(it.savedKey[:0], uk...)
				skipping = true
			case key.TypeMerge:
				// 只有作为"该 key 的第一条可见记录"时才需要折叠：
				// skipping 且 uk == savedKey 说明这个 key 的结局已经定了
				//（删除或遮蔽），它剩下的 operand 全部跟着作废。
				if !skipping || it.ucmp(uk, it.savedKey) > 0 {
					if it.merge == nil {
						it.err = errNoMergeOperator
						return
					}
					// foldMerge 会把归并流推进过整个 operand 链（含终局记录），
					// 停在下一个 user key 或尽头 —— 消费标志必须置上，
					// 否则下一次 Next() 会再跳过一条记录。
					it.consumedKey = true
					if !it.foldMerge(uk) {
						return // it.err 已设置
					}
					it.savedKey = append(it.savedKey[:0], uk...)
					it.valid = true
					return
				}
			}
		}
		it.mi.Next()
	}
}

// foldMerge 从当前位置（该 key 的第一条可见 merge 记录）开始，收集同 key 的
// 全部 operand 直到终局或本 key 结束，按 seq 从旧到新折叠成一个值存进 it.folded。
//
// 终局判定与点查路径（DB.foldMergeLocked）完全一致：
//
//   - 可见的 TypeValue ⇒ 它是 base，收集终止；
//   - 可见的 TypeDeletion ⇒ key 在此被删除，base 为 nil，收集终止；
//   - 被范围墓碑遮蔽的记录 ⇒ 逐条跳过（遮蔽按记录判定），链继续收；
//   - 走到下一个 user key 或数据尽头 ⇒ 没有本层 base，FullMerge 收到 nil。
//
// 返回 false 表示出错（it.err 已设置，迭代器进入错误状态）。
func (it *DBIter) foldMerge(uk []byte) bool {
	// 收集顺序是从新到旧（归并流方向）；FullMerge 要求从旧到新，先收集再反转。
	var operands [][]byte
	var base []byte
	hasBase := false
	for it.mi.Valid() {
		ik := it.mi.Key()
		cur := key.UserKey(ik)
		if it.ucmp(cur, uk) != 0 {
			break // 走到下一个 user key：本输入内没有 base
		}
		seq := key.SeqNum(ik)
		// 收集阶段只会遇到 seq 更小的记录（同 key 按 seq 降序），
		// 不会撞上比快照新的版本；遮蔽判定按记录逐条做。
		if seq <= it.snapshot && key.CoveredByRange(it.ranges, it.ucmp, cur, seq, it.snapshot) {
			it.mi.Next()
			continue
		}
		switch key.KindOf(ik) {
		case key.TypeMerge:
			// value 指向块数据，链要跨多条记录收集，一律复制。
			operands = append(operands, append([]byte(nil), it.mi.Value()...))
		case key.TypeValue:
			base, hasBase = append([]byte(nil), it.mi.Value()...), true
		case key.TypeDeletion:
			hasBase = true // base 为 nil
		}
		it.mi.Next()
		if hasBase {
			break
		}
	}
	for i, j := 0, len(operands)-1; i < j; i, j = i+1, j-1 {
		operands[i], operands[j] = operands[j], operands[i]
	}
	merged, err := it.merge.FullMerge(uk, base, operands)
	if err != nil {
		it.err = fmt.Errorf("kvdb/iterator: merge operator: %w", err)
		return false
	}
	it.folded = merged
	it.foldedSet = true
	return true
}
