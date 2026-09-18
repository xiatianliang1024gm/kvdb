// Package iterator 提供两种迭代器：
//
//   - Iterator：internal key 级别的有序流（MemTable、SSTable 各自实现它）；
//   - MergingIterator：把多个 Iterator 归并成一个有序流；
//   - DBIter：在归并流之上做"快照可见性 + 墓碑遮蔽 + 同 key 版本去重"，
//     对外呈现的是 user key 级别的有序视图，也就是用户拿到的迭代器。
//
// 之所以分层：合并只关心"按 internal key 排序"，而可见性只关心"MergedStream 上的
// 一条记录是否该给用户看"。把两件事混在一个迭代器里，会让两边都难写。
package iterator

import (
	"bytes"

	"kvdb/internal/key"
)

// Iterator 是 internal key 级别的有序迭代器，只支持前向遍历。
//
// 实现方：memdb 的跳表迭代器、sst 的表迭代器。它们都不需要知道"快照"与"墓碑"，
// 只需要按 internal key 顺序把记录吐出来。
//
// 约定：
//   - Seek(target) 定位到第一个 key >= target 的记录，不存在时 Valid() 为 false；
//   - Key() / Value() 返回的切片在下一次移动之前有效；
//   - 出错时 Valid() 变为 false，错误通过 Error() 报出。
type Iterator interface {
	SeekToFirst()
	Seek(target []byte)
	Valid() bool
	Key() []byte
	Value() []byte
	Next()
	Error() error
}

// userCmp 返回只比较 user key 部分的比较函数。
//
// key.InternalComparer.UserCompare 比较的是两个 internal key 的 user 部分，
// 而这里需要的是"把两个裸 user key 拿来比"，所以单独包一层。
func userCmp(cmp key.InternalComparer) func(a, b []byte) int {
	if cmp.User != nil {
		return cmp.User.Compare
	}
	return bytes.Compare
}
