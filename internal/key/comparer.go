package key

import "bytes"

// Comparer 定义 user key 之间的全序关系。
//
// root 包的 kvdb.Comparer 在结构上与本接口一致（同样是 Compare + Name），
// 因此可以直接作为 Comparer 传入，内部包无需反向依赖根包。
type Comparer interface {
	// Compare 返回 -1 / 0 / +1，分别表示 a < b、a == b、a > b。
	Compare(a, b []byte) int
	// Name 返回该比较器的稳定标识，用于 Manifest 校验目录与配置是否匹配。
	Name() string
}

// InternalComparer 在 Comparer 之上叠加 internal key 的排序规则：
// user_key 升序，user_key 相同时尾缀降序（新版本在前）。
//
// 它同时满足 Comparer 接口，因此可以直接喂给跳表、SSTable 迭代器等
// 只会调用 Compare 的组件。
type InternalComparer struct {
	User Comparer
}

// Compare 比较两个已编码的 internal key。
func (c InternalComparer) Compare(a, b []byte) int {
	var userCmp func(a, b []byte) int
	if c.User != nil {
		userCmp = c.User.Compare
	}
	return InternalKeyCompare(a, b, userCmp)
}

// Name 返回该比较器的稳定标识。
func (c InternalComparer) Name() string {
	if c.User == nil {
		return "kvdb.InternalKeyComparer(bytes)"
	}
	return "kvdb.InternalKeyComparer(" + c.User.Name() + ")"
}

// UserCompare 只比较两个 internal key 的 user_key 部分，尾缀被忽略。
//
// 用于判断两个 internal key 是否指向同一个 user key（例如迭代器在
// 归并时找同名 key 的所有版本）。
func (c InternalComparer) UserCompare(a, b []byte) int {
	userCmp := bytes.Compare
	if c.User != nil {
		userCmp = c.User.Compare
	}
	return userCmp(UserKey(a), UserKey(b))
}
