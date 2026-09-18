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
// **入参必须是 internal key**：它会各自剥掉末尾 8 字节的尾缀。传裸 user key 进来
// 是很容易犯的错，而且症状很隐蔽 —— 短于 8 字节的 key 剥完变成空串，
// 于是"两个不同的 key"被判成相等，一路静默地把数据当成重复版本丢掉。
// 要比较裸 user key 请用 CompareUser。
func (c InternalComparer) UserCompare(a, b []byte) int {
	return c.CompareUser(UserKey(a), UserKey(b))
}

// CompareUser 比较两个**裸 user key**（不带尾缀）。
//
// 它与 UserCompare 成对：一个吃 internal key，一个吃 user key。分开成两个名字是
// 刻意的——同一个函数名同时接受两种输入，就只能靠"key 到底多长"来猜，
// 而猜错的方向是静默丢数据，代价太大。
func (c InternalComparer) CompareUser(a, b []byte) int {
	userCmp := bytes.Compare
	if c.User != nil {
		userCmp = c.User.Compare
	}
	return userCmp(a, b)
}
