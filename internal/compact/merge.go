package compact

// MergeOperator 定义"如何把一串 merge operand 折叠成一个值"。
//
// 它是根包 kvdb.MergeOperator 的本体（type alias）。之所以定义在 compact
// 包而不是根包，与 Filter 相同：Compaction 的归并循环要直接调用它，而
// internal 包 import 不到根包。
//
// # 两条契约
//
//   - **Name() 是稳定的**：它会被写进 Manifest 校验目录与配置是否匹配。
//     换一套折叠语义读老目录，会造成"该折的没折、不该折的折了"——老目录里
//     未折叠的 operand 会按新语义折出错误的值。级别同 Comparer.Name。
//   - **算子必须满足结合律**，否则禁止实现 PartialMerge（只能返回 ok=false）。
//     PartialMerge 的结果会再参与后续折叠，不满足结合律时它会与 FullMerge
//     的结果不一致，而引擎无法替上层发现这件事（docs/EXTENSIONS.md §4.3）。
//
// 调用时机：FullMerge 在读路径（Get / 迭代器）与 Compaction 折叠时调用；
// PartialMerge 只在 Compaction 折叠时调用，用于减少 FullMerge 的 operand
// 数量。两者都在后台线程或读路径上被调用，实现必须并发安全。
type MergeOperator interface {
	// Name 返回算子的稳定标识，语义同 Comparer.Name。
	Name() string
	// FullMerge 把 base 与一串 operand 折叠成一个值。
	//
	// base 为 nil 表示没有 base（key 从未被 Put 过，或已被 Delete）；
	// operands 按 seq **从旧到新**排列，FullMerge 必须按这个顺序应用。
	// 返回 error ⇒ 读路径把错误交给调用方，Compaction 侧整体失败并停库
	// —— 折叠失败意味着引擎无法给出"这个 key 现在的值"，静默吞掉等于
	// 返回错误的数据。
	FullMerge(userKey, base []byte, operands [][]byte) ([]byte, error)
	// PartialMerge 折叠两个 operand。ok=false 表示无法局部合并，
	// 引擎会原样保留两个 operand 等 FullMerge 处理。
	//
	// 只有满足结合律的算子才允许返回 ok=true（见类型注释）。
	PartialMerge(userKey, a, b []byte) (merged []byte, ok bool)
}
