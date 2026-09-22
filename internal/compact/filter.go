package compact

import "fmt"

// Decision 是 CompactionFilter 对一条记录的裁决。
//
// 只有两个取值是刻意的：过滤器是用户回调，却参与"数据是否还能被读到"的正确性，
// 可选项越多，出错的组合越多。连"改成另一个值"（Change）也明确不做 —— 那会让
// 用户回调决定写入内容，收益抵不过复杂度（见 docs/EXTENSIONS.md §7）。
type Decision int

const (
	// Keep 保留这条记录，是过滤器缺省该返回的值。
	Keep Decision = iota
	// Drop 丢弃这条记录的值。
	//
	// 引擎会把它转成**墓碑语义**而不是简单跳过：输出层之下如果还可能有这个
	// key 的更旧版本，就写一条同序列号的 TypeDeletion 继续遮蔽；确定没有
	// （isBaseLevel）才整条丢掉。直接跳过会让更深层的旧值"复活"。
	Drop
)

// String 返回裁决的可读名，用于日志与测试输出。
func (d Decision) String() string {
	switch d {
	case Keep:
		return "Keep"
	case Drop:
		return "Drop"
	default:
		return fmt.Sprintf("Decision(%d)", int(d))
	}
}

// Filter 是 Compaction 侧的记录过滤器，根包的 kvdb.CompactionFilter 与它是同一类型
//（type alias）。每条被它判为 Drop 的记录按墓碑语义处理（见 Decision.Drop）。
//
// 调用契约（引擎的承诺，也是实现方必须满足的前提）：
//
//   - 只在**该记录是当前 user key 的最新可见版本**时调用：seq <= SmallestSnapshot
//     的第一条记录。对已被覆盖的旧版本调用毫无意义，引擎保证不会发生；
//   - 在后台线程调用，不保证调用顺序，同一条记录可能因多次 Compaction 被调用
//     多次，实现必须并发安全且幂等；
//   - Filter 返回 error ⇒ 本次 Compaction 整体失败，库按后台故障停机。
//     过滤器的内部故障被当作引擎故障处理，而不是静默跳过；
//   - level 是**输出层**；seq 是记录序列号；value 只在调用期间有效，
//     需要保留请自行复制。
type Filter interface {
	// Name 返回过滤器的稳定标识。它会被写进 Manifest：目录一旦用某个名字
	// 落过盘，之后就必须用同名过滤器打开，语义同 Comparer.Name。
	Name() string
	// Filter 判定一条记录是否还要留下。
	Filter(level int, userKey, value []byte, seq uint64) (Decision, error)
}
