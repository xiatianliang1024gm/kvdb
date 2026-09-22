package kvdb

import "github.com/xiatianliang1024gm/kvdb/internal/compact"

// 本文件是 M6（docs/EXTENSIONS.md §4.2）新增的对外 API：Compaction 过滤器。
//
// 它解决的是"过期的/作废的数据要能真正从磁盘消失"。读侧的惰性判断
// （读到再判过期）只让数据不可见，磁盘上的死数据永远不消失，老层文件里
// 塞满过期键，读放大只增不减。过滤器把清理时机挂在一个已有的事件上 ——
// 后台 Compaction 本来就要重写每一条记录，顺手问一句"这条还要吗"。
//
// 语义要点（详细论证见 EXTENSIONS.md）：
//
//   - 只对每个 user key 的**最新可见版本**调用一次，且仅限后台 Compaction
//     （Flush 侧需显式打开 Options.FilterOnFlush）；
//   - 判为 Drop 的记录按墓碑语义处理：更深层可能还有旧版本时写同序列号的
//     墓碑继续遮蔽，不会让旧值复活；
//   - 只对 seq <= 最小存活快照的记录生效。长期存活的快照会让过滤器大面积
//     失效 —— 上层要把快照当稀缺资源管理；
//   - Name() 写进 Manifest：目录一旦用某个过滤器的名字落过盘，之后只能用
//     同名过滤器打开，换名字（包括去掉过滤器）会报配置不匹配；
//   - Filter 返回 error ⇒ 本次 Compaction 失败、库停机。过滤器故障按引擎
//     故障处理，而不是静默跳过。

// Decision 是 CompactionFilter 对一条记录的裁决。
type Decision = compact.Decision

const (
	// Keep 保留这条记录，是过滤器缺省该返回的值。
	Keep Decision = compact.Keep
	// Drop 丢弃这条记录的值。引擎按墓碑语义处理，见包内 Decision 的说明。
	Drop Decision = compact.Drop
)

// CompactionFilter 在后台 Compaction 时对记录做"留 / 丢"判定，
// 典型用途是 TTL 的物理清除与租户/行级过期清理。
//
// 与 Comparer 一样，实现必须是**稳定**的：同一 (key, value, seq) 在任何时刻
// 得到的判定必须一致 —— 判定结果会影响数据是否落盘，"这次丢下次留"会让
// 行为不可预测。引擎不保证调用顺序与调用次数（同一条记录会随多次 Compaction
// 被多次判定），实现必须并发安全；对幂等的判定（如"按过期时间戳判断"）这是
// 自然的，对有状态的过滤器（如"已清理集合"）需要自己保证。
type CompactionFilter = compact.Filter

// compactionFilterName 返回写进 Manifest 的过滤器标识；未配置时为空。
func compactionFilterName(f CompactionFilter) string {
	if f == nil {
		return ""
	}
	return f.Name()
}
