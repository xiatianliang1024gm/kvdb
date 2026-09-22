package kvdb

import (
	"errors"

	"github.com/xiatianliang1024gm/kvdb/internal/compact"
)

// 本文件是 M8（docs/EXTENSIONS.md §4.3）新增的对外 API：Merge 算子。
//
// 它解决的是"读-改-写原子性"：INCR / APPEND / 计数器列这类操作，没有 Merge
// 时只能是"读出来 → 改好 → 写回去"，并发下要么加锁串行、要么乐观重试，且
// 每次自增都重写整个 value。Merge 把"改"的意图（operand）直接交给引擎追加，
// 读的时候再把 operand 链折叠成值；稳态下 Compaction 会把每个 key 折叠回
// 一条 Value，读路径的额外成本由后台抵消。
//
// 语义要点：
//
//   - operand 的追加是普通写：进 WriteBatch、走组提交、进 WAL 与 MemTable，
//     原子性与 Put 完全一致；
//   - 读路径命中 TypeMerge 后要**向下收集全部 operand** 直到遇到 Value /
//     Deletion 或数据尽头，再按 seq 从旧到新 FullMerge——读变贵是 Merge 的
//     固有代价，Compaction 折叠才是这个特性的收益所在；
//   - Name() 写进 Manifest：目录一旦用某个算子落过 merge 记录，之后只能用
//     同名算子打开（换名或去掉都会被拒），语义同 Comparer / CompactionFilter。
//     注意方向的差异：一个从未写过 merge 记录的目录可以随时配上算子；
//     但写过之后摘掉算子是事故 —— 没有算子就读不回那些 key 的值。

// MergeOperator 把一串 merge operand 折叠成一个值。
//
// 实现必须满足结合律（否则禁止实现 PartialMerge，只能返回 ok=false）；
// Name() 必须稳定，会被写进 Manifest 校验。完整契约见 compact.MergeOperator。
type MergeOperator = compact.MergeOperator

// ErrNoMergeOperator 表示读到了 merge operand，但打开数据库时没有配置
// Options.MergeOperator。
//
// 正常情况下到不了这里：写过 merge 记录的目录在 Manifest 里记着算子的名字，
// 不带同名算子打开会被拒绝。它是一道运行时防线 —— 挡住"Manifest 校验被绕过"
// （比如目录被手工改过）的情形，宁可报错也不返回错误的值。
var ErrNoMergeOperator = errors.New("kvdb: key contains merge operands but Options.MergeOperator is not configured")

// mergeOperatorName 返回写进 Manifest 的算子标识；未配置时为空。
func mergeOperatorName(op MergeOperator) string {
	if op == nil {
		return ""
	}
	return op.Name()
}

// Merge 追加一条 merge operand。operand 的语义由 Options.MergeOperator 解释，
// 引擎本身不理解它的内容——它只负责把 operand 排好队、在读取与 Compaction 时
// 交给算子折叠。
//
// 并发语义与 Put 相同：这条 operand 与同批次的其他记录原子生效。
// 未配置 MergeOperator 时返回 ErrNoMergeOperator（写之前就拒绝，而不是等读的时候才发现）。
func (db *DB) Merge(userKey, operand []byte) error {
	if db.opts.MergeOperator == nil {
		return ErrNoMergeOperator
	}
	b := batchPool.Get().(*WriteBatch)
	defer putBatch(b)
	if err := b.Merge(userKey, operand); err != nil {
		return err
	}
	return db.Write(b)
}
