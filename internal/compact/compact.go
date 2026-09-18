// Package compact 实现 Compaction 的"选谁"与"怎么合"。
//
// 分两块：
//
//	Pick  挑出一组输入文件（Picker）。触发条件是分层的：
//	      L0 按**文件数**触发（它的区间互相重叠，文件数本身就是读放大的度量），
//	      L1 以下按**容量**触发（每层容量约为上一层的 LevelSizeMultiplier 倍）。
//	Run   多路归并这组输入，写出下一层的新文件，并去掉已经没有读者需要的旧版本。
//
// 为什么 L0 和 L1+ 的触发条件不同，这是整个 LSM 设计里最实际的一条经验：
// L0 的文件是 MemTable 直接落盘出来的，彼此区间重叠，一次点查必须逐个扫；
// 所以"L0 有几个文件"直接等于"最坏情况下要探几次元数据"。而 L1 以下同层不重叠，
// 一次点查每层只会碰到一个文件，真正决定成本的是每层的**容量**是否失控。
package compact

import (
	"fmt"

	"kvdb/internal/key"
	"kvdb/internal/version"
)

// LevelConfig 收集分层策略。
type LevelConfig struct {
	// L0CompactionTrigger 是 L0 文件数达到即触发 Compaction 的阈值。
	L0CompactionTrigger int
	// MaxLevels 是层数（含 L0）。
	MaxLevels int
	// LevelMaxBytes 返回第 level 层的容量上限（字节）。L0 返回 0，表示没有容量上限。
	LevelMaxBytes func(level int) uint64
	// TargetFileSize 返回写进第 level 层的单个输出文件的目标字节数。
	TargetFileSize func(level int) uint64
}

// Compaction 描述一次 Compaction 的输入集合。
type Compaction struct {
	// Level 是输入层，OutputLevel 恒为 Level+1。
	//
	// 每次只往下走一层，是刻意的：一次跨多层的归并会把写入量放大到不可控，
	// 而且中间层的旧版本会同时被草掉，出错时无法定位是哪一层的问题。
	Level, OutputLevel int

	// Inputs[0] 是输入层里被选中的文件。
	// Inputs[1] 是输出层里与它们区间重叠、必须一起参与归并的文件。
	//
	// Inputs[1] 不能少：输出文件的 key 区间由两边的并集决定，漏掉任何一个重叠文件，
	// 输出就会和它重叠，L1 以下"同层不重叠"的不变式当场破掉。
	Inputs [2][]*version.FileMeta
}

// InputFiles 返回全部输入文件。
func (c *Compaction) InputFiles() []*version.FileMeta {
	out := make([]*version.FileMeta, 0, len(c.Inputs[0])+len(c.Inputs[1]))
	out = append(out, c.Inputs[0]...)
	return append(out, c.Inputs[1]...)
}

// InputBytes 返回输入文件的总字节数（写放大的分母）。
func (c *Compaction) InputBytes() uint64 {
	var total uint64
	for _, f := range c.InputFiles() {
		total += f.Size
	}
	return total
}

// String 返回可读描述。
func (c *Compaction) String() string {
	return fmt.Sprintf("L%d→L%d: %d+%d files, %d bytes",
		c.Level, c.OutputLevel, len(c.Inputs[0]), len(c.Inputs[1]), c.InputBytes())
}

// Pick 在当前版本上挑出一次 Compaction；没有需要做的就返回 nil。
//
// 顺序上先看 L0：L0 的分数涨得最快（一次 Flush 就多一个文件，而点查要为它
// 多探一次元数据），而且 L0 不收拾干净，L1 的容量压力也降不下来。
func Pick(v *version.Version, cfg LevelConfig) *Compaction {
	if c := pickL0(v, cfg); c != nil {
		return c
	}
	return pickBySize(v, cfg)
}

// pickL0 在 L0 文件数达到阈值时，挑出"最老的那个文件以及与它重叠的同伴"。
//
// 只取最老的文件及其重叠文件，而不是把整个 L0 一次性搬空：
//   - 一次 Compaction 的代价与输入字节数成正比，取最小可用集合才能让写入停顿可控；
//   - 每做一次，L0 至少减少一个文件，进度是单调的，不会出现"越搬越多"；
//   - L0 的文件按编号升序排列（编号大 = 新），所以 Inputs[0][0] 就是最老的那个。
func pickL0(v *version.Version, cfg LevelConfig) *Compaction {
	files := v.Files(0)
	if cfg.L0CompactionTrigger <= 0 || len(files) < cfg.L0CompactionTrigger {
		return nil
	}
	oldest := files[0]
	icmp := v.Comparer()

	inputs := []*version.FileMeta{oldest}
	for _, f := range files[1:] {
		if icmp.Compare(f.Largest, oldest.Smallest) >= 0 && icmp.Compare(f.Smallest, oldest.Largest) <= 0 {
			inputs = append(inputs, f)
		}
	}
	smallest, largest := span(icmp, inputs)
	return &Compaction{
		Level:       0,
		OutputLevel: 1,
		Inputs: [2][]*version.FileMeta{
			inputs,
			v.Overlapping(1, smallest, largest),
		},
	}
}

// pickBySize 在 L1 以下找出第一个超出容量上限的层。
//
// 取该层最左边的文件：所有候选的"性价比"是一样的（都只是把这一层的一小段搬到下一层），
// 而从最左边开始能保证每次选到的区间互不相同，避免反复搬运同一段 key。
// LevelDB 用 compact pointer 轮转来让各段更均匀，那属于把它做"更平"的优化，
// 对正确性与收敛性都不是必需的（每次搬运都会让这一层的字节数下降）。
func pickBySize(v *version.Version, cfg LevelConfig) *Compaction {
	for level := 1; level < cfg.MaxLevels-1; level++ {
		limit := cfg.LevelMaxBytes(level)
		if limit == 0 || v.LevelBytes(level) <= limit {
			continue
		}
		files := v.Files(level)
		if len(files) == 0 {
			continue
		}
		f := files[0]
		return &Compaction{
			Level:       level,
			OutputLevel: level + 1,
			Inputs: [2][]*version.FileMeta{
				{f},
				v.Overlapping(level+1, f.Smallest, f.Largest),
			},
		}
	}
	return nil
}

// span 返回一组文件覆盖的 internal key 区间。
func span(icmp key.InternalComparer, files []*version.FileMeta) (smallest, largest []byte) {
	for _, f := range files {
		if smallest == nil || icmp.Compare(f.Smallest, smallest) < 0 {
			smallest = f.Smallest
		}
		if largest == nil || icmp.Compare(f.Largest, largest) > 0 {
			largest = f.Largest
		}
	}
	return smallest, largest
}
