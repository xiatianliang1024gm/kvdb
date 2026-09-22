package compact

import (
	"fmt"
	"os"
	"sort"

	"github.com/xiatianliang1024gm/kvdb/internal/compress"
	"github.com/xiatianliang1024gm/kvdb/internal/iterator"
	"github.com/xiatianliang1024gm/kvdb/internal/key"
	"github.com/xiatianliang1024gm/kvdb/internal/rate"
	"github.com/xiatianliang1024gm/kvdb/internal/sst"
	"github.com/xiatianliang1024gm/kvdb/internal/version"
)

// rateChargeChunk 是限流计费的粒度：累计读写多少字节之后向限流器申请一次配额。
//
// 为什么不按记录计费：限流器要把并发的申请串行化，粒度过细会让它自己变成瓶颈
// （一次 Compaction 可能要过几百万条记录）。256KB 是一个折中 —— 在 10MB/s 的
// 配额下约每 25ms 停顿一次，既平滑到"不会一口气吃掉几百 MB 带宽"，
// 又粗到"每条记录只加一次整数加法"。
//
// 代价是**尾部不足一块的部分不会被计费**：一次只搬 100KB 的 Compaction 完全
// 不受限。对小 Compaction 来说这无关紧要（它们的绝对带宽本来就低），
// 而每一次归并结束时会补交剩余额度（见 rateAccount.flush）。
const rateChargeChunk = 256 << 10

// Env 是执行一次 Compaction 需要的外部能力。
//
// Compaction 不做任何并发决策，也不碰锁：选输入、开文件、写文件、提交版本
// 这四件事都由调用方（DB）串起来。这样"归并逻辑"可以脱离整个引擎单独测试，
// 也避免了"后台线程持着 DB 锁做磁盘 IO"这类难查的问题。
type Env struct {
	// Dir 是数据目录。
	Dir string
	// ICmp 是 internal key 比较器，必须与这些文件所在的版本用的是同一份。
	ICmp key.InternalComparer
	// BlockSize / BloomBitsPerKey 透传给输出的 SST。
	BlockSize       int
	BloomBitsPerKey int
	// Compression 是输出 SST 的块压缩算法（零值 = 不压缩）。
	//
	// 它只影响**输出**：输入文件按各自块尾的类型字节自行解压，
	// 所以一次 Compaction 完全可以把"未压缩的老文件"搬成"压缩的新文件"，
	// 顺带完成格式升级。
	Compression compress.Type
	// RateLimiter 限制本次 Compaction 的读写带宽；nil 表示不限。
	//
	// 它**不挂在读取器上**，而是包在输入迭代器外面：输入文件的 Reader 是全库共享的
	// （前台的 Get 也会用它），把限流挂在那里会连前台点查一起限掉，
	// 那就完全违背了"限流的目的是保护前台"这件事。
	RateLimiter *rate.Limiter
	// TargetFileSize 是本次输出层单个文件的目标字节数。
	TargetFileSize uint64
	// BlockStats 非 nil 时累计输出文件的块压缩规模。
	BlockStats *sst.BlockStats

	// SmallestSnapshot 是"还可能有读者"的最小序列号：seq <= 它的旧版本可以丢弃。
	//
	// 取的是**所有存活快照里最小的那个**（没有快照时取当前序列号）。这两个选择都安全：
	// 没有快照时，任何新读者看到的都是最新版本，所以更旧的版本不可能被读到；
	// 有快照时，取最小值保证每个快照仍然能看到它该看到的那一版。
	//
	// 至于"比它更新的版本"为什么不丢：那些版本正是所有人现在会读到的东西。
	SmallestSnapshot uint64

	// Filter 非 nil 时，对每个 user key 的"最新可见版本"（seq <= SmallestSnapshot
	// 的第一条记录，且类型为 TypeValue）调用一次。判为 Drop 的记录转成墓碑语义：
	// 输出层以下还有这个 key 就写一条同序列号的 TypeDeletion，否则整条丢掉。
	//
	// 之所以卡在这个位置，是它与快照隔离唯一自洽的交点：seq 更大的记录可能是
	// 某些读者的"现在"，引擎不替用户做丢弃决定；而 seq 更小的旧版本早就被
	// covered 丢掉了，轮不到过滤器。代价是一个长期存活的老快照会让过滤器
	// 大面积失效 —— 这与"快照钉住 GC"是同一件事的两个面。
	Filter Filter

	// Merge 非 nil 时启用 Merge 折叠（M8）：同 key 的 merge operand 链被
	// 收集折叠成一条 TypeValue，稳态下每个 key 只剩一条 Value，读路径的
	// "收集 operand"成本由这里抵消。
	//
	// 与 Filter 的先后顺序是固定的：**先折叠、再过滤**——filter 看到的是
	// 折叠后的值，不必理解 operand 语义。目录里有 merge 记录就必然配了算子
	//（Manifest 校验保证），这里 nil 只可能是程序缺陷，遇到直接报错。
	Merge MergeOperator

	// RangeDeletions 是当前版本的范围墓碑表（M7，只读）。归并循环用它做两件事：
	//
	//   - **遮蔽丢弃**：seq <= SmallestSnapshot 的记录若被某条墓碑盖住
	//     （record.seq < T.Seq <= SmallestSnapshot），整条丢掉、连墓碑都不用写——
	//     墓碑本身挂在 Version 上，对更深层的旧数据继续生效。这是范围删除
	//     唯一真正回收空间的时刻；
	//   - 退休不在 Run 里做：能否退休取决于"区间内是否还有数据"，而 MemTable
	//     与 DB 的当前版本只有 DB 知道，由 commitCompaction 提交后统一判定。
	RangeDeletions []key.RangeDeletion

	// AllocFileNum 分配一个新的文件编号。
	AllocFileNum func() uint64
	// Reader 返回一个已提交文件号的读取器。
	Reader func(num uint64) (*sst.Reader, error)
	// Commit 把输出文件安装进版本树、删掉输入文件。
	//
	// 调用它时输出文件已经写完并 fsync 过；它返回错误则本次 Compaction 整体作废，
	// Run 负责把半成品文件删掉。
	Commit func(c *Compaction, outputs []*version.FileMeta) error
}

// rateAccount 是一次 Compaction 的带宽账本：输入与输出共用一个限流器，
// 但各自累计自己的零头，最后统一补交。
//
// 用一个结构体而不是两个局部变量，是因为它要同时被"输入迭代器包装"和
// "输出文件集合"两条路径共享。
type rateAccount struct {
	lim *rate.Limiter
	// pending 是还没申请配额的字节数（不足一个 rateChargeChunk）。
	input, output int64
}

// chargeInput 记下输入侧读到的字节。
func (a *rateAccount) chargeInput(n int64) {
	if a == nil || a.lim == nil || n <= 0 {
		return
	}
	a.input += n
	if a.input >= rateChargeChunk {
		a.lim.Request(int(a.input))
		a.input = 0
	}
}

// chargeOutput 记下输出侧写出的字节。
func (a *rateAccount) chargeOutput(n int64) {
	if a == nil || a.lim == nil || n <= 0 {
		return
	}
	a.output += n
	if a.output >= rateChargeChunk {
		a.lim.Request(int(a.output))
		a.output = 0
	}
}

// flush 补交两侧的零头。一次 Compaction 结束时调用一次。
//
// 不做这一步的话，一次"刚好搬了 200KB"的 Compaction 会完全不受限 ——
// 零头永远不结算，限流就只在长时间负载下才生效。
func (a *rateAccount) flush() {
	if a == nil || a.lim == nil {
		return
	}
	if total := a.input + a.output; total > 0 {
		a.lim.Request(int(total))
		a.input, a.output = 0, 0
	}
}

// throttledIterator 给一个输入迭代器套上限流：每读出一条记录就记一次账。
//
// 计费在"移动之后"发生，于是每条记录恰好被计一次（SeekToFirst 计第一条，
// 之后每次 Next 计新到位的那一条）。记账本身只是一次整数加法，
// 真正会阻塞的申请被攒到 256KB 才发生一次。
type throttledIterator struct {
	iterator.Iterator
	account *rateAccount
}

func (t *throttledIterator) SeekToFirst() {
	t.Iterator.SeekToFirst()
	t.charge()
}

func (t *throttledIterator) Seek(target []byte) {
	t.Iterator.Seek(target)
	t.charge()
}

func (t *throttledIterator) Next() {
	t.Iterator.Next()
	t.charge()
}

// charge 把当前记录的 key + value 长度记进账本。
func (t *throttledIterator) charge() {
	if t.account == nil || t.account.lim == nil || !t.Iterator.Valid() {
		return
	}
	t.account.chargeInput(int64(len(t.Iterator.Key()) + len(t.Iterator.Value())))
}

// Result 汇报一次 Compaction 的规模，是读写放大统计的数据来源。
type Result struct {
	InputFiles     int
	OutputFiles    int
	InputBytes     uint64
	OutputBytes    uint64
	InputRecords   int
	OutputRecords  int
	DroppedRecords int
}

// Run 执行一次 Compaction：多路归并输入文件，写出下一层的新文件，提交版本变更。
//
// 归并过程中做两件事，它们是 Compaction 存在的全部意义：
//
//  1. **把 L0 的重叠区间收敛掉** —— 输出写进 L1 以下，同层文件区间互不重叠，
//     于是点查每层只需要一次二分定位，而不是逐个文件试探；
//  2. **丢掉不会再被任何读者看见的旧版本** —— 覆盖写留下的老值、以及使命已经完成的墓碑，
//     都在这里被真正删除。Delete 语句的物理删除就发生在这一步。
func Run(c *Compaction, v *version.Version, env Env) (Result, error) {
	res := Result{InputFiles: len(c.Inputs[0]) + len(c.Inputs[1]), InputBytes: c.InputBytes()}

	// 带宽账本：输入与输出共用一份限流器的配额，收尾时补交零头。
	account := &rateAccount{lim: env.RateLimiter}
	defer account.flush()

	children, err := inputIterators(c, env, account)
	if err != nil {
		return res, err
	}
	mi := iterator.NewMerging(env.ICmp, children...)

	out := &outputSet{env: env, level: c.OutputLevel, account: account}
	committed := false
	defer func() {
		if !committed {
			out.abort()
		}
	}()

	base := newBaseLevelChecker(env.ICmp, v, c.OutputLevel)
	var lastUserKey []byte
	// covered 表示"当前 user key 最新可见的那个版本已经写出去了"，之后同 key 的更旧版本
	// 谁也读不到，可以整段丢掉。归并流在同一 user key 内按 seq 降序排列，
	// 所以这个状态只需要一个布尔量。
	//
	// M8 起置位规则收窄：**只在写出 TypeValue / TypeDeletion 时置位**。
	// Merge operand 必须收集齐才能折叠，链没走完之前不能宣布"这个 key 的
	// 答案已经写出去"——沿用旧规则会把还没折叠的 operand 当旧版本丢掉。
	covered := false

	// foldMerge 收集并折叠从 mi 当前位置开始的 merge operand 链（M8）。
	// 调用时 mi 停在链首：该 user key 第一条 seq <= SmallestSnapshot 且未被
	// 遮蔽的 TypeMerge 记录。链首之下所有记录都 <= SmallestSnapshot（同 key
	// 按 seq 降序），链首之上若有更新的 operand，它们已在主循环里原样透传，
	// 读路径会把它们叠在本次折叠出的 Value 之上，语义自洽。
	//
	// 消费规则：mi 一路推进到"下一条待处理记录"——链内每条记录（含终局记录）
	// 都被消费掉，最终停在下一个 user key 的第一条记录、终局之后的第一条
	// 同 key 旧记录、或流尽头。调用方在本层不再推进（advance = false）。
	//
	// 产出：
	//   - 收齐 base（可见 Value；或可见 Deletion ⇒ base 为 nil，更旧的记录
	//     对任何存活快照都作废）⇒ 折叠成一条 seq = seqFirst 的 TypeValue，
	//     先过 filter 再写出；
	//   - 没收齐 base ⇒ 只有输出层以下没有这个 key 的任何数据（isBaseLevel）
	//     时才允许折叠；否则 operand 原样透传——深层可能还有 base 或更旧的
	//     operand，现在折会把它们永久遮掉。透传保持 seq 降序，输出文件
	//     "同 key 版本相邻且降序"的排序前提不破坏。
	foldMerge := func(uk []byte, seqFirst uint64) error {
		if env.Merge == nil {
			return fmt.Errorf("kvdb/compact: merge record found but no merge operator configured")
		}
		// 归并流的 Key() 指向子迭代器的重建缓冲区，推进就会被覆盖；
		// 而折叠要等整条链收集完才发生，这里必须先复制。
		uk = append([]byte(nil), uk...)
		type operand struct {
			seq uint64
			val []byte
		}
		var operands []operand // 收集顺序：新 → 旧（与归并流一致）
		var baseVal []byte
		hasBase := false
		consumed := 0 // 收进折叠的记录数（operand + 终局；被遮蔽跳过的不算）
		for mi.Valid() {
			ik := mi.Key()
			cur := key.UserKey(ik)
			if env.ICmp.CompareUser(cur, uk) != 0 {
				break // 链在 key 边界结束：本输入内没有终局
			}
			seq := key.SeqNum(ik)
			res.InputRecords++
			if key.CoveredByRange(env.RangeDeletions, env.ICmp.CompareUser, cur, seq, env.SmallestSnapshot) {
				// 被遮蔽 = 对所有存活快照不可见，丢弃后链继续收。
				res.DroppedRecords++
				mi.Next()
				continue
			}
			consumed++
			switch key.KindOf(ik) {
			case key.TypeMerge:
				// 先试 PartialMerge：把更旧的这条并进已收下的最后一个 operand。
				// 参数顺序按"从旧到新应用"：v 更旧，operands[n-1] 更新。
				// 只有满足结合律的算子才敢返回 ok=true（MergeOperator 的契约），
				// ok=false 就原样保留两个 operand，等 FullMerge 一次折完。
				// value 指向块数据，链要跨多条记录收集，一律复制。
				v := append([]byte(nil), mi.Value()...)
				if n := len(operands); n > 0 {
					if m, ok := env.Merge.PartialMerge(uk, v, operands[n-1].val); ok {
						// 合并结果代表"截至 operands[n-1].seq 的全部叠加"，
						// 序列号沿用其中较新的那个。
						operands[n-1].val = m
						break
					}
				}
				operands = append(operands, operand{seq: seq, val: v})
			case key.TypeValue:
				baseVal, hasBase = append([]byte(nil), mi.Value()...), true
			case key.TypeDeletion:
				hasBase = true // base 为 nil：key 在链中间被删除
			}
			mi.Next()
			if hasBase {
				break
			}
		}

		if !hasBase && !base.isBaseLevel(uk) {
			for _, op := range operands {
				if err := out.add(key.EncodeInternalKey(uk, op.seq, key.TypeMerge), op.val); err != nil {
					return err
				}
			}
			return nil
		}

		// FullMerge 要求 operand 按 seq 从旧到新：反转收集顺序。
		vals := make([][]byte, len(operands))
		for i, op := range operands {
			vals[len(operands)-1-i] = op.val
		}
		merged, err := env.Merge.FullMerge(uk, baseVal, vals)
		if err != nil {
			return fmt.Errorf("kvdb/compact: merge operator %q: %w", env.Merge.Name(), err)
		}
		// 链里的每条记录都被这唯一一条输出取代。
		res.DroppedRecords += consumed
		// 折叠结果就是"这个 key 当前的值"：先过 filter（M6 与 M8 的顺序：
		// Merge 折叠 → filter 判定），filter 看到的已经是 Value 不是 operand。
		if env.Filter != nil {
			d, ferr := env.Filter.Filter(c.OutputLevel, uk, merged, seqFirst)
			if ferr != nil {
				return fmt.Errorf("kvdb/compact: compaction filter %q: %w", env.Filter.Name(), ferr)
			}
			if d == Drop {
				// 与 Value 分支的 Drop 同构：深层可能还有更旧版本时写一条
				// 同序列号的墓碑继续遮蔽，是 base level 才整条丢掉。
				if base.isBaseLevel(uk) {
					return nil
				}
				return out.add(key.EncodeInternalKey(uk, seqFirst, key.TypeDeletion), nil)
			}
		}
		return out.add(key.EncodeInternalKey(uk, seqFirst, key.TypeValue), merged)
	}

	// advance 表示"当前记录已处理完毕，推进归并流"。Merge 折叠会一次消费
	// 整个链并停在下一条待处理记录上，此时绝不能再推——否则会跳过下一条
	// 记录（它可能是下一个 user key 的第一条）。
	for mi.SeekToFirst(); mi.Valid(); {
		ik := mi.Key()
		uk := key.UserKey(ik)
		advance := true

		if lastUserKey == nil || env.ICmp.CompareUser(uk, lastUserKey) != 0 {
			lastUserKey = append(lastUserKey[:0], uk...)
			covered = false
		}

		seq := key.SeqNum(ik)
		kind := key.KindOf(ik)

		switch {
		case covered:
			res.DroppedRecords++
		case seq > env.SmallestSnapshot:
			// 比最小存活快照新的版本：可能有读者，原样保留。
			// merge operand 也在此列——它们会由读路径叠在折叠结果之上。
			res.InputRecords++
			if err := out.add(ik, mi.Value()); err != nil {
				return res, err
			}
		case key.CoveredByRange(env.RangeDeletions, env.ICmp.CompareUser, uk, seq, env.SmallestSnapshot):
			// 范围墓碑遮蔽（M7）：被盖住的记录对所有现存与未来的读者都不可见
			// （seq < T.Seq <= SmallestSnapshot），整条丢掉。不能置 covered：
			// 那条语义是"这个 key 的答案已经写出去了"，而这里什么都没写——
			// 后续同 key 记录还会逐条走到这一层，它们同样被盖住
			//（遮蔽条件随 seq 减小单调成立），同样丢掉。
			res.DroppedRecords++
		case kind == key.TypeMerge:
			// M8：收集并折叠整个 operand 链，产出一条 TypeValue（或透传）。
			advance = false
			if err := foldMerge(uk, seq); err != nil {
				return res, err
			}
			// 链内所有记录都已消费：本输入内这个 key 的答案已经定了，
			// 之后若还有同 key 旧记录（终局之后的残尾）全部按 covered 丢弃。
			covered = true
		default:
			// TypeValue / TypeDeletion：这个 key 的答案在这里。
			covered = true
			if kind == key.TypeValue && env.Filter != nil {
				d, ferr := env.Filter.Filter(c.OutputLevel, uk, mi.Value(), seq)
				if ferr != nil {
					return res, fmt.Errorf("kvdb/compact: compaction filter %q: %w", env.Filter.Name(), ferr)
				}
				if d == Drop {
					// filter 说丢，语义上等价于这条数据被删除。但"跳过不写"不够：
					// 更深层可能还躺着一个更旧的版本，丢掉本层的记录会让它复活。
					// 处理沿用墓碑的退休判据（与下面的 TypeDeletion 分支完全同构）：
					// 是 base level 就整条丢掉，否则写一条**同序列号**的墓碑继续
					// 遮蔽，交给以后的 Compaction 退休。
					if base.isBaseLevel(uk) {
						res.DroppedRecords++
						break
					}
					res.InputRecords++
					if err := out.add(key.EncodeInternalKey(uk, seq, key.TypeDeletion), nil); err != nil {
						return res, err
					}
					break
				}
			}
			if kind == key.TypeDeletion && base.isBaseLevel(uk) {
				// 墓碑的唯一使命是遮蔽更旧的版本。此刻：更旧的版本已经被 covered 丢掉，
				// 输出层以下也没有这个 key 需要遮蔽 —— 墓碑本身可以消失了。
				// 不满足这个条件时保守保留：把墓碑留下只会浪费一点空间，
				// 丢掉一个还需要的墓碑则会让人读到本该已删除的旧值。
				res.DroppedRecords++
				break
			}
			res.InputRecords++
			if err := out.add(ik, mi.Value()); err != nil {
				return res, err
			}
		}

		if advance {
			mi.Next()
		}
	}
	if err := mi.Error(); err != nil {
		return res, err
	}

	outputs, err := out.finish()
	if err != nil {
		return res, err
	}
	res.OutputFiles = len(outputs)
	res.OutputRecords = out.records
	for _, f := range outputs {
		res.OutputBytes += f.Size
	}
	if err := env.Commit(c, outputs); err != nil {
		return res, err
	}
	committed = true
	return res, nil
}

// inputIterators 按"新 → 旧"的顺序构造全部输入迭代器。
//
// 顺序不是随便定的：归并迭代器在两条记录的 internal key 完全相同时，按下标小的先输出。
// 同层同 key 的不同版本 seq 不同、internal key 也就不同，所以这个决胜规则只在
// "不同层出现同一个 internal key"时起作用 —— 那正是必须让新文件排在前面的时候。
//
// account 非 nil 时，每个子迭代器外面会套一层限流计费。
func inputIterators(c *Compaction, env Env, account *rateAccount) ([]iterator.Iterator, error) {
	first := append([]*version.FileMeta(nil), c.Inputs[0]...)
	if c.Level == 0 {
		// L0：区间互相重叠，必须严格"编号大（新）在前"。
		sort.Slice(first, func(i, j int) bool { return first[i].Num > first[j].Num })
	} else {
		// L1 以下同层不重叠，任一条记录只可能属于一个文件，顺序只影响确定性。
		sort.Slice(first, func(i, j int) bool { return env.ICmp.Compare(first[i].Smallest, first[j].Smallest) < 0 })
	}
	second := append([]*version.FileMeta(nil), c.Inputs[1]...)
	sort.Slice(second, func(i, j int) bool { return env.ICmp.Compare(second[i].Smallest, second[j].Smallest) < 0 })

	out := make([]iterator.Iterator, 0, len(first)+len(second))
	for _, group := range [][]*version.FileMeta{first, second} {
		for _, f := range group {
			r, err := env.Reader(f.Num)
			if err != nil {
				return nil, fmt.Errorf("github.com/xiatianliang1024gm/kvdb/compact: open input file %d: %w", f.Num, err)
			}
			var it iterator.Iterator = r.NewIterator()
			if account != nil && account.lim != nil {
				it = &throttledIterator{Iterator: it, account: account}
			}
			out = append(out, it)
		}
	}
	return out, nil
}

// outputSet 管理本次 Compaction 的输出文件：按目标大小切分、记录每个文件的 key 区间。
type outputSet struct {
	env     Env
	level   int
	account *rateAccount

	w     *sst.Writer
	num   uint64
	path  string
	first []byte // 当前文件的第一个 internal key
	last  []byte // 已写入的最后一个 internal key

	records int
	files   []*version.FileMeta
}

// add 写入一条记录，必要时先切出一个新文件。
func (o *outputSet) add(ik, value []byte) error {
	switch {
	case o.w == nil:
		if err := o.create(); err != nil {
			return err
		}
	case o.w.Size() >= int64(o.env.TargetFileSize) && !o.sameUserKey(ik):
		// **只在 user key 边界上切文件**。
		//
		// 输出文件会落到 L1 以下，那里"同层文件区间互不重叠"是二分定位的前提。
		// 如果把一个 user key 的两个版本切进相邻两个输出文件，两个文件的
		// user key 区间就重叠了：点查按索引只会打开其中一个，另一个里的可见版本
		// 就被静默漏掉。代价是单个文件可能略微超过目标大小，完全可接受。
		if err := o.closeCurrent(); err != nil {
			return err
		}
		if err := o.create(); err != nil {
			return err
		}
	}
	if err := o.w.Add(ik, value); err != nil {
		return err
	}
	o.account.chargeOutput(int64(len(ik) + len(value)))
	if o.first == nil {
		o.first = append([]byte(nil), ik...)
	}
	o.last = append(o.last[:0], ik...)
	o.records++
	return nil
}

// sameUserKey 判断 ik 与当前文件最后一条记录是否属于同一个 user key。
func (o *outputSet) sameUserKey(ik []byte) bool {
	if o.last == nil {
		return false
	}
	return o.env.ICmp.CompareUser(key.UserKey(ik), key.UserKey(o.last)) == 0
}

// create 开始一个新的输出文件。
func (o *outputSet) create() error {
	num := o.env.AllocFileNum()
	path := sst.FilePath(o.env.Dir, num)
	w, err := sst.NewWriter(path, o.env.ICmp.User, sst.WriterOptions{
		BlockSize:       o.env.BlockSize,
		BloomBitsPerKey: o.env.BloomBitsPerKey,
		Compression:     o.env.Compression,
		BlockStats:      o.env.BlockStats,
	})
	if err != nil {
		return err
	}
	o.w, o.num, o.path, o.first = w, num, path, nil
	return nil
}

// closeCurrent 结束当前输出文件并登记它的元信息。没有正在写的文件时是空操作。
func (o *outputSet) closeCurrent() error {
	if o.w == nil {
		return nil
	}
	if err := o.w.Finish(); err != nil {
		o.w.Abandon()
		o.w = nil
		return err
	}
	o.files = append(o.files, &version.FileMeta{
		Num:      o.num,
		Size:     uint64(o.w.Size()),
		Smallest: append([]byte(nil), o.first...),
		Largest:  append([]byte(nil), o.last...),
	})
	o.w = nil
	return nil
}

// finish 结束最后一个输出文件并返回全部输出文件的元信息。
//
// 返回空列表是合法结果：所有记录都已经没有读者时，这次 Compaction 的产物
// 就是"什么都不留下"，输入文件会被 Commit 一并删掉。
func (o *outputSet) finish() ([]*version.FileMeta, error) {
	if err := o.closeCurrent(); err != nil {
		return nil, err
	}
	return o.files, nil
}

// abort 删除本次 Compaction 已经写出的全部文件。
//
// 这一步不做的话，失败路径会在目录里留下一堆没有任何版本引用的 .sst。
// 它们最终会被"孤儿文件清理"扫掉，但那要等到下次打开数据库 —— 期间它们
// 不仅白占磁盘，还会让"目录里有多少文件"这个诊断信息变得不可信。
func (o *outputSet) abort() {
	if o.w != nil {
		o.w.Abandon()
		o.w = nil
	}
	for _, f := range o.files {
		_ = os.Remove(sst.FilePath(o.env.Dir, f.Num))
	}
	o.files = nil
}

// baseLevelChecker 判断"输出层以下的更深处是否还可能有这个 user key"。
//
// 只有它为真时，墓碑才能真正丢掉：墓碑的作用是遮蔽更旧的版本，如果更深处
// 还躺着一个更旧的值，把墓碑丢掉就等于让那个旧值"复活"。
//
// 实现用的是逐层游标：调用方保证 user key 单调递增，于是每层的游标只前进不后退，
// 整个判定是均摊 O(1) 的（每个文件在一层里最多被跳过一次），
// 不需要为每个 key 做一次二分。
type baseLevelChecker struct {
	icmp key.InternalComparer
	deep [][]*version.FileMeta
	ptrs []int
}

// newBaseLevelChecker 收集输出层以下所有非空的层。
func newBaseLevelChecker(icmp key.InternalComparer, v *version.Version, outputLevel int) *baseLevelChecker {
	b := &baseLevelChecker{icmp: icmp}
	for level := outputLevel + 1; level < v.NumLevels(); level++ {
		if files := v.Files(level); len(files) > 0 {
			b.deep = append(b.deep, files)
		}
	}
	b.ptrs = make([]int, len(b.deep))
	return b
}

// isBaseLevel 返回 true 表示更深处一定没有这个 user key。必须按 user key 升序调用。
func (b *baseLevelChecker) isBaseLevel(uk []byte) bool {
	for i, files := range b.deep {
		p := b.ptrs[i]
		for p < len(files) && b.icmp.CompareUser(key.UserKey(files[p].Largest), uk) < 0 {
			p++
		}
		b.ptrs[i] = p
		if p < len(files) && b.icmp.CompareUser(key.UserKey(files[p].Smallest), uk) <= 0 {
			return false
		}
	}
	return true
}
