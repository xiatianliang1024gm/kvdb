package compact

import (
	"fmt"
	"os"
	"sort"

	"kvdb/internal/compress"
	"kvdb/internal/iterator"
	"kvdb/internal/key"
	"kvdb/internal/rate"
	"kvdb/internal/sst"
	"kvdb/internal/version"
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
	covered := false

	for mi.SeekToFirst(); mi.Valid(); mi.Next() {
		ik := mi.Key()
		uk := key.UserKey(ik)

		if lastUserKey == nil || env.ICmp.CompareUser(uk, lastUserKey) != 0 {
			lastUserKey = append(lastUserKey[:0], uk...)
			covered = false
		}
		if covered {
			res.DroppedRecords++
			continue
		}
		if key.SeqNum(ik) <= env.SmallestSnapshot {
			covered = true
			if key.KindOf(ik) == key.TypeDeletion && base.isBaseLevel(uk) {
				// 墓碑的唯一使命是遮蔽更旧的版本。此刻：更旧的版本已经被 covered 丢掉，
				// 输出层以下也没有这个 key 需要遮蔽 —— 墓碑本身可以消失了。
				// 不满足这个条件时保守保留：把墓碑留下只会浪费一点空间，
				// 丢掉一个还需要的墓碑则会让人读到本该已删除的旧值。
				res.DroppedRecords++
				continue
			}
		}
		res.InputRecords++
		if err := out.add(ik, mi.Value()); err != nil {
			return res, err
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
				return nil, fmt.Errorf("kvdb/compact: open input file %d: %w", f.Num, err)
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
