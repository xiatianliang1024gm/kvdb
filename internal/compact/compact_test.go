package compact

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"kvdb/internal/key"
	"kvdb/internal/sst"
	"kvdb/internal/version"
)

// ── 测试脚手架 ────────────────────────────────────────────────────

// testComparer 是按字节序比较的 Comparer。整个测试里只用一个比较器实现，
// 免得"比较器不一致"这种问题混进来干扰判断。
type testComparer struct{}

func (testComparer) Compare(a, b []byte) int { return bytes.Compare(a, b) }
func (testComparer) Name() string            { return "kvdb.compact.test.bytes" }

// rec 是一条记录的可读形式，用来比对"归并之后剩下了什么"。
type rec struct {
	user string
	seq  uint64
	kind key.Kind
	val  string
}

func (r rec) String() string {
	kind := "V"
	if r.kind == key.TypeDeletion {
		kind = "D"
	}
	return fmt.Sprintf("%s#%d%s=%s", r.user, r.seq, kind, r.val)
}

// val 造一条普通记录。
func val(user string, seq uint64, value string) rec {
	return rec{user: user, seq: seq, kind: key.TypeValue, val: value}
}

// tomb 造一条墓碑（删除标记）。
func tomb(user string, seq uint64) rec {
	return rec{user: user, seq: seq, kind: key.TypeDeletion}
}

// harness 管着一个真实的数据目录、一个真实的 VersionSet，以及一批真实的 SST 文件。
//
// 这里刻意不造任何假对象：Compaction 的正确性几乎全在"和真实文件打交道"的细节里
// （区间是否重叠、user key 有没有被切开、墓碑能不能丢），用 mock 换来的速度
// 抵不过它掩盖掉的 bug。
type harness struct {
	t       *testing.T
	dir     string
	icmp    key.InternalComparer
	vs      *version.VersionSet
	nextNum uint64
	readers map[uint64]*sst.Reader
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		t:       t,
		dir:     t.TempDir(),
		icmp:    key.InternalComparer{User: testComparer{}},
		readers: map[uint64]*sst.Reader{},
	}
	vs := version.New(version.Config{Dir: h.dir, Comparer: testComparer{}, MaxLevels: 7})
	if err := vs.SetFromScan(nil, 0); err != nil {
		t.Fatalf("SetFromScan: %v", err)
	}
	if err := vs.NewManifest(); err != nil {
		t.Fatalf("NewManifest: %v", err)
	}
	h.vs = vs
	t.Cleanup(func() {
		// 先关句柄再让 t.TempDir 去删目录：Windows 上打开着的文件删不掉。
		for _, r := range h.readers {
			_ = r.Close()
		}
		_ = vs.Close()
	})
	return h
}

// write 把一组记录写成一个真实的 SST，返回它的元信息。
//
// 记录必须按 internal key 升序给出（user key 升序，同一 user key 内 seq 降序），
// 这是 SST 的硬性要求，也是归并的前提。
func (h *harness) write(entries ...rec) *version.FileMeta {
	h.t.Helper()
	if len(entries) == 0 {
		h.t.Fatal("至少要写一条记录")
	}
	h.nextNum++
	num := h.nextNum
	h.vs.RaiseNextFileNum(num + 1) // 编号空间在 SST 与日志/Manifest 之间共享

	w, err := sst.NewWriter(sst.FilePath(h.dir, num), testComparer{}, sst.WriterOptions{
		BlockSize:       256,
		BloomBitsPerKey: 10,
	})
	if err != nil {
		h.t.Fatalf("NewWriter: %v", err)
	}
	for _, e := range entries {
		if err := w.Add(encode(e), []byte(e.val)); err != nil {
			w.Abandon()
			h.t.Fatalf("写入 %s 失败: %v", e, err)
		}
	}
	if err := w.Finish(); err != nil {
		h.t.Fatalf("Finish: %v", err)
	}
	last := entries[len(entries)-1]
	return &version.FileMeta{
		Num:      num,
		Size:     uint64(w.Size()),
		Smallest: encode(entries[0]),
		Largest:  encode(last),
	}
}

func encode(r rec) []byte {
	return key.EncodeInternalKey([]byte(r.user), r.seq, r.kind)
}

// apply 把一批文件变更提交进版本。
func (h *harness) apply(edits ...version.FileEdit) {
	h.t.Helper()
	if err := h.vs.LogAndApply(&version.VersionEdit{Added: edits}); err != nil {
		h.t.Fatalf("LogAndApply: %v", err)
	}
}

func (h *harness) reader(num uint64) (*sst.Reader, error) {
	if r, ok := h.readers[num]; ok {
		return r, nil
	}
	r, err := sst.Open(sst.FilePath(h.dir, num), sst.OpenOptions{Comparer: testComparer{}, FileNum: num})
	if err != nil {
		return nil, err
	}
	h.readers[num] = r
	return r, nil
}

// env 造一个"提交到 h.vs"的 Env；commit 为 nil 时用默认的提交逻辑。
func (h *harness) env(commit func(*Compaction, []*version.FileMeta) error) Env {
	if commit == nil {
		commit = func(c *Compaction, outputs []*version.FileMeta) error {
			return h.vs.LogAndApply(commitEdit(c, outputs))
		}
	}
	return Env{
		Dir:             h.dir,
		ICmp:            h.icmp,
		BlockSize:       256,
		BloomBitsPerKey: 10,
		TargetFileSize:  1 << 20,
		AllocFileNum:    h.vs.AllocFileNum,
		Reader:          h.reader,
		Commit:          commit,
	}
}

// run 在当前版本上跑一次 Compaction 并提交结果。
func (h *harness) run(c *Compaction, smallestSnapshot uint64) (Result, []*version.FileMeta) {
	h.t.Helper()
	h.vs.RaiseNextFileNum(h.nextNum + 1)
	var committed []*version.FileMeta
	env := h.env(func(_ *Compaction, outputs []*version.FileMeta) error {
		committed = outputs
		return h.vs.LogAndApply(commitEdit(c, outputs))
	})
	env.SmallestSnapshot = smallestSnapshot

	v := h.vs.Current()
	defer v.Unref()
	res, err := Run(c, v, env)
	if err != nil {
		h.t.Fatalf("Run(%s): %v", c, err)
	}
	return res, committed
}

// commitEdit 把一次 Compaction 的结果翻译成一个版本变更：删掉全部输入、加上全部输出。
func commitEdit(c *Compaction, outputs []*version.FileMeta) *version.VersionEdit {
	e := &version.VersionEdit{}
	for _, f := range c.Inputs[0] {
		e.Deleted = append(e.Deleted, version.FileEdit{Level: c.Level, Num: f.Num})
	}
	for _, f := range c.Inputs[1] {
		e.Deleted = append(e.Deleted, version.FileEdit{Level: c.OutputLevel, Num: f.Num})
	}
	for _, f := range outputs {
		e.Added = append(e.Added, f.Edit(c.OutputLevel))
	}
	return e
}

// dump 读回一个文件里的全部记录。
func (h *harness) dump(num uint64) []rec {
	h.t.Helper()
	r, err := h.reader(num)
	if err != nil {
		h.t.Fatalf("打开 sst %d: %v", num, err)
	}
	var out []rec
	if err := r.Iterate(func(ik, value []byte) bool {
		out = append(out, rec{
			user: string(key.UserKey(ik)),
			seq:  key.SeqNum(ik),
			kind: key.KindOf(ik),
			val:  string(value),
		})
		return true
	}); err != nil {
		h.t.Fatalf("遍历 sst %d: %v", num, err)
	}
	return out
}

// flat 把一组输出文件里的记录拼成一个可比较的字符串列表。
func (h *harness) flat(outputs []*version.FileMeta) []string {
	var out []string
	for _, f := range outputs {
		for _, r := range h.dump(f.Num) {
			out = append(out, r.String())
		}
	}
	return out
}

// sstNames 列出目录里当前的 .sst 文件名（已排序）。
func (h *harness) sstNames() []string {
	h.t.Helper()
	entries, err := os.ReadDir(h.dir)
	if err != nil {
		h.t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), sst.Suffix) {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

func levelCfg(trigger int) LevelConfig {
	return LevelConfig{
		L0CompactionTrigger: trigger,
		MaxLevels:           7,
		LevelMaxBytes:       func(int) uint64 { return 0 },
		TargetFileSize:      func(int) uint64 { return 1 << 20 },
	}
}

// ── Picker ────────────────────────────────────────────────────────

// L0 按文件数触发：没到阈值就必须什么都不做。
// 这条是 M3 验收的核心 —— "L0 文件数稳定在阈值附近"靠的就是它不去做无谓的搬运。
func TestPickL0WaitsForTrigger(t *testing.T) {
	h := newHarness(t)
	cfg := levelCfg(4)

	files := []*version.FileMeta{
		h.write(val("a", 1, "a")),
		h.write(val("m", 1, "m")),
		h.write(val("z", 1, "z")),
	}
	for _, f := range files {
		h.apply(f.Edit(0))
	}

	v := h.vs.Current()
	defer v.Unref()
	if len(v.Files(0)) != 3 {
		t.Fatalf("L0 应当时 3 个文件，实际 %d", len(v.Files(0)))
	}
	if c := Pick(v, cfg); c != nil {
		t.Fatalf("L0 只有 3 个文件（阈值 %d）时不该触发: %s", cfg.L0CompactionTrigger, c)
	}
}

// L0 到阈值后触发，且只搬"最老的那个文件 + 与它区间重叠的同伴"，
// 再加上输出层里与它们重叠、必须一起归并的文件。
func TestPickL0TakesOldestAndOverlapping(t *testing.T) {
	h := newHarness(t)
	cfg := levelCfg(4)

	oldest := h.write(val("a", 1, "a"), val("b", 1, "b")) // 区间 [a, b]
	friend := h.write(val("b", 3, "b3"))                  // 与 oldest 重叠
	other := h.write(val("m", 1, "m"))                    // 不重叠
	last := h.write(val("z", 1, "z"))                     // 不重叠
	// 输出层里也要有一个重叠文件：漏掉它就会破坏"同层不重叠"。
	inL1 := h.write(val("a", 2, "a2"), val("c", 2, "c2"))

	h.apply(oldest.Edit(0), friend.Edit(0), other.Edit(0), last.Edit(0), inL1.Edit(1))

	v := h.vs.Current()
	defer v.Unref()
	c := Pick(v, cfg)
	if c == nil {
		t.Fatal("L0 有 4 个文件，应当触发")
	}
	if c.Level != 0 || c.OutputLevel != 1 {
		t.Errorf("触发的是 L%d→L%d, want L0→L1", c.Level, c.OutputLevel)
	}
	got := make([]uint64, 0, len(c.Inputs[0]))
	for _, f := range c.Inputs[0] {
		got = append(got, f.Num)
	}
	want := []uint64{oldest.Num, friend.Num}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Inputs[0] = %v, want %v（最老的文件 + 与它重叠的同伴）", got, want)
	}
	if len(c.Inputs[1]) != 1 || c.Inputs[1][0].Num != inL1.Num {
		t.Errorf("Inputs[1] 应当是与区间重叠的那个 L1 文件 %d，实际 %v", inL1.Num, c.Inputs[1])
	}
}

// L0 干净的时候，容量超标的层才轮到被搬：取该层最左边的文件。
func TestPickBySizeMovesLeftmostFileDown(t *testing.T) {
	h := newHarness(t)
	f1 := h.write(val("a", 1, "a"), val("c", 1, "c"))
	f2 := h.write(val("m", 1, "m"), val("o", 1, "o"))
	f3 := h.write(val("a", 1, "old"), val("o", 1, "old"))
	h.apply(f1.Edit(1), f2.Edit(1), f3.Edit(2))

	// L1 容量上限设成 1 字节，于是"L1 超了"必然成立，而 L2 不设上限。
	cfg := LevelConfig{
		L0CompactionTrigger: 4,
		MaxLevels:           4,
		LevelMaxBytes: func(level int) uint64 {
			if level == 1 {
				return 1
			}
			return 0
		},
		TargetFileSize: func(int) uint64 { return 1 << 20 },
	}

	v := h.vs.Current()
	defer v.Unref()
	c := Pick(v, cfg)
	if c == nil {
		t.Fatal("L1 超出容量上限，应当触发")
	}
	if c.Level != 1 || c.OutputLevel != 2 {
		t.Fatalf("触发的是 L%d→L%d, want L1→L2", c.Level, c.OutputLevel)
	}
	if len(c.Inputs[0]) != 1 || c.Inputs[0][0].Num != f1.Num {
		t.Errorf("应当只搬 L1 最左边的文件 %d，实际 %v", f1.Num, c.Inputs[0])
	}
	if len(c.Inputs[1]) != 1 || c.Inputs[1][0].Num != f3.Num {
		t.Errorf("L2 里区间重叠的文件 %d 必须一起归并，实际 %v", f3.Num, c.Inputs[1])
	}
}

// ── Run：丢掉看不见的旧版本 ───────────────────────────────────────

// 覆盖写留下的旧版本在归并时被真正删掉 —— 这是 Compaction 让空间涨得比写入量慢的原因。
func TestRunDropsCoveredVersions(t *testing.T) {
	h := newHarness(t)
	older := h.write(val("k", 1, "v1"), val("z", 1, "z1"))
	newer := h.write(val("k", 3, "v3"))
	h.apply(older.Edit(0), newer.Edit(0))

	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{older, newer}, nil}}
	// 没有任何快照还需要 seq<=3 之外的旧版本。
	res, outputs := h.run(c, 3)

	if res.DroppedRecords != 1 {
		t.Errorf("DroppedRecords = %d, want 1（k 的旧版本）", res.DroppedRecords)
	}
	if res.OutputRecords != 2 {
		t.Errorf("OutputRecords = %d, want 2", res.OutputRecords)
	}
	want := []string{"k#3V=v3", "z#1V=z1"}
	if got := h.flat(outputs); !reflect.DeepEqual(got, want) {
		t.Fatalf("归并结果 = %v, want %v", got, want)
	}
	if c := res.OutputFiles; c != 1 {
		t.Errorf("OutputFiles = %d, want 1", c)
	}
	if res.InputBytes == 0 || res.OutputBytes == 0 {
		t.Errorf("字节统计不该为 0：in=%d out=%d", res.InputBytes, res.OutputBytes)
	}
}

// 使命已经完成的墓碑被物理删除，输入文件全部消失（Delete 的物理删除就发生在这里）。
func TestRunRetiresObsoleteTombstone(t *testing.T) {
	h := newHarness(t)
	f := h.write(tomb("k", 2), val("k", 1, "old"))
	h.apply(f.Edit(0))

	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{f}, nil}}
	// 输出层是 L1，更深处（L2 以下）没有 k，所以墓碑没有遮蔽对象了。
	res, outputs := h.run(c, 2)

	if res.DroppedRecords != 2 {
		t.Errorf("DroppedRecords = %d, want 2（墓碑 + 被它遮住的旧值）", res.DroppedRecords)
	}
	if len(outputs) != 0 || res.OutputRecords != 0 || res.OutputFiles != 0 {
		t.Fatalf("什么都不该剩下：outputs=%v records=%d files=%d", outputs, res.OutputRecords, res.OutputFiles)
	}
	v := h.vs.Current()
	defer v.Unref()
	if v.FileCount() != 0 {
		t.Errorf("提交之后版本里不该还有文件：%s", v)
	}
}

// 更深处还躺着这个 key 时，墓碑必须保留 —— 丢了它，那个旧值就会"复活"。
func TestRunKeepsTombstoneAboveDeeperLevel(t *testing.T) {
	h := newHarness(t)
	deep := h.write(val("k", 1, "old"))
	mark := h.write(tomb("k", 5))
	h.apply(deep.Edit(2), mark.Edit(0))

	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{mark}, nil}}
	res, outputs := h.run(c, 5)

	if res.DroppedRecords != 0 {
		t.Errorf("DroppedRecords = %d, want 0", res.DroppedRecords)
	}
	if got, want := h.flat(outputs), []string{"k#5D="}; !reflect.DeepEqual(got, want) {
		t.Fatalf("归并结果 = %v, want %v（墓碑必须留下）", got, want)
	}
}

// 还活着的快照需要的旧版本一条都不能丢。
func TestRunRespectsSmallestSnapshot(t *testing.T) {
	h := newHarness(t)
	f := h.write(val("k", 3, "v3"), val("k", 1, "v1"))
	h.apply(f.Edit(0))

	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{f}, nil}}
	// 有一个快照还停在 seq=1，所以 k#1 仍然必须能被读到。
	res, outputs := h.run(c, 1)

	if res.DroppedRecords != 0 {
		t.Errorf("DroppedRecords = %d, want 0", res.DroppedRecords)
	}
	want := []string{"k#3V=v3", "k#1V=v1"}
	if got := h.flat(outputs); !reflect.DeepEqual(got, want) {
		t.Fatalf("归并结果 = %v, want %v", got, want)
	}
}

// ── Run：输出布局 ─────────────────────────────────────────────────

// 输出文件只能在 user key 边界上切分。
//
// 这条是"L1 以下同层不重叠"的前提：把一个 user key 的两个版本切进相邻两个文件，
// 两个文件的 user key 区间就重叠了，点查按索引只会打开其中一个，另一个里的可见版本
// 会被静默漏掉 —— 读不到数据，而且不报错。
func TestRunSplitsOutputAtUserKeyBoundary(t *testing.T) {
	h := newHarness(t)

	const keys, versions = 50, 3
	var entries []rec
	seq := uint64(1000)
	for i := 0; i < keys; i++ {
		user := fmt.Sprintf("k%03d", i)
		for j := 0; j < versions; j++ {
			entries = append(entries, val(user, seq, fmt.Sprintf("v%d", j)))
			seq--
		}
	}
	f := h.write(entries...)
	h.apply(f.Edit(0))

	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{f}, nil}}
	h.vs.RaiseNextFileNum(h.nextNum + 1)
	var committed []*version.FileMeta
	env := h.env(func(_ *Compaction, outputs []*version.FileMeta) error {
		committed = outputs
		return h.vs.LogAndApply(commitEdit(c, outputs))
	})
	// 没有任何快照，但这里刻意把阈值设成 0：让**每个 key 的多个版本都留下来**。
	// 只有这样，"同一个 user key 会不会被切进两个文件"才真的被覆盖到。
	env.SmallestSnapshot = 0
	env.TargetFileSize = 200 // 逼出一堆小输出文件

	v := h.vs.Current()
	defer v.Unref()
	res, err := Run(c, v, env)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(committed) <= 1 {
		t.Fatalf("目标文件只有 200 字节，应当切出多个输出文件，实际 %d 个", len(committed))
	}
	if want := keys * versions; res.OutputRecords != want {
		t.Errorf("OutputRecords = %d, want %d", res.OutputRecords, want)
	}

	owner := make(map[string]uint64, keys)
	count := make(map[string]int, keys)
	for _, out := range committed {
		rs := h.dump(out.Num)
		if len(rs) == 0 {
			t.Fatalf("输出文件 %d 是空的", out.Num)
		}
		// 元信息里的区间必须与文件内容严格对应：L1 以下的二分定位全靠它。
		if !bytes.Equal(out.Smallest, encode(rs[0])) {
			t.Errorf("文件 %d 的 Smallest 与首条记录不符", out.Num)
		}
		if !bytes.Equal(out.Largest, encode(rs[len(rs)-1])) {
			t.Errorf("文件 %d 的 Largest 与末条记录不符", out.Num)
		}
		for _, r := range rs {
			if prev, ok := owner[r.user]; ok && prev != out.Num {
				t.Fatalf("user key %s 被切进了 %d 与 %d 两个输出文件", r.user, prev, out.Num)
			}
			owner[r.user] = out.Num
			count[r.user]++
		}
	}
	if len(owner) != keys {
		t.Errorf("输出里覆盖了 %d 个 user key, want %d", len(owner), keys)
	}
	for user, n := range count {
		if n != versions {
			t.Fatalf("user key %s 的版本数 = %d, want %d（同一个 key 的版本必须整段待在一起）", user, n, versions)
		}
	}
	// 输出文件之间不许重叠。
	for i := 1; i < len(committed); i++ {
		if h.icmp.Compare(committed[i-1].Largest, committed[i].Smallest) >= 0 {
			t.Fatalf("输出 L%d 的第 %d 个与第 %d 个文件区间重叠", c.OutputLevel, i-1, i)
		}
	}
}

// 提交失败时，Run 必须把自己写出的半成品全部删掉。
//
// 不删的话目录里会留下一堆没有任何版本引用的 .sst：它们不仅白占磁盘，
// 还会让"目录里有多少文件"这个诊断信息失真。
func TestRunRemovesOutputsWhenCommitFails(t *testing.T) {
	h := newHarness(t)
	f := h.write(val("k", 1, "v1"), val("m", 1, "m1"))
	h.apply(f.Edit(0))
	before := h.sstNames()

	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{f}, nil}}
	env := h.env(func(*Compaction, []*version.FileMeta) error {
		return errors.New("模拟提交失败")
	})
	env.SmallestSnapshot = 1

	v := h.vs.Current()
	defer v.Unref()
	if _, err := Run(c, v, env); err == nil {
		t.Fatal("Commit 失败必须让 Run 也返回错误")
	}
	if after := h.sstNames(); !reflect.DeepEqual(after, before) {
		t.Fatalf("失败路径必须删掉自己写出的文件：before=%v after=%v", before, after)
	}
}

// ── Run：与版本配合的整体不变式 ───────────────────────────────────

// 一次真实的 L0→L1 归并之后，L0 清空、L1 只有一个区间正确的文件，
// 并且所有数据都能从合并后的版本里读到。
func TestRunRebuildsLevelLayout(t *testing.T) {
	h := newHarness(t)

	a := h.write(val("a", 1, "a1"), val("c", 1, "c1"))
	b := h.write(val("b", 5, "b5"))
	h.apply(a.Edit(0), b.Edit(0))

	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{a, b}, nil}}
	res, outputs := h.run(c, 1)

	if len(outputs) != 1 {
		t.Fatalf("OutputFiles = %d, want 1", len(outputs))
	}
	v := h.vs.Current()
	defer v.Unref()
	if got := len(v.Files(0)); got != 0 {
		t.Errorf("L0 应当被清空，实际还有 %d 个文件", got)
	}
	if v.Files(1)[0].Num != outputs[0].Num {
		t.Errorf("L1 里应当是刚写出的文件 %d，实际 %d", outputs[0].Num, v.Files(1)[0].Num)
	}

	// b#5 应当能定位到，a#1/c#1 也要在。
	if got := h.flat(outputs); !reflect.DeepEqual(got, []string{"a#1V=a1", "b#5V=b5", "c#1V=c1"}) {
		t.Fatalf("归并结果 = %v", got)
	}
	if res.OutputFiles != 1 || res.InputFiles != 2 {
		t.Errorf("文件数统计不对：in=%d out=%d", res.InputFiles, res.OutputFiles)
	}

	// 输出层的区间必须和输出层里的实际文件一致，否则二分定位会漏文件。
	target := key.SeekKey([]byte("b"), 1<<40)
	if f := v.FindFile(1, []byte("b"), target); f == nil || f.Num != outputs[0].Num {
		t.Errorf("按 b 查找 L1 定位到 %v, want %d", f, outputs[0].Num)
	}
}

// 输入层是 L0 时，迭代器必须按"编号大（新）在前"排列。
// 顺序错了，同一个 internal key 出现在两个文件里时就会让旧值赢 —— 静默地读到旧数据。
func TestRunPrefersNewerFileOnTiedKey(t *testing.T) {
	h := newHarness(t)
	// 两个文件里放**完全相同**的 internal key，但值不同。
	// 这种形态在真实数据里不会出现（同一个 seq 不会被写两次），
	// 但它是唯一能把"谁先被归并"这件事单独暴露出来的构造。
	older := h.write(val("k", 7, "from-old-file"))
	newer := h.write(val("k", 7, "from-new-file"))
	h.apply(older.Edit(0), newer.Edit(0))

	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{older, newer}, nil}}
	_, outputs := h.run(c, 7)

	got := h.flat(outputs)
	if len(got) != 1 {
		t.Fatalf("同一个 internal key 应当只留下一条，实际 %v", got)
	}
	if got[0] != "k#7V=from-new-file" {
		t.Fatalf("留下的是 %q，应当来自编号更大的那个文件", got[0])
	}
}
