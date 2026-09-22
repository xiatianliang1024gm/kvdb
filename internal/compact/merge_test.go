package compact

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/xiatianliang1024gm/kvdb/internal/key"
	"github.com/xiatianliang1024gm/kvdb/internal/version"
)

// 这个文件是 M8（docs/EXTENSIONS.md §4.3）的 Compaction 侧折叠测试。
//
// db 层的测试（db_merge_test.go）验证"端到端语义正确"；这里用 harness 直接
// 控制输入文件与快照水位，验证折叠的每条规则：
//
//   - 有 base 的链折叠成一条 TypeValue（seq = 链首）；
//   - 链被 Deletion 终止时 base 为 nil，更旧的记录按 covered 丢弃；
//   - 没有 base 且更深处还有数据 ⇒ operand 原样透传（折叠会让深层 base 失联）；
//   - 没有 base 且是 base level ⇒ 允许折叠（base = nil）；
//   - 比最小存活快照新的 operand 原样保留，读路径把它们叠在折叠结果之上；
//   - 折叠结果先过 filter，Drop 转墓碑语义；
//   - FullMerge 报错 ⇒ Compaction 整体失败。

// testMergeOp 是计数器算子：value = 8 字节大端 uint64，FullMerge = 求和。
// 加法满足结合律，所以 PartialMerge 允许返回 ok=true。
type testMergeOp struct {
	name        string
	failFull    bool
	noPartial   bool
	partialUsed int
}

func (o *testMergeOp) Name() string {
	if o.name == "" {
		return "kvdb.compact.test.incr"
	}
	return o.name
}

func (o *testMergeOp) FullMerge(userKey, base []byte, operands [][]byte) ([]byte, error) {
	if o.failFull {
		return nil, errors.New("full merge boom")
	}
	var acc uint64
	if base != nil {
		acc = binary.BigEndian.Uint64(base)
	}
	for _, op := range operands {
		acc += binary.BigEndian.Uint64(op)
	}
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, acc)
	return out, nil
}

func (o *testMergeOp) PartialMerge(userKey, a, b []byte) ([]byte, bool) {
	if o.noPartial {
		return nil, false
	}
	o.partialUsed++
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, binary.BigEndian.Uint64(a)+binary.BigEndian.Uint64(b))
	return out, true
}

// merge 造一条 merge 记录。
func merge(user string, seq uint64, value string) rec {
	return rec{user: user, seq: seq, kind: key.TypeMerge, val: value}
}

// counter 造一个 8 字节大端的计数值（rec.val 是 string，直接按字节装）。
func counter(n uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return string(b[:])
}

// mflat 与 h.flat 相同，但给 Merge 记录打 M 标记（rec.String 只认 V/D）。
func (h *harness) mflat(outputs []*version.FileMeta) []string {
	var out []string
	for _, f := range outputs {
		for _, r := range h.dump(f.Num) {
			kind := "V"
			switch r.kind {
			case key.TypeDeletion:
				kind = "D"
			case key.TypeMerge:
				kind = "M"
			}
			out = append(out, r.user+"#"+uitoa(r.seq)+kind+"="+r.val)
		}
	}
	return out
}

func uitoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// runWith 在 h.run 的基础上允许注入 Merge 算子与 filter。
func (h *harness) runWith(c *Compaction, smallestSnapshot uint64, op *testMergeOp, filter Filter) (Result, []*version.FileMeta) {
	h.t.Helper()
	h.vs.RaiseNextFileNum(h.nextNum + 1)
	var committed []*version.FileMeta
	env := h.env(func(_ *Compaction, outputs []*version.FileMeta) error {
		committed = outputs
		return h.vs.LogAndApply(commitEdit(c, outputs))
	})
	env.SmallestSnapshot = smallestSnapshot
	env.Merge = op
	env.Filter = filter

	v := h.vs.Current()
	res, err := Run(c, v, env)
	v.Unref()
	if err != nil {
		h.t.Fatalf("Run(%s): %v", c, err)
	}
	return res, committed
}

// 链有 base：折叠成一条 seq = 链首 的 TypeValue，链内记录全部被这一条取代。
func TestRunFoldsMergeChainWithBase(t *testing.T) {
	h := newHarness(t)
	f := h.write(
		val("a", 1, "a1"),
		merge("k", 5, counter(4)), // 链首（最新）
		merge("k", 4, counter(3)),
		val("k", 3, counter(10)), // base
		val("z", 1, "z1"),
	)
	h.apply(f.Edit(0))

	op := &testMergeOp{}
	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{f}, nil}}
	res, outputs := h.runWith(c, 5, op, nil)

	// 10 + 3 + 4 = 17。
	want := []string{"a#1V=a1", "k#5V=" + counter(17), "z#1V=z1"}
	if got := h.mflat(outputs); !reflect.DeepEqual(got, want) {
		t.Fatalf("归并结果 = %v, want %v", got, want)
	}
	if res.OutputRecords != 3 {
		t.Errorf("OutputRecords = %d, want 3", res.OutputRecords)
	}
	if res.DroppedRecords != 3 {
		t.Errorf("DroppedRecords = %d, want 3（链内三条记录被一条输出取代）", res.DroppedRecords)
	}
	if op.partialUsed == 0 {
		t.Error("PartialMerge 应当至少被用过一次")
	}
}

// 链被 Deletion 终止：base 为 nil，Deletion 之下的旧值按 covered 丢弃。
// FullMerge(nil, operands) 说明"删完再叠"的语义成立。
func TestRunFoldsMergeChainEndedByDeletion(t *testing.T) {
	h := newHarness(t)
	f := h.write(
		merge("k", 5, counter(4)),
		tomb("k", 3),
		val("k", 1, counter(100)), // 已被墓碑删除，必须消失
	)
	h.apply(f.Edit(0))

	op := &testMergeOp{}
	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{f}, nil}}
	res, outputs := h.runWith(c, 5, op, nil)

	want := []string{"k#5V=" + counter(4)}
	if got := h.mflat(outputs); !reflect.DeepEqual(got, want) {
		t.Fatalf("归并结果 = %v, want %v", got, want)
	}
	// merge@5 与 tomb@3 被折叠取代（2），旧值被 covered 丢弃（1）。
	if res.DroppedRecords != 3 {
		t.Errorf("DroppedRecords = %d, want 3", res.DroppedRecords)
	}
}

// 没有 base 且更深处还有这个 key：operand 必须原样透传。
// 现在折叠成 Value 会把深层的 base 永久遮掉，是静默的数据错误。
func TestRunPassesThroughMergeChainAboveDeeperBase(t *testing.T) {
	h := newHarness(t)
	deep := h.write(val("k", 1, counter(100)))
	top := h.write(
		merge("k", 5, counter(4)),
		merge("k", 4, counter(3)),
	)
	h.apply(deep.Edit(2), top.Edit(0))

	op := &testMergeOp{}
	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{top}, nil}}
	res, outputs := h.runWith(c, 5, op, nil)

	// 原样透传：全部是 k 的 Merge 记录（PartialMerge 可能合并相邻 operand，
	// 但结果仍须是 Merge 记录且语义等价——这里算子允许合并，两条并成一条
	// 也算正确，所以只断言"都是 Merge 且值的总和不变"）。
	var total uint64
	count := 0
	for _, f := range outputs {
		for _, r := range h.dump(f.Num) {
			if r.user != "k" || r.kind != key.TypeMerge {
				t.Fatalf("透传的记录必须是 k 的 Merge 类型，得到 %v", r)
			}
			total += binary.BigEndian.Uint64([]byte(r.val))
			count++
		}
	}
	if count == 0 || count > 2 {
		t.Errorf("透传记录数 = %d, want 1~2", count)
	}
	if total != 7 {
		t.Errorf("透传 operand 总和 = %d, want 7", total)
	}
	if res.OutputRecords != count {
		t.Errorf("OutputRecords = %d, want %d", res.OutputRecords, count)
	}
}

// 没有 base 但输出层以下是空的（base level）：允许折叠，base = nil。
func TestRunFoldsMergeChainWithoutBaseAtBaseLevel(t *testing.T) {
	h := newHarness(t)
	f := h.write(merge("k", 5, counter(4)), merge("k", 4, counter(3)))
	h.apply(f.Edit(0))

	op := &testMergeOp{}
	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{f}, nil}}
	res, outputs := h.runWith(c, 5, op, nil)

	want := []string{"k#5V=" + counter(7)}
	if got := h.mflat(outputs); !reflect.DeepEqual(got, want) {
		t.Fatalf("归并结果 = %v, want %v", got, want)
	}
	if res.DroppedRecords != 2 {
		t.Errorf("DroppedRecords = %d, want 2", res.DroppedRecords)
	}
}

// 比最小存活快照新的 operand 原样保留；链的其余部分照常折叠。
// 读路径会把新 operand 叠在折叠出的 Value 之上，两部分语义自洽。
func TestRunKeepsOperandsAboveSmallestSnapshot(t *testing.T) {
	h := newHarness(t)
	f := h.write(
		merge("k", 8, counter(100)), // seq=8 > 快照水位 5：可能有读者，原样保留
		merge("k", 5, counter(4)),
		val("k", 3, counter(10)),
	)
	h.apply(f.Edit(0))

	op := &testMergeOp{}
	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{f}, nil}}
	_, outputs := h.runWith(c, 5, op, nil)

	want := []string{"k#8M=" + counter(100), "k#5V=" + counter(14)}
	if got := h.mflat(outputs); !reflect.DeepEqual(got, want) {
		t.Fatalf("归并结果 = %v, want %v", got, want)
	}
}

// 折叠结果先过 filter：Drop 转墓碑语义（深层有数据时写同序列号的墓碑）。
func TestRunFilterDropsFoldedValue(t *testing.T) {
	h := newHarness(t)
	f := h.write(merge("k", 5, counter(4)), val("k", 3, counter(10)))
	deep := h.write(val("k", 1, "old"))
	h.apply(deep.Edit(2), f.Edit(0))

	op := &testMergeOp{}
	dropAll := dropEverything{}
	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{f}, nil}}
	_, outputs := h.runWith(c, 5, op, dropAll)

	// 折成 14 → filter 说丢 → 更深层还有 k（L2），写同序列号墓碑遮蔽。
	want := []string{"k#5D="}
	if got := h.mflat(outputs); !reflect.DeepEqual(got, want) {
		t.Fatalf("归并结果 = %v, want %v", got, want)
	}
}

// dropEverything 是"全丢"的 filter（M6 测试里 dropFF 的本地版本）。
type dropEverything struct{}

func (dropEverything) Name() string { return "kvdb.compact.test.dropAll" }
func (dropEverything) Filter(level int, userKey, value []byte, seq uint64) (Decision, error) {
	return Drop, nil
}

// FullMerge 报错 ⇒ Compaction 整体失败，算子的内部故障按引擎故障处理。
func TestRunMergeOperatorErrorFailsCompaction(t *testing.T) {
	h := newHarness(t)
	f := h.write(merge("k", 5, counter(4)), val("k", 3, counter(10)))
	h.apply(f.Edit(0))

	op := &testMergeOp{failFull: true}
	h.vs.RaiseNextFileNum(h.nextNum + 1)
	env := h.env(nil)
	env.SmallestSnapshot = 5
	env.Merge = op
	c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{f}, nil}}
	v := h.vs.Current()
	_, err := Run(c, v, env)
	v.Unref()
	if err == nil {
		t.Fatal("FullMerge 报错时 Run 必须失败")
	}
	if !strings_Contains(err.Error(), "full merge boom") {
		t.Errorf("错误应当包含算子的原始错误，得到：%v", err)
	}
}

func strings_Contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// 确认 PartialMerge 结果参与折叠时语义与 FullMerge 一致（结合律契约）：
// 无论 PartialMerge 合并掉多少对 operand，最终值都必须相同。
func TestRunPartialMergeGroupingDoesNotChangeResult(t *testing.T) {
	build := func(op *testMergeOp) string {
		h := newHarness(t)
		f := h.write(merge("k", 5, counter(1)), merge("k", 4, counter(2)), merge("k", 3, counter(3)), merge("k", 2, counter(4)))
		h.apply(f.Edit(0))
		c := &Compaction{Level: 0, OutputLevel: 1, Inputs: [2][]*version.FileMeta{{f}, nil}}
		_, outputs := h.runWith(c, 5, op, nil)
		got := h.mflat(outputs)
		if len(got) != 1 {
			t.Fatalf("输出 = %v, want 恰好一条", got)
		}
		return got[0]
	}
	withPartial := build(&testMergeOp{})
	withoutPartial := build(&testMergeOp{noPartial: true})
	if withPartial != withoutPartial {
		t.Fatalf("PartialMerge 改变了结果：%q vs %q", withPartial, withoutPartial)
	}
	if withPartial != "k#5V="+counter(10) {
		t.Fatalf("折叠结果 = %q, want %q", withPartial, "k#5V="+counter(10))
	}
}
