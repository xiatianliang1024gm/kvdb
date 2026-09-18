package version

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"kvdb/internal/key"
)

// ── 测试脚手架 ────────────────────────────────────────────────────

// testComparer 是一个按字节序比较的 Comparer，名字可指定以便测试"比较器不匹配"。
type testComparer struct{ name string }

func (c testComparer) Compare(a, b []byte) int { return bytes.Compare(a, b) }
func (c testComparer) Name() string            { return c.name }

const testComparerName = "kvdb.test.bytes"

func testConfig(dir string) Config {
	return Config{Dir: dir, Comparer: testComparer{name: testComparerName}, MaxLevels: 4}
}

// ik 造一个 internal key。测试里序列号都显式给出，避免依赖任何默认值。
func ik(user string, seq uint64) []byte {
	return key.EncodeInternalKey([]byte(user), seq, key.TypeValue)
}

// fm 造一个文件元信息。
func fm(num, size uint64, smallest, largest []byte) *FileMeta {
	return &FileMeta{Num: num, Size: size, Smallest: smallest, Largest: largest}
}

// newVS 建一个"已经可以追加"的 VersionSet（建好初始空版本与第一份 Manifest）。
func newVS(t *testing.T, dir string) *VersionSet {
	t.Helper()
	vs := New(testConfig(dir))
	if err := vs.SetFromScan(nil, 0); err != nil {
		t.Fatalf("SetFromScan: %v", err)
	}
	if err := vs.NewManifest(); err != nil {
		t.Fatalf("NewManifest: %v", err)
	}
	t.Cleanup(func() { _ = vs.Close() })
	return vs
}

func mustApply(t *testing.T, vs *VersionSet, e *VersionEdit) {
	t.Helper()
	if err := vs.LogAndApply(e); err != nil {
		t.Fatalf("LogAndApply(%s): %v", e, err)
	}
}

// numsOf 取出文件编号列表；空结果保持为 nil，好让 reflect.DeepEqual 与"一个都没挑到"对上。
func numsOf(files []*FileMeta) []uint64 {
	var out []uint64
	for _, f := range files {
		out = append(out, f.Num)
	}
	return out
}

// ── VersionEdit 编解码 ────────────────────────────────────────────

// 全字段往返：编码再解码必须得到一模一样的内容。
func TestVersionEditRoundTrip(t *testing.T) {
	in := &VersionEdit{
		ComparatorName: testComparerName,
		NextFileNum:    42,
		LastSeq:        1 << 40,
		LogNumber:      7,
		Added: []FileEdit{
			{Level: 0, Num: 3, Size: 100, Smallest: ik("a", 5), Largest: ik("c", 1)},
			{Level: 2, Num: 9, Size: 4096, Smallest: ik("k", 9), Largest: ik("k", 1)},
		},
		Deleted: []FileEdit{{Level: 1, Num: 8}},
	}

	got, err := DecodeVersionEdit(in.Encode())
	if err != nil {
		t.Fatalf("DecodeVersionEdit: %v", err)
	}
	if got.ComparatorName != in.ComparatorName ||
		got.NextFileNum != in.NextFileNum ||
		got.LastSeq != in.LastSeq ||
		got.LogNumber != in.LogNumber {
		t.Fatalf("计数字段往返不一致：%s", got)
	}
	if !reflect.DeepEqual(got.Added, in.Added) {
		t.Errorf("Added 往返不一致：\n got %+v\nwant %+v", got.Added, in.Added)
	}
	if !reflect.DeepEqual(got.Deleted, in.Deleted) {
		t.Errorf("Deleted 往返不一致: got %+v want %+v", got.Deleted, in.Deleted)
	}
}

// 零值字段不写进记录：只改文件列表的记录必须很小，否则 Manifest 会随文件数膨胀。
func TestVersionEditOmitsZeroFields(t *testing.T) {
	e := &VersionEdit{Deleted: []FileEdit{{Level: 0, Num: 1}}}
	if got := len(e.Encode()); got != 3 {
		t.Errorf("一条纯删除记录编码后 %d 字节，want 3", got)
	}
	if got, err := DecodeVersionEdit(e.Encode()); err != nil || got.LastSeq != 0 || got.NextFileNum != 0 {
		t.Errorf("零值字段不该出现在记录里: %v %v", got, err)
	}
}

// 坏记录必须在解码阶段就被拒绝。
//
// 这不是洁癖：Manifest 的尾记录可能是崩溃时写了一半的，一个不校验的长度字段
// 会让恢复路径直接按它去分配内存。
func TestDecodeVersionEditRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{"未知标签", []byte{200}},
		{"长度字段被截断", []byte{tagComparator, 0x80}},
		{"长度字段谎报", []byte{tagComparator, 10, 'a'}},
		{"新增文件条目缺字段", []byte{tagNewFile, 0, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeVersionEdit(tc.in); err == nil {
				t.Fatalf("DecodeVersionEdit(%v) 应当报错", tc.in)
			}
		})
	}
}

// ── 版本布局 ──────────────────────────────────────────────────────

// L0 按编号升序落位。编号大 = 新，读路径正是靠"从后往前"拿到最新版本。
func TestVersionEditOrdersL0ByFileNum(t *testing.T) {
	vs := newVS(t, t.TempDir())
	e := &VersionEdit{Added: []FileEdit{
		fm(5, 1, ik("a", 1), ik("a", 1)).Edit(0),
		fm(3, 1, ik("b", 1), ik("b", 1)).Edit(0),
		fm(9, 1, ik("c", 1), ik("c", 1)).Edit(0),
	}}
	mustApply(t, vs, e)

	v := vs.Current()
	defer v.Unref()
	if got, want := numsOf(v.Files(0)), []uint64{3, 5, 9}; !reflect.DeepEqual(got, want) {
		t.Fatalf("L0 文件顺序 = %v, want %v", got, want)
	}
}

// L1 以下按 Smallest 升序落位 —— 二分查找的全部依据。
func TestVersionEditOrdersLevelsBySmallest(t *testing.T) {
	vs := newVS(t, t.TempDir())
	mustApply(t, vs, &VersionEdit{Added: []FileEdit{
		fm(1, 1, ik("m", 1), ik("p", 1)).Edit(1),
		fm(2, 1, ik("a", 1), ik("c", 1)).Edit(1),
		fm(3, 1, ik("f", 1), ik("k", 1)).Edit(1),
	}})

	v := vs.Current()
	defer v.Unref()
	if got, want := numsOf(v.Files(1)), []uint64{2, 3, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("L1 文件顺序 = %v, want %v", got, want)
	}
	if n := v.NumLevels(); n != 4 {
		t.Errorf("NumLevels() = %d, want 4", n)
	}
	if v.FileCount() != 3 || v.AllFiles()[0].Num != 2 {
		t.Errorf("FileCount/AllFiles 结果不对：%s", v)
	}
}

// SetFromScan 是 M2 目录的迁移路径：扫出来的文件全部进 L0，编号水位抬到最大编号之上。
func TestSetFromScanPutsEverythingInL0(t *testing.T) {
	vs := New(testConfig(t.TempDir()))
	t.Cleanup(func() { _ = vs.Close() })

	files := []*FileMeta{
		fm(9, 1, ik("k", 1), ik("k", 1)),
		fm(3, 1, ik("a", 1), ik("a", 1)),
	}
	if err := vs.SetFromScan(files, 9); err != nil {
		t.Fatal(err)
	}
	v := vs.Current()
	defer v.Unref()
	if got, want := numsOf(v.Files(0)), []uint64{3, 9}; !reflect.DeepEqual(got, want) {
		t.Fatalf("L0 文件顺序 = %v, want %v", got, want)
	}
	if v.FileCount() != 2 {
		t.Errorf("迁移进来的文件必须全在 L0，实际 %d 个分布在多层", v.FileCount())
	}
	if got := vs.NextFileNum(); got != 10 {
		t.Errorf("NextFileNum() = %d, want 10（编号必须大于目录里的所有文件）", got)
	}
}

// ── FindFile：点查定位 ────────────────────────────────────────────

func TestFindFileBinarySearch(t *testing.T) {
	vs := newVS(t, t.TempDir())
	mustApply(t, vs, &VersionEdit{Added: []FileEdit{
		fm(1, 1, ik("a", 1), ik("c", 1)).Edit(1),
		fm(2, 1, ik("d", 1), ik("f", 1)).Edit(1),
	}})
	v := vs.Current()
	defer v.Unref()

	cases := []struct {
		key  string
		want uint64 // 0 表示应当找不到
	}{
		{"a", 1},
		{"b", 1},
		{"c", 1},
		{"d", 2},
		{"e", 2},
		{"f", 2},
		{"0", 0},  // 比区间起点还小
		{"ca", 0}, // 落在两个文件的缝隙里
		{"g", 0},  // 越过末尾
	}
	for _, tc := range cases {
		// 用一个很大的快照号，模拟"读最新版本"。
		got := v.FindFile(1, []byte(tc.key), key.SeekKey([]byte(tc.key), 1<<40))
		if tc.want == 0 {
			if got != nil {
				t.Errorf("FindFile(%q) = %d, want nil", tc.key, got.Num)
			}
			continue
		}
		if got == nil || got.Num != tc.want {
			t.Errorf("FindFile(%q) = %v, want %d", tc.key, got, tc.want)
		}
	}

	// L0 一律返回 nil：它的区间互相重叠，只能由调用方从新到旧线性扫。
	if got := v.FindFile(0, []byte("b"), key.SeekKey([]byte("b"), 1<<40)); got != nil {
		t.Errorf("FindFile 对 L0 应当返回 nil，实际 %v", got)
	}
}

// 回归：文件里只有该 key 的**旧**版本时，按更新的快照去查也必须能定位到这个文件。
//
// 这里踩过坑：越界判断如果也用 internal key 比较，("k",1) 在尾缀降序下比
// ("k",200) 更大，整个文件会被判成"key 在它之前"而跳过 —— 数据明明在文件里却读不到。
func TestFindFileWithOlderVersionOfSameKey(t *testing.T) {
	vs := newVS(t, t.TempDir())
	mustApply(t, vs, &VersionEdit{Added: []FileEdit{
		fm(1, 1, ik("k", 1), ik("k", 1)).Edit(1),
	}})
	v := vs.Current()
	defer v.Unref()

	got := v.FindFile(1, []byte("k"), key.SeekKey([]byte("k"), 200))
	if got == nil {
		t.Fatal("文件里只有 (k,1)，但查找 (k,200) 也必须定位到它")
	}
	if got.Num != 1 {
		t.Errorf("定位到文件 %d, want 1", got.Num)
	}
}

// Overlapping 必须把"下一层里与区间相交的文件"一个不漏地挑出来：
// 漏一个就会让输出文件与它重叠，直接破坏 L1 以下"同层不重叠"的不变式。
func TestOverlapping(t *testing.T) {
	vs := newVS(t, t.TempDir())
	mustApply(t, vs, &VersionEdit{Added: []FileEdit{
		fm(1, 1, ik("a", 1), ik("c", 1)).Edit(1),
		fm(2, 1, ik("d", 1), ik("f", 1)).Edit(1),
		fm(3, 1, ik("g", 1), ik("i", 1)).Edit(1),
	}})
	v := vs.Current()
	defer v.Unref()

	cases := []struct {
		name              string
		smallest, largest string
		want              []uint64
	}{
		{"跨两个文件", "b", "e", []uint64{1, 2}},
		{"只碰第一个", "a", "b", []uint64{1}},
		{"恰好覆盖全部", "a", "i", []uint64{1, 2, 3}},
		{"落在缝隙里", "cc", "cc", nil},
		{"完全在末尾之后", "z", "zz", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := v.Overlapping(1, ik(tc.smallest, 9), ik(tc.largest, 9))
			if !reflect.DeepEqual(numsOf(got), tc.want) {
				t.Errorf("Overlapping(%q,%q) = %v, want %v", tc.smallest, tc.largest, numsOf(got), tc.want)
			}
		})
	}
}

// ── 引用计数与存活文件 ────────────────────────────────────────────

// 引用计数决定"哪些文件还能删"。旧版本只要还有人握着，它引用的文件就必须留着 ——
// 这正是迭代器能一直读旧文件的原因。
func TestRefCountKeepsOldFilesAlive(t *testing.T) {
	vs := newVS(t, t.TempDir())
	old := fm(1, 1, ik("a", 1), ik("a", 1))
	mustApply(t, vs, &VersionEdit{Added: []FileEdit{old.Edit(0)}})

	// 假装一个迭代器/快照握住了这个版本。
	held := vs.Current()
	if got := held.RefCount(); got != 2 {
		t.Fatalf("RefCount() = %d, want 2（版本集一份 + 读者一份）", got)
	}

	// 一次 Compaction 把文件 1 换成了文件 2。
	mustApply(t, vs, &VersionEdit{
		Added:   []FileEdit{fm(2, 1, ik("a", 1), ik("a", 1)).Edit(1)},
		Deleted: []FileEdit{{Level: 0, Num: 1}},
	})

	live := vs.LiveFileNums()
	if !live[1] || !live[2] {
		t.Fatalf("旧读者还握着版本，文件 1 与 2 都必须算存活：%v", live)
	}

	held.Unref()
	live = vs.LiveFileNums()
	if live[1] {
		t.Errorf("读者释放之后文件 1 不该再算存活：%v", live)
	}
	if !live[2] {
		t.Errorf("当前版本引用的文件 2 必须存活：%v", live)
	}
	if got := vs.Current().FileCount(); got != 1 {
		t.Errorf("当前版本应当只剩 1 个文件，实际 %d", got)
	}
}

// 同一次编辑里重复 Unref 不会把版本误摘：计数不为 0 时版本仍在存活集合里。
func TestVersionStaysLiveWhileReferenced(t *testing.T) {
	vs := newVS(t, t.TempDir())
	v := vs.current
	if got := v.RefCount(); got != 1 {
		t.Fatalf("刚建好的 VersionSet 应当持有 1 份引用，实际 %d", got)
	}
	v.Ref()   // 模拟一个读者（迭代器或快照）拿到它
	v.Unref() // 读者走了，但版本集那一份还在
	if len(vs.live) != 1 || vs.live[0] != v {
		t.Fatalf("引用未归零时版本不该被摘出存活集合：%v", vs.live)
	}
	v.Unref()
	if len(vs.live) != 0 {
		t.Fatalf("引用归零后版本应当被摘出，实际还剩 %d 个", len(vs.live))
	}
}

// ── Manifest 的持久化与恢复 ───────────────────────────────────────

// 核心不变式：Manifest 重放出来的版本必须和写入时的版本一模一样。
func TestManifestRecoverRebuildsVersion(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	vs := New(cfg)
	t.Cleanup(func() { _ = vs.Close() })
	if err := vs.SetFromScan(nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := vs.NewManifest(); err != nil {
		t.Fatal(err)
	}

	l0a := fm(1, 100, ik("a", 3), ik("c", 1))
	l0b := fm(2, 120, ik("b", 5), ik("d", 1))
	l1a := fm(3, 200, ik("a", 4), ik("d", 2))

	mustApply(t, vs, &VersionEdit{
		NextFileNum: 4, LastSeq: 5, LogNumber: 3,
		Added: []FileEdit{l0a.Edit(0), l0b.Edit(0)},
	})
	// 第二次提交模拟一次 Compaction：L0 的两个文件合并成 L1 的一个。
	mustApply(t, vs, &VersionEdit{
		NextFileNum: 5, LastSeq: 9, LogNumber: 4,
		Added:   []FileEdit{l1a.Edit(1)},
		Deleted: []FileEdit{{Level: 0, Num: 1}, {Level: 0, Num: 2}},
	})

	// 打开时会把历史收敛成一份新 Manifest，编号随之变化，旧的那份要被删掉。
	oldManifest := vs.ManifestNum()
	if err := vs.NewManifest(); err != nil {
		t.Fatal(err)
	}
	if vs.ManifestNum() == oldManifest {
		t.Fatalf("NewManifest 应当换一个编号，仍是 %d", oldManifest)
	}
	if _, err := os.Stat(ManifestName(dir, oldManifest)); !os.IsNotExist(err) {
		t.Errorf("旧的 Manifest 应当被删除: %v", err)
	}
	if got, err := readCurrent(dir); err != nil || got != vs.ManifestNum() {
		t.Errorf("CURRENT 指向 %d (%v), want %d", got, err, vs.ManifestNum())
	}

	wantNext := vs.NextFileNum()
	if err := vs.Close(); err != nil {
		t.Fatal(err)
	}

	// ── 重新打开 ──
	vs2 := New(cfg)
	t.Cleanup(func() { _ = vs2.Close() })
	hasManifest, truncated, err := vs2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if !hasManifest {
		t.Fatal("目录里应当有 Manifest")
	}
	if truncated {
		t.Error("没有损坏字节时不该报告截断")
	}

	v := vs2.Current()
	defer v.Unref()
	if got := numsOf(v.Files(0)); len(got) != 0 {
		t.Errorf("L0 = %v, want 空", got)
	}
	if got, want := numsOf(v.Files(1)), []uint64{3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("L1 = %v, want %v", got, want)
	}
	got := v.Files(1)[0]
	if got.Size != 200 || !bytes.Equal(got.Smallest, ik("a", 4)) || !bytes.Equal(got.Largest, ik("d", 2)) {
		t.Errorf("文件元信息没被完整恢复：%+v", got)
	}

	// 全局计数也必须回来：lastSeq 回落 0 会让新写入与 SST 里的老记录撞号。
	if vs2.LastSeq() != 9 {
		t.Errorf("LastSeq() = %d, want 9", vs2.LastSeq())
	}
	if vs2.LogNumber() != 4 {
		t.Errorf("LogNumber() = %d, want 4", vs2.LogNumber())
	}
	if vs2.NextFileNum() != wantNext {
		t.Errorf("NextFileNum() = %d, want %d", vs2.NextFileNum(), wantNext)
	}

	// 恢复时读到的那份 Manifest 是"读完就关、没留句柄"的。重写它的时候必须
	// 按**编号**把文件删掉，否则只判句柄会漏删，目录里会一层层积下作废的元数据。
	recovered := vs2.ManifestNum()
	if err := vs2.NewManifest(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ManifestName(dir, recovered)); !os.IsNotExist(err) {
		t.Errorf("重写之后旧的 Manifest %d 应当被删除: %v", recovered, err)
	}
}

// 目录里没有 CURRENT 时必须明确地告诉调用方"没有版本元数据"，
// 而不是当成空目录 —— 那会走成扫描迁移路径。
func TestRecoverReportsMissingManifest(t *testing.T) {
	vs := New(testConfig(t.TempDir()))
	t.Cleanup(func() { _ = vs.Close() })
	hasManifest, truncated, err := vs.Recover()
	if err != nil {
		t.Fatalf("空目录不该报错: %v", err)
	}
	if hasManifest || truncated {
		t.Errorf("hasManifest=%v truncated=%v, want false false", hasManifest, truncated)
	}
}

// 尾部半截记录必须被丢掉，并且不影响前面已经完整写下的那些提交。
func TestManifestTruncatedTailIsDropped(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	vs := New(cfg)
	t.Cleanup(func() { _ = vs.Close() })
	if err := vs.SetFromScan(nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := vs.NewManifest(); err != nil {
		t.Fatal(err)
	}
	mustApply(t, vs, &VersionEdit{
		NextFileNum: 3, LastSeq: 1,
		Added: []FileEdit{fm(1, 10, ik("a", 1), ik("a", 1)).Edit(0)},
	})
	manifestNum := vs.ManifestNum()
	mustApply(t, vs, &VersionEdit{
		NextFileNum: 4, LastSeq: 2,
		Added: []FileEdit{fm(2, 10, ik("b", 2), ik("b", 2)).Edit(0)},
	})
	if err := vs.Close(); err != nil {
		t.Fatal(err)
	}

	// 伪造"第二次追加写到一半就崩溃"：一个声称还有 100 字节负载的头，后面什么都没有。
	tail := []byte{0xde, 0xad, 0xbe, 0xef, 0x00, 0x64, 0x01}
	f, err := os.OpenFile(ManifestName(dir, manifestNum), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(tail); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	vs2 := New(cfg)
	t.Cleanup(func() { _ = vs2.Close() })
	hasManifest, truncated, err := vs2.Recover()
	if err != nil {
		t.Fatalf("半截尾记录应当被容忍: %v", err)
	}
	if !hasManifest {
		t.Fatal("hasManifest 应当为 true")
	}
	if !truncated {
		t.Fatal("尾部半截记录必须报告为截断")
	}
	v := vs2.Current()
	defer v.Unref()
	if got, want := numsOf(v.Files(0)), []uint64{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("两条完整的提交都该生效，L0 = %v, want %v", got, want)
	}
	if vs2.LastSeq() != 2 {
		t.Errorf("LastSeq() = %d, want 2", vs2.LastSeq())
	}
}

// 换一个比较器读同一个目录必须报错：块的排序假设当场就不成立了。
func TestComparerMismatchIsRejected(t *testing.T) {
	dir := t.TempDir()

	vsA := New(Config{Dir: dir, Comparer: testComparer{name: testComparerName}, MaxLevels: 4})
	t.Cleanup(func() { _ = vsA.Close() })
	if err := vsA.SetFromScan(nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := vsA.NewManifest(); err != nil {
		t.Fatal(err)
	}
	if err := vsA.Close(); err != nil {
		t.Fatal(err)
	}

	vsB := New(Config{Dir: dir, Comparer: testComparer{name: "kvdb.test.bytes.other"}, MaxLevels: 4})
	t.Cleanup(func() { _ = vsB.Close() })
	_, _, err := vsB.Recover()
	if err == nil {
		t.Fatal("换了比较器读同一个目录必须报错")
	}
	if !strings.Contains(err.Error(), "comparer") {
		t.Errorf("错误信息应当点明是比较器不匹配: %v", err)
	}
}

// 没有可追加的 Manifest 时，LogAndApply 必须明确失败而不是静默丢弃变更。
func TestLogAndApplyWithoutManifest(t *testing.T) {
	vs := New(testConfig(t.TempDir()))
	t.Cleanup(func() { _ = vs.Close() })
	err := vs.LogAndApply(&VersionEdit{Added: []FileEdit{fm(1, 1, ik("a", 1), ik("a", 1)).Edit(0)}})
	if err != ErrNotOpen {
		t.Fatalf("LogAndApply 的 error = %v, want ErrNotOpen", err)
	}
	if got := vs.Current().FileCount(); got != 0 {
		t.Errorf("提交失败之后版本不该变化，实际有 %d 个文件", got)
	}
}

// 回归：换版本时释放旧版本**不能**回头去抢 VersionSet.mu —— sync.Mutex 不可重入。
//
// 这个坑很阴：单独用 VersionSet 时必现（旧版本计数归零，摘存活集合那次加锁就是自锁），
// 而在 DB 里因为 db.v 一直握着一个长期引用、旧版本计数永远降不到 0，
// 整套测试都是绿的。写这个测试就是为了别再靠运气。
//
// 断言只能是"能不能跑完"：死锁发生在 LogAndApply 内部，任何等它返回的检查都等不到。
func TestLogAndApplyReleasesOldVersionWithoutDeadlock(t *testing.T) {
	// 不用 t.TempDir：万一回归成死锁，清理阶段也会卡在锁上，
	// 那样连"这条测试失败"的结论都拿不到。这里自己建目录、尽力而为地清。
	dir, err := os.MkdirTemp("", "kvdb-version-deadlock-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	vs := New(testConfig(dir))
	defer func() { go vs.Close() }()
	if err := vs.SetFromScan(nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := vs.NewManifest(); err != nil {
		t.Fatal(err)
	}

	// 反复提交，且全程不额外持有任何版本引用：每一次都会让上一个版本集的引用归零。
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 50; i++ {
			num := uint64(i + 1)
			e := &VersionEdit{
				NextFileNum: num + 1,
				LastSeq:     num,
				Added:       []FileEdit{fm(num, 1, ik("a", num), ik("a", num)).Edit(0)},
			}
			if err := vs.LogAndApply(e); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("LogAndApply: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("LogAndApply 卡住了：换版本时释放旧版本不能重入 VersionSet.mu")
	}

	v := vs.Current()
	defer v.Unref()
	if v.FileCount() != 50 {
		t.Errorf("提交 50 次之后应当有 50 个文件，实际 %d", v.FileCount())
	}
	if vs.LastSeq() != 50 {
		t.Errorf("LastSeq() = %d, want 50", vs.LastSeq())
	}
}

// ── 导出快照（M5 为 Checkpoint 加的） ──────────────────────────────

// TestSnapshotEditRebuildsVersion 验证 SnapshotEdit 返回的编辑项确实能
// 完整重建当前版本：层号、区间、文件编号一个都不能少。
//
// 它是 Checkpoint 的正确性前提 —— 副本目录里那份 Manifest 就是这个 edit
// 序列化出来的，漏掉一个文件就等于副本里永久少一段数据。
func TestSnapshotEditRebuildsVersion(t *testing.T) {
	dir := t.TempDir()
	vs := newVS(t, dir)
	mustApply(t, vs, &VersionEdit{
		NextFileNum: 10,
		LastSeq:     77,
		LogNumber:   5,
		Added: []FileEdit{
			fm(1, 100, ik("a", 3), ik("c", 1)).Edit(0),
			fm(2, 200, ik("d", 9), ik("f", 2)).Edit(0),
			fm(3, 300, ik("g", 4), ik("m", 1)).Edit(1),
			fm(4, 400, ik("n", 8), ik("z", 1)).Edit(2),
		},
	})

	edit := vs.SnapshotEdit()
	if edit.ComparatorName != testComparerName {
		t.Errorf("ComparatorName = %q, want %q", edit.ComparatorName, testComparerName)
	}
	if edit.LastSeq != 77 {
		t.Errorf("LastSeq = %d, want 77", edit.LastSeq)
	}
	if edit.LogNumber != 5 {
		t.Errorf("LogNumber = %d, want 5", edit.LogNumber)
	}
	if edit.NextFileNum != vs.NextFileNum() {
		t.Errorf("NextFileNum = %d, want %d", edit.NextFileNum, vs.NextFileNum())
	}
	if len(edit.Deleted) != 0 {
		t.Errorf("快照不该带任何删除项，实际 %d 条", len(edit.Deleted))
	}

	// 用一个全新的 VersionSet 应用这份 edit，应当得到一模一样的层布局。
	other := New(testConfig(t.TempDir()))
	t.Cleanup(func() { _ = other.Close() }) // Windows 上句柄没关就删不掉临时目录
	other.SetFromScan(nil, 0)
	if err := other.LogAndApply(&VersionEdit{NextFileNum: 1}); err != nil {
		// 没有 Manifest 时 LogAndApply 会返回 ErrNotOpen，这是预期的守卫。
		if !errors.Is(err, ErrNotOpen) {
			t.Fatalf("LogAndApply: %v", err)
		}
	}
	if err := other.LogAndApply(edit); !errors.Is(err, ErrNotOpen) {
		t.Fatalf("没有 Manifest 时应当报 ErrNotOpen，实际 %v", err)
	}
	if err := other.NewManifest(); err != nil {
		t.Fatal(err)
	}
	if err := other.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply(snapshot): %v", err)
	}
	cur := vs.Current()
	defer cur.Unref()
	got := other.Current()
	defer got.Unref()

	for level := 0; level < cur.NumLevels(); level++ {
		want := numsOf(cur.Files(level))
		if !reflect.DeepEqual(numsOf(got.Files(level)), want) {
			t.Errorf("L%d 的文件不一致：got %v want %v", level, numsOf(got.Files(level)), want)
		}
	}
	if other.LastSeq() != 77 || other.LogNumber() != 5 {
		t.Errorf("计数没有跟着快照走：LastSeq=%d LogNumber=%d", other.LastSeq(), other.LogNumber())
	}
}

// TestWriteManifestProducesRecoverableDirectory 验证 WriteManifest 写出的
// "Manifest + CURRENT"能被正常恢复。
//
// 这是 Checkpoint 的另一半：副本目录里没有本目录的 Manifest 可抄
// （那份文件正在被追加），只能自己写一份。
func TestWriteManifestProducesRecoverableDirectory(t *testing.T) {
	src := t.TempDir()
	vs := newVS(t, src)
	mustApply(t, vs, &VersionEdit{
		NextFileNum: 20,
		LastSeq:     123,
		LogNumber:   7,
		Added: []FileEdit{
			fm(11, 1, ik("a", 1), ik("b", 1)).Edit(0),
			fm(12, 1, ik("c", 1), ik("d", 1)).Edit(1),
		},
	})

	edit := vs.SnapshotEdit()
	dst := t.TempDir()
	const manifestNum = 99
	if err := WriteManifest(dst, manifestNum, edit); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	// CURRENT 必须指向那份 Manifest，而且是"写完再 rename"留下的完整内容。
	num, err := readCurrent(dst)
	if err != nil {
		t.Fatalf("readCurrent: %v", err)
	}
	if num != manifestNum {
		t.Fatalf("CURRENT 指向 %d，want %d", num, manifestNum)
	}

	recovered := New(Config{Dir: dst, Comparer: testComparer{name: testComparerName}, MaxLevels: 4})
	hasManifest, truncated, err := recovered.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	if !hasManifest {
		t.Fatal("应当从 Manifest 恢复，而不是走目录扫描")
	}
	if truncated {
		t.Fatal("这份 Manifest 是完整写出来的，不该被截断")
	}
	cur := recovered.Current()
	defer cur.Unref()
	if got := numsOf(cur.Files(0)); !reflect.DeepEqual(got, []uint64{11}) {
		t.Errorf("L0 = %v, want [11]", got)
	}
	if got := numsOf(cur.Files(1)); !reflect.DeepEqual(got, []uint64{12}) {
		t.Errorf("L1 = %v, want [12]", got)
	}
}

// TestWriteManifestRejectsMismatchedComparer 验证副本目录里的比较器标识
// 仍然能被校验出来 —— 它和源目录用的是同一份 VersionEdit。
func TestWriteManifestRejectsMismatchedComparer(t *testing.T) {
	vs := newVS(t, t.TempDir())
	if err := vs.LogAndApply(&VersionEdit{NextFileNum: 5, ComparatorName: testComparerName}); err != nil {
		t.Fatal(err)
	}
	edit := vs.SnapshotEdit()

	dst := t.TempDir()
	if err := WriteManifest(dst, 41, edit); err != nil {
		t.Fatal(err)
	}
	other := New(Config{Dir: dst, Comparer: testComparer{name: "someone.else"}, MaxLevels: 4})
	if _, _, err := other.Recover(); err == nil {
		t.Fatal("比较器不匹配时应当拒绝打开")
	} else if !strings.Contains(err.Error(), "comparer") {
		t.Fatalf("错误里应当说明是比较器不匹配，实际 %v", err)
	}
}
