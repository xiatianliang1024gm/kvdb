package sst

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"kvdb/internal/cache"
	"kvdb/internal/compress"
	"kvdb/internal/crc"
	"kvdb/internal/key"
)

// 一份可压缩的 value：160 字节的高度重复内容，LSM 里的真实负载长这样
// （尤其是覆盖写留下的多版本）。
func compressibleValue() string { return strings.Repeat("the quick brown fox ", 8) }

// keyOf 生成第 i 个测试 key，定长以便排序直观。
func keyOf(i int) string { return fmt.Sprintf("user-key-%06d", i) }

// compressibleEntries 造 n 条按 internal key 升序排列的可压缩记录。
func compressibleEntries(n int) []entry {
	out := make([]entry, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, entry{K: keyOf(i), S: uint64(i + 1), T: key.TypeValue, V: compressibleValue()})
	}
	return out
}

// incompressibleEntries 造 n 条高熵记录，用来验证"不划算就不压"这条策略。
func incompressibleEntries(n int) []entry {
	rnd := rand.New(rand.NewSource(20250918))
	out := make([]entry, 0, n)
	buf := make([]byte, 160)
	for i := 0; i < n; i++ {
		rnd.Read(buf)
		out = append(out, entry{K: keyOf(i), S: uint64(i + 1), T: key.TypeValue, V: string(buf)})
	}
	return out
}

// roundTripEntries 在前 300 条的基础上给 user-key-000100 补两个更旧的版本
// （一个值、一个墓碑），用来确认压缩不破坏"同 key 多版本 + 墓碑"的语义。
//
// 插入位置必须是"该 key 的最新版本之前"——internal key 在同一个 user key 上
// 按 seq 降序排列。
func roundTripEntries() []entry {
	out := make([]entry, 0, 302)
	for i := 0; i < 300; i++ {
		if i == 100 {
			out = append(out,
				entry{K: keyOf(100), S: 901, T: key.TypeValue, V: "newer version"},
				entry{K: keyOf(100), S: 900, T: key.TypeDeletion, V: ""})
		}
		out = append(out, entry{K: keyOf(i), S: uint64(i + 1), T: key.TypeValue, V: compressibleValue()})
	}
	return out
}

// mixedEntries 造一份"前一半可压、后一半高熵"的数据，用来产出混合形态的文件
// （一部分块压缩、一部分块不压缩）。块迭代器跨这种边界时最容易出错。
func mixedEntries(n int) []entry {
	out := make([]entry, 0, n)
	rnd := rand.New(rand.NewSource(7))
	buf := make([]byte, 160)
	for i := 0; i < n; i++ {
		if i < n/2 {
			out = append(out, entry{K: keyOf(i), S: uint64(i + 1), T: key.TypeValue, V: compressibleValue()})
			continue
		}
		rnd.Read(buf)
		out = append(out, entry{K: keyOf(i), S: uint64(i + 1), T: key.TypeValue, V: string(buf)})
	}
	return out
}

// openWithStats 打开一个 Reader 并接上一份块统计。
func openWithStats(t *testing.T, path string, c *cache.Cache, num uint64, bs *BlockStats) *Reader {
	t.Helper()
	r, err := Open(path, OpenOptions{Comparer: testComparer{}, Cache: c, FileNum: num, BlockStats: bs})
	if err != nil {
		t.Fatalf("Open(%s) failed: %v", path, err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

// buildWithStats 与 buildWith 相同，但把块统计接出去。
func buildWithStats(t *testing.T, entries []entry, opts WriterOptions, bs *BlockStats) string {
	t.Helper()
	opts.BlockStats = bs
	return buildWith(t, entries, opts)
}

// rawBlockContents 从文件的原始字节里取出一块的**解压后**内容。
//
// 这个辅助让人能绕开 Reader 直接检查落盘形态（压缩类型字节、压缩比）。
func rawBlockContents(t *testing.T, file []byte, h blockHandle) []byte {
	t.Helper()
	contents := file[h.offset : h.offset+h.size]
	ctype := compress.Type(file[h.offset+h.size])
	if ctype == compress.TypeNone {
		return contents
	}
	c, err := compress.ByType(ctype)
	if err != nil {
		t.Fatalf("未知的压缩类型 %d", ctype)
	}
	out, err := c.Decompress(nil, contents)
	if err != nil {
		t.Fatalf("解压失败: %v", err)
	}
	return out
}

// readFooter 解析文件尾部的 Footer，返回 MetaIndex / Index 两个 handle。
func readFooter(t *testing.T, path string) (meta, index blockHandle, file []byte) {
	t.Helper()
	file, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(file) < FooterLen {
		t.Fatalf("%s 只有 %d 字节", path, len(file))
	}
	meta, index, err = decodeFooter(file[len(file)-FooterLen:])
	if err != nil {
		t.Fatalf("decodeFooter: %v", err)
	}
	return meta, index, file
}

// firstDataBlock 返回文件里第一个 Data Block 的 handle。
func firstDataBlock(t *testing.T, path string) (blockHandle, []byte) {
	t.Helper()
	_, indexH, file := readFooter(t, path)
	it, err := newBlockIter(rawBlockContents(t, file, indexH), compareBytes)
	if err != nil {
		t.Fatal(err)
	}
	it.SeekToFirst()
	if !it.Valid() {
		t.Fatalf("%s 的索引里没有数据块", path)
	}
	h, _, err := decodeBlockHandle(it.Value())
	if err != nil {
		t.Fatal(err)
	}
	return h, file
}

// blockShape 描述文件里一个块的落盘形态。
type blockShape struct {
	kind   string // data / filter / metaindex / index
	typ    byte   // 块尾的类型字节
	raw    int    // 压缩前的字节数
	stored int    // 实际落盘的字节数
}

// blockShapes 逐个块地读出一个文件的落盘形态。
//
// 有了它，"哪些块被压了、省了多少"就能被直接断言，而不是靠文件总大小间接推断 ——
// 后者会把数据块、过滤器、索引块的效果混在一起，看不出"按块独立判断"这件事。
func blockShapes(t *testing.T, path string) []blockShape {
	t.Helper()
	metaH, indexH, file := readFooter(t, path)
	shape := func(kind string, h blockHandle) blockShape {
		return blockShape{kind: kind, typ: file[h.offset+h.size], raw: len(rawBlockContents(t, file, h)), stored: int(h.size)}
	}

	var out []blockShape
	it, err := newBlockIter(rawBlockContents(t, file, indexH), compareBytes)
	if err != nil {
		t.Fatal(err)
	}
	for it.SeekToFirst(); it.Valid(); it.Next() {
		h, _, err := decodeBlockHandle(it.Value())
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, shape("data", h))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}

	meta := rawBlockContents(t, file, metaH)
	if fh, ok, err := findFilterHandle(meta); err != nil {
		t.Fatal(err)
	} else if ok {
		out = append(out, shape("filter", fh))
	}
	out = append(out, shape("metaindex", metaH), shape("index", indexH))
	return out
}

// dataBlocks 只取数据块的形态。
func dataBlocks(t *testing.T, path string) []blockShape {
	t.Helper()
	var out []blockShape
	for _, b := range blockShapes(t, path) {
		if b.kind == "data" {
			out = append(out, b)
		}
	}
	return out
}

// TestCompressionRoundTrip 是压缩最基本的要求：压了要能解回来，一条不差。
//
// 三种算法、多个块、含墓碑与同 key 多版本 —— 压缩不能改变任何语义。
func TestCompressionRoundTrip(t *testing.T) {
	entries := roundTripEntries()
	want := map[string]string{}
	for _, e := range entries {
		want[string(ik(e.K, e.S, e.T))] = e.V
	}

	for _, typ := range []compress.Type{compress.TypeNone, compress.TypeSnappy, compress.TypeZlib} {
		path := buildWith(t, entries, WriterOptions{BlockSize: 1024, BloomBitsPerKey: 10, Compression: typ})

		var bs BlockStats
		r := openWithStats(t, path, nil, 1, &bs)
		if r.NumBlocks() < 2 {
			t.Fatalf("%v: 期望多块文件，实际 %d 块", typ, r.NumBlocks())
		}

		got := map[string]string{}
		if err := r.Iterate(func(k, v []byte) bool {
			got[string(k)] = string(v)
			return true
		}); err != nil {
			t.Fatalf("%v: iterate: %v", typ, err)
		}
		if len(got) != len(want) {
			t.Fatalf("%v: 读回 %d 条，写入 %d 条", typ, len(got), len(want))
		}
		for k, v := range want {
			g, ok := got[k]
			if !ok {
				t.Fatalf("%v: 记录 %q 丢了", typ, describeKey([]byte(k)))
			}
			if g != v {
				t.Fatalf("%v: %q 的值不对", typ, describeKey([]byte(k)))
			}
		}

		// 点查同样要正确（走的是"索引二分 → 读块 → 块内 seek"）。
		v, kind, found, err := r.Get(uint64(len(entries)+10), []byte(keyOf(7)))
		if err != nil {
			t.Fatalf("%v: Get: %v", typ, err)
		}
		if !found || kind != key.TypeValue || string(v) != compressibleValue() {
			t.Fatalf("%v: Get(user-key-000007) 结果不对: found=%v kind=%v", typ, found, kind)
		}
	}
}

// TestCompressionShrinksFile 验证压缩确实让文件变小 —— 否则这个特性毫无意义。
func TestCompressionShrinksFile(t *testing.T) {
	entries := compressibleEntries(2000)
	opts := WriterOptions{BlockSize: 4 << 10, BloomBitsPerKey: 10}

	base := buildWith(t, entries, opts)
	noneSize := fileSize(t, base)

	for _, typ := range []compress.Type{compress.TypeSnappy, compress.TypeZlib} {
		opts.Compression = typ
		path := buildWith(t, entries, opts)
		got := fileSize(t, path)
		if got >= noneSize {
			t.Errorf("%v: 压缩后 %d 字节，未压缩 %d 字节 —— 没有变小", typ, got, noneSize)
		}
		t.Logf("%v: %d -> %d 字节（省下 %.1f%%）", typ, noneSize, got, 100*(1-float64(got)/float64(noneSize)))
	}
}

// TestCompressionSkippedWhenNotWorthIt 验证"压缩不划算就不压"这条策略真的生效。
//
// 高熵数据压缩后反而更大，此时正确的做法是原样存储：写侧省一次压缩、
// 读侧省一次解压。判据就是块尾的类型字节，这里把它直接读出来断言。
//
// 断言范围刻意限定在**数据块**上：过滤器位图与索引块的收益与数据块完全不同，
// 把它们混在一起断言就等于在测"整文件一刀切"。索引块在下面的
// TestIndexBlockCompressesToo 里单独验证 —— 它其实是压得动的。
func TestCompressionSkippedWhenNotWorthIt(t *testing.T) {
	entries := incompressibleEntries(1500)
	opts := WriterOptions{BlockSize: 4 << 10, Compression: compress.TypeSnappy}
	path := buildWith(t, entries, opts)

	blocks := dataBlocks(t, path)
	if len(blocks) < 10 {
		t.Fatalf("只有 %d 个数据块，样本太少", len(blocks))
	}
	stored, raw := 0, 0
	for i, b := range blocks {
		if b.typ != byte(compress.TypeNone) {
			t.Fatalf("第 %d 个数据块被压缩了（类型 %d），但它不该有收益", i, b.typ)
		}
		stored += b.stored
		raw += b.raw
	}
	if stored != raw {
		t.Fatalf("被跳过的压缩不该改变数据区大小：落盘 %d，原始 %d", stored, raw)
	}

	var bs BlockStats
	r := openWithStats(t, path, nil, 1, &bs)
	if _, _, _, err := r.Get(1<<20, []byte(keyOf(42))); err != nil {
		t.Fatal(err)
	}
	snap := bs.Snapshot()
	if snap.Blocks != 0 {
		t.Fatalf("读侧不该往 BlockStats 里写（Blocks = %d）", snap.Blocks)
	}
	// 读侧的解压最多只可能来自那个"确实有收益"的索引块（打开文件时读一次）。
	// 数据块一个都没压缩，所以每读一个数据块都应当是一次纯粹的磁盘读取。
	if got := snap.Decompressions; got > 1 {
		t.Fatalf("数据块全都没压缩，却发生了 %d 次解压", got)
	}
}

// TestIndexBlockCompressesToo 验证判据是**按块**独立判断的，不是按文件一刀切。
//
// 索引块的条目是"块的最大 key → handle"，相邻 key 共享很长的前缀，
// 因此在文件较大时它反而是压得动的；而 MetaIndex（一条）与过滤器位图压不动。
// 三种块在同一个文件里给出不同的结论，才说明"每个块独立判断"这件事真的在跑。
func TestIndexBlockCompressesToo(t *testing.T) {
	opts := WriterOptions{BlockSize: 4 << 10, BloomBitsPerKey: 10, Compression: compress.TypeSnappy}
	path := buildWith(t, compressibleEntries(1500), opts)

	byKind := map[string]blockShape{}
	for _, b := range blockShapes(t, path) {
		byKind[b.kind] = b
	}
	idx, ok := byKind["index"]
	if !ok {
		t.Fatal("文件里应当有 index 块")
	}
	if idx.typ != byte(compress.TypeSnappy) {
		t.Errorf("大型文件的 index 块应当被压缩，实际类型 %d（%d -> %d 字节）", idx.typ, idx.raw, idx.stored)
	}
	meta, ok := byKind["metaindex"]
	if !ok {
		t.Fatal("文件里应当有 metaindex 块")
	}
	if meta.raw > 0 && meta.typ != byte(compress.TypeNone) {
		t.Errorf("只有一条记录的 metaindex 不该被压缩，实际类型 %d", meta.typ)
	}
	flt, ok := byKind["filter"]
	if !ok {
		t.Fatal("开了 Bloom 就应当有 filter 块")
	}
	if flt.typ != byte(compress.TypeNone) {
		t.Errorf("接近随机的过滤器位图不该被压缩，实际类型 %d", flt.typ)
	}
}

// TestCompressionStatsAndRatio 验证 BlockStats 的账算得对。
func TestCompressionStatsAndRatio(t *testing.T) {
	entries := compressibleEntries(1500)
	var bs BlockStats
	path := buildWithStats(t, entries,
		WriterOptions{BlockSize: 4 << 10, Compression: compress.TypeSnappy}, &bs)

	snap := bs.Snapshot()
	if snap.Blocks == 0 {
		t.Fatal("BlocksWritten 应当大于 0")
	}
	if snap.Compressed == 0 {
		t.Fatal("可压缩的数据应当至少压掉一个块")
	}
	if snap.Compressed > snap.Blocks {
		t.Fatalf("压缩块数 %d 超过了总块数 %d", snap.Compressed, snap.Blocks)
	}
	if snap.StoredBytes >= snap.RawBytes {
		t.Fatalf("落盘 %d 字节，原始 %d 字节 —— 压缩没生效", snap.StoredBytes, snap.RawBytes)
	}
	if ratio := snap.Ratio(); ratio <= 1.0 {
		t.Fatalf("压缩比 %.3f 应当大于 1", ratio)
	}
	if p := snap.SavedPercent(); p <= 0 || p >= 100 {
		t.Fatalf("省下的比例 %.1f%% 不合理", p)
	}
	if fileSize(t, path) == 0 {
		t.Fatal("文件不该是空的")
	}
	// 从未读过任何块，所以解压次数必须是 0。
	if snap.Decompressions != 0 {
		t.Fatalf("只写不读，解压次数却是 %d", snap.Decompressions)
	}
}

// TestCompressionTypeByteInTrailer 验证落盘的类型字节与配置一致。
//
// 类型字节是读路径唯一的路由依据，写错了就是"用 A 算法压、用 B 算法解"。
func TestCompressionTypeByteInTrailer(t *testing.T) {
	entries := compressibleEntries(400)
	for _, typ := range []compress.Type{compress.TypeNone, compress.TypeSnappy, compress.TypeZlib} {
		path := buildWith(t, entries, WriterOptions{BlockSize: 4 << 10, Compression: typ})
		shapes := blockShapes(t, path)
		if len(shapes) == 0 {
			t.Fatalf("%v: 没有读到任何块", typ)
		}
		for i, b := range shapes {
			if b.typ == byte(compress.TypeNone) {
				continue
			}
			if compress.Type(b.typ) != typ {
				t.Fatalf("%v: 第 %d 个块（%s）的类型字节是 %d，期望 none 或 %d", typ, i, b.kind, b.typ, byte(typ))
			}
		}
		if typ != compress.TypeNone {
			found := false
			for _, b := range shapes {
				if compress.Type(b.typ) == typ {
					found = true
				}
			}
			if !found {
				t.Fatalf("%v: 没有一个块用它压缩", typ)
			}
		}
	}
}

// TestCacheStoresDecompressedBlocks 验证块缓存里存的是解压后的内容。
//
// 存压缩流的话，每一次缓存命中都要重新解压一遍 —— 缓存就只省下了磁盘 IO、
// 省不掉 CPU，而 CPU 恰恰是压缩在读路径上新增的唯一成本。
func TestCacheStoresDecompressedBlocks(t *testing.T) {
	entries := compressibleEntries(1200)
	path := buildWith(t, entries, WriterOptions{BlockSize: 4 << 10, Compression: compress.TypeSnappy})

	var bs BlockStats
	c := cache.New(4 << 20)
	r := openWithStats(t, path, c, 7, &bs)

	// 第一次：块缓存是空的，读一个块必然要解压。
	if _, _, _, err := r.Get(1<<20, []byte(keyOf(900))); err != nil {
		t.Fatal(err)
	}
	first := bs.Snapshot().Decompressions
	if first == 0 {
		t.Fatal("第一次读压缩块应当发生解压")
	}

	// 重复读同一个 key：缓存里若是解压后的内容，就不该再解压一次。
	for i := 0; i < 5; i++ {
		if _, _, _, err := r.Get(1<<20, []byte(keyOf(900))); err != nil {
			t.Fatal(err)
		}
	}
	if second := bs.Snapshot().Decompressions; second != first {
		t.Fatalf("缓存命中后仍在解压：%d -> %d", first, second)
	}

	// 换一个不带缓存的读取器再读一次：必须重新解压，
	// 说明上面的"没解压"确实来自缓存而不是别的原因。
	uncached := openWithStats(t, path, nil, 7, &bs)
	if _, _, _, err := uncached.Get(1<<20, []byte(keyOf(900))); err != nil {
		t.Fatal(err)
	}
	if got := bs.Snapshot().Decompressions; got <= first {
		t.Fatalf("无缓存时应当重新解压：%d -> %d", first, got)
	}
}

// TestUnsupportedCompressionTypeRejected 验证未知类型被明确拒绝。
//
// 这里必须**同时改掉 CRC** 才测得到这条路径：校验范围包含类型字节本身，
// 只改类型会让 CRC 先失败。两步都做才是在测"类型合法但引擎不认识"。
//
// 拒绝而不是回退成"不压缩"是刻意的：回退会把一段压缩流当原文交给上层解析，
// 得到一堆看似合法、实际全是乱码的 key —— 静默的数据损坏。
func TestUnsupportedCompressionTypeRejected(t *testing.T) {
	path := buildWith(t, compressibleEntries(200), WriterOptions{BlockSize: 4 << 10})
	h, file := firstDataBlock(t, path)

	const bogus = byte(9)
	file[h.offset+h.size] = bogus
	copy(file[h.offset+h.size+1:], fixed32(crc.ChecksumWithType(file[h.offset:h.offset+h.size], bogus)))
	if err := os.WriteFile(path, file, 0o644); err != nil {
		t.Fatal(err)
	}

	r := openWithStats(t, path, nil, 1, &BlockStats{})
	// Open 只读 Index / Filter / MetaIndex 三个元数据块，所以坏的是数据块时
	// 打开仍然成功，故障要等到真的去读那一个块才暴露。
	_, _, _, err := r.Get(1<<20, []byte(keyOf(0)))
	if err == nil {
		t.Fatal("未知压缩类型的块应当读失败")
	}
	if !errors.Is(err, ErrUnsupportedCompression) {
		t.Fatalf("错误应当是 ErrUnsupportedCompression，实际 %v", err)
	}
}

// TestCorruptCompressedBlockDetected 验证压缩块的 CRC 覆盖的是压缩后的字节。
//
// 如果 CRC 算在压缩前的原文上，那么"磁盘上那个块被改坏了"这件事在解压之前
// 根本发现不了 —— 我们会先拿一段坏位流去解压，于是所有错误都变成"解压失败"，
// 而真正的原因（磁盘坏了）被掩盖。
func TestCorruptCompressedBlockDetected(t *testing.T) {
	path := buildWith(t, compressibleEntries(200),
		WriterOptions{BlockSize: 4 << 10, Compression: compress.TypeSnappy})
	h, file := firstDataBlock(t, path)
	if compress.Type(file[h.offset+h.size]) == compress.TypeNone {
		t.Skip("这个块没有被压缩，构造不出压缩块被改坏的形态")
	}

	// 只翻一个负载字节：CRC 覆盖的就是它，所以必须被发现。
	file[h.offset] ^= 0xff
	if err := os.WriteFile(path, file, 0o644); err != nil {
		t.Fatal(err)
	}

	r := openWithStats(t, path, nil, 1, &BlockStats{})
	_, _, _, err := r.Get(1<<20, []byte(keyOf(0)))
	if err == nil {
		t.Fatal("损坏的压缩块应当被拒绝")
	}
	if !errors.Is(err, ErrCorruptBlock) {
		t.Fatalf("错误应当是 ErrCorruptBlock，实际 %v", err)
	}
}

// TestUncompressedFilesStayReadable 验证"关掉压缩不会让老文件变成不可读"。
//
// 这是 Compression 与 Comparer 的关键区别：Comparer 决定 key 的排序语义，
// 换了就必须拒绝打开；而压缩只是存储形态，读路径按块尾的类型字节自解释，
// 于是 Compression 必须是一个纯粹的**写侧**选项。
func TestUncompressedFilesStayReadable(t *testing.T) {
	path := buildWith(t, compressibleEntries(300), WriterOptions{BlockSize: 4 << 10})

	var bs BlockStats
	r := openWithStats(t, path, nil, 1, &bs)
	for _, k := range []string{keyOf(0), keyOf(150), keyOf(299)} {
		v, _, found, err := r.Get(1<<20, []byte(k))
		if err != nil || !found {
			t.Fatalf("Get(%s): found=%v err=%v", k, found, err)
		}
		if len(v) == 0 {
			t.Fatalf("Get(%s): 空值", k)
		}
	}
	if got := bs.Snapshot().Decompressions; got != 0 {
		t.Fatalf("未压缩文件的解压次数应当是 0，实际 %d", got)
	}
}

// TestWorthCompressing 是判据本身的边界用例。
func TestWorthCompressing(t *testing.T) {
	cases := []struct {
		raw, compressed int
		want            bool
	}{
		{1000, 999, false},  // 几乎没省
		{1000, 900, false},  // 省了 10%，但没到 1/8 的阈值
		{1000, 875, false},  // 边界：正好等于 raw - raw/8，不采用
		{1000, 874, true},   // 边界：刚跨过阈值
		{1000, 500, true},   // 省一半
		{1000, 1200, false}, // 压完更大
		{8, 7, false},       // 小块的流头开销吃掉收益
		{0, 0, false},       // 空块
	}
	for _, c := range cases {
		if got := worthCompressing(c.raw, c.compressed); got != c.want {
			t.Errorf("worthCompressing(%d, %d) = %v, want %v", c.raw, c.compressed, got, c.want)
		}
	}
}

// TestMixedCompressedAndPlainBlocksIterate 验证跨块迭代在"部分块压缩、
// 部分块原样存储"的混合形态下一条不漏。
//
// 块迭代器是按需加载的，混合形态正是最容易在这里出错的地方。
func TestMixedCompressedAndPlainBlocksIterate(t *testing.T) {
	const n = 1200
	entries := mixedEntries(n)
	var bs BlockStats
	path := buildWithStats(t, entries,
		WriterOptions{BlockSize: 1024, Compression: compress.TypeSnappy}, &bs)

	compressed, plain := 0, 0
	for _, b := range dataBlocks(t, path) {
		switch compress.Type(b.typ) {
		case compress.TypeSnappy:
			compressed++
		case compress.TypeNone:
			plain++
		}
	}
	if compressed == 0 || plain == 0 {
		t.Fatalf("期望混合形态，实际 压缩 %d / 未压缩 %d", compressed, plain)
	}
	t.Logf("混合形态：%d 个数据块压缩、%d 个数据块原样", compressed, plain)

	r := openWithStats(t, path, nil, 1, &bs)
	seen := 0
	if err := r.Iterate(func(_, _ []byte) bool { seen++; return true }); err != nil {
		t.Fatal(err)
	}
	if seen != n {
		t.Fatalf("跨块迭代读了 %d 条，期望 %d 条", seen, n)
	}
	if got := bs.Snapshot().Decompressions; got < int64(compressed) {
		t.Fatalf("读了 %d 个压缩块，却只解压了 %d 次", compressed, got)
	}
}

// fileSize 返回文件字节数。
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

// fixed32 把 u 编成 4 字节大端，供测试构造块尾。
func fixed32(u uint32) []byte {
	return []byte{byte(u >> 24), byte(u >> 16), byte(u >> 8), byte(u)}
}
