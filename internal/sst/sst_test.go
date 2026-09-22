package sst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/xiatianliang1024gm/kvdb/internal/cache"
	"github.com/xiatianliang1024gm/kvdb/internal/key"
)

type testComparer struct{}

func (testComparer) Compare(a, b []byte) int { return bytes.Compare(a, b) }
func (testComparer) Name() string            { return "test.Bytewise" }

func ik(s string, seq uint64, kind key.Kind) []byte {
	return key.EncodeInternalKey([]byte(s), seq, kind)
}

// entry 描述一条待写入的测试数据。
type entry struct {
	K string
	S uint64
	T key.Kind
	V string
}

// buildWith 按 opts 写一个测试用的 SST，返回其路径。
func buildWith(t *testing.T, entries []entry, opts WriterOptions) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "000001.sst")
	w, err := NewWriter(path, testComparer{}, opts)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	for _, e := range entries {
		if err := w.Add(ik(e.K, e.S, e.T), []byte(e.V)); err != nil {
			t.Fatalf("Add(%q#%d) failed: %v", e.K, e.S, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	return path
}

// build 用默认参数写一个测试用的 SST。
func build(t *testing.T, entries []entry) string { return buildWith(t, entries, WriterOptions{}) }

// openReader 打开一个测试用的 Reader，测试结束自动关闭。
func openReader(t *testing.T, path string, c *cache.Cache, num uint64) *Reader {
	t.Helper()
	r, err := Open(path, OpenOptions{Comparer: testComparer{}, Cache: c, FileNum: num})
	if err != nil {
		t.Fatalf("Open(%s) failed: %v", path, err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

// sequence 生成 n 个 user key 的升序版本序列：每个 key 5 个版本，尾缀降序。
func sequence(keys, versions int, start uint64) []entry {
	var out []entry
	seq := start
	for k := 0; k < keys; k++ {
		for v := 0; v < versions; v++ {
			out = append(out, entry{fmt.Sprintf("k%04d", k), seq, key.TypeValue, fmt.Sprintf("v%d", v)})
			seq--
		}
	}
	return out
}

func TestWriterReaderRoundTrip(t *testing.T) {
	path := build(t, []entry{
		{"a", 5, key.TypeValue, "a5"},
		{"a", 2, key.TypeValue, "a2"},
		{"c", 4, key.TypeValue, "c4"},
		{"e", 7, key.TypeDeletion, ""},
	})
	r := openReader(t, path, nil, 1)

	if n, err := r.Count(); err != nil || n != 4 {
		t.Fatalf("Count() = %d, %v; want 4", n, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if r.Size() != info.Size() {
		t.Errorf("Size() = %d, 文件大小 = %d", r.Size(), info.Size())
	}
	if r.NumBlocks() != 1 {
		t.Errorf("NumBlocks() = %d, want 1", r.NumBlocks())
	}

	var got []string
	if err := r.Iterate(func(ik, v []byte) bool {
		got = append(got, fmt.Sprintf("%s#%d/%s=%s", key.UserKey(ik), key.SeqNum(ik), key.KindOf(ik), v))
		return true
	}); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}
	want := []string{"a#5/Value=a5", "a#2/Value=a2", "c#4/Value=c4", "e#7/Deletion="}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("遍历结果 %v, want %v", got, want)
		}
	}
}

func TestGetRespectsSnapshotAndTombstone(t *testing.T) {
	// 注意写入顺序：同一 user key 的新版本（尾缀更大）必须排在前面。
	path := build(t, []entry{
		{"a", 3, key.TypeValue, "a3"},
		{"a", 1, key.TypeValue, "a1"},
		{"b", 2, key.TypeDeletion, ""},
		{"c", 4, key.TypeValue, "c4"},
	})
	r := openReader(t, path, nil, 1)

	tests := []struct {
		name     string
		snapshot uint64
		k        string
		wantV    string
		wantKind key.Kind
		wantFind bool
	}{
		{"旧快照看到旧版本", 2, "a", "a1", key.TypeValue, true},
		{"新快照看到新版本", 3, "a", "a3", key.TypeValue, true},
		{"更大的快照仍是最新版本", 100, "a", "a3", key.TypeValue, true},
		{"快照早于首次写入", 0, "a", "", 0, false},
		{"墓碑被命中", 5, "b", "", key.TypeDeletion, true},
		{"墓碑之前不可见", 1, "b", "", 0, false},
		{"快照恰好等于墓碑的 seq", 2, "b", "", key.TypeDeletion, true},
		{"不存在的 key", 5, "bb", "", 0, false},
		{"越过末尾", 5, "z", "", 0, false},
		{"比最小 key 还小", 5, "", "", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, kind, found, err := r.Get(tt.snapshot, []byte(tt.k))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			if found != tt.wantFind {
				t.Fatalf("found = %v, want %v", found, tt.wantFind)
			}
			if !found {
				return
			}
			if kind != tt.wantKind {
				t.Errorf("kind = %v, want %v", kind, tt.wantKind)
			}
			if string(v) != tt.wantV {
				t.Errorf("value = %q, want %q", v, tt.wantV)
			}
		})
	}
}

// Get 返回的 value 必须可以安全保留（缓存命中与未命中两种路径都不能被后续读污染）。
func TestGetValueIsStable(t *testing.T) {
	path := buildWith(t, []entry{
		{"a", 1, key.TypeValue, "alpha"},
		{"b", 1, key.TypeValue, "bravo"},
	}, WriterOptions{BloomBitsPerKey: 10})

	for _, c := range []*cache.Cache{nil, cache.New(1 << 20)} {
		r := openReader(t, path, c, 42)
		first, _, _, err := r.Get(1, []byte("a"))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := r.Get(1, []byte("b")); err != nil {
			t.Fatal(err)
		}
		if string(first) != "alpha" {
			t.Fatalf("第一次 Get 的结果被后续读取污染: %q", first)
		}
		if n, err := r.Count(); err != nil || n != 2 {
			t.Fatalf("Count() = %d, %v; want 2", n, err)
		}
		r.Close()
	}
}

// 多个数据块：写小、读全，验证"跨块遍历"与"跨块点查"都正确。
func TestMultiBlockFile(t *testing.T) {
	entries := sequence(300, 3, 100000)
	path := buildWith(t, entries, WriterOptions{BlockSize: 512, BloomBitsPerKey: 10})
	r := openReader(t, path, nil, 7)

	if r.NumBlocks() < 5 {
		t.Fatalf("NumBlocks() = %d, 期望切成多块", r.NumBlocks())
	}
	if !r.FilterEnabled() {
		t.Error("BloomBitsPerKey > 0 时应当带过滤器")
	}

	// 遍历：条数与顺序都要对上。
	var got []string
	if err := r.Iterate(func(ik, _ []byte) bool {
		got = append(got, fmt.Sprintf("%s#%d", key.UserKey(ik), key.SeqNum(ik)))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(entries) {
		t.Fatalf("遍历出 %d 条, want %d", len(got), len(entries))
	}
	for i, e := range entries {
		if want := fmt.Sprintf("%s#%d", e.K, e.S); got[i] != want {
			t.Fatalf("第 %d 条 = %s, want %s", i, got[i], want)
		}
	}

	// 点查：每个 key 的最新版本都要命中。
	for k := 0; k < 300; k++ {
		uk := fmt.Sprintf("k%04d", k)
		v, _, found, err := r.Get(1<<40, []byte(uk))
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", uk, err)
		}
		if !found || string(v) != "v0" {
			t.Fatalf("Get(%s) = (%q, found=%v), want v0", uk, v, found)
		}
	}
}

// M2 的核心约定：同一个 user key 的所有版本必须落在同一个数据块里，
// 否则"按索引定位到唯一一块、块内 seek 一次"就不再正确。
func TestVersionsOfOneKeyStayInOneBlock(t *testing.T) {
	entries := sequence(60, 5, 100000)
	path := buildWith(t, entries, WriterOptions{BlockSize: 128, BloomBitsPerKey: 10})
	r := openReader(t, path, nil, 1)
	if r.NumBlocks() < 10 {
		t.Fatalf("NumBlocks() = %d, 期望被切成很多块", r.NumBlocks())
	}

	// 逐块扫描，记录每个 user key 出现在哪个块里。
	owner := map[string]int{}
	ii, err := newBlockIter(r.index, r.icmp.Compare)
	if err != nil {
		t.Fatal(err)
	}
	blockIdx := 0
	for ii.SeekToFirst(); ii.Valid(); ii.Next() {
		h, _, err := decodeBlockHandle(ii.Value())
		if err != nil {
			t.Fatal(err)
		}
		block, err := r.readBlock(h)
		if err != nil {
			t.Fatal(err)
		}
		bi, err := newBlockIter(block, r.icmp.Compare)
		if err != nil {
			t.Fatal(err)
		}
		for bi.SeekToFirst(); bi.Valid(); bi.Next() {
			uk := string(key.UserKey(bi.Key()))
			if prev, ok := owner[uk]; ok && prev != blockIdx {
				t.Fatalf("user key %s 的版本跨了块：块 %d 与块 %d", uk, prev, blockIdx)
			}
			owner[uk] = blockIdx
		}
		blockIdx++
	}
	if blockIdx != r.NumBlocks() {
		t.Fatalf("扫描到 %d 块, NumBlocks() = %d", blockIdx, r.NumBlocks())
	}
	if len(owner) != 60 {
		t.Fatalf("共扫描到 %d 个 user key, want 60", len(owner))
	}
}

// 索引项必须等于所属数据块的最大 key，且 handle 单调递增。
func TestIndexRecordsBlockMaxKeys(t *testing.T) {
	entries := sequence(80, 2, 100000)
	path := buildWith(t, entries, WriterOptions{BlockSize: 256})
	r := openReader(t, path, nil, 1)

	var lastOffset uint64
	ii, err := newBlockIter(r.index, r.icmp.Compare)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for ii.SeekToFirst(); ii.Valid(); ii.Next() {
		h, _, err := decodeBlockHandle(ii.Value())
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 && h.offset <= lastOffset {
			t.Fatalf("索引第 %d 项的偏移 %d 没有递增（上一块 %d）", n, h.offset, lastOffset)
		}
		lastOffset = h.offset

		block, err := r.readBlock(h)
		if err != nil {
			t.Fatal(err)
		}
		bi, err := newBlockIter(block, r.icmp.Compare)
		if err != nil {
			t.Fatal(err)
		}
		var maxKey []byte
		for bi.SeekToFirst(); bi.Valid(); bi.Next() {
			maxKey = append(maxKey[:0], bi.Key()...)
		}
		if !bytes.Equal(maxKey, ii.Key()) {
			t.Fatalf("索引第 %d 项的 key = %q, 而块的最大 key = %q", n, ii.Key(), maxKey)
		}
		n++
	}
	if n != r.NumBlocks() {
		t.Fatalf("索引 %d 项, NumBlocks() = %d", n, r.NumBlocks())
	}
}

// 点查的 IO 次数必须是 O(1) 个数据块，而不是"把文件扫一遍"。
//
// 这是 M2 的验收标准之一，用块缓存的未命中计数把它量化下来：
// 索引与过滤器在 Open 时就已读入，所以一次点查最多只会引起一次块读取。
func TestPointLookupReadsASingleBlock(t *testing.T) {
	entries := sequence(200, 3, 100000)
	path := buildWith(t, entries, WriterOptions{BlockSize: 512, BloomBitsPerKey: 10})
	c := cache.New(1 << 20)
	r := openReader(t, path, c, 5)

	// 命中的 key：恰好读入 1 个数据块。
	for _, k := range []string{"k0000", "k0050", "k0100", "k0199"} {
		before := c.Stats().Misses
		if _, _, found, err := r.Get(1<<40, []byte(k)); err != nil || !found {
			t.Fatalf("Get(%s) = (found=%v, err=%v)", k, found, err)
		}
		if got := c.Stats().Misses - before; got != 1 {
			t.Errorf("Get(%s) 读了 %d 个数据块，want 1", k, got)
		}
	}

	// 不存在的 key：Bloom 应当挡掉绝大多数，只有假阳性才会真的去读块。
	before := c.Stats().Misses
	const probes = 1000
	for i := 0; i < probes; i++ {
		if _, _, found, err := r.Get(1<<40, []byte(fmt.Sprintf("absent-%06d", i))); err != nil || found {
			t.Fatalf("不存在的 key 被读到了: found=%v err=%v", found, err)
		}
	}
	reads := c.Stats().Misses - before
	if reads > probes/10 {
		t.Errorf("%d 次不存在查询触发了 %d 次块读取，过滤器几乎没有起作用", probes, reads)
	}
	t.Logf("%d 次不存在查询触发了 %d 次块读取（假阳性）", probes, reads)
}

// 关闭过滤器后结果必须完全相同，只是读块次数变多。
func TestFilterIsOptional(t *testing.T) {
	entries := sequence(50, 2, 1000)
	noFilter := openReader(t, buildWith(t, entries, WriterOptions{BlockSize: 256}), nil, 1)
	withFilter := openReader(t, buildWith(t, entries, WriterOptions{BlockSize: 256, BloomBitsPerKey: 10}), nil, 1)

	if noFilter.FilterEnabled() {
		t.Error("BloomBitsPerKey = 0 时不应当有过滤器")
	}
	if !withFilter.FilterEnabled() {
		t.Error("BloomBitsPerKey > 0 时应当有过滤器")
	}
	for i := 0; i < 50; i++ {
		uk := fmt.Sprintf("k%04d", i)
		a, _, fa, err := noFilter.Get(1<<40, []byte(uk))
		if err != nil {
			t.Fatal(err)
		}
		b, _, fb, err := withFilter.Get(1<<40, []byte(uk))
		if err != nil {
			t.Fatal(err)
		}
		if fa != fb || !bytes.Equal(a, b) {
			t.Fatalf("带/不带过滤器对 %s 的结果不一致", uk)
		}
	}
}

// 块缓存的命中路径必须返回同样的内容。
func TestCacheServesRepeatedReads(t *testing.T) {
	path := buildWith(t, sequence(100, 2, 1000), WriterOptions{BlockSize: 512})
	c := cache.New(1 << 20)
	r := openReader(t, path, c, 9)

	for round := 0; round < 3; round++ {
		for k := 0; k < 100; k++ {
			uk := fmt.Sprintf("k%04d", k)
			v, _, found, err := r.Get(1<<40, []byte(uk))
			if err != nil || !found || string(v) != "v0" {
				t.Fatalf("第 %d 轮 Get(%s) = (%q, found=%v, err=%v)", round, uk, v, found, err)
			}
		}
	}
	st := c.Stats()
	if st.Hits == 0 {
		t.Fatal("重复读取之后缓存应当有命中")
	}
	if st.Evictions != 0 {
		t.Errorf("1MB 缓存装不下 %d 个 512B 的块，说明容量计算有问题（淘汰 %d）", r.NumBlocks(), st.Evictions)
	}
}

// 数据块被改动一个字节，读取时必须报 CRC 错误而不是返回错数据。
func TestCorruptDataBlockDetected(t *testing.T) {
	path := buildWith(t, sequence(50, 2, 1000), WriterOptions{BlockSize: 512, BloomBitsPerKey: 10})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[5] ^= 0xff // 第一个数据块内容里的一个字节
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	r := openReader(t, path, nil, 1)
	_, _, _, err = r.Get(1<<40, []byte("k0000"))
	if !errors.Is(err, ErrCorruptBlock) {
		t.Fatalf("Get 的 error = %v, want ErrCorruptBlock", err)
	}
}

// 索引块损坏必须在 Open 阶段就被发现（Open 会完整校验索引）。
func TestCorruptIndexDetectedAtOpen(t *testing.T) {
	path := buildWith(t, sequence(50, 2, 1000), WriterOptions{BlockSize: 512})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 索引块紧挨着 Footer，改它尾部的校验值必然被 Open 抓到。
	raw[len(raw)-FooterLen-1] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, OpenOptions{Comparer: testComparer{}}); !errors.Is(err, ErrCorruptBlock) {
		t.Fatalf("Open 的 error = %v, want ErrCorruptBlock", err)
	}
}

func TestOpenRejectsBadFiles(t *testing.T) {
	dir := t.TempDir()

	// 1. 太短的文件（M1 的半截 Flush 就是这种形态）
	short := filepath.Join(dir, "short.sst")
	if err := os.WriteFile(short, []byte("half-written garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(short, OpenOptions{Comparer: testComparer{}}); !errors.Is(err, ErrBadFooter) {
		t.Fatalf("半截文件 error = %v, want ErrBadFooter", err)
	}

	// 2. 魔数不对
	bad := filepath.Join(dir, "bad.sst")
	data := make([]byte, FooterLen)
	data[FooterLen-1] = 0xff
	if err := os.WriteFile(bad, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(bad, OpenOptions{Comparer: testComparer{}}); !errors.Is(err, ErrBadFooter) {
		t.Fatalf("魔数错误 error = %v, want ErrBadFooter", err)
	}

	// 3. M1 的魔数：必须报"格式不兼容"，而不是被当成损坏文件
	legacy := filepath.Join(dir, "legacy.sst")
	buf := make([]byte, FooterLen)
	binary.BigEndian.PutUint64(buf[footerHandleArea:], magicM1)
	if err := os.WriteFile(legacy, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(legacy, OpenOptions{Comparer: testComparer{}}); !errors.Is(err, ErrLegacyFormat) {
		t.Fatalf("M1 格式 error = %v, want ErrLegacyFormat", err)
	}

	// 4. Footer 补零区被写脏
	dirty := filepath.Join(dir, "dirty.sst")
	raw, err := os.ReadFile(buildWith(t, []entry{{"a", 1, key.TypeValue, "v"}}, WriterOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-FooterLen+10] = 0x7f
	if err := os.WriteFile(dirty, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dirty, OpenOptions{Comparer: testComparer{}}); !errors.Is(err, ErrBadFooter) {
		t.Fatalf("Footer 补零区脏 error = %v, want ErrBadFooter", err)
	}

	// 5. 不存在的文件
	if _, err := Open(filepath.Join(dir, "missing.sst"), OpenOptions{Comparer: testComparer{}}); err == nil {
		t.Fatal("打开不存在的文件应当报错")
	}
}

// footer 的字节布局必须可逐字节核对。
func TestFooterLayout(t *testing.T) {
	path := build(t, []entry{{"a", 1, key.TypeValue, "v"}})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	footer := raw[len(raw)-FooterLen:]
	if want := "kvdb0002"; string(footer[footerHandleArea:]) != want {
		t.Errorf("魔数 = %q, want %q", footer[footerHandleArea:], want)
	}

	meta, index, err := decodeFooter(footer)
	if err != nil {
		t.Fatal(err)
	}
	// 文件顺序：数据块 → MetaIndex → Index → Footer
	if meta.offset >= index.offset {
		t.Errorf("MetaIndex 偏移 %d 应当在 Index 偏移 %d 之前", meta.offset, index.offset)
	}
	if index.offset+index.size+BlockTrailerLen != uint64(len(raw)-FooterLen) {
		t.Errorf("索引块结束于 %d, footer 从 %d 开始", index.offset+index.size+BlockTrailerLen, len(raw)-FooterLen)
	}
	// 两处 handle 必须能从 Footer 里原样解出来。
	var scratch []byte
	if got := index.encode(scratch); len(got) == 0 {
		t.Error("handle 编码为空")
	}
}

// 空文件（没有任何记录）也要能打开并读到 not found。
func TestEmptyFile(t *testing.T) {
	path := build(t, []entry{})
	r := openReader(t, path, nil, 1)
	if r.NumBlocks() != 0 {
		t.Errorf("NumBlocks() = %d, want 0", r.NumBlocks())
	}
	if n, err := r.Count(); err != nil || n != 0 {
		t.Fatalf("Count() = %d, %v; want 0", n, err)
	}
	if _, _, found, err := r.Get(1, []byte("a")); err != nil || found {
		t.Fatalf("Get on empty file = (found=%v, err=%v), want (false, nil)", found, err)
	}
	it := r.NewIterator()
	it.SeekToFirst()
	if it.Valid() {
		t.Fatal("空文件的迭代器不应有效")
	}
	it.Seek([]byte("a"))
	if it.Valid() {
		t.Fatal("空文件的 Seek 不应有效")
	}
}

// Abandon 不应留下半截文件。
func TestAbandonRemovesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial.sst")
	w, err := NewWriter(path, testComparer{}, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(ik("a", 1, key.TypeValue), []byte("v")); err != nil {
		t.Fatal(err)
	}
	w.Abandon()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("Abandon 之后文件仍存在: %v", err)
	}
}

func TestWriterRejectsUnorderedKeys(t *testing.T) {
	w, err := NewWriter(filepath.Join(t.TempDir(), "x.sst"), testComparer{}, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abandon()

	if err := w.Add(ik("b", 1, key.TypeValue), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(ik("a", 1, key.TypeValue), []byte("2")); err == nil {
		t.Fatal("乱序写入应当被拒绝")
	}
	if err := w.Add(ik("b", 1, key.TypeValue), []byte("2")); err == nil {
		t.Fatal("重复 key 应当被拒绝")
	}
	// 同一 user key 的更旧版本（尾缀更小）排在其后，属于合法顺序。
	if err := w.Add(ik("b", 0, key.TypeValue), []byte("older")); err != nil {
		t.Fatalf("同 key 的旧版本应被接受: %v", err)
	}
	if err := w.Add([]byte("short"), nil); err == nil {
		t.Fatal("过短的 internal key 应当被拒绝")
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(ik("z", 1, key.TypeValue), []byte("x")); err == nil {
		t.Fatal("Finish 之后继续 Add 应当被拒绝")
	}
}

// 大 value 会撑破目标块大小，必须完整读回，且不影响后续记录。
func TestLargeValue(t *testing.T) {
	big := bytes.Repeat([]byte{'q'}, 200<<10)
	path := buildWith(t, []entry{
		{"a", 1, key.TypeValue, string(big)},
		{"b", 1, key.TypeValue, "small"},
	}, WriterOptions{BlockSize: 4 << 10, BloomBitsPerKey: 10})
	r := openReader(t, path, nil, 1)

	// 一个 200KB 的 value 不可能塞进 4KB 的块，所以第一块必然超出目标大小；
	// 但"同一 user key 不跨块"的约定仍然成立，因此只需要两块。
	if r.NumBlocks() != 2 {
		t.Errorf("NumBlocks() = %d, want 2", r.NumBlocks())
	}
	v, _, found, err := r.Get(1, []byte("a"))
	if err != nil || !found {
		t.Fatalf("Get failed: found=%v err=%v", found, err)
	}
	if !bytes.Equal(v, big) {
		t.Fatalf("大 value 读回不一致：%d vs %d 字节", len(v), len(big))
	}
	if v, _, found, _ := r.Get(1, []byte("b")); !found || string(v) != "small" {
		t.Fatal("大 value 之后读不到后续记录")
	}
	// 遍历也要能跨过大 value。
	if n, err := r.Count(); err != nil || n != 2 {
		t.Fatalf("Count() = %d, %v; want 2", n, err)
	}
}
