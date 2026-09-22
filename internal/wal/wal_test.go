package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/xiatianliang1024gm/kvdb/internal/crc"
)

// writeAll 用 Writer 顺序写入若干记录并返回字节流。
func writeAll(t *testing.T, records [][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for i, r := range records {
		if err := w.addRecord(r); err != nil {
			t.Fatalf("addRecord(#%d) failed: %v", i, err)
		}
	}
	if got, want := w.Written(), int64(buf.Len()); got != want {
		t.Fatalf("Written() = %d, 实际写出 %d 字节", got, want)
	}
	return buf.Bytes()
}

// readAll 读回全部记录。
func readAll(t *testing.T, data []byte) [][]byte {
	t.Helper()
	r := NewReader(bytes.NewReader(data))
	var out [][]byte
	for {
		rec, err := r.ReadRecord()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("ReadRecord failed: %v", err)
		}
		out = append(out, append([]byte(nil), rec...))
	}
}

func TestRecordRoundTrip(t *testing.T) {
	sizes := []int{
		0, 1, 6, 7, 8, 100,
		BlockSize - HeaderSize - 1, // 块内留 1 字节，触发补零
		BlockSize - HeaderSize,     // 恰好填满一块
		BlockSize - HeaderSize + 1, // 跨两块
		BlockSize,                  // 跨两块且第二块只剩很少
		2*BlockSize + 123,          // 跨三块
	}
	for _, n := range sizes {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			rec := bytes.Repeat([]byte{'x'}, n)
			second := []byte("tail")
			data := writeAll(t, [][]byte{rec, second})

			got := readAll(t, data)
			if len(got) != 2 {
				t.Fatalf("读回 %d 条记录, want 2", len(got))
			}
			if !bytes.Equal(got[0], rec) {
				t.Fatalf("记录内容不一致：长度 %d vs %d", len(got[0]), len(rec))
			}
			if !bytes.Equal(got[1], second) {
				t.Fatalf("第二条记录 = %q, want %q", got[1], second)
			}
		})
	}
}

// 物理布局必须可被逐字节核对，避免"自己编自己解"的循环验证。
func TestPhysicalRecordLayout(t *testing.T) {
	payload := []byte("hello")
	data := writeAll(t, [][]byte{payload})
	if len(data) != HeaderSize+len(payload) {
		t.Fatalf("长度 = %d, want %d", len(data), HeaderSize+len(payload))
	}
	got := binary.BigEndian.Uint32(data[0:4])
	if want := recordCRC(typeFull, payload); got != want {
		t.Errorf("校验值 = %#x, want %#x", got, want)
	}
	if crc.Unmask(got) == got {
		t.Error("校验值应该是掩码后的形式")
	}
	if got := binary.BigEndian.Uint16(data[4:6]); got != uint16(len(payload)) {
		t.Errorf("长度字段 = %d, want %d", got, len(payload))
	}
	if data[6] != byte(typeFull) {
		t.Errorf("类型字节 = %d, want %d", data[6], typeFull)
	}
	if !bytes.Equal(data[HeaderSize:], payload) {
		t.Errorf("负载 = %q, want %q", data[HeaderSize:], payload)
	}
}

// 块尾放不下一个头时补零，读端必须跳过补零区而不报错。
func TestBlockPaddingIsSkipped(t *testing.T) {
	first := bytes.Repeat([]byte{'a'}, BlockSize-HeaderSize-2) // 留 2 字节
	data := writeAll(t, [][]byte{first, []byte("b")})

	// 第一块尾部应有 2 个 0 字节。
	pad := data[HeaderSize+len(first) : BlockSize]
	if !bytes.Equal(pad, []byte{0, 0}) {
		t.Fatalf("块尾补零 = %v, want [0 0]", pad)
	}
	if got := readAll(t, data); len(got) != 2 || !bytes.Equal(got[1], []byte("b")) {
		t.Fatalf("读回失败: %d 条记录", len(got))
	}
}

// 预分配（全零）的块必须被整块跳过，其后的记录仍要能读到。
func TestZeroFilledBlockIsSkipped(t *testing.T) {
	first := writeAll(t, [][]byte{[]byte("first")})
	if len(first) > BlockSize {
		t.Fatal("测试前提不成立：第一条记录不应跨块")
	}
	data := append([]byte(nil), first...)
	data = append(data, make([]byte, BlockSize-len(first))...) // 补满第一块
	data = append(data, make([]byte, BlockSize)...)            // 一整个全零块

	var tail bytes.Buffer
	if err := NewWriter(&tail).addRecord([]byte("after")); err != nil {
		t.Fatal(err)
	}
	data = append(data, tail.Bytes()...)

	got := readAll(t, data)
	if len(got) != 2 || !bytes.Equal(got[0], []byte("first")) || !bytes.Equal(got[1], []byte("after")) {
		t.Fatalf("读回 = %q, 期望 first/after", got)
	}
}

func TestCorruptChecksum(t *testing.T) {
	data := writeAll(t, [][]byte{[]byte("hello"), []byte("world")})
	data[HeaderSize] ^= 0xff // 翻转第一条记录的负载

	r := NewReader(bytes.NewReader(data))
	if _, err := r.ReadRecord(); err == nil {
		t.Fatal("校验和不匹配时 ReadRecord 应返回错误")
	} else {
		var corrupt *ErrCorruptRecord
		if !errors.As(err, &corrupt) {
			t.Fatalf("错误类型 = %T, want *ErrCorruptRecord", err)
		}
	}
}

func TestTruncatedFragmentReportsCorruption(t *testing.T) {
	// 一条跨块记录只写了一半：读到末尾必须报"记录未拼完"。
	rec := bytes.Repeat([]byte{'z'}, BlockSize+100)
	data := writeAll(t, [][]byte{rec})
	truncated := data[:BlockSize+HeaderSize+40]

	r := NewReader(bytes.NewReader(truncated))
	if _, err := r.ReadRecord(); err == nil {
		t.Fatal("跨块记录被截断时应报错")
	} else {
		var corrupt *ErrCorruptRecord
		if !errors.As(err, &corrupt) {
			t.Fatalf("错误类型 = %T, want *ErrCorruptRecord", err)
		}
	}
}

func TestLogAppendSyncReplay(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir, 7)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if l.Num() != 7 {
		t.Errorf("Num() = %d, want 7", l.Num())
	}
	if want := LogName(dir, 7); l.Path() != want {
		t.Errorf("Path() = %q, want %q", l.Path(), want)
	}

	records := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}
	for _, rec := range records {
		if err := l.Append(rec); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	if l.Size() == 0 {
		t.Error("Size() = 0, 应该已经计入未刷出的缓冲")
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Sync 之后文件必须已经可见，且大小与 Size() 一致。
	info, err := os.Stat(l.Path())
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() != l.Size() {
		t.Errorf("磁盘大小 = %d, Size() = %d", info.Size(), l.Size())
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	var got [][]byte
	res, err := Replay(dir, 7, func(rec []byte) error {
		got = append(got, append([]byte(nil), rec...))
		return nil
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if res.Records != len(records) {
		t.Errorf("重放记录数 = %d, want %d", res.Records, len(records))
	}
	if res.Corrupt != nil {
		t.Errorf("意外报告损坏: %v", res.Corrupt)
	}
	if len(got) != len(records) {
		t.Fatalf("读回 %d 条, want %d", len(got), len(records))
	}
	for i := range records {
		if !bytes.Equal(got[i], records[i]) {
			t.Errorf("记录 #%d = %q, want %q", i, got[i], records[i])
		}
	}
}

// 尾部被写坏时，损坏点之前的数据仍然可用。
func TestReplayToleratesTornTail(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{'p'}, 200)
	for i := 0; i < 3; i++ {
		if err := l.Append(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// 砍掉最后 5 个字节，模拟 kill -9 打断了一次写。
	info, err := os.Stat(l.Path())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(l.Path(), info.Size()-5); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	count := 0
	res, err := Replay(dir, 1, func([]byte) error { count++; return nil })
	if err != nil {
		t.Fatalf("尾部损坏不应作为错误返回: %v", err)
	}
	if res.Corrupt == nil {
		t.Fatal("应报告尾部损坏")
	}
	if count != 2 || res.Records != 2 {
		t.Errorf("重放记录数 = %d/%d, want 2", count, res.Records)
	}
}

// 重放回调返回的错误必须原样抛出，不能被当成"尾部损坏"吞掉。
func TestReplayPropagatesCallbackError(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("boom")
	if _, err := Replay(dir, 1, func([]byte) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("Replay error = %v, want %v", err, sentinel)
	}
}

func TestReplayMissingLog(t *testing.T) {
	if _, err := Replay(t.TempDir(), 42, func([]byte) error { return nil }); err == nil {
		t.Fatal("重放不存在的日志应返回错误")
	}
}

func TestListLogs(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []uint64{3, 1, 12} {
		l, err := Create(dir, n)
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// 干扰项：不符合命名规则的文件不应被当成日志。
	if err := os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "abc.log"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	nums, err := ListLogs(dir)
	if err != nil {
		t.Fatalf("ListLogs failed: %v", err)
	}
	want := []uint64{1, 3, 12}
	if len(nums) != len(want) {
		t.Fatalf("ListLogs() = %v, want %v", nums, want)
	}
	for i := range want {
		if nums[i] != want[i] {
			t.Fatalf("ListLogs() = %v, want %v", nums, want)
		}
	}
}

func TestLogRemoveAndCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	l, err := Create(dir, 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("重复 Close 应无副作用: %v", err)
	}
	if err := l.Append([]byte("x")); err == nil {
		t.Error("向已关闭的日志追加应报错")
	}
	if err := l.Sync(); err == nil {
		t.Error("对已关闭的日志 Sync 应报错")
	}
	if err := l.Remove(); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if _, err := os.Stat(l.Path()); !os.IsNotExist(err) {
		t.Errorf("日志文件仍然存在: %v", err)
	}
	if err := RemoveLog(dir, 5); err != nil {
		t.Errorf("删除不存在的日志不应报错: %v", err)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
