package logger

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestNilLoggerIsNoOp 锁定 nil 约定：引擎里不必到处判空。
func TestNilLoggerIsNoOp(t *testing.T) {
	var l *Logger
	l.Logf(LevelError, "should not panic %d", 1)
	if got := l.Size(); got != 0 {
		t.Errorf("nil.Size() = %d", got)
	}
	if got := l.Name(); got != "" {
		t.Errorf("nil.Name() = %q", got)
	}
	if err := l.Rotate(); err != nil {
		t.Errorf("nil.Rotate() = %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("nil.Close() = %v", err)
	}
}

// TestLineFormat 锁定日志行的格式。
//
// 格式是"人能 grep、机器能解析"的前提，改成别的样子会让既有的事故记录
// 无法与新记录放在一起对照，所以用正则把它钉死。
func TestLineFormat(t *testing.T) {
	var buf bytes.Buffer
	l := NewWriter(&buf)
	l.Logf(LevelInfo, "open db with %d files", 12)

	line := strings.TrimSuffix(buf.String(), "\n")
	re := regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6} INFO  open db with 12 files$`)
	if !re.MatchString(line) {
		t.Fatalf("日志行格式不符: %q", line)
	}
}

// TestLevelStrings 锁定级别名，日志里要按它们对齐。
func TestLevelStrings(t *testing.T) {
	for lvl, want := range map[Level]string{
		LevelDebug: "DEBUG", LevelInfo: "INFO ", LevelWarning: "WARN ", LevelError: "ERROR",
	} {
		if got := lvl.String(); got != want {
			t.Errorf("Level(%d).String() = %q, want %q", lvl, got, want)
		}
	}
	if got := Level(9).String(); got == "" {
		t.Error("未知级别也应当有个可读的名字")
	}
}

// TestNewlinesAreFlattened 验证多行消息被压成一行。
//
// "一行一条事件"是 grep / awk / 人工扫读的前提；放进去一个换行就会让
// 后续所有基于行号的分析失真。
func TestNewlinesAreFlattened(t *testing.T) {
	var buf bytes.Buffer
	l := NewWriter(&buf)
	l.Logf(LevelWarning, "torn logs: %v\ndiscarded: %v\r\nmore", 1, 2)

	out := buf.String()
	if strings.Count(out, "\n") != 1 || !strings.HasSuffix(out, "\n") {
		t.Fatalf("消息里的换行没有被压平: %q", out)
	}
}

// TestFileLoggerWritesAndAppends 验证文件模式：多次打开是追加而不是截断。
//
// 截断会让"上次运行到底发生了什么"永远丢失，而崩溃排查最需要的恰恰是它。
func TestFileLoggerWritesAndAppends(t *testing.T) {
	dir := t.TempDir()
	l, err := NewFile(dir, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := l.Name(); got != filepath.Join(dir, DefaultName) {
		t.Fatalf("Name() = %q", got)
	}
	l.Logf(LevelInfo, "first run")
	if l.Size() <= 0 {
		t.Fatal("Size() 应当随写入增长")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := NewFile(dir, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	l2.Logf(LevelInfo, "second run")
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}

	b, err := os.ReadFile(filepath.Join(dir, DefaultName))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	if !strings.Contains(text, "first run") || !strings.Contains(text, "second run") {
		t.Fatalf("重开后应当追加而不是截断: %q", text)
	}
}

// TestRotation 验证按大小轮转：LOG 滚成 LOG.old，且历史只保留一份。
//
// 不轮转的日志迟早会把磁盘写满，而这正是"生产化"要防的那类事故。
func TestRotation(t *testing.T) {
	dir := t.TempDir()
	l, err := NewFile(dir, "", 512) // 很小的上限，方便触发
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	for i := 0; i < 200; i++ {
		l.Logf(LevelInfo, "event %04d", i)
	}

	cur, err := os.ReadFile(filepath.Join(dir, DefaultName))
	if err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(filepath.Join(dir, DefaultName+oldSuffix))
	if err != nil {
		t.Fatalf("应当有轮转出的历史文件: %v", err)
	}
	curFirst, ok1 := firstEvent(cur)
	oldFirst, ok2 := firstEvent(old)
	if !ok1 || !ok2 {
		t.Fatalf("两个文件里都应当有事件行:\n cur=%q\n old=%q", cur, old)
	}
	if !(oldFirst < curFirst) {
		t.Errorf("历史文件里应当是更早的内容：old 起于 %d，cur 起于 %d", oldFirst, curFirst)
	}
	if !strings.Contains(string(cur), "event 0199") {
		t.Error("当前文件里应当有最新的内容")
	}
	if strings.Contains(string(old), "event 0199") {
		t.Error("最新的内容不该还在历史文件里")
	}

	// 再写一段，历史文件应当被新的历史替换而不是堆积。
	for i := 200; i < 400; i++ {
		l.Logf(LevelInfo, "event %04d", i)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var logs []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), DefaultName) {
			logs = append(logs, e.Name())
		}
	}
	if len(logs) > 2 {
		t.Errorf("日志文件堆积了 %d 个: %v", len(logs), logs)
	}
}

// TestRotateExplicit 覆盖手动轮转。
func TestRotateExplicit(t *testing.T) {
	dir := t.TempDir()
	l, err := NewFile(dir, "", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.Logf(LevelInfo, "before rotate")
	if err := l.Rotate(); err != nil {
		t.Fatal(err)
	}
	if got := l.Size(); got != 0 {
		t.Errorf("轮转之后新文件应当是空的，实际 %d 字节", got)
	}
	if _, err := os.Stat(filepath.Join(dir, DefaultName+oldSuffix)); err != nil {
		t.Fatalf("轮转应当留下历史文件: %v", err)
	}
	l.Logf(LevelInfo, "after rotate")
}

// TestCustomWriterIsNotClosed 验证写向自定义 writer 时 Close 不去关它。
//
// writer 的归属权在调用方：引擎不该把宿主程序的日志 sink 关掉。
func TestCustomWriterIsNotClosed(t *testing.T) {
	var buf bytes.Buffer
	l := NewWriter(&buf)
	l.Logf(LevelInfo, "hello")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Close 之后再写应当被忽略（closed 检查），但 buffer 本身仍然可用。
	l.Logf(LevelInfo, "after close")
	if strings.Contains(buf.String(), "after close") {
		t.Error("Close 之后不该再写入")
	}
	buf.WriteString("still usable\n")
	if !strings.Contains(buf.String(), "still usable") {
		t.Error("自定义 writer 不该被关闭")
	}
	// 重复 Close 是安全的。
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestCloseIsIdempotent 与 Rotate 在已关闭之后的空操作语义。
func TestCloseOnFileLogger(t *testing.T) {
	dir := t.TempDir()
	l, err := NewFile(dir, "CUSTOM", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "CUSTOM")); err != nil {
		t.Fatal(err)
	}
	if err := l.Rotate(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Rotate(); err != nil {
		t.Errorf("关闭之后的 Rotate 应当是空操作: %v", err)
	}
}

// TestConcurrentLogf 在 -race 下压并发写，确保锁覆盖了 written 与 writer。
func TestConcurrentLogf(t *testing.T) {
	dir := t.TempDir()
	l, err := NewFile(dir, "", 8<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				l.Logf(LevelDebug, "g%d i%d", g, i)
			}
		}(g)
	}
	wg.Wait()

	// 并发写 + 轮转之后，当前文件里不该出现半行。
	cur, err := os.ReadFile(filepath.Join(dir, DefaultName))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSuffix(string(cur), "\n"), "\n") {
		if line == "" {
			continue
		}
		if !regexp.MustCompile(`^\d{4}/\d{2}/\d{2} `).MatchString(line) {
			t.Fatalf("并发写入把日志行撕坏了: %q", line)
		}
	}
}

// TestTimestampUsesInjectedClock 用一个固定时钟验证时间戳精确到微秒。
func TestTimestampUsesInjectedClock(t *testing.T) {
	var buf bytes.Buffer
	l := NewWriter(&buf)
	l.now = func() time.Time { return time.Date(2026, 9, 18, 19, 9, 31, 123456000, time.UTC) }
	l.Logf(LevelInfo, "tick")
	want := "2026/09/18 19:09:31.123456 INFO  tick\n"
	if buf.String() != want {
		t.Fatalf("时间戳格式不符:\n got %q\nwant %q", buf.String(), want)
	}
}

// TestDefaultConstants 把默认值钉住：它们会出现在文档与运维手册里。
func TestDefaultConstants(t *testing.T) {
	if DefaultMaxSize != 1<<20 {
		t.Errorf("DefaultMaxSize = %d", DefaultMaxSize)
	}
	if DefaultName != "LOG" {
		t.Errorf("DefaultName = %q", DefaultName)
	}
	if fmt.Sprintf("%s%s", DefaultName, oldSuffix) != "LOG.old" {
		t.Errorf("历史文件名不是 LOG.old")
	}
}

// firstEvent 从日志内容里取出第一个 "event NNNN" 的编号。
func firstEvent(b []byte) (int, bool) {
	re := regexp.MustCompile(`event (\d{4})`)
	m := re.FindSubmatch(b)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(string(m[1]))
	return n, err == nil
}
