// Package logger 提供数据目录里的运行日志（`LOG` 与轮转出的 `LOG.old`）。
//
// 为什么引擎需要一份自己的日志，而不是让调用方去接 slog：
//
//   - **引擎的"事件"是运维事实，不是调试信息**：哪次 Flush 落了哪个文件、
//     哪次 Compaction 吃了多少字节、恢复时丢了什么、压缩比是多少。
//     这些信息在出故障时是唯一能还原现场的东西，必须与数据目录绑在一起
//     —— 数据目录被拷到别的机器上之后，日志要跟着走。
//   - **它必须自己管轮转**：一个长期运行的服务如果日志无限增长，
//     最后是它把磁盘写满。这里的策略与 RocksDB 一致：超过上限就整体滚到
//     `LOG.old`，只保留一份历史。
//
// 格式刻意做成"人能 grep、机器能解析"的定宽前缀：
//
//	2026/09/18 19:09:31.123456 INFO  flush L0 file 000123 (512.4 KB) in 12ms
//
// 时间戳精确到微秒，是因为 Compaction 与 Flush 的耗时经常在毫秒级，
// 用秒级时间戳去看"它们是不是重叠了"完全看不出东西。
package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// 默认值。Options 里 0 走前者，负数表示显式关闭文件日志。
const (
	// DefaultMaxSize 是 LOG 的默认大小上限：超过就轮转到 LOG.old。
	//
	// 1MB 大致能装下上万条事件，够覆盖"最近几小时到几天"的运维窗口，
	// 又小到不会在任何一台机器上成为磁盘占用问题。
	DefaultMaxSize = 1 << 20
	// DefaultName 是日志文件名。
	DefaultName = "LOG"
	// oldSuffix 是轮转出的历史文件的后缀（LOG → LOG.old）。
	oldSuffix = ".old"
)

// Level 是日志级别。数值越大越严重。
type Level uint8

const (
	// LevelDebug 是压测与排查用的细粒度信息。
	LevelDebug Level = iota
	// LevelInfo 是正常的运维事件（打开、Flush、Compaction、Checkpoint）。
	LevelInfo
	// LevelWarning 是"能继续跑，但需要人看一眼"的情况（丢弃了残缺文件、限流触发）。
	LevelWarning
	// LevelError 是故障（后台写失败、停库）。
	LevelError
)

// String 返回定宽的名字，用于对齐日志前缀。
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO "
	case LevelWarning:
		return "WARN "
	case LevelError:
		return "ERROR"
	default:
		return fmt.Sprintf("L%-4d", uint8(l))
	}
}

// Logger 是一个可选的、带轮转的文件日志器，也可以写向任意 io.Writer。
//
// 并发安全。nil 接收者是安全的空操作，所以引擎不必到处判空 —— 这与
// internal/cache 的 nil 约定一致。
type Logger struct {
	mu      sync.Mutex
	w       io.Writer // 当前输出目标
	file    *os.File  // 只有文件模式非 nil
	dir     string
	name    string
	maxSize int64
	written int64
	closed  bool
	// now 可被测试替换，用来断言时间戳格式。
	now func() time.Time
}

// NewFile 在 dir 下创建（必要时轮转后重建）日志文件。
//
// maxSize <= 0 时用 DefaultMaxSize；name 为空时用 DefaultName。
func NewFile(dir, name string, maxSize int) (*Logger, error) {
	if name == "" {
		name = DefaultName
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxSize
	}
	l := &Logger{dir: dir, name: name, maxSize: int64(maxSize), now: time.Now}
	if err := l.openLocked(); err != nil {
		return nil, err
	}
	return l, nil
}

// NewWriter 创建一个只写向 w 的日志器，不做轮转。
//
// 它存在的理由是让引擎内部的日志可被测试与宿主程序接管：测试用它把日志收进
// 内存缓冲区，宿主程序可以用它把事件转投到自己的日志系统里。
func NewWriter(w io.Writer) *Logger {
	return &Logger{w: w, now: time.Now}
}

// openLocked 打开（或重建）日志文件句柄。调用方必须持有 l.mu。
func (l *Logger) openLocked() error {
	path := filepath.Join(l.dir, l.name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("kvdb/logger: open %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("kvdb/logger: stat %s: %w", path, err)
	}
	l.file, l.w, l.written = f, f, info.Size()
	return nil
}

// Name 返回日志文件路径；写向自定义 writer 时返回空串。
func (l *Logger) Name() string {
	if l == nil || l.dir == "" {
		return ""
	}
	return filepath.Join(l.dir, l.name)
}

// Size 返回当前日志文件已有的字节数（含本次进程之前写下的）。
func (l *Logger) Size() int64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.written
}

// Logf 写一条带时间戳与级别的事件。
//
// 消息里的换行会被替换成空格：多行消息会让"一行一条事件"这个前提失效，
// 之后 grep 与 awk 都会失真。
func (l *Logger) Logf(level Level, format string, args ...any) {
	if l == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	if strings.ContainsAny(msg, "\r\n") {
		msg = strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ").Replace(msg)
	}
	line := fmt.Sprintf("%s %s %s\n", l.now().Format("2006/01/02 15:04:05.000000"), level.String(), msg)

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.w == nil {
		return
	}
	if l.file != nil && l.written+int64(len(line)) > l.maxSize {
		// 轮转失败不该让"写日志"把引擎带崩：忽略错误，退回继续往当前文件写。
		// 目录被写满时引擎还有机会通过别的路径把这个故障报出去，
		// 而在这里 panic 或返回错误只会让一个次要功能拖垮主要功能。
		_ = l.rotateLocked()
	}
	n, _ := l.w.Write([]byte(line))
	l.written += int64(n)
}

// rotateLocked 把当前 LOG 滚成 LOG.old，并重开一份空的 LOG。调用方必须持有 l.mu。
func (l *Logger) rotateLocked() error {
	if l.file == nil {
		return nil
	}
	if err := l.file.Close(); err != nil {
		// 关不掉也继续：下面重开句柄之后再报错意义不大。
		_ = err
	}
	l.file, l.w = nil, nil

	path := filepath.Join(l.dir, l.name)
	old := path + oldSuffix
	// Windows 上 rename 到已存在的文件会失败，所以先删旧的。
	// "先删再改名"中间有一个窗口期，但日志的历史文件丢一份是可以接受的代价。
	if err := os.Remove(old); err != nil && !os.IsNotExist(err) {
		_ = err
	}
	if err := os.Rename(path, old); err != nil {
		// 改不动（文件被占用之类）：直接把新日志写回原文件名，
		// 于是旧内容被截断 —— 依然比"日志系统把引擎拖垮"好。
		_ = os.Remove(path)
	}
	return l.openLocked()
}

// Rotate 强制轮转一次。返回错误说明日志文件已经不可写。
func (l *Logger) Rotate() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	return l.rotateLocked()
}

// Close 关闭底层文件。写向自定义 writer 时是空操作（writer 的归属权在调用方）。
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file, l.w = nil, nil
	if err != nil {
		return fmt.Errorf("kvdb/logger: close %s: %w", filepath.Join(l.dir, l.name), err)
	}
	return nil
}
