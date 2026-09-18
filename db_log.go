package kvdb

import "fmt"

// nopLogger 是"显式禁用文件日志"时的丢弃型实现。
//
// 它让 eventLog 永远非 nil：所有事件路径都不用再判空。
// （用户显式传了负的 LogMaxSize 且没给 Logger，就是在说"我不要事件"。）
type nopLogger struct{}

func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Warnf(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

// setupEventLog 按配置决定事件日志的去向，Open 时调用一次。
//
// 优先级：用户提供的 Logger > 目录下的 LOG 文件 > 丢弃。
// 文件日志创建失败不算致命——日志只是运维辅助，引擎的可用性不该被它绑架；
// 此时退回丢弃实现，并把原因如实交给调用方记录（如果它自己也有地方记的话）。
func setupEventLog(opts *Options) (Logger, func() error, error) {
	if opts.Logger != nil {
		return opts.Logger, func() error { return nil }, nil
	}
	if opts.LogMaxSize < 0 {
		return nopLogger{}, func() error { return nil }, nil
	}
	l, closeFn, err := NewFileLogger(opts.Dir, opts.LogMaxSize)
	if err != nil {
		return nopLogger{}, func() error { return nil }, fmt.Errorf("kvdb: create LOG file: %w", err)
	}
	return l, closeFn, nil
}

// logInfof / logWarnf / logErrorf 是引擎内部记事件的三件套。
//
// 事件只描述"发生了什么"，不参与控制流；写日志永远不返回错误，
// 也不会因为日志写失败而停库。
func (db *DB) logInfof(format string, args ...any)  { db.eventLog.Infof(format, args...) }
func (db *DB) logWarnf(format string, args ...any)  { db.eventLog.Warnf(format, args...) }
func (db *DB) logErrorf(format string, args ...any) { db.eventLog.Errorf(format, args...) }

// humanBytes 把字节数格式成带单位的可读形式，用于事件日志与压测报告。
func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit && exp < 3; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGT"[exp])
}
