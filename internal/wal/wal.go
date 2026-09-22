package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// LogSuffix 是日志文件的扩展名。
const LogSuffix = ".log"

// LogName 返回编号为 num 的日志文件名，编号补零到 6 位以便按文件名排序。
func LogName(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", num, LogSuffix))
}

// Log 是一个按编号命名的预写日志文件，负责"顺序追加 + fsync"。
//
// 它的写路径没有锁之外的额外开销：Append 只往 bufio 缓冲里塞字节，
// Sync 才真正把数据推给内核并 fsync，因此写延迟 ≈ 一次 fsync。
type Log struct {
	dir  string
	num  uint64
	path string

	file *os.File
	buf  *bufio.Writer
	w    *Writer

	base int64 // 打开时文件已有的长度，用于计算逻辑文件大小

	mu     sync.Mutex
	closed bool
}

// Create 创建（或清空）编号为 num 的日志。
func Create(dir string, num uint64) (*Log, error) {
	path := LogName(dir, num)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("github.com/xiatianliang1024gm/kvdb/wal: create %s: %w", path, err)
	}
	buf := bufio.NewWriterSize(f, 2*BlockSize)
	return &Log{
		dir:  dir,
		num:  num,
		path: path,
		file: f,
		buf:  buf,
		w:    NewWriter(buf),
	}, nil
}

// Num 返回日志编号。
func (l *Log) Num() uint64 { return l.num }

// Path 返回日志文件的完整路径。
func (l *Log) Path() string { return l.path }

// Size 返回逻辑文件大小（含尚未刷出缓冲区的部分）。
func (l *Log) Size() int64 { return l.base + l.w.Written() }

// Append 追加一条记录。它只写缓冲区，不保证落盘；确定性由 Sync 提供。
func (l *Log) Append(record []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("github.com/xiatianliang1024gm/kvdb/wal: append to a closed log")
	}
	return l.w.addRecord(record)
}

// Flush 把缓冲区推给内核，但不等 fsync。
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("github.com/xiatianliang1024gm/kvdb/wal: flush a closed log")
	}
	return l.buf.Flush()
}

// Sync 落盘：先刷出缓冲区，再 fsync。返回时数据已经持久化。
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("github.com/xiatianliang1024gm/kvdb/wal: sync a closed log")
	}
	if err := l.buf.Flush(); err != nil {
		return fmt.Errorf("github.com/xiatianliang1024gm/kvdb/wal: flush %s: %w", l.path, err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("github.com/xiatianliang1024gm/kvdb/wal: sync %s: %w", l.path, err)
	}
	return nil
}

// Close 刷出缓冲区并关闭文件句柄，但不删除文件。
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	err := l.buf.Flush()
	if cerr := l.file.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("github.com/xiatianliang1024gm/kvdb/wal: close %s: %w", l.path, err)
	}
	return nil
}

// Remove 删除日志文件。
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	if err := os.Remove(l.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("github.com/xiatianliang1024gm/kvdb/wal: remove %s: %w", l.path, err)
	}
	return nil
}

// ReplayResult 汇报一次重放的结果。
type ReplayResult struct {
	// Records 是成功重放的记录数。
	Records int
	// GoodBytes 是完好部分的字节数；文件从 GoodBytes 起的内容已被丢弃。
	GoodBytes int64
	// Corrupt 非 nil 时表示尾部有一段被丢弃的数据，通常是崩溃时写了一半的记录。
	//
	// 它是"可容忍损坏"的标记，Replay 的返回 error 为 nil；只有 IO 层面的
	// 失败才会作为 error 返回。
	Corrupt error
}

// Replay 按记录边界读回编号为 num 的日志，把每条完整记录交给 fn。
//
// fn 返回错误时立即停止并把该错误抛出（用于 WriteBatch 解析失败等不可恢复的情况）。
// 文件尾部的损坏不会导致失败：它被记为 ReplayResult.Corrupt 后正常返回，
// 因为崩溃时写了一半的记录本来就该丢弃。
func Replay(dir string, num uint64, fn func(record []byte) error) (ReplayResult, error) {
	var res ReplayResult
	path := LogName(dir, num)
	f, err := os.Open(path)
	if err != nil {
		return res, fmt.Errorf("github.com/xiatianliang1024gm/kvdb/wal: open %s: %w", path, err)
	}
	defer f.Close()

	r := NewReader(f)
	for {
		record, err := r.ReadRecord()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return res, nil
			}
			var corrupt *ErrCorruptRecord
			if errors.As(err, &corrupt) {
				// 损坏点之前的数据仍然有效，把损坏标记回传后正常结束。
				res.Corrupt = err
				return res, nil
			}
			return res, fmt.Errorf("github.com/xiatianliang1024gm/kvdb/wal: read %s: %w", path, err)
		}
		if err := fn(record); err != nil {
			return res, err
		}
		res.Records++
		res.GoodBytes += int64(len(record))
	}
}

// ListLogs 扫描目录并返回全部日志编号，按升序排列。
func ListLogs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("github.com/xiatianliang1024gm/kvdb/wal: read dir %s: %w", dir, err)
	}
	var nums []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), LogSuffix) {
			continue
		}
		base := strings.TrimSuffix(e.Name(), LogSuffix)
		n, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue // 不符合命名规则的文件不属于日志
		}
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

// RemoveLog 删除指定编号的日志文件，文件不存在时不算错误。
func RemoveLog(dir string, num uint64) error {
	path := LogName(dir, num)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("github.com/xiatianliang1024gm/kvdb/wal: remove %s: %w", path, err)
	}
	return nil
}
