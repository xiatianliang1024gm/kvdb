//go:build !windows

package kvdb

import (
	"fmt"
	"os"
	"syscall"
)

// 目录锁在类 Unix 系统上用 flock 实现。
//
// flock 的锁挂在"打开的文件描述"上，由内核在进程退出（含被 SIGKILL）时释放，
// 因此不会留下需要人工清理的僵死锁文件。
func lockDirFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("lock %s: %w", f.Name(), err)
	}
	return nil
}

// unlockDirFile 释放独占锁。
func unlockDirFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("unlock %s: %w", f.Name(), err)
	}
	return nil
}
