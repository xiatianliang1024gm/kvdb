//go:build windows

package kvdb

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// 目录锁在 Windows 上用 LockFileEx 实现。
//
// 这里刻意不用"创建即独占"的 O_CREATE|O_EXCL：那种做法在进程被 kill -9
// 之后会留下一个僵死的锁文件，导致数据目录再也打不开。LockFileEx 的锁由内核
// 持有，进程一死（包括被强杀）就自动释放，这才是崩溃恢复能工作的前提。
var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

const (
	lockfileExclusiveLock   = 0x0000_0002
	lockfileFailImmediately = 0x0000_0001
)

// lockDirFile 尝试对 f 加独占锁，抢不到就立刻返回错误而不是阻塞。
func lockDirFile(f *os.File) error {
	var ol syscall.Overlapped
	ret, _, err := procLockFileEx.Call(
		f.Fd(),
		uintptr(lockfileExclusiveLock|lockfileFailImmediately),
		0,
		1, 0,
		uintptr(unsafe.Pointer(&ol)),
	)
	if ret == 0 {
		return fmt.Errorf("lock %s: %w", f.Name(), err)
	}
	return nil
}

// unlockDirFile 释放独占锁。
func unlockDirFile(f *os.File) error {
	var ol syscall.Overlapped
	ret, _, err := procUnlockFileEx.Call(f.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&ol)))
	if ret == 0 {
		return fmt.Errorf("unlock %s: %w", f.Name(), err)
	}
	return nil
}
