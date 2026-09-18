package kvdb

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// 崩溃恢复测试通过"另一个进程写完之后直接退出、不做任何清理"来模拟 kill -9。
//
// 用子进程而不是同进程内放弃 Close，是因为目录锁挂在内核持有的文件句柄上：
// 只有进程真的死了，锁才会被释放——这正是我们要验证的那条路径。
const (
	crashChildEnv = "KVDB_TEST_CRASH_CHILD"
	crashDirEnv   = "KVDB_TEST_CRASH_DIR"
	crashNEnv     = "KVDB_TEST_CRASH_N"
	crashTornEnv  = "KVDB_TEST_CRASH_TORN"
)

// TestCrashWriterChild 是被父进程拉起的"崩溃进程"，正常测试时自动跳过。
func TestCrashWriterChild(t *testing.T) {
	if os.Getenv(crashChildEnv) != "1" {
		t.Skip("仅作为崩溃恢复测试的子进程运行")
	}
	dir := os.Getenv(crashDirEnv)
	n, err := strconv.Atoi(os.Getenv(crashNEnv))
	if err != nil || n <= 0 {
		fmt.Fprintln(os.Stderr, "kvdb-test-child: invalid record count")
		os.Exit(2)
	}

	// 默认配置：SyncWrites = true，每条写都要等 fsync 返回，因此崩溃不该丢任何已确认的写。
	db, err := Open(DefaultOptions(dir))
	if err != nil {
		fmt.Fprintf(os.Stderr, "kvdb-test-child: open: %v\n", err)
		os.Exit(2)
	}
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(childKey(i)), []byte(childValue(i))); err != nil {
			fmt.Fprintf(os.Stderr, "kvdb-test-child: put #%d: %v\n", i, err)
			os.Exit(2)
		}
	}

	if os.Getenv(crashTornEnv) == "1" {
		// 直接在日志尾部追加几个字节，模拟"写到一半被打断"。
		logs, err := filepathGlobLogs(dir)
		if err != nil || len(logs) == 0 {
			fmt.Fprintf(os.Stderr, "kvdb-test-child: no log to tear: %v\n", err)
			os.Exit(2)
		}
		last := logs[len(logs)-1]
		f, err := os.OpenFile(last, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "kvdb-test-child: open %s: %v\n", last, err)
			os.Exit(2)
		}
		if _, err := f.Write([]byte{0xde, 0xad, 0xbe}); err != nil {
			fmt.Fprintf(os.Stderr, "kvdb-test-child: tear %s: %v\n", last, err)
			os.Exit(2)
		}
		f.Close()
	}

	fmt.Printf("kvdb-test-child: wrote %d records, exiting without cleanup\n", n)
	// 不做 Close：不刷缓冲区、不关文件、不释放锁，等价于被 kill -9。
	os.Exit(1)
}

// childKey / childValue 保证父子进程用完全一样的键值。
func childKey(i int) string   { return fmt.Sprintf("key%06d", i) }
func childValue(i int) string { return fmt.Sprintf("value-%06d", i) }

// filepathGlobLogs 返回目录里按文件名升序的日志文件路径。
func filepathGlobLogs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".log") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out, nil
}

// runCrashChild 拉起子进程并确认它确实是"异常退出"。
func runCrashChild(t *testing.T, dir string, n int, torn bool) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashWriterChild", "-test.v")
	cmd.Env = append(os.Environ(),
		crashChildEnv+"=1",
		crashDirEnv+"="+dir,
		crashNEnv+"="+strconv.Itoa(n),
		crashTornEnv+"="+boolEnv(torn),
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("子进程应当以非 0 状态退出，实际成功退出\n输出：%s", out)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("子进程执行失败：%v\n输出：%s", err, out)
	}
	if !strings.Contains(string(out), "exiting without cleanup") {
		t.Fatalf("子进程没有跑完写入就退出了：%v\n输出：%s", err, out)
	}
}

func boolEnv(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// 崩溃恢复的正面用例：每条写都 fsync 过，进程被强杀后数据一条都不能少。
func TestCrashRecoveryKeepsSyncedWrites(t *testing.T) {
	const n = 300
	dir := t.TempDir()
	runCrashChild(t, dir, n, false)

	// 崩溃进程不会删掉 LOCK 文件，但它持有的锁已经被内核收回。
	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("崩溃后重新打开失败（锁没有随进程退出释放？）: %v", err)
	}
	defer db.Close()

	rep := db.RecoveryReport()
	if len(rep.TornLogs) != 0 {
		t.Errorf("干净崩溃不该报告日志损坏: %v", rep.TornLogs)
	}
	if rep.ReplayedRecords != n {
		t.Errorf("重放记录数 = %d, want %d", rep.ReplayedRecords, n)
	}
	for i := 0; i < n; i++ {
		v, err := db.Get([]byte(childKey(i)))
		if err != nil {
			t.Fatalf("崩溃恢复后 Get(%s) failed: %v", childKey(i), err)
		}
		if string(v) != childValue(i) {
			t.Fatalf("崩溃恢复后 Get(%s) = %q, want %q", childKey(i), v, childValue(i))
		}
	}

	// 恢复之后必须能继续正常写入与读取。
	if err := db.Put([]byte("after-crash"), []byte("ok")); err != nil {
		t.Fatalf("恢复后写入失败: %v", err)
	}
	if v, err := db.Get([]byte("after-crash")); err != nil || string(v) != "ok" {
		t.Fatalf("恢复后 Get(after-crash) = (%q, %v)", v, err)
	}
}

// 日志尾部被写坏（崩溃时写了一半）时：损坏点之前的数据必须全部保住，
// 损坏部分被丢弃且如实汇报，而不是让整个数据库打不开。
func TestCrashRecoveryToleratesTornLogTail(t *testing.T) {
	const n = 200
	dir := t.TempDir()
	runCrashChild(t, dir, n, true)

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("日志尾部损坏不应导致打开失败: %v", err)
	}
	defer db.Close()

	rep := db.RecoveryReport()
	if len(rep.TornLogs) == 0 {
		t.Fatal("尾部损坏没有被汇报")
	}
	if rep.ReplayedRecords != n {
		t.Errorf("重放记录数 = %d, want %d（损坏点之前的记录都要保住）", rep.ReplayedRecords, n)
	}
	for i := 0; i < n; i++ {
		v, err := db.Get([]byte(childKey(i)))
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", childKey(i), err)
		}
		if string(v) != childValue(i) {
			t.Fatalf("Get(%s) = %q, want %q", childKey(i), v, childValue(i))
		}
	}
}
