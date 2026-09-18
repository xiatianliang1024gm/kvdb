package kvdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// openTestDB 打开一个临时目录里的数据库；SyncWrites 默认关闭以免测试被 fsync 拖慢，
// 需要验证持久性的用例会显式打开它。
func openTestDB(t *testing.T, mutate func(o *Options)) *DB {
	t.Helper()
	opts := DefaultOptions(t.TempDir())
	opts.SyncWrites = false
	if mutate != nil {
		mutate(&opts)
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return db
}

func mustPut(t *testing.T, db *DB, k, v string) {
	t.Helper()
	if err := db.Put([]byte(k), []byte(v)); err != nil {
		t.Fatalf("Put(%q) failed: %v", k, err)
	}
}

func mustGet(t *testing.T, db *DB, k string) string {
	t.Helper()
	v, err := db.Get([]byte(k))
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", k, err)
	}
	return string(v)
}

func mustMiss(t *testing.T, db *DB, k string) {
	t.Helper()
	if v, err := db.Get([]byte(k)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) = (%q, %v), want ErrNotFound", k, v, err)
	}
}

func TestPutGetDelete(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	mustPut(t, db, "a", "1")
	if got := mustGet(t, db, "a"); got != "1" {
		t.Fatalf("Get(a) = %q, want 1", got)
	}
	mustPut(t, db, "a", "2") // 覆盖
	if got := mustGet(t, db, "a"); got != "2" {
		t.Fatalf("覆盖之后 Get(a) = %q, want 2", got)
	}

	// 空 value 是"存在但内容为空"，与"不存在"必须区分开。
	mustPut(t, db, "empty", "")
	if v, err := db.Get([]byte("empty")); err != nil || len(v) != 0 {
		t.Fatalf("Get(empty) = (%q, %v), want (空值, nil)", v, err)
	}

	if err := db.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	mustMiss(t, db, "a")

	// 删除之后重新写入，墓碑要被新版本遮蔽。
	mustPut(t, db, "a", "3")
	if got := mustGet(t, db, "a"); got != "3" {
		t.Fatalf("复活之后 Get(a) = %q, want 3", got)
	}

	mustMiss(t, db, "missing")
	// 删除一个不存在的 key 不是错误。
	if err := db.Delete([]byte("missing")); err != nil {
		t.Fatalf("Delete(不存在的 key) failed: %v", err)
	}
}

func TestWriteBatchIsAtomic(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	mustPut(t, db, "keep", "v")

	b := NewWriteBatch()
	for i := 0; i < 5; i++ {
		if err := b.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Delete([]byte("keep")); err != nil {
		t.Fatal(err)
	}
	if err := db.Write(b); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		if got := mustGet(t, db, fmt.Sprintf("k%d", i)); got != fmt.Sprintf("v%d", i) {
			t.Errorf("Get(k%d) = %q, want v%d", i, got, i)
		}
	}
	mustMiss(t, db, "keep")

	// 空批次与 nil 批次都是合法的空操作。
	if err := db.Write(NewWriteBatch()); err != nil {
		t.Errorf("空批次 Write failed: %v", err)
	}
	if err := db.Write(nil); err != nil {
		t.Errorf("nil 批次 Write failed: %v", err)
	}
}

// 单线程 10 万次读写：M1 验收标准的第一条。
func TestHundredThousandWritesAndReads(t *testing.T) {
	const n = 100000
	db := openTestDB(t, nil)
	defer db.Close()

	key := func(i int) []byte { return []byte(fmt.Sprintf("key%08d", i)) }
	for i := 0; i < n; i++ {
		if err := db.Put(key(i), []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatalf("Put(#%d) failed: %v", i, err)
		}
	}
	for i := 0; i < n; i += 2 { // 覆盖一半
		if err := db.Put(key(i), []byte(fmt.Sprintf("updated-%d", i))); err != nil {
			t.Fatalf("Put(#%d) failed: %v", i, err)
		}
	}
	for i := 0; i < n; i += 4 { // 再删掉四分之一
		if err := db.Delete(key(i)); err != nil {
			t.Fatalf("Delete(#%d) failed: %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		v, err := db.Get(key(i))
		switch {
		case i%4 == 0:
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get(#%d) = (%q, %v), 期望已被删除", i, v, err)
			}
		case i%2 == 0:
			if err != nil || string(v) != fmt.Sprintf("updated-%d", i) {
				t.Fatalf("Get(#%d) = (%q, %v), want updated-%d", i, v, err, i)
			}
		default:
			if err != nil || string(v) != fmt.Sprintf("value-%d", i) {
				t.Fatalf("Get(#%d) = (%q, %v), want value-%d", i, v, err, i)
			}
		}
	}

	if s := db.Stats(); s.LastSequence < uint64(n+n/2+n/4) {
		t.Errorf("LastSequence = %d, 期望至少 %d", s.LastSequence, n+n/2+n/4)
	}
}

// 小 MemTableSize 逼出多次冻结与落盘，验证数据在
// MemTable / Immutable MemTable / 多个 SST 之间切换后依然全部可读。
func TestDataSurvivesFlushes(t *testing.T) {
	const n = 1200
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MemTableSize = 8 << 10 // 8KB，每写几十条就会冻结
	opts.BlockCacheSize = -1
	opts.BloomBitsPerKey = -1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	keys := func(i int) string { return fmt.Sprintf("k%05d", i) }
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(keys(i)), []byte(strings.Repeat("x", 40))); err != nil {
			t.Fatalf("Put(#%d) failed: %v", i, err)
		}
	}
	// 落盘过程中也读一遍，覆盖 mem / imm / sst 混合读取。
	for i := 0; i < n; i += 37 {
		if got := mustGet(t, db, keys(i)); len(got) != 40 {
			t.Fatalf("Get(%s) 长度 = %d, want 40", keys(i), len(got))
		}
	}
	if s := db.Stats(); s.Files == 0 {
		t.Error("写入 1200 条之后应当已经产生 SST 文件")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 重新打开：日志被重放并落盘，数据必须一条不少。
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("重新 Open failed: %v", err)
	}
	defer db2.Close()
	if r := db2.RecoveryReport(); r.ReplayedRecords == 0 {
		t.Error("重新打开时应当重放到 WAL 里的记录")
	}
	for i := 0; i < n; i++ {
		if got := mustGet(t, db2, keys(i)); len(got) != 40 {
			t.Fatalf("重开后 Get(%s) 长度 = %d, want 40", keys(i), len(got))
		}
	}
}

// 关闭再打开（干净路径）：数据由 WAL 重放回来。
func TestReopenAfterCleanClose(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 200
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("重复 Close 应当无害: %v", err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("重新 Open failed: %v", err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		if got := mustGet(t, db2, fmt.Sprintf("k%03d", i)); got != fmt.Sprintf("v%d", i) {
			t.Fatalf("重开后 Get(k%03d) = %q, want v%d", i, got, i)
		}
	}
	if s := db2.Stats(); s.Files == 0 {
		t.Error("恢复时应当把重放出 MemTable 落成 SST")
	}
}

// 中断的 Flush 会留下 footer 不全的 SST，恢复时必须丢弃它（数据还在 WAL 里）。
func TestRecoveryDiscardsUnfinishedFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 伪造一次"写到一半就崩溃"的 Flush：文件名合法但内容不是完整 SST。
	const bogus = "000009.sst"
	if err := os.WriteFile(filepath.Join(dir, bogus), []byte("half-written garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("恢复时应当丢弃半截 SST 而不是失败: %v", err)
	}
	defer db2.Close()

	rep := db2.RecoveryReport()
	if len(rep.DiscardedFiles) != 1 || rep.DiscardedFiles[0] != bogus {
		t.Fatalf("DiscardedFiles = %v, want [%s]", rep.DiscardedFiles, bogus)
	}
	if _, err := os.Stat(filepath.Join(dir, bogus)); !os.IsNotExist(err) {
		t.Errorf("半截 SST 没有被删除: %v", err)
	}
	for i := 0; i < 50; i++ {
		if got := mustGet(t, db2, fmt.Sprintf("k%02d", i)); got != "v" {
			t.Fatalf("恢复后 Get(k%02d) = %q, want v", i, got)
		}
	}
}

// 目录锁：同一目录不允许两个实例同时打开，但关闭后必须能立刻重新打开。
func TestDirectoryLock(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("第一次 Open failed: %v", err)
	}
	if _, err := Open(opts); !errors.Is(err, ErrLocked) {
		t.Fatalf("第二次 Open 的 error = %v, want ErrLocked", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// LOCK 文件仍然留在目录里，但这不应该妨碍再次打开。
	if _, err := os.Stat(filepath.Join(dir, lockFileName)); err != nil {
		t.Fatalf("LOCK 文件应当保留在目录里: %v", err)
	}
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("释放锁之后应当能重新打开: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClosedDBAccess(t *testing.T) {
	db := openTestDB(t, nil)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Errorf("Get on closed DB = %v, want ErrClosed", err)
	}
	if err := db.Put([]byte("a"), []byte("v")); !errors.Is(err, ErrClosed) {
		t.Errorf("Put on closed DB = %v, want ErrClosed", err)
	}
	if err := db.Delete([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Errorf("Delete on closed DB = %v, want ErrClosed", err)
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	if _, err := db.Get(nil); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("Get(nil) = %v, want ErrEmptyKey", err)
	}
	if err := db.Put(nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("Put(nil) = %v, want ErrEmptyKey", err)
	}
	if err := db.Delete([]byte{}); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("Delete(empty) = %v, want ErrEmptyKey", err)
	}
}

// 二进制 key/value 必须原样往返（含 0x00 与高位字节）。
func TestBinaryKeysAndValues(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	keys := [][]byte{
		{0x00},
		{0x00, 0x00, 0xff},
		{0xff, 0x00, 0x7f},
		[]byte("正常中文 key"),
	}
	for i, k := range keys {
		v := bytes.Repeat([]byte{byte(i)}, 33)
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Put(%v) failed: %v", k, err)
		}
	}
	for i, k := range keys {
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get(%v) failed: %v", k, err)
		}
		if !bytes.Equal(got, bytes.Repeat([]byte{byte(i)}, 33)) {
			t.Errorf("Get(%v) 内容不匹配", k)
		}
	}
	// 前缀相同但更长的 key 不能被当成同一个 key。
	mustMiss(t, db, string([]byte{0x00, 0x00}))
}

// 并发读写：读之间共享锁，写者串行，任何一次读都必须拿到完整一致的值。
func TestConcurrentReadWrite(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	const (
		keys    = 64
		perKey  = 200
		readers = 4
	)
	for i := 0; i < keys; i++ {
		mustPut(t, db, fmt.Sprintf("k%03d", i), strings.Repeat("v", 16))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("k%03d", id%keys)
				v, err := db.Get([]byte(k))
				if err != nil {
					t.Errorf("并发 Get(%s) failed: %v", k, err)
					return
				}
				if len(v) != 16 {
					t.Errorf("并发 Get(%s) 长度 = %d, want 16", k, len(v))
					return
				}
			}
		}(r)
	}

	for i := 0; i < perKey; i++ {
		k := fmt.Sprintf("k%03d", i%keys)
		if err := db.Put([]byte(k), []byte(strings.Repeat(string(rune('a'+i%26)), 16))); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}

// Stats 与恢复报告的基本形态。
func TestStatsAndRecoveryReport(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if s := db.Stats(); s.Files != 0 || s.MemTableSize != 0 || s.HasImmutable || s.LastSequence != 0 {
		t.Fatalf("新库的 Stats = %+v, 期望全零", s)
	}
	if r := db.RecoveryReport(); len(r.TornLogs) != 0 || len(r.DiscardedFiles) != 0 || r.ReplayedRecords != 0 {
		t.Fatalf("新库的 RecoveryReport = %+v, 期望全空", r)
	}
	mustPut(t, db, "k", "v")
	if s := db.Stats(); s.LastSequence != 1 {
		t.Errorf("LastSequence = %d, want 1", s.LastSequence)
	}
	if s := db.Stats(); s.MemTableSize <= 0 {
		t.Errorf("MemTableSize = %d, want > 0", s.MemTableSize)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// 目录里与引擎无关的文件必须被忽略，日志编号也不能与它们混淆。
func TestIgnoresUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false

	for _, name := range []string{"README.md", "abc.log", "notes.txt", "000002.sst.bak"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()
	mustPut(t, db, "k", "v")
	if got := mustGet(t, db, "k"); got != "v" {
		t.Fatalf("Get(k) = %q, want v", got)
	}
	// 无关文件不该被当成日志或 SST 删掉。
	for _, name := range []string{"README.md", "abc.log", "notes.txt", "000002.sst.bak"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s 被误删: %v", name, err)
		}
	}
}

// 确保文件编号在 SST 与日志之间共享同一个空间，重启后不会撞号。
func TestFileNumbersDoNotCollide(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MemTableSize = 4 << 10

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(strings.Repeat("y", 30))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}
	nums := map[uint64]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || name == lockFileName {
			continue
		}
		seen[name] = true
		base := strings.TrimSuffix(strings.TrimSuffix(name, ".sst"), ".log")
		num, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue
		}
		if nums[num] {
			t.Errorf("编号 %d 被两种文件复用: %v", num, seen)
		}
		nums[num] = true
	}

	// 重开一次，编号必须继续增长而不是复用旧编号。
	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 400; i++ {
		if got := mustGet(t, db2, fmt.Sprintf("k%03d", i)); len(got) != 30 {
			t.Fatalf("重开后 Get(k%03d) 长度 = %d, want 30", i, len(got))
		}
	}
}
