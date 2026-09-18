package kvdb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestCheckpointCopyIsOpenableAndConsistent 是 Checkpoint 的主验收用例：
// 副本必须能独立打开、数据与源库一致，且源库 afterwards 照常工作。
func TestCheckpointCopyIsOpenableAndConsistent(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		// 小 MemTable 逼出多次 Flush，小目标文件逼出 Compaction，
		// 让副本里既有 SST 也有 WAL 尾巴。
		o.MemTableSize = 64 << 10
		o.LevelBaseSize = 256 << 10
	})
	defer db.Close()

	const n = 2000
	for i := 0; i < n; i++ {
		mustPut(t, db, ckKey(i), ckVal(i))
	}
	// 一部分删除：副本必须同样"看不到"它们。
	for i := 0; i < n/10; i++ {
		mustDelete(t, db, ckKey(i))
	}
	// 覆盖写：副本读到的必须是新值。
	for i := n / 2; i < n/2+50; i++ {
		mustPut(t, db, ckKey(i), ckVal(i*1000))
	}

	dst := filepath.Join(t.TempDir(), "copy")
	if err := db.Checkpoint(dst); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// ── 副本可打开且数据一致 ──
	copied, err := Open(DefaultOptions(dst))
	if err != nil {
		t.Fatalf("open checkpoint: %v", err)
	}
	defer copied.Close()

	for i := n / 10; i < n/2; i++ {
		if got := mustGet(t, copied, ckKey(i)); got != ckVal(i) {
			t.Fatalf("copy: Get(%s) = %q, want %q", ckKey(i), got, ckVal(i))
		}
	}
	for i := n / 2; i < n/2+50; i++ {
		if got := mustGet(t, copied, ckKey(i)); got != ckVal(i*1000) {
			t.Fatalf("copy: 覆盖写没有生效 Get(%s) = %q", ckKey(i), got)
		}
	}
	for i := 0; i < n/10; i++ {
		mustMiss(t, copied, ckKey(i))
	}

	// ── 源库不受影响，可以继续写 ──
	mustPut(t, db, "after-checkpoint", "yes")
	if got := mustGet(t, db, "after-checkpoint"); got != "yes" {
		t.Fatalf("源库被 Checkpoint 破坏了")
	}
}

func ckKey(i int) string { return "key-" + pad6(i) }
func ckVal(i int) string {
	return "value-" + pad6(i) + "-" + string(rune('a'+i%26)) + string(rune('A'+i%26))
}

func pad6(i int) string {
	const digits = "0123456789"
	b := []byte("000000")
	for p := len(b) - 1; p >= 0 && i > 0; p-- {
		b[p] = digits[i%10]
		i /= 10
	}
	return string(b)
}

func mustDelete(t *testing.T, db *DB, k string) {
	t.Helper()
	if err := db.Delete([]byte(k)); err != nil {
		t.Fatalf("Delete(%q) failed: %v", k, err)
	}
}

// TestCheckpointRejectsBadTargets 验证两个防御性检查：目标目录非空、目标即源目录。
func TestCheckpointRejectsBadTargets(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()
	mustPut(t, db, "a", "1")

	// 目标目录非空。
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(dst, "some-file"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(dst); !errors.Is(err, ErrCheckpointTargetNotEmpty) {
		t.Fatalf("Checkpoint(非空目录) = %v, want ErrCheckpointTargetNotEmpty", err)
	}

	// 目标即源目录。
	if err := db.Checkpoint(db.opts.Dir); err == nil {
		t.Fatal("Checkpoint(源目录) 应当被拒绝")
	}

	// 空目录是允许的：先建出来再往里做。
	empty := t.TempDir()
	sub := filepath.Join(empty, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(sub); err != nil {
		t.Fatalf("Checkpoint(空的已存在目录): %v", err)
	}
}

// TestCheckpointCatchesUnflushedTail 验证 WAL 尾巴真的被带走了：
// 写完立刻做 Checkpoint（数据只存在于 MemTable/WAL），副本必须同样读得到。
func TestCheckpointCatchesUnflushedTail(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 8 << 20 // 足够大，保证这些写不会触发 Flush
	})
	defer db.Close()

	for i := 0; i < 100; i++ {
		mustPut(t, db, ckKey(i), ckVal(i))
	}
	dst := filepath.Join(t.TempDir(), "copy")
	if err := db.Checkpoint(dst); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	copied, err := Open(DefaultOptions(dst))
	if err != nil {
		t.Fatalf("open checkpoint: %v", err)
	}
	defer copied.Close()
	for i := 0; i < 100; i++ {
		if got := mustGet(t, copied, ckKey(i)); got != ckVal(i) {
			t.Fatalf("WAL 尾巴丢了：Get(%s) = %q", ckKey(i), got)
		}
	}
}

// TestCheckpointSurvivesSourceMutation 验证"源库在 Checkpoint 之后继续写、继续
// Compaction"不会破坏副本 —— 硬链接路径下，源目录删除旧文件后副本仍要完整可读。
func TestCheckpointSurvivesSourceMutation(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.MemTableSize = 64 << 10
		o.LevelBaseSize = 128 << 10 // 极小：逼出多轮 Compaction
	})
	defer db.Close()

	for i := 0; i < 3000; i++ {
		mustPut(t, db, ckKey(i), ckVal(i))
	}
	dst := filepath.Join(t.TempDir(), "copy")
	if err := db.Checkpoint(dst); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// 源库继续写、逼 Compaction 推进、删掉一批 Checkpoint 时的旧文件。
	for i := 3000; i < 6000; i++ {
		mustPut(t, db, ckKey(i), ckVal(i))
	}
	db.Close()

	copied, err := Open(DefaultOptions(dst))
	if err != nil {
		t.Fatalf("open checkpoint after source mutation: %v", err)
	}
	defer copied.Close()
	for i := 0; i < 3000; i += 137 {
		if got := mustGet(t, copied, ckKey(i)); got != ckVal(i) {
			t.Fatalf("副本数据变了：Get(%s) = %q", ckKey(i), got)
		}
	}
	for i := 3000; i < 6000; i += 137 {
		mustMiss(t, copied, ckKey(i))
	}
}

// TestCheckpointOnClosedDB 顺手覆盖错误分支。
func TestCheckpointOnClosedDB(t *testing.T) {
	db := openTestDB(t, nil)
	mustPut(t, db, "a", "1")
	dst := filepath.Join(t.TempDir(), "copy")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(dst); !errors.Is(err, ErrClosed) {
		t.Fatalf("Checkpoint(closed) = %v, want ErrClosed", err)
	}
}
