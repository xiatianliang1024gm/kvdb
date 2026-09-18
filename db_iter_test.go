package kvdb

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// waitForFiles 等到磁盘上出现至少 n 个 SST 文件，用于把"数据确实落在 SST 里"
// 这一类断言变得确定（后台 Flush 是异步的）。
func waitForFiles(t *testing.T, db *DB, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if db.Stats().Files >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 SST 文件超时：只有 %d 个，期望至少 %d 个", db.Stats().Files, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// collect 把迭代器里的 key=value 全部读出来。
func collect(t *testing.T, it Iterator) []string {
	t.Helper()
	defer it.Close()
	var got []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, fmt.Sprintf("%s=%s", it.Key(), it.Value()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("迭代出错: %v", err)
	}
	return got
}

// 迭代器必须把 MemTable / Immutable / 多个 SST 归并成一条有序流。
func TestIteratorMergesAllSources(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MemTableSize = 4 << 10 // 很小，逼出多次冻结与落盘

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 400
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitForFiles(t, db, 2)

	// 覆盖一部分、删掉一部分，让"最新版本在 MemTable、旧版本在 SST"的混合状态出现。
	for i := 0; i < n; i += 3 {
		if err := db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("updated%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < n; i += 5 {
		if err := db.Delete([]byte(fmt.Sprintf("k%04d", i))); err != nil {
			t.Fatal(err)
		}
	}

	got := collect(t, db.NewIterator(nil))
	want := 0
	for i := 0; i < n; i++ {
		uk := fmt.Sprintf("k%04d", i)
		switch {
		case i%5 == 0:
			continue // 已删除
		case i%3 == 0:
			want++
			if !contains(got, uk+"=updated"+fmt.Sprint(i)) {
				t.Fatalf("缺少更新后的 %s", uk)
			}
		default:
			want++
			if !contains(got, uk+"="+"v"+fmt.Sprint(i)) {
				t.Fatalf("缺少 %s", uk)
			}
		}
	}
	if len(got) != want {
		t.Fatalf("迭代出 %d 条, want %d", len(got), want)
	}
	// 顺序必须是 user key 升序且无重复。
	for i := 1; i < len(got); i++ {
		if got[i-1][:5] >= got[i][:5] {
			t.Fatalf("输出顺序不是升序: %s 在 %s 之前", got[i-1], got[i])
		}
	}
	if s := db.Stats(); s.Files == 0 {
		t.Error("应当已经有 SST 参与读取")
	}
}

// 快照隔离：快照之后的写入对快照不可见，对实时视图可见。
func TestSnapshotIsolation(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	mustPut(t, db, "a", "old")
	mustPut(t, db, "b", "keep")

	snap := db.GetSnapshot()
	defer snap.Release()

	mustPut(t, db, "a", "new")
	mustPut(t, db, "c", "fresh")
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}

	// 快照看到的还是取快照那一刻的数据。
	if v, err := snap.Get([]byte("a")); err != nil || string(v) != "old" {
		t.Fatalf("快照 Get(a) = (%q, %v), want old", v, err)
	}
	if v, err := snap.Get([]byte("b")); err != nil || string(v) != "keep" {
		t.Fatalf("快照 Get(b) = (%q, %v), want keep", v, err)
	}
	if _, err := snap.Get([]byte("c")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("快照不该看到快照之后写入的 c: %v", err)
	}

	// 实时视图看到全部变化。
	if v := mustGet(t, db, "a"); v != "new" {
		t.Fatalf("实时 Get(a) = %q, want new", v)
	}
	mustMiss(t, db, "b")
	if v := mustGet(t, db, "c"); v != "fresh" {
		t.Fatalf("实时 Get(c) = %q, want fresh", v)
	}

	// 迭代器同样遵守快照。
	if got := keysOf(t, snap.NewIterator(nil)); !equalSlices(got, []string{"a", "b"}) {
		t.Fatalf("快照迭代 = %v, want [a b]", got)
	}
	if got := keysOf(t, db.NewIterator(nil)); !equalSlices(got, []string{"a", "c"}) {
		t.Fatalf("实时迭代 = %v, want [a c]", got)
	}
}

// 快照跨越落盘也依然有效：取快照 → 大量写入逼出 SST → 快照视图不变。
func TestSnapshotSurvivesFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MemTableSize = 4 << 10

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustPut(t, db, "a", "before")
	snap := db.GetSnapshot()
	defer snap.Release()

	for i := 0; i < 600; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	waitForFiles(t, db, 2)

	if v, err := snap.Get([]byte("a")); err != nil || string(v) != "before" {
		t.Fatalf("落盘之后快照 Get(a) = (%q, %v), want before", v, err)
	}
	if _, err := snap.Get([]byte("k0000")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("快照不该看到取快照之后写入的 k0000: %v", err)
	}
	if got := keysOf(t, snap.NewIterator(nil)); !equalSlices(got, []string{"a"}) {
		t.Fatalf("落盘之后快照迭代 = %v, want [a]", got)
	}
}

// SeekToFirst / Seek / Next 与上下界在 DB 层的行为。
func TestIteratorSeekAndBounds(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	for _, k := range []string{"a", "c", "e", "g"} {
		mustPut(t, db, k, strings.ToUpper(k))
	}

	it := db.NewIterator(nil)
	defer it.Close()

	it.Seek([]byte("d"))
	if !it.Valid() || string(it.Key()) != "e" {
		t.Fatalf("Seek(d) = %q, want e", it.Key())
	}
	it.Next()
	if !it.Valid() || string(it.Key()) != "g" {
		t.Fatalf("Seek(d)+Next = %q, want g", it.Key())
	}
	it.Next()
	if it.Valid() {
		t.Fatalf("应当结束，却还有 %q", it.Key())
	}
	it.Seek([]byte("z"))
	if it.Valid() {
		t.Fatal("Seek(z) 应当失效")
	}

	// 闭区间上下界。
	bounded := db.NewIterator(&IteratorOptions{LowerBound: []byte("c"), UpperBound: []byte("e")})
	if got := keysOf(t, bounded); !equalSlices(got, []string{"c", "e"}) {
		t.Fatalf("带边界的迭代 = %v, want [c e]", got)
	}
}

// Get 返回的必须是一份独立的数据：调用方修改它不能影响库里的内容。
func TestGetReturnsIndependentCopy(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MemTableSize = 1 << 10 // 逼着数据落到 SST，走块缓存那条路径

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 200
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("payload-payload")); err != nil {
			t.Fatal(err)
		}
	}
	waitForFiles(t, db, 1)

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%03d", i)
		v, err := db.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != "payload-payload" {
			t.Fatalf("Get(%s) = %q", k, v)
		}
		for j := range v { // 故意写脏这次拿到的那份数据
			v[j] = 'X'
		}
		again, err := db.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if string(again) != "payload-payload" {
			t.Fatalf("修改 Get 的返回值污染了库里的数据: Get(%s) = %q", k, again)
		}
	}
}

// 块缓存打开时统计必须动起来；显式关闭时不能有任何缓存活动。
func TestStatsReportCacheUsage(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	opts.MemTableSize = 1 << 10

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	waitForFiles(t, db, 1)
	for i := 0; i < 200; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("k%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	s := db.Stats()
	if s.CacheMisses == 0 {
		t.Error("读 SST 时应当有块缓存未命中")
	}
	if s.CacheBytes <= 0 || s.CacheItems == 0 {
		t.Errorf("缓存里应当有内容: items=%d bytes=%d", s.CacheItems, s.CacheBytes)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 显式关闭缓存：读路径照常工作，统计保持为零。
	opts.BlockCacheSize = -1
	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 200; i++ {
		if _, err := db2.Get([]byte(fmt.Sprintf("k%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if s := db2.Stats(); s.CacheHits != 0 || s.CacheMisses != 0 || s.CacheItems != 0 {
		t.Errorf("缓存被关闭时统计应当全零: %+v", s)
	}
}

// 关闭之后的库与已释放的快照都要给出明确的错误，而不是空结果或 panic。
func TestIteratorAndSnapshotAfterClose(t *testing.T) {
	db := openTestDB(t, nil)
	snap := db.GetSnapshot()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	it := db.NewIterator(nil)
	if !errors.Is(it.Error(), ErrClosed) {
		t.Fatalf("Close 之后 NewIterator 的 error = %v, want ErrClosed", it.Error())
	}
	if it.Valid() {
		t.Error("报错的迭代器不应有效")
	}
	if err := it.Close(); err != nil {
		t.Errorf("Close 应当无害: %v", err)
	}

	if _, err := snap.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Close 之后快照 Get 的 error = %v, want ErrClosed", err)
	}
	snap.Release()

	// 释放之后再打开一个库，用旧快照必须报"已释放"。
	db2 := openTestDB(t, nil)
	defer db2.Close()
	s2 := db2.GetSnapshot()
	s2.Release()
	if _, err := s2.Get([]byte("a")); !errors.Is(err, ErrSnapshotReleased) {
		t.Fatalf("已释放快照的 Get error = %v, want ErrSnapshotReleased", err)
	}
	it2 := s2.NewIterator(nil)
	if !errors.Is(it2.Error(), ErrSnapshotReleased) {
		t.Fatalf("已释放快照的 NewIterator error = %v, want ErrSnapshotReleased", it2.Error())
	}
}

// 空库上的迭代器必须干净地失效。
func TestIteratorOnEmptyDB(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	it := db.NewIterator(nil)
	if it.Valid() {
		t.Fatal("空库的迭代器不应有效")
	}
	it.SeekToFirst()
	if it.Valid() {
		t.Fatal("SeekToFirst 之后仍不应有效")
	}
	it.Seek([]byte("a"))
	if it.Valid() {
		t.Fatal("Seek 之后仍不应有效")
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if got := collect(t, db.NewIterator(nil)); len(got) != 0 {
		t.Fatalf("空库迭代出 %v", got)
	}
}

// key 必须在迭代过程中稳定，且 Value 不受其它读取影响。
func TestIteratorKeyValueOwnership(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()
	mustPut(t, db, "only", "value")

	it := db.NewIterator(nil)
	defer it.Close()
	it.SeekToFirst()
	if !it.Valid() {
		t.Fatal("应当有一个 key")
	}
	k := string(it.Key())
	v := string(it.Value())
	if k != "only" || v != "value" {
		t.Fatalf("Key/Value = %q/%q", k, v)
	}
	if s := string(it.Key()); s != "only" {
		t.Fatalf("重复调用 Key() 得到 %q", s)
	}
}

func keysOf(t *testing.T, it Iterator) []string {
	t.Helper()
	defer it.Close()
	var got []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("迭代出错: %v", err)
	}
	return got
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
