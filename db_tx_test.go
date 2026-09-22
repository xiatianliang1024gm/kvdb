package kvdb

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 本文件是 M9（提交期校验原语）的验收测试，对应 docs/EXTENSIONS.md §4.4：
//
//	1. GetForUpdate 的 seq 语义                              → TestGetForUpdateSeq
//	2. check 通过 ⇒ 写入；check 失败 ⇒ 整批不写 + 不停库      → TestWriteCheckedCommitAbort
//	3. 读集被并发事务改过 ⇒ check 查出冲突                    → TestWriteCheckedDetectsConflict
//	4. check 失败后 WAL 不留痕（Stats.WALBytes 不增长）       → TestWriteCheckedAbortLeavesNoWAL
//	5. check 内 panic 不挂写队列、库照常可用                  → TestWriteCheckedPanicRecovered
//	6. "check 通过 → 落库"窗口内其他写者插不进来              → TestWriteCheckedExclusiveWindow
//	7. 并发 CAS 计数器：无丢失更新、无死锁                    → TestWriteCheckedConcurrentCAS
//	8. merge 批次走 WriteChecked 的 seq 语义                  → TestGetForUpdateMergeSeq

var errTxConflict = errors.New("kvdb.test: transaction conflict")

func mustPutBytes(t *testing.T, db *DB, k string, v []byte) {
	t.Helper()
	if err := db.Put([]byte(k), v); err != nil {
		t.Fatalf("Put(%q) failed: %v", k, err)
	}
}

func mustGetBytes(t *testing.T, db *DB, k string) []byte {
	t.Helper()
	v, err := db.Get([]byte(k))
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", k, err)
	}
	return v
}

// --- 1. GetForUpdate 的 seq 语义 -------------------------------------------------

func TestGetForUpdateSeq(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	// 不存在 ⇒ (nil, 0, nil)，error 必须是 nil（"不存在"是正常状态，不是错误）。
	v, seq, err := db.GetForUpdate([]byte("missing"))
	if v != nil || seq != 0 || err != nil {
		t.Fatalf("GetForUpdate(missing) = (%v, %d, %v), want (nil, 0, nil)", v, seq, err)
	}

	mustPut(t, db, "k", "v1")
	v1, seq1, err := db.GetForUpdate([]byte("k"))
	if err != nil || string(v1) != "v1" || seq1 == 0 {
		t.Fatalf("GetForUpdate(k) = (%q, %d, %v), want (v1, >0, nil)", v1, seq1, err)
	}

	// 覆盖 ⇒ 新版本新 seq。
	mustPut(t, db, "k", "v2")
	_, seq2, _ := db.GetForUpdate([]byte("k"))
	if seq2 <= seq1 {
		t.Fatalf("覆盖后 seq = %d, want > %d", seq2, seq1)
	}

	// 删除 ⇒ 回到"不存在"，seq 归零 —— 删除后重新提交的"读集 = 不存在"校验才成立。
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if v, seq, err := db.GetForUpdate([]byte("k")); v != nil || seq != 0 || err != nil {
		t.Fatalf("删除后 GetForUpdate(k) = (%v, %d, %v), want (nil, 0, nil)", v, seq, err)
	}

	// 范围墓碑遮蔽 ⇒ 同样是不存在。
	mustPut(t, db, "r1", "x")
	b := NewWriteBatch()
	if err := b.DeleteRange([]byte("r"), []byte("s")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}
	if err := db.Write(b); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if v, seq, err := db.GetForUpdate([]byte("r1")); v != nil || seq != 0 || err != nil {
		t.Fatalf("范围删除后 GetForUpdate(r1) = (%v, %d, %v), want (nil, 0, nil)", v, seq, err)
	}

	if _, _, err := db.GetForUpdate(nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("GetForUpdate(nil) = %v, want ErrEmptyKey", err)
	}
}

// --- 2. commit / abort 的基本语义 ------------------------------------------------

func TestWriteCheckedCommitAbort(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	mustPut(t, db, "acct", "100")

	// check 通过 ⇒ 批次落库。
	b := NewWriteBatch()
	b.Put([]byte("acct"), []byte("200"))
	if err := db.WriteChecked(b, func() error { return nil }); err != nil {
		t.Fatalf("WriteChecked failed: %v", err)
	}
	if got := mustGet(t, db, "acct"); got != "200" {
		t.Fatalf("提交后 acct = %q, want 200", got)
	}

	// check 失败 ⇒ 整批不写，数据保持原样，引擎不停库。
	b = NewWriteBatch()
	b.Put([]byte("acct"), []byte("999"))
	b.Put([]byte("ghost"), []byte("boo"))
	txErr := errors.New("kvdb.test: abort")
	if err := db.WriteChecked(b, func() error { return txErr }); !errors.Is(err, txErr) {
		t.Fatalf("WriteChecked = %v, want txErr", err)
	}
	if got := mustGet(t, db, "acct"); got != "200" {
		t.Fatalf("abort 后 acct = %q, want 200（整批不得写入）", got)
	}
	mustMiss(t, db, "ghost")

	// 引擎还活着：后续普通写、以及一次通过的 WriteChecked 都正常。
	mustPut(t, db, "after", "ok")
	if got := mustGet(t, db, "after"); got != "ok" {
		t.Fatalf("abort 后普通写失效: acct-after = %q", got)
	}
	b = NewWriteBatch()
	b.Put([]byte("acct"), []byte("300"))
	if err := db.WriteChecked(b, func() error { return nil }); err != nil {
		t.Fatalf("abort 后 WriteChecked 失败: %v", err)
	}
	if got := mustGet(t, db, "acct"); got != "300" {
		t.Fatalf("第二次提交后 acct = %q, want 300", got)
	}

	// 空批次 / nil check：与 Write 的空批次语义一致，直接 no-op。
	if err := db.WriteChecked(nil, func() error { return txErr }); err != nil {
		t.Fatalf("WriteChecked(nil) = %v, want nil", err)
	}
	if err := db.WriteChecked(NewWriteBatch(), func() error { return txErr }); err != nil {
		t.Fatalf("WriteChecked(空批次) = %v, want nil", err)
	}
}

// --- 3. 读集冲突检测（验收标准场景） ---------------------------------------------

func TestWriteCheckedDetectsConflict(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	mustPut(t, db, "acct", "100")

	// T1 读读集。
	_, seq1, err := db.GetForUpdate([]byte("acct"))
	if err != nil {
		t.Fatalf("GetForUpdate failed: %v", err)
	}
	b1 := NewWriteBatch()
	b1.Put([]byte("acct"), []byte("t1"))
	check1 := func() error {
		_, cur, err := db.GetForUpdate([]byte("acct"))
		if err != nil {
			return err
		}
		if cur != seq1 {
			return errTxConflict
		}
		return nil
	}

	// T2 先提交，动了 T1 的读集。
	b2 := NewWriteBatch()
	b2.Put([]byte("acct"), []byte("t2"))
	if err := db.WriteChecked(b2, func() error { return nil }); err != nil {
		t.Fatalf("T2 提交失败: %v", err)
	}

	// T1 后提交：check 查出 seq 变了 ⇒ abort，数据是 T2 的。
	if err := db.WriteChecked(b1, check1); !errors.Is(err, errTxConflict) {
		t.Fatalf("T1 提交 = %v, want errTxConflict", err)
	}
	if got := mustGet(t, db, "acct"); got != "t2" {
		t.Fatalf("T1 abort 后 acct = %q, want t2（T1 不得覆盖）", got)
	}

	// "改了又改回来"也查得出：seq 是单调的，字节相同也骗不过比对。
	mustPut(t, db, "acct", "100") // 字节上回到 T1 读到的值
	if err := db.WriteChecked(b1, check1); !errors.Is(err, errTxConflict) {
		t.Fatalf("改了又改回来 T1 提交 = %v, want errTxConflict", err)
	}
}

// --- 4. abort 不在 WAL 里留痕 ----------------------------------------------------

func TestWriteCheckedAbortLeavesNoWAL(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	mustPut(t, db, "k", "v")
	before := db.Stats().WALBytes

	b := NewWriteBatch()
	b.Put([]byte("k2"), []byte("v2"))
	if err := db.WriteChecked(b, func() error { return errTxConflict }); !errors.Is(err, errTxConflict) {
		t.Fatalf("WriteChecked = %v, want errTxConflict", err)
	}
	if after := db.Stats().WALBytes; after != before {
		t.Fatalf("abort 后 WALBytes = %d, want 仍为 %d —— abort 不得写 WAL", after, before)
	}
}

// --- 5. check 的 panic 被兜住 ----------------------------------------------------

func TestWriteCheckedPanicRecovered(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	b := NewWriteBatch()
	b.Put([]byte("k"), []byte("v"))
	if err := db.WriteChecked(b, func() error { panic("boom in check") }); err == nil {
		t.Fatalf("check panic 应转为 error 返回")
	}
	// 整批不写。
	mustMiss(t, db, "k")

	// 写队列没有被 panic 炸挂：并发写者照常完成，队长照常让位。
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := db.Put([]byte("w"), []byte{byte(i)}); err != nil {
				t.Errorf("panic 后 Put 失败: %v", err)
			}
		}(i)
	}
	wg.Wait()
	mustPut(t, db, "k", "v") // 自己再走一遍写路径
	if got := mustGet(t, db, "k"); got != "v" {
		t.Fatalf("panic 后数据异常: k = %q", got)
	}
}

// --- 6. 校验窗口的独占性 ---------------------------------------------------------

func TestWriteCheckedExclusiveWindow(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	mustPut(t, db, "hot", "v1")
	_, seq1, err := db.GetForUpdate([]byte("hot"))
	if err != nil {
		t.Fatalf("GetForUpdate failed: %v", err)
	}

	clobberDone := make(chan struct{})
	otherDone := make(chan struct{})
	check := func() error {
		// 校验窗口里发动两个并发写：必须被挡在队列里，直到本批落库之后。
		// 一个写同一个 key（冲突路径），一个写别的 key（普通路径，证明没有饿死）。
		go func() {
			if err := db.Put([]byte("hot"), []byte("clobber")); err != nil {
				t.Errorf("并发 Put 失败: %v", err)
			}
			close(clobberDone)
		}()
		go func() {
			if err := db.Put([]byte("other"), []byte("o")); err != nil {
				t.Errorf("并发 Put 失败: %v", err)
			}
			close(otherDone)
		}()
		time.Sleep(50 * time.Millisecond) // 给并发写充足的机会越窗（若能越，立即生效）

		v, cur, err := db.GetForUpdate([]byte("hot"))
		if err != nil {
			return err
		}
		if cur != seq1 || string(v) != "v1" {
			t.Errorf("校验窗口内读集被改: (%q, %d), want (v1, %d)", v, cur, seq1)
			return errTxConflict
		}
		return nil
	}

	b := NewWriteBatch()
	b.Put([]byte("hot"), []byte("v2"))
	if err := db.WriteChecked(b, check); err != nil {
		t.Fatalf("WriteChecked failed: %v", err)
	}
	// 两个并发写都排在本批之后：队长让位前会排空队列，所以返回时它们都已落地。
	// "本批的 v2"这个中间态从外部观察不到（已被 clobber 覆盖）—— 窗口独占性
	// 由 check 内部的断言保证：窗口里 hot 仍是 (v1, seq1)。
	for _, done := range []chan struct{}{clobberDone, otherDone} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("并发 Put 未在 5s 内落地 —— 写队列疑似挂死")
		}
	}
	if got := mustGet(t, db, "hot"); got != "clobber" {
		t.Fatalf("并发写落地后 hot = %q, want clobber", got)
	}
	mustGet(t, db, "other") // 并发写者没有被校验窗口饿死
}

// --- 7. 并发 CAS：无丢失更新、无死锁 ---------------------------------------------

func TestWriteCheckedConcurrentCAS(t *testing.T) {
	db := openTestDB(t, nil)
	defer db.Close()

	const workers = 8
	var success atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				v, seq, err := db.GetForUpdate([]byte("counter"))
				if err != nil {
					t.Errorf("GetForUpdate failed: %v", err)
					return
				}
				var cur uint64
				if v != nil {
					cur = binary.BigEndian.Uint64(v)
				}
				b := NewWriteBatch()
				b.Put([]byte("counter"), counter(cur+1))
				err = db.WriteChecked(b, func() error {
					_, curSeq, err := db.GetForUpdate([]byte("counter"))
					if err != nil {
						return err
					}
					if curSeq != seq {
						return errTxConflict
					}
					return nil
				})
				if err == nil {
					success.Add(1)
					return // 本 worker 的自增已提交
				}
				if !errors.Is(err, errTxConflict) {
					t.Errorf("WriteChecked = %v, want nil 或 errTxConflict", err)
					return
				}
				// 冲突：重试（快照隔离 + 先提交者胜的标准玩法）
			}
		}()
	}
	wg.Wait()

	// 无丢失更新：成功次数 == 计数器终值 == worker 数。
	if got := binary.BigEndian.Uint64(mustGetBytes(t, db, "counter")); got != workers {
		t.Fatalf("counter = %d, want %d —— 出现丢失更新", got, workers)
	}
	if n := success.Load(); n != workers {
		t.Fatalf("成功提交 %d 次, want %d", n, workers)
	}
}

// --- 8. merge 链的 seq 语义 ------------------------------------------------------

func TestGetForUpdateMergeSeq(t *testing.T) {
	db := openTestDB(t, func(o *Options) { o.MergeOperator = incrOp{} })
	defer db.Close()

	mustPutBytes(t, db, "c", counter(10))
	b := NewWriteBatch()
	if err := b.Merge([]byte("c"), counter(1)); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	if err := b.Merge([]byte("c"), counter(2)); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	if err := db.Write(b); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 折叠值 13；seq 是最新 operand 的序列号 —— 校验"没落新 operand"就靠它。
	v, seq1, err := db.GetForUpdate([]byte("c"))
	if err != nil || binary.BigEndian.Uint64(v) != 13 {
		t.Fatalf("GetForUpdate(c) = (%v, %d, %v), want (13, _, nil)", v, seq1, err)
	}

	b = NewWriteBatch()
	if err := b.Merge([]byte("c"), counter(4)); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	if err := db.Write(b); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	_, seq2, _ := db.GetForUpdate([]byte("c"))
	if seq2 <= seq1 {
		t.Fatalf("新 operand 落下后 seq = %d, want > %d", seq2, seq1)
	}

	// check 里比对 merge 链的 seq：期间又落了一个 operand ⇒ 冲突。
	_, seq3, _ := db.GetForUpdate([]byte("c"))
	b = NewWriteBatch()
	if err := b.Merge([]byte("c"), counter(100)); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	if err := db.Write(b); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	b = NewWriteBatch()
	b.Put([]byte("c"), counter(99))
	if err := db.WriteChecked(b, func() error {
		_, cur, err := db.GetForUpdate([]byte("c"))
		if err != nil {
			return err
		}
		if cur != seq3 {
			return errTxConflict
		}
		return nil
	}); err == nil {
		t.Fatal("check 应当发现 merge 链在读取之后又前进了")
	}
	if got := binary.BigEndian.Uint64(mustGetBytes(t, db, "c")); got != 117 {
		t.Fatalf("abort 后 c = %d, want 117（13+4+100，batch 不得写入）", got)
	}
}
