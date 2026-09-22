package main

// checkpoint 模式验证 M5 的最后一个交付物：一致性副本。
//
// 流程：写入数据（刻意让一部分停留在 MemTable / WAL 里）→ DB.Checkpoint
// 生成副本 → 副本独立打开并全量校验 → 源库继续写、做 Checkpoint 之后的变更，
// 确认副本数据停留在快照时刻。
//
// 顺便测量三段时间：Checkpoint 本身（应当远快于全量重写，硬链接时接近零拷贝）、
// 副本打开时间、校验耗时。

import (
	"bytes"
	"fmt"
	"time"

	"github.com/xiatianliang1024gm/kvdb"
)

func runCheckpointBench(cfg config) error {
	dir, cleanup, err := prepareDir(cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	db, err := openDB(cfg, dir, false)
	if err != nil {
		return err
	}
	defer db.Close()

	fmt.Println("kvdb-bench checkpoint（一致性副本）")
	printHeader(cfg, dir, db)
	fmt.Println()

	value := randomValue(cfg.valueSize, int(cfg.seed))
	begin := time.Now()
	for i := 0; i < cfg.numKeys; i++ {
		if err := db.Put(key(i), value); err != nil {
			return fmt.Errorf("put #%d: %w", i, err)
		}
	}
	fmt.Printf("  load             %v (%d keys)\n", time.Since(begin).Round(time.Millisecond), cfg.numKeys)

	// 一部分删除 + 一部分覆盖：副本必须精确停在这个时刻。
	for i := 0; i < cfg.numKeys/10; i++ {
		if err := db.Delete(key(i)); err != nil {
			return fmt.Errorf("delete #%d: %w", i, err)
		}
	}

	dst := dir + "-copy"
	begin = time.Now()
	if err := db.Checkpoint(dst); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	ckElapsed := time.Since(begin)

	begin = time.Now()
	copied, err := kvdb.Open(kvdb.DefaultOptions(dst))
	if err != nil {
		return fmt.Errorf("open checkpoint: %w", err)
	}
	openElapsed := time.Since(begin)

	begin = time.Now()
	verified := 0
	for i := cfg.numKeys / 10; i < cfg.numKeys; i++ {
		got, gerr := copied.Get(key(i))
		if gerr != nil {
			copied.Close()
			return fmt.Errorf("verify key #%d: %w", i, gerr)
		}
		if !bytes.Equal(got, value) {
			copied.Close()
			return fmt.Errorf("verify key #%d: value mismatch", i)
		}
		verified++
	}
	verifyElapsed := time.Since(begin)
	if err := copied.Close(); err != nil {
		return err
	}

	fmt.Printf("  checkpoint       %v -> %s\n", ckElapsed.Round(time.Millisecond), dst)
	fmt.Printf("  open copy        %v\n", openElapsed.Round(time.Millisecond))
	fmt.Printf("  verify           %v (%d keys, %.2f us/key)\n",
		verifyElapsed.Round(time.Millisecond), verified, usPerOpValue(verifyElapsed, verified))

	// 源库继续写：这些变更不允许出现在副本里。
	for i := cfg.numKeys; i < cfg.numKeys+100; i++ {
		if err := db.Put(key(i), value); err != nil {
			return err
		}
	}
	copied2, err := kvdb.Open(kvdb.DefaultOptions(dst))
	if err != nil {
		return err
	}
	defer copied2.Close()
	for i := cfg.numKeys; i < cfg.numKeys+100; i++ {
		if _, err := copied2.Get(key(i)); err != kvdb.ErrNotFound {
			return fmt.Errorf("副本包含了 Checkpoint 之后的数据（key #%d, err=%v）", i, err)
		}
	}
	fmt.Println("  isolation        ok（Checkpoint 之后的写入没有泄漏进副本）")

	if cfg.report != "" {
		var b bytes.Buffer
		fmt.Fprintf(&b, "- keys: %d, value: %dB, compression: %s\n\n", cfg.numKeys, cfg.valueSize, cfg.compression)
		fmt.Fprintf(&b, "| 项目 | 耗时 |\n|---|---:|\n")
		fmt.Fprintf(&b, "| Checkpoint 生成 | %v |\n| 副本打开 | %v |\n| 校验 %d keys | %v |\n\n", ckElapsed.Round(time.Millisecond), openElapsed.Round(time.Millisecond), verified, verifyElapsed.Round(time.Millisecond))
		fmt.Fprintf(&b, "隔离性：Checkpoint 之后的 100 条写入未出现在副本中。\n")
		if err := appendReport(cfg, fmt.Sprintf("Checkpoint 一致性副本（%s）", time.Now().Format("2006-01-02 15:04")), b.String()); err != nil {
			return err
		}
	}
	return nil
}
