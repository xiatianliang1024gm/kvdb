package main

// compress 模式回答 M5 的第一个问题：块压缩到底省了多少、贵了多少。
//
// 三种算法（none / snappy / zlib）各建一个库、写入同一份负载、做同一轮点查，
// 然后并排比较三件事：
//
//   - 磁盘占用：数据目录里 SST 的实际字节数（压缩比由数据决定，不是由口号决定）
//   - 写入吞吐：压缩发生在 Flush / Compaction 的关键路径上，CPU 是要付钱的
//   - 点查延迟：读侧多了"块缓存未命中时解压一次"的成本
//
// 判读方式：none 是基线。snappy 通常以个位数的 CPU 换 30%+ 的空间；
// zlib(BestSpeed) 空间更省但写放大在 CPU 上明显。负载的熵决定一切——
// 换 -value-size 或真实数据再跑一遍，结论可能完全不同。

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xiatianliang1024gm/kvdb"
)

type compressCase struct {
	name string
	comp kvdb.Compression
}

func runCompress(cfg config) error {
	cases := []compressCase{
		{"none", kvdb.CompressionNone},
		{"snappy", kvdb.CompressionSnappy},
		{"zlib", kvdb.CompressionZlib},
	}

	fmt.Println("kvdb-bench compress（块压缩对照）")
	fmt.Printf("  keys             %d\n", cfg.numKeys)
	fmt.Printf("  value size       %d bytes (random text)\n", cfg.valueSize)
	fmt.Printf("  point lookups    %d / case\n", cfg.lookups)
	fmt.Printf("  memtable size    %d bytes\n", memTableOfSize(cfg))
	fmt.Println()
	fmt.Printf("  %-8s %-12s %-11s %-12s %-12s %-12s\n",
		"algo", "sst bytes", "ratio", "write", "get avg", "get p99")
	fmt.Printf("  %-8s %-12s %-11s %-12s %-12s %-12s\n",
		"----", "---------", "-----", "-----", "-------", "------")

	type row struct {
		name      string
		sstBytes  uint64
		ratio     float64
		write     time.Duration
		getAvg    float64
		getP99    float64
		decomp    int64
		blocks    int64
		compSaved string
	}
	var rows []row

	for _, c := range cases {
		cc := cfg
		cc.dir = ""
		cc.comp = c.comp
		cc.verify = false

		dir, cleanup, err := prepareDir(cc)
		if err != nil {
			return err
		}

		db, err := openDB(cc, dir, false)
		if err != nil {
			cleanup()
			return err
		}
		value := randomValue(cc.valueSize, int(cc.seed))
		begin := time.Now()
		for i := 0; i < cc.numKeys; i++ {
			if err := db.Put(key(i), value); err != nil {
				_ = db.Close()
				cleanup()
				return fmt.Errorf("%s: put #%d: %w", c.name, i, err)
			}
		}
		writeElapsed := time.Since(begin)
		if err := waitFlushed(db); err != nil {
			_ = db.Close()
			cleanup()
			return err
		}
		// 等 Compaction 收敛，让磁盘占用反映的是稳态而不是一堆 L0 小文件。
		db.Close()

		db, err = openDB(cc, dir, false)
		if err != nil {
			cleanup()
			return err
		}
		begin = time.Now()
		gets := &latRecorder{}
		for i := 0; i < cc.lookups; i++ {
			k := (i * 7919) % cc.numKeys // 步进质数，覆盖全空间
			start := time.Now()
			if _, err := db.Get(key(k)); err != nil {
				_ = db.Close()
				cleanup()
				return fmt.Errorf("%s: get #%d: %w", c.name, i, err)
			}
			gets.add(time.Since(start))
		}
		readElapsed := time.Since(begin)
		stats := db.Stats()
		db.Close()

		var sstBytes uint64
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() && filepath.Ext(path) == ".sst" {
				sstBytes += uint64(info.Size())
			}
			return nil
		})
		_, _, _, _, p99, _ := gets.summary()
		comp := stats.Compression
		ratio := 1.0
		if comp.StoredBytes > 0 {
			ratio = float64(comp.RawBytes) / float64(comp.StoredBytes)
		}
		rows = append(rows, row{
			name:      c.name,
			sstBytes:  sstBytes,
			ratio:     ratio,
			write:     writeElapsed,
			getAvg:    usPerOpValue(readElapsed, cc.lookups),
			getP99:    p99,
			decomp:    comp.Decompressions,
			blocks:    comp.BlocksWritten,
			compSaved: fmt.Sprintf("%d/%d", comp.CompressedBlocks, comp.BlocksWritten),
		})
		cleanup()
	}

	for _, r := range rows {
		fmt.Printf("  %-8s %-12s %-11.2f %-12v %-12.2f %-12.2f\n",
			r.name, humanBytes(r.sstBytes), r.ratio,
			r.write.Round(time.Millisecond), r.getAvg, r.getP99)
	}
	fmt.Println()
	fmt.Println("  判读方式：ratio = 写出块的原始字节 / 实际落盘字节（仅压缩过的块计入，块级独立判断）。")
	fmt.Println("  get p99 的差异主要来自块缓存未命中时的解压；块缓存全命中时三者应当几乎一致。")

	if cfg.report != "" {
		var b bytes.Buffer
		fmt.Fprintf(&b, "- keys: %d, value: %dB (random text), memtable: %dB\n\n", cfg.numKeys, cfg.valueSize, memTableOfSize(cfg))
		fmt.Fprintf(&b, "| 算法 | SST 占用 | 压缩比 | 写入 | get avg(us) | get p99(us) | 压缩块/总块 |\n")
		fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|\n")
		for _, r := range rows {
			fmt.Fprintf(&b, "| %s | %s | %.2f | %v | %.2f | %.2f | %s |\n",
				r.name, humanBytes(r.sstBytes), r.ratio, r.write.Round(time.Millisecond), r.getAvg, r.getP99, r.compSaved)
		}
		if err := appendReport(cfg, fmt.Sprintf("压缩算法对照（%s）", time.Now().Format("2006-01-02 15:04")), b.String()); err != nil {
			return err
		}
	}
	return nil
}

func memTableOfSize(cfg config) int {
	if cfg.memTable > 0 {
		return cfg.memTable
	}
	return 64 << 20
}
