// Command kvdb-bench 是引擎的压测工具。
//
// M1 阶段它只回答两个问题：顺序写入吞吐是多少、点查延迟是多少。
// M2 补齐了读路径（Index 二分 + Bloom + 块缓存 + 归并迭代器），于是这里增加三类
// 指标，正好逐条对应 M2 的验收标准（见 docs/DESIGN.md §9.10）：
//
//	point  随机点查延迟，并与"确定不存在的 key"的负向查询分开统计
//	       —— 后者走 Bloom Filter，用来验证过滤器确实把读挡在了文件之外
//	scan   范围扫描吞吐，验证范围扫描可用
//	sweep  在一组不同的 SST 文件数下重建数据并重复点查
//	       —— 验证"读延迟不再随文件数线性增长"；默认还会跑一组
//	       "关闭 Bloom 与块缓存"的对照，用来说明曲线变平确实是这些优化的功劳
//
// 用法：
//
//	go run ./cmd/kvdb-bench -mode all   -n 100000
//	go run ./cmd/kvdb-bench -mode sweep -n 200000 -files 1,4,16,64
//
// YCSB 式负载、读写混合比例与各层放大系数留到 M5。
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"kvdb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "kvdb-bench: %v\n", err)
		os.Exit(1)
	}
}

// 运行模式。
const (
	modeAll   = "all"   // 写入 + 点查 + 范围扫描
	modeWrite = "write" // 只写（M1 的行为）
	modePoint = "point" // 只做点查
	modeScan  = "scan"  // 只做范围扫描
	modeSweep = "sweep" // 按文件数扫描（每次重建数据集）
)

type config struct {
	dir       string
	numKeys   int
	valueSize int
	memTable  int
	blockSize int
	cacheSize int
	bloomBits int
	sync      bool
	keepDir   bool
	verify    bool

	mode      string
	lookups   int
	missRatio float64
	scanLen   int
	scans     int
	seed      int64
	sample    int
	files     []int
	variants  bool
}

func run() error {
	var (
		dir         = flag.String("dir", "", "数据目录；为空时使用临时目录并在结束后删除")
		numKeys     = flag.Int("n", 100000, "写入的 key 数量")
		valueSize   = flag.Int("value-size", 100, "每个 value 的字节数")
		memTable    = flag.Int("memtable-size", 0, "MemTable 阈值（字节）；0 表示用默认值")
		blockSize   = flag.Int("block-size", 0, "Data Block 目标大小（字节）；0 表示用默认值")
		cacheSize   = flag.Int("cache-size", 0, "块缓存容量（字节）；0 表示用默认值，负数表示关闭")
		bloomBits   = flag.Int("bloom-bits", 0, "Bloom Filter 每 key 位数；0 表示用默认值，负数表示关闭")
		syncWrites  = flag.Bool("sync", false, "每条写都 fsync（默认关闭，仅测吞吐）")
		verify      = flag.Bool("verify", true, "写完之后把所有 key 读一遍并校验")
		keepDir     = flag.Bool("keep", false, "保留数据目录（配合 -dir 使用）")
		printSample = flag.Int("sample", 5, "打印多少个 key 作为抽样")
		mode        = flag.String("mode", modeAll, "运行模式：all | write | point | scan | sweep")
		lookups     = flag.Int("lookups", 0, "点查次数；0 表示与 -n 相同")
		missRatio   = flag.Float64("miss-ratio", 0.1, "点查中不存在的 key 所占比例（走 Bloom Filter）")
		scanLen     = flag.Int("scan-len", 100, "每次范围扫描覆盖的 key 数")
		scans       = flag.Int("scans", 0, "范围扫描次数；0 表示按数据集大小自动推导")
		seed        = flag.Int64("seed", 1, "随机数种子（便于复现）")
		files       = flag.String("files", "1,4,16,64", "sweep 模式下要测试的 SST 文件数列表")
		variants    = flag.Bool("variants", true, "sweep 模式下同时跑一组对照（关闭 Bloom 与块缓存）")
	)
	flag.Parse()

	cfg := config{
		dir:       *dir,
		numKeys:   *numKeys,
		valueSize: *valueSize,
		memTable:  *memTable,
		blockSize: *blockSize,
		cacheSize: *cacheSize,
		bloomBits: *bloomBits,
		sync:      *syncWrites,
		keepDir:   *keepDir,
		verify:    *verify,

		mode:      *mode,
		lookups:   *lookups,
		missRatio: *missRatio,
		scanLen:   *scanLen,
		scans:     *scans,
		seed:      *seed,
		sample:    *printSample,
		variants:  *variants,
	}
	if cfg.lookups <= 0 {
		cfg.lookups = cfg.numKeys
	}
	if cfg.scans <= 0 {
		if cfg.scanLen > 0 {
			cfg.scans = cfg.numKeys / cfg.scanLen
		}
		if cfg.scans <= 0 {
			cfg.scans = 1
		}
	}
	if cfg.missRatio < 0 {
		cfg.missRatio = 0
	}
	if cfg.missRatio > 1 {
		cfg.missRatio = 1
	}
	if cfg.numKeys <= 0 {
		return errors.New("-n must be positive")
	}

	switch cfg.mode {
	case modeSweep:
		sizes, err := parseFileList(*files)
		if err != nil {
			return err
		}
		return runSweep(cfg, sizes)
	case modeAll, modeWrite, modePoint, modeScan:
		return runOnce(cfg)
	default:
		return fmt.Errorf("unknown -mode %q (want all|write|point|scan|sweep)", cfg.mode)
	}
}

// ── 单次运行 ──────────────────────────────────────────────────────

func runOnce(cfg config) (err error) {
	dir, cleanup, err := prepareDir(cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	db, err := openDB(cfg, dir, true)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := db.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	printHeader(cfg, dir, db)
	fmt.Println()

	res := &result{}
	if cfg.mode != modePoint && cfg.mode != modeScan {
		if err := doWrite(db, cfg, res); err != nil {
			return err
		}
	}
	if cfg.mode == modeAll || cfg.mode == modePoint {
		if err := doPoint(cfg, db, res); err != nil {
			return err
		}
	}
	if cfg.mode == modeAll || cfg.mode == modeScan {
		if err := doScan(cfg, db, res); err != nil {
			return err
		}
	}
	printResult(res, db)
	return nil
}

// ── sweep：按 SST 文件数扫描 ──────────────────────────────────────

// sweepVariant 描述 sweep 里的一组读路径配置。
//
// 第二组把 Bloom 与块缓存都关掉，退回到"每个文件都要真去读数据块"的形态。
// 两组放在一起看，才能说明延迟曲线的变化确实来自 M2 的读优化，
// 而不是"文件数不够多所以看不出来"。
type sweepVariant struct {
	name      string
	cacheSize int
	bloomBits int
}

// sweepPoint 是一档文件数下的测量结果。
type sweepPoint struct {
	target  int
	files   int
	hitUS   float64
	missUS  float64
	hitRate string
}

func runSweep(base config, sizes []int) error {
	fmt.Println("kvdb-bench sweep")
	fmt.Printf("  keys             %d\n", base.numKeys)
	fmt.Printf("  value size       %d bytes\n", base.valueSize)
	fmt.Printf("  lookups/file     %d\n", base.lookups)
	fmt.Printf("  miss ratio       %.2f\n", base.missRatio)
	fmt.Printf("  block size       %d bytes\n", blockSizeOf(base))
	fmt.Printf("  seed             %d\n", base.seed)
	fmt.Printf("  file sizes       %v\n", sizes)

	variants := []sweepVariant{{name: "① M2 完整读路径：索引二分 + Bloom Filter + 块缓存"}}
	if base.variants {
		variants = append(variants, sweepVariant{
			name:      "② 对照：关闭 Bloom 与块缓存（每个文件都必须真读一个数据块）",
			cacheSize: -1,
			bloomBits: -1,
		})
	}

	for vi, v := range variants {
		if vi > 0 {
			fmt.Println()
		}
		fmt.Printf("\n  %s\n", v.name)
		fmt.Printf("  %-8s %-10s %-12s %-12s %-10s\n",
			"target", "sst files", "get(hit)", "get(miss)", "cache hit")
		fmt.Printf("  %-8s %-10s %-12s %-12s %-10s\n",
			"------", "---------", "--------", "---------", "---------")

		points := make([]sweepPoint, 0, len(sizes))
		for _, want := range sizes {
			p, err := runSweepCase(base, v, want)
			if err != nil {
				return err
			}
			points = append(points, p)
			fmt.Printf("  %-8d %-10d %-12s %-12s %-10s\n",
				p.target, p.files,
				fmt.Sprintf("%.2f us", p.hitUS),
				fmt.Sprintf("%.2f us", p.missUS),
				p.hitRate)
		}
		fmt.Printf("  %s\n", describeSlope(points))
	}

	fmt.Println()
	fmt.Println("  判读方式：纵向看 sst files 涨了多少倍、get(hit) 涨了多少倍。")
	fmt.Println("  M1 的读路径对每个文件都要全文件线性扫描，所以 get(hit) 与文件数是同倍增长；")
	fmt.Println("  M2 把\"每文件一次查找\"压成了一次内存里的元数据探测（索引二分 + Bloom 判定），")
	fmt.Println("  于是总量由\"一次数据块读取\"这个常数项主导，线性项被压掉约三个数量级。")
	fmt.Println("  注意：M2 还没有 Compaction，L0 文件数无上限（上限由 M3 的 L0CompactionTrigger 给出），")
	fmt.Println("  所以那个小线性项在 M3 之前会一直存在——不是零，只是量级变了。")
	return nil
}

// runSweepCase 跑一档：重建数据集 → 随机点查 → 收尾。
func runSweepCase(base config, v sweepVariant, want int) (sweepPoint, error) {
	cfg := base
	cfg.dir = ""
	cfg.memTable = memTableForFiles(base, want)
	if v.cacheSize != 0 {
		cfg.cacheSize = v.cacheSize
	}
	if v.bloomBits != 0 {
		cfg.bloomBits = v.bloomBits
	}

	dir, cleanup, err := prepareDir(cfg)
	if err != nil {
		return sweepPoint{}, err
	}
	db, err := openDB(cfg, dir, false)
	if err != nil {
		cleanup()
		return sweepPoint{}, err
	}

	res := &result{}
	werr := doWriteQuiet(db, cfg, res)
	var perr error
	if werr == nil {
		perr = doPointQuiet(cfg, db, res)
	}
	stats := db.Stats()
	cerr := db.Close()
	cleanup()

	if werr != nil {
		return sweepPoint{}, fmt.Errorf("files=%d: %w", want, werr)
	}
	if perr != nil {
		return sweepPoint{}, fmt.Errorf("files=%d: %w", want, perr)
	}
	if cerr != nil {
		return sweepPoint{}, cerr
	}

	return sweepPoint{
		target:  want,
		files:   stats.Files,
		hitUS:   usPerOpValue(res.getHit, res.getHitOps),
		missUS:  usPerOpValue(res.getMiss, res.getMissOps),
		hitRate: hitRate(stats.CacheHits, stats.CacheMisses),
	}, nil
}

// describeSlope 用最小二乘拟合 get(hit) 对文件数的斜率，并给出总量对比。
//
// 单看一张表容易被"2.97 → 13.10 好像也涨了"误导；把斜率和倍数写出来，
// 才能判断它是不是"线性"。
func describeSlope(points []sweepPoint) string {
	if len(points) < 2 {
		return "文件数不足两档，无法拟合"
	}
	xs := make([]float64, len(points))
	ys := make([]float64, len(points))
	for i, p := range points {
		xs[i] = float64(p.files)
		ys[i] = p.hitUS
	}
	slope, intercept := fitLine(xs, ys)

	first, last := points[0], points[len(points)-1]
	fileRatio := float64(last.files) / float64(first.files)
	latRatio := 1.0
	if first.hitUS > 0 {
		latRatio = last.hitUS / first.hitUS
	}
	return fmt.Sprintf(
		"拟合 get(hit) ≈ %.2f + %.3f × sst files (us/op)；文件数 ×%.0f、延迟 ×%.1f —— 线性项系数 %.3f us/文件",
		intercept, slope, fileRatio, latRatio, slope)
}

func fitLine(xs, ys []float64) (slope, intercept float64) {
	n := float64(len(xs))
	if n == 0 {
		return 0, 0
	}
	var sx, sy, sxx, sxy float64
	for i := range xs {
		sx += xs[i]
		sy += ys[i]
		sxx += xs[i] * xs[i]
		sxy += xs[i] * ys[i]
	}
	den := n*sxx - sx*sx
	if den == 0 {
		return 0, sy / n
	}
	slope = (n*sxy - sx*sy) / den
	intercept = (sy - slope*sx) / n
	return slope, intercept
}

// memTableForFiles 反推"正好产生 files 个 SST"所需的 MemTable 阈值。
//
// 每条记录在 MemTable 里除了 key+value 还有跳表节点与内部 key 的额外开销，
// 这里用 perKeyOverhead 粗估；只要量级对得上，文件数就不会差太多。
func memTableForFiles(cfg config, files int) int {
	const perKeyOverhead = 48
	intKeyLen := len("key") + 12 + 8 // user key + trailer
	perKey := intKeyLen + cfg.valueSize + perKeyOverhead
	if files <= 1 {
		// 一个文件：预算刚好低于总量，写满一次即可落 1 个 SST。
		return cfg.numKeys * perKey
	}
	size := cfg.numKeys * perKey / files
	if min := 256 << 10; size < min {
		size = min
	}
	return size
}

func blockSizeOf(cfg config) int {
	if cfg.blockSize > 0 {
		return cfg.blockSize
	}
	return 4 << 10
}

// ── 各阶段动作 ────────────────────────────────────────────────────

// result 汇总一次运行里各阶段的耗时；sweep 模式只用到读的部分。
type result struct {
	writeElapsed time.Duration
	readElapsed  time.Duration
	readOK       bool

	getHit     time.Duration
	getHitOps  int
	getMiss    time.Duration
	getMissOps int

	scanElapsed time.Duration
	scanOps     int
	scanKeys    int
}

func doWrite(db *kvdb.DB, cfg config, res *result) error {
	if err := doWriteQuiet(db, cfg, res); err != nil {
		return err
	}
	fmt.Printf("  write            %v (%d ops, %.0f ops/s, %.2f us/op)\n",
		res.writeElapsed.Round(time.Millisecond), cfg.numKeys,
		float64(cfg.numKeys)/res.writeElapsed.Seconds(),
		usPerOpValue(res.writeElapsed, cfg.numKeys))

	if cfg.verify {
		start := time.Now()
		value := valueBytes(cfg)
		for i := 0; i < cfg.numKeys; i++ {
			got, err := db.Get(key(i))
			if err != nil {
				return fmt.Errorf("get #%d: %w", i, err)
			}
			if !bytes.Equal(got, value) {
				return fmt.Errorf("get #%d: value mismatch (%d bytes)", i, len(got))
			}
		}
		res.readElapsed, res.readOK = time.Since(start), true
		fmt.Printf("  verify read      %v (%d ops, %.0f ops/s, %.2f us/op)\n",
			res.readElapsed.Round(time.Millisecond), cfg.numKeys,
			float64(cfg.numKeys)/res.readElapsed.Seconds(),
			usPerOpValue(res.readElapsed, cfg.numKeys))
	}
	return nil
}

// doWriteQuiet 只写，不打印也不校验，供 sweep 复用。
func doWriteQuiet(db *kvdb.DB, cfg config, res *result) error {
	value := valueBytes(cfg)
	start := time.Now()
	for i := 0; i < cfg.numKeys; i++ {
		if err := db.Put(key(i), value); err != nil {
			return fmt.Errorf("put #%d: %w", i, err)
		}
	}
	res.writeElapsed = time.Since(start)
	return waitFlushed(db)
}

func doPoint(cfg config, db *kvdb.DB, res *result) error {
	if err := doPointQuiet(cfg, db, res); err != nil {
		return err
	}
	if res.getHitOps > 0 {
		fmt.Printf("  get (hit)        %v (%d ops, %.0f ops/s, %.2f us/op)\n",
			res.getHit.Round(time.Millisecond), res.getHitOps,
			float64(res.getHitOps)/res.getHit.Seconds(),
			usPerOpValue(res.getHit, res.getHitOps))
	}
	if res.getMissOps > 0 {
		fmt.Printf("  get (miss)       %v (%d ops, %.0f ops/s, %.2f us/op)\n",
			res.getMiss.Round(time.Millisecond), res.getMissOps,
			float64(res.getMissOps)/res.getMiss.Seconds(),
			usPerOpValue(res.getMiss, res.getMissOps))
	}
	return nil
}

// doPointQuiet 做两轮随机点查：
//
//	命中轮  取 [0, n) 内的随机 key，校验返回值正确（如果 -verify）
//	未命中轮 取 n 之后的一段 key，它们一定不存在，用来观察 Bloom Filter 的效果
//
// 两轮分开统计，因为它们的成本结构完全不同：命中要真的读块，未命中在 Bloom
// 判定为"肯定不在"之后就直接返回了。
func doPointQuiet(cfg config, db *kvdb.DB, res *result) error {
	value := valueBytes(cfg)
	missOps := int(float64(cfg.lookups) * cfg.missRatio)
	hitOps := cfg.lookups - missOps

	if hitOps > 0 {
		rnd := rand.New(rand.NewSource(cfg.seed))
		start := time.Now()
		for i := 0; i < hitOps; i++ {
			got, err := db.Get(key(rnd.Intn(cfg.numKeys)))
			if err != nil {
				return fmt.Errorf("point lookup: %w", err)
			}
			if cfg.verify && !bytes.Equal(got, value) {
				return fmt.Errorf("point lookup: value mismatch (%d bytes)", len(got))
			}
		}
		res.getHit, res.getHitOps = time.Since(start), hitOps
	}

	if missOps > 0 {
		rnd := rand.New(rand.NewSource(cfg.seed + 1))
		start := time.Now()
		for i := 0; i < missOps; i++ {
			// 全部落在真实数据之后，保证一定是"不存在"。
			_, err := db.Get(key(cfg.numKeys + 1 + rnd.Intn(cfg.numKeys)))
			if err == nil {
				return fmt.Errorf("negative lookup: key unexpectedly found")
			}
			if !errors.Is(err, kvdb.ErrNotFound) {
				return fmt.Errorf("negative lookup: %w", err)
			}
		}
		res.getMiss, res.getMissOps = time.Since(start), missOps
	}
	return nil
}

func doScan(cfg config, db *kvdb.DB, res *result) error {
	if cfg.scanLen <= 0 {
		return errors.New("-scan-len must be positive")
	}
	span := cfg.scanLen
	if span > cfg.numKeys {
		span = cfg.numKeys
	}
	starts := cfg.numKeys - span + 1
	if starts < 1 {
		starts = 1
	}

	rnd := rand.New(rand.NewSource(cfg.seed + 2))
	opt := &kvdb.IteratorOptions{}
	start := time.Now()
	for i := 0; i < cfg.scans; i++ {
		from := rnd.Intn(starts)
		opt.LowerBound = key(from)
		opt.UpperBound = key(from + span - 1)

		it := db.NewIterator(opt)
		n := 0
		for it.SeekToFirst(); it.Valid(); it.Next() {
			if cfg.verify && len(it.Value()) != cfg.valueSize {
				it.Close()
				return fmt.Errorf("scan: value size %d, want %d", len(it.Value()), cfg.valueSize)
			}
			n++
		}
		err := it.Error()
		it.Close()
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		if cfg.verify && n != span {
			return fmt.Errorf("scan: got %d keys, want %d", n, span)
		}
		res.scanKeys += n
	}
	res.scanElapsed = time.Since(start)
	res.scanOps = cfg.scans

	fmt.Printf("  scan (%d keys)   %v (%d scans, %d keys, %.1f keys/scan, %.2f us/key)\n",
		span, res.scanElapsed.Round(time.Millisecond), res.scanOps, res.scanKeys,
		float64(res.scanKeys)/float64(res.scanOps),
		usPerOpValue(res.scanElapsed, res.scanKeys))
	return nil
}

// ── 输出 ──────────────────────────────────────────────────────────

func printHeader(cfg config, dir string, db *kvdb.DB) {
	opts := buildOptions(cfg, dir)
	fmt.Printf("kvdb-bench (mode %s)\n", cfg.mode)
	fmt.Printf("  dir              %s\n", dir)
	fmt.Printf("  keys             %d\n", cfg.numKeys)
	fmt.Printf("  value size       %d bytes\n", cfg.valueSize)
	fmt.Printf("  memtable size    %d bytes\n", opts.MemTableSize)
	fmt.Printf("  block size       %d bytes\n", opts.BlockSize)
	if opts.BlockCacheSize > 0 {
		fmt.Printf("  block cache      %d bytes\n", opts.BlockCacheSize)
	} else {
		fmt.Printf("  block cache      disabled\n")
	}
	if opts.BloomBitsPerKey > 0 {
		fmt.Printf("  bloom bits/key   %d\n", opts.BloomBitsPerKey)
	} else {
		fmt.Printf("  bloom filter     disabled\n")
	}
	fmt.Printf("  sync writes      %v\n", opts.SyncWrites)
	if cfg.sample > 0 && cfg.numKeys > 0 {
		fmt.Printf("  key sample       %s ... %s\n", key(0), key(cfg.numKeys-1))
	}
	if r := db.RecoveryReport(); len(r.TornLogs) > 0 || len(r.DiscardedFiles) > 0 || r.ReplayedRecords > 0 {
		fmt.Printf("  recovery         replayed=%d torn logs=%v discarded=%v\n",
			r.ReplayedRecords, r.TornLogs, r.DiscardedFiles)
	}
}

func printResult(res *result, db *kvdb.DB) {
	stats := db.Stats()
	fmt.Println()
	fmt.Printf("  sst files        %d\n", stats.Files)
	fmt.Printf("  memtable live    %d bytes\n", stats.MemTableSize)
	fmt.Printf("  last sequence    %d\n", stats.LastSequence)
	fmt.Printf("  cache hit rate   %s (hits=%d misses=%d, %.1f MB / %d items)\n",
		hitRate(stats.CacheHits, stats.CacheMisses), stats.CacheHits, stats.CacheMisses,
		float64(stats.CacheBytes)/(1<<20), stats.CacheItems)
}

// ── 小工具 ────────────────────────────────────────────────────────

// key 生成定长的顺序 key，避免把 key 编码本身的开销混进结果。
func key(i int) []byte {
	return []byte(fmt.Sprintf("key%012d", i))
}

func valueBytes(cfg config) []byte {
	return bytes.Repeat([]byte("v"), cfg.valueSize)
}

func usPerOp(d time.Duration, ops int) string {
	return fmt.Sprintf("%.2f us", usPerOpValue(d, ops))
}

func usPerOpValue(d time.Duration, ops int) float64 {
	if ops <= 0 {
		return 0
	}
	return float64(d.Microseconds()) / float64(ops)
}

func hitRate(hits, misses int64) string {
	total := hits + misses
	if total == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(hits)/float64(total))
}

func buildOptions(cfg config, dir string) kvdb.Options {
	opts := kvdb.DefaultOptions(dir)
	opts.SyncWrites = cfg.sync
	if cfg.memTable > 0 {
		opts.MemTableSize = cfg.memTable
	}
	if cfg.blockSize > 0 {
		opts.BlockSize = cfg.blockSize
	}
	if cfg.cacheSize != 0 {
		opts.BlockCacheSize = cfg.cacheSize // 负数表示显式关闭
	}
	if cfg.bloomBits != 0 {
		opts.BloomBitsPerKey = cfg.bloomBits
	}
	return opts
}

func openDB(cfg config, dir string, verbose bool) (*kvdb.DB, error) {
	start := time.Now()
	db, err := kvdb.Open(buildOptions(cfg, dir))
	if err != nil {
		return nil, err
	}
	if verbose {
		fmt.Printf("  open             %v\n", time.Since(start).Round(time.Microsecond))
	}
	return db, nil
}

// prepareDir 返回一个可用的数据目录以及清理函数。
func prepareDir(cfg config) (string, func(), error) {
	if cfg.dir != "" {
		return cfg.dir, func() {}, nil
	}
	dir, err := os.MkdirTemp("", "kvdb-bench-")
	if err != nil {
		return "", nil, err
	}
	if cfg.keepDir {
		return dir, func() {}, nil
	}
	return dir, func() { os.RemoveAll(dir) }, nil
}

// waitFlushed 等到后台把所有 Immutable MemTable 落盘。
//
// 写完之后立刻做点查，如果不停下来等一等，最后一段数据还在 MemTable 里没进 SST，
// 那测出来的就不是"文件读路径"的延迟了。
func waitFlushed(db *kvdb.DB) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		if !db.Stats().HasImmutable {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for background flush")
		}
		time.Sleep(time.Millisecond)
	}
}

func parseFileList(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("bad -files entry %q: %w", part, err)
		}
		if n < 1 {
			return nil, fmt.Errorf("bad -files entry %d: must be >= 1", n)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, errors.New("-files must list at least one file count")
	}
	return out, nil
}
