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
// M4 补齐了写路径的一致性（Snapshot / WriteBatch / Group Commit），于是增加：
//
//	group  在一组不同的并发写者数下重复"并发写 -> 读回校验"
//	       —— 验证组提交把并发写请求合并进了更少的 fsync。
//	       SyncWrites 打开时写吞吐的天花板就是 fsync 次数，所以这一模式下
//	       ops/s 随写者数上升、而 fsync 次数几乎不动，正是组提交在起作用。
//
// 用法：
//
//	go run ./cmd/kvdb-bench -mode all   -n 100000
//	go run ./cmd/kvdb-bench -mode sweep -n 200000 -files 1,4,16,64
//	go run ./cmd/kvdb-bench -mode group -writes 2000 -writers 1,2,4,8,16,32
//
//	ycsb        YCSB 式负载（workload A~F、Zipfian 分布、per-op 延迟分位）
//	compress    none / snappy / zlib 三种块压缩的磁盘占用与读写开销对照
//	checkpoint  一致性副本的生成、打开、隔离性验证
//
// 三个新模式都受 -compression / -rate-limit 影响，头部会打印当前配置。
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
	"sync"
	"time"

	"github.com/xiatianliang1024gm/kvdb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "kvdb-bench: %v\n", err)
		os.Exit(1)
	}
}

// 运行模式。
const (
	modeAll       = "all"        // 写入 + 点查 + 范围扫描
	modeWrite     = "write"      // 只写（M1 的行为）
	modePoint     = "point"      // 只做点查
	modeScan      = "scan"       // 只做范围扫描
	modeSweep     = "sweep"      // 按文件数扫描（每次重建数据集）
	modeGroup     = "group"      // 按并发写者数扫描（组提交）
	modeYCSB      = "ycsb"       // YCSB 式负载（M5）
	modeCompress  = "compress"   // 压缩算法对照（M5）
	modeCheckpoin = "checkpoint" // 一致性副本（M5）
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

	writes     int
	writerList []int
	groupSync  bool

	// ── M5：ycsb / compress / checkpoint ──
	workload    string
	ops         int
	zipf        bool
	compression string // 原始旗标值（打印用）
	comp        kvdb.Compression
	rateLimit   int
	report      string
	l0Trigger   int
	levelBase   int
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
		mode        = flag.String("mode", modeAll, "运行模式：all | write | point | scan | sweep | group | ycsb | compress | checkpoint")
		lookups     = flag.Int("lookups", 0, "点查次数；0 表示与 -n 相同")
		missRatio   = flag.Float64("miss-ratio", 0.1, "点查中不存在的 key 所占比例（走 Bloom Filter）")
		scanLen     = flag.Int("scan-len", 100, "每次范围扫描覆盖的 key 数")
		scans       = flag.Int("scans", 0, "范围扫描次数；0 表示按数据集大小自动推导")
		seed        = flag.Int64("seed", 1, "随机数种子（便于复现）")
		files       = flag.String("files", "1,4,16,64", "sweep 模式下要测试的 SST 文件数列表")
		variants    = flag.Bool("variants", true, "sweep 模式下同时跑一组对照（关闭 Bloom 与块缓存）")

		writes     = flag.Int("writes", 2000, "group 模式下每档的总写入次数")
		writerList = flag.String("writers", "1,2,4,8,16,32", "group 模式下要测试的并发写者数列表")
		groupSync  = flag.Bool("group-sync", true, "group 模式下是否每条写都 fsync（组提交只有开着它才有意义）")

		workload    = flag.String("workload", "A", "ycsb 模式的负载：A|B|C|D|E|F")
		ops         = flag.Int("ops", 100000, "ycsb 模式 run 阶段的操作数")
		zipf        = flag.Bool("zipf", true, "ycsb 模式使用 Zipfian 请求分布（false = 均匀分布）")
		compression = flag.String("compression", "snappy", "块压缩算法：none|snappy|zlib")
		rateLimit   = flag.Int("rate-limit", 0, "后台 Compaction 带宽上限（字节/秒）；0 表示不限流")
		l0Trigger   = flag.Int("l0-trigger", 0, "L0 触发 Compaction 的文件数；0 表示用默认值")
		levelBase   = flag.Int("level-base-size", 0, "L1 容量上限（字节）；0 表示用默认值")
		report      = flag.String("report", "", "把本次运行的 markdown 摘要追加到该文件（配合 ycsb/compress/checkpoint 模式）")
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

		writes:    *writes,
		groupSync: *groupSync,

		workload:    *workload,
		ops:         *ops,
		zipf:        *zipf,
		compression: *compression,
		rateLimit:   *rateLimit,
		report:      *report,
		l0Trigger:   *l0Trigger,
		levelBase:   *levelBase,
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
	comp, err := parseCompression(cfg.compression)
	if err != nil {
		return err
	}
	cfg.comp = comp

	switch cfg.mode {
	case modeSweep:
		sizes, err := parseFileList(*files)
		if err != nil {
			return err
		}
		return runSweep(cfg, sizes)
	case modeGroup:
		ws, err := parseFileList(*writerList)
		if err != nil {
			return err
		}
		return runGroup(cfg, ws)
	case modeYCSB:
		if cfg.ops <= 0 {
			return errors.New("-ops must be positive")
		}
		return runYCSB(cfg)
	case modeCompress:
		return runCompress(cfg)
	case modeCheckpoin:
		return runCheckpointBench(cfg)
	case modeAll, modeWrite, modePoint, modeScan:
		return runOnce(cfg)
	default:
		return fmt.Errorf("unknown -mode %q (want all|write|point|scan|sweep|group|ycsb|compress|checkpoint)", cfg.mode)
	}
}

// parseCompression 解析 -compression 的取值。
func parseCompression(s string) (kvdb.Compression, error) {
	switch s {
	case "none", "off":
		return kvdb.CompressionNone, nil
	case "snappy":
		return kvdb.CompressionSnappy, nil
	case "zlib", "flate":
		return kvdb.CompressionZlib, nil
	default:
		return 0, fmt.Errorf("unknown -compression %q (want none|snappy|zlib)", s)
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
	printResult(cfg, res, db)
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

// ── group：按并发写者数扫描（组提交） ────────────────────────────
//
// 这一模式是 M4 的验收工具。写性能的天花板由 fsync 次数决定（见 docs/DESIGN.md §3），
// 所以只要把 SyncWrites 打开，"每秒能 fsync 多少次"就是写吞吐的上限。
//
// M3 的写路径里每个写者各做一次 fsync，并发只会让它们排队；M4 的组提交把同一时刻
// 排队的写者合并进一次 fsync，于是 ops/s 随写者数上升而 fsyncs 这一列几乎不动。
// 两列放在一起看，"组提交到底省了多少 fsync"就不需要靠论证了。

// groupPoint 是一档并发写者数下的测量结果。
type groupPoint struct {
	writers  int
	elapsed  time.Duration
	batches  int64 // 写入次数（= 参与组提交的批次数）
	groups   int64 // 提交组数
	fsyncs   int64 // 实际执行的 WAL fsync 次数
	maxGroup int64 // 观察到的最大组大小
}

// opsPerSec 是写入吞吐。
func (p groupPoint) opsPerSec() float64 {
	if p.elapsed <= 0 {
		return 0
	}
	return float64(p.batches) / p.elapsed.Seconds()
}

// merge 是平均一组合并了多少个写者，也就是"每次 fsync 摊销了几次写"。
func (p groupPoint) merge() float64 {
	if p.groups == 0 {
		return 0
	}
	return float64(p.batches) / float64(p.groups)
}

// usPerFsync 是分摊到每次 fsync 的墙钟时间。它近似于这台机器上一次 fsync 的代价，
// 也是"ops/s 为什么涨不上去"的直接解释。
func (p groupPoint) usPerFsync() float64 {
	if p.fsyncs == 0 {
		return 0
	}
	return float64(p.elapsed.Microseconds()) / float64(p.fsyncs)
}

func runGroup(base config, writerCounts []int) error {
	opts := buildOptions(base, "")
	fmt.Println("kvdb-bench group（组提交）")
	fmt.Printf("  writes/case      %d\n", base.writes)
	fmt.Printf("  value size       %d bytes\n", base.valueSize)
	fmt.Printf("  memtable size    %d bytes\n", opts.MemTableSize)
	fmt.Printf("  sync writes      %v\n", base.groupSync)
	fmt.Printf("  writer counts    %v\n", writerCounts)
	fmt.Println()
	fmt.Printf("  %-8s %-11s %-9s %-9s %-8s %-6s %-10s %-9s\n",
		"writers", "ops/s", "groups", "fsyncs", "merge", "max", "us/write", "us/fsync")
	fmt.Printf("  %-8s %-11s %-9s %-9s %-8s %-6s %-10s %-9s\n",
		"-------", "-----", "------", "------", "-----", "---", "--------", "--------")

	points := make([]groupPoint, 0, len(writerCounts))
	for _, w := range writerCounts {
		p, err := runGroupCase(base, w)
		if err != nil {
			return fmt.Errorf("writers=%d: %w", w, err)
		}
		points = append(points, p)
		fmt.Printf("  %-8d %-11.0f %-9d %-9d %-8.2f %-6d %-10.1f %-9.1f\n",
			p.writers, p.opsPerSec(), p.groups, p.fsyncs, p.merge(), p.maxGroup,
			usPerOpValue(p.elapsed, int(p.batches)), p.usPerFsync())
	}

	fmt.Println()
	fmt.Println("  " + describeGroupScaling(points))
	fmt.Println("  判读方式：sync writes 为 true 时，ops/s 的上限就是「每秒能做多少次 fsync」。")
	fmt.Println("  看 ops/s 与 fsyncs 两列的走向：ops/s 涨而 fsyncs 不跟着涨，说明多出来的")
	fmt.Println("  写入被合并进了同一批 fsync —— merge 这一列就是每次 fsync 摊销的写次数。")
	return nil
}

// runGroupCase 跑一档：W 个写者并发写 total 条（key 空间互不重叠），然后读回校验。
func runGroupCase(base config, writers int) (groupPoint, error) {
	if writers < 1 {
		return groupPoint{}, errors.New("writers must be >= 1")
	}
	cfg := base
	cfg.dir = ""
	cfg.sync = base.groupSync

	dir, cleanup, err := prepareDir(cfg)
	if err != nil {
		return groupPoint{}, err
	}
	defer cleanup()

	db, err := openDB(cfg, dir, false)
	if err != nil {
		return groupPoint{}, err
	}

	// key 空间按写者切开：这样读回校验失败时能指出是谁写丢的。
	per, extra := cfg.writes/writers, cfg.writes%writers
	value := valueBytes(cfg)

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	next := 0
	for w := 0; w < writers; w++ {
		n := per
		if w < extra {
			n++
		}
		from := next
		next += n
		wg.Add(1)
		go func(from, n int) {
			defer wg.Done()
			<-start // 一起出发，才谈得上"同时排队"
			for i := 0; i < n; i++ {
				if err := db.Put(key(from+i), value); err != nil {
					errs <- err
					return
				}
			}
		}(from, n)
	}

	begin := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(begin)
	close(errs)
	for err := range errs {
		_ = db.Close()
		return groupPoint{}, err
	}

	if cfg.verify {
		for i := 0; i < cfg.writes; i++ {
			got, gerr := db.Get(key(i))
			if gerr != nil {
				_ = db.Close()
				return groupPoint{}, fmt.Errorf("verify key #%d: %w", i, gerr)
			}
			if !bytes.Equal(got, value) {
				_ = db.Close()
				return groupPoint{}, fmt.Errorf("verify key #%d: value mismatch (%d bytes)", i, len(got))
			}
		}
	}

	stats := db.Stats()
	if err := db.Close(); err != nil {
		return groupPoint{}, err
	}
	return groupPoint{
		writers:  writers,
		elapsed:  elapsed,
		batches:  stats.WriteBatches,
		groups:   stats.WriteGroups,
		fsyncs:   stats.WALFsyncs,
		maxGroup: stats.MaxWriteGroup,
	}, nil
}

// describeGroupScaling 把首尾两档放在一起比，避免读者自己去算倍数。
func describeGroupScaling(points []groupPoint) string {
	if len(points) < 2 {
		return "写者数不足两档，无法比较"
	}
	first, last := points[0], points[len(points)-1]
	return fmt.Sprintf(
		"写者数 %d -> %d：ops/s ×%.1f，fsync 次数 ×%.2f，合并率 %.2f -> %.2f —— 吞吐涨而 fsync 没涨",
		first.writers, last.writers,
		ratioFloat(last.opsPerSec(), first.opsPerSec()),
		ratioFloat(float64(last.fsyncs), float64(first.fsyncs)),
		first.merge(), last.merge())
}

func ratioFloat(num, den float64) float64 {
	if den == 0 {
		return 0
	}
	return num / den
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
		// M7 起上界是半开的（不含），直接指向区间后第一个 key。
		opt.UpperBound = key(from + span)

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
	fmt.Printf("  compression      %s\n", opts.Compression)
	if opts.CompactionRateLimit > 0 {
		fmt.Printf("  rate limit       %d bytes/sec\n", opts.CompactionRateLimit)
	} else {
		fmt.Printf("  rate limit       unlimited\n")
	}
	// 分层的两个参数决定了"什么时候搬、搬到哪一层"，是解读下面 level 分布的前提。
	fmt.Printf("  l0 trigger       %d files\n", opts.L0CompactionTrigger)
	fmt.Printf("  level sizing     L1 %s x %d, max levels %d\n",
		humanBytes(uint64(opts.LevelBaseSize)), opts.LevelSizeMultiplier, opts.MaxLevels)
	if cfg.sample > 0 && cfg.numKeys > 0 {
		fmt.Printf("  key sample       %s ... %s\n", key(0), key(cfg.numKeys-1))
	}
	if r := db.RecoveryReport(); len(r.TornLogs) > 0 || len(r.DiscardedFiles) > 0 || r.ReplayedRecords > 0 {
		fmt.Printf("  recovery         replayed=%d torn logs=%v discarded=%v\n",
			r.ReplayedRecords, r.TornLogs, r.DiscardedFiles)
	}
}

func printResult(cfg config, res *result, db *kvdb.DB) {
	stats := db.Stats()
	fmt.Println()
	fmt.Printf("  sst files        %d\n", stats.Files)
	// 分层布局。M3 的写入优化效果几乎全在这几行里：L0 的文件数应当稳定在
	// L0CompactionTrigger 附近（而 M2 里它会随写入量线性增长），L1 以下的
	// 字节数应当按 LevelSizeMultiplier 呈阶梯式增长。
	for i, lv := range stats.Levels {
		if lv.Files == 0 && i > 0 {
			continue
		}
		fmt.Printf("  level %-2d         %d files, %s\n", i, lv.Files, humanBytes(lv.Bytes))
	}
	fmt.Printf("  memtable live    %d bytes\n", stats.MemTableSize)
	fmt.Printf("  last sequence    %d\n", stats.LastSequence)
	fmt.Printf("  cache hit rate   %s (hits=%d misses=%d, %.1f MB / %d items)\n",
		hitRate(stats.CacheHits, stats.CacheMisses), stats.CacheHits, stats.CacheMisses,
		float64(stats.CacheBytes)/(1<<20), stats.CacheItems)

	// ── 写放大 ──
	// 定义是 LSM 里通用的那一个：引擎总共写出多少字节 / 用户放进去多少字节。
	// 三个来源一个都不能漏 —— WAL（每条写都先落一遍）、Flush（MemTable 落成 L0）、
	// Compaction 的输出。分子刻意不含"Compaction 读进来的字节"，那是读放大的一部分。
	user := uint64(cfg.numKeys) * uint64(len(key(0))+cfg.valueSize)
	written := stats.WALBytes + stats.FlushBytes + stats.Compaction.OutputBytes
	fmt.Printf("  write amp        %s (wal %s + flush %s + compaction-out %s / user %s)\n",
		ratio(written, user), humanBytes(stats.WALBytes), humanBytes(stats.FlushBytes),
		humanBytes(stats.Compaction.OutputBytes), humanBytes(user))

	// ── 读放大 ──
	// 定义是"一次点查平均向几个 SST 发起查找"。这是 Bloom Filter 挡不掉的那部分成本：
	// Bloom 只让"碰一次"变得便宜（一次哈希查内存），真正让次数降下来的是 Compaction
	// 把散在 L0 的一堆小文件收敛成 L1 以下的少数大文件。
	if stats.Gets > 0 {
		fmt.Printf("  read amp         %.2f probes/get (gets=%d probes=%d)\n",
			float64(stats.ReadProbes)/float64(stats.Gets), stats.Gets, stats.ReadProbes)
	}

	// ── 组提交（M4）──
	// 写入次数与 fsync 次数的比值就是组提交的合并率。SyncWrites 为真时它直接决定
	// 写吞吐的天花板；为假时这一行只有合并率可看（fsyncs 恒为 0）。
	if stats.WriteBatches > 0 {
		merge := ratioFloat(float64(stats.WriteBatches), float64(stats.WriteGroups))
		if stats.WALFsyncs > 0 {
			fmt.Printf("  group commit     %d writes / %d groups = %.2f merge, %d wal fsyncs (%.2f writes per fsync, max group %d)\n",
				stats.WriteBatches, stats.WriteGroups, merge, stats.WALFsyncs,
				ratioFloat(float64(stats.WriteBatches), float64(stats.WALFsyncs)), stats.MaxWriteGroup)
		} else {
			fmt.Printf("  group commit     %d writes / %d groups = %.2f merge, wal fsyncs disabled (max group %d)\n",
				stats.WriteBatches, stats.WriteGroups, merge, stats.MaxWriteGroup)
		}
	}
	if stats.LiveSnapshots != 0 {
		fmt.Printf("  live snapshots   %d（还有快照没 Release，旧版本暂时清不掉）\n", stats.LiveSnapshots)
	}

	// ── Compaction 规模 ──
	// 输入字节与输出字节的比值就是"这一层被反复搬运"的程度，是调优的直接依据。
	if c := stats.Compaction; c.Count > 0 {
		fmt.Printf("  compaction       %d times, in %d files %s -> out %d files %s, dropped %d records\n",
			c.Count, c.InputFiles, humanBytes(c.InputBytes), c.OutputFiles,
			humanBytes(c.OutputBytes), c.DroppedRecords)
	}
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

// ratio 打印 num/den 的比值，保留两位小数；den 为 0 时返回 "n/a"。
func ratio(num, den uint64) string {
	if den == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2fx", float64(num)/float64(den))
}

// humanBytes 把字节数打印成便于比较的量级。
func humanBytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
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
	opts.Compression = cfg.comp
	if cfg.rateLimit > 0 {
		opts.CompactionRateLimit = cfg.rateLimit
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

// ── 报告输出 ──────────────────────────────────────────────────────

// appendReport 把一段 markdown 追加到 cfg.report 指定的文件；文件不存在时
// 先写报告头。报告只记录事实（配置 + 数字），结论留给阅读的人。
func appendReport(cfg config, title, body string) error {
	if cfg.report == "" {
		return nil
	}
	f, err := os.OpenFile(cfg.report, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	info, _ := f.Stat()
	if info != nil && info.Size() == 0 {
		fmt.Fprintf(f, "# kvdb 基准报告\n\n由 `kvdb-bench` 生成。每一节是一次独立的运行记录。\n\n---\n\n")
	}
	fmt.Fprintf(f, "## %s\n\n%s\n\n---\n\n", title, body)
	return f.Close()
}
