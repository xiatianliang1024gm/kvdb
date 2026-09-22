package main

// YCSB 式负载是 M5 的验收工具（见 docs/DESIGN.md §6 与 §9.24）。
//
// 这里实现了 Yahoo! Cloud Serving Benchmark 的核心思想而不是它的全部：
// 六种经典负载（A~F）各自对应一种读写配比，请求分布支持 Zipfian——
// 真实系统里"少数 key 被反复访问"的倾斜分布，对缓存与压缩的效果远比均匀分布苛刻。
//
// 判读要点：
//   - workload A/B：MemTable 与 L0 的写入压力 + 点查为主的读路径；
//   - workload C：纯读，等于"块缓存 + Bloom + 分层布局"的净成绩；
//   - workload D：写入集中在尾部、读取也偏向最新 —— 观察新数据是否被压在了低层；
//   - workload E：范围扫描，主要看归并迭代器的开销；
//   - workload F：读改写，最接近"计数器/状态"类业务。

import (
	"bytes"
	"fmt"
	"math/rand"

	"sort"
	"strings"
	"time"

	"github.com/xiatianliang1024gm/kvdb"
)

// workloadSpec 是一种 YCSB 负载的读写配比。
type workloadSpec struct {
	name string
	desc string
	read float64
	// update / insert / scan / rmw 是其余操作的占比，五者之和为 1。
	update float64
	insert float64
	scan   float64
	rmw    float64
}

var workloads = map[string]workloadSpec{
	"A": {name: "A", desc: "Update heavy: 50% read / 50% update", read: 0.5, update: 0.5},
	"B": {name: "B", desc: "Read mostly: 95% read / 5% update", read: 0.95, update: 0.05},
	"C": {name: "C", desc: "Read only: 100% read", read: 1},
	"D": {name: "D", desc: "Read latest: 95% read (skewed to latest) / 5% insert", read: 0.95, insert: 0.05},
	"E": {name: "E", desc: "Scan: 100% range scan", scan: 1},
	"F": {name: "F", desc: "Read-modify-write: 50% read / 50% RMW", read: 0.5, rmw: 0.5},
}

// latRecorder 收集每操作延迟样本并输出分位数。
type latRecorder struct {
	samples []float64 // 微秒
}

func (l *latRecorder) add(d time.Duration) {
	l.samples = append(l.samples, float64(d.Microseconds()))
}

// summary 返回 (count, avg, p50, p95, p99, max)，单位微秒。
func (l *latRecorder) summary() (int, float64, float64, float64, float64, float64) {
	if len(l.samples) == 0 {
		return 0, 0, 0, 0, 0, 0
	}
	s := append([]float64(nil), l.samples...)
	sort.Float64s(s)
	pct := func(p float64) float64 {
		i := int(p * float64(len(s)-1))
		return s[i]
	}
	var sum float64
	for _, v := range s {
		sum += v
	}
	n := len(s)
	return n, sum / float64(n), pct(0.50), pct(0.95), pct(0.99), s[n-1]
}

// ycsbPhase 汇总一个阶段（load / run）的结果。
type ycsbPhase struct {
	name    string
	elapsed time.Duration
	ops     int
	reads   latRecorder
	writes  latRecorder
	scans   latRecorder
	found   int // 读到存在的 key 的次数
	missing int // 读到不存在 key 的次数
}

func (p *ycsbPhase) throughput() float64 {
	if p.elapsed <= 0 {
		return 0
	}
	return float64(p.ops) / p.elapsed.Seconds()
}

func (p *ycsbPhase) print() {
	fmt.Printf("  %-6s %v  %d ops  %.0f ops/s\n", p.name, p.elapsed.Round(time.Millisecond), p.ops, p.throughput())
	for _, e := range []struct {
		label string
		rec   *latRecorder
	}{
		{"read", &p.reads},
		{"write", &p.writes},
		{"scan", &p.scans},
	} {
		n, avg, p50, p95, p99, mx := e.rec.summary()
		if n == 0 {
			continue
		}
		fmt.Printf("         %-6s %6d ops  avg %7.2f  p50 %7.2f  p95 %7.2f  p99 %7.2f  max %8.2f (us)\n",
			e.label, n, avg, p50, p95, p99, mx)
	}
	if p.found+p.missing > 0 {
		fmt.Printf("         read hit %d / miss %d\n", p.found, p.missing)
	}
}

func parseWorkload(name string) (workloadSpec, error) {
	w, ok := workloads[name]
	if !ok {
		return workloadSpec{}, fmt.Errorf("unknown -workload %q (want A|B|C|D|E|F)", name)
	}
	return w, nil
}

// runYCSB 执行一次完整的 YCSB 式测量：load 阶段写入初始数据集，run 阶段按负载配比发请求。
func runYCSB(cfg config) error {
	w, err := parseWorkload(cfg.workload)
	if err != nil {
		return err
	}

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

	fmt.Println("kvdb-bench ycsb")
	fmt.Printf("  workload         %s (%s)\n", w.name, w.desc)
	fmt.Printf("  distribution     %s\n", map[bool]string{true: "zipfian", false: "uniform"}[cfg.zipf])
	printHeader(cfg, dir, db)
	fmt.Println()

	value := randomValue(cfg.valueSize, int(cfg.seed))

	// ── load 阶段：插入初始数据集 ──
	load := &ycsbPhase{name: "load"}
	begin := time.Now()
	for i := 0; i < cfg.numKeys; i++ {
		start := time.Now()
		if err := db.Put(ycsbKey(i), value); err != nil {
			return fmt.Errorf("load #%d: %w", i, err)
		}
		load.writes.add(time.Since(start))
		load.ops++
	}
	load.elapsed = time.Since(begin)
	if err := waitFlushed(db); err != nil {
		return err
	}
	load.print()

	// ── run 阶段 ──
	run := &ycsbPhase{name: "run"}
	rnd := rand.New(rand.NewSource(cfg.seed + 100))
	// Zipfian：s=1.1 足够倾斜又不至于全部请求挤在几十个 key 上。
	var zipf *rand.Zipf
	if cfg.zipf {
		zipf = rand.NewZipf(rnd, 1.1, 1, uint64(cfg.numKeys-1))
	}
	nextKey := func() int {
		if zipf != nil {
			return int(zipf.Uint64())
		}
		return rnd.Intn(cfg.numKeys)
	}

	// workload D 的"最新窗口"：读取偏向最近插入的 key。
	const latestWindow = 1000
	cursor := cfg.numKeys // insert 游标，指向下一个未使用的 key

	begin = time.Now()
	for i := 0; i < cfg.ops; i++ {
		roll := rnd.Float64()
		var opStart time.Time
		switch {
		case w.insert > 0 && roll < w.insert:
			// insert：顺序推进游标。
			opStart = time.Now()
			if err := db.Put(ycsbKey(cursor), value); err != nil {
				return fmt.Errorf("insert #%d: %w", cursor, err)
			}
			run.writes.add(time.Since(opStart))
			cursor++
			run.ops++

		case w.scan > 0 || roll < w.read+w.update+w.insert+w.scan+w.rmw:
			// 依概率落入 read / update / scan / rmw 之一。
			switch {
			case roll < w.read:
				// workload D：读取偏向最近窗口。
				k := nextKey()
				if w.insert > 0 && cursor > 0 {
					window := latestWindow
					if cursor < window {
						window = cursor
					}
					k = cursor - 1 - rnd.Intn(window)
				}
				opStart = time.Now()
				_, gerr := db.Get(ycsbKey(k))
				elapsed := time.Since(opStart)
				run.reads.add(elapsed)
				if gerr == nil {
					run.found++
				} else if gerr == kvdb.ErrNotFound {
					run.missing++
				} else {
					return fmt.Errorf("read: %w", gerr)
				}
				run.ops++

			case roll < w.read+w.update:
				k := nextKey()
				opStart = time.Now()
				if err := db.Put(ycsbKey(k), value); err != nil {
					return fmt.Errorf("update: %w", err)
				}
				run.writes.add(time.Since(opStart))
				run.ops++

			case roll < w.read+w.update+w.scan:
				k := nextKey()
				length := cfg.scanLen
				if k+length > cursor {
					length = cursor - k
				}
				if length <= 0 {
					continue
				}
				opt := &kvdb.IteratorOptions{}
				opt.LowerBound = ycsbKey(k)
				opt.UpperBound = ycsbKey(k + length - 1)
				opStart = time.Now()
				it := db.NewIterator(opt)
				n := 0
				for it.SeekToFirst(); it.Valid(); it.Next() {
					n++
				}
				serr := it.Error()
				it.Close()
				if serr != nil {
					return fmt.Errorf("scan: %w", serr)
				}
				run.scans.add(time.Since(opStart))
				_ = n
				run.ops++

			default: // rmw
				k := nextKey()
				opStart = time.Now()
				if _, gerr := db.Get(ycsbKey(k)); gerr != nil && gerr != kvdb.ErrNotFound {
					return fmt.Errorf("rmw read: %w", gerr)
				}
				if err := db.Put(ycsbKey(k), value); err != nil {
					return fmt.Errorf("rmw write: %w", err)
				}
				run.writes.add(time.Since(opStart))
				run.ops++
			}
		}
	}
	run.elapsed = time.Since(begin)

	// workload D 的 insert 语义：读取偏向"最新窗口"，而游标推进本身就是 insert。
	if w.insert > 0 && run.ops == 0 {
		return fmt.Errorf("workload D produced no ops")
	}

	fmt.Println()
	run.print()

	fmt.Println()

	printResult(cfg, &result{}, db)

	if cfg.report != "" {
		var b bytes.Buffer
		fmt.Fprintf(&b, "- workload: %s（%s）\n", w.name, w.desc)
		fmt.Fprintf(&b, "- keys: %d, ops: %d, value: %dB, compression: %s, rate-limit: %d B/s, zipfian: %v\n\n",
			cfg.numKeys, cfg.ops, cfg.valueSize, cfg.compression, cfg.rateLimit, cfg.zipf)
		fmt.Fprintf(&b, "| 阶段 | 操作 | 次数 | ops/s | p50(us) | p95(us) | p99(us) |\n|---|---|---:|---:|---:|---:|---:|\n")
		for _, e := range []struct {
			phase string
			rec   *latRecorder
		}{
			{"load", &load.writes},
			{"run/read", &run.reads},
			{"run/write", &run.writes},
			{"run/scan", &run.scans},
		} {
			n, _, p50, p95, p99, _ := e.rec.summary()
			if n == 0 {
				continue
			}
			thr := run.throughput()
			stage, op := "run", e.phase
			if idx := strings.Index(e.phase, "/"); idx >= 0 {
				stage, op = e.phase[:idx], e.phase[idx+1:]
			} else {
				op, stage = e.phase, "load"
				thr = load.throughput()
			}
			fmt.Fprintf(&b, "| %s | %s | %d | %.0f | %.1f | %.1f | %.1f |\n",
				stage, op, n, thr, p50, p95, p99)
		}
		fmt.Fprintf(&b, "\n总吞吐：load %.0f ops/s，run %.0f ops/s（读命中 %d / 未命中 %d）\n",
			load.throughput(), run.throughput(), run.found, run.missing)
		title := fmt.Sprintf("YCSB workload %s", w.name)
		if cfg.rateLimit > 0 {
			title += fmt.Sprintf("（限流 %d B/s）", cfg.rateLimit)
		}
		title += "（" + time.Now().Format("2006-01-02 15:04") + "）"
		if err := appendReport(cfg, title, b.String()); err != nil {
			return err
		}
	}
	return nil
}

// ycsbKey 生成 YCSB 风格的定长 key。
func ycsbKey(i int) []byte {
	return []byte(fmt.Sprintf("user%012d", i))
}

// randomValue 生成一段看起来像文本的随机 value（可压缩性介于纯重复与纯随机之间）。
func randomValue(size int, seed int) []byte {
	rnd := rand.New(rand.NewSource(int64(seed) + 7))
	b := make([]byte, size)
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 \n"
	for i := range b {
		b[i] = alphabet[rnd.Intn(len(alphabet))]
	}
	return b
}

// l0TriggerOf / levelBaseOf 返回生效的分层配置（0 = 默认值）。
func l0TriggerOf(cfg config) int {
	if cfg.l0Trigger > 0 {
		return cfg.l0Trigger
	}
	return 4
}

func levelBaseOf(cfg config) int {
	if cfg.levelBase > 0 {
		return cfg.levelBase
	}
	return 256 << 20
}
