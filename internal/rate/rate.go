// Package rate 提供字节速率限制器（令牌桶），用于给后台 Compaction 的磁盘带宽封顶。
//
// 为什么需要它：Compaction 是引擎里唯一会"长时间吃满磁盘"的后台任务。
// 一次 L1→L2 的搬运可能要读写几百 MB，如果不加约束，它会把顺序带宽全部占满，
// 前台点查的 p99 延迟跟着劣化。RocksDB 里这个组件叫 RateLimiter，位置和作用都一样。
//
// 三条设计上的取舍：
//
//  1. **0 表示不限流**，且 nil 接收者是安全的空操作 —— 与 internal/cache 的
//     约定一致。这样调用方不必在热路径上到处写 `if lim != nil`。
//  2. **按"块"计费而不是按"次"**：调用方（compact 包）按 256KB 累积一次，
//     而不是每条记录都调一次。限流器要跨协程串行化，粒度太细会让它自己变成瓶颈。
//  3. **长时间空闲后允许一次性突发，但额度有上限**。完全不设上限的话，
//     空闲一小时的引擎会攒出几小时的额度，一次 Compaction 就能瞬间吃掉。
//     上限取"一秒的额度"，够吸收任何正常的突发，又不至于让限流形同虚设。
package rate

import (
	"sync"
	"time"
)

// Stats 是限流器的累计统计。
type Stats struct {
	// Bytes 是累计放行的字节数。
	Bytes uint64
	// Waits 是实际发生了阻塞的次数，WaitNanos 是累计阻塞时长。
	//
	// 这两个数是"限流是否真的在起作用"的直接证据：Bytes 高而 Waits 为 0，
	// 说明载荷从没打满过配额，限流器没被触发（不是它没工作）。
	Waits     int64
	WaitNanos int64
}

// Limiter 是令牌桶式的字节速率限制器。零值不可用，请用 New 构造。
//
// 并发安全。一个实例可以被多条输出流水线共享（限流本来就是全局配额）。
type Limiter struct {
	mu     sync.Mutex
	rate   float64 // 字节/秒
	burst  float64 // 一次可以攒下的最大额度（字节）
	tokens float64
	last   time.Time

	closed   bool
	closedCh chan struct{}

	stats Stats
}

// minChunk 是"一次最多放行多少字节"的下限。
//
// 它与 burst 的关系是：Request 会把一次请求拆成若干笔不超过 credit 的配额，
// 于是即使调用方一次要 10MB，也能被切成小份、平滑地摊到时间轴上。
// 这个下限保证巨额的请求也总能被拆开（credit 不会小到 0）。
const minChunk = 64 << 10

// New 创建一个速率上限为 bytesPerSecond 的限流器。
//
// bytesPerSecond <= 0 时返回 nil，表示"不限流"：Request 在 nil 接收者上是空操作。
// 这个约定让"关掉限流"这条路径完全不需要分支。
func New(bytesPerSecond int) *Limiter {
	if bytesPerSecond <= 0 {
		return nil
	}
	r := float64(bytesPerSecond)
	burst := r // 最多攒一秒的额度
	if burst < minChunk {
		burst = minChunk
	}
	return &Limiter{
		rate:     r,
		burst:    burst,
		last:     time.Now(),
		closedCh: make(chan struct{}),
	}
}

// Rate 返回配置的字节速率（字节/秒）；nil 接收者返回 0。
func (l *Limiter) Rate() float64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rate
}

// credit 返回"一次最多放行多少字节"。
func (l *Limiter) credit() float64 {
	b := l.burst
	if b < minChunk {
		b = minChunk
	}
	return b
}

// refill 按流逝的时间补充令牌。调用方必须持有 l.mu。
func (l *Limiter) refillLocked(now time.Time) {
	el := now.Sub(l.last).Seconds()
	if el <= 0 {
		return
	}
	l.last = now
	l.tokens += el * l.rate
	if max := l.credit(); l.tokens > max {
		// 空闲时间不能无限累积成额度，否则一次 Compaction 就能把攒下的
		// 配额一口气吃完，"限速"变成"限速但可以无限透支"。
		l.tokens = max
	}
}

// Request 阻塞直到累计放行了 n 字节的配额。l 为 nil 或 n <= 0 时立即返回。
//
// 一次请求可能大于桶的上限（credit），此时会被拆成若干笔依次放行 ——
// 所以这个函数对任意大的 n 都能正常返回，调用方不需要自己分片。
//
// 计费发生在**放行之前**：这样"限住的是带宽"而不是"限住的是事后统计"。
func (l *Limiter) Request(n int) {
	if l == nil || n <= 0 {
		return
	}
	remaining := float64(n)
	var waited time.Duration
	waits := int64(0)

	for remaining > 0 {
		now := time.Now()
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return
		}
		l.refillLocked(now)

		grant := l.credit()
		if grant > remaining {
			grant = remaining
		}
		if l.tokens >= grant {
			l.tokens -= grant
			remaining -= grant
			l.stats.Bytes += uint64(grant)
			l.mu.Unlock()
			continue
		}

		// 攒不够这一笔：算出还差多少时间。
		need := grant - l.tokens
		d := time.Duration(need / l.rate * float64(time.Second))
		if d < time.Microsecond {
			// 极小额度下浮点除法会算出 0，不睡的话会退化成忙等。
			d = time.Microsecond
		}
		l.mu.Unlock()

		// 睡在锁外：把锁的持有时间压到最短，多个调用方（理论上）可以并行等待。
		waits++
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
		case <-l.closedCh:
			timer.Stop()
			l.recordWait(waits, waited)
			return
		}
		waited += d
	}
	l.recordWait(waits, waited)
}

// recordWait 把这一轮请求里发生的等待累加进统计。
func (l *Limiter) recordWait(waits int64, waited time.Duration) {
	if waits == 0 {
		return
	}
	l.mu.Lock()
	l.stats.Waits += waits
	l.stats.WaitNanos += waited.Nanoseconds()
	l.mu.Unlock()
}

// Stats 返回累计统计。
func (l *Limiter) Stats() Stats {
	if l == nil {
		return Stats{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stats
}

// Close 唤醒所有正在等待的调用方，并让后续的 Request 立即返回。
//
// 它必须存在：关库时要等后台 Compaction 收尾，而一个正在限流器里睡着的
// Compaction 可能要睡到下一个 256KB 的配额到手 —— 没有这一步，Close 会被
// 一个纯粹为了礼貌而存在的等待拖住。
//
// 放行而不报错是刻意的：限流是"软约束"，关库时为了它多等一秒毫无意义。
func (l *Limiter) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	close(l.closedCh)
}
