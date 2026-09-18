package rate

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// resetTokens 把桶里的令牌清零并把计时基准挪到现在。
//
// 它让用例不必为了"先花掉初始额度"而白等一秒。属于白盒手法，但只出现在测试里：
// 从外部观察，桶里有初始额度这件事本身没有错（这正是"允许一次突发"）。
func resetTokens(l *Limiter, tokens float64) {
	l.mu.Lock()
	l.tokens = tokens
	l.last = time.Now()
	l.mu.Unlock()
}

// TestNilLimiterIsNoOp 锁定"关掉限流"这条路径不需要分支。
//
// 与 internal/cache 的 nil 约定一致：Options 里写 0 就落到 nil，
// 调用方永远不用写 `if lim != nil`。
func TestNilLimiterIsNoOp(t *testing.T) {
	var l *Limiter
	if got := New(0); got != nil {
		t.Fatalf("New(0) = %v, want nil", got)
	}
	if got := New(-1); got != nil {
		t.Fatalf("New(-1) = %v, want nil", got)
	}
	start := time.Now()
	l.Request(1 << 20) // 传 nil 接收者，不能 panic
	if got := l.Rate(); got != 0 {
		t.Errorf("nil.Rate() = %v", got)
	}
	if got := l.Stats(); got != (Stats{}) {
		t.Errorf("nil.Stats() = %v", got)
	}
	l.Close()
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("nil 限流器不该阻塞，实际等了 %v", d)
	}
}

// TestRequestEnforcesRate 是核心用例：放行 N 字节应当至少花掉 N/rate 的时间。
//
// 断言方向刻意取"不小于"：调度抖动只会让实际耗时更长、不会更短，
// 所以这是一条稳定的下界断言，而不是一条会随机器波动的等值断言。
func TestRequestEnforcesRate(t *testing.T) {
	const rate = 8 << 20 // 8 MB/s
	l := New(rate)
	defer l.Close()
	resetTokens(l, 0)

	const total = 4 << 20 // 4MB，理论耗时 0.5 秒
	start := time.Now()
	l.Request(total)
	elapsed := time.Since(start)

	want := time.Duration(float64(total) / rate * float64(time.Second))
	if elapsed < want*9/10 {
		t.Errorf("放行 %d 字节只花了 %v，理论下界 %v —— 限流没生效", total, elapsed, want)
	}
	// 上界给得宽松：这条只用来抓"限流器把请求卡死或反复重试"这类量级错误。
	if elapsed > want*4 {
		t.Errorf("放行 %d 字节花了 %v，远超理论值 %v", total, elapsed, want)
	}

	st := l.Stats()
	if st.Bytes < total {
		t.Errorf("Stats().Bytes = %d，应当至少 %d", st.Bytes, total)
	}
	if st.Waits == 0 {
		t.Error("发生了限流却没有记录等待次数")
	}
	if st.WaitNanos <= 0 {
		t.Error("发生了等待却没有记录等待时长")
	}
}

// TestSmallRequestsPassThrough 验证"没打满配额时不等待"。
//
// 限流器不该给小额请求强加延迟：一个只有几 MB 的 Compaction 在 64MB/s 的
// 配额下应当一路畅通。这条用例防的是"每笔请求都至少睡一个最小间隔"那种实现。
func TestSmallRequestsPassThrough(t *testing.T) {
	l := New(64 << 20) // 64 MB/s
	defer l.Close()
	resetTokens(l, 0)
	start := time.Now()
	for i := 0; i < 100; i++ {
		l.Request(4 << 10) // 共 400KB ≈ 6ms 的额度
	}
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Errorf("400KB 的请求被拖了 %v，限流器粒度太粗", d)
	}
}

// TestOversizedRequestIsSplit 验证单次请求大于桶容量时也能正常返回。
//
// 桶的额度有上限（防"空闲一小时攒出无限额度"），所以一笔超过上限的请求
// 必须被拆成多笔放行。写错的话这里会**死循环** —— 而不是返回错误，
// 那才是最糟的失败形态，所以用超时看门狗把它变成一条明确的失败。
func TestOversizedRequestIsSplit(t *testing.T) {
	const rate = 4 << 20 // 桶容量 = 4MB
	l := New(rate)
	defer l.Close()
	resetTokens(l, 0)

	done := make(chan struct{})
	go func() {
		l.Request(4<<20 + 64<<10) // 略大于桶容量，必然触发拆分
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("超过桶容量的请求没有返回（多半是死循环）")
	}
	if got := l.Stats().Bytes; got < 4<<20 {
		t.Errorf("拆分后只放行了 %d 字节", got)
	}
}

// TestIdleCreditIsBounded 验证长时间空闲不会攒出无限额度。
//
// 不设上限的话，一台空闲的引擎在第一次 Compaction 时会把攒了几小时的配额
// 一口气花掉，"限流"就只剩下一个名字了。
func TestIdleCreditIsBounded(t *testing.T) {
	const rate = 4 << 20
	l := New(rate)
	defer l.Close()

	l.mu.Lock()
	l.tokens = 0
	l.last = time.Now().Add(-time.Hour) // 模拟长期空闲
	l.mu.Unlock()

	start := time.Now()
	l.Request(2 * (4 << 20)) // 8MB：前 4MB 吃掉被封顶的额度，后 4MB 必须真等 1 秒
	elapsed := time.Since(start)
	if elapsed < 800*time.Millisecond {
		t.Errorf("空闲一小时后一次性放行了 8MB 只花 %v，额度上限没起作用", elapsed)
	}
}

// TestCloseUnblocksWaiters 验证关库能把睡着的调用方叫醒。
//
// 没有这一步，Close 会被一次"纯粹为了礼貌"的限流等待拖住 —— 限流是软约束，
// 关库时为了它多等一个配额毫无意义。
func TestCloseUnblocksWaiters(t *testing.T) {
	l := New(1 << 10) // 1 KB/s
	resetTokens(l, 1<<20)

	l.Request(1 << 10) // 额度充足，立即返回
	done := make(chan struct{})
	go func() {
		l.Request(1 << 20) // 按 1KB/s 要等几十分钟
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	l.Close()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close 之后等待者仍未被唤醒")
	}

	// 关闭之后的请求必须立刻返回，而不是继续慢慢放行。
	start := time.Now()
	l.Request(1 << 20)
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Errorf("Close 之后的请求等了 %v", d)
	}
}

// TestConcurrentRequests 验证并发调用下统计不丢、且不会串行化到不可用。
func TestConcurrentRequests(t *testing.T) {
	const rate = 16 << 20
	l := New(rate)
	defer l.Close()
	resetTokens(l, 0)

	const goroutines = 8
	const per = 256 << 10
	var total int64 = goroutines * per

	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Request(per)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	if got := int64(l.Stats().Bytes); got < total {
		t.Errorf("Stats().Bytes = %d，应当至少 %d", got, total)
	}
	// 2MB 按 16MB/s 约 125ms，留足余量。
	if elapsed > 5*time.Second {
		t.Errorf("2MB 的并发请求花了 %v，限流器串行化过重", elapsed)
	}
}

// TestRateAccessor 覆盖 Rate()，配置摘要里要打这个值。
func TestRateAccessor(t *testing.T) {
	l := New(1234)
	defer l.Close()
	if got := l.Rate(); got != 1234 {
		t.Errorf("Rate() = %v, want 1234", got)
	}
}

// TestConcurrentStatsRace 在 -race 下压一遍 Stats 与 Request 的交错，
// 确保统计字段的读写都在锁内。
func TestConcurrentStatsRace(t *testing.T) {
	l := New(2 << 20)
	defer l.Close()
	resetTokens(l, 2<<20)

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			l.Request(32 << 10)
		}
	}()
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		_ = l.Stats()
	}
	stop.Store(true)
	wg.Wait()
}
