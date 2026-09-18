package kvdb

import (
	"errors"
	"fmt"
	"runtime"

	"kvdb/internal/key"
	"kvdb/internal/wal"
)

// 这个文件是 M4 的核心：**组提交（Group Commit）**。
//
// 写路径的成本几乎全在一次 fsync 上（见 docs/DESIGN.md §3），所以"把多少个
// 并发写请求合并进一次 fsync"直接决定了并发写的吞吐上限。把这件事做成
// "写者排进一条队列，队首那个写者替全组做一次 fsync"，就得到了组提交。
//
// 三个不能搞混的点：
//
//  1. **分号在前，落盘在中，落库在后。** 序列号写在 WAL 记录头里，必须最先定；
//     但 db.lastSeq（读者的快照水位）要等到数据真的进了 MemTable 才抬 ——
//     提前抬会让读者看到一个"还不存在的已提交点"。
//  2. **fsync 全程不持 db.mu。** 读者不该为写者的磁盘等待买单。这也是为什么
//     写队列用自己的一把锁（DB.wmu）而不是复用 db.mu。
//  3. **整组同生共死。** 组里任意一个批次出问题，整组返回同一个错误，并且
//     把库停下来。理由见 Write 的注释。

// writeRequest 是一次排进提交队列的写入。
//
// 它把"调用方的批次"和"这一批的结论"绑在一起：队长提交完整组之后，把同一个 err
// 写进组里每一个 request 再统一唤醒。于是同一组里的写者拿到的结论必然一致。
type writeRequest struct {
	batch *WriteBatch
	seq   uint64 // 起始序列号，由队长分配
	err   error  // 由队长在 completeWriteGroup 里填
	done  bool
}

// Write 原子地写入一个批次，是引擎唯一的写入口。
//
// Put / Delete 都只是它的语法糖。原子性来自"整个批次编码成一条 WAL 记录"：
// 恢复时要么整条重放成功，要么整条被丢弃，不存在只应用一半的情况。
//
// # 组提交
//
// 并发写者在这里排队，由队首的那个写者（队长）把当前队列里的**所有**批次
// 合并成一次 WAL 追加与**一次 fsync**，再把整组的结论发给每一个写者。
// 于是"平均每次 fsync 服务了多少个批次"就成了并发写吞吐的倍数：
// 32 个并发写者拿到接近 32 倍的单次 fsync 摊销（实测见 docs/DESIGN.md §9.20）。
//
// 队长只在两个极短的临界区里持 db.mu（分配序列号、把整组落进 MemTable），
// 最贵的 fsync 完全在锁外完成 —— 所以组提交既不挡读，也不挡后来的写者：
// 它们在队列里等着，正好凑成下一组，组的大小自动跟着并发度走。
//
// # 失败的语义
//
// 返回 nil 表示这一批已经落盘（SyncWrites 为真时）并且对后续读可见。
// 返回错误分两类，含义不同：
//
//   - ErrClosed 与之前已经存在的后台错误：这一批**一定没有**写进去。
//   - 磁盘故障（追加或 fsync 失败）：这一批**可能**已经写进去了，而且整个库
//     会被停下来，后续写入一律返回同一个错误。组提交里靠前的批次可能已经落盘，
//     "返回了错误但其实写成功了"是比崩溃更难查的一致性缺口，宁可停库。
//     此时重新打开数据库即可 —— WAL 始终是唯一的事实来源。
func (db *DB) Write(b *WriteBatch) error {
	if b == nil || b.Len() == 0 {
		return nil
	}
	req := &writeRequest{batch: b}

	db.wmu.Lock()
	if db.wclosing {
		db.wmu.Unlock()
		return ErrClosed
	}
	db.wqueue = append(db.wqueue, req)
	if db.wleader {
		// 已经有一位队长在提交：排进它这一组，等它把整组做完。
		//
		// 这里用条件变量而不是超时或轮询，是因为队长的结论是必然会来的：
		// 无论成功失败，它都会走 completeWriteGroup 把整组唤醒。
		for !req.done {
			db.wcond.Wait()
		}
		err := req.err
		db.wmu.Unlock()
		return err
	}
	db.wleader = true
	db.wmu.Unlock()

	// 队长一直干到队列真的空了为止，然后才发结论。
	//
	// 这里**不能**是"提交一组、让位、再看队列"：队长把 wleader 放下去的瞬间，
	// 队列里可能还留着刚刚排进来的写者 —— 它们看到的 wleader 已经是 false，
	// 于是谁也不会去提交它们，那批写者就永远等不到 done。
	// （这个丢失唤醒实测会稳定挂死，栈上表现为一堆 goroutine 卡在 wcond.Wait
	// 而场上没有队长。）所以"还要不要继续当队长"必须和"清空 wleader"
	// 放在同一个 wmu 临界区里判断。
	for {
		group, err := db.runWriteGroup()
		if !db.finishWriteGroup(group, err) {
			break
		}
	}
	return req.err
}

// collectYields 是队长在开始 fsync 之前"让出调度"的次数，用来把**已经就绪**的
// 写者收进本组。
//
// 为什么需要它：队长是由"第一个到达的写者"担任的，而它到达的那一刻队列里往往
// 只有它自己 —— 上一组刚被放行的写者还没来得及重新入队（它们要走完
// 唤醒 → 返回 Write → 返回 Put → 再调用 Write → 抢 wmu → 入队这几步）。
// 于是每组会退化成"1 个 + (N-1) 个"交替，一半的 fsync 浪费在只有队长自己的那一组上。
//
// 让出几次调度等价于"让已经就绪的写者先跑一步"：不睡眠、不等固定时长，
// 代价是几微秒，相对一次毫秒级的 fsync 可以忽略。实测（32 个并发写者、
// 每档 3000 次写）把合并率从 15.9 提到 22.4~27.8，即吞吐再涨四到七成。
const collectYields = 4

// runWriteGroup 是组提交的实现：把当前队列合并成一次 WAL 追加 + 一次 fsync，
// 然后落进 MemTable。
//
// 返回整组的结论与参与本组的写者列表 —— 发结论是调用方的事，因为"谁是队长"
// 由 Write 决定，这里只负责提交。
func (db *DB) runWriteGroup() ([]*writeRequest, error) {
	group := db.takeWriteQueue()
	if len(group) == 0 {
		return nil, nil
	}
	for i := 0; i < collectYields; i++ {
		runtime.Gosched()
		if extra := db.takeWriteQueue(); len(extra) > 0 {
			group = append(group, extra...)
		}
	}

	// ① 分配序列号 + 固定日志句柄。
	//
	// 序列号就写在 WAL 记录头里，必须先定下来；但这时**不能**抬高 db.lastSeq ——
	// 它同时是读者的快照水位，提前抬高会让读者看到一个尚未落地的"已提交点"。
	// 反正同一时刻只有这一组在飞，db.lastSeq 停在组前是安全的。
	firstSeq, lastSeq, log, err := db.beginWriteGroup(group)
	if err != nil {
		return group, err
	}

	// ② 追加 + fsync。整条写路径里最贵的一步，刻意放在 db.mu 之外：
	//    读者在这一步里完全不受影响。
	//
	//    这里不必担心 db.log 被换掉 —— 换日志只发生在 freezeLocked 里，
	//    而 freezeLocked 只由队长调用，队长同一时刻只有一个。
	err = db.appendWriteGroup(log, group)

	// ③ 落盘成功才动内存：读者看到的每一个序列号都必须是已经持久化的。
	return group, db.applyWriteGroup(group, firstSeq, lastSeq, err)
}

// beginWriteGroup 为整组分配连续的序列号，并固定这一组要写的那份日志。
//
// 返回值 firstSeq / lastSeq 是整组的序列号区间，log 是本组要追加的日志句柄。
func (db *DB) beginWriteGroup(group []*writeRequest) (firstSeq, lastSeq uint64, log *wal.Log, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, 0, nil, ErrClosed
	}
	if db.bgErr != nil {
		return 0, 0, nil, db.bgErr
	}

	seq := db.lastSeq + 1
	firstSeq = seq
	for _, r := range group {
		r.seq = seq
		r.batch.SetSequence(seq)
		seq += uint64(r.batch.Len())
	}
	return firstSeq, seq - 1, db.log, nil
}

// appendWriteGroup 把整组批次依次追加到 WAL，并且只 fsync 一次。
//
// 每个批次仍然占一条独立的 WAL 记录：这是"批次内的原子性"所依赖的边界，
// 组提交合并的是 fsync，不是记录。
func (db *DB) appendWriteGroup(log *wal.Log, group []*writeRequest) error {
	var bytes int64
	for i, r := range group {
		record := r.batch.Encode()
		if err := log.Append(record); err != nil {
			return fmt.Errorf("kvdb: append batch %d/%d to the wal: %w", i+1, len(group), err)
		}
		bytes += int64(len(record))
	}
	if db.opts.SyncWrites {
		if err := log.Sync(); err != nil {
			return fmt.Errorf("kvdb: sync the wal: %w", err)
		}
		db.counters.walSyncs.Add(1)
	}
	db.counters.walBytes.Add(uint64(bytes))
	return nil
}

// applyWriteGroup 把已经落盘的整组写入落进 MemTable，并推进序列号水位。
//
// 整组在**同一个 db.mu 临界区**里应用，这是"批次原子性"在并发下的表现形式：
// 读者要么看到 lastSeq 停在组前（整组都不可见），要么看到组后（整组都可见），
// 绝不会读到"组里一半已经可见"的中间态。
func (db *DB) applyWriteGroup(group []*writeRequest, firstSeq, lastSeq uint64, walErr error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	switch {
	case db.closed:
		return ErrClosed
	case walErr != nil:
		// 写日志失败意味着磁盘已经不可信。整组一律失败，而且整个库要停下来：
		// 组里靠前的批次可能已经进了 WAL，让调用方以为"没写进去"是危险的。
		err := fmt.Errorf("kvdb: write group: %w", walErr)
		db.failLocked(err)
		return err
	case db.bgErr != nil:
		return db.bgErr
	}

	for _, r := range group {
		if err := r.batch.Range(r.seq, func(seq uint64, kind key.Kind, userKey, value []byte) bool {
			db.mem.Add(seq, kind, userKey, value)
			return true
		}); err != nil {
			err = fmt.Errorf("kvdb: apply write batch: %w", err)
			db.failLocked(err)
			return err
		}
	}
	db.lastSeq = lastSeq

	// 组提交的规模指标。只有队长会走到这里，所以不需要 CAS。
	n := int64(len(group))
	db.counters.writeGroups.Add(1)
	db.counters.writeBatches.Add(n)
	if cur := db.counters.maxWriteGroup.Load(); n > cur {
		db.counters.maxWriteGroup.Store(n)
	}

	// 写满就冻结。注意返回值的含义：数据已经落盘、也已经可读，
	// 但冻结失败意味着库没法继续接受写入了 —— 记进 bgErr 让后续写入
	// 拿到同一个错误，而不是让 MemTable 无限涨下去。
	if db.mem.ApproximateSize() >= int64(db.opts.MemTableSize) {
		if err := db.freezeLocked(); err != nil {
			// freezeLocked 也可能返回 ErrClosed 或已经存在的 bgErr，
			// 那两种情况不该被包装成一个新的故障。
			if !errors.Is(err, ErrClosed) && db.bgErr == nil {
				db.failLocked(fmt.Errorf("kvdb: freeze memtable: %w", err))
			}
			return err
		}
	}
	return nil
}

// takeWriteQueue 取走当前排队的全部写入。
//
// 队长开工前"一次性取空"，之后到来的写者自然组成下一组 —— 组的大小因此
// 自动跟着并发度走，不需要任何启发式或延迟窗口。
func (db *DB) takeWriteQueue() []*writeRequest {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	group := db.wqueue
	db.wqueue = nil
	return group
}

// finishWriteGroup 发掉这一组的结论，并告诉队长还要不要继续。
//
// 返回值 more 为真表示"队列里已经又有人排着了，你继续当队长"。
//
// 顺序与判断都必须在这一个 wmu 临界区里完成，理由有两条：
//
//  1. **先写好 done 再广播**。反过来的话，被唤醒的写者可能读到 done == false
//     又回去睡，而此刻已经没有人会再唤醒它 —— 那种挂起不可恢复。
//  2. **"队列非空"与"清空 wleader"必须同时判断**。否则队长会在队列还有人的
//     情况下让位，那批写者就再也没人提交了。由此还得到一个有用的不变式：
//     `len(wqueue) > 0 ⟹ wleader` —— 队列里有东西就一定有队长。
func (db *DB) finishWriteGroup(group []*writeRequest, err error) (more bool) {
	db.wmu.Lock()
	for _, r := range group {
		r.err = err
		r.done = true
	}
	more = len(db.wqueue) > 0
	if !more {
		db.wleader = false
	}
	db.wcond.Broadcast()
	db.wmu.Unlock()
	return more
}

// drainWrites 关掉写队列并等当前这一组提交完。它是 Close 的第一步。
//
// 为什么必须排在 db.mu 之前：队长在后半段要拿 db.mu（把整组落进 MemTable），
// 而 Close 一旦先拿住 db.mu 再等队长，两边就是互相等待。
//
// 为什么要**先关门再等**：在持续写入的负载下，队长会一直有人可提交，队列永远不空，
// 于是"等队长停下来"可能永远等不到。先把新写者挡在外面，队列就只会变短，
// 队长一定能停。
func (db *DB) drainWrites() {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	db.wclosing = true
	for db.wleader {
		db.wcond.Wait()
	}
}
