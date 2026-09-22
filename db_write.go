package kvdb

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/xiatianliang1024gm/kvdb/internal/key"
	"github.com/xiatianliang1024gm/kvdb/internal/wal"
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
	err   error  // 由队长在 finishWriteGroup 里填
	done  bool
	// check 非 nil 表示这是 M9 的"提交期校验"批次（WriteChecked）。它不能和
	// 普通批次混在同一段提交里：普通组是"整组同生共死 + 停库"，而 check
	// 失败是正常的事务 abort（只失败自己、不停库）。队长收集队列后会按
	// check 把组切成若干段，每个带 check 的批次独占一段（见 splitChecked）。
	check func() error
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
	// 批次里有 merge 记录但没配算子：写之前就拒绝。这种数据一旦落盘，
	// 读路径折不出值、Manifest 也不会记算子名——是静默的数据不可用。
	if b.hasMerge && db.opts.MergeOperator == nil {
		return ErrNoMergeOperator
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
	db.leadWrites()
	return req.err
}

// WriteChecked 在"提交的同一窗口"里先跑 check 再决定是否写入本批（M9）。
//
// 它是上层实现快照隔离与唯一约束的最小原语：事务读时用 GetForUpdate 记下
// 每个读集 key 的 (key, seq)，提交时把"比对 seq"写进 check。check 返回
// nil 才写入，返回 error 则整批不写 —— 引擎不停库、WAL 不留痕，调用方拿到
// 的 error 就是"校验失败，请重试"。
//
// # 为什么 check 不能放进 db.mu 临界区
//
// check 的本职就是重读读集，而读要拿 db.mu.RLock；如果引擎先锁住 db.mu
// 再跑 check，读集校验就是自己锁死自己。真正的原子性由**队长的独占性**
// 给出：同一时刻只有一个队长在提交，check 通过到本批落库之间，其他写者
// 全部在队列里，没有任何提交能插进来 —— 读写冲突检测要挡的本就是并发写，
// 读者在这个窗口里进不进来无关紧要。
//
// # 与组提交的关系
//
// 带 check 的批次独占一个提交段（不与普通批次合并），check 失败只失败自己：
// 事务 abort 是正常控制流，混进"整组同生共死"的普通组里，一次正常 abort
// 就会演变成停库。代价是事务提交拿不到组提交的 fsync 摊销 —— 上层应当把
// 显式事务提交当低频操作，单语句自动提交照走普通 Write。
//
// check 的 panic 被转成 error 返回（见 runCheck），不会挂住写队列；
// check 为 nil 时退化为普通 Write。空批次与 Write 一致：直接返回 nil，
// 连 check 都不跑 —— 没有写入就没有需要守护的提交。
func (db *DB) WriteChecked(b *WriteBatch, check func() error) error {
	if b == nil || b.Len() == 0 {
		return nil
	}
	// 与 Write 同一条前置校验：批次里有 merge 记录但没配算子，写之前就拒绝。
	if b.hasMerge && db.opts.MergeOperator == nil {
		return ErrNoMergeOperator
	}
	if check == nil {
		return db.Write(b)
	}
	req := &writeRequest{batch: b, check: check}

	db.wmu.Lock()
	if db.wclosing {
		db.wmu.Unlock()
		return ErrClosed
	}
	db.wqueue = append(db.wqueue, req)
	if db.wleader {
		// 已有队长在提交：排进队列等它做到我们这一段。队长会按 check
		// 把组切段，我们的 check 会在它自己的提交段里运行。
		for !req.done {
			db.wcond.Wait()
		}
		err := req.err
		db.wmu.Unlock()
		return err
	}
	db.wleader = true
	db.wmu.Unlock()

	db.leadWrites()
	return req.err
}

// leadWrites 是队长的主循环：反复收集队列、切段提交，直到队列彻底空了才让位。
//
// "要不要让位"的判断在 finishWriteGroup 里与清空 wleader 同一个临界区完成，
// 理由见 Write 里那段"丢失唤醒"的注释。
func (db *DB) leadWrites() {
	for {
		group := db.takeWriteGroup()
		if !db.commitWriteGroup(group) {
			return
		}
	}
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

// takeWriteGroup 是组提交的收集步：把当前队列取空，再用几次"让出调度"把
// 刚刚就绪的写者收进本组（见 collectYields 的注释）。
func (db *DB) takeWriteGroup() []*writeRequest {
	group := db.takeWriteQueue()
	if len(group) == 0 {
		return nil
	}
	for i := 0; i < collectYields; i++ {
		runtime.Gosched()
		if extra := db.takeWriteQueue(); len(extra) > 0 {
			group = append(group, extra...)
		}
	}
	return group
}

// commitWriteGroup 提交一段收集到的队列，并把结论发给其中每个写者。
//
// 返回队长是否还要继续 —— 队列里又有人排着就继续当，没人就让位。
//
// 组里混着带 check 的批次（M9）时，整组不能一把提交：普通组是"整组同生共死
// + 停库"，而 check 失败是正常的事务 abort。所以先把组按 check 切成段，
// 逐段提交、逐段发结论 —— 段与段之间在同一个队长任期内顺序执行，其他写者
// 插不进来，"check 通过 → 本批可见"的原子窗口依然成立。
func (db *DB) commitWriteGroup(group []*writeRequest) bool {
	if len(group) == 0 {
		return db.finishWriteGroup(nil, nil)
	}
	for _, segment := range splitChecked(group) {
		db.completeSegment(segment, db.commitSegment(segment))
	}
	return db.finishWriteGroup(nil, nil)
}

// splitChecked 把收集到的队列按"带 check 的批次"切段：普通批次继续合并成组
// 享受 fsync 摊销，每个带 check 的批次独占一段。段的先后顺序保持入队顺序，
// 所以先入队的普通批次先提交，后到的 checked 批次的 check 能看到它们的写入。
func splitChecked(group []*writeRequest) [][]*writeRequest {
	segments := make([][]*writeRequest, 0, 2)
	var cur []*writeRequest
	flush := func() {
		if len(cur) > 0 {
			segments = append(segments, cur)
			cur = nil
		}
	}
	for _, r := range group {
		if r.check != nil {
			flush()
			segments = append(segments, []*writeRequest{r})
			continue
		}
		cur = append(cur, r)
	}
	flush()
	return segments
}

// commitSegment 提交一个段：分配序列号 → WAL 追加 + 一次 fsync → 落库。
//
// 只有一个写者的 checked 段走 commitChecked（它要先跑 check）；
// 普通段就是原来的三步组提交。
func (db *DB) commitSegment(segment []*writeRequest) error {
	if len(segment) == 1 && segment[0].check != nil {
		return db.commitChecked(segment[0])
	}
	firstSeq, lastSeq, log, err := db.beginWriteGroup(segment)
	if err != nil {
		return err
	}
	err = db.appendWriteGroup(log, segment)
	return db.applyWriteGroup(segment, firstSeq, lastSeq, err)
}

// commitChecked 提交一个带提交期校验的批次（M9）。
//
// check 在 WAL 追加**之前**运行：事务 abort 不该在 WAL 里留下任何痕迹。
// 它刻意**不持 db.mu** —— 此刻队长身份已经把其他写者全部挡在队列里，
// "check 通过 → 本批落库"之间不会有任何其他提交插入，这正是校验需要的
// 原子窗口；而 check 要做的恰恰是重读读集（GetForUpdate），那个读要拿
// db.mu.RLock —— 先把 db.mu 锁住再跑 check，就是自己锁死自己。
//
// check 失败 ⇒ 整批不写、返回该 error、**不停库**：事务 abort 是正常控制流，
// 与"磁盘故障 ⇒ 停库"是两回事。代价是本批独占一次 fsync，拿不到组提交的
// 摊销 —— 上层应当把显式事务提交当低频操作，单语句自动提交照走普通 Write。
func (db *DB) commitChecked(req *writeRequest) error {
	if err := runCheck(req.check); err != nil {
		return err
	}
	group := []*writeRequest{req}
	firstSeq, lastSeq, log, err := db.beginWriteGroup(group)
	if err != nil {
		return err
	}
	err = db.appendWriteGroup(log, group)
	return db.applyWriteGroup(group, firstSeq, lastSeq, err)
}

// runCheck 运行提交期校验，把 check 的 panic 转成 error。
//
// 校验代码是上层逻辑，不能让它把写队列炸挂：队长要是带着 panic 退场，
// wleader 永远不清零，全体写者就永远等不到结论。转成 error 之后这次提交
// 正常失败、队长正常让位，库照常运转（验收要求的"panic 不锁死"由此成立，
// 而且比"靠 defer 释放锁"更进一步 —— 根本没有锁需要释放）。
func runCheck(check func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("kvdb: write check panicked: %v", r)
		}
	}()
	return check()
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

	// 收集本组里的范围墓碑（M7）：它们**不进 MemTable**——读路径只在
	// Version 的全局表上判范围遮蔽，把墓碑塞进 MemTable 反而要给 Flush、
	// 迭代器各加一套处理。它随本组的 WAL 记录落盘（崩溃安全的来源），
	// 并在下面的同一临界区里提交到 Version。
	var newRanges []key.RangeDeletion
	for _, r := range group {
		if err := r.batch.rangeRecords(r.seq, func(seq uint64, kind key.Kind, userKey, value []byte) bool {
			if kind == key.TypeRangeDeletion {
				// rangeRecords 给出的切片指向批次缓冲，Version 会长期持有，复制一份。
				newRanges = append(newRanges, key.RangeDeletion{
					Start: append([]byte(nil), userKey...),
					End:   append([]byte(nil), value...),
					Seq:   seq,
				})
				return true
			}
			db.mem.Add(seq, kind, userKey, value)
			return true
		}); err != nil {
			err = fmt.Errorf("kvdb: apply write batch: %w", err)
			db.failLocked(err)
			return err
		}
	}
	db.lastSeq = lastSeq

	// 范围墓碑在同一临界区里提交到 Version：这个临界区结束（读者可以进来了）
	// 时，"范围内不可见"必须已经生效，否则组提交的原子性对范围删除不成立。
	// Manifest 追加失败 ⇒ 整组失败 + 停库，与其它提交失败同一待遇；
	// WAL 记录还在，重启重放会把墓碑补回来。
	if len(newRanges) > 0 {
		edit := db.baseEdit()
		edit.RangeDeletions = newRanges
		if err := db.logAndApplyLocked(edit); err != nil {
			err = fmt.Errorf("kvdb: commit range deletion: %w", err)
			db.failLocked(err)
			return err
		}
	}

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

// completeSegment 把一个提交段的结论发给段内每个写者。
//
// 它刻意**不动 wleader**：队长可能还有后续段要提交，而 finishWriteGroup 的
// "队列空 ⇒ 让位"一旦在段间触发，新写者就会在上一任队长还在飞的时候抢走
// 队长身份，两边并发分配序列号 —— 同一个 seq 会被写两次（MemTable 的重复
// internal key panic 就是这么来的）。队长去留只允许在全部段都提交完之后，
// 由 commitWriteGroup 末尾的 finishWriteGroup 判定一次。
func (db *DB) completeSegment(segment []*writeRequest, err error) {
	db.wmu.Lock()
	for _, r := range segment {
		r.err = err
		r.done = true
	}
	db.wcond.Broadcast()
	db.wmu.Unlock()
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
