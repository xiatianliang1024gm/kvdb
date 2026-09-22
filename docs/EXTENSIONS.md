# kvdb 上层扩展设计

> 为在 kvdb 之上封装**关系型存储引擎**与 **Redis 协议存储**所需的能力增补。
>
> 本文只写"还缺什么、怎么补"；**kvdb 现有的功能以 `docs/DESIGN.md` 为准**，两份文档不重叠。
>
> 状态：M6（§4.2 CompactionFilter）、M7（§4.1 DeleteRange + §5.1 半开上界/Prefix）、
> M8（§4.3 Merge 算子）与 M9（§4.4 提交期校验原语）已实施 ｜ 最后更新：2026-09-22

## 目录

- [1. 这份文档解决什么](#1-这份文档解决什么)
- [2. 现有对外契约盘点](#2-现有对外契约盘点)
- [3. 两个上层的形态](#3-两个上层的形态)
- [4. 必做：四项能力](#4-必做四项能力)
- [5. 应做：性能与易用性](#5-应做性能与易用性)
- [6. 约束与已知风险](#6-约束与已知风险)
- [7. 明确不做](#7-明确不做)
- [8. 实施顺序](#8-实施顺序)
- [附录 A：API 增补清单](#附录-aapi-增补清单)
- [附录 B：两个上层的键编码示例](#附录-b两个上层的键编码示例)
- [附录 C：关键设计决策](#附录-c关键设计决策)

---

## 1. 这份文档解决什么

kvdb 现在承诺的语义是：**单键读、单键写、批量原子写、有序区间扫描、快照读**。

这恰好是"关系型引擎"与"Redis 协议存储"两个上层的**公共子集**——它们都能在上面跑起来，但各自都有一部分需求落在子集之外。本文要回答的是：子集之外的部分，哪些必须引擎补，哪些上层自己能解决，补的时候会碰到哪一行代码。

一个总判断先摆出来：**这两类上层对引擎的要求高度重合**。它们都把"一个逻辑单元"（一张表 / 一个大 key）编码成**一批共享前缀的子键**，于是"按前缀整段删除"和"按前缀枚举"同时成为两者的刚需；它们都需要"过期的/作废的数据能真正从磁盘消失"；它们都需要某种形式的**读-改-写原子性**。这决定了下面四项 P0 里，有三项是两个上层共用的。

---

## 2. 现有对外契约盘点

`api.go` 里声明的三个接口就是全部对外能力，`Open` 返回 `*DB`（具体类型），其余入口散在根包：

| 分组 | 入口 |
|---|---|
| 生命周期 | `Open(Options)` / `DB.Close()` |
| 写 | `Put` / `Delete` / `Write(*WriteBatch)`（**唯一写入口**） |
| 读 | `Get` / `NewIterator(*IteratorOptions)` |
| 快照 | `GetSnapshot()` → `Snapshot{Seq, Get, NewIterator, Release}` |
| 运维 | `Stats()` / `RecoveryReport()` / `Checkpoint(dir)` |

由此推出的几个事实，是后面所有讨论的前提：

- **记录类型只有两种**：`key.TypeDeletion = 0`、`key.TypeValue = 1`（`internal/key/internal_key.go:26-29`）。没有 Merge、没有范围墓碑。
- **批次只有 Put / Delete 两种记录**（`batch.go:83` 的 `addRecord`），且 `decodeBatch` 对 kind 做白名单校验（`batch.go:180`）。
- **迭代器只支持前向**：`SeekToFirst` / `Seek` / `Next`，无 `Prev` / `SeekForPrev` / `SeekToLast`。
- **边界是双闭区间**：`IteratorOptions.LowerBound` / `UpperBound` 均为**含**（`db_iter.go:15-22`）。
- **没有 Compaction 钩子**：`compact.Env`（`internal/compact/run.go:33`）里没有 filter 字段，`Run` 的输出点只有一处（`run.go:231`）。
- **没有手动触发**：`Flush` / `Sync` / `Compact` 都是包内私有（`notifyFlush` / `notifyCompact`），对外没有。
- **`Get` 每次都复制值**（`db.go:594` 的 `copyValue`），且每次都拿一次 `db.mu.RLock()`（`db.go:583`）。

---

## 3. 两个上层的形态

这一节不是设计，是**约束的来源**：上层怎么编码键，决定了引擎要提供什么。

### 3.1 Redis 协议存储

Redis 的数据模型对存储层提出三个要求，其中前两个是硬要求：

1. **大集合必须拆成子键。** 一个 1000 万元素的 list 若编码成单个 value，每次 `LPUSH` 都要重写 100MB——写放大与元素数成正比，且这条记录会撑出一个 100MB 的 Data Block（同一 user key 的所有版本必须在同一块内，见 DESIGN §4.1 硬约束 1）。正确做法是 `key + 分隔符 + 元素ID` 拆成独立子键。于是 `HGETALL` / `LRANGE` / `SCAN` 全部退化成**前缀扫描**，`DEL` 一个大 key 退化成**前缀删除**。
2. **TTL 必须能被物理清除。** `EXPIRE` 打在每个 key 上，过期数据是持续产生的。只做"读的时候惰性判断"是不够的：磁盘上的死数据永远不消失，老层文件里塞满过期键，读放大只增不减。
3. **`INCR` 是热路径且必须原子。** 没有 Merge 算子时它只能是读-改-写：并发下要么加锁串行（把写路径打回单线程），要么乐观重试（热点计数器上退化成重试风暴）；而且每次自增都重写整个 value。

### 3.2 关系型存储引擎

1. **元组键是复合的。** `表ID + 主键列...`，可插拔 `Comparer` 正好承接；二级索引是 `索引ID + 索引列... + 主键`，维护索引 = "一行 + N 个索引项"写进**同一个 WriteBatch**——这个能力现有引擎已经具备。
2. **DDL 与批量 DELETE 是"整段"操作。** `DROP TABLE` / `TRUNCATE` 面对的是整个表前缀。没有范围删除时，清表要写与行数等量的墓碑，成本线性于数据量。
3. **事务需要"校验并提交"的原子性。** WriteBatch 给了多键原子可见、Snapshot 给了一致读，但"在提交的同一临界区里检查我的读集有没有被改过"这件事没有原语——这是快照隔离与唯一约束的唯一缺口。

### 3.3 公共形状

两者都把**逻辑单元 = 前缀**。这条共同点直接决定了 4.1（范围删除）与 5.1（前缀扫描）是两者共用的；而 4.2（过期清除）主要服务 Redis，4.4（提交校验）只服务 SQL，4.3（Merge）主要服务 Redis、顺带服务 SQL 的计数器列。

---

## 4. 必做：四项能力

这一节的四项，缺任何一项上层都要么做不下去，要么得付一个数量级的代价去绕。

### 4.1 DeleteRange（范围删除）

**需求**：SQL 的 `DROP TABLE` / `TRUNCATE` / 带条件的批量 `DELETE`；Redis 的 `DEL` 大 key、`FLUSHDB`（一个 db 前缀）。

**现状缺口**：`key.Kind` 只有两种（`internal_key.go:26-29`）；`VersionEdit` 只有文件级的 Added / Deleted（`internal/version/edit.go:33-35`）；`compact.Run` 的丢弃判据只有 `covered` + `SmallestSnapshot` + `isBaseLevel` 三条（`run.go:207-234`）。

**设计**

```
范围墓碑 = (Start, End, Seq)    区间语义：半开 [Start, End)
```

- **区间一律半开。** 这不是偏好而是必需：范围墓碑要能表达"从 `users:` 到下一个前缀之前"，闭区间做不到——你找不到"该前缀下最后一个键"的后继。这条同时推动 5.1 把 `IteratorOptions.UpperBound` 改成半开：**两套区间语义并存是 bug 温床**，不如一次统一。
- **范围墓碑挂在 `Version` 上做成一张全局有序表，不按文件存。** RocksDB 是 per-file 的，代价是每个文件都带一份、点查命中文件后才能判定；全局表的点查成本是 `O(log 范围数)`，而范围数等于"执行过的范围删除次数"，通常是几十量级。**用一点常数开销换掉一整层 per-file 状态**，在这个量级上是划算的取舍。
- **范围墓碑必须参与读路径判据。** 不能只在 Compaction 生效——那样删完还能读到，是正确性错误。判定规则：某 key 的可见版本 `seq < 覆盖它的范围墓碑的 seq` ⇒ 不可见。
- **退休条件复用现有的 `isBaseLevel`。** 范围墓碑只有在"输出层以下不再有该区间的数据"时才能丢弃，这与单键墓碑的退休判据完全同构（`run.go:221`），直接沿用即可，**不要另写一套**。
- **与快照**：范围墓碑的 seq > 快照 seq 时，对快照不可见（快照仍能读到旧数据）。这与单键墓碑的处理方式一致。
- **Compaction 侧**：`covered` 判定之前加一条"被范围墓碑盖住 ⇒ 整段丢弃"。这是范围删除唯一真正回收空间的时刻。
- **Flush 侧不处理**。MemTable → L0 时不做范围判定：它在前台关键路径上（决定 MemTable 能否释放），而 MemTable 里的记录反正马上会被 Compaction 处理。**推迟到 Compaction 是免费的。**

**落点**

| 文件 | 改动 |
|---|---|
| `internal/key/internal_key.go` | 新增 `TypeRangeDeletion Kind = 3`；`Kind.String()`；`DecodeInternalKey` / `ParseInternalKey` 白名单放行 |
| `batch.go` | `WriteBatch.DeleteRange(start, end)`；`addRecord` 支持"非 Value 记录也带 value"；`decodeBatch` 白名单放行 |
| `internal/version/edit.go` | `VersionEdit` 加 `RangeDeletions` / `RetiredTombstones`；新增 tag（编号沿用 LevelDB 风格，取 10 / 11） |
| `internal/version/version.go` | `Version` 持有范围墓碑有序表；`RangeCovers(userKey, seq) bool` |
| `internal/compact/run.go` | `Env` 增加范围墓碑来源；主循环在 `covered` 判定前加遮蔽分支；输出时用 `isBaseLevel` 判退休 |
| `db.go` | `getLocked` 命中后调 `v.RangeCovers` |
| `internal/iterator/dbiter.go` | `findNextUserEntry` 同样遮蔽 |

**验收**

- 删一个范围后，范围内所有键 `Get` → `ErrNotFound`；范围外不受影响。
- 范围删除对**之前取的快照**不可见。
- 触发一次覆盖全范围的 Compaction 后磁盘字节数下降，且范围墓碑退休。
- 崩溃重启后范围墓碑仍在（Manifest 恢复）。
- **更深层还有数据时范围墓碑不得退休**——与 `TestRunKeepsTombstoneAboveDeeperLevel` 同构，这是最容易写错的一点。

**不做**：不支持撤销一次范围删除；不优化大量重叠范围的最坏情况；不保证删除后立刻回收空间（回收时机由 Compaction 决定）。

---

### 4.2 CompactionFilter（Compaction 过滤器）

**需求**：Redis TTL 的物理清除；SQL 的行级过期 / 租户清理。

**现状缺口**：`compact.Env` 无钩子，`Run` 只有一个输出点（`run.go:231`），`flushMemTable` 只有一个输出点（`db_flush.go:451`）。

**设计**

```go
type CompactionFilter interface {
    // Name 写进 Manifest 用于校验目录与配置是否匹配，语义同 Comparer.Name。
    Name() string
    // Filter 判定一条记录是否还要留下。level 是**输出层**，seq 是记录序列号。
    // 返回 error ⇒ 本次 Compaction 整体失败并停库（验收要求，签名因此带 error）。
    Filter(level int, userKey, value []byte, seq uint64) (Decision, error)
}

type Decision int  // Keep / Drop（先不做 Change）
```

四条关键决策：

- **只在该记录是"当前 user key 的最新可见版本"时调用。** 落点精确在 `run.go` 的 `covered == false` 且通过 `SmallestSnapshot` 判定之后、`out.add` 之前（`run.go:219-231`）。对已被覆盖的旧版本调 filter 毫无意义，且会让调用次数从"键数"涨到"记录数"。

- **filter 说 Drop，等于把它转成墓碑语义。** 直接跳过是不够的：更深层可能还躺着一个更旧版本，跳过本层会让旧值"复活"。所以 Drop 的正确处理是——**沿用现有的 `base.isBaseLevel(uk)` 判据**（`run.go:221`）：是 base level 就整条丢掉，否则写一条墓碑继续遮蔽。这一处直接复用 M3 已经验证过的逻辑，是整份设计里最省力的一环。

- **filter 只对 `seq <= SmallestSnapshot` 的记录生效。** 这是它与快照隔离的**唯一自洽方式**：seq 更大的记录还可能被某个存活快照读到，丢掉它就是破坏快照隔离。**代价必须写清楚**：一个长期存活的老快照会让 filter 大面积失效——这与"快照钉住 GC"（见 6 节）是同一件事的两个面，上层必须把快照当稀缺资源管理。

- **Flush 侧默认不挂 filter。** 新增 `Options.FilterOnFlush bool`，默认 false。理由：Flush 在前台关键路径上，把用户回调插在那里等于把引擎的延迟暴露给用户代码；而且 Flush 丢弃后如果紧接着删 WAL 失败，数据会从 WAL 重放回来——**这是安全的**（过滤是幂等的，重放只让数据临时回来），但语义上不如只在后台生效干净。

配套两条：

- `Name()` 写进 Manifest 校验，机制复用 `ComparatorName`（`edit.go:18-20`）。换一个 filter 语义读老目录，会导致"该丢的没丢、不该丢的丢了"，与换比较器同级别的事故。
- filter 在后台线程被调用，**引擎不保证调用顺序、不保证同一条记录只被调用一次、要求实现并发安全**。filter 返回 error ⇒ 该次 Compaction 整体失败，`outputSet.abort()` 兜底（`run.go:401`），与现有"Compaction 失败即停库"一致。

**落点**

| 文件 | 改动 |
|---|---|
| `options.go` | `Options.CompactionFilter` / `Options.FilterOnFlush` |
| `internal/compact/run.go` | `Env.Filter`；主循环在 `out.add` 前插判定；Drop 分支复用 `isBaseLevel` |
| `db_compact.go` | 构造 `compact.Env` 时透传（`db_compact.go:63`） |
| `db_flush.go` | `FilterOnFlush` 为真时在 `w.Add` 前插判定（`db_flush.go:451`） |
| `internal/version/edit.go` | `VersionEdit` 加 `FilterName`（第一条记录写入 + 重放校验） |

**验收**

- 一个"丢掉 value 首字节为 0xFF 的所有记录"的 filter，跑完 Compaction 后这些 key 全部 `Get` 不到。
- **更深层有旧版本时不会复活**（核心正确性）。
- 存活快照期间 filter 不生效；快照 Release 后再 Compaction，数据被清除。
- filter 报错 ⇒ Compaction 失败、输出文件被清理、库停下。
- 换一个 `Name()` 打开同一目录 ⇒ 报配置不匹配。

---

### 4.3 Merge 算子

**需求**：Redis 的 `INCR` / `DECR` / `HINCRBY` / `APPEND` / `SETBIT`；SQL 的自增序列、计数器列。

**现状缺口**：kind 只有两种；`db.getLocked` 命中第一条 `seq <= snapshot` 就返回（`db.go:604-645`）；`DBIter.findNextUserEntry` 的 switch 只有两个 case（`dbiter.go:146-157`）；`compact.Run` 的 `covered` 不看 kind。

**Kind 编号必须一次定死**（两个特性会争抢同一个编号）：

```
0 = TypeDeletion      1 = TypeValue
2 = TypeMerge         3 = TypeRangeDeletion
```

编号一旦写进磁盘就冻结，所以 4.1 与 4.3 无论谁先做，都要按这张表占位。

**设计**

```go
type MergeOperator interface {
    Name() string
    // FullMerge 把 base 与一串 operand 折叠成一个值；base 为 nil 表示没有 base。
    FullMerge(userKey, base []byte, operands [][]byte) ([]byte, error)
    // PartialMerge 折叠两个 operand。ok=false 表示无法局部合并，
    // 引擎会原样保留两个 operand 等 FullMerge 处理。
    PartialMerge(userKey, a, b []byte) (merged []byte, ok bool)
}
```

- **读路径变贵是 Merge 的固有代价。** 命中 TypeMerge 后不能立刻返回，要继续向下收集同 key 的全部 operand，直到遇到 `TypeValue` / `TypeDeletion` 或无更多版本，再按 seq **从旧到新** FullMerge。稳态下这个代价由 Compaction 抵消——**所以 Compaction 侧的折叠才是这个特性的收益所在，不是可选项**。
- **Compaction 折叠**：同 key 的连续 Merge 记录用 `PartialMerge` 折叠；遇到 base（`TypeValue`）就 `FullMerge` 出一条 TypeValue。稳态下每个 key 只剩一条 Value。
- **`covered` 逻辑必须改**：现在是"遇到 `seq <= SmallestSnapshot` 的第一条就 `covered = true`，之后同 key 全丢"（`run.go:219`）。有 Merge 之后这条不成立——Merge operand 必须收集齐才能丢。**新规则：`covered` 只在遇到 `TypeValue` / `TypeDeletion` 时置位。** 这是实现上最容易写错的一处。
- **Seek 定位边界必须跟着改**：`SeekKey` 的定位尾缀原来是 `(snapshot, TypeValue)`，在只有 Value（1）/ Deletion（0）时它是"seq <= snapshot 的最大尾缀"；TypeMerge（2）比它大——与定位点**同序列号**的 merge 记录会被 Seek 静默跳过，读到旧版本。M8 起定位尾缀取 kind 字段的上界（`0xFF`），对任何未来的新类型都成立。（RocksDB 为同一原因把 `kValueTypeForSeek` 定成 `kTypeMerge`。）
- **要求算子满足结合律**，否则禁止实现 `PartialMerge`（只能返回 `ok=false`）。理由：`PartialMerge` 的结果会再参与后续折叠，不满足结合律时会与 `FullMerge` 的结果不一致，而引擎无法替上层发现这件事。
- **filter 看到的是折叠后的值**：先折叠成 TypeValue 再交给 filter，让 filter 不必理解 operand 语义（4.2 与 4.3 都做时，顺序是 Merge 折叠 → filter 判定）。
- `Name()` 同样写进 Manifest 校验。

**落点**

| 文件 | 改动 |
|---|---|
| `internal/key/internal_key.go` | `TypeMerge Kind = 2`；`String()`；解码白名单放行 |
| `options.go` | `Options.MergeOperator` |
| `batch.go` | `WriteBatch.Merge(userKey, operand)`；`decodeBatch` 白名单放行 |
| `db.go` | `getLocked` 命中 Merge 后向下收集 operand 并 FullMerge |
| `internal/iterator/dbiter.go` | `findNextUserEntry` 加 Merge 分支（收集 + 折叠） |
| `internal/compact/run.go` | `Env.MergeOperator`；`covered` 规则改；折叠逻辑 |
| `internal/version/edit.go` | `VersionEdit` 加 `MergeOperatorName` 校验 |

**验收**

- 连续 1000 次 `Merge(+1)` 后读回正确值。
- 之后触发 Compaction，值仍正确，且 `Stats.Compaction.OutputBytes` 接近单条 Value 的量级。
- 快照点之后的 merge 对快照不可见。
- `Merge → Delete → Merge` 序列结果正确。
- 经 WAL 重放后（崩溃重启）merge 结果仍正确。

---

### 4.4 提交期校验原语

**需求**：SQL 的快照隔离提交、唯一约束。**这一项只服务关系型上层。**

**现状缺口**：已经具备"多键原子可见"（WriteBatch）与"一致读"（Snapshot），但**没有"在提交的同一临界区里校验读集未被改动"**。用现有 API 只能凑出"上层加一把提交锁 → 锁内重读读集比对 → 再 `Write`"，能关掉窗口，代价是每次提交 `O(读集 × value 大小)`，且"改了又改回来"检测不出来。

**设计：两个最小原语**

```go
// GetForUpdate 与 Get 语义相同，额外返回可见版本的序列号。
// seq == 0 表示 key 不存在——"不存在"同样是一个可校验的状态。
func (db *DB) GetForUpdate(userKey []byte) (value []byte, seq uint64, err error)

// WriteChecked 在持有写锁的临界区里先跑 check，再决定是否写入本批。
// check 返回 error ⇒ 整批不写入，且**不停库**（事务 abort 不是故障）。
func (db *DB) WriteChecked(b *WriteBatch, check func() error) error
```

- **`GetForUpdate` 的 seq 是校验的关键**：上层事务读时记下 `(key, seq)`，提交时在 `check` 里比对 seq。校验从"重读并比对字节"降成"比一个 uint64"，而且能检测"改了又改回来"（seq 必然递增）。seq 是每次写入单调递增的，同一 key 的版本 seq 唯一，所以"seq 相同 = 版本相同"是可靠的。
- **`check` 放在 WAL 追加之前。** 事务回滚不该在 WAL 里留下任何痕迹。
- **`WriteChecked` 的批次不与普通批次合并，独占一个提交段。** 理由：现有语义是"整组同生共死，任一批次出错 ⇒ 整组失败 + 停库"（`db_write.go:215-226`），而事务 abort 是正常控制流，不是故障。把带 check 的批次混进普通组，会让一次正常 abort 演变成停库。**代价必须写明：事务提交拿不到组提交的 fsync 摊销。** 这也意味着上层应当把"显式事务提交"当低频操作，把"单语句自动提交"当高频操作（后者走普通 `Write`，照常享受摊销）。
- **实施注记（M9）**：独占段做成了"组内切段"——队长收集队列后按 check 把组切成若干段（`splitChecked`），普通段照旧合并提交，每个 checked 批次独占一段，段间在同一队长任期内顺序执行。这比"整个批次单独排队"保住了普通写者的 fsync 摊销，等价于原设想里留作后续的"组内混排、失败的批次单独 abort 而整组继续"。
- **check 运行时不持 `db.mu`**（原设想的"beginWriteGroup 的 db.mu 临界区内跑 check"不成立）：check 的本职是重读读集（`GetForUpdate` 要拿 `db.mu.RLock`），先锁 `db.mu` 再跑 check 就是自己锁死自己。原子窗口改由**队长的独占性**给出——同一时刻只有一个队长在提交，"check 通过 → 本批落库"之间其他写者全在队列里；读者在此窗口进不进来与写写冲突检测无关。
- **check 的 panic 转成 error 返回**（`runCheck`）：队长若带着 panic 退场，`wleader` 永不清零，全体写者永远等不到结论。转成 error 后这次提交正常失败、队长正常让位，比"靠 defer 释放锁"更强——根本没有锁需要释放。
- **不进 `api.go` 的 `Reader` / `Writer` 接口。** 那三个接口是"抽象 kvdb 的最小面"，事务原语是 kvdb 特有的能力，塞进去会抬高所有替代实现（测试假库、装饰器）的门槛。

**这不是在引擎里实现事务。** 隔离级别、读写集管理、回滚、死锁检测全在上层；引擎只提供"原子地校验并提交"这一个原语。这与 DESIGN §8「不做事务隔离级别 / SSI」不冲突——那里拒绝的是"引擎实现隔离级别"，这里做的是"让上层能自己实现"。

**落点**

| 文件 | 改动 |
|---|---|
| `db.go` | `GetForUpdate`（复用 `seekVisibleLocked` → 范围遮蔽 → merge 折叠的既有链路，取命中 seq） |
| `db_write.go` | `WriteChecked`：批次带 check 入队；队长切段（`splitChecked`/`commitSegment`/`commitChecked`），checked 段先跑 check（不持 `db.mu`，见实施注记），失败只失败自己、不停库 |

**验收**

- 两个并发事务改同一 key：只有一个成功，另一个 check 失败且数据未被修改。
- check 失败后 `Stats.WALBytes` 不增长——直接验证"没写 WAL"。
- check 失败不影响其他写者（引擎不停库，后续 `Write` 正常）。
- check 内 panic 不挂写队列（转为 error，队长正常让位，库照常可用）。

---

## 5. 应做：性能与易用性

这六项都不阻塞上层跑起来，但缺了会让上层"跑得很难看"。它们的共同点是**全在根包内，不碰磁盘格式**（5.3 会碰到 `internal/sst`，是唯一例外）。

### 5.1 前缀扫描 + 半开上界

现状：`IteratorOptions` 双闭区间（`db_iter.go:15-22`）。上层的 `HGETALL` / `LRANGE` / `SCAN` / 索引范围扫全部要自己算"前缀的后继"。

- 新增 `IteratorOptions.Prefix []byte`；`UpperBound` 语义**改为半开**。
- 为什么敢做破坏性变更：module 刚改名、尚未打 tag，**没有外部调用方**；而且 4.1 的范围墓碑本身是半开区间，两套区间语义并存是 bug 温床。
- `Prefix` 与 `LowerBound` / `UpperBound` 互斥，同时设置时报错。
- 落点：`db_iter.go`、`internal/iterator/dbiter.go`（`ucmp(uk, it.upper) > 0` 改成 `>= 0`）。
- **可选延伸**：有了 `Prefix` 就能接 prefix Bloom（`internal/filter` 加 `PrefixExtractor`），让"这个 SST 里有没有该前缀"在开文件之前判定。先不做。

### 5.2 MultiGet

```go
func (db *DB) MultiGet(keys [][]byte) ([][]byte, []error)
```

现在每次 `Get` 都要一次 `db.mu.RLock()` + 逐层二进制定位一次。`MGET` / `WHERE id IN (...)` / JOIN 探测可以把这些摊销掉。**关键收益在排序**：内部按 user key 排序后，相邻 key 会命中同一个 SST 的同一个块，把 N 次随机块读变成顺序块读。

落点：`db.go` 新方法，内部复用 `getLocked`；不需要动 `internal/`。

### 5.3 反向迭代

`Iterator` 增加 `SeekToLast()` / `SeekForPrev(target)` / `Prev()`。

- SQL 的 `ORDER BY x DESC LIMIT n` 和 Redis 的 `ZREVRANGE` 现在只能"正扫 + 缓冲"或者为倒序列多存一份反向编码。
- 落点：`internal/iterator/merging.go`（双向归并）、`internal/sst` 块迭代器、`internal/memdb` 跳表（有 prev 指针，最容易）。
- **块内反向的代价要预先接受**：前缀压缩下无法直接反向解出上一条 key，做法是"回退到上一个重启点再正向解到目标"，每步 `O(restartInterval)`。可接受。

### 5.4 Flush / Sync / Compact

```go
func (db *DB) Flush() error              // 冻结当前 MemTable 并等它落盘
func (db *DB) Sync() (uint64, error)     // fsync WAL，返回已耐久的序列号
func (db *DB) CompactRange(start, end []byte) error
```

- `Flush`：`Close` 刻意不落 MemTable（DESIGN §9，设计如此），所以长跑服务重启要重放整条 WAL。想要"优雅关闭快速重启"就需要它。实现：拿 `db.mu.Lock()` → `freezeLocked()` → 在 `db.cond` 上等 `db.imm == nil`。**复用 `db.cond`，不引入新机制。**
- `Sync`：`SyncWrites` 是全局开关，且没有"想 fsync 时能 fsync、并知道耐久到了哪个序列号"。`appendfsync everysec` 这类策略必须要有它。
- `CompactRange`：清表之后的空间回收只能等后台自动挑中，需要能手动催。注意 `Checkpoint` 已经覆盖"备份不丢未落盘数据"，它解决不了这个问题。

### 5.5 GetInto（零拷贝读）

`DB.Get` → `copyValue`（`db.go:594`），每次读都分配 + 复制。对"读出来直接往 socket 写"的服务，这一份拷贝是纯浪费。

```go
func (db *DB) GetInto(dst []byte, userKey []byte) ([]byte, error)
```

`dst` 容量够就 copy 进去，不够则退化为现在的分配行为。**先不做"返回持有块缓存引用的句柄 + Release"**——那会引入一整套生命周期约定，收益不足以抵消复杂度。

### 5.6 读路径去全局锁

现状：每次 `Get` 一次 `db.mu.RLock()` / `RUnlock()`。单机多核时会变成缓存行乒乓。

做法：把 `mem` / `imm` / `v` / `lastSeq` 收进一个不可变的 `readState`，用 `atomic.Pointer` 换上去，`Get` 只做一次原子加载 + 一次 `v.Ref()`。

**真正的难点不是换原子量**，是 `db.readers` 这个 map 和 `db.closed` 也在锁内被读。建议拆两步：先把 `closed` 与 `v` 原子化（收益最大、改动最小），`readers` 暂留锁内或改成 `sync.Map`。**放最后做。**

---

## 6. 约束与已知风险

这几条不是缺失，是上层必须知道的事实。踩中任何一条的代价都远高于补那四项 P0。

| 约束 | 说明 |
|---|---|
| **Comparer 一旦写进 Manifest 就冻结** | `Options.Comparer.Name()` 会被校验（`TestComparerMismatchIsRejected`）。SQL 层的元组编码必须设计成"以后加列也不改比较函数"，否则老目录直接打不开。同理适用于 4.2 的 filter 与 4.3 的 merge operator。 |
| **单个 value 有隐性上限** | 单块解压上限是 `decompressedLimit = 1<<30`（`internal/compress/compress.go:79`）。一个**可压缩**且接近/超过 1GB 的 value 会被正常压进去，读回来时 snappy 的 `DecodedLen` 直接超限返回 `ErrCorrupt`——**写成功、读永久失败**。Redis 的 512MB string 刚好卡在下面，但与其靠巧合，不如在 `WriteBatch` 入口显式拒绝超大 value。 |
| **快照全局钉住 GC** | 丢弃上界 = `smallestSnapshot`。长事务、`SCAN` 持快照跨调用，都会把旧版本回收按住；4.2 的 filter 还会因此失效。现在只有 `Stats().LiveSnapshots` 能观察，没有超时告警。上层要把快照当稀缺资源。 |
| **序列号耗尽 = panic** | `MakeTrailer` 对 `seq > MaxSeqNum` 直接 panic（`internal_key.go:59`）。2^56 在正常速率下用不完，但常驻服务更该拿到一个可处理的 error。 |
| **单批次上限 64MB** | `MaxBatchBytes`（`batch.go:39`）。一个事务的写集、一次大 `MSET` 都受它约束。 |
| **同一 user key 的所有版本必须同块** | DESIGN §4.1 硬约束 1。于是大 value 会撑出大块：块缓存条目巨大、点查一次要读整个块。上层应当把大对象拆成子键（3.1 已经推过一次这个结论）。 |
| **无 Column Family** | 建议绕开而不是补：SQL 用"表 ID 前缀 + 4.1 的范围删除"，Redis 的 16 个 db 用"一字节 db 前缀"。真要做 CF，代价是共享 WAL + 每 CF 的 log number，属于大改。 |

---

## 7. 明确不做

- **事务隔离级别、MVCC 写写冲突检测、死锁检测**——只提供 4.4 的原语，语义全在上层。
- **二级索引**——"一行 + N 个索引项写进同一个 WriteBatch"已经足够，维护逻辑在上层。
- **RESP 解析、SQL parser、查询优化器、网络层**——kvdb 是嵌入式库，不定义协议。
- **值日志分离（KV separation）**——badger 那条路。大对象请用子键拆分。
- **Column Family**——见 6 节的绕开方案。
- **filter 的 Change 决策**——会让用户回调参与写路径的正确性，收益不抵复杂度。
- **多进程同目录**——目录锁兜住即可，与 DESIGN §8 一致。

---

## 8. 实施顺序

排序依据是"杠杆 / 成本"，且刻意把两个**磁盘格式级**改动（4.1、4.3）分开做，中间夹一个便宜的——这样即使中途停下，引擎也始终处于可用状态。

| 阶段 | 内容 | 格式级改动 | 解锁什么 |
|---|---|---|---|
| **M6** | 4.2 CompactionFilter | 否（只加一个 Manifest 字段） | Redis 的 TTL 物理清除 |
| **M7** | 4.1 DeleteRange + 5.1 半开上界 / Prefix | **是** | SQL 的 DDL、Redis 的 `DEL` 大 key、两者共用的前缀枚举 |
| **M8** | 4.3 Merge 算子 | **是** | `INCR` 系列、SQL 计数器列 |
| **M9** | 4.4 提交期校验原语 | 否 | SQL 的快照隔离与唯一约束 |
| **M10** | 5.2 MultiGet · 5.4 Flush/Sync/Compact · 5.5 GetInto | 否 | 一批纯 API 级，改动全在根包，可一次做完 |
| **M11** | 5.3 反向迭代 · 5.6 读路径去锁 | 否（5.3 会碰 `internal/sst`） | 性能收尾 |

M6 先做的理由：它是四项 P0 里唯一不碰磁盘格式的，而 TTL 是 Redis 语义的核心——**投入最小、解锁最早**。M7 最贵但杠杆最大，一个改动同时解锁 SQL 的 DDL 和 Redis 的大 key 删除。

---

## 附录 A：API 增补清单

```go
// ── 4.1 范围删除
func (b *WriteBatch) DeleteRange(start, end []byte) error   // 半开 [start, end)

// ── 4.2 Compaction 过滤器
type CompactionFilter interface {
    Name() string
    Filter(level int, userKey, value []byte, seq uint64) (Decision, error)
}
type Decision int
const (Keep Decision = iota; Drop)

// ── 4.3 Merge 算子
type MergeOperator interface {
    Name() string
    FullMerge(userKey, base []byte, operands [][]byte) ([]byte, error)
    PartialMerge(userKey, a, b []byte) (merged []byte, ok bool)
}
func (b *WriteBatch) Merge(userKey, operand []byte) error

// ── 4.4 提交期校验
func (db *DB) GetForUpdate(userKey []byte) (value []byte, seq uint64, err error)
func (db *DB) WriteChecked(b *WriteBatch, check func() error) error

// ── 5. 性能与易用性
type IteratorOptions struct {
    Prefix     []byte   // 与 LowerBound/UpperBound 互斥
    LowerBound []byte   // 含
    UpperBound []byte   // **不含**（语义变更）
}
func (db *DB) MultiGet(keys [][]byte) ([][]byte, []error)
func (db *DB) Flush() error
func (db *DB) Sync() (uint64, error)                        // 返回已耐久的序列号
func (db *DB) CompactRange(start, end []byte) error
func (db *DB) GetInto(dst []byte, userKey []byte) ([]byte, error)
type Iterator interface {
    // ... 现有方法 ...
    SeekToLast()
    SeekForPrev(target []byte)
    Prev()
}
```

新增 `Options` 字段：`CompactionFilter`、`FilterOnFlush`（默认 false）、`MergeOperator`、`MaxValueSize`（0 = 不限制，建议上层显式设置）。

---

## 附录 B：两个上层的键编码示例

**Redis 层**

```
string  key                       → value（带 8 字节 expireAt 头）
hash    key \x00 field            → value
list    key \x00 <单调 seq:8B BE> → element
zset    key \x00 <score:8B 序保持> \x00 member → value
set     key \x00 member           → 空值
db 编号 key 首字节前再加一字节 db 索引
```

- `key` 与子键之间用 `\x00` 分隔，因为它是所有可打印字符里最小的，能保证"前缀 P"的后继计算不出意外。
- `DEL bigkey` = `DeleteRange(key+"\x00", key+"\x01")`（`\x00` 的下一个字节是 `\x01`）。
- 集合长度、list 的头尾指针这类"每操作都要改的元数据"，用 4.3 的 Merge 算子维护，避免读-改-写。

**SQL 层**

```
主表    <tableID:4B> <pk 列按 comparer 编码...>   → 行值
索引    <indexID:4B> <索引列编码...> <pk 编码...>  → 空值（覆盖索引则带列）
倒序列  编码时按位取反，直接用正序 comparer 扫
元信息  <0x00> <"schema"> ...                     → DDL
```

- 一张表的全部数据 = 一个前缀区间 ⇒ `DROP TABLE` = 一次 `DeleteRange`。
- Comparer 必须一次设计到位（6 节第一条）：加列、改排序规则都不得改变比较函数，只能靠新增索引 + 前缀迁移。

---

## 附录 C：关键设计决策

| 决策 | 理由 |
|---|---|
| 范围墓碑用**半开区间** | 闭区间无法表达"到下一个前缀之前"——找不到该前缀下最后一个键的后继 |
| 范围墓碑挂在 `Version` 上做全局表，不按文件存 | 范围数等于"执行过的范围删除次数"，通常是几十量级；`O(log n)` 常数开销换掉一整层 per-file 状态 |
| 范围墓碑**必须参与读路径判据** | 只在 Compaction 生效 ⇒ 删完还能读到，是正确性错误 |
| 范围墓碑退休复用 `isBaseLevel` | 与单键墓碑的退休条件完全同构，不另写一套 |
| Flush 侧不做范围判定 | Flush 在前台关键路径上（决定 MemTable 能否释放），且推迟到 Compaction 是免费的 |
| filter 只对**最新可见版本**调用 | 对已覆盖的旧版本调用毫无意义，且会让调用次数从键数涨到记录数 |
| filter 的 Drop **转成墓碑语义**，复用 `isBaseLevel` | 直接跳过会让更深层的旧值复活；复用现成判据最省力 |
| filter 只对 `seq <= SmallestSnapshot` 生效 | 否则破坏快照隔离。代价：老快照会让 filter 大面积失效 |
| Flush 侧默认不挂 filter | 把用户回调插在前台关键路径上，等于把引擎延迟暴露给用户代码 |
| filter / merge 的 `Name()` 写进 Manifest 校验 | 与 Comparer 同级别：换语义读老目录会造成"该丢的没丢、不该丢的丢了" |
| 过滤器名**记过之后才冻结**：老目录可首次配过滤器，但换名/去掉都会被拒 | M6 之前的 Manifest 没有这个名字，"首次引入"不改变任何既有数据的判定；而"换掉一个已在生效的过滤器"才是真正的事故 |
| Kind 编号一次定死：2 = Merge，3 = RangeDeletion | 编号一旦落盘就冻结；两个特性无论谁先做都要按同一张表占位 |
| `covered` 只在遇到 Value/Deletion 时置位 | Merge operand 必须收集齐才能丢，沿用原规则会丢掉还没折叠的 operand |
| 要求 Merge 算子满足结合律 | `PartialMerge` 的结果会再参与折叠，不满足结合律会与 `FullMerge` 结果不一致，而引擎无法替上层发现 |
| `WriteChecked` 的批次**独占提交组** | 现有"整组同生共死 + 停库"语义会让一次正常的事务 abort 演变成停库 |
| `check` 放在 WAL 追加之前 | 事务回滚不该在 WAL 里留下痕迹 |
| 事务原语**不进 `Reader`/`Writer` 接口** | 那三个接口是抽象 kvdb 的最小面，塞进去会抬高所有替代实现的门槛 |
| `UpperBound` 改为半开 | 与范围墓碑的区间语义统一；且 module 未打 tag，无外部调用方 |
| `Flush` 复用 `db.cond` 等待 | 不引入新机制；等待条件是 `db.imm == nil` |
| 先不做 `Get` 的 Release 句柄，只做 `GetInto` | 句柄会引入一整套生命周期约定，收益不抵复杂度 |
| 不补 Column Family，用前缀 + DeleteRange 绕开 | 真做 CF 要改共享 WAL + 每 CF 的 log number，属于大改 |
