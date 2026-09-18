# kvdb 设计文档

> 一个用 Go 实现的高性能嵌入式 KV 存储引擎，架构参考 RocksDB / LevelDB 的 LSM-Tree。
>
> 状态：M0（骨架）、M1（最小可用）、M2（读优化）、M3（写优化）、M4（一致性）均已完成 ｜ 最后更新：2026-09-18

## 目录

- [1. 项目定位与边界](#1-项目定位与边界)
- [2. 整体架构](#2-整体架构)
- [3. 读写路径](#3-读写路径)
- [4. SSTable 文件格式](#4-sstable-文件格式)
- [5. 功能清单](#5-功能清单)
- [6. 实施路线](#6-实施路线)
- [7. Go 实现建议](#7-go-实现建议)
- [8. 明确不做的](#8-明确不做的)
- [9. 实施进度](#9-实施进度)
- [附录 A：关键数据结构](#附录-a关键数据结构)
- [附录 B：核心接口](#附录-b核心接口)
- [附录 C：包结构](#附录-c包结构)
- [附录 D：关键设计决策](#附录-d关键设计决策)

---

## 1. 项目定位与边界

RocksDB 本体是 40 万行 C++、上百个可调参数。本项目不追求功能对齐，只保留 LSM-Tree 的内核思想，因此需要先把边界划死，否则功能会无限膨胀。

**做：**

- 单机嵌入式存储库（被进程直接链接，不是独立服务，不定义网络协议）
- 单进程访问（用文件锁保证同一目录不被多进程同时打开）
- 有序 KV，支持点查、范围扫描、原子批量写、快照读
- 第一优先级：**顺序写吞吐** 与 **点查延迟**

**不做：**

- 分布式、Raft / Paxos、一致性协议
- 事务隔离级别（SI / SSI）、二级索引
- 加密、审计、鉴权

---

## 2. 整体架构

LSM-Tree 的全部设计思想可以压缩成一句话：**用顺序写换随机写，接受空间放大和读放大**。所有写入只追加到 WAL 和内存，磁盘上永远不原地更新。

```mermaid
flowchart TD
    W["写入请求<br/>Put / Delete / Merge"]
    W --> WAL["WAL 预写日志<br/>顺序追加 + CRC 校验"]
    W --> MT["MemTable<br/>跳表 · 有序 · 纯内存"]
    MT -->|写满转只读| IMT["Immutable MemTable<br/>等待后台落盘"]
    IMT -->|后台 Flush| L0["L0 — 文件 key 区间重叠<br/>读取时按新旧顺序逐个查找"]
    L0 -->|Compaction 分层合并| L1["L1 — 同层文件区间互不重叠<br/>二分查找直接定位"]
    L1 -->|Compaction| L2["L2 … Ln<br/>每层容量约为上一层 10 倍"]
```

### 三个容易被低估的点

**1）WAL 和 MemTable 是并行的两条路，不是先后顺序。**
一次写请求既要追加 WAL（持久化保证），也要插入 MemTable（可读性保证）。WAL 的 fsync 成功是返回客户端的充要条件，MemTable 的插入只是为了让数据可读。理解成"先写日志再写内存"会在实现并发时走弯路。

**2）Immutable MemTable 是必需的，不是优化。**
MemTable 写满后不能就地转成 SST——后台 Flush 需要时间，这段时间里新写入必须继续。所以标准做法是：写满 → 冻结为只读的 Immutable MemTable → 立刻挂上新的空 MemTable 继续接受写入 → 后台线程慢慢把 Immutable 落盘。没有这一层，Flush 期间写入会被全程阻塞。

**3）L0 和其他层的规则不同，这是整个引擎效率的分水岭。**
L0 的文件是 MemTable 直接 Flush 出来的，彼此 key 区间**会重叠**，所以只能按新旧顺序逐个线性查找。L1 以下每层内部文件区间**互不重叠**，才能二分定位。所有 Compaction 策略的设计目标，本质上就是"尽快把 L0 的重叠区间收敛成 L1 的有序不重叠布局"。

---

## 3. 读写路径

两条路径的成本来源完全不同：写路径的成本几乎全在一次 fsync 上，读路径的成本在层数和无效 IO 上。

```mermaid
flowchart LR
    subgraph WP["写路径 · 低成本"]
        direction TB
        A1["写入请求<br/>Put · Delete · Merge"] --> A2["WAL 顺序追加<br/>追加并 fsync，落盘即成功"]
        A2 --> A3["MemTable 跳表插入<br/>纯内存，无随机 IO"]
        A3 --> A4["返回成功<br/>写延迟 ≈ 一次 fsync"]
    end
    subgraph RP["读路径 · 高成本"]
        direction TB
        B1["读取请求<br/>Get · Seek · 范围扫描"] --> B2["MemTable → Immutable<br/>内存命中直接返回"]
        B2 --> B3["L0 → L1 … Ln<br/>Bloom 过滤 · 二分定位 · 缓存"]
        B3 --> B4["返回可见版本<br/>按 seq 取最新可见记录"]
    end
```

**写路径的关键在 WAL**：只要 fsync 成功就可以返回，MemTable 操作是纯内存的，所以写是 `O(log n)` 且没有随机 IO。这也意味着**写性能的天花板由 fsync 次数决定**，这是 Group Commit 存在的理由——把并发写请求合并成一次 fsync。

> **M4 之后写路径的实际形态**：并发写者先排进一条写队列，队首那个写者（队长）把当前
> 队列里的所有批次合并成**一次 WAL 追加 + 一次 fsync**，然后整组一起落进 MemTable。
> 于是"写吞吐的天花板"从"每秒能做多少次 fsync"变成了"每秒能做多少次 fsync × 合并率"。
> 队长只在两个极短的临界区里持 `db.mu`，最贵的 fsync 完全在锁外完成，所以
> **读者不会被写者的磁盘等待堵住**。实测 32 个并发写者下合并率约 22～28（见 9.20），
> 即同样次数 fsync 服务了二十多倍的写入。

**读路径的代价在层数**：最坏情况下要穿透 MemTable、Immutable 和每一层。所以必须靠两个组件把无效查找挡在外面：

- **Bloom Filter**：每个 SST 一个位图，能在打开文件之前就判定"key 一定不在这个文件里"，挡掉绝大部分无效磁盘 IO。
- **Block Cache**：分片 LRU，缓存解压后的数据块，让热点读不落盘。

> **读到 M3 为止的实际形态**：上面两个组件都已经接上（`internal/filter` + `internal/cache`），
> 每层 SST 内部是"Index 二分 → Bloom 判定 → 读块（先查缓存）→ 块内 seek"。
> **分层也已经长出来了** —— L0 的文件数被 `L0CompactionTrigger` 兜在阈值附近，
> L1 以下同层区间互不重叠，于是每层只需要**一次二分**定位到唯一候选文件，
> "M 个文件就要探 M 次元数据"这件事不再随写入量增长。
> 版本由 Manifest 持久记录（`internal/version`），合并由 `internal/compact` 执行。
> 具体格式见 4.1，实测数据见 9.12 与 9.16。

**Delete 是写操作，不是删除操作。** 它会写入一条带删除标记（墓碑）的记录，真正的物理删除发生在后续 Compaction 归并时。这是 LSM-Tree 不可变性的直接推论。

---

## 4. SSTable 文件格式

SSTable 不是简单地把 KV 顺序写进文件。它必须切块、建索引、加过滤器，才能支持"一块一块地二分查找"而不用全文件扫描。**这是实现量最大的一块。**

| 区块 | 作用 |
|---|---|
| Data Block 0 … N | 有序 KV 对。默认约 4KB 一块，块内用前缀压缩 + 重启点（restart points）降低体积，每块带 CRC32 校验 |
| Filter Block | Bloom 位图，用于在打开块之前判定 key 是否存在 |
| Index Block | 记录每个 Data Block 的首 key 与文件偏移，二分它就能定位到目标块 |
| MetaIndex Block | 指向 Filter Block 及 properties 等元数据区的索引 |
| Footer | 定长，记录 Index Block 与 MetaIndex Block 的位置，是读取文件的入口 |

一次点查的完整链路：读 Footer → 读 Index Block（可缓存）→ 查 Filter Block 判断是否存在 → 二分 Index 定位 Data Block → 读块（先查 Block Cache）→ 块内二分得到记录。

### 4.1 实装细节（M2）

上面是目标形态；M2 已经把它落地，具体编码如下。

```
┌──────────────────────────────────────────────────────────┐
│ Data Block 0                                             │
│   entry... entry... [restart array] [numRestarts(fixed32)]│
│ Data Block 1 … N                                         │
├──────────────────────────────────────────────────────────┤
│ Filter Block                                             │
│   bloom bitmap… │ offset array(fixed32 × N+1) │ baseLg(1B)│
├──────────────────────────────────────────────────────────┤
│ MetaIndex Block   "filter.kvdb.BloomFilter" → Filter handle│
├──────────────────────────────────────────────────────────┤
│ Index Block       block max key → Data Block handle      │
├──────────────────────────────────────────────────────────┤
│ Footer (48B)                                             │
│   MetaIndex handle(变长) + Index handle(变长) + 填充 +    │
│   magic = "kvdb0002"                                     │
└──────────────────────────────────────────────────────────┘
```

- **Data Block 内部**：每条 entry 是 `shared_len:uvarint | unshared_len:uvarint | value_len:uvarint | key_delta | value`，
  前缀压缩的基准是**每 16 条一个的重启点**（`restartInterval = 16`）。重启点数组倒序写在块尾，
  最后跟一个 `fixed32` 的重启点个数。块内查找先二分重启点，再在段内线性扫。
- **块尾（trailer，5 字节）**：`compression_type(1B) + crc32c(4B，掩码后)`。M5 起该字节实装：0 = 不压缩，1 = Snappy，2 = zlib（flate.BestSpeed）。
  压缩在"块写满"时按块独立判断，压完省不下 1/8 就原样存储，策略见 9.25。
- **Index Block 的 key 是数据块的 max key**（不是首 key），value 是 `blockHandle{offset, size}` 的编码。
  handle 本身是 `uvarint(offset) | uvarint(size)`，长度不含 5 字节块尾。
- **Filter Block 的偏移数组**是 `fixed32 × (N+1)`：末尾多一项"数组自身起点"当哨兵，
  于是"第 i 段的终点"就是第 i+1 项，最后一段正好落在哨兵上，读取时不需要为末段写特判。
- **Footer 定长 48 字节** = 40 字节的 handle 区（两个 handle 顺序写入，剩余补零，读取时校验补零区确实全零）
  \+ 8 字节大端 magic。因此"打开"是先读尾部 48 字节定位两个索引块，再读这两个块 —— 一共三次 pread，
  之后所有点查都不再碰元数据 IO。

**两条硬约束**（正确性所依赖，见 9.13）：

1. **同一个 user key 的所有版本必须落在同一个 Data Block 里** —— 块边界只在 user key 变化时才允许切。
2. **Bloom Filter 建在 user key 上**，不是 internal key（后者带 `(seq, type)` 后缀，每个版本都不同）。

**格式兼容性**：M1 的 SST 是"数据区 + 16B footer（`fixed64(dataLen)` + magic `kvdb0001`）"，
与五段式不兼容。M2 遇到 `kvdb0001` 的文件直接返回 `ErrLegacyFormat`，不尝试解析。

---

## 5. 功能清单

分四档。**MVP 范围只包含前两档。**

| 模块 | 必要性 | 关键内容 |
|---|---|---|
| **WAL 预写日志** | 必做 | 顺序追加、分块、每条 CRC 校验、崩溃重放、flush 完成后可清理 |
| **MemTable** | 必做 | 内存有序表（跳表），并发读 + 单写者，容量阈值触发切换 |
| **SSTable 文件格式** | 必做 | Data / Filter / Index / MetaIndex / Footer 五段式结构 |
| **Manifest + Version** | 必做 | 版本变更日志 + CURRENT 指针 + 引用计数，崩溃一致性的根 |
| **Compaction** | 必做 | 选文件 → 多路归并 → 原子替换 Version |
| **internal key 编码** | 必做 | `user_key + 8 字节 (seq<<8 \| type)`，Comparer 可插拔 |
| **Put/Get/Delete/Iterator** | 必做 | Delete 写墓碑；Iterator 用 MergingIterator 做多路归并 |
| **崩溃恢复** | 必做 | 重放 WAL + 从 Manifest 重建 VersionSet |
| **Bloom Filter** | 强烈建议 | 每个 SST 一个 filter block，挡掉绝大部分无效磁盘 IO |
| **Block Cache** | 强烈建议 | 分片 LRU，缓存解压后的数据块 |
| **WriteBatch** | 强烈建议 | 一次 WAL append 完成多条写，顺带拿到原子性 |
| **Snapshot / MVCC** | 强烈建议 | 序列号 + 快照读。**建议一开始就在 key 里留 seq 字段**，后补代价极大 |
| **Group Commit** | 建议 | 并发写合并成一次 fsync，高并发吞吐的关键（M4 已完成，见 9.18） |
| **块压缩** | 建议 | Snappy / LZ4 / ZSTD，块级压缩（M5 已完成：Snappy / zlib，见 9.22） |
| **Metrics / LOG** | 建议 | 命中率、各层文件数、读写放大倍数（M5 已完成：压缩/限流指标入 Stats，事件写 LOG，见 9.22） |
| **Rate Limiter** | 可选 | 限制 compaction 带宽，避免挤压前台请求（M5 已完成，见 9.22） |
| **Column Family** | 可选 | 多列族共享 WAL，各自独立 MemTable / SST |
| **事务 / TTL / 加密** | 可选 | 单机 KV 初期全都用不上 |

---

## 6. 实施路线

每个阶段都有明确的验收标准，避免"写了很多代码但不知道对不对"。当前进度见 [9. 实施进度](#9-实施进度)。

| 阶段 | 交付内容 | 验收标准 | 状态 |
|---|---|---|---|
| **M0 骨架** | Options / Comparer / varint / internal key 编解码 | 编解码单测全绿 | 已完成 |
| **M1 最小可用** | WAL + MemTable（跳表） + 简单 SST + Get/Put/Delete + 崩溃恢复 | 单线程 10 万次读写结果正确；`kill -9` 后数据不丢 | 已完成 |
| **M2 读优化** | Index Block + Bloom + Block Cache + Iterator | 点查不触发全文件扫描；范围扫描可用 | 已完成 |
| **M3 写优化** | Flush + Leveled Compaction + Manifest/Version | L0 文件数收敛在阈值附近；读放大不随写入量增长 | 已完成 |
| **M4 一致性** | Snapshot + WriteBatch + Group Commit | 并发压测下 race detector 无告警 | 已完成 |
| **M5 生产化** | 块压缩 + 限流 + Metrics/LOG + Checkpoint | 跑通 YCSB 并输出基准报告 | 已完成 |

建议先把 M0 + M1 打通成一条完整链路，哪怕 SST 只有一个文件、查找用线性扫描——**能跑通"写入→崩溃→恢复→读回"这条闭环，比先把 SST 做得多漂亮重要得多。**

---

## 7. Go 实现建议

### 参考资料

| 项目 | 价值 |
|---|---|
| **cockroachdb/pebble** | 最值得读。RocksDB 血统的 Go 生产级实现，`internal/` 目录几乎与上表每个模块一一对应 |
| **dgraph-io/badger** | 另一条路线（KV 分离到 value log），写放大更低，但范围查询弱 |
| **LevelDB `doc/table_format.md`** | 吃透 SSTable 字节布局最快的一份材料 |

推荐读法：先照 LevelDB 文档把 SSTable 字节格式读透，再对照 pebble 的 `internal/` 看工程实现。

### 依赖策略

第三方库**只用在工具层**，存储引擎主体自己写，否则这个项目就失去了意义。

- 可以用：`github.com/cespare/xxhash`、`github.com/golang/snappy`、`github.com/klauspost/compress`、`github.com/bits-and-blooms/bloom`
- 建议自己写：跳表（约 400 行）、块编解码、Manifest 读写、Compaction
- 跳表也可以先用 `github.com/google/btree` 顶一阵，但要注意 B-Tree 的写并发控制与 LSM 的"无锁读 + 单写者"模型不完全匹配

---

## 8. 明确不做的

这些属于"另一个项目"，初期碰它们会直接拖垮进度：

- 分布式 / Raft / 网络协议
- 事务隔离级别（SI / SSI）、多版本并发控制的完整实现
- 二级索引、加密、审计
- 多进程并发访问同一目录（文件锁兜住即可）
- 兼容 RocksDB 的 API 或磁盘格式

---

## 9. 实施进度

### 9.1 阶段状态

| 阶段 | 状态 | 完成时间 | 依据 |
|---|---|---|---|
| M0 骨架 | **已完成** | 2026-09-18 | `go test ./...` 全绿，见 9.4 |
| M1 最小可用 | **已完成** | 2026-09-18 | 10 万次读写正确 + 子进程强杀后数据不丢，见 9.8 |
| M2 读优化 | **已完成** | 2026-09-18 | 点查不再全文件扫描 + 范围扫描可用 + 读延迟的线性项被压掉约 3 个数量级，见 9.12 |
| M3 写优化 | **已完成** | 2026-09-18 | L0 文件数被钉在触发阈值附近 + 读放大收敛为常数 + 崩溃/迁移路径均可恢复，见 9.16 |
| M4 一致性 | **已完成** | 2026-09-18 | 并发压测下 race detector 无告警 + 组提交把 1024 次写合并成 100 余次 fsync，见 9.20 |
| M5 生产化 | **已完成** | 2026-09-18 | YCSB 六种负载跑通并输出基准报告（docs/BENCH.md）+ 压缩对照 6.87x + 限流/Checkpoint 验证，见 9.24 |

### 9.2 M0 交付物

| 文件 | 内容 |
|---|---|
| `go.mod` | 模块声明：`module kvdb`，`go 1.23` |
| `doc.go` | 包级文档 |
| `options.go` | `Options` 全量字段与默认值、`Comparer` 接口、`BytewiseComparer`、internal key 比较器叠加、`levelMaxBytes` |
| `options_test.go` | Options 默认值 / 校验 / 比较器可插拔性单测 |
| `internal/key/varint.go` | uvarint、zigzag varint、fixed32/64 编解码与三类错误值 |
| `internal/key/internal_key.go` | `Kind`、trailer 打包、internal key 编解码、`InternalKeyCompare`、`SeekKey` |
| `internal/key/varint_test.go` | varint 编解码单测（含与标准库逐字节交叉校验） |
| `internal/key/internal_key_test.go` | 字节布局、错误处理、排序语义、`SeekKey` 可见性语义单测 |
| `.gitignore` | 忽略测试产物与本地数据目录 |

### 9.3 M0 API 一览

```go
// ── package kvdb（options.go）

const DefaultMemTableSize = 64 << 20 // 另有 BlockSize / BlockCacheSize / BloomBitsPerKey /
                                     // L0CompactionTrigger / LevelBaseSize /
                                     // LevelSizeMultiplier / MaxLevels

type Comparer interface {
    Compare(a, b []byte) int // -1 / 0 / +1
    Name() string            // 稳定标识，后续写入 Manifest 校验目录与配置是否匹配
}

type BytewiseComparer struct{} // 默认比较器，等价 bytes.Compare

type Options struct {
    Dir                 string
    Comparer            Comparer
    MemTableSize        int
    BlockSize           int
    BlockCacheSize      int
    BloomBitsPerKey     int
    L0CompactionTrigger int
    LevelBaseSize       int
    LevelSizeMultiplier int
    MaxLevels           int
    SyncWrites          bool
}

func DefaultOptions(dir string) Options // 字段全部填好
func (o *Options) Validate() error      // 只校验，不填默认值

// ── package kvdb/internal/key

// varint.go
func PutUvarint(dst []byte, x uint64) []byte
func Uvarint(buf []byte) (value uint64, n int, err error)
func UvarintLen(x uint64) int
func PutVarint(dst []byte, x int64) []byte
func Varint(buf []byte) (value int64, n int, err error)
func VarintLen(x int64) int
func PutFixed32(dst []byte, x uint32) []byte
func Fixed32(buf []byte) (uint32, error)
func PutFixed64(dst []byte, x uint64) []byte
func Fixed64(buf []byte) (uint64, error)

// internal_key.go
type Kind uint8 // TypeDeletion = 0 / TypeValue = 1
const TrailerLen = 8
const MaxSeqNum uint64 = (1 << 56) - 1

func MakeTrailer(seq uint64, kind Kind) uint64
func AppendInternalKey(dst, userKey []byte, seq uint64, kind Kind) []byte
func EncodeInternalKey(userKey []byte, seq uint64, kind Kind) []byte
func DecodeInternalKey(ik []byte) (userKey []byte, seq uint64, kind Kind, err error)
func ParseInternalKey(ik []byte) (ParsedInternalKey, error)
func UserKey(ik []byte) []byte
func Trailer(ik []byte) uint64
func SeqNum(ik []byte) uint64
func KindOf(ik []byte) Kind
func SeekKey(userKey []byte, snapshot uint64) []byte
func InternalKeyCompare(a, b []byte, userCmp func(a, b []byte) int) int
```

### 9.4 M0 验收结果

M0 的验收标准是"编解码单测全绿"，实测：

```text
$ gofmt -l .            # 无输出
$ go vet ./...          # 无输出
$ go test ./... -cover
ok      kvdb                 1.25s   coverage: 100.0% of statements
ok      kvdb/internal/key    1.27s   coverage:  98.9% of statements
```

- 测试函数 35 个（含子测试共 54 个用例），**全部 PASS，0 FAIL**。
- 关键断言覆盖：
  - uvarint / zigzag varint 的编码结果与 `encoding/binary` **逐字节一致**，长度函数与实际编码长度一致（避免"自己编自己解"的循环验证）；
  - fixed32/fixed64 的大端字节布局、长度不足时的错误；
  - varint 截断 / 溢出边界（第 10 字节为 1 时恰好是合法上界 `1<<63`，为 2 时溢出）；
  - internal key 尾缀的字节布局（`"foo" + 00 00 00 00 00 00 0a 01`）、`seq` 超过 56 位时 panic；
  - 排序规则：`user_key` 升序 + 尾缀降序，并验证反对称性与自定义比较器真正生效；
  - `SeekKey` 在 5 个快照点上全部落在正确版本——包含"墓碑的 seq 恰好等于快照 seq"这一容易写错的边界。
- `go test -race` 在当前环境不可用（`go: -race requires cgo`）；M0 无并发代码，留到 M4 在启用 CGO 的环境补验。
- 验证环境：`go1.27.1 windows/amd64`。

### 9.5 M0 期间新增 / 细化的决策

| 决策 | 说明 |
|---|---|
| `Comparer` 用接口而非函数字段 | 接口能带 `Name()`，可写入 Manifest 校验"数据目录与配置是否匹配"；裸函数字段做不到 |
| `MakeTrailer` 对 `seq > MaxSeqNum` 直接 panic | 静默截断会破坏排序前提、产生难以定位的数据错乱，这类调用方错误应当立刻暴露 |
| `SeekKey` 尾缀用 `(snapshot, TypeValue)` | 尾缀降序排列下它是"seq ≤ snapshot 的全部版本"中最大者，seek 落点恰好是最新可见版本；用单测锁定了 `seq == snapshot` 的墓碑也能被命中 |
| Options 数值字段：`0` = 用默认值，负数 = 关闭可选特性 | `BlockCacheSize` / `BloomBitsPerKey` 需要"显式关闭"的语义，用负数表达才不会和"用默认值"混淆 |
| 二进制编码全程大端 | 与 LevelDB 的 trailer / fixed 编码一致，便于对照 `doc/table_format.md` 排查问题 |
| varint 与 internal key 同放 `internal/key` | 沿用附录 C 的规划；varint 后续主要由 sst 块编码消费，暂不单独拆包 |
| 访问器与校验解码分开 | `UserKey` / `SeqNum` / `KindOf` 是零成本尽力而为的访问器；`DecodeInternalKey` / `ParseInternalKey` 会校验长度与 kind，用于磁盘读回后的完整性检查 |

### 9.6 M1 交付物

M1 的目标不是功能齐全，而是**把"写入 → 崩溃 → 恢复 → 读回"这条闭环跑通**。
因此 SST 只有一个文件的线性扫描版本，索引、Bloom、缓存、分层压缩全部留给后面。

| 文件 | 内容 |
|---|---|
| `db.go` | `DB`：`Open / Close / Get / Put / Delete / Write`、目录锁、冻结 Immutable、`Stats`、`RecoveryReport` |
| `db_flush.go` | 目录扫描恢复、WAL 重放、后台 Flush 协程、SST 落盘、过期日志清理 |
| `batch.go` | `WriteBatch` 与它的二进制编解码（WAL 的负载就是它） |
| `file_lock_windows.go` / `file_lock_unix.go` | 目录锁：Windows 用 `LockFileEx`，类 Unix 用 `flock` |
| `db_test.go` | 读写语义、10 万次读写、多次落盘、重开恢复、目录锁、并发读写、半截文件丢弃 |
| `db_crash_test.go` | 用子进程"写完直接退出"模拟 `kill -9`，覆盖干净崩溃与日志尾部损坏两种情形 |
| `batch_test.go` | 批次字节布局、往返、损坏输入拒绝 |
| `internal/memdb/skiplist.go` | 跳表：单写者 + 多读者，无锁读 |
| `internal/memdb/memtable.go` | MemTable：internal key 存储、快照可见性、近似内存统计 |
| `internal/wal/writer.go` | 分块日志写入：CRC32C、类型拆分、块尾补零 |
| `internal/wal/reader.go` | 分块日志读取：片段重组、损坏定位 |
| `internal/wal/wal.go` | 编号日志文件：追加、fsync、重放、目录扫描与删除 |
| `internal/sst/writer.go` | 最小 SST 写入：顺序 entry + 定长 footer |
| `internal/sst/reader.go` | 最小 SST 读取：footer 校验 + 线性扫描 |
| `internal/key/comparer.go` | `Comparer` 接口与 `InternalComparer`（根包与内部包共用一份实现） |
| `cmd/kvdb-bench/main.go` | 压测工具雏形：顺序写吞吐、点查延迟、恢复报告 |
| `scripts/gotest.ps1` | 本机跑 `go test` 的小脚本（把输出写进日志，规避 PowerShell 不回显 stdout） |

### 9.7 M1 API 一览

```go
// ── package kvdb（db.go / batch.go）

type Options struct { /* 同 M0 */ }

func Open(opts Options) (*DB, error)
func (db *DB) Get(key []byte) ([]byte, error)          // 不存在或已删除 → ErrNotFound
func (db *DB) Put(key, value []byte) error
func (db *DB) Delete(key []byte) error
func (db *DB) Write(b *WriteBatch) error               // 唯一写入口
func (db *DB) Close() error
func (db *DB) Stats() Stats                            // 文件数、MemTable 占用、末序列号
func (db *DB) RecoveryReport() RecoveryReport          // 被截断的日志、被丢弃的文件

var ErrNotFound error
var ErrClosed error
var ErrLocked error
var ErrEmptyKey error
var ErrBatchCorrupt error

func NewWriteBatch() *WriteBatch
func (b *WriteBatch) Put(key, value []byte) error
func (b *WriteBatch) Delete(key []byte) error
func (b *WriteBatch) Len() int
func (b *WriteBatch) Reset()
func (b *WriteBatch) Sequence() uint64
func (b *WriteBatch) SetSequence(seq uint64)
func (b *WriteBatch) Encode() []byte
func (b *WriteBatch) EncodeTo(dst []byte) []byte
func (b *WriteBatch) Range(start uint64, fn func(seq uint64, kind key.Kind, k, v []byte) bool) error

// ── package kvdb/internal/memdb
func New(cmp key.Comparer, logNum uint64) *MemTable
func (m *MemTable) Add(seq uint64, kind key.Kind, userKey, value []byte)
func (m *MemTable) Get(snapshot uint64, userKey []byte) (value []byte, kind key.Kind, found bool)
func (m *MemTable) NewIterator() *Iterator
func (m *MemTable) ApproximateSize() int64
func (m *MemTable) LogNumber() uint64

// ── package kvdb/internal/wal
func Create(dir string, num uint64) (*Log, error)
func (l *Log) Append(record []byte) error
func (l *Log) Sync() error
func (l *Log) Close() error
func (l *Log) Remove() error
func Replay(dir string, num uint64, fn func(record []byte) error) (ReplayResult, error)
func ListLogs(dir string) ([]uint64, error)
func RemoveLog(dir string, num uint64) error

// ── package kvdb/internal/sst
func NewWriter(path string, cmp key.Comparer) (*Writer, error)
func (w *Writer) Add(internalKey, value []byte) error
func (w *Writer) Finish() error
func Open(path string, cmp key.Comparer) (*Reader, error)
func (r *Reader) Get(snapshot uint64, userKey []byte) (value []byte, kind key.Kind, found bool, err error)
func (r *Reader) Iterate(fn func(internalKey, value []byte) bool) error

// ── package kvdb/internal/key（M1 新增）
type Comparer interface{ Compare(a, b []byte) int; Name() string }
type InternalComparer struct{ User Comparer }
func (c InternalComparer) Compare(a, b []byte) int
func (c InternalComparer) UserCompare(a, b []byte) int
```

### 9.8 M1 验收结果

验收标准是"单线程 10 万次读写结果正确；`kill -9` 后数据不丢"，两条都用测试锁住了：

```text
$ gofmt -l .            # 无输出
$ go vet ./...          # 无输出
$ GOOS=linux go build ./...   # 无输出（交叉编译验证 Unix 分支的目录锁）

$ go test ./... -count=1 -cover
ok      kvdb                    5.64s   coverage: 89.3% of statements
ok      kvdb/internal/key       1.18s   coverage: 98.1% of statements
ok      kvdb/internal/memdb     1.49s   coverage: 100.0% of statements
ok      kvdb/internal/sst       1.72s   coverage: 80.5% of statements
ok      kvdb/internal/wal       1.59s   coverage: 85.1% of statements
```

**1）单线程 10 万次读写正确**

`TestHundredThousandWritesAndReads`：顺序写 10 万条 → 覆盖其中的一半 → 再删除四分之一 →
逐条读回并校验"已删除 / 已更新 / 原值"三种状态，全部吻合。总耗时 1.3s。

**2）`kill -9` 后数据不丢**

同进程里"不调用 Close"证明不了什么：目录锁挂在内核持有的文件句柄上，只有进程真的死了才会释放。
所以用子进程做实验（`db_crash_test.go`）：子进程用 `Open(DefaultOptions(dir))`（`SyncWrites = true`）
写入 300 条记录，**不做任何清理就 `os.Exit(1)`**，父进程再重新打开并逐条校验。

覆盖两种崩溃形态：

| 用例 | 崩溃形态 | 期望 |
|---|---|---|
| `TestCrashRecoveryKeepsSyncedWrites` | 写完立刻被杀 | 300 条全部读回，`RecoveryReport.TornLogs` 为空 |
| `TestCrashRecoveryToleratesTornLogTail` | 日志尾部被追加 3 个垃圾字节 | 损坏点之前的 200 条全部读回，`TornLogs` 如实报告该日志 |

两条都额外验证了"恢复之后还能继续正常读写"——崩溃不会把数据库留在不可用状态。

**3）落盘与恢复的组合路径**

- `TestDataSurvivesFlushes`：把 `MemTableSize` 压到 8KB，写 1200 条，强制反复"冻结 → 落盘"，
  数据要在 MemTable / Immutable MemTable / 多个 SST 三种位置之间切换后依然全部可读；
  关闭再打开（走 WAL 重放 + 恢复落盘）后逐条复核。
- `TestRecoveryDiscardsUnfinishedFlush`：伪造一个 footer 不全的 `000009.sst`（对应"Flush 写到一半被杀"），
  恢复时必须丢弃它——它的数据仍在 WAL 里——并把它记进 `RecoveryReport.DiscardedFiles`。
- `TestDirectoryLock`：同目录第二次 `Open` 返回 `ErrLocked`；`Close` 之后 LOCK 文件仍在目录里，但必须能立刻重新打开。

**4）性能观感（`cmd/kvdb-bench`，本机 Windows / go1.27.1，`SyncWrites = false`）**

```text
memtable size 1MB, 20000 keys, value 100B:
  write  111ms  (180422 ops/s, 5.54 us/op)      # 3 次冻结 + 落盘
  read   5.09s  (  3930 ops/s, 254.48 us/op)    # 3 个 SST，每个 Get 都要线性扫
```

写入侧的 5.5 µs/op 说明"顺序追加 + 跳表插入"这条路径本身很快；
读侧的 254 µs/op 则是 M1 刻意留下的坑：**没有索引，文件里的查找是线性扫描**，
点查成本随 SST 数量线性增长。这正是 M2 要解决的问题，数据摆在这里比任何论证都有说服力。

### 9.9 M1 期间新增 / 细化的决策

| 决策 | 说明 |
|---|---|
| 恢复时**强制把重放结果落成 SST** | 否则重放出的 MemTable 一落盘就会连带删掉旧日志，而这段数据还没有别的副本。先落盘再删日志，删日志的判据才能简单到"编号 < min(mem, imm) 的日志" |
| 崩溃时中断的 Flush 直接删除 | 它没有 footer，所以一定没被提交过；数据仍完整地躺在 WAL 里，随重放重新落盘。M3 有了 Manifest 之后，"未提交文件"由版本信息判定 |
| 目录锁用 OS 级锁（`LockFileEx` / `flock`），不用 `O_EXCL` 锁文件 | `O_EXCL` 会在进程被强杀后留下打不开的死锁文件，直接毁掉崩溃恢复路径；OS 锁随进程消亡自动释放 |
| 同一时刻只允许一张 Immutable MemTable | 写者在冻结时最多等到上一张落盘完成，实现简单且内存有上界；多张 Immutable 的并行落盘留给 M3 |
| 序列号写进 WriteBatch 头部，恢复时原样沿用 | 恢复期重新分配序列号会让同一目录在不同次恢复后得到不同的版本顺序，快照语义随之崩坏 |
| `Close` 不落盘 MemTable | 数据本来就在 WAL 里，下次 `Open` 重放即可。好处是 Close 快、且"恢复"路径每次打开都必然跑一遍，不容易腐烂 |
| 日志尾部损坏 = 可容忍，文件中间损坏 = 不可容忍 | 前者是崩溃的半截写，丢弃即可；后者说明后面的字节不可信，必须报错而不是继续重放 |
| WAL 记录头校验覆盖"类型字节 + 负载" | 只校验负载的话，类型字段被翻转会得到一条看似完好、语义错乱的记录 |
| `MemTable.Get` 返回 `(value, kind, found)` | 三态（不存在 / 墓碑 / 数据）用 `key.Kind` 表达，比多布尔量清晰；SST 的 `Get` 用同一组返回值 |
| `Comparer` 与 `InternalComparer` 下沉到 `internal/key` | 根包与内部包都要用它，放在内部包可以避免根包被反向依赖；根包保留 `internalComparer` 别名 |
| 文件编号在 SST 与日志之间共享 | `编号大 = 更新` 这一条规则同时对两种文件成立，L0 的"从新到旧"查找顺序才有依据 |

### 9.10 M2 交付物

M1 把闭环跑通了，但读路径还是"每个文件线性扫一遍"。M2 的目标是**让点查不触发全文件扫描**。
这一阶段没有引入任何新的持久化状态（没有 Manifest，没有 Compaction），全部改动都是"把已有的
SST 文件从线性数组改成可分块定位的结构"，因此**旧格式的 SST 文件不再可读**，magic 从
`kvdb0001` 升到 `kvdb0002`，遇到旧文件直接报 `ErrLegacyFormat` 而不是尝试解析。

| 文件 | 内容 |
|---|---|
| `internal/crc/crc.go` | CRC32C（Castagnoli）+ LevelDB 掩码算法的共用实现，WAL 与 SST 共用一份 |
| `internal/filter/bloom.go` | Bloom 哈希（LevelDB 版，小端取字节）与位图置位 / 判定 |
| `internal/filter/block.go` | Filter Block：按 2KB 切段的 Bloom 位图 + 偏移数组 + `baseLg` 字节；写侧 `BlockBuilder`、读侧 `BlockReader` |
| `internal/cache/lru.go` | 16 分片 LRU 块缓存，按 `(fileNum, offset)` 索引、按字节计费；`New(0)` 返回 nil，nil 缓存是安全的空操作 |
| `internal/sst/format.go` | 常量（Footer 长度、magic、块尾长度、重启点间隔）与 Footer 编解码、错误值 |
| `internal/sst/block.go` | Data Block 读写：前缀压缩 + 重启点（每 16 条一个）；块内先二分重启点再线性扫 |
| `internal/sst/writer.go` | 五段式写入：Data Block → Filter → MetaIndex → Index → Footer；块尾带 1 字节压缩类型 + 4 字节 CRC |
| `internal/sst/reader.go` | 打开时载入 Index / MetaIndex（各几 KB）；`Get` 走 **Index 二分 → Bloom 判定 → 读块（先查缓存）→ 块内 seek** |
| `internal/sst/iterator.go` | SST 级迭代器：按需加载数据块，跨块自动推进 |
| `internal/iterator/iterator.go` | 统一的只读迭代器接口（MemTable / SST 都实现它） |
| `internal/iterator/merging.go` | `MergingIterator`：对多个子迭代器做最小堆归并，顺序即"新 → 旧" |
| `internal/iterator/dbiter.go` | `DBIter`：在归并流之上做快照可见性过滤、墓碑屏蔽、同 key 去重、上下界裁剪 |
| `db_iter.go` | `DB.NewIterator` / `DB.GetSnapshot` / `Snapshot` / 导出 `Iterator` 接口 / `IteratorOptions` |
| `internal/crc/crc_test.go` | 掩码往返、与标准库 crc32c 交叉校验、增量更新等价性 |
| `internal/filter/filter_test.go` | 无假阴性（硬保证）、假阳性率、哈希分散度、分段语义、损坏容错 |
| `internal/cache/lru_test.go` | nil 安全、命中/未命中、容量上限、LRU 顺序、并发访问、分片分散度 |
| `internal/sst/block_test.go` | 前缀压缩、重启点、Seek 边界、损坏拒绝、entry 字节布局 |
| `internal/sst/sst_test.go` | 往返、快照/墓碑、多块、**版本同块**、索引记 max key、**点查只读一个块**、过滤器可选、缓存生效、块与索引损坏检测、旧格式拒绝、Footer 布局、空文件、大 value |
| `internal/iterator/iterator_test.go` | 归并顺序/Seek/错误传播；DBIter 的跨层可见性、边界、去重 |
| `db_iter_test.go` | 全源归并、快照隔离、快照跨 Flush 存活、Seek 与边界、Get 返回独立副本、缓存统计、关闭后的迭代器、空库 |
| `cmd/kvdb-bench/main.go` | 从"只测写+顺序读回"扩成 `write / point / scan / sweep` 四种模式，带 Bloom 与缓存的对照组 |

### 9.11 M2 API 一览

```go
// ── package kvdb（db_iter.go）

func (db *DB) NewIterator(opt *IteratorOptions) Iterator
func (db *DB) GetSnapshot() *Snapshot

type Iterator interface {          // 面向 user key 的只读有序迭代器，只支持前向
    SeekToFirst()
    Seek(target []byte)
    Valid() bool
    Key() []byte                   // 只在下次 Seek/Next/Close 前有效，且不得修改
    Value() []byte
    Next()
    Error() error
    Close() error
}

type IteratorOptions struct {
    LowerBound []byte              // 含
    UpperBound []byte              // 含；与 LevelDB 的 ReadOptions 一致，是闭区间
}

type Snapshot struct{ /* 只记一个序列号，不复制数据、不阻塞写入 */ }
func (s *Snapshot) Seq() uint64
func (s *Snapshot) Get(userKey []byte) ([]byte, error)
func (s *Snapshot) NewIterator(opt *IteratorOptions) Iterator
func (s *Snapshot) Release()       // M2 只是标记失效；序列号回收是 M4 的事

// Stats 新增的读路径指标
//   CacheHits / CacheMisses / CacheBytes / CacheItems

var ErrSnapshotReleased error

// ── package kvdb/internal/cache
func New(size int) *Cache                          // size <= 0 → nil（空操作）
func (c *Cache) Get(fileNum, offset uint64) ([]byte, bool)
func (c *Cache) Put(fileNum, offset uint64, block []byte)
func (c *Cache) Stats() Stats                      // Hits / Misses / Bytes / Count
const NumShards = 16

// ── package kvdb/internal/filter
func NewBlockBuilder(bitsPerKey int) *BlockBuilder
func (b *BlockBuilder) AddKey(userKey []byte)      // 注意：喂的是 user key，不是 internal key
func (b *BlockBuilder) StartBlock(blockOffset uint64)
func (b *BlockBuilder) Finish() []byte
func NewBlockReader(contents []byte) *BlockReader
func (r *BlockReader) KeyMayMatch(blockOffset uint64, userKey []byte) bool
func (r *BlockReader) NumRegions() int
func Hash(key []byte) uint32
const PolicyName = "kvdb.BloomFilter"

// ── package kvdb/internal/sst
type WriterOptions struct {
    BlockSize       int
    BloomBitsPerKey int            // 0 → 不建 Filter Block
}
func NewWriter(path string, cmp key.Comparer, opts WriterOptions) (*Writer, error)
func (w *Writer) Add(ik, value []byte) error       // ik 是 internal key；要求严格升序
func (w *Writer) Finish() error
func (w *Writer) Abandon()                         // 放弃并删除半成品文件

type OpenOptions struct {
    Comparer key.Comparer
    Cache    *cache.Cache          // nil → 不走缓存
    FileNum  uint64                // 缓存的键要带文件号
}
func Open(path string, o OpenOptions) (*Reader, error)
func (r *Reader) Get(snapshot uint64, userKey []byte) (value []byte, kind key.Kind, found bool, err error)
func (r *Reader) NewIterator() *Iterator
func (r *Reader) NumBlocks() int
func (r *Reader) FilterEnabled() bool
func (r *Reader) Close() error

var ErrBadFooter error               // footer 不完整 / magic 不对
var ErrLegacyFormat error            // M1 的 kvdb0001 格式
var ErrCorruptBlock error            // 块 CRC 校验失败

// ── package kvdb/internal/iterator
type Iterator interface { /* 与 kvdb.Iterator 同构，内部包用 */ }
func NewMerging(cmp key.InternalComparer, children ...Iterator) *MergingIterator
func NewDBIter(icmp key.InternalComparer, mi Iterator, snapshot uint64, lower, upper []byte) *DBIter
```

### 9.12 M2 验收结果

验收标准三条：**① 点查不触发全文件扫描；② 范围扫描可用；③ `kvdb-bench` 的读延迟不再随文件数线性增长。**

**① 点查不触发全文件扫描** — 由两条证据支撑：

- 单测 `TestPointLookupReadsASingleBlock`：统计一次 `Get` 实际读了多少个数据块，要求恒为 1。
  这条断言就是"不全文件扫描"的形式化表达，回归时会被守住。
- 块尾 CRC + 打开时校验 Index：读路径只碰 Footer / Index / MetaIndex（打开时一次性载入，各几 KB）
  与**一个**数据块。

**② 范围扫描可用** — `MergingIterator` 把 MemTable、Immutable、以及从新到旧的 SST 归并成一条流，
`DBIter` 在其上做可见性 / 墓碑 / 去重 / 边界处理。100 万次量级的扫描在 bench 里跑到 **0.06–0.15 µs/key**，
且 `TestDBIter*` 覆盖了跨层可见性、Seek 语义、闭区间边界、同 key 多版本只出一次。

**③ 读延迟不再随文件数线性增长** — `kvdb-bench -mode sweep` 的实测（10 万 key、value 100B、
随机点查 9 万次 + 负向查询 1 万次、seed 固定）：

| target 文件数 | 实际 sst files | get(hit) | get(miss) | 缓存命中率 |
|---|---|---|---|---|
| 1 | 1 | 2.55 µs | 0.66 µs | 77.4% |
| 2 | 2 | 2.63 µs | 0.87 µs | 77.4% |
| 4 | 4 | 2.84 µs | 1.45 µs | 77.6% |
| 8 | 8 | 2.99 µs | 2.84 µs | 78.1% |
| 16 | 17 | 4.33 µs | 4.11 µs | 74.9% |
| 32 | 34 | 6.11 µs | 7.05 µs | 76.7% |
| 64 | 69 | 11.17 µs | 13.93 µs | 79.0% |

拟合 `get(hit) ≈ 2.22 + 0.126 × files`。**文件数 ×69、延迟 ×4.4** —— 不再是同倍增长。

同一份数据、关掉 Bloom 与块缓存再跑一遍（用来确认曲线变平确实是这两个优化的功劳，
而不是"文件数不够多所以看不出来"）：

| target 文件数 | 实际 sst files | get(hit)（无 Bloom / 无缓存） |
|---|---|---|
| 1 | 1 | 9.27 µs |
| 4 | 4 | 15.90 µs |
| 16 | 17 | 49.15 µs |
| 64 | 69 | 168.11 µs |

拟合 `get(hit) ≈ 7.94 + 2.354 × files`，**线性项系数涨了 18.7 倍**；69 个文件时
168.11 µs vs 11.17 µs，**差 15 倍**。

**与 M1 的同口径对比**：M1 在 3 个 SST、同样 10 万 key / value 100B 下的顺序读回是
**3930 ops/s（254 µs/op）**（记录于 M1 验收，见 9.8）；M2 在 4 个 SST 下同一操作的读回是
**1.72 µs/op**，相差约 **148 倍**。这个对比是"同一台机器、同一操作、同一数据规模"的口径，
因为 M1 的读延迟里绝大部分是逐条解码全文件。

**一句话结论**：M2 之后，一次点查的成本 ≈ **一次数据块读取（常数项，约 2 µs）**
\+ **每多一个文件一次内存里的元数据探测（0.126 µs）**。线性项没有归零 —— M2 没有 Compaction，
L0 文件数无上限，这个线性项要等 M3 的 `L0CompactionTrigger` 才被兜住。
但它的**量级**已经从"每文件一次全文件扫描"变成"每文件一次 Bloom 判定"，
这就是验收标准里说的"不再线性增长"的实际含义。

**测试与静态检查**：`go test ./... -count=1 -cover` 全绿（kvdb 89.6%、cache 96.8%、crc 100%、
filter 94.4%、iterator 86.1%、key 98.1%、memdb 98.6%、sst 79.5%、wal 84.9%）；
`go vet ./...` 与 `gofmt -l` 无输出；`go test -race ./...` 全绿，无 data race 报告 ——
M2 新增的并发结构（分片 LRU 的 16 个分片各自加锁、多个 goroutine 并发读同一 SST 的块缓存路径）
通过了竞态检测。

### 9.13 M2 期间新增 / 细化的决策

| 决策 | 结论与理由 |
|---|---|
| **同一 user key 的所有版本必须落在同一个 Data Block 里** | 块边界只在 user key 变化时才允许切。这是"seek 到一个块就能找到可见版本"的正确性前提：如果同一个 key 的 v1/v3 分在相邻两块，点查只读一块就会读不到快照可见的版本。代价是单个块可能超出 `BlockSize`，可接受。单测 `TestVersionsOfOneKeyStayInOneBlock` 守住。 |
| **Bloom Filter 建在 user key 上，不建在 internal key 上** | internal key 带 `(seq, type)` 后缀，每次更新都会生成一个新 key，用它建 Bloom 等于每个版本都占一份位图，且"key 存在吗"这个问题会被旧版本的位图错误回答。Filter Block 里必须记 user key。 |
| **Filter Block 按 2KB 切段（`BaseLg=11`），段与数据块偏移对应** | 沿用 LevelDB 的做法：一块数据块对应一段 Bloom 位图。判定时先由块偏移算出段号，只查那一段的位图，避免为了一个 key 去碰整张位图（大文件下能把一次判定限制在几百字节内）。`StartBlock` 在写侧记录段边界。 |
| **Bloom 命中"文件损坏"时返回"可能存在"** | 读 Filter Block 解析失败时不报错、也不返回"肯定不存在"，而是保守地当作"可能在"。理由是过滤器只是加速器，不能因为元数据损坏就把一次本来能成功的读判成 miss（那会变成静默的数据丢失）。真正的完整性由数据块的 CRC 把关。 |
| **块尾 = 内容 + 1 字节压缩类型 + 4 字节 CRC32C（掩码后）** | 和 WAL 记录的校验方式统一走 `internal/crc`。M2 还没实装压缩，但长度先按最终形态留出来，M5 加 Snappy 时不需要改格式。 |
| **Footer 定长 48 字节，magic 升到 `kvdb0002`** | `MetaIndex handle + Index handle + 填充 + magic`。换 magic 是刻意的：五段式格式与 M1 的"数据区 + footer"不兼容，遇到 `kvdb0001` 直接报 `ErrLegacyFormat`，比解析出错再报 CRC 失败好定位得多。 |
| **Index 与 MetaIndex 在 `Open` 时一次性读进内存** | 各几 KB，换来"点查不需要任何额外元数据 IO"。打开成本换查询成本，对小文件数场景（L0）是明显划算的。 |
| **索引里记的是"块的最大 key"（max key），不是首 key** | 与 LevelDB 一致：`seekIndex` 找第一个 `max key >= target` 的块，配合"块内再 seek"不会漏。记首 key 在"target 落在块之间空隙"时容易定位错块。单测 `TestIndexRecordsBlockMaxKeys` 守住。 |
| **块缓存的键是 `(fileNum, offset)`，按字节计费，16 分片** | 只用 offset 会在文件编号复用时串味（删了又建的编号相同的文件）。分片用 splitmix64 的 fmix 打散**对齐后**的偏移 —— 数据块偏移都是 4KB 对齐的，直接取低位会让相邻块全落进同一个分片。 |
| **`cache.New(size<=0)` 返回 nil，并在所有方法上容忍 nil 接收者** | 让"关缓存"这条路径完全不需要分支：`Options.BlockCacheSize < 0` 归一化成 0，`New(0)` 给 nil，`Get` 恒 miss、`Put` 直接丢。避免了在热路径上到处写 `if c != nil`。 |
| **`DB.Get` 出口统一复制一次值** | 返回的切片可能直接指向块缓存的内部字节或跳表节点。调用方理所当然地会认为自己拿到的是自己的数据，不改的话"用户改返回的切片"会污染缓存里其他读者看到的内容。单测 `TestGetReturnsIndependentCopy` 守住。 |
| **迭代器接口现在就把 `Close()` 放进去** | M2 的迭代器其实没有需要显式释放的资源（挂的都是 MemTable 指针与已打开的 Reader）。但 M3 引入版本引用计数后 `Close` 会变成必需，现在放进接口就不用到时改签名。 |
| **`Snapshot.Release()` 目前只是标记失效，不回收序列号** | 序列号回收要等 M4 —— 得先知道"没有更旧的读者了"才能让 Compaction 丢掉旧版本。M2 忘记调 Release 不会泄漏任何东西，文档里明确写出来，避免使用者以为是 bug。 |
| **`scanSSTFiles` 只在 `ErrBadFooter` / `ErrCorruptBlock` 时丢弃文件** | 恢复时"丢掉一个坏文件"是很危险的动作。M2 把判据收紧：只有"footer 不完整"（说明是崩溃中断的 Flush，数据还在 WAL 里）与"块 CRC 失败"这两种可容忍情形才丢；`ErrLegacyFormat` 必须往上抛，让用户看见"格式不兼容"，不能静默丢数据。 |
| **`Get` 的负向查询单独统计** | Bloom 的价值只体现在"key 不存在"的查询上，混在一起看平均延迟会把它稀释掉。`-mode point` 把命中与未命中分开报（1.35 µs vs 2.83 µs 级别的差别），`-mode sweep` 里也只保留两列。 |
| **`kvdb-bench` 的 sweep 默认带对照组** | 只有"文件数涨、延迟没涨"这一个现象，说服力不够 —— 读者无法排除"文件数还不够多"。加一组关掉 Bloom 与缓存的对照后，线性项系数差 18.7 倍，"是这两个优化的功劳"才成立。 |

### 9.14 M3 交付物

M2 把读路径的常数项压住了，但**写路径的账还没算**：L0 文件只增不合，那条 0.126 µs/文件 的
线性项会随写入量一直累积，"一次点查要翻 n 个文件"的读放大也跟着涨。

M3 引入两样新的持久化状态 —— **Manifest**（版本变更的追加日志 + `CURRENT` 指针）与**分层版本树**，
以及整个项目里**唯一一个会真正删数据**的组件：**Compaction**。

一句话概括这一阶段：*版本从"内存里的一串文件"变成了"可原子切换、可被多个读者同时持有、
可被持久重建"的对象。* 由此带来三个连锁后果，它们构成 M3 的主要工作量和主要风险：

- **文件有生命周期了** —— 旧版本被替换后，它引用的文件不能立刻删（可能还有迭代器在读），
  于是需要引用计数 + 垃圾回收；
- **恢复要能重建版本** —— 靠 Manifest 重放而不是每次都扫目录；
- **`Close` 变得有意义** —— 要等后台两条流水线排空并把最后的垃圾收干净。

| 文件 | 内容 |
|---|---|
| `internal/version/edit.go` | `VersionEdit` / `FileEdit` 的二进制编码（Manifest 的每条追加记录）+ 解码错误值；标签式布局，便于将来加字段而不破坏老记录 |
| `internal/version/version.go` | `FileMeta`、`Version`（不可变的分层文件清单 + 引用计数）、`VersionSet`（当前版本、文件编号 / 序列号 / log number 分配、`LogAndApply`、`SetFromScan`、`LiveFileNums`） |
| `internal/version/manifest.go` | `Manifest`（复用 WAL 记录格式的追加日志）、`CURRENT` 的读写与原子替换、`Recover`（重放 + 尾部截断容忍 + 比较器校验）、`NewManifest`（换新日志并按编号删旧的） |
| `internal/compact/compact.go` | `LevelConfig`、`Compaction`、`Pick`（L0 按文件数、L1 以下按容量）、`span` / `Overlapping` |
| `internal/compact/run.go` | `Run`：多路归并 → 丢弃被覆盖的旧版本与可退休的墓碑 → **在 user key 边界切分输出** → 走 `Commit` 原子替换版本；失败时把半成品文件删掉 |
| `db_compact.go` | 后台 Compaction 协程、每轮重新规划、`commitCompaction`（先开 reader → 再登记 → 最后提交）、快照登记与 `releaseVersion` |
| `db_flush.go` | 恢复时对孤儿文件分类（`DiscardedFiles` vs `ObsoleteFiles`）、Flush 提交改走 Manifest、`removeUnreferencedSSTs` + 关库兜底回收 |
| `db.go` | `Stats` / `LevelStats` / `CompactionStats`、`RecoveryReport` 扩充、`Close` 等后台排空并做最后一次 GC |
| `db_iter.go` | `NewIterator` / `GetSnapshot` 持有版本引用并登记存活快照 |
| `options.go` | `L0CompactionTrigger` / `LevelBaseSize` / `LevelSizeMultiplier` / `MaxLevels` 从"已存在但没人读"变成真正接入；新增 `targetFileSize` |
| `internal/version/version_test.go` | 17 条：`VersionEdit` 编解码与坏输入拒绝、L0 按编号 / 深层按 smallest 排序、`FindFile` 二分（含"同 key 更旧版本"回归）、`Overlapping`、引用计数保持旧文件存活、Manifest 重建版本、尾部截断丢弃、比较器不匹配拒绝、`unrefLocked` 无自死锁回归 |
| `internal/compact/compact_test.go` | 11 条：Picker 三条触发规则、丢弃被覆盖版本、退休 / 保留墓碑、尊重 `SmallestSnapshot`、**输出在 user key 边界切分**、提交失败删输出、重建层布局、同 key 平局取新 |
| `db_compact_test.go` | 11 条验收测试：L0 有界、向深层溢出、读放大在 10 倍数据后仍有界、快照 / 迭代器跨 Compaction 存活、重开保持层布局、M2 目录迁移、孤儿文件分类、`Stats` 分层与放大、旧数据目录可开、反复开关不留残渣 |
| `cmd/kvdb-bench/main.go` | 输出分层布局 + **写放大**（`wal + flush + compaction-out / user`）+ **读放大**（`probes/get`）+ Compaction 规模；头部打印 `l0 trigger` 与 `level sizing` |

### 9.15 M3 API 一览

```go
// ── package kvdb（db.go / db_compact.go）

func (db *DB) Stats() Stats
func (db *DB) RecoveryReport() RecoveryReport

type LevelStats struct{ Files int; Bytes uint64 }

type CompactionStats struct {
    Count, InputFiles, OutputFiles int64
    InputBytes, OutputBytes        uint64
    DroppedRecords                 int64 // 被覆盖的旧版本 + 可退休的墓碑
}

type Stats struct {
    Files, MemTableSize, HasImmutable, LastSequence, ObsoleteLogs
    CacheHits, CacheMisses, CacheBytes, CacheItems
    Levels     []LevelStats  // 索引即层号；L0 的文件数应当稳定在 L0CompactionTrigger 附近
    Compaction CompactionStats
    FlushBytes, WALBytes   uint64
    Gets, ReadProbes       int64  // 读放大 = ReadProbes / Gets
}

type RecoveryReport struct {
    TornLogs          []uint64 // 尾部损坏、被截断恢复的日志编号
    DiscardedFiles    []string // 残缺的残片：footer 不全 / 块校验失败（根本不该存在）
    ObsoleteFiles     []string // 完整但提交没成功（曾经想提交，没提交成）
    TruncatedManifest bool
    RecoveredFromScan bool     // 没有 Manifest，文件列表来自目录扫描（M2 目录迁移）
    ReplayedRecords   int
}

// ── package kvdb/internal/version

type FileMeta struct{ Num, Size uint64; Smallest, Largest []byte } // 区间是 internal key
type FileEdit struct{ Level int; Num, Size uint64; Smallest, Largest []byte }

type VersionEdit struct {
    ComparatorName string // 只在第一条记录里出现，用于校验目录与配置是否匹配
    NextFileNum    uint64
    LastSeq        uint64 // 修掉 M2 的缺陷：不再只依赖重放 WAL 恢复
    LogNumber      uint64
    Added, Deleted []FileEdit
}
func (e *VersionEdit) Encode() []byte
func DecodeVersionEdit(buf []byte) (*VersionEdit, error)

type Config struct { Dir string; ICmp key.InternalComparer; ComparerName string; MaxLevels int; LogNumber, NextFileNum uint64 }
type VersionSet struct{ /* 当前版本 + 全部存活版本 + 计数器 */ }

func New(cfg Config) *VersionSet
func (vs *VersionSet) Recover() (hasManifest, truncated bool, err error)
func (vs *VersionSet) NewManifest() error
func (vs *VersionSet) LogAndApply(e *VersionEdit) error
func (vs *VersionSet) SetFromScan(files []*FileMeta, maxFileNum uint64) error
func (vs *VersionSet) Current() *Version
func (vs *VersionSet) AllocFileNum() uint64
func (vs *VersionSet) RaiseNextFileNum(n uint64)      // 恢复时把游标抬到已见的最大编号之上
func (vs *VersionSet) LastSeq() uint64 / SetLastSeq(seq uint64)
func (vs *VersionSet) LogNumber() uint64 / SetLogNumber(n uint64)
func (vs *VersionSet) LiveFileNums() map[uint64]bool  // 垃圾回收的判据
func (vs *VersionSet) Close() error
var ErrNotOpen error

func (v *Version) Ref() / Unref() / RefCount() int
func (v *Version) NumLevels() int
func (v *Version) Files(level int) []*FileMeta
func (v *Version) FileCount() int / LevelBytes(level int) uint64 / AllFiles() []*FileMeta
func (v *Version) FindFile(level int, userKey, target []byte) *FileMeta
func (v *Version) Overlapping(level int, smallest, largest []byte) []*FileMeta

func ManifestName(dir string, num uint64) string
func IsManifestName(name string) bool

// ── package kvdb/internal/compact

type LevelConfig struct {
    L0CompactionTrigger int
    MaxLevels           int
    LevelMaxBytes       func(level int) uint64 // L0 返回 0，表示没有容量上限
    TargetFileSize      func(level int) uint64
}

type Compaction struct {
    Level, OutputLevel int            // OutputLevel 恒为 Level+1
    Inputs             [2][]*version.FileMeta
}
func (c *Compaction) InputFiles() []*version.FileMeta
func (c *Compaction) InputBytes() uint64
func Pick(v *version.Version, cfg LevelConfig) *Compaction // 无事可做时返回 nil

type Env struct {
    Dir                            string
    ICmp                           key.InternalComparer
    BlockSize, BloomBitsPerKey     int
    TargetFileSize                 uint64
    SmallestSnapshot               uint64 // seq <= 它的旧版本可以丢弃
    AllocFileNum                   func() uint64
    Reader                         func(num uint64) (*sst.Reader, error)
    Commit                         func(*Compaction, []*version.FileMeta) error
}

type Result struct{ InputFiles, OutputFiles int; InputBytes, OutputBytes uint64; InputRecords, OutputRecords, DroppedRecords int }
func Run(c *Compaction, v *version.Version, env Env) (Result, error)
```

### 9.16 M3 验收结果

M2 结束时列出的 M3 验收标准，逐条交代如下。压测口径统一为 `kvdb-bench -mode all -memtable-size 1048576`
（10 万 / 50 万 key、value 100B、块缓存 8MB、Bloom 10 bits/key、`L0CompactionTrigger=4`、
`LevelBaseSize=256MB`），跑在本机同一台机器上。

① ② 是当初写下的两条硬指标；③ ④ ⑤ ⑥ 不是额外加码，而是"①② 能成立"所依赖的不变式 ——
没有它们，前两条只是两个恰好好看的数字。

**① L0 文件数稳定在触发阈值附近，不再随写入量增长** —— 数据量 ×5，L0 恒为 **3** 个文件
（阈值 4，收敛在阈值之下）：

| 规模 | sst files | L0 | L1 | memtable live | write amp | read amp |
|---|---|---|---|---|---|---|
| 10 万 key | 17 | **3** (1.87 MB) | 14 (8.71 MB) | 872 KB | 2.89x | 3.54 probes/get |
| 50 万 key | 89 | **3** (1.87 MB) | 86 (53.48 MB) | 166 KB | 3.12x | 3.90 probes/get |

M2 里这一列文件数会随 Flush 次数线性增长（写 50 万 key、1MB MemTable 就是约 86 次 Flush）；
M3 之后它被钉在触发阈值附近，**增长的是 L1 的容量而不是 L0 的文件数**，
这就是"分层"的实际含义。单测 `TestCompactionKeepsL0Bounded` 把这条断言固化了：
它额外要求"文件总数 ≤ 30"（几十次 Flush 最后只剩十几个文件），比"L0 < 阈值"更能说明收敛。

**② 写入 10 倍数据后读放大收敛为常数** —— 上表里 **3.54 → 3.90**，数据 ×5 而读放大 ×1.10。
读放大的定义是"一次点查平均向几个 SST 发起查找"（`ReadProbes / Gets`），
它是 Bloom Filter **挡不掉**的那部分成本：Bloom 只让"碰一次"变便宜（一次哈希查内存），
真正让**次数**降下来的只有 Compaction。理论上界是"L0 每个文件 + 每层各一个候选文件"，
所以它是个**小常数**，与数据量无关 —— L0 收敛在阈值以下、层数按 `log_10` 增长，
两项都不会随写入量爆掉。

单测 `TestReadAmplificationStaysBoundedAfterTenfoldGrowth` 用固定的一批 key（保证已被挤出
MemTable，测的是真读路径）在 2000 → 20000 key 两端各测一次，断言 `big ≤ 6` 且 `big ≤ small + 2`。
它比 bench 更严格的地方是**只在稳态测量**：必须等到 `Compaction.Count > 0` 才取数，
否则会在"L0 还没开始搬"的窗口里测到一个虚高的值。

**③ 分层结构逐层长出来，且 L1 以下同层不重叠** —— `TestCompactionOverflowsIntoDeeperLevel`
把 `LevelBaseSize` 压到 64KB，于是 L1 会被撑满并往 L2 搬，验证"每层只往下走一层"的阶梯确实会
依次点亮。`checkLevelInvariant` 在多个测试里反复检查：**L1 以下的文件区间两两不重叠**，
这是点查能把每层定位成"一次二分"的前提。`TestRunSplitsOutputAtUserKeyBoundary` 专门守住它的
写侧来源：输出只在 user key 变化处切分，绝不在 internal key 中间切。

**④ 读视图在 Compaction 期间保持稳定** —— `TestSnapshotSurvivesCompaction`：
在一次 Compaction 跨越两个快照之间时，两个快照各自读到的仍是它们该看到的版本；
`TestIteratorSurvivesCompaction`：迭代器打开期间即使输入文件被 Compaction 合并掉，
迭代仍能读完全部 key。`smallestSnapshot` 这个丢弃上界的唯一来源是**登记在册的快照**。

> 这里原本写成"迭代器也会登记存活快照"，与代码不符，M4 期间核实时订正：
> 迭代器和快照是**两套不同的保护机制**，各自解决一半的问题（详见 9.21）——
>
>   - **快照**只记一个序列号，靠**登记**把自己写进 `db.snapshots`，作用是抬高
>     `smallestSnapshot`，让 Compaction 保留它还要读的那些**旧版本**；
>   - **迭代器**直接持有当时的**版本引用**（`Version.Ref`），靠引用计数让那批
>     **输入文件**在读完之前删不掉。它读的永远是那批文件，而 Compaction 只会写新文件，
>     所以**不需要**登记快照 —— 反过来说，如果给迭代器也登记快照，一个长命迭代器
>     会把丢弃上界永久钉在它打开时的序列号上，白白拦住旧版本的回收。

**⑤ 崩溃与迁移路径都能恢复，且不留残渣** ——
`TestRecoveryClassifiesOrphanFiles` 区分两类孤儿：完整的进 `ObsoleteFiles`、
残缺的进 `DiscardedFiles`；`TestMigrateLegacyDirectoryWithoutManifest` 验证 M2 的
"没有 Manifest 的目录"第一次打开时会走目录扫描并把结果固化成第一份 Manifest；
`TestNoStrayFilesAfterCycles` 反复开关之后目录里最多只留一份 Manifest。
`TestReopenPreservesLevelLayout` 要求重开后**已经在 L1 以下的文件仍留在原层**
（只允许 WAL 重放多出至多一个 L0 文件）。

**⑥ 墓碑真的被物理删掉、旧版本真的被丢弃** —— `TestRunDropsCoveredVersions`（覆盖写留下的老值被丢）、
`TestRunRetiresObsoleteTombstone`（使命已完成的墓碑被退休）、`TestRunKeepsTombstoneAboveDeeperLevel`
（墓碑下面还有更深层的数据时**不能**退休）、`TestRunRespectsSmallestSnapshot`（快照还要读的版本不能丢）。
需要说明的是：上面 bench 的 `compaction` 一行显示 `dropped 0 records` —— 这是**预期**的，
因为该负载全是唯一的 key，没有覆盖写也没有删除，本来就没有任何记录该被丢。

**测试与静态检查**：`go test ./... -count=1 -cover` 全绿 —— kvdb 83.0%、cache 96.8%、
compact 86.9%、crc 100%、filter 94.4%、iterator 86.1%、key 98.1%、memdb 98.6%、
sst 76.2%、version 78.5%、wal 84.5%。
`go vet ./...` 与 `gofmt -l .` 无输出；`go test -race ./...` 全绿 —— 新增的两处并发结构
（后台 Compaction 协程与前台读写的版本引用计数、`releaseVersion` 归零时的 GC）
**无 data race 报告**。

> **两条需要如实说明的边界**：
>
> 1. **kvdb 包覆盖率从 M2 的 89.6% 降到 83.0%**，不是质量退化，而是 M3 新增了
>    `db_compact.go` 里大量只在异常路径或关库竞态才走到的分支（提交失败回滚、
>    `errClosing` 中断、关库兜底 GC）。这些分支由单测与崩溃测试覆盖，但因为
>    "每行都要跑到"在并发代码里代价过高，覆盖率的分母涨得比分母快。
> 2. **bench 里 `get(hit)` 从 3.47 µs 涨到 10.34 µs，涨幅不代表读路径退化**。
>    数据从 11MB 涨到 55MB，而块缓存固定 8MB，命中率从 86.7% 掉到 65.2% ——
>    这个涨幅来自"缓存装不下、要去磁盘读块"，属于缓存容量问题（M5 的调优范围），
>    不是读放大问题：同一时间段里探测次数只从 3.54 涨到 3.90。
>    把这两件事分开看，才是验收标准"读延迟不显著退化"的正确读法。

### 9.17 M3 期间新增 / 细化的决策

| 决策 | 结论与理由 |
|---|---|
| **恢复时把"孤儿文件"分成两类，而不是一律删除** | 两者成因不同：`DiscardedFiles` 是崩溃中断的那次写入（footer 不全，**根本不该存在**），`ObsoleteFiles` 是文件写完并 fsync 了但 Manifest 那次提交没落盘（**曾经想提交但没提交成**）。判据是"能不能正常打开"：能打开 → obsolete，报 `isDiscardableSSTError` → discarded，其余错误一律上抛。混在一起报会让用户无法区分"崩溃丢了一次写入"和"格式坏了"。单测 `TestRecoveryClassifiesOrphanFiles` 守住。 |
| **`VersionSet.mu` 内不能调 `Unref`，改用 `unrefLocked`** | `Unref` 归零时要拿 `VersionSet.mu` 把自己从存活集合里摘掉，而 `LogAndApply` / `SetFromScan` / 重放路径**本来就持有这把锁** —— `sync.Mutex` 不可重入，于是构成自死锁。这个 bug 在 DB 层被**侥幸掩盖**：`db.v` 永远持有一个长期引用，旧版本永远到不了 0，所以只有单独测 `version` 包才会暴露。修法是拆出 `unrefLocked()`（调用方持锁）与 `removeLiveLocked`，凡是持锁路径一律用前者。回归测试 `TestLogAndApplyReleasesOldVersionWithoutDeadlock` 守住（修之前它会卡到超时）。 |
| **`NewManifest` 删旧 Manifest 要按编号删，不能按句柄删** | 恢复之后 `vs.manifest` 句柄是 nil，但旧的 `MANIFEST-*` 文件还实实在在躺在目录里。只删"有句柄的那份"会导致每次重开都多留一份 Manifest（实测累积到 3 份）。改成按 `manifestNum` 拼路径删，`TestNoStrayFilesAfterCycles` 守住。 |
| **`Close` 之后要补一次垃圾回收（`collectGarbageFinal`）** | 存在这样一个窗口：一次 Compaction 提交完新版本、输入文件失去了最后一个引用，但它收尾的那次 `collectGarbage` 恰好撞上 `db.closed == true`（关库正在进行），于是被跳过 —— 结果就是"数据全对，但目录里留了一个谁也引用不到的 SST"。修法是在 `bgWG.Wait()` 之后调一次 `collectGarbageFinal()`，它走 `removeUnreferencedSSTs(allowClosed=true)`，跳过 closed 检查。 |
| **删除失败时不要把 reader 从表里摘掉** | GC 的顺序改成"先 `os.Remove` 成功、再删 `db.readers` 里的条目"。反过来写的话，一次删除失败（例如 Windows 上文件仍被占用）会让 reader 表里少一个条目，而文件还在 —— 后续任何一次读都会报"没有登记的读取器"，一个瞬时的 IO 错误被放大成永久性故障。 |
| **`LastSeq` 必须写进 Manifest，不能只靠重放 WAL 恢复** | 这是 M3 顺带修掉的 M2 缺陷：如果崩溃发生在"所有 MemTable 都已落盘、当前 WAL 是空的"这个**完全正常**的时刻，重启后 `lastSeq` 会退回 0，新写入的序列号就会和 SST 里已有的老记录撞号 —— 那是静默的数据正确性问题。Manifest 里每次提交都带上 `LastSeq`，这个问题才被根治。 |
| **Compaction 每次只往下走一层** | 一次跨多层的归并会把写入量放大到不可控（一次 Compaction 重写好几层的数据），而且中间层的旧版本会被同时清掉，出错时无法定位是哪一层的问题。逐层搬运的代价是多跑几次，换来的是每步都可解释、可回归。`TestCompactionOverflowsIntoDeeperLevel` 验证这条阶梯确实会逐层点亮。 |
| **输出文件只在 user key 变化处切分** | "同一 user key 的所有版本必须落在同一文件里"（M2 的块级不变式的文件级版本）。在 internal key 中间切会把一个 key 的 v1/v3 劈到两个文件里，点查只命中一个文件时就会漏掉可见版本 —— 又是静默的数据损坏。`TestRunSplitsOutputAtUserKeyBoundary` 同时断言不重叠、且 `FileMeta` 里记的区间与文件实际内容一致。 |
| **`Inputs[1]`（输出层重叠文件）必须一起参与归并，不能省** | 输出文件的 key 区间由"输入层选中文件 ∪ 输出层重叠文件"的并集决定。只搬 `Inputs[0]` 的话，输出必然和输出层里某个已有文件区间重叠，**"L1 以下同层不重叠"当场破掉**，之后二分会定位到错的文件。这不是优化，是正确性前提。 |
| **`Commit` 三步的顺序：先开 reader → 再登记 → 最后提交版本** | 反过来会出现一个窗口："版本已经指向新文件、但读取器表里还没有它"，那一刻的读会报"没有登记的读取器"而不是拿到数据。所以 `commitCompaction` 把最贵的"打开输出文件"放在锁外做，进锁后先登记 reader 再落 Manifest。 |
| **`Pick` 无事可做时返回 nil，而不是返回一个空 Compaction** | `runCompactions` 的循环因此有了干净的终止条件（`c == nil` 就退出本轮）。每轮都从**当前版本**重新挑一次，而不是一次挑完排队 —— 上一轮的结果会改变各层规模，也改变了下一轮该挑谁。 |
| **`kvdb-bench` 的写放大分子不含"Compaction 读进来的字节"** | 写放大按 LSM 里通用的定义：`(WAL + Flush + Compaction 输出) / 用户放入的字节`。Compaction 的**输入**字节不计入分子 —— 那是读放大的一部分，混进写放大会让两个指标互相污染、都失去诊断价值。读放大单独按 `probes/get` 报。 |

### 9.18 M4 交付物

M4 的标题是"一致性"，但**这一阶段真正新写的东西只有一样：Group Commit**。

三件交付物里，`WriteBatch` 在 M1 就已落地（`batch.go`：批次编码成一条 WAL 记录，
原子性由"一条记录"保证），`Snapshot` 在 M2 就已落地（`db_iter.go` 的序列号视图 +
M3 的 `db.snapshots` 登记表）。M4 对它们做的是三件事：补上语义边界、补上可观测性、
用并发的用例把它们真正锁住 —— 因为"单线程下正确"和"并发下正确"完全不是一回事。

于是这一阶段的全部设计压力都落在写路径上：**M3 的写路径是"一个写者持 `db.mu`，
从头到尾做完 WAL 追加 + fsync + MemTable 插入"**。它正确，但有两个后果：

- **每个写者各做一次 fsync**。fsync 是写路径唯一的硬成本，N 个并发写者就要付 N 次；
- **fsync 期间持有 `db.mu` 写锁**，读者的每一次点查都要排在磁盘等待后面。

Group Commit 就是针对这两条：把同时排队的写者合并进一次 fsync，并且把 fsync
挪到锁外。一句话概括：*写队列从 `db.mu` 里独立出来，队长替全组做一次 fsync*。

| 文件 | 内容 |
|---|---|
| `db_write.go` | **新增**。写队列的全部实现：`writeRequest`、队长/跟随者模型、`runWriteGroup`（分配序列号 → 追加 → **一次 fsync** → 落 MemTable）、`finishWriteGroup`（发结论 + 决定队长是否续任）、`drainWrites`（关库前排队列）、`applyWriteGroup`、`failLocked` 的写侧入口 |
| `db.go` | `Write` 从"持 `db.mu` 直接落盘"改成"排进写队列"；`DB` 新增 `wmu/wcond/wqueue/wleader/wclosing`；`Close` 前置 `drainWrites`；`Stats` 新增 `WriteGroups/WriteBatches/MaxWriteGroup/WALFsyncs/LiveSnapshots`；`counters` 同步扩充 |
| `db_compact.go` | `setBgErr` 与新加的 `failLocked` 合并成**唯一一处"停库"入口**（持锁版本），写路径与后台路径共用 |
| `db_iter.go` | `Snapshot` 的语义注释补齐（它护住的是旧版本，与迭代器的版本引用是两套机制，见 9.21） |
| `db_concurrent_test.go` | **新增**。9 条并发用例：组提交合并率、关 SyncWrites 的对照组、并发写不丢记录不串号、批次原子可见、快照隔离、快照挡住 Compaction 丢弃、**写失败停库**、关闭与并发写的竞态、混合负载压测 |
| `cmd/kvdb-bench/main.go` | 新增 `-mode group -writers 1,2,4,8,16,32`：按并发写者数扫描，输出 ops/s / 提交组数 / fsync 次数 / 合并率 / 每次 fsync 的墙钟代价；`printResult` 也补了组提交一行 |
| `doc.go` | 包文档更新到 M4（原先还停在 M1） |

### 9.19 M4 API 一览

```go
// ── package kvdb（db_write.go）
//
// 对外签名一个都没变，变的是 Write 的并发语义与失败语义。

func (db *DB) Write(b *WriteBatch) error

// 组提交：并发写者排进队列，队长替全组做一次 fsync。
// 返回 nil 表示这一批已经落盘并对后续读可见；错误分两类：
//   - ErrClosed / 已存在的后台错误 → 这一批一定没写进去；
//   - 磁盘故障                     → 这一批可能已经写进去了，且整个库会停下。

// 内部结构（不导出）：
type writeRequest struct {
    batch *WriteBatch
    seq   uint64 // 起始序列号，由队长分配
    err   error  // 由队长在 finishWriteGroup 里填
    done  bool
}

func (db *DB) runWriteGroup() ([]*writeRequest, error)
func (db *DB) beginWriteGroup(group []*writeRequest) (firstSeq, lastSeq uint64, log *wal.Log, err error)
func (db *DB) appendWriteGroup(log *wal.Log, group []*writeRequest) error
func (db *DB) applyWriteGroup(group []*writeRequest, firstSeq, lastSeq uint64, walErr error) error
func (db *DB) takeWriteQueue() []*writeRequest
func (db *DB) finishWriteGroup(group []*writeRequest, err error) (more bool)
func (db *DB) drainWrites()

const collectYields = 4 // 队长开始 fsync 前"让出调度"的次数，见 9.21

// ── Stats 新增字段（db.go）

type Stats struct {
    // ...（M3 的字段不变）
    WriteGroups   int64 // 提交组数
    WriteBatches  int64 // 参与组提交的批次数，≈ Write 调用次数
    MaxWriteGroup int64 // 观察到的最大组大小
    WALFsyncs     int64 // 实际执行的 WAL fsync 次数；SyncWrites 为假时恒为 0
    LiveSnapshots int   // 尚未 Release 的快照数
}

// 合并率 = WriteBatches / WriteGroups
// fsync 摊销 = WriteBatches / WALFsyncs   ← 这个比值就是写吞吐相对"每条写一次 fsync"的倍数

// ── 写路径的并发不变式（db_write.go 里以注释形式写死，测试逐条守住）

// 1. len(wqueue) > 0 ⟹ wleader   —— 队列里有东西就一定有队长（finishWriteGroup 保证）
// 2. db.lastSeq 只在整组落进 MemTable 之后推进 —— 读者不会看到未落地的"已提交点"
// 3. 整组在同一个 db.mu 临界区里应用       —— 不存在"半组可见"
// 4. Close 与写入之间没有中间态            —— drainWrites 排空队列之后才轮到 db.closed
```

### 9.20 M4 验收结果

验收标准是**"并发压测下 race detector 无告警"**。但"没有告警"本身太弱 ——
一个把写入彻底串行化的实现同样没有告警。所以下面按三段交代：① 竞态检测；
② 组提交真的在合并 fsync（这是 M4 唯一改变写路径吞吐行为的地方，也是它存在的
全部理由）；③ 几条在任何交错下都必须成立的不变式，以及它们被哪条用例锁住。

**① race detector 无告警**

```text
$ go test -race ./... -count=1
ok      kvdb                    38.997s
ok      kvdb/internal/cache      2.486s
ok      kvdb/internal/compact    3.075s
ok      kvdb/internal/crc        2.070s
ok      kvdb/internal/filter     2.249s
ok      kvdb/internal/iterator   2.207s
ok      kvdb/internal/key        2.191s
ok      kvdb/internal/memdb      2.198s
ok      kvdb/internal/sst        2.562s
ok      kvdb/internal/version    2.742s
ok      kvdb/internal/wal        2.276s
```

M4 新增的并发结构全部通过：写队列（队长/跟随者 + 条件变量）、`Close` 与在途提交的
握手、以及"把 fsync 挪出 db.mu"带来的读者-写者新交错。

**② 组提交的合并率** —— `kvdb-bench -mode group -writes 3000 -writers 1,2,4,8,16,32`
（本机 Windows / go1.27.1 / `SyncWrites = true`；写者各自写一段不重叠的 key 空间，
写完逐个读回校验）：

| writers | ops/s | 提交组数 | **fsync 次数** | **合并率** | 最大组 | us/write | us/fsync |
|---|---|---|---|---|---|---|---|
| 1 | 487 | 3000 | 3000 | 1.00 | 1 | 2053 | 2053 |
| 2 | 975 | 1527 | 1527 | 1.96 | 2 | 1026 | 2015 |
| 4 | 1934 | 774 | 774 | 3.88 | 4 | 517 | 2004 |
| 8 | 3813 | 388 | 388 | 7.73 | 8 | 262 | 2028 |
| 16 | 6689 | 205 | 205 | 14.63 | 16 | 150 | 2188 |
| 32 | **12504** | 110 | **110** | **27.27** | 32 | 80 | 2181 |

（"提交组数"与"fsync 次数"逐行相等不是笔误：`SyncWrites = true` 时一组恰好一次 fsync，
这也是为什么合并率既能按组算也能按 fsync 算。关掉 `SyncWrites` 时后者恒为 0。）

三件事同时被这张表说清楚了：

- **写入吞吐 ×25.7（487 → 12504）、fsync 次数 ÷27.3（3000 → 110）**。写吞吐的分子
  是用户写入、分母是 fsync，所以"ops/s 涨而 fsyncs 不涨"就是组提交在起作用的
  **定义**，不需要额外论证。
- **`us/fsync` 这一列几乎不动（2.0～2.2 ms）**。它说明 fsync 的代价是这台机器上一个
  固定的物理量，W=1 时的 2053 µs/write 就是"一次 fsync 换一次写"的直接后果。
  换句话说：这张表的每一行都受同一个常数约束，`ops/s ≈ 1000 / 2.1ms × 合并率`。
- **合并率随写者数单调上升，接近 W-1 的上界**（32 个写者时平均 22～28，三次重复
  实测为 27.78 / 22.39 / 27.27）。它没有等于 W-1，原因见 9.21 里 `collectYields` 那条。

单测层面把同一件事固化成断言（`TestGroupCommitAmortizesFsync`）：16 个并发写者各写
64 次，断言 `WriteBatches == 1024`、`WALFsyncs > 0`、**`WALFsyncs < WriteBatches`**、
`MaxWriteGroup >= 2`。配套的对照组 `TestWALFsyncsDisabledWhenSyncWritesOff` 断言
`SyncWrites = false` 时 `WALFsyncs == 0` —— 没有这条，上面那个"≪"就不能说明任何事。

**③ 并发下的不变式与它们对应的用例**

| 不变式 | 用例 | 断言方式 |
|---|---|---|
| 并发写不丢记录、不串号 | `TestConcurrentWritersKeepAllRecords` | 8 写者 × 200 条，全部读回；且 `LastSequence` 恰好 == 1600（序列号必须**稠密**：重号会偏小、跳号会偏大） |
| **批次原子性在并发下成立** | `TestWriteBatchesStayAtomicUnderConcurrentWrites` | 每个批次把同一个值写进一对 key，4 个读者在同一快照上读这一对，要求"都没出现 / 都是同一个值"，绝不允许一边有一边无 |
| 快照隔离 | `TestConcurrentOverwritesThenSnapshotIsolation` | 4 写者猛覆盖同一个 key，读者在快照内连读两次必须完全一致 |
| 存活快照挡住旧版本丢弃 | `TestSnapshotPinsOldVersionsAcrossCompaction` | 取快照 → 大量覆盖写逼出多轮 Compaction → 快照仍读到 v1；`Release` 后读到 v2、`LiveSnapshots` 归 0 |
| **写失败 = 整组失败 + 停库** | `TestWALFailureStopsTheDatabase` | 关掉日志句柄制造真实写失败：失败的那批读不到、之前的写入不受影响、后续写入拿到同一个错误、**只读仍然可用** |
| 关库与在途写不互相等待 | `TestCloseWhileWritersRunning` | 8 个写者边写边关库（5 轮）：写入只允许返回 nil 或 `ErrClosed`，`Close` 必须返回，关库后的 `Put` 立刻拿 `ErrClosed` 而不是挂住 |
| 混合负载（写 + 点查 + 快照 + 扫描 + Flush + Compaction） | `TestConcurrentMixedWorkload` | 迭代顺序严格递增、值形态合法、收尾逐条复核；这条是用例里最能压到竞态的（也是 `-race` 下的主要负载） |

**④ 测试与静态检查**：`go test ./... -count=1 -cover` 全绿 —— kvdb 84.2%、
cache 96.8%、compact 86.9%、crc 100%、filter 94.4%、iterator 86.1%、key 98.1%、
memdb 98.6%、sst 76.2%、version 78.5%、wal 84.5%（kvdb 包比 M3 的 83.0% 略有回升，
因为新加的并发用例把 `db.go` 上原先只有异常路径才走到的分支盖住了）；
`go vet ./...` 与 `gofmt -l .` 无输出。

> **一条需要如实说明的边界**：写路径上那几个**致命错误分支**（`appendWriteGroup`
> 里的 fsync 失败、`beginWriteGroup` 里 `db.closed` 的兜底）在正常测试里走不到。
> 前者由 `TestWALFailureStopsTheDatabase` 用"关掉日志句柄"制造，后者**不可达**——
> 因为 `drainWrites` 先关门、再等队长、最后才允许 `Close` 置 `db.closed`，
> 于是"已关库但仍在提交"这个状态根本不存在。它作为防御性检查保留，但不计入覆盖率。

### 9.21 M4 期间新增 / 细化的决策

| 决策 | 结论与理由 |
|---|---|
| **写队列用自己的一把锁（`wmu`），不复用 `db.mu`** | `db.mu` 保护的是全局状态（MemTable、版本、序列号水位），而队列要解决的是"谁来做这次 fsync"。混在一起就会出现"写者为了排队而拿写锁"的荒唐局面。分开之后队长做 fsync 时**既不持 `db.mu`（读者照常读），也没有让别人空等（后来的写者只是在队尾排队，正好凑成下一组）**。"多少个写者合并进一次 fsync"因此完全由并发度决定，不需要任何启发式。 |
| **队长必须"排空队列才让位"，不能"提交一组就让位"** | 这是 M4 里唯一一个**实测挂死过**的设计错误。原先的写法是"提交完这一组 → 清空 `wleader` → 广播"，但队长清空 `wleader` 的那一瞬间，队列里可能还留着刚刚排进来的写者 —— 它们看到的 `wleader` 已经是 false，于是谁也不会去提交它们，那批写者永远等不到 `done`。现象是 16 个 goroutine 全部卡在 `wcond.Wait` 而场上**没有队长**。修法是把"还要不要继续当队长"和"清空 `wleader`"放进同一个 `wmu` 临界区：队列非空就继续当队长，队列空才让位。由此得到一条有用的不变式：**`len(wqueue) > 0 ⟹ wleader`**，队列里有东西就一定有队长。 |
| **`finishWriteGroup` 必须先写 `done` 再 `Broadcast`** | 反过来的话，被唤醒的写者可能重新检查时看到 `done == false` 又回去睡，而此刻已经没有人会再唤醒它。这类丢失唤醒一旦发生就是不可恢复的挂起，所以顺序在这里是语义的一部分，不只是风格问题。 |
| **`drainWrites` 必须"先关门、再等队长"** | 关库要等队列排空。如果先等队长停下来再关门，那么在持续写入的负载下队长永远有人可提交、队列永远不空，`Close` 会一直等下去（测试里表现为超时）。先把 `wclosing` 置上、把新写者挡在 `ErrClosed` 之外，队列就只会变短，队长一定能停。 |
| **队列排空必须排在 `Close` 拿 `db.mu` 之前** | 队长在后半段要拿 `db.mu`（把整组落进 MemTable），而 `Close` 一旦先拿住 `db.mu` 再去等队长，两边就是互相等待。锁序被固定为：`wmu`（且不重叠持有）→ `db.mu`。 |
| **`collectYields = 4`：队长开始 fsync 之前让出几次调度** | 队长由"第一个到达的写者"担任，而它到达时队列里往往只有它自己 —— 上一组刚被放行的写者还没走完"唤醒 → 返回 `Write` → 返回 `Put` → 重新入队"这几步。结果每组退化成"1 个 + (N-1) 个"交替，**一半的 fsync 浪费在只有队长自己的那一组上**。让出几次调度等价于"让已经就绪的写者先跑一步"：不睡眠、不等固定时长，代价几微秒，相对一次 2 ms 的 fsync 可以忽略。实测（32 写者 × 3000 次写）把合并率从 **15.87 提到 22.4～27.8**，即吞吐再涨四到七成。这是本项目里唯一一处"调度层面的主动让步"，它换来的是实打实的 2 倍吞吐。 |
| **组提交失败 = 整组失败，并且整个库停下来** | 一条批次是原子的，但一个**提交组**不是：组里靠前的批次可能已经进了 WAL，靠后的没有。这时候如果只把错误返回给失败的那几个写者、然后继续服务，就会出现"返回了错误但其实写成功了"—— 调用方据此重试会重复写、据此放弃会丢数据，而且**不报任何错**。所以规定：组内任何一步出错，整组一起失败，并记入 `bgErr` 让后续写入一律返回同一个错误。`Write` 的错误因此分成两类并有明确语义：`ErrClosed`/既有后台错误 → 一定没写进去；磁盘故障 → **可能写进去了，库已停止，重开即可**（WAL 始终是唯一的事实来源）。 |
| **整组在同一个 `db.mu` 临界区里应用，`lastSeq` 最后才推进** | 这是"批次原子性"在并发下的表现形式，也是它和组提交不冲突的原因：读者要么看到 `lastSeq` 停在组前（整组都不可见），要么看到组后（整组都可见），**不存在"半组可见"**。序列号则在 `beginWriteGroup` 里就先分配好了 —— 它就写在 WAL 记录头里，必须先定；但那时**不能**抬高 `db.lastSeq`，因为那同时也是读者的快照水位，提前抬高等于让读者看到一个尚未落地的"已提交点"。 |
| **快照与迭代器是两套机制，不要合并** | 快照只记序列号，靠**登记**抬高 `smallestSnapshot`，作用对象是**旧版本**；迭代器持有**版本引用**，靠引用计数让那批**输入文件**在读完前删不掉，作用对象是**文件**。迭代器读的永远是它打开那一刻的那批文件，而 Compaction 只会写新文件，所以迭代器**不需要**也**不应该**登记快照 —— 给它登记的话，一个长命迭代器会把丢弃上界永久钉死，白白拦住旧版本回收。（9.16 里原有一句"打开迭代器时登记存活快照"与代码不符，M4 核实时订正。） |
| **`Close` 与写入之间刻意不留中间态** | `drainWrites` 的次序决定了"已关库但仍在提交"这个状态不存在，所以写路径上两处 `db.closed` 检查是**防御性**的、正常路径不可达。这是刻意的取舍：宁可留两行走不到的守卫，也不要让"关库过程中还能写进去一条"这种窗口存在 —— 那种窗口在真实故障里最难复现也最难解释。 |
| **把"合并率"做成指标而不是只报墙钟** | 组提交的效果如果只用 ops/s 表达，会被机器、磁盘、缓存干扰得无法比较。`WriteBatches / WriteGroups`（合并率）与 `WriteBatches / WALFsyncs`（每次 fsync 摊销多少次写）是两个与机器无关的比值，它们直接说出"省了多少 fsync"；`WALFsyncs` 在关掉 `SyncWrites` 时恒为 0，天然给了自己一个对照组。`MaxWriteGroup` 则是"实际最多同时有几个写者在跑"的观测。 |
| **`LiveSnapshots` 作为快照遗忘的观测口** | 忘记 `Release` 不会读错数据，只是让旧版本一直留在磁盘上 —— 这是一种**没有症状的错误**。把存活快照数放进 `Stats`，它就成了唯一能看见这件事的地方；`TestSnapshotPinsOldVersionsAcrossCompaction` 同时断言取快照后为 1、`Release` 后为 0。 |

---

### 9.22 M5 交付物

| 文件 / 包 | 内容 |
|---|---|
| `internal/compress` | 压缩类型字节（none/Snappy/zlib）、`Compressor` 接口、按类型路由的实现表。Snappy 引入 `github.com/golang/snappy`（§7 允许的第三方库），zlib 用 stdlib flate（BestSpeed），读写缓冲均池化。 |
| `internal/rate` | 令牌桶式字节限流器：`Request(n)` 阻塞到配额可用，大请求拆成 ≤64KB 的小笔平滑摊到时间轴；nil 接收者是空操作（与 cache 的约定一致）；`Close` 放行所有等待者（限流是软约束，停库不为它多等）。 |
| `internal/logger` | 数据目录下的 LOG 文件：带时间戳与级别前缀的行格式，超过 `LogMaxSize` 轮转为 LOG.old；也可指向任意 io.Writer。轮转失败退化为"继续写当前文件"，不把引擎带崩。 |
| `internal/sst`（改造） | Writer 按块独立压缩（省不下 1/8 就原样存），块尾类型字节实装；Reader 按类型字节解压后入块缓存（缓存存**解压后**内容）；新增 `BlockStats`（原子计数，Writer/Reader 共享）与 `ErrUnsupportedCompression`。 |
| `internal/compact`（改造） | `Env` 增加 `Compression` 与 `RateLimiter`；输入侧用节流迭代器按 256KB 粒度计费、输出侧按写出字节计费；归并逻辑与 Result 不变。 |
| `internal/version`（改造） | 抽出 `SnapshotEdit()` 与 `WriteManifest(dir, num, edit)`：前者返回"完整重建当前版本"的编辑项，后者把它写到**另一个**目录 —— Checkpoint 的元数据来源。 |
| `options.go` | 新增 `Compression`（0=默认 Snappy，-1 显式关闭，1/2 为具体算法）、`CompactionRateLimit`、`LogMaxSize`、`Logger` 接口与 `NewFileLogger`。 |
| `db.go` / `db_log.go` | Open 接线事件日志 / 限流器 / 块统计；`Stats` 增加 `Compression`、`RateLimit` 两组指标；事件日志三档去向（用户 Logger > LOG 文件 > 丢弃）。 |
| `db_checkpoint.go` | `DB.Checkpoint(dir)`：一致性副本，详见 9.25。 |
| `cmd/kvdb-bench` | 新增 `-mode ycsb`（workload A~F、Zipfian 分布、per-op 延迟分位）、`-mode compress`（三种算法对照）、`-mode checkpoint`；`-compression` / `-rate-limit` / `-l0-trigger` / `-level-base-size` / `-report`（markdown 报告）。 |
| `docs/BENCH.md` | 基准报告：压缩对照、YCSB A~F、限流对照、zlib 对照、Checkpoint，含判读分析。 |

### 9.23 M5 API 一览

```go
// Options 新增字段
type Options struct {
    Compression         Compression // 0=默认(Snappy)，-1=关闭，1=Snappy，2=zlib
    CompactionRateLimit int         // 字节/秒；0 = 不限流
    LogMaxSize          int         // LOG 轮转阈值；0=默认 1MB，负数=不写文件日志
    Logger              Logger      // 事件日志；nil = 写数据目录下的 LOG
}

// 根包新增
type Compression int8          // CompressionNone / CompressionSnappy / CompressionZlib / CompressionDefault
type CompressionStats struct { /* BlocksWritten / CompressedBlocks / RawBytes / StoredBytes / Decompressions / CompressedBytesRead */ }
type RateLimitStats struct { /* Bytes / Waits / WaitNanos */ }
func NewFileLogger(dir string, maxSize int) (Logger, func() error, error)

// DB 新增方法
func (db *DB) Checkpoint(dir string) error // 一致性副本；目标必须为空且不同于源目录

// Stats 新增字段
Compression CompressionStats
RateLimit   RateLimitStats
```

### 9.24 M5 验收结果

全部数据见 `docs/BENCH.md`（2026-09-18，单次运行实测）。要点：

| 项目 | 结果 |
|---|---|
| 块压缩（200k×200B 文本负载） | Snappy：41.74MB → 6.10MB（6.87x），写耗时 668ms vs 基线 660ms；zlib：3.62MB（11.66x），读 p99 可见代价 |
| 压缩块占比 | snappy/zlib 组 10001/10003 块压缩，none 组 0/10003 —— "不划算就原样存"的策略按块生效 |
| YCSB workload A~F | 100k keys / 100k ops 全部跑通；纯读 ~59 万 ops/s，混合写 ~28 万 ops/s |
| Compaction 触发型负载 | 200k keys + 4MB MemTable + 16MB L1：7 轮 Compaction，读放大 0.84 probes/get |
| 限流 | 4MB/s 配额下 Compaction 从 7 轮退到 1 轮，前台吞吐 -13%；等待发生在 Compaction 线程内部 |
| Checkpoint | 10 万 key 副本生成 56ms（硬链接），副本独立打开且隔离性验证通过 |
| 测试 | `go test ./...` 全绿（新增 compress/rate/logger/Checkpoint/快照导出 用例）；`-race` 无告警 |

### 9.25 M5 期间新增 / 细化的决策

| 决策 | 结论与理由 |
|---|---|
| **压缩按块独立判断，"至少省 1/8"才压** | `worthCompressing(raw, out) = out <= raw - raw/8`。一个 SST 里同时存在数据块（可压）、过滤器位图与索引块（几乎不可压），对整文件套一个算法是错误的粒度；块级判断让"压了反而更大"的块自动落到不压缩的一侧，读侧也因此少付一次解压。1/8 的门槛来自"块尾类型字节 + CRC 共 5 字节 + 解压 CPU"的最低回报线。 |
| **块缓存存解压后的内容** | 解压一次、缓存 forever，缓存命中率直接决定解压开销；`Decompressions` 与缓存命中率构成一对互相印证的指标。代价是缓存条目比落盘数据大（压缩率 6x 时缓存同样 6x），是空间换 CPU 的显式交换。 |
| **限流的计费粒度是 256KB，输入与输出都计费** | Compaction 的磁盘成本一半在读一半在写，只限写挡不住读放大。按 256KB 计费（而非每条记录）把限流器的锁竞争压到可忽略；`throttledIterator` 在归并迭代器外面包一层，归并逻辑对限流毫无感知。 |
| **限流是软约束：`Close` 放行等待者而不报错** | 关库时为一个"礼貌性"的等待多停一秒毫无意义。放行而不报错与"失败语义只有停库"的整体设计一致。 |
| **过期日志删除失败不再停库（M5 唯一的行为放宽）** | 实测（Windows + 杀毒软件）CheckPoint 读过的 WAL 在随后被 flushLoop 删除时稳定撞上"文件被另一进程占用"的瞬时失败，而旧代码把删除失败当致命错误直接 `bgErr` 停库。删除判据是单调的（编号只增），这次删不掉下次 Flush 会重试，多留一会儿的唯一代价是磁盘占用 —— LevelDB/RocksDB 的过期文件删除同样是尽力而为。判据本身不变，只是把"删不掉"从故障降级为警告。 |
| **Checkpoint 的一致性来自"一个临界区里捕获全部状态"** | 版本、WAL 尾巴下界、快照编辑项必须在**同一个** `db.mu.RLock` 里捕获：`SnapshotEdit` 放出去之后再取，可能拿到并发 Flush/Compaction 提交之后的新版本，副本的 Manifest 就会引用根本没复制过去的文件。锁序延续 `db.mu → VersionSet.mu`，无新增风险。 |
| **复制 WAL 尾巴前先 Flush 缓冲** | `SyncWrites=false` 时记录还在 WAL 的 bufio 里，文件上看不到 —— 最初实现漏了这一步，副本丢掉全部未落盘写入，被 `TestCheckpointCatchesUnflushedTail` 抓住。Flush 只推缓冲不做 fsync：副本只要"读得到"，持久性仍由源库的写路径负责。 |
| **副本 Manifest 编号 = 源 nextFileNum，edit.NextFileNum 再 +1** | 编号空间在 SST / WAL / Manifest 之间共享，副本恢复时会从 edit.NextFileNum 起继续分配编号；不自增的话，副本第一次 NewManifest 就会分配出与自己的 MANIFEST 相同的编号。虽然后缀不同不会直接撞文件名，但"编号大 = 更新"这条不变式不该有例外。 |
| **SST 优先硬链接、失败退回复制** | 同一文件系统上硬链接是 O(文件数) 的零拷贝；Compaction 之后源目录会删掉旧文件，但副本的版本握着引用，inode 不会消失。跨卷（硬链接失败）自动退化为整文件复制 + fsync。 |
| **正在写入的 WAL 尾巴可以"多读不要紧、读坏也安全"** | Checkpoint 之后源库继续提交，复制出去的当前日志可能比捕获时刻多几条已提交记录 —— 多出来的也是真实数据，无害；写入中途的撕裂尾部由副本恢复时既有的"截断损坏尾部"路径处理。Checkpoint 因此不需要停止世界。 |
| **Options.Compression 的零值语义与数值字段不同** | 数值字段约定"0 = 沿用默认"；Compression 的 0 同样是"默认"（= Snappy）而**不是**"关闭"，显式关闭必须写 `CompressionNone`（负数）。`DefaultOptions` 直接给 `CompressionSnappy`，保证它不经 ensureDefaults 也能过 Validate。 |
| **事件日志三档去向，日志失败永不停库** | 用户 `Options.Logger` > 目录下 LOG 文件 > 丢弃。日志是运维辅助，不是控制流的一部分：创建失败退回丢弃实现并继续打开，所有 `logInfof/logWarnf/logErrorf` 只描述"发生了什么"，不参与任何错误处理。 |

---

## 附录 A：关键数据结构

### internal key 编码

```
┌──────────────┬──────────────────────────────┐
│  user_key    │  (seq << 8) | type           │
│  变长        │  定长 8 字节，大端序           │
└──────────────┴──────────────────────────────┘

type:  0 = Deletion（墓碑）    1 = Value
```

**排序规则（InternalKeyComparator）：**

1. 先按 `user_key` 升序
2. `user_key` 相同时，按 `(seq, type)` **降序** —— 保证新版本排在前面

第 2 条是整个 MVCC 的基础：查找时从头扫，遇到的第一个满足 `seq <= 快照序列号` 的记录就是可见版本；如果它是墓碑，说明 key 已被删除。

### Options

已在 `options.go` 落地，字段与默认值如下。数值字段统一遵循"**0 = 沿用默认值**"，
`BlockCacheSize` / `BloomBitsPerKey` 另用**负数表示显式关闭**。

```go
type Options struct {
    Dir                 string   // 数据目录；同一目录同时只允许一个进程打开
    Comparer            Comparer // 默认 BytewiseComparer，nil 时自动补齐
    MemTableSize        int      // 默认 64MB，写满即冻结
    BlockSize           int      // 默认 4KB
    BlockCacheSize      int      // 默认 8MB
    BloomBitsPerKey     int      // 默认 10
    L0CompactionTrigger int      // 默认 4，L0 文件数达到即触发
    LevelBaseSize       int      // 默认 256MB，L1 的容量上限
    LevelSizeMultiplier int      // 默认 10，相邻两层容量倍数
    MaxLevels           int      // 默认 7
    SyncWrites          bool     // 每条写是否 fsync；默认 true
}
```

`Comparer` 是接口而非函数字段，因为需要一个稳定标识 `Name()` 写进 Manifest：

```go
type Comparer interface {
    Compare(a, b []byte) int // -1 / 0 / +1
    Name() string
}
```

配套：`DefaultOptions(dir)` 返回填好默认值的配置，`Options.Validate()` 只做校验，
内部 `prepare()` = 补齐默认值 + 校验，是打开数据库前的标准化入口。

---

## 附录 B：核心接口

```go
type DB interface {
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Write(batch *WriteBatch) error
    NewIterator(opt *IteratorOptions) Iterator
    GetSnapshot() *Snapshot
    Close() error
}
```

`Get` / `Put` / `Delete` 都是 `WriteBatch` 的语法糖。真正的写入口只有 `Write`，这样原子性、Group Commit、WAL 追加都只需要在一个地方实现。

截至 M3，这个接口已经全部落地（`NewIterator` / `GetSnapshot` 在 `db_iter.go`），与 9.7、9.11 的清单一致。
M3 为它补上了两个诊断入口：`Stats()`（分层布局 + 读写放大）与 `RecoveryReport()`（恢复时丢弃了什么），
两者都不改变上面的读写语义。

**截至 M4，`Write` 的并发与失败语义被正式定义**（见 9.19），其余签名一个都没变：

- 并发写者由写队列合并进组提交，**队首的写者替全组做一次 fsync**；
- `Write` 返回 nil ⇒ 这一批已落盘（`SyncWrites` 为真时）且对后续读可见；
- 返回 `ErrClosed` 或之前已存在的后台错误 ⇒ 这一批**一定没有**写进去；
- 返回磁盘故障 ⇒ 这一批**可能**已经写进去了，同时整个库会停下来（后续写入一律返回
  同一个错误），重开数据库即可正确恢复。

`Stats()` 在 M4 里增加了 `WriteGroups / WriteBatches / MaxWriteGroup / WALFsyncs /
LiveSnapshots` 五个观测口 —— 组提交的效果如果只用 ops/s 表达会被机器差异淹没，
而"合并率 = `WriteBatches / WriteGroups`"是一个与机器无关、直接说明省了多少 fsync 的比值。

---

## 附录 C：包结构

方括号标注的是该项的**目标里程碑与当前状态**，便于对照 9.1 的进度表。

```
kvdb/
├── go.mod                  # [M0 已完成] module kvdb
├── doc.go                  # [M0 已完成] 包级文档
├── options.go              # [M0 已完成] Options / Comparer
├── db.go                   # [M1 已完成 / M2 扩充 / M4 接组提交] DB 对外接口、读路径、冻结 Immutable、块缓存、写队列状态
├── db_write.go             # [M4 已完成] 组提交：写队列、队长/跟随者、一次 fsync 服务整组、关库前排空队列
├── db_flush.go             # [M1 已完成 / M2 扩充 / M3 扩充 / M5 接压缩] 恢复（目录扫描 + WAL 重放）、后台 Flush、五段式落盘
├── db_compact.go           # [M3 已完成 / M5 接限流与压缩] 后台 Compaction 循环、快照登记、停库入口
├── db_checkpoint.go        # [M5 已完成] DB.Checkpoint：一致性副本（硬链接 / 复制 + WAL 尾巴 + 快照 Manifest）
├── db_log.go               # [M5 已完成] 事件日志接线（用户 Logger / LOG 文件 / 丢弃三档）
├── db_iter.go              # [M2 已完成] NewIterator / GetSnapshot / Snapshot / 导出迭代器接口
├── batch.go                # [M1 已完成] WriteBatch 与它的二进制编解码（M4 的原子性载体，无需改动）
├── db_concurrent_test.go   # [M4 已完成] 并发一致性：组提交合并率、批次原子可见、快照隔离、停库语义、关库竞态
├── file_lock_windows.go    # [M1 已完成] 目录锁（LockFileEx，随进程消亡释放）
├── file_lock_unix.go       # [M1 已完成] 目录锁（flock）
├── docs/
│   └── DESIGN.md
├── scripts/
│   ├── gotest.ps1          # [M1 已完成] 本机 go test 包装脚本（pwsh 7，输出写 _t.log）
│   └── gotest-race.sh      # [M1 已完成] 走 Bash 通道的 -race 脚本（自带 PATH/CGO 配置）
├── internal/
│   ├── key/                # [M0 已完成] internal key 编码、varint；[M1] Comparer / InternalComparer
│   ├── crc/                # [M2 已完成] CRC32C + 掩码，WAL 与 SST 共用
│   ├── memdb/              # [M1 已完成 / M2 扩充] 跳表 + MemTable；迭代器补 Error()
│   ├── wal/                # [M1 已完成 / M2 收敛] 预写日志；CRC 实现改为复用 internal/crc
│   ├── sst/                # [M2 已完成] 五段式读写：block / format / writer / reader / iterator
│   ├── filter/             # [M2 已完成] Bloom Filter 与 Filter Block
│   ├── cache/              # [M2 已完成] 16 分片 LRU 块缓存
│   ├── iterator/           # [M2 已完成] Iterator 接口 + MergingIterator + DBIter
│   ├── version/            # [M3 已完成 / M5 加快照导出] Manifest 追加日志、CURRENT、VersionSet、版本引用计数、VersionEdit、SnapshotEdit/WriteManifest
│   ├── compact/            # [M3 已完成 / M5 接限流与压缩] Compaction Picker（L0 按文件数 / L1+ 按容量）与多路归并执行
│   ├── compress/           # [M5 已完成] 块压缩：类型字节、Compressor 接口、Snappy（golang/snappy）与 flate 实现
│   ├── rate/               # [M5 已完成] 令牌桶限流器（nil 接收者是空操作，Close 放行等待者）
│   └── logger/             # [M5 已完成] LOG 文件与按大小轮转（LOG → LOG.old）
└── cmd/
    └── kvdb-bench/         # [M1 雏形 / M2 扩充读路径 / M3 补放大统计 / M4 补组提交统计 / M5 完整]
                            # write / point / scan / sweep / group / ycsb / compress / checkpoint，-report 输出 markdown
```

---

## 附录 D：关键设计决策

| 决策 | 理由 |
|---|---|
| WAL 与 MemTable 并行写，而非串行 | 两者职责不同：WAL 管持久化，MemTable 管可读性。并行才能让 fsync 成为唯一瓶颈 |
| 引入 Immutable MemTable | 否则 Flush 期间写入被全程阻塞 |
| L0 不做二分，L1 以下才做 | L0 文件由 MemTable 直接落盘，区间必然重叠 |
| internal key 必须带 seq | MVCC 与快照读的基础，事后补代价极大 |
| Delete 写墓碑而非真删 | LSM 文件不可变，物理删除只能在 Compaction 时做 |
| Manifest 独立于 SST 记录版本变更 | 崩溃恢复时靠它重建"当前有哪些文件"，原子切换版本 |
| 写入口统一收敛到 `Write` | 原子性、Group Commit、WAL 追加只需实现一次 |
| 同一 user key 的所有版本必须在同一个 Data Block 里 | 块内 seek 到一条就能确定可见版本的**前提**；跨块会让点查只读一块时漏掉可见版本 |
| Bloom Filter 建在 user key 上，不建在 internal key 上 | internal key 的 `(seq, type)` 后缀让每个版本都成为不同 key，位图会随更新次数膨胀且语义错位 |
| 块缓存按 `(fileNum, offset)` 索引，且容忍 nil 接收者 | 文件编号会因删除而复用，只用 offset 会串味；nil 缓存让"关缓存"不必在热路径上写分支 |
| 主体自己写，不引第三方存储引擎 | 否则失去项目意义；工具层依赖可以放开 |
| Compaction 每次只往下走一层，不做跨层归并 | 一次跨多层的归并会把写入量放大到不可控，而且中间层的旧版本会被同时清掉，出错时无法定位是哪一层的问题 |
| 输出文件只在 user key 变化处切分 | 「同一 user key 的所有版本必须同文件」这条不变式的写侧等价物；在 internal key 中间切会把一个 key 的版本劈到两个文件里，点查只读一个文件时就会漏版本 |
| 读到比 `smallestSnapshot` 更旧的版本即可丢 | 丢掉任何存活快照还要读的版本都是静默的数据损坏（读到新值，或本该存在的值变成不存在）且不报错；取所有存活快照的最小值是这个上界的唯一安全选择 |
| 写队列独立于 `db.mu`，fsync 在锁外做 | 写者为了排队去拿全局写锁，就等于让每次 fsync 都堵住所有读者；分开之后"多少个写者合并进一次 fsync"完全由并发度决定，不需要启发式 |
| 队长排空队列才让位 | "提交一组就让位"会在让位的那一瞬间把刚排进来的写者变成没人负责的孤儿 —— 它们永远等不到通知（实测挂死） |
| 组提交失败 = 整组失败 + 整库停写 | 一条批次是原子的，一个提交组不是；"返回了错误但其实写成功了"会让调用方的重试变成重复写、放弃变成丢数据，且不报错 |
| 整组在同一个临界区里落库，序列号水位最后推进 | 批次原子性在并发下的表现形式：读者要么整组可见、要么整组不可见，不存在"半组可见"；序列号先分配是因为它写在 WAL 记录头里，而水位后推进是因为它同时是读者的快照水位 |
| 快照靠**登记**、迭代器靠**版本引用** | 两者作用在不同对象上（旧版本 vs 输入文件），各自解决一半问题；给迭代器也登记快照反而会把丢弃上界永久钉死，白拦旧版本回收 |

（M2 期间更细的决策 —— 过滤器损坏时的保守策略、索引记 max key、`ErrLegacyFormat` 不可容忍等 —— 见 9.13。
M3 期间更细的决策 —— 孤儿文件的两类区分、`unrefLocked` 规避自死锁、按编号删旧 Manifest、关库兜底 GC 等 —— 见 9.17。
M4 期间更细的决策 —— 丢失唤醒的修法、`drainWrites` 的次序、`collectYields`、停库语义、快照与迭代器的分工等 —— 见 9.21。）
