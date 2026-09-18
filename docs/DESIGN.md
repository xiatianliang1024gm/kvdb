# kvdb 设计文档

> 一个用 Go 实现的高性能嵌入式 KV 存储引擎，架构参考 RocksDB / LevelDB 的 LSM-Tree。
>
> 状态：M0（骨架）已完成，M1（最小可用）已完成 ｜ 最后更新：2026-09-18

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

**读路径的代价在层数**：最坏情况下要穿透 MemTable、Immutable 和每一层。所以必须靠两个组件把无效查找挡在外面：

- **Bloom Filter**：每个 SST 一个位图，能在打开文件之前就判定"key 一定不在这个文件里"，挡掉绝大部分无效磁盘 IO。
- **Block Cache**：分片 LRU，缓存解压后的数据块，让热点读不落盘。

> **读到 M2 为止的实际形态**：上面两个组件都已经接上（`internal/filter` + `internal/cache`），
> 每层 SST 内部是"Index 二分 → Bloom 判定 → 读块（先查缓存）→ 块内 seek"。
> 但**分层本身还没有** —— `L0 → L1 … Ln` 里目前只有 L0，且所有 L0 文件的 key range 互相重叠、
> 没有 Compaction 去合并，所以"M 个文件就要探 M 次元数据"这件事在 M3 之前会一直存在。
> 具体格式见 4.1，实测数据见 9.12。

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
- **块尾（trailer，5 字节）**：`compression_type(1B) + crc32c(4B，掩码后)`。M2 的压缩类型恒为 0（不压缩），
  但长度先按最终形态留出，M5 加压缩不用改格式。
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
| **Group Commit** | 建议 | 并发写合并成一次 fsync，高并发吞吐的关键 |
| **块压缩** | 建议 | Snappy / LZ4 / ZSTD，块级压缩 |
| **Metrics / LOG** | 建议 | 命中率、各层文件数、读写放大倍数 |
| **Rate Limiter** | 可选 | 限制 compaction 带宽，避免挤压前台请求 |
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
| **M3 写优化** | Flush + Leveled Compaction + Manifest/Version | 写入 1GB 数据后读延迟不塌方 | 未开始 |
| **M4 一致性** | Snapshot + WriteBatch + Group Commit | 并发压测下 race detector 无告警 | 未开始 |
| **M5 生产化** | 块压缩 + 限流 + Metrics + Checkpoint | 跑通 YCSB 并输出基准报告 | 未开始 |

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
| M3 写优化 | 未开始 | — | 待办清单见 9.14 |
| M4 一致性 | 未开始 | — | — |
| M5 生产化 | 未开始 | — | — |

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

### 9.14 M3 待办

M2 之后，读路径的常数项已经压住，但**写路径的账还没算**：L0 文件数只会增加、不会合并，
所以上面那个 0.126 µs/文件 的线性项会一直累积；同时"读一个 key 要翻 n 个文件"也让读放大随写入量上升。

1. `internal/version`：Manifest（`VersionEdit` 的追加日志）+ `CURRENT` 指针 + `VersionSet` 与引用计数。
2. `internal/compact`：Compaction Picker（L0 按文件数触发、L1 以下按容量触发）+ 多路归并执行 + 原子替换 Version。
3. `db.go`：把 `files []*fileMeta` 换成 `Version`；`NewIterator` / `Snapshot` 改为持有版本引用，`Close` 才真正有意义。
4. `options.go`：`L0CompactionTrigger` / `LevelBaseSize` / `LevelSizeMultiplier` 三个已存在但尚未被读的字段接入。
5. 验收标准：L0 文件数稳定在 `L0CompactionTrigger` 附近不再增长；写入 10 倍数据后读延迟不显著退化；
   `kvdb-bench` 增加"写放大 / 读放大"统计。

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

截至 M2，这个接口已经全部落地（`NewIterator` / `GetSnapshot` 在 `db_iter.go`），与 9.7、9.11 的清单一致。

---

## 附录 C：包结构

方括号标注的是该项的**目标里程碑与当前状态**，便于对照 9.1 的进度表。

```
kvdb/
├── go.mod                  # [M0 已完成] module kvdb
├── doc.go                  # [M0 已完成] 包级文档
├── options.go              # [M0 已完成] Options / Comparer
├── db.go                   # [M1 已完成 / M2 扩充] DB 对外接口、读写路径、冻结 Immutable、块缓存
├── db_flush.go             # [M1 已完成 / M2 扩充] 恢复（目录扫描 + WAL 重放）、后台 Flush、五段式落盘
├── db_iter.go              # [M2 已完成] NewIterator / GetSnapshot / Snapshot / 导出迭代器接口
├── batch.go                # [M1 已完成] WriteBatch 与它的二进制编解码
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
│   ├── version/            # [M3] Manifest、VersionSet、VersionEdit
│   └── compact/            # [M3] Compaction Picker 与执行
└── cmd/
    └── kvdb-bench/         # [M1 雏形 / M2 扩充 / M5 完整] 压测工具：write / point / scan / sweep
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

（M2 期间更细的决策 —— 过滤器损坏时的保守策略、索引记 max key、`ErrLegacyFormat` 不可容忍等 —— 见 9.13。）
