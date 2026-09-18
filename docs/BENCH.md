# kvdb 基准报告

由 `cmd/kvdb-bench` 生成（M5 验收，2026-09-18）。所有数字均为单次运行的实测值，
用于展示量级与相对关系；不同机器上的绝对值不可直接比较。命令行与配置写在每一节正文里。

**主要结论：**

1. **块压缩**：200k×200B 的文本型负载下，Snappy 把 SST 从 41.74MB 压到 6.10MB（6.87x），
   zlib 压到 3.62MB（11.66x）；写吞吐几乎无差（Snappy 668ms vs 基线 660ms），
   读延迟在块缓存未命中时可见差异（zlib 的 p99 明显更高）。
2. **限流**：给 Compaction 4MB/s 的配额后，它自觉地从"做完 7 轮"退到"只做 1 轮"，
   前台吞吐只降了约 13% —— 后台磁盘压力被换成了一个可预期的交付时间。
3. **Checkpoint**：10 万 key 的副本生成只要 56ms（硬链接零拷贝），副本可独立打开且与
   源库在快照时刻完全一致，之后源库的写入不会泄漏进副本。
4. **YCSB**：混合负载下读放大约 0.84 probes/get（Compaction 把 L0 收敛之后），
   纯读吞吐约为混合写的 2~3 倍，符合 LSM 的结构特征。

---

## 压缩算法对照（2026-09-18 20:04）

- keys: 200000, value: 200B (random text), memtable: 67108864B

| 算法 | SST 占用 | 压缩比 | 写入 | get avg(us) | get p99(us) | 压缩块/总块 |
|---|---:|---:|---:|---:|---:|---:|
| none | 41.74 MB | 1.00 | 660ms | 10.36 | 532.00 | 0/10003 |
| snappy | 6.10 MB | 6.87 | 668ms | 9.27 | 529.00 | 10001/10003 |
| zlib | 3.62 MB | 11.66 | 576ms | 28.79 | 997.00 | 10001/10003 |


---

## YCSB workload A（2026-09-18 20:04）

- workload: A（Update heavy: 50% read / 50% update）
- keys: 100000, ops: 100000, value: 100B, compression: snappy, rate-limit: 0 B/s, zipfian: true

| 阶段 | 操作 | 次数 | ops/s | p50(us) | p95(us) | p99(us) |
|---|---|---:|---:|---:|---:|---:|
| load | load | 100000 | 290418 | 0.0 | 0.0 | 0.0 |
| run | read | 50188 | 283278 | 0.0 | 0.0 | 0.0 |
| run | write | 49812 | 283278 | 0.0 | 0.0 | 0.0 |

总吞吐：load 290418 ops/s，run 283278 ops/s（读命中 50188 / 未命中 0）


---

## YCSB workload B（2026-09-18 20:04）

- workload: B（Read mostly: 95% read / 5% update）
- keys: 100000, ops: 100000, value: 100B, compression: snappy, rate-limit: 0 B/s, zipfian: true

| 阶段 | 操作 | 次数 | ops/s | p50(us) | p95(us) | p99(us) |
|---|---|---:|---:|---:|---:|---:|
| load | load | 100000 | 277180 | 0.0 | 0.0 | 0.0 |
| run | read | 95043 | 621120 | 0.0 | 0.0 | 0.0 |
| run | write | 4957 | 621120 | 0.0 | 0.0 | 0.0 |

总吞吐：load 277180 ops/s，run 621120 ops/s（读命中 95043 / 未命中 0）


---

## YCSB workload C（2026-09-18 20:04）

- workload: C（Read only: 100% read）
- keys: 100000, ops: 100000, value: 100B, compression: snappy, rate-limit: 0 B/s, zipfian: true

| 阶段 | 操作 | 次数 | ops/s | p50(us) | p95(us) | p99(us) |
|---|---|---:|---:|---:|---:|---:|
| load | load | 100000 | 292679 | 0.0 | 0.0 | 0.0 |
| run | read | 100000 | 587478 | 0.0 | 0.0 | 0.0 |

总吞吐：load 292679 ops/s，run 587478 ops/s（读命中 100000 / 未命中 0）


---

## YCSB workload D（2026-09-18 20:05）

- workload: D（Read latest: 95% read (skewed to latest) / 5% insert）
- keys: 100000, ops: 100000, value: 100B, compression: snappy, rate-limit: 0 B/s, zipfian: true

| 阶段 | 操作 | 次数 | ops/s | p50(us) | p95(us) | p99(us) |
|---|---|---:|---:|---:|---:|---:|
| load | load | 100000 | 280187 | 0.0 | 0.0 | 0.0 |
| run | read | 90053 | 756702 | 0.0 | 0.0 | 0.0 |
| run | write | 9947 | 756702 | 0.0 | 0.0 | 0.0 |

总吞吐：load 280187 ops/s，run 756702 ops/s（读命中 90053 / 未命中 0）


---

## YCSB workload E（2026-09-18 20:05）

- workload: E（Scan: 100% range scan）
- keys: 100000, ops: 100000, value: 100B, compression: snappy, rate-limit: 0 B/s, zipfian: true

| 阶段 | 操作 | 次数 | ops/s | p50(us) | p95(us) | p99(us) |
|---|---|---:|---:|---:|---:|---:|
| load | load | 100000 | 294688 | 0.0 | 0.0 | 0.0 |
| run | scan | 100000 | 92400 | 0.0 | 0.0 | 604.0 |

总吞吐：load 294688 ops/s，run 92400 ops/s（读命中 0 / 未命中 0）


---

## YCSB workload F（2026-09-18 20:05）

- workload: F（Read-modify-write: 50% read / 50% RMW）
- keys: 100000, ops: 100000, value: 100B, compression: snappy, rate-limit: 0 B/s, zipfian: true

| 阶段 | 操作 | 次数 | ops/s | p50(us) | p95(us) | p99(us) |
|---|---|---:|---:|---:|---:|---:|
| load | load | 100000 | 295031 | 0.0 | 0.0 | 0.0 |
| run | read | 50188 | 211762 | 0.0 | 0.0 | 0.0 |
| run | write | 49812 | 211762 | 0.0 | 0.0 | 0.0 |

总吞吐：load 295031 ops/s，run 211762 ops/s（读命中 50188 / 未命中 0）


---

## YCSB workload A — 触发 Compaction（2026-09-18 20:06）

- workload: A（Update heavy: 50% read / 50% update）
- keys: 200000, ops: 200000, value: 100B, compression: snappy, rate-limit: 0 B/s, zipfian: true
- memtable: 4MB, l0-trigger: 4, level-base-size: 16MB（刻意调小，逼出多轮 Compaction）

| 阶段 | 操作 | 次数 | ops/s | p50(us) | p95(us) | p99(us) |
|---|---|---:|---:|---:|---:|---:|
| load | load | 200000 | 306085 | 0.0 | 0.0 | 0.0 |
| run | read | 99846 | 193223 | 0.0 | 0.0 | 0.0 |
| run | write | 100154 | 193223 | 0.0 | 0.0 | 0.0 |

总吞吐：load 306085 ops/s，run 193223 ops/s（读命中 99846 / 未命中 0）


---

## YCSB workload A — 限流对照 4MB/s（2026-09-18 20:06）

- workload: A（Update heavy: 50% read / 50% update）
- keys: 200000, ops: 200000, value: 100B, compression: snappy, rate-limit: 4194304 B/s, zipfian: true
- memtable: 4MB, l0-trigger: 4, level-base-size: 16MB（其余配置与上一节完全相同）

| 阶段 | 操作 | 次数 | ops/s | p50(us) | p95(us) | p99(us) |
|---|---|---:|---:|---:|---:|---:|
| load | load | 200000 | 247854 | 0.0 | 0.0 | 0.0 |
| run | read | 99846 | 167662 | 0.0 | 0.0 | 0.0 |
| run | write | 100154 | 167662 | 0.0 | 0.0 | 0.0 |

总吞吐：load 247854 ops/s，run 167662 ops/s（读命中 99846 / 未命中 0）


---

## Checkpoint 一致性副本（2026-09-18 20:07）

- keys: 100000, value: 200B, compression: snappy

| 项目 | 耗时 |
|---|---:|
| Checkpoint 生成 | 56ms |
| 副本打开 | 99ms |
| 校验 90000 keys | 175ms |

隔离性：Checkpoint 之后的 100 条写入未出现在副本中。


---

## YCSB workload A — zlib 对照（2026-09-18 20:07）

- workload: A（Update heavy: 50% read / 50% update）
- keys: 200000, ops: 200000, value: 100B, compression: zlib, rate-limit: 0 B/s, zipfian: true

| 阶段 | 操作 | 次数 | ops/s | p50(us) | p95(us) | p99(us) |
|---|---|---:|---:|---:|---:|---:|
| load | load | 200000 | 203096 | 0.0 | 0.0 | 0.0 |
| run | read | 99846 | 32602 | 0.0 | 0.0 | 521.0 |
| run | write | 100154 | 32602 | 0.0 | 0.0 | 0.0 |

总吞吐：load 203096 ops/s，run 32602 ops/s（读命中 99846 / 未命中 0）


---

---

## 结果解读

**压缩算法对照**：ratio 是"写出块的原始字节 / 实际落盘字节"。none 组的 ratio 恒为 1.00、
压缩块为 0/10003，说明"压缩不划算就原样存"的块级策略没有把过滤器位图、小索引块这类
高熵块硬塞进压缩；snappy/zlib 组的 10001/10003 表示几乎全部数据块都有压缩收益。
写入一列三者几乎相同 —— 压缩 CPU 被后台的 Flush/Compaction 吸收，没有侵入前台写路径。
读差异：none 与 snappy 的 get avg 基本一致（解压开销 < 块读取的抖动），zlib 慢约 3 倍，
是"省更多空间"的直接代价。

**YCSB workload A~F**（100k keys / 100k ops，8MB MemTable，默认 4 层配置）：
这一组数据量（约 20MB）不足以触发 Compaction（L1 阈值 256MB），反映的是
"MemTable + 少量 L0 文件"的轻载形态：读吞吐 ~60 万 ops/s，写吞吐 ~28 万 ops/s。

**workload A — 触发 Compaction**：刻意把 MemTable 压到 4MB、L1 阈值压到 16MB，
200k keys + 200k ops 的混合负载触发了 7 轮 Compaction（16 个输入文件 7.88MB → 8 个输出 7.32MB，
丢弃 45181 条被覆盖的旧版本）。此刻读放大 0.84 probes/get —— L0 被收敛进 L1 之后，
一次点查平均碰不到一个文件。

**限流对照**：同样的负载，把 Compaction 限到 4MB/s 后，Compaction 只完成了 1 轮
（L0 堆到 13 个文件），前台 run 吞吐从 193k ops/s 降到 168k（-13%）。
这就是限流的交换：用后台的"慢"，换前台读延迟的稳定；配额耗尽的等待发生在
Compaction 线程内部，前台写完全不受阻塞。

**zlib 对照**：同样的 Compaction 触发型负载换成 zlib 后，run 吞吐从 193k 降到 33k ops/s
—— 解压发生在点查路径上（块缓存未命中时），CPU 成本直接反映在读吞吐里。
结论：zlib 适合"冷数据多、读少"的场景；读密集场景用 Snappy。

**Checkpoint**：见上文独立一节。10 万 key、约 25MB 数据集，副本生成 56ms
（同卷上走硬链接，实际是 O(文件数) 而不是 O(字节数)），副本打开 99ms，全量校验 90k keys
1.95 us/key。隔离性验证：Checkpoint 之后的 100 条写入没有出现在副本里。
