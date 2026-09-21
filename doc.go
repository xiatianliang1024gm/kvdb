// Package kvdb 是一个用 Go 实现的嵌入式 KV 存储引擎，架构参考 RocksDB / LevelDB 的 LSM-Tree。
//
// 设计文档见 docs/DESIGN.md。当前进度：M0（骨架）、M1（最小可用）、M2（读优化）、
// M3（写优化）、M4（一致性）、M5（生产化）均已完成。
//
// # 基本用法
//
//	opts := kvdb.DefaultOptions("data")
//	db, err := kvdb.Open(opts)
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//
//	if err := db.Put([]byte("k"), []byte("v")); err != nil {
//		return err
//	}
//	v, err := db.Get([]byte("k")) // key 不存在或已被删除时返回 ErrNotFound
//
// 所有导出方法都可以并发调用。
//
// # 写入语义
//
// 所有写入都收敛到 DB.Write：Put / Delete 只是它的语法糖。一次 Write 里的
// 所有记录编码成一条 WAL 记录，因此要么整批可见、要么整批不可见。
// SyncWrites 为真时，Write 返回即代表数据已经 fsync 落盘。
//
// 并发写者会先排进一条写队列，队首的写者（队长）把当前队列里的所有批次合并成
// **一次 WAL 追加与一次 fsync**，然后整组一起落进 MemTable。写性能的天花板由
// fsync 次数决定，所以这个合并率就是并发写吞吐的倍数。最贵的 fsync 在 db.mu
// 之外完成，因此写者的磁盘等待不会堵住读者。
//
// 失败的语义分两类，调用方需要区别对待：
//
//   - ErrClosed，或者在此之前已经存在的后台错误：这一批一定没有写进去；
//   - 磁盘故障：这一批**可能**已经写进去了，同时整个库会停下来，后续写入一律
//     返回同一个错误。此时重新打开数据库即可 —— WAL 始终是唯一的事实来源。
//
// # 读语义
//
// DB.Get 读到的是"当前已提交的最后一个序列号"下的可见版本。需要固定视图时用
// DB.GetSnapshot：它只记下一个序列号，不复制数据、不阻塞写入，但在 Release 之前
// 会拦住 Compaction 丢弃它还需要读的旧版本。
//
// DB.NewIterator 返回一个归并了 MemTable、Immutable MemTable 与当前版本里所有 SST
// 的有序迭代器。它持有构造那一刻的版本引用，因此读的那批文件在关闭之前不会被
// Compaction 删掉 —— 这也是 Close 必须调用的原因。
//
// # 对外契约
//
// api.go 把 DB 的方法收成三个接口，想知道"这个库能干什么"看那一个文件就够了：
//
//   - Reader：Get / NewIterator。*DB（最新视图）与 Snapshot（固定视图）都实现它
//   - Writer：Put / Delete / Write。Write 是唯一写入口，另两个是它的语法糖
//   - Store：Reader + Writer + Close
//
// 剩下的 Stats / RecoveryReport / Checkpoint / GetSnapshot 属于运维与生命周期，
// 没有收进接口：它们绑死在具体实例上，抽象出来没有调用方受益。
//
// Open 返回具体类型 *DB 而不是接口，这是有意的 —— Go 的惯例是"接受接口、
// 返回结构体"，这样以后给 DB 加方法不会打破任何调用方。
//
// # 已经具备的能力
//
//   - WAL：分块顺序追加、每条 CRC32C 校验、崩溃后重放、尾部损坏可容忍
//   - MemTable：跳表实现，读无锁、写串行；写满冻结为 Immutable MemTable
//   - 后台 Flush：Immutable 落成 SST 后即可删除对应的日志
//   - SSTable：Data / Filter / Index / MetaIndex / Footer 五段式；点查走
//     索引二分 + Bloom 判定 + 块缓存，不触发全文件扫描
//   - 分层与合并：Manifest + CURRENT + 版本引用计数；L0 按文件数触发、L1 以下
//     按容量触发，同层区间互不重叠
//   - 崩溃恢复：Manifest 重建版本 + WAL 重放，被强杀的进程不会让数据目录打不开
//   - 一致性：快照读、原子批量写、组提交
//   - 目录锁：同一目录同时只允许一个实例打开，锁由内核持有、进程消亡即释放
//   - 块压缩：SSTable 数据块按块独立压缩（Snappy / zlib），压不划算的块原样存储；
//     块缓存存解压后内容，解压开销被命中率摊薄
//   - 限流：后台 Compaction 的读写带宽受令牌桶约束，等待只发生在 Compaction
//     线程内部，前台读写不受阻塞
//   - 事件日志：打开/Flush/Compaction/故障写进数据目录下的 LOG（按大小轮转），
//     或通过 Options.Logger 接入调用方自己的日志体系
//   - Checkpoint：DB.Checkpoint 在另一个目录生成可独立打开的一致性副本，
//     源库无需停止世界，之后可以照常读写
//
// # 尚未具备的能力（见设计文档 §8）
//
// 分布式、事务隔离级别、二级索引、加密、Column Family。
package kvdb
