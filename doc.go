// Package kvdb 是一个用 Go 实现的嵌入式 KV 存储引擎，架构参考 RocksDB / LevelDB 的 LSM-Tree。
//
// 设计文档见 docs/DESIGN.md。当前进度：M0（骨架）与 M1（最小可用）已完成，
// 对外提供 Options / Comparer / DB / WriteBatch。
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
// # 写入语义
//
// 所有写入都收敛到 DB.Write：Put / Delete 只是它的语法糖。一次 Write 里的
// 所有记录编码成一条 WAL 记录，因此要么整批可见、要么整批不可见。
// SyncWrites 为真时，Write 返回即代表数据已经 fsync 落盘。
//
// # 已经具备的能力
//
//   - WAL：分块顺序追加、每条 CRC32C 校验、崩溃后重放、尾部损坏可容忍
//   - MemTable：跳表实现，读无锁、写串行；写满冻结为 Immutable MemTable
//   - 后台 Flush：Immutable 落成 SST 后即可删除对应的日志
//   - 崩溃恢复：目录扫描 + WAL 重放，被强杀的进程不会让数据目录打不开
//   - 目录锁：同一目录同时只允许一个实例打开，锁由内核持有、进程消亡即释放
//
// # 尚未具备的能力（见设计文档的实施路线）
//
// 索引、Bloom Filter、块缓存、迭代器（M2）；Flush 与分层 Compaction、
// Manifest（M3）；快照、Group Commit（M4）。因此当前版本的点查是
// SST 文件内的线性扫描，读延迟会随文件数线性增长。
package kvdb
