package kvdb

// 本文件只做一件事：把 kvdb 的对外契约写成一个"能被一眼看见、被编译器检查"的形状。
//
// # 为什么 Open 返回 *DB 而不是接口
//
// Go 的惯例是"接受接口、返回结构体"。返回具体类型意味着以后给 DB 加方法
// （Compact、Flush、Range……）不会打破任何调用方；而返回接口的话，每加一个
// 方法都是一次对所有实现者的破坏性变更。bbolt、pebble、goleveldb 都返回具体类型。
//
// 这条惯例的代价是：**没人替你把"对外到底有哪些能力"汇总一遍**。DB 的 10 个
// 方法散在 db.go、db_write.go、db_iter.go、db_checkpoint.go 四个文件里，
// 而 go doc 只会按字母序平铺，看不出读 / 写 / 运维的分组。
//
// 下面这三个接口就是那份汇总，同时也是调用方抽象 kvdb 的抓手。文件末尾的
// `var _ ... = (*DB)(nil)` 断言不产生任何运行时开销，它只是让"DB 少了一个方法"
// 在编译期就报错，而不是等到调用方那边才发现。
//
// # 没有收进接口的部分
//
// Stats / RecoveryReport / Checkpoint 属于运维，GetSnapshot 返回的是另一个
// 抽象（Snapshot），它们都绑死在具体实例上：抽象出来既没有第二个实现，
// 也没有调用方受益。想看全貌就翻这个文件顶部的注释。

// Reader 是只读能力。
//
// 它有两个实现，而且这不是巧合：**DB 与 Snapshot 的 Get / NewIterator
// 签名本就完全一致**。"对最新视图读"和"对某个固定序列号读"从来就是同一件
// 事的两个参数，这个接口只是把本来就存在的关系说了出来。
//
// 于是"要不要读一个一致的视图"可以在调用方之间自由切换，而读的那段代码不用改。
type Reader interface {
	// Get 读取 userKey 的可见版本；key 不存在或已被删除时返回 ErrNotFound。
	//
	// DB.Get 读到的是"调用那一刻已提交的最新版本"，Snapshot.Get 读到的是
	// 取快照那一刻的版本。返回的切片是一份独立数据，可以随意持有与修改。
	Get(userKey []byte) ([]byte, error)
	// NewIterator 返回一个归并了 MemTable 与所有 SST 的有序只读迭代器。
	// opt 为 nil 表示全表扫描。
	//
	// 迭代器持有构造那一刻的版本引用，所以它读的那批文件在 Close 之前不会被
	// Compaction 删掉 —— 这也是 Iterator.Close 必须调用的原因。
	NewIterator(opt *IteratorOptions) Iterator
}

// Writer 是写入能力。
//
// 三个方法不是并列关系：Write 是**唯一**的写入口，Put / Delete 只是它的语法糖
// （各自装一个只含一条记录的 WriteBatch）。所以原子性只在 Write 这一层有意义 ——
// 一次 Write 里的所有记录要么全部可见、要么全部不可见。
type Writer interface {
	// Put 写入一个键值对。value 允许为空（空串与"不存在"是两种不同的结果）。
	Put(userKey, value []byte) error
	// Delete 删除一个键。它写入墓碑，物理删除留给后续 Compaction。
	Delete(userKey []byte) error
	// Write 原子地写入一个批次，是引擎唯一的写入口。
	//
	// SyncWrites 为真时，返回 nil 代表数据已经 fsync 落盘。返回错误分两类，
	// 含义不同：ErrClosed（或此前已存在的后台错误）表示这一批一定没写进去；
	// 磁盘故障表示这一批**可能**已经写进去了，且整个库会停下来 —— 此时重新
	// 打开数据库即可，WAL 始终是唯一的事实来源。
	Write(b *WriteBatch) error
}

// Store 是一个可读可写、需要显式关闭的 KV 存储。
type Store interface {
	Reader
	Writer
	// Close 停止后台任务、把 WAL 落盘并释放目录锁。
	//
	// 它**不**把 MemTable 刷成 SST：内存里的数据本来就已经在 WAL 里，
	// 下次 Open 时会重放并落盘。
	Close() error
}

var (
	_ Reader = (*DB)(nil)
	_ Writer = (*DB)(nil)
	_ Store  = (*DB)(nil)

	// 快照只有读能力，它自己就是 Reader 的一个实现。
	_ Reader   = (*snapshot)(nil)
	_ Snapshot = (*snapshot)(nil)
)
