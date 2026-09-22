package kvdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/xiatianliang1024gm/kvdb/internal/key"
)

// 写入批次的二进制布局（整数字段大端，与项目既有约定一致）：
//
//	┌──────────────┬───────────────┬──────────────────────────────┐
//	│ seq (8B)     │ count (4B)    │ 记录区                        │
//	└──────────────┴───────────────┴──────────────────────────────┘
//
//	记录 = 类型(1B) | uvarint(keyLen) | key | [uvarint(valLen) | value]
//
// 头部里的 seq 是批次中**第一条**记录的序列号，其余记录依次 +1。
// 把序列号写进日志是崩溃恢复的关键：重放时不需要（也不能）重新分配序列号，
// 否则同一个目录在不同次恢复后会得到不同的版本顺序。
const (
	batchHeaderLen = 8 + 4
)

var (
	// ErrEmptyKey 表示 key 为空。
	ErrEmptyKey = errors.New("kvdb: key must not be empty")
	// ErrBatchCorrupt 表示从日志里读回批次时发现数据损坏。
	ErrBatchCorrupt = errors.New("kvdb: corrupt write batch")
	// ErrBatchTooLarge 表示单个批次超过了单条 WAL 记录允许的上限。
	ErrBatchTooLarge = fmt.Errorf("kvdb: write batch exceeds %d bytes", MaxBatchBytes)
	// ErrInvalidRange 表示 DeleteRange 的区间不合法（end <= start 的半开区间是空的）。
	ErrInvalidRange = errors.New("kvdb: DeleteRange requires end > start")
)

// MaxBatchBytes 是单个 WriteBatch 编码后的上限。
//
// 批次会被整体塞进一条 WAL 记录，设上限是为了避免一条超大记录把日志的
// 内存占用顶上去；64MB 远大于正常使用场景。
const MaxBatchBytes = 64 << 20

// WriteBatch 是一组原子生效的写入。
//
// 它只是"写入意图"的容器，不持有任何存储资源；真正落盘发生在 DB.Write 里：
// 整个批次编码成一条 WAL 记录，因此同一批次的记录要么全部可见、要么全部不可见。
type WriteBatch struct {
	seq   uint64 // 起始序列号，由 DB 在写入前赋值
	count int    // 记录条数
	data  []byte // 记录区，不含头部
}

// NewWriteBatch 创建一个空批次。
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{data: make([]byte, 0, 128)}
}

// Len 返回批次中的记录条数。
func (b *WriteBatch) Len() int { return b.count }

// Sequence 返回批次起始序列号。
func (b *WriteBatch) Sequence() uint64 { return b.seq }

// SetSequence 设置批次起始序列号，由 DB 在写入前调用。
func (b *WriteBatch) SetSequence(seq uint64) { b.seq = seq }

// Reset 清空批次以便复用，底层缓冲会被保留因此不会反复分配。
func (b *WriteBatch) Reset() {
	b.seq = 0
	b.count = 0
	b.data = b.data[:0]
}

// Put 追加一条写入。value 允许为空（空串与"不存在"是两种不同的结果）。
func (b *WriteBatch) Put(userKey, value []byte) error {
	return b.addRecord(key.TypeValue, userKey, value)
}

// Delete 追加一条删除。它写入的是墓碑标记，物理删除发生在后续 Compaction。
func (b *WriteBatch) Delete(userKey []byte) error {
	return b.addRecord(key.TypeDeletion, userKey, nil)
}

// DeleteRange 追加一条范围删除：半开区间 [start, end) 内的所有键全部不可见。
//
// 它**不**逐键写墓碑，而是写一条范围墓碑记录（key = start，value = end）：
// 一条记录覆盖任意大的区间，成本与区间内的键数无关——这是 SQL 的
// DROP TABLE / TRUNCATE 和 Redis 的 DEL 大 key 能成立的前提。
//
// 语义要点（docs/EXTENSIONS.md §4.1）：
//   - 可见性立即生效：返回后范围内所有键读不到、迭代器扫不到，对之前取的
//     快照仍可见；
//   - 物理回收发生在后续 Compaction，删除后磁盘占用不会立刻下降；
//   - end <= start 是空区间，返回 ErrInvalidRange 而不是静默无事发生——
//     那几乎总是调用方的 bug（比如算错了前缀的后继）。
func (b *WriteBatch) DeleteRange(start, end []byte) error {
	if len(start) == 0 || len(end) == 0 {
		return ErrEmptyKey
	}
	// 比较器语义下 start >= end 都算空区间；这里用字节序即可——
	// 自定义 Comparer 也必须与字节序在"前缀包含"上同向，否则整个引擎
	// 的区间运算（SST 文件区间、Compaction 选择）都不成立。
	if bytes.Compare(start, end) >= 0 {
		return ErrInvalidRange
	}
	return b.addRecord(key.TypeRangeDeletion, start, end)
}

// addRecord 把一条记录追加到记录区。
func (b *WriteBatch) addRecord(kind key.Kind, userKey, value []byte) error {
	if len(userKey) == 0 {
		return ErrEmptyKey
	}
	if len(b.data) > MaxBatchBytes {
		return ErrBatchTooLarge
	}
	b.data = append(b.data, byte(kind))
	b.data = key.PutUvarint(b.data, uint64(len(userKey)))
	b.data = append(b.data, userKey...)
	if hasValue(kind) {
		b.data = key.PutUvarint(b.data, uint64(len(value)))
		b.data = append(b.data, value...)
	}
	b.count++
	return nil
}

// hasValue 表示该记录类型的编码里是否带 value 字段。
//
// 范围墓碑是"非 Value 记录也带 value"的唯一一种：value 存的是区间终点 End。
func hasValue(kind key.Kind) bool {
	return kind == key.TypeValue || kind == key.TypeRangeDeletion
}

// Encode 返回批次的完整编码（头部 + 记录区）。
func (b *WriteBatch) Encode() []byte {
	return b.EncodeTo(make([]byte, 0, batchHeaderLen+len(b.data)))
}

// EncodeTo 把批次编码追加到 dst 末尾，返回扩展后的切片，避免重复分配。
func (b *WriteBatch) EncodeTo(dst []byte) []byte {
	dst = key.PutFixed64(dst, b.seq)
	dst = key.PutFixed32(dst, uint32(b.count))
	return append(dst, b.data...)
}

// rangeRecords 按序把批次里的每条记录交给 fn，序列号从 start 起逐条递增。
//
// fn 返回 false 时停止遍历。正常构造（或经 decodeBatch 校验）的批次不会解析失败，
// 返回 error 只是为了让损坏数据不至于被静默吞掉。
//
// 它之所以不导出：回调签名里的 key.Kind 来自 internal 包，包外既 import 不到
// github.com/xiatianliang1024gm/kvdb/internal/key，也就写不出这个函数字面量 —— 导出等于给了一个用不了的
// 方法。所有调用点（组提交落库、WAL 重放、单测）都在包内。
func (b *WriteBatch) rangeRecords(start uint64, fn func(seq uint64, kind key.Kind, userKey, value []byte) bool) error {
	seq := start
	rest := b.data
	for i := 0; i < b.count; i++ {
		kind := key.Kind(rest[0])
		klen, n, err := key.Uvarint(rest[1:])
		if err != nil {
			return fmt.Errorf("%w: key length: %v", ErrBatchCorrupt, err)
		}
		rest = rest[1+n:]
		if int(klen) > len(rest) {
			return fmt.Errorf("%w: key claims %d bytes, %d left", ErrBatchCorrupt, klen, len(rest))
		}
		userKey := rest[:klen]
		rest = rest[klen:]

		var value []byte
		if hasValue(kind) {
			vlen, vn, err := key.Uvarint(rest)
			if err != nil {
				return fmt.Errorf("%w: value length: %v", ErrBatchCorrupt, err)
			}
			rest = rest[vn:]
			if int(vlen) > len(rest) {
				return fmt.Errorf("%w: value claims %d bytes, %d left", ErrBatchCorrupt, vlen, len(rest))
			}
			value = rest[:vlen]
			rest = rest[vlen:]
		}
		if !fn(seq, kind, userKey, value) {
			return nil
		}
		seq++
	}
	return nil
}

// decodeBatch 解析一个编码后的批次，用于 WAL 重放。
//
// 它同时校验头部与记录区的长度是否自洽，任何不一致都返回 ErrBatchCorrupt：
// 日志里的字节可能被写坏，这里必须当作不可信输入。
func decodeBatch(rep []byte) (*WriteBatch, error) {
	if len(rep) < batchHeaderLen {
		return nil, fmt.Errorf("%w: needs at least %d bytes, got %d", ErrBatchCorrupt, batchHeaderLen, len(rep))
	}
	seq := binary.BigEndian.Uint64(rep[0:8])
	count := int(binary.BigEndian.Uint32(rep[8:12]))
	if count < 0 {
		return nil, fmt.Errorf("%w: negative count %d", ErrBatchCorrupt, count)
	}
	b := &WriteBatch{seq: seq, count: count, data: rep[batchHeaderLen:]}

	// 按 count 逐条走一遍，确保记录区长度与计数一致。
	rest := b.data
	for i := 0; i < count; i++ {
		if len(rest) == 0 {
			return nil, fmt.Errorf("%w: record %d/%d missing", ErrBatchCorrupt, i+1, count)
		}
		kind := key.Kind(rest[0])
		if kind != key.TypeValue && kind != key.TypeDeletion && kind != key.TypeRangeDeletion {
			return nil, fmt.Errorf("%w: unknown kind %d", ErrBatchCorrupt, uint8(kind))
		}
		klen, n, err := key.Uvarint(rest[1:])
		if err != nil {
			return nil, fmt.Errorf("%w: key length: %v", ErrBatchCorrupt, err)
		}
		rest = rest[1+n:]
		if int(klen) > len(rest) {
			return nil, fmt.Errorf("%w: key claims %d bytes, %d left", ErrBatchCorrupt, klen, len(rest))
		}
		rest = rest[klen:]
		if hasValue(kind) {
			vlen, vn, err := key.Uvarint(rest)
			if err != nil {
				return nil, fmt.Errorf("%w: value length: %v", ErrBatchCorrupt, err)
			}
			rest = rest[vn:]
			if int(vlen) > len(rest) {
				return nil, fmt.Errorf("%w: value claims %d bytes, %d left", ErrBatchCorrupt, vlen, len(rest))
			}
			rest = rest[vlen:]
		}
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrBatchCorrupt, len(rest))
	}
	return b, nil
}
