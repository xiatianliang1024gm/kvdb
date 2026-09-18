package version

import (
	"fmt"

	"kvdb/internal/key"
)

// VersionEdit 描述一次版本变更：新增了哪些文件、删除了哪些文件，
// 以及随变更一起持久化的全局计数。
//
// 它是 Manifest 的"记录单位"：Manifest 就是一连串 VersionEdit 的追加日志，
// 从头重放一遍就能重建当前版本。用追加日志而不是"每次写一份完整快照"，是因为
// 变更本身很小，而快照会随文件数增长 —— 打开一个上千文件的库不该重写几 MB 元数据。
// 完整快照只在两种情况出现：打开数据库时（把选中的 Manifest 收敛成一份），
// 以及 M2 目录迁移时（把目录扫描的结果固化成第一份 Manifest）。
type VersionEdit struct {
	// ComparatorName 只在第一条记录里出现。重放时用它校验"数据目录与配置是否匹配"：
	// 换一个比较器读同一个目录，块的排序假设立刻不成立。
	ComparatorName string

	// NextFileNum 是"本记录生效之后"的下一个可用文件编号。
	NextFileNum uint64
	// LastSeq 是已提交的最大序列号。
	//
	// 它是 M3 修掉的一个真实缺陷：M2 的 lastSeq 只能靠重放 WAL 恢复，
	// 而"所有 MemTable 都已落盘、当前日志是空的"这个完全正常的时刻一旦崩溃，
	// 重启后的 lastSeq 会退回 0，新写入就会与 SST 里的老记录撞号。
	LastSeq uint64
	// LogNumber 是当前正在追加的 WAL 编号，用于诊断（哪些日志还有用）。
	LogNumber uint64

	// Added / Deleted 是本次变更涉及的文件。
	Added   []FileEdit
	Deleted []FileEdit
}

// FileEdit 是 VersionEdit 里针对单个文件的变更描述。
//
// 删除时只有 Level 与 Num 有意义；新增时五个字段都要写全，
// 因为恢复后的版本不再去读文件头，全靠这里记录的 key 区间做分层查找。
type FileEdit struct {
	Level    int
	Num      uint64
	Size     uint64
	Smallest []byte // internal key
	Largest  []byte // internal key
}

// 标签值沿用 LevelDB 的编号（1..9），便于对照它的 Manifest 实现排查问题。
// 用"每段自带标签"而不是固定顺序，是为了让将来的字段可以只出现在部分记录里：
// 重放一串历史记录时，老记录里没有新字段是正常的，固定顺序做不到这点。
const (
	tagComparator  = 1
	tagLogNumber   = 2
	tagNextFileNum = 3
	tagLastSeq     = 4
	tagNewFile     = 7
	tagDeletedFile = 9
)

// Encode 把变更编码成一个字节串。零值字段不写入，因此"只改文件列表"的记录非常小。
func (e *VersionEdit) Encode() []byte {
	var dst []byte
	if e.ComparatorName != "" {
		dst = key.PutUvarint(dst, tagComparator)
		dst = appendBytes(dst, []byte(e.ComparatorName))
	}
	if e.LogNumber != 0 {
		dst = key.PutUvarint(dst, tagLogNumber)
		dst = key.PutUvarint(dst, e.LogNumber)
	}
	if e.NextFileNum != 0 {
		dst = key.PutUvarint(dst, tagNextFileNum)
		dst = key.PutUvarint(dst, e.NextFileNum)
	}
	if e.LastSeq != 0 {
		dst = key.PutUvarint(dst, tagLastSeq)
		dst = key.PutUvarint(dst, e.LastSeq)
	}
	for _, f := range e.Added {
		dst = key.PutUvarint(dst, tagNewFile)
		dst = key.PutUvarint(dst, uint64(f.Level))
		dst = key.PutUvarint(dst, f.Num)
		dst = key.PutUvarint(dst, f.Size)
		dst = appendBytes(dst, f.Smallest)
		dst = appendBytes(dst, f.Largest)
	}
	for _, f := range e.Deleted {
		dst = key.PutUvarint(dst, tagDeletedFile)
		dst = key.PutUvarint(dst, uint64(f.Level))
		dst = key.PutUvarint(dst, f.Num)
	}
	return dst
}

// DecodeVersionEdit 解析一条编码后的变更。
//
// 它对输入做完整的边界检查：任何"声称的长度超过剩余字节"都在这里被拒绝。
// 这条检查不是洁癖 —— Manifest 的尾记录可能是崩溃时写了一半的，一个没校验的
// 长度字段会让恢复路径直接按它去分配内存。
func DecodeVersionEdit(buf []byte) (*VersionEdit, error) {
	e := &VersionEdit{}
	r := reader{buf: buf}
	for !r.done() {
		tag, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		switch tag {
		case tagComparator:
			b, err := r.bytes()
			if err != nil {
				return nil, err
			}
			e.ComparatorName = string(b)
		case tagLogNumber:
			if e.LogNumber, err = r.uvarint(); err != nil {
				return nil, err
			}
		case tagNextFileNum:
			if e.NextFileNum, err = r.uvarint(); err != nil {
				return nil, err
			}
		case tagLastSeq:
			if e.LastSeq, err = r.uvarint(); err != nil {
				return nil, err
			}
		case tagNewFile:
			f, err := r.file()
			if err != nil {
				return nil, err
			}
			e.Added = append(e.Added, f)
		case tagDeletedFile:
			level, err := r.uvarint()
			if err != nil {
				return nil, err
			}
			num, err := r.uvarint()
			if err != nil {
				return nil, err
			}
			e.Deleted = append(e.Deleted, FileEdit{Level: int(level), Num: num})
		default:
			return nil, fmt.Errorf("kvdb/version: unknown manifest tag %d", tag)
		}
	}
	return e, nil
}

// String 返回可读描述，供诊断与测试使用。
func (e *VersionEdit) String() string {
	return fmt.Sprintf("VersionEdit{comparer=%q nextFileNum=%d lastSeq=%d logNum=%d +%d -%d}",
		e.ComparatorName, e.NextFileNum, e.LastSeq, e.LogNumber, len(e.Added), len(e.Deleted))
}

// appendBytes 追加"长度前缀 + 内容"。
func appendBytes(dst, b []byte) []byte {
	dst = key.PutUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

// reader 是一个带边界检查的前向游标。
type reader struct {
	buf []byte
	pos int
}

func (r *reader) done() bool { return r.pos >= len(r.buf) }

// uvarint 读一个 varint。
func (r *reader) uvarint() (uint64, error) {
	if r.done() {
		return 0, fmt.Errorf("kvdb/version: truncated manifest record")
	}
	v, n, err := key.Uvarint(r.buf[r.pos:])
	if err != nil {
		return 0, fmt.Errorf("kvdb/version: bad varint in manifest: %w", err)
	}
	r.pos += n
	return v, nil
}

// bytes 读一个长度前缀字符串。
func (r *reader) bytes() ([]byte, error) {
	n, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if n > uint64(len(r.buf)-r.pos) {
		return nil, fmt.Errorf("kvdb/version: manifest record claims %d bytes but only %d left", n, len(r.buf)-r.pos)
	}
	out := r.buf[r.pos : r.pos+int(n)]
	r.pos += int(n)
	return out, nil
}

// file 读一个新增文件条目。
func (r *reader) file() (FileEdit, error) {
	var f FileEdit
	level, err := r.uvarint()
	if err != nil {
		return f, err
	}
	f.Level = int(level)
	if f.Num, err = r.uvarint(); err != nil {
		return f, err
	}
	if f.Size, err = r.uvarint(); err != nil {
		return f, err
	}
	if f.Smallest, err = r.bytes(); err != nil {
		return f, err
	}
	if f.Largest, err = r.bytes(); err != nil {
		return f, err
	}
	return f, nil
}
