// Package version 管理 LSM-Tree 的"版本"：当前有哪些 SST 文件、各自在哪一层、
// 以及这些信息如何原子地持久化与切换。
//
// 为什么需要它 —— M2 之前"当前有哪些文件"是目录扫描出来的，这个做法有两个硬伤：
//
//  1. **没有原子性**：一次 Compaction 要"删掉 N 个输入文件、加进 M 个输出文件"，
//     目录扫描看到的是操作中途的形态，无法区分"已经提交"与"做了一半"；
//  2. **没有分层信息**：目录里躺着一堆 .sst，谁是 L0、谁是 L3 只能靠文件编号猜。
//
// M3 用 Manifest + Version 解决：
//
//	MANIFEST-000123  一串 VersionEdit 的追加日志（块化 + CRC32C，复用 WAL 的记录格式）
//	CURRENT          一行文本，指向当前生效的 Manifest
//	Version          一份不可变的"各层文件快照"，带引用计数
//	VersionSet       持有当前 Version，并负责把变更追加到 Manifest 后原子换版本
//
// 核心不变式（整个包的正确性都建立在它上面）：
//
//	**先 fsync Manifest，再让新版本可见。**
//
// 于是"打开时看到的状态"永远是某个已提交编辑之后的完整状态；崩溃只会丢掉
// 最后一次没写完的追加，而那次追加的调用方根本没拿到成功返回。
package version

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/xiatianliang1024gm/kvdb/internal/key"
)

// ErrNotOpen 表示 VersionSet 还没有可追加的 Manifest。
var ErrNotOpen = errors.New("kvdb/version: version set has no open manifest")

// FileMeta 描述一个已提交、可读取的 SST 文件。
//
// Smallest / Largest 存的是 **internal key**（带 (seq, kind) 尾缀），它们决定了
// L1 以下"文件区间互不重叠"这条能成立，也决定了一次点查能不能二分定位到唯一一个文件。
type FileMeta struct {
	Num  uint64
	Size uint64
	// Smallest 是文件里最小的 internal key。
	Smallest []byte
	// Largest 是文件里最大的 internal key。
	Largest []byte
}

// Edit 把文件打包成一个"加入第 level 层"的变更项。
func (f *FileMeta) Edit(level int) FileEdit {
	return FileEdit{Level: level, Num: f.Num, Size: f.Size, Smallest: f.Smallest, Largest: f.Largest}
}

// fileFromEdit 把变更项还原成元信息（丢掉层号，层号由它在 levels 里的位置表达）。
func fileFromEdit(level int, e FileEdit) *FileMeta {
	return &FileMeta{
		Num:      e.Num,
		Size:     e.Size,
		Smallest: append([]byte(nil), e.Smallest...),
		Largest:  append([]byte(nil), e.Largest...),
	}
}

// Version 是一个不可变的版本快照：levels[i] 是第 i 层的文件。
//
// 不可变这点很关键 —— 读者（点查、迭代器、快照）拿到一个 Version 之后，
// 它指向的文件集合在整个读过程中都不会变，因此不需要在读路径上加任何锁。
// 新版本是在旧版本上"派生"出来的（applyEdit），旧版本照旧可用。
//
//	levels[0]     L0：按文件编号升序（编号大 = 新）。区间可能互相重叠，只能从新到旧线性查找
//	levels[i>0]   L1+：按 Smallest 升序，区间互不重叠，可以二分定位
type Version struct {
	vset   *VersionSet
	levels [][]*FileMeta
	refs   atomic.Int32
}

// Ref 增加一个引用。调用方用完之后必须 Unref。
func (v *Version) Ref() { v.refs.Add(1) }

// RefCount 返回当前引用计数。
//
// 它只用于"顺手回收"这种启发式判断（引用归零了就试着清理一次文件），
// 不参与任何正确性决策 —— 并发的增删只会让结果偏大或偏小，
// 而偏大只是让清理晚一点发生，偏小则根本不会发生（计数不可能小于 0）。
func (v *Version) RefCount() int { return int(v.refs.Load()) }

// Unref 释放一个引用；归零时把自己从 VersionSet 的存活集合里摘掉。
//
// 摘掉之后，"只存在于这个版本里"的文件才可以被删除 —— 这正是迭代器能安全地
// 一直读旧文件的原因：它握着引用，文件就不会被 Compaction 顺手删掉。
//
// **不要在持有 VersionSet.mu 时调用它**：摘存活集合要拿那把锁，sync.Mutex 不可重入，
// 会直接死锁。持锁的调用方请用 unrefLocked。
func (v *Version) Unref() {
	if v.refs.Add(-1) == 0 {
		v.vset.mu.Lock()
		v.vset.removeLiveLocked(v)
		v.vset.mu.Unlock()
	}
}

// unrefLocked 是 Unref 的"已持 VersionSet.mu"版本。
//
// VersionSet 内部换版本时用的就是它：LogAndApply 已经把 old 的那一份引用换成了
// nv 的，此时必须在同一个临界区里把 old 摘掉，否则 re-lock 就是自锁。
// 这个坑很隐蔽 —— 单独用 VersionSet 的时候必现，而 DB 里因为 db.v 一直握着
// 一个长期引用，old 的计数永远降不到 0，于是"看起来是好的"。
func (v *Version) unrefLocked() {
	if v.refs.Add(-1) == 0 {
		v.vset.removeLiveLocked(v)
	}
}

// NumLevels 返回层数。
func (v *Version) NumLevels() int { return len(v.levels) }

// Comparer 返回这个版本使用的 internal key 比较器。
//
// 暴露它是为了让 compact 包能自己做区间判断：那些判断（"两个文件重叠吗"、
// "这个 key 落在哪一段"）用的必须是和版本一模一样的排序规则，各写一份迟早会分叉。
func (v *Version) Comparer() key.InternalComparer { return v.vset.icmp }

// Files 返回第 level 层的文件（只读，调用方不得修改）。
func (v *Version) Files(level int) []*FileMeta {
	if level < 0 || level >= len(v.levels) {
		return nil
	}
	return v.levels[level]
}

// FileCount 返回参与读取的文件总数（所有层之和）。
func (v *Version) FileCount() int {
	n := 0
	for _, files := range v.levels {
		n += len(files)
	}
	return n
}

// LevelBytes 返回第 level 层的文件总字节数。
func (v *Version) LevelBytes(level int) uint64 {
	var total uint64
	for _, f := range v.Files(level) {
		total += f.Size
	}
	return total
}

// AllFiles 返回所有层的文件，按层号从小到大。
func (v *Version) AllFiles() []*FileMeta {
	out := make([]*FileMeta, 0, v.FileCount())
	for _, files := range v.levels {
		out = append(out, files...)
	}
	return out
}

// applyEdit 在旧版本上套用一次变更，返回派生出的新版本。
//
// 它不修改旧版本（只复制切片头），因此可以放心地在后台 Compaction 里慢慢算，
// 而前台的读继续用旧版本。
func (v *Version) applyEdit(e *VersionEdit) (*Version, error) {
	levels := make([][]*FileMeta, len(v.levels))
	for i := range v.levels {
		levels[i] = append([]*FileMeta(nil), v.levels[i]...)
	}
	for _, d := range e.Deleted {
		if d.Level < 0 || d.Level >= len(levels) {
			return nil, fmt.Errorf("kvdb/version: delete file %d from invalid level %d", d.Num, d.Level)
		}
		levels[d.Level] = removeFile(levels[d.Level], d.Num)
	}
	for _, a := range e.Added {
		if a.Level < 0 || a.Level >= len(levels) {
			return nil, fmt.Errorf("kvdb/version: add file %d to invalid level %d", a.Num, a.Level)
		}
		levels[a.Level] = insertFile(levels[a.Level], fileFromEdit(a.Level, a), v.vset.icmp, a.Level == 0)
	}
	return &Version{vset: v.vset, levels: levels}, nil
}

// removeFile 从有序列表里摘掉编号为 num 的文件。找不到就原样返回。
func removeFile(files []*FileMeta, num uint64) []*FileMeta {
	for i, f := range files {
		if f.Num == num {
			return append(files[:i], files[i+1:]...)
		}
	}
	return files
}

// insertFile 把文件插进有序列表：L0 按编号升序（编号大 = 新），L1+ 按 Smallest 升序。
//
// 每次插入都重新排序而不是二分插入：单个编辑涉及的文件只有几十个，
// 而"排序规则写错"是这类代码最容易出的 bug，排序让规则集中在 lessFile 一处。
func insertFile(files []*FileMeta, f *FileMeta, icmp key.InternalComparer, byNum bool) []*FileMeta {
	files = append(files, f)
	if byNum {
		sort.SliceStable(files, func(i, j int) bool { return files[i].Num < files[j].Num })
		return files
	}
	sort.SliceStable(files, func(i, j int) bool { return icmp.Compare(files[i].Smallest, files[j].Smallest) < 0 })
	return files
}

// FindFile 在 L1 以下的层里定位"可能包含 userKey 的那个文件"，没有则返回 nil。
//
// target 是带 (snapshot, TypeValue) 尾缀的 internal key，用于在 Largest 上二分；
// userKey 是它的 user 部分，用于最终的越界判断。
//
// 两个参数都要，而且分工必须分清，这里是本文件最容易写错的一处：
//
//   - 二分只能用 internal key 比较（Largest >= target），否则会漏掉"该 key 在文件里
//     但更新版本排在 target 之前"的文件；
//   - 越界判断只能用 **user key** 比较。用 internal key 比会误判：假设文件里
//     key "k" 只有 seq=1 的版本，而查找用的是 snapshot=200，那么 target 是
//     ("k",200) 而文件 Smallest 是 ("k",1) —— 同 key、尾缀降序下 ("k",1) 反而"更大"，
//     于是整个文件被当成"key 在它之前"直接跳过，数据明明在文件里却读不到。
func (v *Version) FindFile(level int, userKey, target []byte) *FileMeta {
	files := v.Files(level)
	if level <= 0 || len(files) == 0 {
		return nil
	}
	icmp := v.vset.icmp
	i := sort.Search(len(files), func(i int) bool {
		return icmp.Compare(files[i].Largest, target) >= 0
	})
	if i == len(files) {
		return nil
	}
	if icmp.CompareUser(userKey, key.UserKey(files[i].Smallest)) < 0 {
		return nil
	}
	return files[i]
}

// Overlapping 返回第 level 层里与 [smallest, largest] 有重叠的全部文件。
//
// Compaction 用它挑出"下一层里必须一起参与归并的文件"。一个都不能漏：
// 漏掉的文件会和输出文件落在同一段 key 区间上，直接破坏 L1 以下的
// "同层不重叠"不变式，而那个不变式是二分查找的全部依据。
func (v *Version) Overlapping(level int, smallest, largest []byte) []*FileMeta {
	files := v.Files(level)
	if len(files) == 0 {
		return nil
	}
	icmp := v.vset.icmp
	// 第一个 Largest >= smallest 的文件是区间起点；从这里往后一直取到 Smallest > largest。
	start := sort.Search(len(files), func(i int) bool {
		return icmp.Compare(files[i].Largest, smallest) >= 0
	})
	var out []*FileMeta
	for i := start; i < len(files); i++ {
		if icmp.Compare(files[i].Smallest, largest) > 0 {
			break
		}
		out = append(out, files[i])
	}
	return out
}

// String 返回各层文件数的可读描述。
func (v *Version) String() string {
	var b strings.Builder
	b.WriteString("Version[")
	for i, files := range v.levels {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "L%d=%d", i, len(files))
	}
	b.WriteByte(']')
	return b.String()
}

// Config 是 VersionSet 的构造参数。
type Config struct {
	// Dir 是数据目录，Manifest 与 CURRENT 都放在这里。
	Dir string
	// Comparer 决定 key 顺序；它的 Name() 会写进 Manifest，用于校验目录与配置匹配。
	Comparer key.Comparer
	// FilterName 是 CompactionFilter 的稳定标识（未配置过滤器时为空）。
	// 它会写进 Manifest 并在重放时校验，语义同 Comparer 的名字：目录一旦用某个
	// 过滤器落过盘，之后就只能用同名过滤器打开。
	FilterName string
	// MaxLevels 是层数（含 L0）。
	MaxLevels int
}

// VersionSet 持有当前版本，并负责把版本变更持久化到 Manifest。
//
// 锁的顺序（全局约定，写反了会死锁）：
//
//	db.mu  →  VersionSet.mu
//
// VersionSet 从不在持锁时回调到 DB。因此 DB 可以在写锁里安全地调用这里的方法。
type VersionSet struct {
	dir  string
	icmp key.InternalComparer
	cfg  Config

	mu           sync.Mutex
	current      *Version
	live         []*Version // 所有引用计数 > 0 的版本（含 current）
	nextFileNum  uint64
	lastSeq      uint64
	logNumber    uint64
	manifest     *Manifest
	manifestNum  uint64
	comparerName string
	filterName   string
	closed       bool
}

// New 创建一个尚未加载任何状态的 VersionSet。真正可用之前必须先 Recover 或 SetFromScan。
func New(cfg Config) *VersionSet {
	if cfg.MaxLevels < 2 {
		cfg.MaxLevels = 2
	}
	vs := &VersionSet{
		dir:          cfg.Dir,
		cfg:          cfg,
		icmp:         key.InternalComparer{User: cfg.Comparer},
		comparerName: cfg.Comparer.Name(),
		filterName:   cfg.FilterName,
		nextFileNum:  1,
	}
	vs.current = vs.emptyVersion()
	vs.current.Ref()
	vs.live = append(vs.live, vs.current)
	return vs
}

// emptyVersion 返回一份各层都为空的新版本。
func (vs *VersionSet) emptyVersion() *Version {
	return &Version{vset: vs, levels: make([][]*FileMeta, vs.cfg.MaxLevels)}
}

// ComparerName 返回写入 Manifest 的比较器标识。
func (vs *VersionSet) ComparerName() string { return vs.comparerName }

// MaxLevels 返回层数。
func (vs *VersionSet) MaxLevels() int { return vs.cfg.MaxLevels }

// Current 返回当前版本，并额外持有一个引用；调用方用完必须 Unref。
func (vs *VersionSet) Current() *Version {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.current.Ref()
	return vs.current
}

// NextFileNum 返回下一个可用的文件编号（不分配）。
func (vs *VersionSet) NextFileNum() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.nextFileNum
}

// AllocFileNum 分配一个新的文件编号。
//
// 编号由 Manifest 持久化，因此重启后一定大于目录里出现过的所有编号 ——
// 块缓存的键正是 (文件编号, 块偏移)，编号复用会让缓存串味。
func (vs *VersionSet) AllocFileNum() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	n := vs.nextFileNum
	vs.nextFileNum++
	return n
}

// RaiseNextFileNum 把下一个编号抬到至少 n，用于迁移旧目录（编号来自目录扫描）。
func (vs *VersionSet) RaiseNextFileNum(n uint64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if n > vs.nextFileNum {
		vs.nextFileNum = n
	}
}

// LastSeq 返回已提交的最大序列号。
func (vs *VersionSet) LastSeq() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.lastSeq
}

// SetLastSeq 抬高已提交的最大序列号（只在打开数据库时用，写入路径不走这里）。
func (vs *VersionSet) SetLastSeq(seq uint64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if seq > vs.lastSeq {
		vs.lastSeq = seq
	}
}

// LogNumber 返回当前 WAL 编号。
func (vs *VersionSet) LogNumber() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.logNumber
}

// SetLogNumber 更新当前 WAL 编号。
func (vs *VersionSet) SetLogNumber(n uint64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if n > vs.logNumber {
		vs.logNumber = n
	}
}

// ManifestNum 返回当前 Manifest 的编号；0 表示还没有。
func (vs *VersionSet) ManifestNum() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.manifestNum
}

// removeLiveLocked 在一个版本的引用计数归零时把它从存活集合里摘掉。
// 调用方必须持有 vs.mu。
func (vs *VersionSet) removeLiveLocked(v *Version) {
	for i, x := range vs.live {
		if x == v {
			vs.live = append(vs.live[:i], vs.live[i+1:]...)
			return
		}
	}
}

// LiveFileNums 返回所有存活版本引用到的文件编号集合。
//
// 只有**不在**这个集合里的 .sst 才能删除：它既不属于当前版本，也没有任何
// 迭代器或快照还握着旧版本。
func (vs *VersionSet) LiveFileNums() map[uint64]bool {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	out := make(map[uint64]bool, 64)
	for _, v := range vs.live {
		for _, files := range v.levels {
			for _, f := range files {
				out[f.Num] = true
			}
		}
	}
	return out
}

// LogAndApply 把一次变更追加到 Manifest 并原子地换上新版本。
//
// 执行顺序是不可调换的：
//
//  1. 追加记录并 fsync —— 这一步失败就整体失败，磁盘上什么都没变；
//  2. 派生新版本、换掉 current、释放旧版本的那一份引用。
//
// 反过来（先换版本再落盘）会制造出一个窗口：崩溃后磁盘上不知道那批新文件
// 算不算数，只能靠"扫描目录 + 猜"，正是 M2 的形态。
func (vs *VersionSet) LogAndApply(e *VersionEdit) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.closed {
		return errors.New("kvdb/version: version set is closed")
	}
	if vs.manifest == nil {
		return ErrNotOpen
	}
	if err := vs.manifest.Append(e.Encode()); err != nil {
		return err
	}
	if err := vs.manifest.Sync(); err != nil {
		return err
	}
	vs.applyCountersLocked(e)

	nv, err := vs.current.applyEdit(e)
	if err != nil {
		return err
	}
	nv.Ref()
	vs.live = append(vs.live, nv)
	old := vs.current
	vs.current = nv
	old.unrefLocked() // 版本集持有的那一份；还有读者时不会归零
	return nil
}

// applyCountersLocked 把记录里的全局计数合并进来（只增不减）。
func (vs *VersionSet) applyCountersLocked(e *VersionEdit) {
	if e.NextFileNum > vs.nextFileNum {
		vs.nextFileNum = e.NextFileNum
	}
	if e.LastSeq > vs.lastSeq {
		vs.lastSeq = e.LastSeq
	}
	if e.LogNumber > vs.logNumber {
		vs.logNumber = e.LogNumber
	}
}

// SetFromScan 用"目录扫描的结果"建立初始版本，供 M2 目录迁移使用。
//
// 扫出来的文件全部放进 L0：我们不知道它们的区间关系，而 L0 的规则
// （区间可以重叠、按编号从新到旧线性查找）对任何输入都成立。
// 第一次 Compaction 之后它们就会收敛成 L1 以下的有序不重叠布局。
func (vs *VersionSet) SetFromScan(files []*FileMeta, maxFileNum uint64) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if len(files) > 0 {
		vs.nextFileNum = maxFileNum + 1
	} else if maxFileNum+1 > vs.nextFileNum {
		vs.nextFileNum = maxFileNum + 1
	}
	old := vs.current
	nv := vs.emptyVersion()
	nv.levels[0] = append([]*FileMeta(nil), files...)
	sort.Slice(nv.levels[0], func(i, j int) bool { return nv.levels[0][i].Num < nv.levels[0][j].Num })
	nv.Ref()
	vs.live = append(vs.live, nv)
	vs.current = nv
	old.unrefLocked()
	return nil
}

// Close 关闭 Manifest。
func (vs *VersionSet) Close() error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.closed {
		return nil
	}
	vs.closed = true
	if vs.manifest == nil {
		return nil
	}
	err := vs.manifest.Close()
	vs.manifest = nil
	return err
}
