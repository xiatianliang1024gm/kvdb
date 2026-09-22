package version

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/xiatianliang1024gm/kvdb/internal/wal"
)

// Manifest 文件的命名与格式。
//
// 它是"版本变更的追加日志"，物理格式直接复用 WAL 的记录格式（32KB 分块、
// 每条 7 字节头、CRC32C 覆盖"类型字节 + 负载"）。复用而不是另造一套的理由很实际：
// 崩溃恢复要处理的形态（尾部半截记录、块尾补零、跨块记录）完全一样，
// 两套实现等于两处可能出错的地方，而 WAL 那套已经被崩溃测试打过一遍了。
const (
	// CurrentName 是指向当前生效 Manifest 的指针文件名。
	CurrentName = "CURRENT"
	// ManifestPrefix 是 Manifest 的文件名前缀。
	ManifestPrefix = "MANIFEST-"
	// currentTmpName 是写 CURRENT 时的临时文件名（写完 rename，保证指针不会半新半旧）。
	currentTmpName = "CURRENT.tmp"
)

// ManifestName 返回编号为 num 的 Manifest 文件路径。
func ManifestName(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%06d", ManifestPrefix, num))
}

// IsManifestName 判断文件名是否是 Manifest（供目录扫描跳过它）。
func IsManifestName(name string) bool {
	return strings.HasPrefix(name, ManifestPrefix)
}

// Manifest 是一个可追加、可 fsync 的 Manifest 文件句柄。
type Manifest struct {
	num  uint64
	path string
	f    *os.File
	buf  *bufio.Writer
	w    *wal.Writer
}

// createManifest 创建（或清空）编号为 num 的 Manifest。
func createManifest(dir string, num uint64) (*Manifest, error) {
	path := ManifestName(dir, num)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("kvdb/version: create manifest %s: %w", path, err)
	}
	buf := bufio.NewWriterSize(f, 64<<10)
	return &Manifest{num: num, path: path, f: f, buf: buf, w: wal.NewWriter(buf)}, nil
}

// Num 返回 Manifest 编号。
func (m *Manifest) Num() uint64 { return m.num }

// Path 返回文件路径。
func (m *Manifest) Path() string { return m.path }

// Size 返回已写入的逻辑字节数。
func (m *Manifest) Size() int64 { return m.w.Written() }

// Append 追加一条 VersionEdit 记录。它只写缓冲区，落盘由 Sync 保证。
func (m *Manifest) Append(record []byte) error {
	if err := m.w.Append(record); err != nil {
		return fmt.Errorf("kvdb/version: append to manifest %s: %w", m.path, err)
	}
	return nil
}

// Sync 刷出缓冲区并 fsync。返回后这条记录已经持久化。
func (m *Manifest) Sync() error {
	if err := m.buf.Flush(); err != nil {
		return fmt.Errorf("kvdb/version: flush manifest %s: %w", m.path, err)
	}
	if err := m.f.Sync(); err != nil {
		return fmt.Errorf("kvdb/version: sync manifest %s: %w", m.path, err)
	}
	return nil
}

// Close 刷出缓冲区并关闭句柄，不删除文件。
func (m *Manifest) Close() error {
	err := m.buf.Flush()
	if cerr := m.f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("kvdb/version: close manifest %s: %w", m.path, err)
	}
	return nil
}

// readCurrent 读回 CURRENT 指向的 Manifest 编号。
//
// 目录里没有 CURRENT 时返回 os.ErrNotExist 包裹的错误，调用方据此判断
// "这是一个还没有版本元数据的目录"（全新目录，或 M2 及更早的目录）。
func readCurrent(dir string) (uint64, error) {
	path := filepath.Join(dir, CurrentName)
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	name := strings.TrimSpace(string(b))
	if !strings.HasPrefix(name, ManifestPrefix) {
		return 0, fmt.Errorf("kvdb/version: %s contains %q, want a %s* file name", path, name, ManifestPrefix)
	}
	num, err := strconv.ParseUint(strings.TrimPrefix(name, ManifestPrefix), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("kvdb/version: %s contains an unparsable manifest name %q: %w", path, name, err)
	}
	return num, nil
}

// writeCurrent 原子地把 CURRENT 指向新的 Manifest。
//
// 先写临时文件、fsync、再 rename：rename 在同一文件系统内是原子的，
// 因此 CURRENT 要么指向旧的、要么指向新的，绝不会出现半截内容。
func writeCurrent(dir string, num uint64) error {
	tmp := filepath.Join(dir, currentTmpName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("kvdb/version: create %s: %w", tmp, err)
	}
	content := fmt.Sprintf("%s%06d\n", ManifestPrefix, num)
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("kvdb/version: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("kvdb/version: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("kvdb/version: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, CurrentName)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("kvdb/version: install %s: %w", CurrentName, err)
	}
	return nil
}

// Recover 读取 CURRENT 指向的 Manifest 并重放，重建当前版本。
//
// 返回 hasManifest 为 false 表示目录里没有版本元数据（全新目录，或 M2 及更早
// 的目录），调用方应当扫描目录后用 SetFromScan 建立初始版本。
// truncated 为 true 表示 Manifest 尾部有一截被丢弃的记录 —— 那是崩溃时
// 没写完的一次追加，它的调用方没有拿到成功返回，因此丢弃它是安全的。
func (vs *VersionSet) Recover() (hasManifest, truncated bool, err error) {
	num, err := readCurrent(vs.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, false, nil
		}
		return false, false, err
	}
	vs.mu.Lock()
	defer vs.mu.Unlock()
	truncated, err = vs.replayManifestLocked(num)
	if err != nil {
		return true, truncated, err
	}
	vs.manifestNum = num
	return true, truncated, nil
}

// replayManifestLocked 重放一份 Manifest，把结果装成当前版本。
func (vs *VersionSet) replayManifestLocked(num uint64) (truncated bool, err error) {
	path := ManifestName(vs.dir, num)
	f, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("kvdb/version: open the manifest that CURRENT points to (%s): %w", path, err)
	}
	defer f.Close()

	levels := make([][]*FileMeta, vs.cfg.MaxLevels)
	recordedName := ""
	r := wal.NewReader(f)
	for {
		record, rerr := r.ReadRecord()
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			var corrupt *wal.ErrCorruptRecord
			if errors.As(rerr, &corrupt) {
				// 尾部的半截记录：一次没写完的追加。见 Recover 的说明。
				truncated = true
				break
			}
			return truncated, fmt.Errorf("kvdb/version: read manifest %s: %w", path, rerr)
		}
		e, derr := DecodeVersionEdit(record)
		if derr != nil {
			// 记录本身通过了 CRC 却解析不了，说明编码有两套实现，属于程序缺陷。
			return truncated, fmt.Errorf("kvdb/version: decode manifest %s: %w", path, derr)
		}
		if e.ComparatorName != "" {
			if recordedName == "" {
				recordedName = e.ComparatorName
			} else if recordedName != e.ComparatorName {
				return truncated, fmt.Errorf("kvdb/version: manifest %s mixes comparers %q and %q", path, recordedName, e.ComparatorName)
			}
		}
		vs.applyCountersLocked(e)
		for _, d := range e.Deleted {
			if d.Level < 0 || d.Level >= len(levels) {
				return truncated, fmt.Errorf("kvdb/version: manifest %s deletes file %d from invalid level %d", path, d.Num, d.Level)
			}
			levels[d.Level] = removeFile(levels[d.Level], d.Num)
		}
		for _, a := range e.Added {
			if a.Level < 0 || a.Level >= len(levels) {
				return truncated, fmt.Errorf("kvdb/version: manifest %s adds file %d to invalid level %d", path, a.Num, a.Level)
			}
			levels[a.Level] = insertFile(levels[a.Level], fileFromEdit(a.Level, a), vs.icmp, a.Level == 0)
		}
	}

	if recordedName != "" && recordedName != vs.comparerName {
		return truncated, fmt.Errorf(
			"kvdb/version: data directory was written with comparer %q, but the option specifies %q",
			recordedName, vs.comparerName)
	}

	nv := &Version{vset: vs, levels: levels}
	nv.Ref()
	old := vs.current
	vs.live = append(vs.live, nv)
	vs.current = nv
	old.unrefLocked() // 调用方（Recover）持着 vs.mu，不能走会加锁的 Unref
	return truncated, nil
}

// NewManifest 把当前版本完整写成一份新 Manifest，并原子切换 CURRENT。
//
// 每次打开数据库都会做一次：把"历史上积累的一长串增量编辑"收敛成一份快照。
// 不做的话，Manifest 会随使用年限无限增长（打开时要重放全部历史），
// 而这里重写一次的代价只有"当前文件数 × 每条几十字节"。
//
// 崩溃安全：新 Manifest 写完并 fsync 之后才 rename CURRENT。中途崩溃时
// CURRENT 仍然指向旧 Manifest，旧的那份内容此刻依然完全有效。
func (vs *VersionSet) NewManifest() error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.closed {
		return errors.New("kvdb/version: version set is closed")
	}
	// 先占用编号、再取快照：这样快照里记下的 NextFileNum 已经把这份 Manifest
	// 自己算进去了。反过来（先取快照再占号）会让恢复后的 nextFileNum 恰好等于
	// 这个 Manifest 的编号，下一次分配就会与它重号。
	num := vs.nextFileNum
	vs.nextFileNum++
	edit := vs.snapshotEditLocked()

	m, err := createManifest(vs.dir, num)
	if err != nil {
		return err
	}
	abort := func(err error) error {
		m.Close()
		os.Remove(m.path)
		return err
	}

	if err := m.Append(edit.Encode()); err != nil {
		return abort(err)
	}
	if err := m.Sync(); err != nil {
		return abort(err)
	}
	if err := writeCurrent(vs.dir, num); err != nil {
		return abort(err)
	}

	// 换完 CURRENT，旧的 Manifest 就没人引用了，可以删。
	//
	// **不能只看 vs.manifest 这个句柄**：从磁盘恢复时我们只是把旧 Manifest 读了一遍
	// 就关掉了，句柄是 nil，但文件确实还在目录里。只判句柄会让每一轮"打开—重写"
	// 都在目录里留下一个 MANIFEST-xxxxxx，打开几百次之后目录里就躺着几百份
	// 已经作废的元数据。所以这里按**编号**删。
	oldNum, old := vs.manifestNum, vs.manifest
	vs.manifest = m
	vs.manifestNum = num
	if old != nil {
		_ = old.Close()
	}
	if oldNum != 0 && oldNum != num {
		_ = os.Remove(ManifestName(vs.dir, oldNum))
	}
	return nil
}

// snapshotEditLocked 构造一份"把当前版本完整重建出来"的 VersionEdit。
//
// 调用方必须持有 vs.mu（NewManifest 正在改 vs.nextFileNum，SnapshotEdit 只想读，
// 两者的差别就在这把锁上，所以实现共用一个内部函数）。
func (vs *VersionSet) snapshotEditLocked() *VersionEdit {
	edit := &VersionEdit{
		ComparatorName: vs.comparerName,
		NextFileNum:    vs.nextFileNum,
		LastSeq:        vs.lastSeq,
		LogNumber:      vs.logNumber,
	}
	for level, files := range vs.current.levels {
		for _, fm := range files {
			edit.Added = append(edit.Added, fm.Edit(level))
		}
	}
	return edit
}

// SnapshotEdit 返回一份"把当前版本完整重建出来"的 VersionEdit。
//
// 它与 NewManifest 写进本目录的内容是同一份东西，区别是这里**不碰磁盘**。
// 需要它是为了 Checkpoint：副本要写到**另一个目录**去，而 NewManifest 只会
// 在 vs.dir 下操作。拿到这个 edit 之后交给 WriteManifest 即可。
//
// 返回的 edit 里 LastSeq 只是"最后一次提交时记下的值"，可能落后于当前的
// 已提交水位（提交之外的写入只在 WAL 里）。调用方若需要精确水位，应当自行
// 取 max 之后覆盖 —— 见 DB.Checkpoint。
func (vs *VersionSet) SnapshotEdit() *VersionEdit {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.snapshotEditLocked()
}

// WriteManifest 在 dir 下新建编号为 num 的 Manifest、写入 e，并把 CURRENT 指向它。
//
// 它与 NewManifest 的执行顺序完全一致，那条顺序不是风格问题：
// **先写完并 fsync Manifest，最后才 rename CURRENT**。反过来的话，
// 任何一次崩溃都会留下"CURRENT 指向一份残缺 Manifest"的状态，
// 而这个状态在恢复时是无法与"数据真的丢了"区分开的。
func WriteManifest(dir string, num uint64, e *VersionEdit) error {
	m, err := createManifest(dir, num)
	if err != nil {
		return err
	}
	abort := func(err error) error {
		m.Close()
		os.Remove(m.path)
		return err
	}
	if err := m.Append(e.Encode()); err != nil {
		return abort(err)
	}
	if err := m.Sync(); err != nil {
		return abort(err)
	}
	if err := writeCurrent(dir, num); err != nil {
		return abort(err)
	}
	return m.Close()
}
