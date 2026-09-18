package kvdb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"kvdb/internal/sst"
	"kvdb/internal/version"
	"kvdb/internal/wal"
)

// ErrCheckpointTargetNotEmpty 表示 Checkpoint 的目标目录非空。
//
// Checkpoint 往目录里写什么完全由它自己决定，别人留下的文件只会造成歧义
// （半份旧副本？用户的文档？），所以拒绝而不是合并。
var ErrCheckpointTargetNotEmpty = errors.New("kvdb: checkpoint target directory is not empty")

// Checkpoint 在 dir 下生成一份可独立打开的完整副本。
//
// 与"备份 = 把目录复制走"的区别在于一致性：数据目录随时在被后台协程改写
// （MemTable 落盘、Compaction 换文件、Manifest 追加），直接复制目录
// 会得到一个新旧文件互相矛盾的拼盘。Checkpoint 的做法是：
//
//  1. 持 db.mu 读锁捕获一致快照 —— 版本（哪些 SST 参与读取）、快照 Manifest
//     所需的编辑项、以及"哪些 WAL 日志还承载着没落盘的数据"；
//  2. 锁外把参与读取的 SST 链接/复制过去（SST 不可变，之后怎么追加都不影响它）；
//  3. 复制这些 WAL 日志。正在被写入的当前日志可能比捕获时刻多出几条已提交的
//     记录、甚至尾部有一截没写完 —— 前者无害（多出来的也是已提交数据），
//     后者由副本恢复时的"截断损坏尾部"路径处理；
//  4. 用 WriteManifest 把快照 Manifest + CURRENT 写进副本。
//
// 之后对副本目录 Open 即可独立使用；源库不受任何影响，可以继续读写。
//
// SST 优先用硬链接（同一文件系统上零拷贝），失败自动退回复制 —— Compaction
// 很快会让源目录里的旧文件被删除，但副本版本握着引用（inode 仍存活），
// Windows 与 POSIX 的语义在这点上是一致的。
func (db *DB) Checkpoint(dir string) error {
	if dir == "" {
		return errors.New("kvdb: checkpoint target directory must not be empty")
	}
	// 与源目录相同（或互为符号链接）时拒绝：往自己目录里写副本是一定会出事的。
	srcAbs, err := filepath.Abs(db.opts.Dir)
	if err != nil {
		return err
	}
	dstAbs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if filepath.Clean(srcAbs) == filepath.Clean(dstAbs) {
		return errors.New("kvdb: checkpoint target must differ from the database directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("kvdb: create checkpoint directory %s: %w", dir, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("kvdb: read checkpoint directory %s: %w", dir, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("%w: %s", ErrCheckpointTargetNotEmpty, dir)
	}

	// ── 第 1 步：持读锁捕获一致状态 ──────────────────────────────
	//
	// SnapshotEdit 必须在同一个临界区里取：放出去之后再取，可能拿到的是
	// "并发 Flush / Compaction 提交之后"的版本，与上面捕获的文件集合对不上，
	// 副本的 Manifest 就会引用根本没复制过去的文件。
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	if db.bgErr != nil {
		err := db.bgErr
		db.mu.RUnlock()
		return fmt.Errorf("kvdb: database has a background error: %w", err)
	}
	v := db.v
	v.Ref()
	// SyncWrites=false 时记录还躺在 WAL 的 bufio 缓冲里，文件上看不到；
	// 先刷给内核，复制出去的内容才是完整的。Flush 只推缓冲不做 fsync：
	// 副本只要"读得到"即可，持久性仍由源库的写路径负责。
	if db.log != nil {
		if err := db.log.Flush(); err != nil {
			db.mu.RUnlock()
			v.Unref()
			return fmt.Errorf("kvdb: flush WAL before checkpoint: %w", err)
		}
	}
	// WAL 尾巴的下界：编号更小的日志，其内容一定已经全部落进 SST。
	minLogNum := db.mem.LogNumber()
	if db.imm != nil && db.imm.LogNumber() < minLogNum {
		minLogNum = db.imm.LogNumber()
	}
	var liveFiles []*version.FileMeta
	for level := 0; level < v.NumLevels(); level++ {
		liveFiles = append(liveFiles, v.Files(level)...)
	}
	edit := db.vset.SnapshotEdit()
	db.mu.RUnlock()
	defer v.Unref()

	// 列出目录里现存日志，取 >= minLogNum 的那一段。
	logs, err := wal.ListLogs(db.opts.Dir)
	if err != nil {
		return err
	}
	var tailLogs []uint64
	for _, num := range logs {
		if num >= minLogNum {
			tailLogs = append(tailLogs, num)
		}
	}

	// ── 第 2 步：链接 / 复制 SST ─────────────────────────────────
	//
	// 出错时把已创建的文件逐个删掉。不碰整个目录（目标目录是调用方给的，
	// 里面可能有我们没创建的东西——虽然进来时它是空的，但那是它的事）。
	var created []string
	cleanUp := func(cause error) error {
		for _, name := range created {
			_ = os.Remove(filepath.Join(dir, name))
		}
		return fmt.Errorf("kvdb: checkpoint to %s: %w", dir, cause)
	}
	for _, f := range liveFiles {
		name := sst.FileName(f.Num)
		if err := linkOrCopyFile(filepath.Join(db.opts.Dir, name), filepath.Join(dir, name)); err != nil {
			return cleanUp(err)
		}
		created = append(created, name)
	}

	// ── 第 3 步：复制 WAL 尾巴 ───────────────────────────────────
	for _, num := range tailLogs {
		if err := copyFile(wal.LogName(db.opts.Dir, num), wal.LogName(dir, num)); err != nil {
			return cleanUp(err)
		}
		created = append(created, fmt.Sprintf("%06d%s", num, wal.LogSuffix))
	}

	// ── 第 4 步：写副本的 Manifest + CURRENT ─────────────────────
	//
	// Manifest 编号取源库的 nextFileNum（一定大于副本里所有已用的编号），
	// 而 edit.NextFileNum 再往上加一：副本自己 AllocFileNum 时从它拿号，
	// 不能回头撞上自己这份 Manifest 的编号。
	edit.LogNumber = minLogNum
	manifestNum := edit.NextFileNum
	edit.NextFileNum++
	if err := version.WriteManifest(dir, manifestNum, edit); err != nil {
		return cleanUp(err)
	}
	db.logInfof("checkpoint created at %s: files=%d logs=%d links/copied SSTs done", dir, len(liveFiles), len(tailLogs))
	return nil
}

// linkOrCopyFile 优先硬链接，不行就整文件复制。
func linkOrCopyFile(src, dst string) error {
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	return copyFile(src, dst)
}

// copyFile 复制一个普通文件并 fsync 目标。
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	return out.Close()
}
