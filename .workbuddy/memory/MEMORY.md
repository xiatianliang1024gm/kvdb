# kvdb 项目长期笔记

## 项目定位
Go 实现的单机嵌入式 KV 存储引擎，架构参考 RocksDB / LevelDB 的 LSM-Tree。
设计文档：`docs/DESIGN.md`（唯一权威来源，进度、决策、路线都在里面）。
边界：不做分布式/Raft、事务隔离级别、二级索引、加密、多进程并发访问同一目录。

## 包结构约定
按 DESIGN.md 附录 C：根包放 Options/Comparer + db.go / db_flush.go / batch.go / file_lock_*.go；
internal/ 下按模块切分 key / memdb / wal / sst / filter / cache / version / compact / iterator。
方括号里标注的是目标里程碑，新增目录前先看附录 C 是否已规划。

## 阶段进度
- M0 骨架：**已完成**（2026-09-18）
- M1 最小可用：**已完成**（2026-09-18）
- M2 读优化：**已完成**（2026-09-18）
- M3 写优化：**已完成**（2026-09-18）
- M4 一致性：**已完成**（2026-09-18）
- M5 生产化：未开始

## git
已 init。基线 `6a375c1`（M0+M1+M2），`683e9c3`（M3），`1de8952`（M4）。
中文 commit message 用 Write 写进 `_commitmsg.log` 再 `git commit -F`（`/_*.log` 已被忽略）。
Bash 工具能跑 git 本身，但**别接管道**（coreutils 缺失）。

## M4 确立的实现约定
- 写队列用**自己的一把锁 `wmu`**，不复用 `db.mu`：`db.mu` 保护全局状态，
  队列解决"谁来做这次 fsync"。分开才能做到 **fsync 在锁外做**（读者不被写者的磁盘等待堵住）
- 锁序：`wmu`（且不重叠持有）→ `db.mu`。`Close` 的顺序是 **drainWrites → 再拿 db.mu**
- **队长必须"排空队列才让位"**，不能"提交一组就让位"（后者会丢失唤醒：
  让位瞬间队列里刚排进来的写者永远等不到 done。症状 = 一堆 goroutine 卡在 wcond.Wait 而场上没队长）
  ⇒ 不变式 **`len(wqueue) > 0 ⟹ wleader`**
- `finishWriteGroup` **先写 done 再 Broadcast**（反了会丢失唤醒且不可恢复）
- `drainWrites` **先关门（wclosing=true）再等队长**：反过来在持续写入下队列永远不空，Close 永远等
- 组提交三步：`beginWriteGroup`（分配序列号，**不动 db.lastSeq**）→
  `appendWriteGroup`（追加 + **一次 fsync**，锁外）→ `applyWriteGroup`（整组在一个 db.mu 临界区里落库，
  最后才推进 `db.lastSeq`）⇒ **不存在"半组可见"**
- `collectYields = 4`：队长在 fsync 前 `runtime.Gosched()` 几次，把刚被放行的写者收进本组。
  不加的话每组退化成"1 个 + (N-1) 个"交替，合并率只有 W/2
- **失败语义**：组内任何一步出错 → 整组失败 + `bgErr` 停库（RocksDB 式）。
  一条批次是原子的，一个**提交组**不是；"报错但写成功了"是比崩溃更难查的一致性缺口
- `failLocked`（持有 db.mu 写锁）是**唯一**的停库入口，`setBgErr` 只是它的加锁包装
- **快照与迭代器是两套机制，别合并**：快照靠**登记**（护旧版本，抬高 `smallestSnapshot`）；
  迭代器靠**版本引用**（护输入文件）。给迭代器登记快照会把丢弃上界永久钉死
- `Stats` 的组提交指标：`WriteGroups` / `WriteBatches` / `MaxWriteGroup` / `WALFsyncs`；
  合并率 = `WriteBatches/WriteGroups`，fsync 摊销 = `WriteBatches/WALFsyncs`
- 实测合并率：32 写者 → 22~28（us/fsync 恒定 2.0~2.2ms，是这台机器的固定物理量）


## M3 确立的实现约定
- `internal/version` 管版本：Manifest（复用 WAL 记录格式的追加日志）+ `CURRENT` + `VersionSet`；
  `Version` 不可变、靠引用计数存活。`internal/compact` 管合并：`Pick` + `Run`。
- **`VersionSet.mu` 内一律用 `unrefLocked`，绝不用 `Unref`**（后者归零时重入加锁 → 自死锁）
- Compaction **每次只往下走一层**；输出**只在 user key 变化处切分**（同 key 版本必须同文件）；
  `Inputs[1]`（输出层重叠文件）必须一起归并，否则"L1 以下同层不重叠"破掉
- `Commit` 三步顺序固定：锁外开 reader → 锁内先登记 reader → 最后落 Manifest（否则有"版本已指向但读不到"的窗口）
- 丢弃旧版本的上界 = `smallestSnapshot`（存活快照最小值，无快照时取 lastSeq）；迭代器打开即登记快照
- 孤儿文件分两类：`DiscardedFiles`（残缺、根本不该存在）vs `ObsoleteFiles`（完整、提交没成功），判据"能否正常打开"
- `LastSeq` 每次提交都写进 Manifest（只靠重放 WAL 会在"WAL 为空时崩溃"退回 0 并撞号）
- GC 顺序：先 `os.Remove` 成功、再删 `db.readers` 条目
- `Close` 里 `bgWG.Wait()` 之后要调 `collectGarbageFinal()` 兜底（去掉 closed 检查）
- `options.go` 的 `L0CompactionTrigger`(4) / `LevelBaseSize`(256MB) / `LevelSizeMultiplier`(10) / `MaxLevels`(7) 已接入

## 已确立的实现约定
- 二进制编码（trailer、fixed32/64、WAL 记录头、WriteBatch 头）一律大端
- internal key = `user_key + 8 字节 (seq<<8|kind)`；排序为 user_key 升序 + 尾缀降序（新版本在前）
- 序列号上限 2^56-1；越界 panic 而非静默截断
- `SeekKey(userKey, snapshot)` 的尾缀用 `(snapshot, TypeValue)`，落点即该快照下最新可见版本
- Options 数值字段：0 = 用默认值；BlockCacheSize / BloomBitsPerKey 用负数表示显式关闭
- Comparer 是接口（带 Name() 以便写 Manifest），不是函数字段；`key.InternalComparer` 是唯一实现（根包留别名）
- 写入口统一收敛到 `Write`，Get/Put/Delete 都是 WriteBatch 的语法糖
- 第三方库只用在工具层（xxhash/snappy/compress），存储引擎主体自己写
- WAL：32KB 分块 + 7 字节头（fixed32 CRC32C 掩码 | fixed16 长度 | 类型），跨块用 FIRST/MIDDLE/LAST 拆
- SST（M1 最小版）：`uvarint(ikLen)|internalKey|uvarint(valLen)|value` 顺序排列，末尾 16 字节 footer = `fixed64(dataLen)|magic "kvdb0001"`；M2 换成五段式
- 文件编号在 SST 与日志之间共享：**编号大 = 更新**，L0 的"从新到旧"查找顺序依赖这一点
- 恢复：先落盘重放结果再删旧日志；删日志判据 = 编号 < min(mem.logNum, imm.logNum)；footer 不全的 SST 直接删（数据还在 WAL）
- 目录锁用 OS 级锁（Windows LockFileEx / Unix flock），**不要**改成 O_EXCL 锁文件，否则 kill -9 后目录永久打不开
- 崩溃测试用子进程 + `os.Exit(1)` 模拟（见 db_crash_test.go），同进程放弃 Close 测不出锁释放

## 本机环境坑（重要）
- **PowerShell 工具不回显 stdout**：所有需要看输出的命令都要 `Set-Content/Add-Content` 写文件，再用 Read 读文件
- 跑 go 命令统一用 `scripts/gotest.ps1`：`pwsh -NoProfile -File scripts\gotest.ps1 test ./... -cover`，结果落在 `_t.log`
  （**必须用 `pwsh`（PS7），不要用 `powershell`（5.1）**；脚本内已加守卫，被 5.1 启动会自动切到 7 重跑）
- 该脚本会设 `[Console]::OutputEncoding = UTF8`，否则 go 的中文输出经 PowerShell 管道会变乱码
- **Bash 工具只有 bash 内置命令可用**：`ls/grep/sed/which/uname/dirname/head/cat/ls` 等 coreutils 全部缺失（因为 PortableGit 的 usr/bin 不在 PATH 里）；
  `rm` 更糟——它是安全删除 shim，会让 `dirname` 缺失而直接报 `safe_delete_main: command not found`。
  但 `echo/cd/export/重定向/while read` 正常，**且 `export PATH=...` 能真正传给子进程** —— 这点比 PowerShell 工具强，见下条。
  结论：Bash 工具只用来跑 go 命令；查目录、删文件、看状态一律走 PowerShell
- **PowerShell：一律用 PowerShell 7（`pwsh`）跑脚本**。本机 PS7 = `C:\Program Files\PowerShell\7\pwsh.exe`（7.6.6 Core），
  5.1 = `C:\WINDOWS\System32\WindowsPowerShell\v1.0\powershell.exe`（Desktop）。
  WorkBuddy 的 PowerShell 工具**本身就是 pwsh 7**，所以坑只出在嵌套调用 `powershell -File ...` 时。
  5.1 会按 GBK 解码无 BOM 的 UTF-8，中文注释吞换行 → 整行代码静默失效或报 ParserError（详见下一条）
- **`$env:PATH` 在 PowerShell 工具里其实是能传给子孙进程的**（之前记的"传不下去"是错的，已实测推翻）。
  但即便如此，**cgo / -race 在 PowerShell 通道里仍然跑不通**：`CC=gcc` + PATH 时编译能过、卡在
  `link.exe: running gcc failed: exec: "gcc": executable file not found in %PATH%`（4/4 稳定复现）；
  `CC` 给绝对路径（正斜杠或反斜杠）则在 cgo 阶段报 `cgo.exe: exit status 2`。
  结论：**cgo / -race 只走 Bash 通道**，别在 PowerShell 里折腾
- **本机 MSYS2 已装（2026-09-18）**：`C:\msys64`，gcc 16.1.0 位于 `C:\msys64\ucrt64\bin\gcc.exe`（只有 UCRT64 环境装了 toolchain，mingw64/mingw32 无 gcc）。
  已把 `C:\msys64\ucrt64\bin` 追加进**用户级 PATH**（HKCU:\Environment，保留 ExpandString 类型；改前原值备份在 `_path_backup.log`）。
  注意：这条只对用户自己新开的终端生效，WorkBuddy 的 PowerShell 工具因为会重写子进程 PATH 仍然吃不到
- **跑 -race / cgo 的固定入口是 `scripts/gotest-race.sh`**（Bash 工具里必须 `source` 调用）：
  ```bash
  cd /c/Users/summer/repos/kvdb && source scripts/gotest-race.sh        # = go test -race ./...
  cd /c/Users/summer/repos/kvdb && source scripts/gotest-race.sh ./internal/wal/...   # 只测某包
  ```
  输出同时打到屏幕和 `_race.log`；脚本自己会 export PATH/CGO_ENABLED/CC。
  实测通过：`ok kvdb 11.0s` + key/memdb/sst/wal 全 ok，**无 data race**（此前记录的"最大未覆盖风险"已消除）
- **不要在 Bash 工具里嵌套 `bash script.sh`**：沙箱会去探测 wsl.exe 并命中程序黑名单直接拦下，
  所以 WorkBuddy 里一律用 `source`（脚本已处理：被 source 时 `return` 而非 `exit`，不会关掉调用方 shell）
- **.ps1 文件带 UTF-8 BOM 更稳**：`scripts/gotest.ps1` 曾是无 BOM 的 UTF-8，Windows PowerShell 5.1 按 GBK 解码后
  中文注释吃字节，直接报 `表达式或语句中包含意外的标记"}"`（ParserError，且行号和真实内容对不上）。
  已用 `[System.IO.File]::WriteAllText($p, $text, (New-Object System.Text.UTF8Encoding $true))` 补上 BOM。
  **Write 工具创建的文件不带 BOM，Edit 工具会保留已有 BOM** —— 用 Write 重建含中文的 .ps1 后要检查一次。
  根治办法是用 pwsh 7 跑（见上），BOM 只是双保险。
  上述整套环境知识已固化为用户级 skill `~/.workbuddy/skills/win-go-cgo-race/`（含通用版脚本），跨项目可用
- Go 1.27.1（`C:\Program Files\Go\bin\go.exe`）；删除文件受 safe-delete 沙箱限制，偶尔拒删，可直接把临时文件加进 .gitignore。
  仓库已 init（见上"## git"）。
