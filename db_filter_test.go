package kvdb

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xiatianliang1024gm/kvdb/internal/compact"
)

// 这个文件是 M6（docs/EXTENSIONS.md §4.2，CompactionFilter）的验收测试。
//
// 覆盖设计稿列出的五条验收，外加 FilterOnFlush 与"老目录首次配过滤器"两条：
//
//	1. 命中过滤条件的记录在 Compaction 之后 Get 不到，其余不受影响；
//	2. 更深层还有旧版本时不得复活（核心正确性：Drop 必须转成墓碑语义）；
//	3. 存活快照期间过滤器不生效；Release 之后的下一轮 Compaction 才清除；
//	4. filter 报错 ⇒ Compaction 失败、库停机，且数据无损（重开可读）；
//	5. 换一个 Name() 打开同一目录 ⇒ 报配置不匹配；
//	6. FilterOnFlush 打开时 Flush 侧同样清除；
//	7. M6 之前的老目录（Manifest 没记过过滤器名）允许首次配置过滤器。
//
// 快照测试的数据布局说明：过滤器只对"seq <= 最小存活快照"的记录生效，
// 所以"存活快照期间不生效"必须用"先取快照、后写数据"来构造；而 Release
// 之后那批记录已经落进 SST，只能靠下一轮 Compaction 重新扫到它们 ——
// 靠"偶数 key 写目标数据、奇数 key 写 filler"的区间交错，保证后续的
// L0→L1 归并总把含目标记录的 L1 文件一起卷进来。

// dropFF 是验收用的最小过滤器：丢弃 value 首字节为 0xFF 的所有记录。
// errOn 非空时，遇到那个 key 直接返回 error，用于验收"过滤器故障停库"。
type dropFF struct {
	name  string
	errOn string
}

func (f *dropFF) Name() string {
	if f.name == "" {
		return "kvdb.test.dropFF"
	}
	return f.name
}

func (f *dropFF) Filter(level int, userKey, value []byte, seq uint64) (Decision, error) {
	if f.errOn != "" && string(userKey) == f.errOn {
		return Keep, fmt.Errorf("inject failure on %q", userKey)
	}
	if len(value) > 0 && value[0] == 0xFF {
		return Drop, nil
	}
	return Keep, nil
}

// ffValue 构造一个会被 dropFF 命中的 value。
func ffValue(s string) []byte { return append([]byte{0xFF}, s...) }

// aKey 生成测试 2/3 用的交错 key：偶数下标放目标数据，奇数下标放 filler，
// 两者的 key 区间彼此咬合，后续 Compaction 必然把它们卷进同一次归并。
func aKey(i int) []byte { return []byte(fmt.Sprintf("a%06d", i)) }

// loadOdd 向 aKey 的奇数下标写 filler，from/to 是奇数下标的序号。
func loadOdd(t *testing.T, db *DB, from, to int, value string) {
	t.Helper()
	for i := from; i < to; i++ {
		mustPut(t, db, string(aKey(2*i+1)), value)
	}
}

// newFilterDB 在指定目录打开一个测试库，同时返回配置（重开时要用同一份）。
func newFilterDB(t *testing.T, dir string, mutate func(o *Options)) (*DB, Options) {
	t.Helper()
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	if mutate != nil {
		mutate(&opts)
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return db, opts
}

// waitFilterStopped 轮询直到后台故障把写路径停下来（Put 返回 bgErr）。
func waitFilterStopped(t *testing.T, db *DB) error {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		err := db.Put([]byte("probe"), []byte("probe"))
		if err != nil {
			return err
		}
		if time.Now().After(deadline) {
			t.Fatal("等待过滤器故障停库超时：Put 一直没有报错")
		}
		time.Sleep(3 * time.Millisecond)
	}
}

// ── 验收 1：命中过滤条件的记录被物理清除，其余不受影响 ──────────────

func TestCompactionFilterDropsMatchingRecords(t *testing.T) {
	dir := t.TempDir()
	db, _ := newFilterDB(t, dir, func(o *Options) {
		o.MemTableSize = 16 << 10 // 逼出多次 Flush
		o.LevelBaseSize = 64 << 20
		o.CompactionFilter = &dropFF{}
	})
	// 数据量要明显超过 MemTableSize：过滤器只在 Flush/Compaction 生效，
	// 记录还躺在 MemTable 里时 Get 到原值是正确行为。
	for i := 0; i < 1200; i++ {
		if i%3 == 0 {
			mustPut(t, db, string(benchKey(i)), string(ffValue(strings.Repeat("x", 60))))
		} else {
			mustPut(t, db, string(benchKey(i)), strings.Repeat("s", 60))
		}
	}
	// 再灌一段 filler 把含目标 key 的最后一个活跃 MemTable 也推出去：
	// 过滤器只作用于已落盘的记录，活跃表里的数据能读到是正确行为。
	for i := 0; i < 500; i++ {
		mustPut(t, db, fmt.Sprintf("zz%05d", i), strings.Repeat("z", 60))
	}
	waitConverged(t, db)
	checkLevelInvariant(t, db)
	for i := 0; i < 1200; i++ {
		if i%3 == 0 {
			if v, err := db.Get(benchKey(i)); err != ErrNotFound {
				t.Fatalf("Get(%s) = %q, %v；被过滤的 key 应当读不到", benchKey(i), v, err)
			}
		} else if v, err := db.Get(benchKey(i)); err != nil || string(v) != strings.Repeat("s", 60) {
			t.Fatalf("Get(%s) = %q, %v；未命中的 key 不应受影响", benchKey(i), v, err)
		}
	}
	// 重启：墓碑与 Manifest 里的过滤器名都要经得起恢复路径。
	db.Close()
	db2, _ := newFilterDB(t, dir, func(o *Options) {
		o.MemTableSize = 16 << 10
		o.LevelBaseSize = 64 << 20
		o.CompactionFilter = &dropFF{}
	})
	defer db2.Close()
	if _, err := db2.Get(benchKey(0)); err != ErrNotFound {
		t.Fatalf("重启后 Get(被过滤的 key) = %v，应当仍是 ErrNotFound", err)
	}
	if v, err := db2.Get(benchKey(1)); err != nil || string(v) != strings.Repeat("s", 60) {
		t.Fatalf("重启后 Get = %q, %v", v, err)
	}
}

// ── 验收 2：更深层还有旧版本时不得复活（核心正确性） ────────────────

func TestCompactionFilterDoesNotResurrectOldVersions(t *testing.T) {
	db, _ := newFilterDB(t, t.TempDir(), func(o *Options) {
		o.MemTableSize = 8 << 10
		o.LevelBaseSize = 8 << 10 // L1 极小，把数据快速挤向深层
		o.CompactionFilter = &dropFF{}
	})
	defer db.Close()

	// "deep" 的旧版本第一批落盘，随后被 filler 一路挤到深层
	//（"deep" 按字节序排在所有 a0000/数据 key 之前，自成一个文件区间）。
	mustPut(t, db, "deep", "old-version")
	load(t, db, 0, 4000, strings.Repeat("f", 100))
	waitConverged(t, db)
	if s := db.Stats(); s.Compaction.Count == 0 {
		t.Fatal("前置条件不成立：应当已经发生过 Compaction")
	}

	// 新版本命中过滤器。此刻更深层还躺着 "old-version"。
	mustPut(t, db, "deep", string(ffValue("new-version")))
	load(t, db, 4000, 8000, strings.Repeat("f", 100)) // 再压一轮，逼含新版本的 L0 落下去
	waitConverged(t, db)

	// 正确行为：Drop 转成墓碑，old-version 依然被遮蔽，读到一个干干净净的"不存在"。
	// 错误行为（直接跳过不写）：这里会读到 "old-version"。
	if v, err := db.Get([]byte("deep")); err != ErrNotFound {
		t.Fatalf("Get(deep) = %q, %v；旧版本复活了", v, err)
	}
}

// ── 验收 3：存活快照期间过滤器不生效，Release 之后再清除 ────────────

func TestCompactionFilterWaitsForLiveSnapshot(t *testing.T) {
	db, _ := newFilterDB(t, t.TempDir(), func(o *Options) {
		o.MemTableSize = 8 << 10
		o.LevelBaseSize = 64 << 20 // 不让数据自己往深层走，留给测试自己控制
		o.CompactionFilter = &dropFF{}
	})
	defer db.Close()

	sn := db.GetSnapshot() // 先取快照：之后写入的数据 seq 全部大于快照
	defer sn.Release()

	// 偶数 key 写目标数据（会被过滤），奇数 key 写 filler（不会）。
	for i := 0; i < 400; i++ {
		mustPut(t, db, string(aKey(2*i)), string(ffValue("expiring")))
	}
	loadOdd(t, db, 0, 600, strings.Repeat("o", 100))
	waitConverged(t, db)

	// 快照还活着：这批记录的 seq > 快照，是"现在"，过滤器不得替用户做决定。
	for i := 0; i < 400; i += 40 {
		v, err := db.Get(aKey(2 * i))
		if err != nil || string(v) != string(ffValue("expiring")) {
			t.Fatalf("存活快照期间 Get(%s) = %q, %v；过滤器不应生效", aKey(2*i), v, err)
		}
	}
	// 快照本身也读不到这批数据（seq 大于快照点），确认快照语义没有被搅动。
	if _, err := sn.Get(aKey(0)); err != ErrNotFound {
		t.Fatalf("快照不应看到后写的数据：Get = %v", err)
	}

	sn.Release()
	// 重写同一批奇数 key：新 L0 文件与含目标记录的 L1 文件区间咬合，
	// 下一轮 L0→L1 归并会把它们一起卷进去重新判定 —— 此时已无存活快照，
	// 过滤器应当生效。
	loadOdd(t, db, 0, 600, strings.Repeat("p", 100))
	waitConverged(t, db)
	for i := 0; i < 400; i += 40 {
		if _, err := db.Get(aKey(2 * i)); err != ErrNotFound {
			t.Fatalf("Release 之后 Get(%s) = %v；过滤器应当已生效", aKey(2*i), err)
		}
		if v, err := db.Get(aKey(2*i + 1)); err != nil || len(v) != 100 {
			t.Fatalf("Release 之后 Get(奇数 key) = %q, %v；filler 不应被清除", v, err)
		}
	}
}

// ── 验收 4：filter 报错 ⇒ Compaction 失败、库停机，数据无损 ─────────

func TestCompactionFilterErrorStopsDB(t *testing.T) {
	dir := t.TempDir()
	db, _ := newFilterDB(t, dir, func(o *Options) {
		o.MemTableSize = 8 << 10
		o.CompactionFilter = &dropFF{errOn: "boom"}
	})
	mustPut(t, db, "boom", string(ffValue("x")))
	// 不用 load/mustPut：注入的故障随时会让 Put 报错，那是验收目标而不是失败。
	for i := 0; i < 1500; i++ {
		if err := db.Put(benchKey(i), []byte(strings.Repeat("f", 100))); err != nil {
			break
		}
	}

	err := waitFilterStopped(t, db)
	if err == nil || !strings.Contains(err.Error(), "compaction filter") {
		t.Fatalf("Put 应当透传过滤器故障，得到 %v", err)
	}
	db.Close()

	// 重开：Compaction 已经整体作废（输出被 abort 清掉），数据应当无损。
	db2, _ := newFilterDB(t, dir, func(o *Options) {
		o.MemTableSize = 8 << 10
		o.CompactionFilter = &dropFF{}
	})
	defer db2.Close()
	if v, gerr := db2.Get([]byte("boom")); gerr != nil || string(v) != string(ffValue("x")) {
		t.Fatalf("重开后 Get(boom) = %q, %v；停库前的数据必须无损", v, gerr)
	}
	if v, gerr := db2.Get(benchKey(100)); gerr != nil || len(v) != 100 {
		t.Fatalf("重开后 Get = %q, %v", v, gerr)
	}
}

// ── 验收 5：换 Name() 打开同一目录 ⇒ 配置不匹配 ─────────────────────

func TestCompactionFilterNameMismatch(t *testing.T) {
	dir := t.TempDir()
	mutA := func(o *Options) { o.CompactionFilter = &dropFF{name: "filterA"} }
	dbA, _ := newFilterDB(t, dir, mutA)
	mustPut(t, dbA, "k", "v")
	dbA.Close()

	// 换一个名字 → 拒绝。
	if _, err := Open(withFilterOptions(dir, "filterB")); err == nil {
		t.Fatal("换名字打开同一目录应当报配置不匹配")
	} else if !strings.Contains(err.Error(), "compaction filter") {
		t.Fatalf("错误信息应当提到 compaction filter，得到：%v", err)
	}
	// 去掉过滤器 → 同样拒绝：名字记进 Manifest 就冻结了。
	if _, err := Open(DefaultOptions(dir)); err == nil {
		t.Fatal("已记录过滤器名的目录不允许在无过滤器下打开")
	}
	// 原名 → 正常。
	dbSame, _ := newFilterDB(t, dir, mutA)
	dbSame.Close()
}

// withFilterOptions 构造一个只改过滤器名的配置（用于故意制造不匹配）。
func withFilterOptions(dir, name string) Options {
	opts := DefaultOptions(dir)
	opts.SyncWrites = false
	opts.CompactionFilter = &dropFF{name: name}
	return opts
}

// ── 验收 6：FilterOnFlush 打开时 Flush 侧同样清除 ───────────────────

func TestFilterOnFlush(t *testing.T) {
	db, _ := newFilterDB(t, t.TempDir(), func(o *Options) {
		o.MemTableSize = 8 << 10
		o.FilterOnFlush = true
		o.CompactionFilter = &dropFF{}
	})
	defer db.Close()

	const keys = 100
	for i := 0; i < keys; i++ {
		mustPut(t, db, fmt.Sprintf("ff%04d", i), string(ffValue("gone")))
		mustPut(t, db, fmt.Sprintf("ok%04d", i), "stay")
	}
	// 灌过 MemTableSize 的 filler，确保含目标 key 的表已被冻结并落盘。
	load(t, db, 0, 600, strings.Repeat("h", 80))
	s := waitConverged(t, db)
	if s.FlushBytes == 0 {
		t.Fatal("前置条件不成立：应当已经发生过 Flush")
	}
	for i := 0; i < keys; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("ff%04d", i))); err != ErrNotFound {
			t.Fatalf("Get(ff%04d) = %v；Flush 过滤后应当读不到", i, err)
		}
		if v, err := db.Get([]byte(fmt.Sprintf("ok%04d", i))); err != nil || string(v) != "stay" {
			t.Fatalf("Get(ok%04d) = %q, %v", i, v, err)
		}
	}
}

// ── 验收 7：老目录（没记过过滤器名）允许首次配置过滤器 ──────────────

func TestCompactionFilterCanBeIntroducedOnOldDirectory(t *testing.T) {
	dir := t.TempDir()
	dbPlain, _ := newFilterDB(t, dir, nil)
	mustPut(t, dbPlain, "k", "v")
	dbPlain.Close()

	dbFilter, _ := newFilterDB(t, dir, func(o *Options) {
		o.CompactionFilter = &dropFF{}
	})
	defer dbFilter.Close()
	if v, err := dbFilter.Get([]byte("k")); err != nil || string(v) != "v" {
		t.Fatalf("老目录首次配过滤器后 Get = %q, %v", v, err)
	}
}

// 编译期钉住 compact 包与根包的别名确实是同一个类型。
var _ compact.Filter = CompactionFilter(&dropFF{})
