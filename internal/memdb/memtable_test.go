package memdb

import (
	"bytes"
	"testing"

	"kvdb/internal/key"
)

func newTestMemTable() *MemTable { return New(testComparer{}, 7) }

// snapshot 可见性：写入 k 的 3 个版本后，每个快照看到的都必须是它当时的最新版本。
func TestMemTableGetRespectsSnapshot(t *testing.T) {
	m := newTestMemTable()
	m.Add(1, key.TypeValue, []byte("k"), []byte("v1"))
	m.Add(2, key.TypeValue, []byte("k"), []byte("v2"))
	m.Add(3, key.TypeValue, []byte("k"), []byte("v3"))

	// 快照 0 表示"什么都没写"，看不到任何版本。
	if _, _, found := m.Get(0, []byte("k")); found {
		t.Error("snapshot 0 不应该看到任何版本")
	}
	want := map[uint64]string{1: "v1", 2: "v2", 3: "v3", 99: "v3"}
	for snapshot, expected := range want {
		v, kind, found := m.Get(snapshot, []byte("k"))
		if !found {
			t.Errorf("snapshot %d: found = false, want true", snapshot)
			continue
		}
		if kind != key.TypeValue {
			t.Errorf("snapshot %d: kind = %v, want Value", snapshot, kind)
		}
		if string(v) != expected {
			t.Errorf("snapshot %d: value = %q, want %q", snapshot, v, expected)
		}
	}
}

// 墓碑：seq 等于快照序列号时也必须命中，否则会读到已删除的数据。
func TestMemTableGetSeesDeletion(t *testing.T) {
	m := newTestMemTable()
	m.Add(1, key.TypeValue, []byte("k"), []byte("v1"))
	m.Add(2, key.TypeDeletion, []byte("k"), nil)

	if v, kind, found := m.Get(1, []byte("k")); !found || kind != key.TypeValue || string(v) != "v1" {
		t.Fatalf("snapshot 1: (%q, %v, %v), want (v1, Value, true)", v, kind, found)
	}
	for _, snapshot := range []uint64{2, 3} {
		_, kind, found := m.Get(snapshot, []byte("k"))
		if !found {
			t.Fatalf("snapshot %d: found = false, 墓碑必须被命中", snapshot)
		}
		if kind != key.TypeDeletion {
			t.Fatalf("snapshot %d: kind = %v, want Deletion", snapshot, kind)
		}
	}

	// 删除后重新写入，新值可见、墓碑被遮蔽。
	m.Add(3, key.TypeValue, []byte("k"), []byte("v2"))
	if v, kind, found := m.Get(3, []byte("k")); !found || kind != key.TypeValue || string(v) != "v2" {
		t.Fatalf("重新写入后 snapshot 3: (%q, %v, %v), want (v2, Value, true)", v, kind, found)
	}
}

// 不存在的 key 必须报 not found，且落点跑到相邻 key 上时不能误判为命中。
func TestMemTableGetMiss(t *testing.T) {
	m := newTestMemTable()
	m.Add(1, key.TypeValue, []byte("b"), []byte("vb"))
	m.Add(2, key.TypeValue, []byte("d"), []byte("vd"))

	for _, k := range []string{"a", "c", "e", "", "bb", "zzz"} {
		if _, _, found := m.Get(10, []byte(k)); found {
			t.Errorf("Get(%q) found = true, want false", k)
		}
	}
	// 字符串前缀相近但不是同一个 key：不能因为共享前缀而误命中。
	if _, _, found := m.Get(10, []byte("d")); !found {
		t.Errorf("Get(\"d\") 应命中")
	}
}

// Add 之后修改调用方持有的切片，不能影响表内数据。
func TestMemTableAddCopiesInput(t *testing.T) {
	m := newTestMemTable()
	k := []byte("key")
	v := []byte("value")
	m.Add(1, key.TypeValue, k, v)
	k[0] = 'X'
	v[0] = 'Y'

	got, _, found := m.Get(1, []byte("key"))
	if !found {
		t.Fatal("Get 未命中")
	}
	if string(got) != "value" {
		t.Fatalf("value = %q, want \"value\"（Add 必须复制入参）", got)
	}
}

func TestMemTableSizeAndLogNumber(t *testing.T) {
	m := newTestMemTable()
	if m.LogNumber() != 7 {
		t.Errorf("LogNumber() = %d, want 7", m.LogNumber())
	}
	if !m.Empty() {
		t.Error("新建的 MemTable 应为空")
	}
	if m.ApproximateSize() != 0 {
		t.Errorf("ApproximateSize() = %d, want 0", m.ApproximateSize())
	}

	m.Add(1, key.TypeValue, []byte("key"), []byte("value"))
	if m.Empty() {
		t.Error("Add 之后 Empty() = true")
	}
	if m.Len() != 1 {
		t.Errorf("Len() = %d, want 1", m.Len())
	}
	if m.ApproximateSize() <= 0 {
		t.Errorf("ApproximateSize() = %d, want > 0", m.ApproximateSize())
	}
	m.SetLogNumber(9)
	if m.LogNumber() != 9 {
		t.Errorf("SetLogNumber 后 LogNumber() = %d, want 9", m.LogNumber())
	}
}

// Flush 依赖的能力：迭代器要能按 internal key 升序吐出全部记录。
func TestMemTableIteratorCoversAllVersions(t *testing.T) {
	m := newTestMemTable()
	m.Add(1, key.TypeValue, []byte("a"), []byte("a1"))
	m.Add(4, key.TypeValue, []byte("a"), []byte("a2"))
	m.Add(2, key.TypeDeletion, []byte("b"), nil)
	m.Add(3, key.TypeValue, []byte("c"), []byte("c1"))

	type entry struct {
		user string
		seq  uint64
		kind key.Kind
	}
	var got []entry
	it := m.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, entry{
			user: string(key.UserKey(it.Key())),
			seq:  key.SeqNum(it.Key()),
			kind: key.KindOf(it.Key()),
		})
	}
	want := []entry{
		{"a", 4, key.TypeValue},
		{"a", 1, key.TypeValue},
		{"b", 2, key.TypeDeletion},
		{"c", 3, key.TypeValue},
	}
	if len(got) != len(want) {
		t.Fatalf("迭代出 %d 条记录, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 条 = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// Seek 是范围扫描的起点定位，应当落在指定 key 的最新可见版本上。
func TestMemTableSeekLocatesVisibleVersion(t *testing.T) {
	m := newTestMemTable()
	m.Add(1, key.TypeValue, []byte("a"), []byte("a1"))
	m.Add(5, key.TypeValue, []byte("b"), []byte("b1"))
	m.Add(6, key.TypeValue, []byte("b"), []byte("b2"))

	it := m.Seek(5, []byte("b"))
	if !it.Valid() {
		t.Fatal("Seek 之后迭代器失效")
	}
	if got := key.SeqNum(it.Key()); got != 5 {
		t.Fatalf("Seek(5, \"b\") 落在 seq = %d, want 5", got)
	}
	if !bytes.Equal(key.UserKey(it.Key()), []byte("b")) {
		t.Fatalf("Seek 落点 user key = %q, want b", key.UserKey(it.Key()))
	}
}
