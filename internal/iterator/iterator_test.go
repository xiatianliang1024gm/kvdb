package iterator

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"kvdb/internal/key"
	"kvdb/internal/memdb"
)

type testComparer struct{}

func (testComparer) Compare(a, b []byte) int { return bytes.Compare(a, b) }
func (testComparer) Name() string            { return "test.Bytewise" }

var icmp = key.InternalComparer{User: testComparer{}}

// rec 描述一条加到 MemTable 里的记录。
type rec struct {
	k    string
	seq  uint64
	kind key.Kind
	v    string
}

// child 用一张 MemTable 伪装成一个子迭代器（真实读路径上的子迭代器就是它和 SST 迭代器）。
func child(records ...rec) Iterator {
	m := memdb.New(testComparer{}, 0)
	for _, r := range records {
		m.Add(r.seq, r.kind, []byte(r.k), []byte(r.v))
	}
	return m.NewIterator()
}

// errIter 是一个会报错的子迭代器，用于验证错误传播。
type errIter struct{ err error }

func (e errIter) SeekToFirst()  {}
func (e errIter) Seek([]byte)   {}
func (e errIter) Valid() bool   { return false }
func (e errIter) Key() []byte   { return nil }
func (e errIter) Value() []byte { return nil }
func (e errIter) Next()         {}
func (e errIter) Error() error  { return e.err }

// merge 把若干子迭代器归并起来，并把全部 key 读成 "user#seq" 字符串。
func merge(t *testing.T, children ...Iterator) []string {
	t.Helper()
	mi := NewMerging(icmp, children...)
	var got []string
	for mi.SeekToFirst(); mi.Valid(); mi.Next() {
		got = append(got, fmt.Sprintf("%s#%d", key.UserKey(mi.Key()), key.SeqNum(mi.Key())))
	}
	if err := mi.Error(); err != nil {
		t.Fatalf("归并出错: %v", err)
	}
	return got
}

func TestMergingIteratorOrdersChildren(t *testing.T) {
	// 子迭代器顺序 = 新旧顺序：越靠前越新。
	newer := child(rec{"a", 10, key.TypeValue, "a10"}, rec{"c", 8, key.TypeValue, "c8"})
	older := child(rec{"a", 5, key.TypeValue, "a5"}, rec{"c", 1, key.TypeValue, "c1"})

	got := merge(t, newer, older)
	want := []string{"a#10", "a#5", "c#8", "c#1"}
	if !equalStrings(got, want) {
		t.Fatalf("归并结果 = %v, want %v", got, want)
	}
}

func TestMergingIteratorSeek(t *testing.T) {
	newer := child(rec{"a", 10, key.TypeValue, "a10"}, rec{"c", 8, key.TypeValue, ""})
	older := child(rec{"a", 5, key.TypeValue, "a5"}, rec{"b", 4, key.TypeValue, "b4"})

	mi := NewMerging(icmp, newer, older)
	mi.Seek(key.SeekKey([]byte("b"), 100))
	if !mi.Valid() || string(key.UserKey(mi.Key())) != "b" {
		t.Fatalf("Seek(b) 落在 %q", mi.Key())
	}
	mi.Seek(key.SeekKey([]byte("zzz"), 100))
	if mi.Valid() {
		t.Fatalf("Seek(zzz) 应当失效，实得 %q", mi.Key())
	}
	mi.Seek(key.SeekKey([]byte("a"), 7)) // 比 a#10 旧，比 a#5 新
	if !mi.Valid() || key.SeqNum(mi.Key()) != 5 {
		t.Fatalf("Seek(a@7) 落在 %s, want a#5", mi.Key())
	}
}

func TestMergingIteratorWithoutChildren(t *testing.T) {
	mi := NewMerging(icmp)
	mi.SeekToFirst()
	if mi.Valid() {
		t.Fatal("没有子迭代器时不应有效")
	}
	mi.Next() // 不应 panic
	if mi.Error() != nil {
		t.Fatal(mi.Error())
	}
}

func TestMergingIteratorPropagatesError(t *testing.T) {
	boom := errors.New("boom")
	mi := NewMerging(icmp, child(rec{"a", 1, key.TypeValue, "v"}), errIter{err: boom})
	mi.SeekToFirst()
	if mi.Valid() {
		t.Fatal("有子迭代器报错时不应当继续返回数据")
	}
	if !errors.Is(mi.Error(), boom) {
		t.Fatalf("Error() = %v, want boom", mi.Error())
	}
}

// DBIter 的可见性：快照过滤 + 墓碑遮蔽 + 同 key 去重。
func TestDBIterVisibility(t *testing.T) {
	// 三个层次，模拟 MemTable（最新）→ Immutable → SST（最旧）。
	newest := child(
		rec{"a", 10, key.TypeValue, "a10"},
		rec{"b", 9, key.TypeDeletion, ""},
		rec{"c", 8, key.TypeValue, "c8"},
	)
	middle := child(
		rec{"a", 5, key.TypeValue, "a5"},
		rec{"b", 4, key.TypeValue, "b4"},
		rec{"c", 1, key.TypeDeletion, ""},
	)
	oldest := child(rec{"a", 1, key.TypeValue, "a1"})

	cases := []struct {
		name     string
		snapshot uint64
		want     map[string]string // user key → 期望的 value（不含已删除的 key）
	}{
		{
			name:     "最新快照：a 取新版本，b 被墓碑遮蔽，c 取新版本",
			snapshot: 100,
			want:     map[string]string{"a": "a10", "c": "c8"},
		},
		{
			name:     "中间快照：a 回退到 a5，b 回退到 b4，c 的新版本与墓碑都不可见",
			snapshot: 6,
			want:     map[string]string{"a": "a5", "b": "b4"},
		},
		{
			name:     "最旧快照：只有 a1 活下来（b、c 的可见版本都是墓碑）",
			snapshot: 1,
			want:     map[string]string{"a": "a1"},
		},
		{
			name:     "快照早于全部写入",
			snapshot: 0,
			want:     map[string]string{},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			it := NewDBIter(icmp, NewMerging(icmp, newest, middle, oldest), c.snapshot, nil, nil)
			defer it.Close()

			got := map[string]string{}
			var keys []string
			for it.SeekToFirst(); it.Valid(); it.Next() {
				k := string(it.Key())
				if _, dup := got[k]; dup {
					t.Fatalf("user key %s 被输出了两次", k)
				}
				got[k] = string(it.Value())
				keys = append(keys, k)
			}
			if err := it.Error(); err != nil {
				t.Fatal(err)
			}
			if len(got) != len(c.want) {
				t.Fatalf("扫描出 %v, want %v", got, c.want)
			}
			for k, v := range c.want {
				if got[k] != v {
					t.Errorf("key %s = %q, want %q", k, got[k], v)
				}
			}
			for i := 1; i < len(keys); i++ {
				if icmp.User.Compare([]byte(keys[i-1]), []byte(keys[i])) >= 0 {
					t.Fatalf("输出顺序不是升序: %v", keys)
				}
			}
		})
	}
}

// Seek 的语义：定位到第一个"可见且未被墓碑遮蔽"的 key。
func TestDBIterSeek(t *testing.T) {
	newest := child(rec{"a", 10, key.TypeValue, "a10"}, rec{"c", 8, key.TypeValue, "c8"})
	middle := child(rec{"b", 4, key.TypeDeletion, ""})

	cases := []struct {
		target   string
		snapshot uint64
		want     string
	}{
		{"", 100, "a"},
		{"a", 100, "a"},
		{"b", 100, "c"},  // b 被墓碑遮蔽，落点是 c
		{"bb", 100, "c"}, // 目标本身不存在
		{"c", 100, "c"},
		{"d", 100, ""}, // 越过末尾
		{"b", 2, ""},   // 快照 2 时 b 还没有墓碑，但 b 也没有数据版本
	}
	for _, c := range cases {
		it := NewDBIter(icmp, NewMerging(icmp, newest, middle), c.snapshot, nil, nil)
		it.Seek([]byte(c.target))
		got := ""
		if it.Valid() {
			got = string(it.Key())
		}
		if got != c.want {
			t.Errorf("Seek(%q)@%d = %q, want %q", c.target, c.snapshot, got, c.want)
		}
		it.Close()
	}
}

// 上下界都是闭区间，且 Seek 到界外时要直接失效（不能退化成"扫全表"）。
func TestDBIterBounds(t *testing.T) {
	children := []Iterator{child(
		rec{"a", 1, key.TypeValue, "a"},
		rec{"b", 1, key.TypeValue, "b"},
		rec{"c", 1, key.TypeValue, "c"},
		rec{"d", 1, key.TypeValue, "d"},
	)}

	collect := func(lower, upper string, seek string) []string {
		var lo, up []byte
		if lower != "" {
			lo = []byte(lower)
		}
		if upper != "" {
			up = []byte(upper)
		}
		it := NewDBIter(icmp, NewMerging(icmp, children...), 100, lo, up)
		defer it.Close()
		var got []string
		if seek == "" {
			it.SeekToFirst()
		} else {
			it.Seek([]byte(seek))
		}
		for ; it.Valid(); it.Next() {
			got = append(got, string(it.Key()))
		}
		return got
	}

	if got := collect("b", "c", ""); !equalStrings(got, []string{"b", "c"}) {
		t.Errorf("[b, c] = %v, want [b c]", got)
	}
	if got := collect("b", "", ""); !equalStrings(got, []string{"b", "c", "d"}) {
		t.Errorf("[b, ∞) = %v, want [b c d]", got)
	}
	if got := collect("", "b", ""); !equalStrings(got, []string{"a", "b"}) {
		t.Errorf("[∞, b] = %v, want [a b]", got)
	}
	// Seek 到上界之外：直接失效，不需要把剩下的都扫一遍。
	if got := collect("b", "c", "d"); len(got) != 0 {
		t.Errorf("Seek(d) 超出上界 c，应当为空，实得 %v", got)
	}
	// Seek 到下界之下：应当从下界开始。
	if got := collect("c", "", "a"); !equalStrings(got, []string{"c", "d"}) {
		t.Errorf("Seek(a) 带下界 c = %v, want [c d]", got)
	}
}

// Next 之后必须跳过刚刚输出那个 key 的所有旧版本。
func TestDBIterNextSkipsOlderVersions(t *testing.T) {
	children := []Iterator{child(
		rec{"a", 10, key.TypeValue, "a10"},
		rec{"a", 9, key.TypeValue, "a9"},
		rec{"a", 8, key.TypeValue, "a8"},
		rec{"b", 7, key.TypeValue, "b7"},
	)}
	it := NewDBIter(icmp, NewMerging(icmp, children...), 100, nil, nil)
	defer it.Close()

	it.SeekToFirst()
	if got := string(it.Key()); got != "a" {
		t.Fatalf("第一个 key = %q, want a", got)
	}
	it.Next()
	if got := string(it.Key()); got != "b" {
		t.Fatalf("第二个 key = %q, want b", got)
	}
	it.Next()
	if it.Valid() {
		t.Fatalf("应当结束，却还有 %q", it.Key())
	}
}

// Key() 在两次移动之间必须稳定（值被复制过），Close 之后整体失效。
func TestDBIterKeyStabilityAndClose(t *testing.T) {
	children := []Iterator{child(
		rec{"alpha", 2, key.TypeValue, "v1"},
		rec{"beta", 1, key.TypeValue, "v2"},
	)}
	it := NewDBIter(icmp, NewMerging(icmp, children...), 100, nil, nil)
	it.SeekToFirst()

	first := it.Key()
	value := it.Value() // 读 value 不应破坏 key
	if string(first) != "alpha" || string(it.Key()) != "alpha" {
		t.Fatalf("Key() 不稳定: %q / %q", first, it.Key())
	}
	if string(value) != "v1" {
		t.Fatalf("Value() = %q, want v1", value)
	}

	it.Close()
	if it.Valid() || it.Key() != nil || it.Value() != nil {
		t.Fatal("Close 之后迭代器应当完全失效")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("重复 Close 应当无害: %v", err)
	}
	it.SeekToFirst() // 不应 panic
	it.Next()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
