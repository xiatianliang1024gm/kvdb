package memdb

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/xiatianliang1024gm/kvdb/internal/key"
)

// 测试用的比较器：字节序，与 kvdb.BytewiseComparer 行为一致。
type testComparer struct{}

func (testComparer) Compare(a, b []byte) int { return bytes.Compare(a, b) }
func (testComparer) Name() string            { return "test.Bytewise" }

func newTestSkiplist() *Skiplist {
	return NewSkiplist(key.InternalComparer{User: testComparer{}})
}

func ik(s string, seq uint64) []byte {
	return key.EncodeInternalKey([]byte(s), seq, key.TypeValue)
}

// 按给定顺序插入后，顺序遍历必须得到按 internal key 升序的序列。
func TestSkiplistIterateInOrder(t *testing.T) {
	keys := []string{"delta", "alpha", "charlie", "bravo", "echo"}
	s := newTestSkiplist()
	for i, k := range keys {
		s.Insert(ik(k, uint64(i+1)), []byte(k))
	}

	var got []string
	it := s.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, string(key.UserKey(it.Key())))
	}
	want := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	if !equalStrings(got, want) {
		t.Fatalf("顺序遍历 = %v, want %v", got, want)
	}
	if n := s.Len(); n != int64(len(keys)) {
		t.Fatalf("Len() = %d, want %d", n, len(keys))
	}
}

// 升序插入是跳表的最坏情况，验证层高增长与有序性都不出问题。
func TestSkiplistAscendingInsert(t *testing.T) {
	const n = 2000
	s := newTestSkiplist()
	for i := 0; i < n; i++ {
		s.Insert(ik(fmt.Sprintf("k%06d", i), uint64(i+1)), []byte{byte(i)})
	}
	if s.Len() != n {
		t.Fatalf("Len() = %d, want %d", s.Len(), n)
	}
	if h := s.Height(); h < 2 {
		t.Errorf("Height() = %d, want >= 2（2000 个节点不该只有一层）", h)
	}

	it := s.NewIterator()
	var prev []byte
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if prev != nil && bytes.Compare(prev, it.Key()) > 0 {
			t.Fatalf("遍历顺序被破坏：%q 之后出现 %q", prev, it.Key())
		}
		prev = it.Key()
	}
}

// 降序插入同样必须落成升序结构。
func TestSkiplistDescendingInsert(t *testing.T) {
	const n = 1000
	s := newTestSkiplist()
	for i := n - 1; i >= 0; i-- {
		s.Insert(ik(fmt.Sprintf("k%06d", i), uint64(n-i)), nil)
	}
	it := s.NewIterator()
	it.SeekToFirst()
	if !it.Valid() {
		t.Fatal("跳表为空")
	}
	if got := string(key.UserKey(it.Key())); got != "k000000" {
		t.Fatalf("首个节点 = %q, want k000000", got)
	}
}

func TestSkiplistSeek(t *testing.T) {
	s := newTestSkiplist()
	for i, k := range []string{"b", "d", "f"} {
		s.Insert(ik(k, uint64(i+1)), []byte(k))
	}

	tests := []struct {
		name    string
		target  string
		wantKey string
		valid   bool
	}{
		{"精确命中", "d", "d", true},
		{"落在下一个 key", "c", "d", true},
		{"低于最小值", "a", "b", true},
		{"高于最大值", "z", "", false},
		{"空 key", "", "b", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := s.NewIterator()
			// 用最大序列号做 seek 目标，等价于"这个 user key 的全部版本之前"，
			// 落点即该 key 的最新版本。
			it.Seek(key.SeekKey([]byte(tt.target), key.MaxSeqNum))
			if it.Valid() != tt.valid {
				t.Fatalf("Valid() = %v, want %v", it.Valid(), tt.valid)
			}
			if !tt.valid {
				return
			}
			if got := string(key.UserKey(it.Key())); got != tt.wantKey {
				t.Fatalf("Seek(%q) 落点 = %q, want %q", tt.target, got, tt.wantKey)
			}
		})
	}
}

// 同一 user key 的多个版本必须按尾缀降序（新版本在前）。
func TestSkiplistVersionsOrderedNewestFirst(t *testing.T) {
	s := newTestSkiplist()
	for seq := uint64(1); seq <= 5; seq++ {
		s.Insert(ik("k", seq), []byte(fmt.Sprintf("v%d", seq)))
	}
	it := s.NewIterator()
	it.SeekToFirst()
	for want := uint64(5); want >= 1; want-- {
		if !it.Valid() {
			t.Fatalf("版本 %d 缺失", want)
		}
		if got := key.SeqNum(it.Key()); got != want {
			t.Fatalf("第 %d 个版本 seq = %d, want %d", 6-want, got, want)
		}
		it.Next()
	}
	if it.Valid() {
		t.Fatal("出现了多余的版本")
	}
}

// 序列号唯一是跳表的隐含前提，重复插入必须立刻暴露而不是静默覆盖。
func TestSkiplistDuplicatePanics(t *testing.T) {
	s := newTestSkiplist()
	s.Insert(ik("k", 1), []byte("v"))
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("重复插入 internal key 没有 panic")
		}
	}()
	s.Insert(ik("k", 1), []byte("v2"))
}

// 单写者 + 多读者：写者持续插入，读者并发遍历，验证结构始终可读。
func TestSkiplistConcurrentReaders(t *testing.T) {
	const writes = 2000
	s := newTestSkiplist()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				it := s.NewIterator()
				var prev []byte
				for it.SeekToFirst(); it.Valid(); it.Next() {
					if prev != nil && bytes.Compare(prev, it.Key()) > 0 {
						t.Errorf("并发遍历时顺序被破坏")
						return
					}
					prev = it.Key()
				}
			}
		}()
	}

	for i := 0; i < writes; i++ {
		s.Insert(ik(fmt.Sprintf("k%05d", i), uint64(i+1)), []byte("v"))
	}
	close(stop)
	wg.Wait()

	if s.Len() != writes {
		t.Fatalf("Len() = %d, want %d", s.Len(), writes)
	}
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
