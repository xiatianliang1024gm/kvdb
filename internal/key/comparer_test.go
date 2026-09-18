package key

import (
	"bytes"
	"strings"
	"testing"
)

// fakeComparer 是仅用于测试的自定义比较器：按字节序的反序排列。
type fakeComparer struct{}

func (fakeComparer) Compare(a, b []byte) int { return -bytes.Compare(a, b) }
func (fakeComparer) Name() string            { return "test.reverse" }

func TestInternalComparerOrdering(t *testing.T) {
	c := InternalComparer{User: nil}

	// user key 升序
	if got := c.Compare(EncodeInternalKey([]byte("a"), 1, TypeValue), EncodeInternalKey([]byte("b"), 1, TypeValue)); got >= 0 {
		t.Errorf("Compare(a, b) = %d, want < 0", got)
	}
	// 同一 user key：尾缀降序，新版本在前
	newer := EncodeInternalKey([]byte("k"), 7, TypeValue)
	older := EncodeInternalKey([]byte("k"), 3, TypeValue)
	if got := c.Compare(newer, older); got >= 0 {
		t.Errorf("Compare(新版本, 旧版本) = %d, want < 0", got)
	}
	if got := c.Compare(older, newer); got <= 0 {
		t.Errorf("Compare(旧版本, 新版本) = %d, want > 0", got)
	}
	// 同一 (seq)：Value(1) 比 Deletion(0) 更"新"，排在前面
	val := EncodeInternalKey([]byte("k"), 5, TypeValue)
	del := EncodeInternalKey([]byte("k"), 5, TypeDeletion)
	if got := c.Compare(val, del); got >= 0 {
		t.Errorf("Compare(Value, Deletion) = %d, want < 0", got)
	}
	// 反对称性
	for _, pair := range [][2][]byte{{newer, older}, {older, newer}, {val, del}} {
		a, b := pair[0], pair[1]
		if c.Compare(a, b) != -c.Compare(b, a) {
			t.Errorf("比较结果不满足反对称性: %v vs %v", a, b)
		}
	}
	// 相同的 internal key
	if got := c.Compare(newer, newer); got != 0 {
		t.Errorf("Compare(x, x) = %d, want 0", got)
	}
}

func TestInternalComparerUsesUserComparer(t *testing.T) {
	a := EncodeInternalKey([]byte("a"), 1, TypeValue)
	b := EncodeInternalKey([]byte("b"), 1, TypeValue)

	// 同一个 InternalComparer 但持有不同 user 比较器，结果必须翻转。
	bytewise := InternalComparer{User: fakeComparerNamed{}}
	reversed := InternalComparer{User: fakeComparer{}}

	if got := bytewise.Compare(a, b); got >= 0 {
		t.Errorf("字节序下 Compare(a, b) = %d, want < 0", got)
	}
	if got := reversed.Compare(a, b); got <= 0 {
		t.Errorf("逆序下 Compare(a, b) = %d, want > 0", got)
	}
}

// 版本降序规则不受 user 比较器影响。
func TestInternalComparerVersionOrderIndependentOfUserComparer(t *testing.T) {
	newer := EncodeInternalKey([]byte("k"), 9, TypeValue)
	older := EncodeInternalKey([]byte("k"), 2, TypeValue)
	for _, c := range []InternalComparer{{User: nil}, {User: fakeComparer{}}, {User: fakeComparerNamed{}}} {
		if got := c.Compare(newer, older); got >= 0 {
			t.Errorf("%s: 新版本应排在旧版本之前, got %d", c.Name(), got)
		}
	}
}

func TestInternalComparerName(t *testing.T) {
	if name := (InternalComparer{User: nil}).Name(); !strings.Contains(name, "bytes") {
		t.Errorf("Name() = %q, 期望包含 bytes", name)
	}
	name := InternalComparer{User: fakeComparer{}}.Name()
	if !strings.Contains(name, "test.reverse") {
		t.Errorf("Name() = %q, 期望包含 user 比较器的名字", name)
	}
	if (InternalComparer{User: fakeComparer{}}).Name() == (InternalComparer{User: fakeComparerNamed{}}).Name() {
		t.Error("不同 user 比较器应产生不同的 Name()")
	}
}

// UserCompare 只看 user key，忽略尾缀。
func TestInternalComparerUserCompare(t *testing.T) {
	c := InternalComparer{User: nil}
	v1 := EncodeInternalKey([]byte("k"), 1, TypeValue)
	v2 := EncodeInternalKey([]byte("k"), 99, TypeDeletion)
	if got := c.UserCompare(v1, v2); got != 0 {
		t.Errorf("UserCompare(同 key 的不同版本) = %d, want 0", got)
	}
	other := EncodeInternalKey([]byte("l"), 1, TypeValue)
	if got := c.UserCompare(v1, other); got >= 0 {
		t.Errorf("UserCompare(k, l) = %d, want < 0", got)
	}

	// nil user 比较器时退化为字节序。
	if got := (InternalComparer{}).UserCompare(v1, other); got >= 0 {
		t.Errorf("UserCompare with nil comparer = %d, want < 0", got)
	}
}

// fakeComparerNamed 与 fakeComparer 行为一致但名字不同，用于验证 Name 的可区分性。
type fakeComparerNamed struct{}

func (fakeComparerNamed) Compare(a, b []byte) int { return bytes.Compare(a, b) }
func (fakeComparerNamed) Name() string            { return "test.bytewise" }
