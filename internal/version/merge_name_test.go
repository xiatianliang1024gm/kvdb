package version

import (
	"strings"
	"testing"
)

// M8（docs/EXTENSIONS.md §4.3）：Merge 算子的名字写进 Manifest，记过即冻结。
// 语义与过滤器同一档：换名字、去掉都被拒绝；错误信息要能定位到是算子不匹配。
func TestMergeOperatorNameMismatchIsRejected(t *testing.T) {
	dir := t.TempDir()
	vsA := New(Config{Dir: dir, Comparer: testComparer{}, MaxLevels: 4, MergeName: "opA"})
	if err := vsA.SetFromScan(nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := vsA.NewManifest(); err != nil {
		t.Fatal(err)
	}
	if err := vsA.Close(); err != nil {
		t.Fatal(err)
	}

	// 换名字 → 拒绝。
	vsB := New(Config{Dir: dir, Comparer: testComparer{}, MaxLevels: 4, MergeName: "opB"})
	_, _, err := vsB.Recover()
	vsB.Close()
	if err == nil {
		t.Fatal("换了算子名读同一个目录必须报错")
	}
	if !strings.Contains(err.Error(), "merge operator") {
		t.Errorf("错误信息应当提到 merge operator: %v", err)
	}

	// 去掉算子 → 同样拒绝（名字记过即冻结）。
	vsC := New(Config{Dir: dir, Comparer: testComparer{}, MaxLevels: 4})
	_, _, err = vsC.Recover()
	vsC.Close()
	if err == nil {
		t.Fatal("已记录算子名的目录不允许在无算子下打开")
	}

	// 原名 → 正常。
	vsD := New(Config{Dir: dir, Comparer: testComparer{}, MaxLevels: 4, MergeName: "opA"})
	defer vsD.Close()
	if _, _, err := vsD.Recover(); err != nil {
		t.Fatalf("原名打开应当成功: %v", err)
	}

	// 反方向：没记过名字的目录，首次配算子是允许的。
	fresh := t.TempDir()
	vsE := New(Config{Dir: fresh, Comparer: testComparer{}, MaxLevels: 4})
	if err := vsE.SetFromScan(nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := vsE.NewManifest(); err != nil {
		t.Fatal(err)
	}
	if err := vsE.Close(); err != nil {
		t.Fatal(err)
	}
	vsF := New(Config{Dir: fresh, Comparer: testComparer{}, MaxLevels: 4, MergeName: "opA"})
	defer vsF.Close()
	if _, _, err := vsF.Recover(); err != nil {
		t.Fatalf("老目录首次配算子应当允许: %v", err)
	}
}
