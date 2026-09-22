package kvdb

import (
	"bytes"
	"strings"
	"testing"

	"github.com/xiatianliang1024gm/kvdb/internal/key"
)

func TestDefaultOptionsIsValid(t *testing.T) {
	o := DefaultOptions("data")
	if err := o.Validate(); err != nil {
		t.Fatalf("DefaultOptions failed validation: %v", err)
	}
	if err := o.prepare(); err != nil {
		t.Fatalf("DefaultOptions failed prepare: %v", err)
	}

	fields := []struct {
		name string
		got  int
		want int
	}{
		{"MemTableSize", o.MemTableSize, DefaultMemTableSize},
		{"BlockSize", o.BlockSize, DefaultBlockSize},
		{"BlockCacheSize", o.BlockCacheSize, DefaultBlockCacheSize},
		{"BloomBitsPerKey", o.BloomBitsPerKey, DefaultBloomBitsPerKey},
		{"L0CompactionTrigger", o.L0CompactionTrigger, DefaultL0CompactionTrigger},
		{"LevelBaseSize", o.LevelBaseSize, DefaultLevelBaseSize},
		{"LevelSizeMultiplier", o.LevelSizeMultiplier, DefaultLevelSizeMultiplier},
		{"MaxLevels", o.MaxLevels, DefaultMaxLevels},
	}
	for _, f := range fields {
		if f.got != f.want {
			t.Errorf("DefaultOptions().%s = %d, want %d", f.name, f.got, f.want)
		}
	}
	if o.Comparer == nil {
		t.Error("DefaultOptions().Comparer is nil")
	}
	if !o.SyncWrites {
		t.Error("DefaultOptions().SyncWrites = false, want true（默认不牺牲持久性）")
	}
}

func TestEnsureDefaultsFillsZeroValues(t *testing.T) {
	o := Options{Dir: "data"}
	if err := o.prepare(); err != nil {
		t.Fatalf("prepare() on bare Options failed: %v", err)
	}

	want := DefaultOptions("data")
	if o.Comparer.Name() != want.Comparer.Name() {
		t.Errorf("Comparer = %q, want %q", o.Comparer.Name(), want.Comparer.Name())
	}
	if o.MemTableSize != want.MemTableSize ||
		o.BlockSize != want.BlockSize ||
		o.BlockCacheSize != want.BlockCacheSize ||
		o.BloomBitsPerKey != want.BloomBitsPerKey ||
		o.L0CompactionTrigger != want.L0CompactionTrigger ||
		o.LevelBaseSize != want.LevelBaseSize ||
		o.LevelSizeMultiplier != want.LevelSizeMultiplier ||
		o.MaxLevels != want.MaxLevels {
		t.Fatalf("ensureDefaults() = %+v, want fields of %+v", o, want)
	}
}

// 负数表示显式关闭可选的读优化组件，归一化后为 0。
func TestEnsureDefaultsNegativeDisablesFeature(t *testing.T) {
	o := Options{Dir: "data", BlockCacheSize: -1, BloomBitsPerKey: -1}
	if err := o.prepare(); err != nil {
		t.Fatalf("prepare() failed: %v", err)
	}
	if o.BlockCacheSize != 0 {
		t.Errorf("BlockCacheSize = %d, want 0（已关闭）", o.BlockCacheSize)
	}
	if o.BloomBitsPerKey != 0 {
		t.Errorf("BloomBitsPerKey = %d, want 0（已关闭）", o.BloomBitsPerKey)
	}
}

func TestValidateRejectsBadOptions(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(o *Options)
		wantSub string
	}{
		{"empty dir", func(o *Options) { o.Dir = "" }, "Dir"},
		{"nil comparer", func(o *Options) { o.Comparer = nil }, "Comparer"},
		{"zero memtable size", func(o *Options) { o.MemTableSize = 0 }, "MemTableSize"},
		{"negative block size", func(o *Options) { o.BlockSize = -1 }, "BlockSize"},
		{"l0 trigger too small", func(o *Options) { o.L0CompactionTrigger = 1 }, "L0CompactionTrigger"},
		{"zero level base size", func(o *Options) { o.LevelBaseSize = 0 }, "LevelBaseSize"},
		{"multiplier too small", func(o *Options) { o.LevelSizeMultiplier = 1 }, "LevelSizeMultiplier"},
		{"single level", func(o *Options) { o.MaxLevels = 1 }, "MaxLevels"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := DefaultOptions("data")
			tt.mutate(&o)
			err := o.Validate()
			if err == nil {
				t.Fatal("Validate() = nil, want error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Fatalf("Validate() error = %q, want it to mention %q", err, tt.wantSub)
			}
		})
	}
}

func TestLevelMaxBytes(t *testing.T) {
	o := DefaultOptions("data")
	base := uint64(DefaultLevelBaseSize)

	if got := o.levelMaxBytes(0); got != 0 {
		t.Errorf("levelMaxBytes(0) = %d, want 0（L0 不设容量上限）", got)
	}
	if got := o.levelMaxBytes(1); got != base {
		t.Errorf("levelMaxBytes(1) = %d, want %d", got, base)
	}
	if got := o.levelMaxBytes(2); got != base*10 {
		t.Errorf("levelMaxBytes(2) = %d, want %d", got, base*10)
	}
	if got := o.levelMaxBytes(3); got != base*100 {
		t.Errorf("levelMaxBytes(3) = %d, want %d", got, base*100)
	}
}

func TestBytewiseComparer(t *testing.T) {
	c := BytewiseComparer{}
	tests := []struct {
		a, b []byte
		want int
	}{
		{[]byte("a"), []byte("b"), -1},
		{[]byte("b"), []byte("a"), 1},
		{[]byte("a"), []byte("a"), 0},
		{[]byte("a"), []byte("ab"), -1},
		{nil, []byte("a"), -1},
		{[]byte{0x00}, []byte{0xff}, -1},
	}
	for _, tt := range tests {
		got := c.Compare(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("BytewiseComparer.Compare(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
		if got != bytes.Compare(tt.a, tt.b) {
			t.Errorf("BytewiseComparer.Compare(%q, %q) diverges from bytes.Compare", tt.a, tt.b)
		}
	}
	if c.Name() == "" {
		t.Error("BytewiseComparer.Name() is empty")
	}
}

// reverseComparer 用于验证 Comparer 的可插拔性：逆序排列 user key。
type reverseComparer struct{}

func (reverseComparer) Compare(a, b []byte) int { return -bytes.Compare(a, b) }
func (reverseComparer) Name() string            { return "test.reverseComparer" }

func TestInternalKeyComparerAppliesUserComparer(t *testing.T) {
	o := DefaultOptions("data")
	c := o.internalKeyComparer()

	newer := key.EncodeInternalKey([]byte("k"), 2, key.TypeValue)
	older := key.EncodeInternalKey([]byte("k"), 1, key.TypeValue)
	if c.Compare(newer, older) >= 0 {
		t.Error("internalKeyComparer: 新版本应排在旧版本之前")
	}
	if c.Compare(older, newer) <= 0 {
		t.Error("internalKeyComparer: 旧版本应排在新版本之后")
	}

	a := key.EncodeInternalKey([]byte("a"), 1, key.TypeValue)
	b := key.EncodeInternalKey([]byte("b"), 1, key.TypeValue)
	if c.Compare(a, b) >= 0 {
		t.Error("internalKeyComparer: 默认比较器下 a 应排在 b 之前")
	}

	// 换成逆序比较器后顺序必须翻转，说明 Options.Comparer 真正生效。
	reversed := Options{Comparer: reverseComparer{}}.internalKeyComparer()
	if reversed.Compare(a, b) <= 0 {
		t.Error("internalKeyComparer: 逆序比较器下 a 应排在 b 之后")
	}
	if reversed.Compare(newer, older) >= 0 {
		t.Error("internalKeyComparer: 版本降序规则不应受 user 比较器影响")
	}
}

// Comparer 为 nil 时 internalComparer 退化为字节序比较，不 panic。
func TestInternalKeyComparerWithoutUserComparer(t *testing.T) {
	var c internalComparer
	a := key.EncodeInternalKey([]byte("a"), 1, key.TypeValue)
	b := key.EncodeInternalKey([]byte("b"), 1, key.TypeValue)
	if c.Compare(a, b) >= 0 {
		t.Error("internalComparer{} should fall back to bytes.Compare")
	}
}
