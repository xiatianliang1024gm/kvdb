package key

import (
	"bytes"
	"errors"
	"sort"
	"testing"
)

// 尾缀必须是 (seq<<8|kind) 的 8 字节大端序，这是与磁盘格式绑定的约定。
func TestEncodeInternalKeyLayout(t *testing.T) {
	ik := EncodeInternalKey([]byte("foo"), 0x0a, TypeValue)
	want := append([]byte("foo"), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x01)
	if !bytes.Equal(ik, want) {
		t.Fatalf("EncodeInternalKey = %v, want %v", ik, want)
	}
	if len(ik) != len("foo")+TrailerLen {
		t.Fatalf("encoded length = %d, want %d", len(ik), len("foo")+TrailerLen)
	}
}

func TestAppendInternalKeyDoesNotAlias(t *testing.T) {
	dst := []byte{0xaa}
	got := AppendInternalKey(dst, []byte("k"), 1, TypeValue)
	if !bytes.Equal(got[:1], []byte{0xaa}) {
		t.Fatalf("AppendInternalKey clobbered the destination prefix: %v", got)
	}
	if len(got) != 1+1+TrailerLen {
		t.Fatalf("appended length = %d, want %d", len(got), 1+1+TrailerLen)
	}
}

func TestEncodeDecodeInternalKeyRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		userKey []byte
		seq     uint64
		kind    Kind
	}{
		{"empty user key", []byte(""), 0, TypeValue},
		{"short user key", []byte("k"), 1, TypeValue},
		{"tombstone", []byte("k"), 42, TypeDeletion},
		{"zero seq", []byte("hello"), 0, TypeDeletion},
		{"max seq", []byte("hello"), MaxSeqNum, TypeValue},
		{"binary user key", []byte{0x00, 0xff, 0x7f, 0x00}, 1 << 40, TypeValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ik := EncodeInternalKey(tt.userKey, tt.seq, tt.kind)

			// 校验型解码。
			userKey, seq, kind, err := DecodeInternalKey(ik)
			if err != nil {
				t.Fatalf("DecodeInternalKey: unexpected error: %v", err)
			}
			if !bytes.Equal(userKey, tt.userKey) {
				t.Errorf("user key = %q, want %q", userKey, tt.userKey)
			}
			if seq != tt.seq {
				t.Errorf("seq = %d, want %d", seq, tt.seq)
			}
			if kind != tt.kind {
				t.Errorf("kind = %v, want %v", kind, tt.kind)
			}

			// 非校验型访问器必须给出一致结果。
			if !bytes.Equal(UserKey(ik), tt.userKey) {
				t.Errorf("UserKey = %q, want %q", UserKey(ik), tt.userKey)
			}
			if SeqNum(ik) != tt.seq {
				t.Errorf("SeqNum = %d, want %d", SeqNum(ik), tt.seq)
			}
			if KindOf(ik) != tt.kind {
				t.Errorf("KindOf = %v, want %v", KindOf(ik), tt.kind)
			}
			if Trailer(ik) != MakeTrailer(tt.seq, tt.kind) {
				t.Errorf("Trailer = %d, want %d", Trailer(ik), MakeTrailer(tt.seq, tt.kind))
			}

			// struct 版本解码。
			parsed, err := ParseInternalKey(ik)
			if err != nil {
				t.Fatalf("ParseInternalKey: unexpected error: %v", err)
			}
			if !bytes.Equal(parsed.UserKey, tt.userKey) || parsed.Seq != tt.seq || parsed.Kind != tt.kind {
				t.Errorf("ParseInternalKey = %v, want {userKey:%q seq:%d kind:%v}", parsed, tt.userKey, tt.seq, tt.kind)
			}
		})
	}
}

func TestDecodeInternalKeyRejectsShortInput(t *testing.T) {
	for n := 0; n < TrailerLen; n++ {
		ik := make([]byte, n)
		if _, _, _, err := DecodeInternalKey(ik); !errors.Is(err, ErrCorruptInternalKey) {
			t.Errorf("DecodeInternalKey(len=%d) error = %v, want ErrCorruptInternalKey", n, err)
		}
	}
}

func TestDecodeInternalKeyRejectsUnknownKind(t *testing.T) {
	// 0..3 已按 docs/EXTENSIONS.md §4.3 的编号表定死（2 = Merge 预留、
	// 3 = RangeDeletion），未定义的编号一律拒绝。
	for _, kind := range []Kind{4, 5, 0xff} {
		ik := EncodeInternalKey([]byte("k"), 7, kind)
		if _, _, _, err := DecodeInternalKey(ik); !errors.Is(err, ErrCorruptInternalKey) {
			t.Errorf("DecodeInternalKey(kind=%d) error = %v, want ErrCorruptInternalKey", kind, err)
		}
	}
	// 2 与 3 是合法（或已预留）的编号，必须放行。
	for _, kind := range []Kind{TypeRangeDeletion} {
		ik := EncodeInternalKey([]byte("k"), 7, kind)
		if _, _, _, err := DecodeInternalKey(ik); err != nil {
			t.Errorf("DecodeInternalKey(kind=%d) error = %v, want nil", kind, err)
		}
	}
}

// 序列号超过 56 位是调用方的编程错误，必须立刻暴露而不是静默截断。
func TestMakeTrailerPanicsOnSeqOverflow(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MakeTrailer(MaxSeqNum+1) did not panic")
		}
	}()
	MakeTrailer(MaxSeqNum+1, TypeValue)
}

func TestMakeTrailerAcceptsMaxSeqNum(t *testing.T) {
	if got, want := MakeTrailer(MaxSeqNum, TypeValue), uint64(MaxSeqNum)<<8|1; got != want {
		t.Fatalf("MakeTrailer(MaxSeqNum) = %d, want %d", got, want)
	}
}

// 长度不足 8 字节的输入下，访问器返回零值而不 panic。
func TestAccessorsOnShortKey(t *testing.T) {
	ik := []byte("abc")
	if UserKey(ik) != nil {
		t.Errorf("UserKey(short) = %v, want nil", UserKey(ik))
	}
	if Trailer(ik) != 0 || SeqNum(ik) != 0 {
		t.Errorf("Trailer/SeqNum on short key = %d/%d, want 0/0", Trailer(ik), SeqNum(ik))
	}
	if KindOf(ik) != TypeDeletion {
		t.Errorf("KindOf(short) = %v, want TypeDeletion", KindOf(ik))
	}
}

func TestKindString(t *testing.T) {
	tests := []struct {
		kind Kind
		want string
	}{
		{TypeDeletion, "Deletion"},
		{TypeValue, "Value"},
		{Kind(9), "Kind(9)"},
	}
	for _, tt := range tests {
		if got := tt.kind.String(); got != tt.want {
			t.Errorf("Kind(%d).String() = %q, want %q", uint8(tt.kind), got, tt.want)
		}
	}
}

func TestParsedInternalKeyString(t *testing.T) {
	p := ParsedInternalKey{UserKey: []byte("foo"), Seq: 12, Kind: TypeValue}
	if got, want := p.String(), "foo#12,Value"; got != want {
		t.Fatalf("ParsedInternalKey.String() = %q, want %q", got, want)
	}
}

// 排序规则的完整验收：user_key 升序；同一 user_key 下尾缀降序（新版本在前）。
func TestInternalKeyCompareOrdering(t *testing.T) {
	a2v := EncodeInternalKey([]byte("a"), 2, TypeValue) // 最新版本
	a2d := EncodeInternalKey([]byte("a"), 2, TypeDeletion)
	a1v := EncodeInternalKey([]byte("a"), 1, TypeValue)
	b0v := EncodeInternalKey([]byte("b"), 0, TypeValue) // user_key 更大

	keys := [][]byte{b0v, a1v, a2d, a2v}
	sort.Slice(keys, func(i, j int) bool {
		return InternalKeyCompare(keys[i], keys[j], nil) < 0
	})

	want := [][]byte{a2v, a2d, a1v, b0v}
	for i := range want {
		if !bytes.Equal(keys[i], want[i]) {
			t.Fatalf("sorted[%d] = %s, want %s", i, describe(keys[i]), describe(want[i]))
		}
	}
}

// 同一 user_key 的版本必须从新到旧排列，这是 MVCC 可见性判定的前提。
func TestInternalKeyCompareVersionOrder(t *testing.T) {
	for seq := uint64(1); seq <= 8; seq++ {
		newer := EncodeInternalKey([]byte("k"), seq+1, TypeValue)
		older := EncodeInternalKey([]byte("k"), seq, TypeValue)
		if got := InternalKeyCompare(newer, older, nil); got >= 0 {
			t.Fatalf("Compare(seq=%d, seq=%d) = %d, want < 0", seq+1, seq, got)
		}
		if got := InternalKeyCompare(older, newer, nil); got <= 0 {
			t.Fatalf("Compare(seq=%d, seq=%d) = %d, want > 0", seq, seq+1, got)
		}
	}
}

func TestInternalKeyCompareIsAntisymmetric(t *testing.T) {
	keys := [][]byte{
		EncodeInternalKey([]byte("a"), 1, TypeValue),
		EncodeInternalKey([]byte("a"), 2, TypeDeletion),
		EncodeInternalKey([]byte("ab"), 1, TypeValue),
		EncodeInternalKey([]byte("b"), 0, TypeValue),
	}
	for _, a := range keys {
		for _, b := range keys {
			ab := InternalKeyCompare(a, b, nil)
			ba := InternalKeyCompare(b, a, nil)
			if ab != -ba {
				t.Fatalf("Compare(%s, %s) = %d but Compare(%s, %s) = %d",
					describe(a), describe(b), ab, describe(b), describe(a), ba)
			}
		}
		if got := InternalKeyCompare(a, a, nil); got != 0 {
			t.Fatalf("Compare(%s, %s) = %d, want 0", describe(a), describe(a), got)
		}
	}
}

// 自定义 user key 比较器必须真正生效：倒序比较器让 "b" 排在 "a" 前面。
func TestInternalKeyCompareUsesCustomComparer(t *testing.T) {
	reverse := func(a, b []byte) int { return -bytes.Compare(a, b) }
	a := EncodeInternalKey([]byte("a"), 1, TypeValue)
	b := EncodeInternalKey([]byte("b"), 1, TypeValue)
	if got := InternalKeyCompare(a, b, reverse); got <= 0 {
		t.Fatalf("Compare(a, b, reverse) = %d, want > 0", got)
	}
	if got := InternalKeyCompare(a, b, nil); got >= 0 {
		t.Fatalf("Compare(a, b, bytes.Compare) = %d, want < 0", got)
	}
}

// SeekKey 的语义验收：落点必须是 snapshot 时刻最新可见的那条记录；
// 若该记录是墓碑，则说明 key 在 snapshot 时刻已被删除。
func TestSeekKeyLandsOnNewestVisibleVersion(t *testing.T) {
	keys := [][]byte{
		EncodeInternalKey([]byte("a"), 5, TypeDeletion), // key 在 seq=5 被删除
		EncodeInternalKey([]byte("a"), 3, TypeValue),
		EncodeInternalKey([]byte("a"), 1, TypeValue),
		EncodeInternalKey([]byte("b"), 2, TypeValue),
	}
	sort.Slice(keys, func(i, j int) bool {
		return InternalKeyCompare(keys[i], keys[j], nil) < 0
	})

	tests := []struct {
		snapshot uint64
		wantSeq  uint64
		wantKind Kind
	}{
		{snapshot: 1, wantSeq: 1, wantKind: TypeValue},
		{snapshot: 2, wantSeq: 1, wantKind: TypeValue},
		{snapshot: 3, wantSeq: 3, wantKind: TypeValue},
		{snapshot: 4, wantSeq: 3, wantKind: TypeValue},
		{snapshot: 5, wantSeq: 5, wantKind: TypeDeletion},
	}
	for _, tt := range tests {
		target := SeekKey([]byte("a"), tt.snapshot)
		i := sort.Search(len(keys), func(i int) bool {
			return InternalKeyCompare(keys[i], target, nil) >= 0
		})
		if i >= len(keys) {
			t.Fatalf("snapshot=%d: seek found no record at all", tt.snapshot)
		}
		userKey, seq, kind, err := DecodeInternalKey(keys[i])
		if err != nil {
			t.Fatalf("snapshot=%d: %v", tt.snapshot, err)
		}
		if !bytes.Equal(userKey, []byte("a")) {
			t.Fatalf("snapshot=%d: seek skipped past user key a, landed on %q", tt.snapshot, userKey)
		}
		if seq != tt.wantSeq || kind != tt.wantKind {
			t.Fatalf("snapshot=%d: landed on (seq=%d, %v), want (seq=%d, %v)",
				tt.snapshot, seq, kind, tt.wantSeq, tt.wantKind)
		}
	}
}

func TestSeekKeyForMissingUserKey(t *testing.T) {
	keys := [][]byte{
		EncodeInternalKey([]byte("a"), 1, TypeValue),
		EncodeInternalKey([]byte("b"), 1, TypeValue),
	}
	sort.Slice(keys, func(i, j int) bool {
		return InternalKeyCompare(keys[i], keys[j], nil) < 0
	})

	target := SeekKey([]byte("zz"), 1)
	i := sort.Search(len(keys), func(i int) bool {
		return InternalKeyCompare(keys[i], target, nil) >= 0
	})
	if i != len(keys) {
		t.Fatalf("seek for missing key landed on index %d (%s), want end of slice", i, describe(keys[i]))
	}
}

// describe 供失败信息使用，把 internal key 渲染成可读形式。
func describe(ik []byte) string {
	p, err := ParseInternalKey(ik)
	if err != nil {
		return "<corrupt>"
	}
	return p.String()
}
