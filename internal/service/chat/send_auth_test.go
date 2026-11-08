package chat

import "testing"

// 仅覆盖不触达 DB/Redis 的纯校验分支（空值、未知前缀）。
func TestCanSendToRejectsInvalidInputs(t *testing.T) {
	cases := [][2]string{
		{"", "U-1"},
		{"U-1", ""},
		{"", ""},
		{"U-1", "X-1"},
		{"U-1", "123"},
	}
	for _, c := range cases {
		if CanSendTo(c[0], c[1]) {
			t.Fatalf("expected deny for %q -> %q", c[0], c[1])
		}
	}
}
