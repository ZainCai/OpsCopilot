package main

import "testing"

// TestResolveTenant 租户兜底口径（第七轮 H3）：空值必须落到 "default"，
// 否则评估取数静默为空、M1 出口门禁被假 PASS 击穿。
func TestResolveTenant(t *testing.T) {
	cases := map[string]string{
		"":              "default",
		"  ":            "default",
		"eval-20260911": "eval-20260911",
		"default":       "default",
	}
	for in, want := range cases {
		if got := resolveTenant(in); got != want {
			t.Fatalf("resolveTenant(%q) = %q, want %q", in, got, want)
		}
	}
}
