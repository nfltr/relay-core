// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package frontend

import (
	"net"
	"testing"
)

func TestSharePolicyAdmitsAndGated(t *testing.T) {
	ip := net.ParseIP("203.0.113.7")
	for _, c := range []struct {
		name         string
		p            SharePolicy
		admits, gate bool
	}{
		{"empty", SharePolicy{}, true, false},
		{"cidr in", SharePolicy{AllowedIPs: []string{" 203.0.113.0/24 "}}, true, false},
		{"exact out", SharePolicy{AllowedIPs: []string{"198.51.100.5"}}, false, false},
		{"garbage", SharePolicy{AllowedIPs: []string{"nope"}}, false, false},
		{"basic auth", SharePolicy{BasicAuth: "sealed"}, true, true},
		{"bearer", SharePolicy{BearerToken: "sealed"}, true, true},
		{"headers", SharePolicy{RequiredHeaders: map[string]string{"X": "sealed"}}, true, true},
	} {
		if got := c.p.Admits(ip); got != c.admits {
			t.Errorf("%s: Admits = %v, want %v", c.name, got, c.admits)
		}
		if got := c.p.Gated(); got != c.gate {
			t.Errorf("%s: Gated = %v, want %v", c.name, got, c.gate)
		}
	}
	if (SharePolicy{AllowedIPs: []string{"0.0.0.0/0"}}).Admits(nil) {
		t.Error("an allowlist admits an unknown client address")
	}
}
