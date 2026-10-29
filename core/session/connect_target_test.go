// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import "testing"

func TestValidPort(t *testing.T) {
	for in, want := range map[string]bool{
		"22": true, "1": true, "65535": true, "8080": true,
		"0": false, "65536": false, "99999": false, "100000": false, "": false,
		"abc": false, "-1": false, "22.5": false, "22/tcp": false,
	} {
		if got := ValidPort(in); got != want {
			t.Errorf("ValidPort(%q) = %v, want %v", in, got, want)
		}
	}
}
