// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import "testing"

// TestReservedOrchIsAnOrdinaryLabel: "orch" is no built-in product name, so
// with no site mapping an agent may register under it.
func TestReservedOrchIsAnOrdinaryLabel(t *testing.T) {
	for _, r := range []Reserved{{}, NewReserved("docs-site")} {
		if r.Label("orch") || r.AgentName("orch") {
			t.Fatal(`"orch" is reserved without a site mapping`)
		}
		if err := r.RefuseAgentName("orch"); err != nil {
			t.Fatalf("RefuseAgentName(orch) = %v, want nil", err)
		}
		if !r.Label("relay") || r.RefuseAgentName("dashboard") == nil {
			t.Fatal("built-in product names are no longer reserved")
		}
	}
}

// TestReservedSiteMappingReservesOrch: a relay that maps orch to a site
// (http.site_hosts orch=..., as orch.nfltr.xyz does) keeps the label the
// site's; dropping the mapping frees it.
func TestReservedSiteMappingReservesOrch(t *testing.T) {
	r := NewReserved("orch")
	if !r.Label("ORCH") || r.RefuseAgentName("orch") == nil {
		t.Fatal("a site-mapped orch label is not reserved")
	}
	r.Set()
	if r.AgentName("orch") {
		t.Fatal("orch stays reserved after its site mapping is removed")
	}
}
