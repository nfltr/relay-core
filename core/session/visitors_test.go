// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"log/slog"
	"net"
	"testing"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/platform/connlimit"
)

// A visitor's connection counts against the agent's owner: two agents of
// one account share its budget, another account's agent has its own.
func TestAdmitVisitorCountsTheAgentsAccount(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, directory.NewNoOpInventory())
	owners := map[string]string{"web": "alice", "api": "alice", "blog": "bob"}
	d.ownerOf = func(agentID string) (string, error) { return owners[agentID], nil }
	var refused []string
	d.VisitorRefused = func(owner, agentID, reason string) { refused = append(refused, owner+"/"+agentID+"/"+reason) }
	ip := net.ParseIP("203.0.113.7")
	if _, why := d.AdmitVisitor("web", ip); why != "" {
		t.Fatalf("no limits set: refused %s", why)
	}
	d.Visitors = connlimit.NewAccounts(connlimit.Limits{SourceMax: 1})
	release, why := d.AdmitVisitor("web", ip)
	if why != "" {
		t.Fatal(why)
	}
	if _, why := d.AdmitVisitor("api", ip); why != connlimit.ReasonAccountSourceConns {
		t.Fatalf("alice's second agent from one address: %q, want %s", why, connlimit.ReasonAccountSourceConns)
	}
	if _, why := d.AdmitVisitor("blog", ip); why != "" {
		t.Fatalf("bob refused for alice's visitors: %s", why)
	}
	release()
	if _, why := d.AdmitVisitor("api", ip); why != "" {
		t.Fatalf("after the release: %s", why)
	}
	if len(refused) != 1 || refused[0] != "alice/api/"+connlimit.ReasonAccountSourceConns {
		t.Fatalf("VisitorRefused heard %v", refused)
	}
}
