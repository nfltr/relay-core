// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sni

import (
	"errors"
	"net"
	"testing"

	"github.com/nfltr/relay-core/core/frontend"
)

type signInGrants map[string]frontend.Grant

func (g signInGrants) IsCode(s string) bool { return len(s) > 4 }
func (g signInGrants) Grant(code string) (frontend.Grant, error) {
	if code == "downx" {
		return frontend.Grant{}, errors.New("store unreachable")
	}
	if gr, ok := g[code]; ok {
		return gr, nil
	}
	return frontend.Grant{}, frontend.ErrNoGrant
}

// TestSignInShareNeverE2EE: the relay cannot see who visits an E2EE share,
// so a share whose rule is sign-in is not routed end to end: the mux hands
// its name to the relay's own TLS (the HTTP share front checks the
// sign-in), and the gate refuses one that reached the E2EE path anyway.
// Every other code-shaped name stays E2EE, an unanswering store included
// (routeE2EE refuses it).
func TestSignInShareNeverE2EE(t *testing.T) {
	g := signInGrants{
		"plain": {AgentID: "a.web"},
		"login": {AgentID: "a.web", Policy: frontend.SharePolicy{Login: true}},
	}
	l := &muxListener{e2eeSuffix: ".nfltr.xyz", grants: g}
	for host, want := range map[string]bool{"plain.nfltr.xyz": true, "login.nfltr.xyz": false, "downx.nfltr.xyz": true, "nopex.nfltr.xyz": true, "grpc.nfltr.xyz": false} {
		if got := l.isE2EE(host); got != want {
			t.Errorf("isE2EE(%s) = %v, want %v", host, got, want)
		}
	}
	ip := net.ParseIP("192.0.2.1")
	if err := (target{Grant: g["login"], Share: "login"}).admitClient(ip); err == nil {
		t.Fatal("a sign-in share admitted on the E2EE path")
	}
	if err := (target{Grant: g["plain"], Share: "plain"}).admitClient(ip); err != nil {
		t.Fatalf("an open share refused: %v", err)
	}
}
