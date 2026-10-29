// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// fakeProvider knows tokens; calls counts its lookups.
type fakeProvider struct {
	name   string
	tokens map[string]Principal
	err    error
	calls  int
}

func (f *fakeProvider) Name() string { return f.name }

// MintsSubjects: the fake's subjects are minted key ids (Minting).
func (f *fakeProvider) MintsSubjects() {}

// outside is a provider whose subjects an outside authority chose (an
// IdP, a CA): it is not Minting.
type outside struct{ f *fakeProvider }

func (o outside) Name() string { return o.f.name }

func (o outside) Authenticate(ctx context.Context, c Credential) (Principal, error) {
	return o.f.Authenticate(ctx, c)
}

func (f *fakeProvider) Authenticate(_ context.Context, c Credential) (Principal, error) {
	f.calls++
	if f.err != nil {
		return Principal{}, f.err
	}
	p, ok := f.tokens[c.Token]
	if !ok {
		return Principal{}, ErrUnknownCredential
	}
	return p, nil
}

// TestOwnersAreNamespacedByProvider: whatever owner a provider states, the
// core keys the principal on it under the provider's namespace (the
// primary's stay as stated), and refuses an owner or subject with a ':',
// so no provider can mint another provider's owner.
func TestOwnersAreNamespacedByProvider(t *testing.T) {
	accounts := &fakeProvider{name: "accounts", tokens: map[string]Principal{
		"ka": {Owner: "alice@example.com", Subject: "alice-hub", Names: []string{"*"}},
	}}
	keys := &fakeProvider{name: "keys", tokens: map[string]Principal{
		"kb":      {Owner: "alice@example.com", Subject: "alice-hub", Names: []string{"*"}}, // the same strings
		"forge":   {Owner: "accounts:alice@example.com", Subject: "x", Names: []string{"*"}},
		"forge-s": {Owner: "bob", Subject: "accounts:alice-hub", Names: []string{"*"}},
	}}
	a := NewAuthenticator("accounts", accounts, keys)
	pa, err := a.Authenticate(t.Context(), Credential{Token: "ka"})
	if err != nil || pa.Owner != "alice@example.com" || pa.Provider != "accounts" {
		t.Fatalf("primary provider: %+v, %v", pa, err)
	}
	pb, err := a.Authenticate(t.Context(), Credential{Token: "kb"})
	if err != nil || pb.Owner != "keys:alice@example.com" || pb.Key() == pa.Key() {
		t.Fatalf("another provider stating the same owner and subject: %+v, %v; want them apart", pb, err)
	}
	for _, tok := range []string{"forge", "forge-s"} {
		if p, err := a.Authenticate(t.Context(), Credential{Token: tok}); err == nil {
			t.Fatalf("a provider minting another namespace (%s): %+v", tok, p)
		}
	}
	if _, err := a.Authenticate(t.Context(), Credential{Token: "nobody"}); !errors.Is(err, ErrUnknownCredential) {
		t.Fatalf("an unknown credential: %v", err)
	}
	keys.err = errors.New("database down")
	if _, err := a.Authenticate(t.Context(), Credential{Token: "kb"}); err == nil || errors.Is(err, ErrUnknownCredential) {
		t.Fatalf("a provider error must refuse (fail closed), got %v", err)
	}
}

// TestIsolationAcrossOwners: each owner's credential takes only the names
// its principal allows, scoped under its own subject; another owner's
// credential asking for them gets its own scope, never the other's agent.
func TestIsolationAcrossOwners(t *testing.T) {
	prov := &fakeProvider{name: "accounts", tokens: map[string]Principal{
		"a": {Owner: "alice", Subject: "alice-key", Names: []string{"site", "ci-*"}},
		"b": {Owner: "bob", Subject: "bob-key", Names: []string{"*"}},
	}}
	a := NewAuthenticator("accounts", prov)
	for _, tc := range []struct {
		token, name, agent string
		err                error
	}{
		{"a", "", "alice-key", nil},
		{"a", "site", "alice-key.site", nil},
		{"a", "ci-7", "alice-key.ci-7", nil},
		{"a", "alice-key.site", "alice-key.site", nil},
		{"a", "db", "", ErrNameNotAllowed},
		{"a", "alice-key.db", "", ErrNameNotAllowed},
		{"b", "site", "bob-key.site", nil},
		{"b", "alice-key.site", "bob-key.alice-key.site", nil}, // bob's own scope, never alice's agent
	} {
		p, err := a.Authenticate(t.Context(), Credential{Token: tc.token, Name: tc.name})
		if !errors.Is(err, tc.err) || (err == nil && p.AgentID != tc.agent) {
			t.Fatalf("%s as %q: %q, %v; want %q, %v", tc.token, tc.name, p.AgentID, err, tc.agent, tc.err)
		}
	}
}

// TestExpiryEndsConnections: a credential past NotAfter does not
// authenticate, and a connection held with it ends at NotAfter.
func TestExpiryEndsConnections(t *testing.T) {
	soon := time.Now().Add(150 * time.Millisecond)
	prov := &fakeProvider{name: "accounts", tokens: map[string]Principal{
		"t":    {Owner: "o", Subject: "s", Names: []string{"*"}, NotAfter: soon},
		"past": {Owner: "o", Subject: "s2", Names: []string{"*"}, NotAfter: time.Now().Add(-time.Second)},
	}}
	a := NewAuthenticator("accounts", prov)
	if _, err := a.Authenticate(t.Context(), Credential{Token: "past"}); !errors.Is(err, ErrExpired) {
		t.Fatalf("an expired credential: %v", err)
	}
	p, err := a.Authenticate(t.Context(), Credential{Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := a.Hold(t.Context(), p)
	defer ctx.Release()
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), ErrExpired) {
			t.Fatalf("cause %v, want ErrExpired", context.Cause(ctx))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a connection outlived its credential's NotAfter")
	}
}

// TestRevokeCutsAndForgets: Revoke ends the credential's held connections
// and drops cached answers, so the next use asks the provider again.
func TestRevokeCutsAndForgets(t *testing.T) {
	prov := &fakeProvider{name: "accounts", tokens: map[string]Principal{
		"t": {Owner: "o", Subject: "s", Names: []string{"*"}, CacheTTL: time.Minute},
	}}
	a := NewAuthenticator("accounts", prov)
	p, err := a.Authenticate(t.Context(), Credential{Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Authenticate(t.Context(), Credential{Token: "t"}); err != nil || prov.calls != 1 {
		t.Fatalf("a cached answer: %v, %d provider calls", err, prov.calls)
	}
	ctx := a.Hold(t.Context(), p)
	defer ctx.Release()
	delete(prov.tokens, "t")
	if n := a.Revoke(p.Key()); n != 1 || !errors.Is(context.Cause(ctx), ErrRevoked) {
		t.Fatalf("revoke cut %d, cause %v", n, context.Cause(ctx))
	}
	if _, err := a.Authenticate(t.Context(), Credential{Token: "t"}); !errors.Is(err, ErrUnknownCredential) {
		t.Fatalf("after revoke: %v, want the provider asked again", err)
	}
}

// TestRevokeForgetsOnlyThatCredential: a revocation drops the revoked
// credential's cached answers and no other's, and the authenticator starts
// no goroutine (no cache janitor waking an idle relay).
func TestRevokeForgetsOnlyThatCredential(t *testing.T) {
	defer goleak.VerifyNone(t)
	prov := &fakeProvider{name: "accounts", tokens: map[string]Principal{
		"a": {Owner: "o", Subject: "sa", Names: AnyName, CacheTTL: time.Minute},
		"b": {Owner: "o", Subject: "sb", Names: AnyName, CacheTTL: time.Minute},
	}}
	a := NewAuthenticator("accounts", prov)
	pa, _ := a.Authenticate(t.Context(), Credential{Token: "a"})
	_, _ = a.Authenticate(t.Context(), Credential{Token: "b"})
	a.Revoke(pa.Key())
	calls := prov.calls
	if _, err := a.Authenticate(t.Context(), Credential{Token: "b"}); err != nil || prov.calls != calls {
		t.Fatalf("b's cached answer went with a's revocation (%d provider calls)", prov.calls-calls)
	}
	if _, err := a.Authenticate(t.Context(), Credential{Token: "a"}); err != nil || prov.calls != calls+1 {
		t.Fatal("a's revoked answer stayed cached")
	}
}

// TestOutsideSubjectsAreNamespaced: a provider that does not mint its
// subjects (an IdP, a CA) cannot state an accounts key's id and take its
// agent id or the agents under it: its subjects are "<provider>:<subject>"
// (no minted id holds a ':'); it cannot grant admin; SubjectKey keys what
// it revokes as the core keys its principals.
func TestOutsideSubjectsAreNamespaced(t *testing.T) {
	accounts := &fakeProvider{name: "accounts", tokens: map[string]Principal{
		"ka": {Owner: "alice@example.com", Subject: "alice-hub", Names: AnyName},
	}}
	idp := outside{&fakeProvider{name: "idp", tokens: map[string]Principal{
		"t": {Owner: "alice@example.com", Subject: "alice-hub", Names: AnyName, Scopes: []string{"read", "admin"}},
	}}}
	a := NewAuthenticator("accounts", accounts, idp)
	pa, _ := a.Authenticate(t.Context(), Credential{Token: "ka", Name: "laptop"})
	for _, name := range []string{"", "laptop", "alice-hub.laptop"} {
		p, err := a.Authenticate(t.Context(), Credential{Token: "t", Name: name})
		if err != nil {
			t.Fatal(err)
		}
		if p.Subject != "idp:alice-hub" || !strings.HasPrefix(p.AgentID, "idp:alice-hub") || p.AgentID == pa.AgentID || p.AgentID == "alice-hub" || strings.HasPrefix(p.AgentID, "alice-hub.") {
			t.Fatalf("as %q: subject %q, agent %q: an outside provider took an accounts key's agent id", name, p.Subject, p.AgentID)
		}
		// The reach rule takes an agent under "<subject>.": neither is
		// under the other's.
		if strings.HasPrefix(pa.AgentID, p.Subject+".") || strings.HasPrefix(p.AgentID, pa.Subject+".") {
			t.Fatalf("agent %q under subject %q, or %q under %q", pa.AgentID, p.Subject, p.AgentID, pa.Subject)
		}
		if p.Owner == pa.Owner || p.Admin() {
			t.Fatalf("owner %q (accounts %q), admin %v", p.Owner, pa.Owner, p.Admin())
		}
		if a.SubjectKey("idp", "alice-hub") != p.Key() || a.SubjectKey("accounts", "alice-hub") != pa.Key() || a.SubjectKey("none", "x") != "" {
			t.Fatalf("SubjectKey does not key the principals: %q %q", p.Key(), pa.Key())
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("two providers of one name were taken")
		}
	}()
	NewAuthenticator("accounts", accounts, outside{&fakeProvider{name: "accounts"}})
}
