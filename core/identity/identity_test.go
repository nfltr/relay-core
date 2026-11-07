// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"context"
	"crypto/sha256"
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
	raw    bool // state principals as given: no test machine filled in
}

// testMachine is a fixed machine id per subject, for fakes whose
// principals name none.
func testMachine(subject string) MachineID {
	sum := sha256.Sum256([]byte(subject))
	return MachineID("m-" + machineB32.EncodeToString(sum[:16]))
}

func (f *fakeProvider) Name() string { return f.name }

func (f *fakeProvider) Authenticate(_ context.Context, c Credential) (Principal, error) {
	f.calls++
	if f.err != nil {
		return Principal{}, f.err
	}
	p, ok := f.tokens[c.Token]
	if !ok {
		return Principal{}, ErrUnknownCredential
	}
	if p.Machine == "" && !f.raw {
		p.Machine = testMachine(p.Subject)
	}
	return p, nil
}

// TestOwnersAreNamespacedByProvider: whatever owner a provider states, the
// core keys the principal on it under the provider's namespace (the
// primary's stay as stated), and refuses an owner with a ':', so no
// provider can mint another provider's owner.
func TestOwnersAreNamespacedByProvider(t *testing.T) {
	accounts := &fakeProvider{name: "accounts", tokens: map[string]Principal{
		"ka": {Owner: "alice@example.com", Subject: "alice-hub", Names: []string{"*"}},
	}}
	keys := &fakeProvider{name: "keys", tokens: map[string]Principal{
		"kb":    {Owner: "alice@example.com", Subject: "alice-hub", Names: []string{"*"}}, // the same strings
		"forge": {Owner: "accounts:alice@example.com", Subject: "x", Names: []string{"*"}},
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
	if a.Owner("keys", "alice@example.com") != pb.Owner || a.Owner("accounts", "alice@example.com") != pa.Owner || (*Authenticator)(nil).Owner("keys", "x") != "x" {
		t.Fatal("Owner must be the owner form admit gives principals")
	}
	for _, tok := range []string{"forge"} {
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

// TestAdmitUsesPrincipalMachine (spec Reach.tla AgentIdOwned, mutant
// owner-by-name): the agent a connection registers is always its own
// machine's ref; a request naming another machine's ref, or its own, is a
// name that is not a bound service and is refused.
func TestAdmitUsesPrincipalMachine(t *testing.T) {
	ma, mb := NewMachineID(), NewMachineID()
	prov := &fakeProvider{name: "accounts", tokens: map[string]Principal{
		"a": {Owner: "alice", Machine: ma, Subject: "alice-key", Names: []string{"site", "ci"}},
		"b": {Owner: "bob", Machine: mb, Subject: "bob-key", Names: []string{"site"}},
	}}
	a := NewAuthenticator("accounts", prov)
	ref := func(m MachineID, s string) string { return AgentRef{Machine: m, Service: s}.String() }
	for _, tc := range []struct {
		token, name, agent string
		err                error
	}{
		{"a", "", "", nil}, // not an agent: a client
		{"a", "site", ref(ma, "site"), nil},
		{"a", "ci", ref(ma, "ci"), nil},
		{"b", "site", ref(mb, "site"), nil},
		{"a", ref(mb, "site"), "", ErrNameNotAllowed}, // another machine's agent
		{"a", ref(ma, "site"), "", ErrNameNotAllowed}, // a ref is not a service
		{"b", ref(ma, "site"), "", ErrNameNotAllowed},
	} {
		p, err := a.Authenticate(t.Context(), Credential{Token: tc.token, Name: tc.name})
		if !errors.Is(err, tc.err) || (err == nil && (p.AgentID != tc.agent || p.Machine != prov.tokens[tc.token].Machine)) {
			t.Fatalf("%s as %q: %q, %v; want %q, %v", tc.token, tc.name, p.AgentID, err, tc.agent, tc.err)
		}
	}
}

// TestAdmitOnlyBoundServices (spec KeyBoundToNames): a machine registers
// exactly its bound services; no pattern is a wildcard, and a listed name
// that is not a valid service is refused anyway.
func TestAdmitOnlyBoundServices(t *testing.T) {
	long := strings.Repeat("x", MaxServiceLen+1)
	prov := &fakeProvider{name: "accounts", tokens: map[string]Principal{
		"k": {Owner: "o", Subject: "s", Names: []string{"site", "*", "ci-*", "SITE", long}},
	}}
	a := NewAuthenticator("accounts", prov)
	if p, err := a.Authenticate(t.Context(), Credential{Token: "k", Name: "site"}); err != nil || p.AgentID == "" {
		t.Fatalf("a bound service: %+v, %v", p, err)
	}
	for _, name := range []string{"web", "ci-1", "SITE", long, "*", "site.x"} {
		if p, err := a.Authenticate(t.Context(), Credential{Token: "k", Name: name}); !errors.Is(err, ErrNameNotAllowed) {
			t.Fatalf("%q: %+v, %v; want ErrNameNotAllowed", name, p, err)
		}
	}
}

// TestUnenrolledKeyRefused: a credential its provider knows but whose
// principal names no machine (a key from before machines) is refused with
// the typed ErrMachineNotEnrolled, for a client and an agent alike, and is
// never held or cached; a malformed machine id is refused too.
func TestUnenrolledKeyRefused(t *testing.T) {
	prov := &fakeProvider{name: "accounts", raw: true, tokens: map[string]Principal{
		"old": {Owner: "o", Subject: "cli-1234", Names: []string{"site"}, CacheTTL: time.Minute},
		"bad": {Owner: "o", Machine: "m-not-an-id", Subject: "k", Names: []string{"site"}},
	}}
	a := NewAuthenticator("accounts", prov)
	for _, name := range []string{"", "site"} {
		if p, err := a.Authenticate(t.Context(), Credential{Token: "old", Name: name}); !errors.Is(err, ErrMachineNotEnrolled) || p.Owner != "" {
			t.Fatalf("unenrolled as %q: %+v, %v; want ErrMachineNotEnrolled", name, p, err)
		}
	}
	if _, err := a.Authenticate(t.Context(), Credential{Token: "old"}); prov.calls != 3 || err == nil {
		t.Fatalf("an unenrolled answer was cached (%d provider calls)", prov.calls)
	}
	if p, err := a.Authenticate(t.Context(), Credential{Token: "bad"}); err == nil {
		t.Fatalf("a malformed machine id: %+v", p)
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
		"a": {Owner: "o", Subject: "sa", Names: []string{"svc"}, CacheTTL: time.Minute},
		"b": {Owner: "o", Subject: "sb", Names: []string{"svc"}, CacheTTL: time.Minute},
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

// TestOutsideProvidersAreNamespaced: a provider other than the primary
// stating the primary's owner and subject gets its own owner namespace and
// key, cannot grant admin, and SubjectKey keys what it revokes as the core
// keys its principals.
func TestOutsideProvidersAreNamespaced(t *testing.T) {
	accounts := &fakeProvider{name: "accounts", tokens: map[string]Principal{
		"ka": {Owner: "alice@example.com", Subject: "alice-hub", Names: []string{"laptop"}},
	}}
	idp := &fakeProvider{name: "idp", tokens: map[string]Principal{
		"t": {Owner: "alice@example.com", Subject: "alice-hub", Names: []string{"laptop"}, Scopes: []string{"read", "admin"}},
	}}
	a := NewAuthenticator("accounts", accounts, idp)
	pa, _ := a.Authenticate(t.Context(), Credential{Token: "ka", Name: "laptop"})
	p, err := a.Authenticate(t.Context(), Credential{Token: "t", Name: "laptop"})
	if err != nil {
		t.Fatal(err)
	}
	if p.Owner == pa.Owner || p.Admin() || p.Key() == pa.Key() {
		t.Fatalf("owner %q (accounts %q), admin %v, key %q", p.Owner, pa.Owner, p.Admin(), p.Key())
	}
	if a.SubjectKey("idp", "alice-hub") != p.Key() || a.SubjectKey("accounts", "alice-hub") != pa.Key() || a.SubjectKey("none", "x") != "" {
		t.Fatalf("SubjectKey does not key the principals: %q %q", p.Key(), pa.Key())
	}
	defer func() {
		if recover() == nil {
			t.Fatal("two providers of one name were taken")
		}
	}()
	NewAuthenticator("accounts", accounts, &fakeProvider{name: "accounts"})
}
