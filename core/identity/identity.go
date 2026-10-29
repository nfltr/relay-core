// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nfltr/relay-core/internal/platform/expiring"
)

// Credential is what a connection presented: a Token, or else the TLS
// client certificate chain (PeerCerts, leaf first; the TLS handshake proved
// the client holds the leaf's key, a provider verifies the chain).
type Credential struct {
	Token     string              // x-api-key, or Authorization: Bearer|ApiKey
	PeerCerts []*x509.Certificate // the TLS client chain, when no Token
	Name      string              // x-agent-id: the agent name asked for ("" none)
	Labels    map[string]string   // the agent's labels (a fleet token matches them)
}

// Principal is what the core keys every decision on. A provider fills
// Owner, Subject, Names, Scopes, NotAfter and CacheTTL; the core namespaces
// Owner and sets Provider and AgentID.
type Principal struct {
	Provider string        // the provider that authenticated the credential
	Owner    string        // who owns what the credential opens, namespaced by Provider
	Subject  string        // the credential's own id: a minted key's agent id or a token's id as stated (Minting), else "<provider>:<subject>"
	Names    []string      // agent names it may take under Subject: "*" any, "p*" a prefix, else exact
	Scopes   []string      // opaque to the core but "admin"
	NotAfter time.Time     // its connections end then; zero: until revoked
	CacheTTL time.Duration // how long the core may reuse this answer (0: not at all)
	AgentID  string        // the agent id the connection takes: Subject, or "<Subject>.<Name>"
	key      string        // Key(), set by the core
}

// Admin reports whether the principal holds the admin scope.
func (p Principal) Admin() bool { return slices.Contains(p.Scopes, "admin") }

// Key names the credential across providers: what the core holds its
// connections under and revokes (Revoke).
func (p Principal) Key() string {
	if p.key != "" {
		return p.key
	}
	return Key(p.Provider, p.Subject)
}

// Key is Principal.Key for a provider's subject.
func Key(provider, subject string) string { return provider + ":" + subject }

// Provider turns a credential into a principal. It never sees a target, a
// session or another owner's data, so it cannot widen reach: it can only
// authenticate or not.
type Provider interface {
	// Name is the provider's namespace, unique per relay.
	Name() string
	// Authenticate returns ErrUnknownCredential for a credential it does
	// not hold (the next provider is asked), an error wrapping
	// ErrUnavailable when it cannot tell now (its store is down: the
	// caller retries); any other error refuses it.
	Authenticate(ctx context.Context, c Credential) (Principal, error)
}

// Minting is a provider whose subjects are credential ids this relay
// minted (its key file, its accounts store): they are agent ids as they
// are. Every other provider's subjects are names an outside authority
// chose (a CA, an IdP); the core makes them "<provider>:<subject>", which
// no minted id (the core refuses a ':' in a subject) and no other
// provider's can equal, so no provider can take another's agent ids or
// reach the agents under them ("<subject>.<name>").
type Minting interface{ MintsSubjects() }

// Revoker is a provider that revokes credentials itself (a reloaded CA
// bundle or revocation list): it calls the function the relay sets with
// each revoked subject (as it states subjects), after the subject stopped
// authenticating, and the relay ends its connections on every replica
// (Authenticator.Revoke with Authenticator.SubjectKey).
type Revoker interface {
	OnRevoke(func(subject string))
}

// Errors the core answers with.
var (
	ErrUnknownCredential = errors.New("unknown credential")
	ErrNameNotAllowed    = errors.New("agent name not allowed for this credential")
	// ErrUnavailable: a provider could not look the credential up (its
	// store is down or out of connections). Not a verdict on the
	// credential: it is not cached, and callers answer it as retryable
	// (HTTP 503, gRPC Unavailable), never as a bad credential.
	ErrUnavailable = errors.New("credential store unavailable")
)

// cacheSize bounds the cached answers; cacheCeiling bounds any one's age
// (a principal's CacheTTL is the usual bound; the ceiling is a backstop for
// a replica that missed a revocation on the bus).
const (
	cacheSize    = 4096
	cacheCeiling = 5 * time.Minute
)

// cacheKey names one credential (a token's hash, or a certificate's
// fingerprint: cert tells which) asked for one name with one label set; it
// costs no allocation to build for a token with no labels.
type cacheKey struct {
	token  [sha256.Size]byte
	cert   bool
	name   string
	labels string
}

// AnyName is the Names of a principal that may take any agent name.
var AnyName = []string{"*"}

// Authenticator is the core's provider chain: the first provider that
// knows a credential decides. It namespaces owners, enforces the names a
// credential may take and its expiry, caches answers for their CacheTTL,
// and holds and revokes the connections credentials open (Hold, Revoke).
type Authenticator struct {
	providers []Provider
	primary   string
	cache     *expiring.Cache[cacheKey, Principal]
	live      Live
	now       func() time.Time
}

// NewAuthenticator asks providers in order. primary names the provider
// whose owners stay as the provider states them (the relay's own accounts,
// whose stored data is keyed by them); every other provider's owners are
// "<provider>:<owner>". No provider may state an owner with a ':', so no
// provider can mint another's owner; only the primary provider's admin
// scope is honoured. Two providers of one name panic: they would share a
// namespace.
func NewAuthenticator(primary string, providers ...Provider) *Authenticator {
	seen := map[string]bool{}
	for _, p := range providers {
		if seen[p.Name()] {
			panic("identity: two providers named " + p.Name())
		}
		seen[p.Name()] = true
	}
	return &Authenticator{
		providers: providers,
		primary:   primary,
		cache:     expiring.New[cacheKey, Principal](cacheSize, cacheCeiling, nil),
		now:       time.Now,
	}
}

// Authenticate turns c into a principal, or fails: ErrUnknownCredential
// when no provider knows it, ErrExpired, ErrNameNotAllowed, or the
// deciding provider's error (ErrUnavailable: retry).
func (a *Authenticator) Authenticate(ctx context.Context, c Credential) (Principal, error) {
	if a == nil || (c.Token == "" && len(c.PeerCerts) == 0) {
		return Principal{}, ErrUnknownCredential
	}
	key := keyOf(c)
	if p, ok := a.cache.Get(key); ok {
		return p, nil
	}
	now := a.now()
	for _, prov := range a.providers {
		p, err := prov.Authenticate(ctx, c)
		if errors.Is(err, ErrUnknownCredential) {
			continue
		}
		if err != nil {
			return Principal{}, err
		}
		if p, err = a.admit(prov, p, c.Name, now); err != nil {
			return Principal{}, err
		}
		if ttl := p.CacheTTL; ttl > 0 {
			until := now.Add(ttl)
			if !p.NotAfter.IsZero() && p.NotAfter.Before(until) {
				until = p.NotAfter
			}
			a.cache.AddUntil(key, p, until)
		}
		return p, nil
	}
	return Principal{}, ErrUnknownCredential
}

// admit applies the core's rules to a provider's answer.
func (a *Authenticator) admit(prov Provider, p Principal, name string, now time.Time) (Principal, error) {
	provider := prov.Name()
	if p.Subject == "" || p.Owner == "" || strings.ContainsRune(p.Owner, ':') || strings.ContainsRune(p.Subject, ':') {
		return Principal{}, fmt.Errorf("identity: provider %s stated owner %q, subject %q: want both, without ':'", provider, p.Owner, p.Subject)
	}
	if !p.NotAfter.IsZero() && !now.Before(p.NotAfter) {
		return Principal{}, ErrExpired
	}
	p.Subject = subjectOf(prov, p.Subject)
	if name != "" && name != p.Subject && !nameAllowed(p.Names, strings.TrimPrefix(name, p.Subject+".")) {
		return Principal{}, ErrNameNotAllowed
	}
	p.Provider = provider
	if provider != a.primary {
		p.Owner = provider + ":" + p.Owner
		if p.Admin() {
			p.Scopes = slices.DeleteFunc(slices.Clone(p.Scopes), func(s string) bool { return s == "admin" })
		}
	}
	p.AgentID = scopedName(p.Subject, name)
	p.key = Key(provider, p.Subject)
	return p, nil
}

// subjectOf is a provider's subject as the core keys it.
func subjectOf(prov Provider, subject string) string {
	if _, minted := prov.(Minting); minted {
		return subject
	}
	return prov.Name() + ":" + subject
}

// SubjectKey is Key for a subject as the named provider states it (a
// Revoker's): "" for a provider this chain does not have.
func (a *Authenticator) SubjectKey(provider, subject string) string {
	for _, p := range a.providers {
		if p.Name() == provider {
			return Key(provider, subjectOf(p, subject))
		}
	}
	return ""
}

func nameAllowed(patterns []string, name string) bool {
	for _, pat := range patterns {
		if pat == "*" || pat == name || (strings.HasSuffix(pat, "*") && strings.HasPrefix(name, strings.TrimSuffix(pat, "*"))) {
			return true
		}
	}
	return false
}

// scopedName is the agent id a connection with subject takes for name: the
// subject itself, or "<subject>.<name>", so names are unique per credential.
func scopedName(subject, name string) string {
	switch {
	case name == "":
		return subject
	case name == subject || strings.HasPrefix(name, subject+"."):
		return name
	default:
		return subject + "." + name
	}
}

func keyOf(c Credential) cacheKey {
	k := cacheKey{name: c.Name}
	if c.Token != "" {
		k.token = sha256.Sum256([]byte(c.Token))
	} else {
		k.token, k.cert = sha256.Sum256(c.PeerCerts[0].Raw), true
	}
	if len(c.Labels) > 0 {
		keys := make([]string, 0, len(c.Labels))
		for l := range c.Labels {
			keys = append(keys, l)
		}
		slices.Sort(keys)
		var b strings.Builder
		for _, l := range keys {
			b.WriteString(l)
			b.WriteByte('=')
			b.WriteString(c.Labels[l])
			b.WriteByte(0)
		}
		k.labels = b.String()
	}
	return k
}

// Hold holds a connection p authenticated until it ends: the Held is its
// context, which carries p (FromContext) and ends when the credential is
// revoked (ErrRevoked) or expires (ErrExpired, one timer per held
// connection, no polling). Defer its Release.
func (a *Authenticator) Hold(ctx context.Context, p Principal) *Held {
	return a.live.hold(ctx, p.Key(), p)
}

// Revoke ends the connections of the credential key names (Key) and drops
// its cached answers (and only its), so it is asked of its provider again.
func (a *Authenticator) Revoke(key string) int {
	a.cache.RemoveIf(func(_ cacheKey, p Principal) bool { return p.Key() == key })
	return a.live.Cut(key, ErrRevoked)
}

// Held returns how many connections the credential key names holds.
func (a *Authenticator) Held(key string) int { return a.live.Held(key) }

type principalKey struct{}

// WithPrincipal returns ctx carrying p.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext returns the principal ctx carries.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
