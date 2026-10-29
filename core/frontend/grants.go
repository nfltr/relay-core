// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package frontend

// Shares the relay verifies but does not issue: the issuer (the accounts
// service) writes share codes, and a relay reads one code at a time through
// a lookup, cached here (CachedGrants). nfltr-relay reads the service's
// database (the relay package's shareReader); the core-only relay asks the
// service over its agent session (ServiceGrantLookup, sharecode.GrantPath).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/nfltr/relay-core/core/sharecode"
)

// Bounds of CachedGrants: a granted code is read again after ShareReadTTL
// (or its expiry, if sooner), an unknown one after ShareMissTTL, so a burst
// of visits to one share is one lookup.
const (
	ShareReadTTL = time.Minute
	ShareMissTTL = 5 * time.Second
	ShareReadMax = 4096
)

// ShareLookup answers one share code: the subject it was issued for
// (sharecode.SplitSubject) and its policy; ErrNoGrant for no such share,
// any other error when the issuer could not answer.
type ShareLookup func(code string) (subject string, policy SharePolicy, err error)

// CachedGrants is the GrantVerifier over an issuer's share codes, read-only:
// lookups cached, each grant reported to used (the issuer keeps the code
// reserved for its agent).
type CachedGrants struct {
	lookup ShareLookup
	used   func(code string) // a code granted; nil: none
	Now    func() time.Time  // the clock (tests)
	mu     sync.Mutex
	hits   map[string]cachedGrant // code -> grant
	miss   map[string]time.Time   // code -> until
}

type cachedGrant struct {
	grant Grant
	until time.Time
}

// NewCachedGrants caches lookup; used, when set, hears every grant.
func NewCachedGrants(lookup ShareLookup, used func(string)) *CachedGrants {
	return &CachedGrants{lookup: lookup, used: used, Now: time.Now, hits: map[string]cachedGrant{}, miss: map[string]time.Time{}}
}

// IsCode reports whether s has the shape of a share code.
func (r *CachedGrants) IsCode(s string) bool { return sharecode.Valid(s) }

// Grant answers code from the cache or the lookup: ErrNoGrant for no share,
// the lookup's error (not cached) when it could not answer.
func (r *CachedGrants) Grant(code string) (Grant, error) {
	g, err := r.grant(code, r.Now())
	if err == nil && r.used != nil {
		r.used(code)
	}
	return g, err
}

func (r *CachedGrants) grant(code string, now time.Time) (Grant, error) {
	r.mu.Lock()
	if e, ok := r.hits[code]; ok && now.Before(e.until) {
		r.mu.Unlock()
		return e.grant, nil
	}
	if until, ok := r.miss[code]; ok && now.Before(until) {
		r.mu.Unlock()
		return Grant{}, ErrNoGrant
	}
	r.mu.Unlock()

	subject, policy, err := r.lookup(code)
	if errors.Is(err, ErrNoGrant) {
		r.remember(func() { r.miss[code] = now.Add(ShareMissTTL) })
		return Grant{}, ErrNoGrant
	}
	if err != nil {
		return Grant{}, err
	}
	agentID, service := sharecode.SplitSubject(subject)
	g := Grant{AgentID: agentID, Service: service, Policy: policy}
	until := now.Add(ShareReadTTL)
	if exp, perr := time.Parse(time.RFC3339, policy.ExpiresAt); perr == nil && exp.Before(until) {
		until = exp
	}
	r.remember(func() { r.hits[code] = cachedGrant{grant: g, until: until} })
	return g, nil
}

// remember records one answer. Misses and grants are bounded apart, so
// unknown codes (a scanner's) never push live grants out. ponytail: a full
// map is emptied, not evicted entry by entry: grants fill it only with
// ShareReadMax live shares visited within ShareReadTTL, then each is read
// once more; an LRU when that many shares are live.
func (r *CachedGrants) remember(put func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.hits) >= ShareReadMax {
		clear(r.hits)
	}
	if len(r.miss) >= ShareReadMax {
		clear(r.miss)
	}
	put()
}

// Forget drops code's cached answer (the issuer revoked it).
func (r *CachedGrants) Forget(code string) {
	r.mu.Lock()
	delete(r.hits, code)
	delete(r.miss, code)
	r.mu.Unlock()
}

// ForgetAll drops every cached answer: the issuer revoked a code it did not
// name (its journal word, sharecode.JournalPath). ponytail: every live share
// is read once more after each revocation; name the codes when revocations
// are frequent.
func (r *CachedGrants) ForgetAll() {
	r.mu.Lock()
	clear(r.hits)
	clear(r.miss)
	r.mu.Unlock()
}

// ShareUses reports the share codes a relay granted (Used, a CachedGrants'
// used hook) to their issuer (Send), in one report per After, so it keeps
// each reserved for its agent for Reserve after its last use. Best effort:
// a report the issuer does not take is logged and dropped (a lost report
// shortens a reservation, never grants access). No goroutine while nothing
// is used.
type ShareUses struct {
	Send    func(ctx context.Context, used []sharecode.Used) error
	Reserve func() time.Duration // how long a use reserves its code; <= 0: no reports
	After   time.Duration        // how long uses accumulate
	Log     func(msg string, args ...any)

	mu    sync.Mutex
	codes map[string]time.Time // code -> reserved until
}

// Used records code granted now.
func (u *ShareUses) Used(code string) {
	ttl := u.Reserve()
	if ttl <= 0 {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.codes) == 0 {
		u.codes = map[string]time.Time{}
		time.AfterFunc(u.After, u.Flush)
	}
	u.codes[code] = time.Now().Add(ttl)
}

// Flush reports the uses not yet reported, now.
func (u *ShareUses) Flush() {
	u.mu.Lock()
	codes := u.codes
	u.codes = nil
	u.mu.Unlock()
	if len(codes) == 0 {
		return
	}
	used := make([]sharecode.Used, 0, len(codes))
	for code, until := range codes {
		used = append(used, sharecode.Used{Code: code, Until: until})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := u.Send(ctx, used); err != nil {
		u.Log("shares: use not reported to the issuer", "shares", len(used), "error", err)
	}
}

// ServiceShareUses sends share uses to sharecode.FactsPath of the issuer
// base (e.g. "http://accounts.service") through client.
func ServiceShareUses(client *http.Client, base string) func(context.Context, []sharecode.Used) error {
	return func(ctx context.Context, used []sharecode.Used) error {
		body, err := json.Marshal(sharecode.UsedReport{SharesUsed: used})
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+sharecode.FactsPath, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(sharecode.RelayHeader, "1")
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusNoContent {
			return fmt.Errorf("share issuer answered %s", res.Status)
		}
		return nil
	}
}

// ServiceGrantLookup reads share codes at sharecode.GrantPath of the issuer
// base (e.g. "http://accounts.service") through client.
func ServiceGrantLookup(client *http.Client, base string) ShareLookup {
	return func(code string) (string, SharePolicy, error) {
		req, err := http.NewRequest(http.MethodGet, base+sharecode.GrantPath+"?code="+url.QueryEscape(code), nil)
		if err != nil {
			return "", SharePolicy{}, err
		}
		req.Header.Set(sharecode.RelayHeader, "1")
		res, err := client.Do(req)
		if err != nil {
			return "", SharePolicy{}, fmt.Errorf("share issuer: %w", err)
		}
		defer res.Body.Close() //nolint:errcheck
		switch res.StatusCode {
		case http.StatusOK:
		case http.StatusNotFound:
			return "", SharePolicy{}, ErrNoGrant
		default:
			return "", SharePolicy{}, fmt.Errorf("share issuer answered %s", res.Status)
		}
		var a sharecode.Grant
		var p SharePolicy
		if err := json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&a); err != nil || a.Subject == "" || json.Unmarshal(a.Policy, &p) != nil {
			return "", SharePolicy{}, fmt.Errorf("share issuer: no grant in its answer (%v)", err)
		}
		return a.Subject, p, nil
	}
}
