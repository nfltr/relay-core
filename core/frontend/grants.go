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
	// UsedEnd, when set, hears every grant in place of used, with the
	// code's end the grant read (zero: none): ShareUses.UsedEnding.
	UsedEnd func(code string, end time.Time)
	Now     func() time.Time // the clock (tests)
	mu      sync.Mutex
	hits    map[string]cachedGrant // code -> grant
	miss    map[string]time.Time   // code -> until
	gen     uint64                 // forgets so far: a lookup that saw one start caches no grant
}

type cachedGrant struct {
	grant Grant
	id    string // sharecode.ID of the code (ForgetID)
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
	switch {
	case err != nil:
	case r.UsedEnd != nil:
		end, _ := time.Parse(time.RFC3339, g.Policy.ExpiresAt)
		r.UsedEnd(code, end)
	case r.used != nil:
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
	gen := r.gen
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
	id := sharecode.ID(code)
	r.remember(func() {
		// A forget since the lookup started may be this code's revocation,
		// read after the store answered: caching the answer would undo it.
		// ponytail: any forget skips caching every lookup in flight (each
		// read once more), not only its own code's.
		if r.gen == gen {
			r.hits[code] = cachedGrant{grant: g, id: id, until: until}
		}
	})
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

// ForgetID drops the cached grant of the code whose share id
// (sharecode.ID) is id: the issuer revoked it. A revocation names the id,
// never the code (a secret). A cached miss is kept: it grants nothing.
func (r *CachedGrants) ForgetID(id string) {
	r.mu.Lock()
	r.gen++
	for code, e := range r.hits {
		if e.id == id {
			delete(r.hits, code)
		}
	}
	r.mu.Unlock()
}

// ForgetAll drops every cached answer: the issuer revoked a code it did not
// name (its journal word, sharecode.JournalPath). ponytail: every live share
// is read once more after each revocation; name the codes when revocations
// are frequent.
func (r *CachedGrants) ForgetAll() {
	r.mu.Lock()
	r.gen++
	clear(r.hits)
	clear(r.miss)
	r.mu.Unlock()
}

// ShareUses reports the share codes a relay granted (Used, a CachedGrants'
// used hook) to their issuer (Send), in one report per After, so it keeps
// each reserved for its agent for Reserve after its last use. A report the
// issuer does not take is logged and its uses kept for the next flush, one
// After later, until their reservation passes (relay-server spec/Share.tla
// FailReport, finding 3; a lost report shortens a reservation, never grants
// access). No goroutine while nothing is used.
type ShareUses struct {
	Send    func(ctx context.Context, used []sharecode.Used) error
	Reserve func() time.Duration // how long a use reserves its code; <= 0: no reports
	After   time.Duration        // how long uses accumulate
	Log     func(msg string, args ...any)

	mu    sync.Mutex
	codes map[string]time.Time // code -> reserved until
	soon  bool                 // a Flush started for an urgent use has not taken codes yet
}

// Used records code granted now; its report waits for the flush.
func (u *ShareUses) Used(code string) { u.UsedEnding(code, time.Time{}) }

// UsedEnding records code granted now by a grant that read the code's end
// (zero: none). An end within After sends the report at once, so the issuer
// extends the code before it ends (relay-server spec/Share.tla urgent,
// finding S2; a visit within the send's latency of the end stays lost).
// ponytail: one send per urgent visit while a send is in flight; a code
// within After of its end is visited at that rate only until the issuer's
// extension reaches this relay's cache (ShareReadTTL).
func (u *ShareUses) UsedEnding(code string, end time.Time) {
	ttl := u.Reserve()
	if ttl <= 0 {
		return
	}
	now := time.Now()
	u.mu.Lock()
	if len(u.codes) == 0 {
		u.codes = map[string]time.Time{}
		time.AfterFunc(u.After, u.Flush)
	}
	u.codes[code] = now.Add(ttl)
	urgent := !end.IsZero() && end.Sub(now) <= u.After && !u.soon
	u.soon = u.soon || urgent
	u.mu.Unlock()
	if urgent {
		go u.Flush()
	}
}

// Flush reports the uses not yet reported, now; a failed report's uses are
// kept for the next flush.
func (u *ShareUses) Flush() {
	u.mu.Lock()
	codes := u.codes
	u.codes = nil
	u.soon = false
	u.mu.Unlock()
	now := time.Now()
	used := make([]sharecode.Used, 0, len(codes))
	for code, until := range codes {
		if until.After(now) { // a kept use past its reservation reserves nothing
			used = append(used, sharecode.Used{Code: code, Until: until})
		}
	}
	if len(used) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := u.Send(ctx, used); err != nil {
		u.Log("shares: use not reported to the issuer, kept for the next report", "shares", len(used), "error", err)
		u.keep(used)
	}
}

// keep puts a failed report's uses back, a later use's reservation winning,
// and arms the next flush if no use since has.
func (u *ShareUses) keep(used []sharecode.Used) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.codes) == 0 {
		u.codes = map[string]time.Time{}
		time.AfterFunc(u.After, u.Flush)
	}
	for _, x := range used {
		if until, ok := u.codes[x.Code]; !ok || x.Until.After(until) {
			u.codes[x.Code] = x.Until
		}
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
