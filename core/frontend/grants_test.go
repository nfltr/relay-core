// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/sharecode"
)

// TestServiceGrantLookup: the issuer's 200 is the grant (with the relay's
// header sent), its 404 no share, anything else the issuer's unavailability
// (not cached as no share by CachedGrants).
func TestServiceGrantLookup(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != sharecode.GrantPath || r.Header.Get(sharecode.RelayHeader) != "1" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"subject":"alice.web/app","policy":{"preset":"demo"}}`))
	}))
	defer srv.Close()
	lookup := ServiceGrantLookup(srv.Client(), srv.URL)
	if subject, p, err := lookup("x"); err != nil || subject != "alice.web/app" || p.Preset != "demo" {
		t.Fatalf("200: %q %+v %v", subject, p, err)
	}
	status = http.StatusNotFound
	if _, _, err := lookup("x"); !errors.Is(err, ErrNoGrant) {
		t.Fatalf("404: %v, want ErrNoGrant", err)
	}
	status = http.StatusServiceUnavailable
	if _, _, err := lookup("x"); err == nil || errors.Is(err, ErrNoGrant) {
		t.Fatalf("503: %v, want the issuer's error", err)
	}
}

// TestShareUsesReportsGrantedCodes: each code a CachedGrants granted reaches
// the issuer once per report (its latest reservation), at
// sharecode.FactsPath with the relay's header; nothing used, no report; a
// zero reservation reports nothing.
func TestShareUsesReportsGrantedCodes(t *testing.T) {
	got := make(chan sharecode.UsedReport, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != sharecode.FactsPath || r.Header.Get(sharecode.RelayHeader) != "1" {
			t.Errorf("report %s %s without the relay's header", r.Method, r.URL.Path)
		}
		var rep sharecode.UsedReport
		_ = json.NewDecoder(r.Body).Decode(&rep)
		got <- rep
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	reserve := time.Hour
	uses := &ShareUses{Send: ServiceShareUses(srv.Client(), srv.URL), Reserve: func() time.Duration { return reserve }, After: 20 * time.Millisecond, Log: func(string, ...any) {}}
	g := NewCachedGrants(func(string) (string, SharePolicy, error) { return "alice.web", SharePolicy{}, nil }, uses.Used)
	before := time.Now()
	for range 3 {
		if _, err := g.Grant("code-a"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.Grant("code-b"); err != nil {
		t.Fatal(err)
	}
	select {
	case rep := <-got:
		if len(rep.SharesUsed) != 2 {
			t.Fatalf("report %+v, want code-a and code-b once each", rep)
		}
		for _, u := range rep.SharesUsed {
			if u.Until.Before(before.Add(reserve)) || u.Until.After(time.Now().Add(reserve)) {
				t.Errorf("%s reserved until %v, want an hour from its use", u.Code, u.Until)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no report")
	}
	reserve = 0
	_, _ = g.Grant("code-a")
	select {
	case rep := <-got:
		t.Fatalf("a report with reservation off or nothing used: %+v", rep)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestShareUsesReportsAtOnceNearTheEnd (relay-server spec/Share.tla
// urgent, finding S2): a visit to a code whose end, as its grant read it, is
// within the flush interval is reported at once, so the issuer extends the
// code before it ends; a visit to a code far from its end waits for the
// flush.
func TestShareUsesReportsAtOnceNearTheEnd(t *testing.T) {
	got := make(chan sharecode.UsedReport, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rep sharecode.UsedReport
		_ = json.NewDecoder(r.Body).Decode(&rep)
		got <- rep
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	uses := &ShareUses{Send: ServiceShareUses(srv.Client(), srv.URL), Reserve: func() time.Duration { return time.Hour }, After: time.Hour, Log: func(string, ...any) {}}
	ends := map[string]time.Time{"far": time.Now().Add(2 * time.Hour), "near": time.Now().Add(time.Minute)}
	g := NewCachedGrants(func(code string) (string, SharePolicy, error) {
		return "alice.web", SharePolicy{ExpiresAt: ends[code].Format(time.RFC3339)}, nil
	}, nil)
	g.UsedEnd = uses.UsedEnding
	if _, err := g.Grant("far"); err != nil {
		t.Fatal(err)
	}
	select {
	case rep := <-got:
		t.Fatalf("a code far from its end reported before the flush: %+v", rep)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := g.Grant("near"); err != nil {
		t.Fatal(err)
	}
	select {
	case rep := <-got:
		if len(rep.SharesUsed) != 2 {
			t.Fatalf("report %+v, want both codes (the urgent report takes every use so far)", rep)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a code within the flush interval of its end was not reported at once")
	}
}

// TestShareUsesKeepsUsesAFailedSendDid (relay-server spec/Share.tla
// FailReport/Resend, finding 3): a report the issuer does not take keeps
// its uses, and the next flush, one After later, sends them again (merged
// with uses since, the later reservation kept); a use whose reservation has
// passed is not sent again.
func TestShareUsesKeepsUsesAFailedSendDid(t *testing.T) {
	got := make(chan []sharecode.Used, 4)
	fail := make(chan bool, 4)
	fail <- true
	uses := &ShareUses{Send: func(_ context.Context, used []sharecode.Used) error {
		got <- used
		select {
		case <-fail:
			return errors.New("issuer down")
		default:
			return nil
		}
	}, Reserve: func() time.Duration { return time.Hour }, After: 20 * time.Millisecond, Log: func(string, ...any) {}}
	uses.Used("a")
	recv := func() []sharecode.Used {
		t.Helper()
		select {
		case u := <-got:
			return u
		case <-time.After(5 * time.Second):
			t.Fatal("no report")
			return nil
		}
	}
	if u := recv(); len(u) != 1 || u[0].Code != "a" {
		t.Fatalf("first report %+v, want a", u)
	}
	u := recv() // the retry, with no use since
	if len(u) != 1 || u[0].Code != "a" {
		t.Fatalf("retry %+v, want a again", u)
	}
	select {
	case u := <-got:
		t.Fatalf("a report after the retry was taken: %+v", u)
	case <-time.After(100 * time.Millisecond):
	}
	// A kept use whose reservation passed while the issuer was down is
	// dropped (flushes by hand: no timer fires within the test).
	uses.After = time.Hour
	uses.Reserve = func() time.Duration { return 30 * time.Millisecond }
	fail <- true
	uses.Used("b")
	uses.Flush()
	if u := recv(); len(u) != 1 || u[0].Code != "b" {
		t.Fatalf("report %+v, want b", u)
	}
	time.Sleep(50 * time.Millisecond)
	uses.Flush()
	select {
	case u := <-got:
		t.Fatalf("a use past its reservation sent again: %+v", u)
	default:
	}
}

// TestCachedGrantsForgetAll: after the issuer's word (a revocation it did
// not name), every code is read again.
func TestCachedGrantsForgetAll(t *testing.T) {
	reads := 0
	g := NewCachedGrants(func(string) (string, SharePolicy, error) { reads++; return "alice.web", SharePolicy{}, nil }, nil)
	_, _ = g.Grant("a")
	_, _ = g.Grant("a")
	g.ForgetAll()
	_, _ = g.Grant("a")
	if reads != 2 {
		t.Fatalf("%d reads, want 2 (cached, then read again after ForgetAll)", reads)
	}
}

// TestCachedGrantsForgetID: a revocation names the share's id, never its
// code (lane RSPOST); the id's grant is read again, another code's stays
// cached.
func TestCachedGrantsForgetID(t *testing.T) {
	reads := map[string]int{}
	g := NewCachedGrants(func(code string) (string, SharePolicy, error) { reads[code]++; return "alice.web", SharePolicy{}, nil }, nil)
	for _, c := range []string{"a", "b", "a", "b"} {
		_, _ = g.Grant(c)
	}
	g.ForgetID(sharecode.ID("a"))
	_, _ = g.Grant("a")
	_, _ = g.Grant("b")
	if reads["a"] != 2 || reads["b"] != 1 {
		t.Fatalf("reads %v, want a read again after ForgetID, b still cached", reads)
	}
}

// TestCachedGrantsForgetDuringLookup: a revocation applied while a lookup
// of the code is in flight is not undone by that lookup caching what it
// read before the revoke (the cached grant would outlive the revocation by
// up to ShareReadTTL on the replica that applied it).
func TestCachedGrantsForgetDuringLookup(t *testing.T) {
	reads := 0
	var g *CachedGrants
	g = NewCachedGrants(func(code string) (string, SharePolicy, error) {
		reads++
		if reads == 1 {
			g.ForgetID(sharecode.ID(code)) // the revocation lands mid-lookup
		}
		return "alice.web", SharePolicy{}, nil
	}, nil)
	_, _ = g.Grant("a")
	_, _ = g.Grant("a")
	if reads != 2 {
		t.Fatalf("%d reads, want 2: a grant read before a revocation was cached after it", reads)
	}
}
