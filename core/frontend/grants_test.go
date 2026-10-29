// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package frontend

import (
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
