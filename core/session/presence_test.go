// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"strings"
	"testing"
	"time"

	presencev1 "github.com/nfltr/relay/proto/nfltr/presence/v1"
)

func TestPresenceFeedWatch(t *testing.T) {
	var f presenceFeed
	base := f.Watch(t.Context(), "", time.Second, nil)
	if base.GetCursor() == "" || len(base.GetEvents()) != 0 {
		t.Fatalf("baseline = %v, want a cursor and no events", base)
	}
	f.up("a.one", map[string]string{"k": "v"}, []string{"worker"})
	f.up("b.two", nil, nil)
	f.labels("a.one", map[string]string{"k": "w"})
	f.down("a.one")

	onlyA := func(id string) bool { return strings.HasPrefix(id, "a.") }
	got := f.Watch(t.Context(), base.GetCursor(), time.Second, onlyA)
	if len(got.GetEvents()) != 3 || got.GetReset_() {
		t.Fatalf("events after baseline (a.* only) = %v, want up, labels, down of a.one", got.GetEvents())
	}
	if got.GetEvents()[0].GetUp().GetLabels()["k"] != "v" || got.GetEvents()[1].GetLabels().GetLabels()["k"] != "w" || got.GetEvents()[2].GetDown() == nil {
		t.Fatalf("event payloads = %v", got.GetEvents())
	}
	// A current cursor waits for the next visible change, ignoring invisible ones.
	woke := make(chan *presencev1.WatchResponse, 1)
	go func() { woke <- f.Watch(t.Context(), got.GetCursor(), 5*time.Second, onlyA) }()
	time.Sleep(20 * time.Millisecond)
	f.up("b.three", nil, nil)
	f.up("a.four", nil, nil)
	select {
	case r := <-woke:
		if len(r.GetEvents()) != 1 || r.GetEvents()[0].GetAgentId() != "a.four" {
			t.Fatalf("woke with %v, want only a.four", r.GetEvents())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watch did not wake on a visible change")
	}
	// No change: answers empty at maxWait.
	if r := f.Watch(t.Context(), f.Cursor(), 10*time.Millisecond, nil); len(r.GetEvents()) != 0 || r.GetReset_() {
		t.Fatalf("idle watch = %v", r)
	}
	// Another process's cursor, or one older than the history: reset.
	if r := f.Watch(t.Context(), "other.1", time.Second, nil); !r.GetReset_() {
		t.Fatal("foreign cursor did not reset")
	}
	old := f.Cursor()
	for range presenceHistory + 1 {
		f.labels("c.x", nil)
	}
	if r := f.Watch(t.Context(), old, time.Second, nil); !r.GetReset_() || len(r.GetEvents()) != presenceHistory {
		t.Fatalf("watch past history: reset=%v events=%d, want reset and %d events", r.GetReset_(), len(r.GetEvents()), presenceHistory)
	}
}

func TestPresenceBusCarriesPeerChanges(t *testing.T) {
	wire := make(chan []byte, 4)
	var local, peer presenceFeed
	local.BindBus(t.Context(), fakePresenceBus{pub: wire, sub: make(chan []byte)})
	peer.BindBus(t.Context(), fakePresenceBus{pub: make(chan []byte, 4), sub: wire})
	base := peer.Cursor()
	local.up("a.one", map[string]string{"k": "v"}, nil)
	r := peer.Watch(t.Context(), base, 3*time.Second, nil)
	if len(r.GetEvents()) == 0 || r.GetEvents()[0].GetUp().GetLabels()["k"] != "v" {
		t.Fatalf("peer saw %v, want the up of a.one", r.GetEvents())
	}
}

// fakePresenceBus publishes to pub and delivers sub (a bus never echoes a
// replica's own payloads back to it).
type fakePresenceBus struct{ pub, sub chan []byte }

func (b fakePresenceBus) Publish(_ context.Context, p []byte) error { b.pub <- p; return nil }

func (b fakePresenceBus) Subscribe(context.Context) <-chan []byte { return b.sub }

// TestPresenceBusResyncResetsWatchers: the bus's resync signal (an empty
// payload: its subscription was re-established and may have missed peer
// changes) starts a new feed epoch, so a watcher's cursor answers reset
// and it re-lists: a waiting watcher at once, a later one at its next
// Watch.
func TestPresenceBusResyncResetsWatchers(t *testing.T) {
	sub := make(chan []byte)
	var f presenceFeed
	f.BindBus(t.Context(), fakePresenceBus{pub: make(chan []byte, 4), sub: sub})
	before := f.Cursor()
	waiting := make(chan *presencev1.WatchResponse, 1)
	go func() { waiting <- f.Watch(t.Context(), before, time.Minute, nil) }()
	sub <- []byte{}
	select {
	case r := <-waiting:
		if !r.GetReset_() {
			t.Fatalf("waiting watcher woke with %v, want reset", r)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("a waiting watcher did not wake on the bus's resync")
	}
	if r := f.Watch(t.Context(), before, time.Minute, nil); !r.GetReset_() || r.GetCursor() == before {
		t.Fatalf("watch with a cursor from before the resync = %v, want reset and a new cursor", r)
	}
}
