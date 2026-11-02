// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	presencev1 "github.com/nfltr/relay/proto/nfltr/presence/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// gatedPresenceBus holds every Publish until open is closed, then records
// the payloads in the order published.
type gatedPresenceBus struct {
	open chan struct{}
	mu   sync.Mutex
	got  []*presencev1.PresenceEvent
}

func (b *gatedPresenceBus) Publish(ctx context.Context, p []byte) error {
	select {
	case <-b.open:
	case <-ctx.Done():
		return ctx.Err()
	}
	var ev presencev1.PresenceEvent
	if err := protojson.Unmarshal(p, &ev); err != nil {
		return err
	}
	b.mu.Lock()
	b.got = append(b.got, &ev)
	b.mu.Unlock()
	return nil
}

func (b *gatedPresenceBus) Subscribe(context.Context) <-chan []byte { return nil }

// Presence changes reach peers in each agent's order, and a slow bus
// holds a bounded number of goroutines whatever the rate of changes:
// changes not yet published fold into the agent's latest. Before, each
// change was published from a goroutine of its own: a flap storm on a
// slow bus held one goroutine per change for up to 5 s, and an agent's
// quick down then up could reach peers as up then down.
func TestPresenceFanoutOrderedAndBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	bus := &gatedPresenceBus{open: make(chan struct{})}
	var f presenceFeed
	f.BindBus(ctx, bus)
	before := runtime.NumGoroutine()
	const agents, flaps = 20, 50
	for range flaps {
		for a := range agents {
			id := fmt.Sprint("agent-", a)
			f.up(id, map[string]string{"v": "1"}, []string{"cap"})
			f.down(id)
		}
	}
	for a := range agents { // odd agents end up, with a label change after
		if a%2 == 1 {
			id := fmt.Sprint("agent-", a)
			f.up(id, map[string]string{"v": "1"}, []string{"cap"})
			f.labels(id, map[string]string{"v": "2"})
		}
	}
	if n := runtime.NumGoroutine() - before; n > 5 {
		t.Fatalf("%d goroutines for %d changes on a held bus, want a bounded few", n, agents*flaps*2)
	}
	close(bus.open)
	last := func() map[string]*presencev1.PresenceEvent {
		bus.mu.Lock()
		defer bus.mu.Unlock()
		m := map[string]*presencev1.PresenceEvent{}
		for _, ev := range bus.got {
			m[ev.GetAgentId()] = ev
		}
		return m
	}
	want := func(m map[string]*presencev1.PresenceEvent) error {
		for a := range agents {
			ev := m[fmt.Sprint("agent-", a)]
			switch {
			case ev == nil:
				return fmt.Errorf("agent-%d: nothing published", a)
			case a%2 == 0 && ev.GetDown() == nil:
				return fmt.Errorf("agent-%d: last published %v, want down", a, ev)
			case a%2 == 1 && (ev.GetUp() == nil || ev.GetUp().GetLabels()["v"] != "2" || len(ev.GetUp().GetCapabilities()) != 1):
				return fmt.Errorf("agent-%d: last published %v, want up with labels v=2 and its capability", a, ev)
			}
		}
		return nil
	}
	var err error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if err = want(last()); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
}

// flakyPresenceBus fails every Publish while failing is set, signalling
// each failure on failed; otherwise it records the payloads in order.
type flakyPresenceBus struct {
	failing atomic.Bool
	failed  chan struct{}
	mu      sync.Mutex
	got     []*presencev1.PresenceEvent
}

func (b *flakyPresenceBus) Publish(_ context.Context, p []byte) error {
	if b.failing.Load() {
		select {
		case b.failed <- struct{}{}:
		default:
		}
		return errors.New("publish failed")
	}
	var ev presencev1.PresenceEvent
	if err := protojson.Unmarshal(p, &ev); err != nil {
		return err
	}
	b.mu.Lock()
	b.got = append(b.got, &ev)
	b.mu.Unlock()
	return nil
}

func (b *flakyPresenceBus) Subscribe(context.Context) <-chan []byte { return nil }

// A change whose PUBLISH failed is sent again, folded under the agent's
// newer changes: the peers' subscriptions did not break, so no resync
// would ever tell their watchers (spec/README.md finding 25, Presence.tla
// publish-drop). Before, the outbox took the change before the send and
// ignored the error.
func TestPresenceOutboxRepublishesAFailedSend(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	bus := &flakyPresenceBus{failed: make(chan struct{}, 1)}
	bus.failing.Store(true)
	var f presenceFeed
	f.BindBus(ctx, bus)
	f.up("a", map[string]string{"v": "1"}, nil)
	<-bus.failed
	f.labels("a", map[string]string{"v": "2"}) // newer than the failed up
	f.up("b", nil, nil)
	<-bus.failed
	f.down("b") // a pending or failed up of b becomes its down
	bus.failing.Store(false)
	last := func() (map[string]*presencev1.PresenceEvent, int) {
		bus.mu.Lock()
		defer bus.mu.Unlock()
		m := map[string]*presencev1.PresenceEvent{}
		for _, ev := range bus.got {
			m[ev.GetAgentId()] = ev
		}
		return m, len(bus.got)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		m, n := last()
		a, b := m["a"], m["b"]
		aOK := a != nil && (a.GetUp() != nil && a.GetUp().GetLabels()["v"] == "2" || a.GetLabels().GetLabels()["v"] == "2")
		if aOK && b != nil && b.GetDown() != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("published %d events, last per agent %v: want a up with v=2 and b down", n, m)
		}
	}
}
