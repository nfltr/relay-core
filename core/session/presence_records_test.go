// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	presencev1 "github.com/nfltr/relay/proto/nfltr/presence/v1"
)

// recordsTestDispatcher holds agent-1's stream on this replica (10.0.0.1),
// session Seq seq, over a shared store whose record is rec.
func recordsTestDispatcher(t *testing.T, seq int64, rec *directory.Value, cache directory.Cache) (*Dispatcher, *GRPCStream, *fake.MockCache) {
	t.Helper()
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	d, cancel := newTestDispatcher(t)
	t.Cleanup(cancel)
	mc := newMockCache()
	key := directory.GetStreamerCacheKey("agent-1")
	if rec != nil {
		mc.Data[key] = rec
	}
	if cache == nil {
		cache = mc
	}
	inv, err := directory.NewIPInventory(fake.TestLogger(), cache, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	if err != nil {
		t.Fatal(err)
	}
	d.StreamerIPInventory = inv
	stream := NewGRPCStream("agent-1", nil, nil, nil, nil)
	stream.connectedAt, stream.seq, stream.credential, stream.account = time.Unix(100, 0), seq, "cred", "acct"
	stream.connectLabels = map[string]string{"host": "a"}
	stream.SetLabels(map[string]string{"host": "a"})
	d.Upstreams.Store("agent-1", stream)
	return d, stream, mc
}

// Finding 8 (spec/README.md, Presence.tla unfenced-label-write): a label
// update renews the stream's own session record, keeping its Seq,
// credential and account; from a session a newer one on another replica
// superseded it writes nothing and is refused.
func TestPresenceLabelUpdateKeepsTheSession(t *testing.T) {
	key := directory.GetStreamerCacheKey("agent-1")
	t.Run("current session", func(t *testing.T) {
		d, _, mc := recordsTestDispatcher(t, 3, &directory.Value{PodIP: "10.0.0.1", Seq: 3, Credential: "cred", Account: "acct"}, nil)
		if _, ok := d.UpdateAgentLabels("agent-1", serialOf(d, "agent-1"), map[string]string{"k": "v"}, true); !ok {
			t.Fatal("update refused")
		}
		got := mc.Data[key]
		if got.Seq != 3 || got.Credential != "cred" || got.Account != "acct" || got.Labels["k"] != "v" || got.PodIP != "10.0.0.1" {
			t.Fatalf("record = %+v, want session 3's with the new labels", got)
		}
	})
	t.Run("superseded session", func(t *testing.T) {
		newer := &directory.Value{PodIP: "10.0.0.2", Seq: 4, Credential: "cred", Account: "acct"}
		d, _, mc := recordsTestDispatcher(t, 3, newer, nil)
		if _, ok := d.UpdateAgentLabels("agent-1", serialOf(d, "agent-1"), map[string]string{"k": "v"}, true); ok {
			t.Fatal("a superseded session's update was accepted")
		}
		if mc.Data[key] != newer {
			t.Fatalf("record = %+v, want the newer session's, untouched", mc.Data[key])
		}
	})
}

// gatedCache holds the first record write until release is closed,
// signalling held when it starts waiting.
type gatedCache struct {
	*fake.MockCache
	held, release chan struct{}
	first         bool
}

func (g *gatedCache) Set(ctx context.Context, key string, value any, exp time.Duration) (string, error) {
	if _, ok := value.(directory.Value); ok && !g.first {
		g.first = true
		close(g.held)
		<-g.release
	}
	return g.MockCache.Set(ctx, key, value, exp)
}

// Finding 10 (Presence.tla renew-race): a renewal that read the stream's
// labels before an update never writes them over the update's: both write
// under the stream's record lock.
func TestPresenceRenewalNeverWritesReplacedLabels(t *testing.T) {
	gc := &gatedCache{MockCache: newMockCache(), held: make(chan struct{}), release: make(chan struct{})}
	gc.Data[directory.GetStreamerCacheKey("agent-1")] = &directory.Value{PodIP: "10.0.0.1", Seq: 3, Credential: "cred", Account: "acct"}
	d, stream, _ := recordsTestDispatcher(t, 3, nil, gc)
	renewed := make(chan error, 1)
	go func() {
		renewed <- d.refreshAgentInventoryOnInboundActivity(t.Context(), "agent-1", d.StreamerIPInventory, stream)
	}()
	<-gc.held // the renewal is writing the labels it read
	updated := make(chan struct{})
	go func() {
		d.UpdateAgentLabels("agent-1", serialOf(d, "agent-1"), map[string]string{"k": "new"}, true)
		close(updated)
	}()
	select { // the update has written (the bug), or waits on the lock
	case <-updated:
	case <-time.After(100 * time.Millisecond):
	}
	close(gc.release)
	if err := <-renewed; err != nil {
		t.Fatal(err)
	}
	<-updated
	if got := gc.Data[directory.GetStreamerCacheKey("agent-1")].Labels["k"]; got != "new" {
		t.Fatalf("record labels k=%q after the update, want new", got)
	}
}

func setCacheErr(mc *fake.MockCache, err error) {
	mc.Mu.Lock()
	mc.Err = err
	mc.Mu.Unlock()
}

// presenceSince returns the feed's events of change (up, down) after cursor.
func presenceSince(t *testing.T, d *Dispatcher, cursor, change string) []*presencev1.PresenceEvent {
	var out []*presencev1.PresenceEvent
	for _, ev := range d.Presence.Watch(t.Context(), cursor, 0, nil).GetEvents() {
		if presenceChangeName(ev) == change {
			out = append(out, ev)
		}
	}
	return out
}

// Finding 27 (spec/README.md, Presence.tla UpdateFail and Lose, mutant
// silent-rewrite): a record write that failed, or a record the store lost,
// is published by the renewal that writes it next. The other replicas
// listed the old labels, or no agent, and nothing else would wake their
// watchers when the record changed back.
func TestPresenceRecordRewriteIsPublished(t *testing.T) {
	key := directory.GetStreamerCacheKey("agent-1")
	current := func() *directory.Value {
		return &directory.Value{PodIP: "10.0.0.1", Seq: 3, Credential: "cred", Account: "acct"}
	}
	t.Run("failed label update", func(t *testing.T) {
		d, stream, mc := recordsTestDispatcher(t, 3, current(), nil)
		setCacheErr(mc, errors.New("store down"))
		if _, ok := d.UpdateAgentLabels("agent-1", serialOf(d, "agent-1"), map[string]string{"k": "v"}, true); !ok {
			t.Fatal("update refused")
		}
		if !d.RecordWriteFailed.Load() {
			t.Fatal("a failed label write asked for no rewrite (RecordWriteFailed)")
		}
		cursor := d.Presence.Cursor()
		setCacheErr(mc, nil)
		if err := d.refreshAgentInventoryOnInboundActivity(t.Context(), "agent-1", d.StreamerIPInventory, stream); err != nil {
			t.Fatal(err)
		}
		if got := mc.Data[key].Labels["k"]; got != "v" {
			t.Fatalf("record labels k=%q after the renewal, want v", got)
		}
		if ups := presenceSince(t, d, cursor, "up"); len(ups) != 1 || ups[0].GetUp().GetLabels()["k"] != "v" {
			t.Fatalf("published %v after the renewal wrote the labels, want one up with k=v", ups)
		}
	})
	t.Run("record lost", func(t *testing.T) {
		d, stream, mc := recordsTestDispatcher(t, 3, current(), nil)
		delete(mc.Data, key) // the store restarted empty
		cursor := d.Presence.Cursor()
		if err := d.rewriteRecord(t.Context(), "agent-1", d.StreamerIPInventory, stream); err != nil {
			t.Fatal(err)
		}
		if mc.Data[key] == nil {
			t.Fatal("record not written back")
		}
		if ups := presenceSince(t, d, cursor, "up"); len(ups) != 1 {
			t.Fatalf("published %v after the record was written back, want one up", ups)
		}
	})
}

// Finding 27 (Presence.tla EndFail, LateDown, mutant no-late-down): a
// release that failed leaves the record, listed by every replica until its
// TTL, whose expiry publishes nothing; a second down once the TTL passed
// wakes their watchers to re-list the agent gone.
func TestPresenceFailedReleaseDownsAgainAfterTheTTL(t *testing.T) {
	d, stream, mc := recordsTestDispatcher(t, 3, &directory.Value{PodIP: "10.0.0.1", Seq: 3, Credential: "cred", Account: "acct"}, nil)
	d.Config.HeartbeatRecvThreshold = 50 * time.Millisecond
	setCacheErr(mc, errors.New("store down"))
	cursor := d.Presence.Cursor()
	d.Upstreams.Delete("agent-1") // the teardown's DeregisterUpstreamAgent
	d.departed("agent-1", stream.Session(), stream)
	downs := 0
	for deadline := time.Now().Add(10 * time.Second); downs < 2 && time.Now().Before(deadline); {
		resp := d.Presence.Watch(t.Context(), cursor, time.Second, nil)
		for _, ev := range resp.GetEvents() {
			if ev.GetDown() != nil {
				downs++
			}
		}
		cursor = resp.GetCursor()
	}
	if downs != 2 {
		t.Fatalf("%d downs after a failed release, want 2 (now, and after the record's TTL)", downs)
	}
}

// Finding 29 with finding 27 (Presence.tla End, EndFail, LateDown): an
// HTTP-tunnel (ConnectHTTP) agent whose release fails downs again once the
// record's TTL passed, as a ConnectAgentTunnel or ConnectTCP one does.
func TestPresenceHTTPAgentFailedReleaseDownsAgainAfterTheTTL(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.Config.HeartbeatRecvThreshold = 500 * time.Millisecond
	mc := newMockCache()
	inv, err := directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	if err != nil {
		t.Fatal(err)
	}
	d.IPInventory = inv
	client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "agent-1"))
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	s, err := client.ConnectHTTP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Header(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return d.IsAgentConnected("agent-1") }, func() string { return "agent not registered" })
	cursor := d.Presence.Cursor()
	setCacheErr(mc, errors.New("store down"))
	stop() // the agent goes; its release fails
	downs := 0
	for deadline := time.Now().Add(10 * time.Second); downs < 2 && time.Now().Before(deadline); {
		resp := d.Presence.Watch(t.Context(), cursor, time.Second, nil)
		for _, ev := range resp.GetEvents() {
			if ev.GetDown() != nil {
				downs++
			}
		}
		cursor = resp.GetCursor()
	}
	if downs != 2 {
		t.Fatalf("%d downs after a failed release, want 2 (now, and after the record's TTL)", downs)
	}
}

// Review r2 F6: an older session's end on this replica publishes no down
// once a newer session of the agent is listed here. Its CompareAndDelete
// can precede the newer one's Store, so the feed got the newer up and
// then the older down for a connected agent, and the outbox folded the
// pair into a down for every peer: a peer's hub that applies ups
// (hubArrivals) missed the arrival.
func TestPresenceReplacedSessionPublishesNoDown(t *testing.T) {
	d, older, _ := recordsTestDispatcher(t, 3, &directory.Value{PodIP: "10.0.0.1", Seq: 4, Credential: "cred", Account: "acct"}, nil)
	newer := NewGRPCStream("agent-1", nil, nil, nil, nil)
	d.Upstreams.Store("agent-1", newer) // session 4 listed here
	cursor := d.Presence.Cursor()
	d.departed("agent-1", older.Session(), older)
	if downs := presenceSince(t, d, cursor, "down"); len(downs) != 0 {
		t.Fatalf("published %v for a session replaced here, want no down", downs)
	}
}
