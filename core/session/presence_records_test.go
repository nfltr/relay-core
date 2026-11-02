// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
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
		if _, ok := d.UpdateAgentLabels("agent-1", map[string]string{"k": "v"}, true); !ok {
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
		if _, ok := d.UpdateAgentLabels("agent-1", map[string]string{"k": "v"}, true); ok {
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
		d.UpdateAgentLabels("agent-1", map[string]string{"k": "new"}, true)
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
