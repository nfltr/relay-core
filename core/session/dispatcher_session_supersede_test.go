// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/client/rpcclient"
)

// An agent cut off from its replica (a partition, a replica that died
// silently) redials another replica while the first still holds its old
// session. Before: the new replica refused it ("already connected to
// another pod") until the old record expired, and the old replica's renewal
// kept pointing the agent back at its dead stream until its transport
// keepalive noticed (kind soak 2026-10-10: visits failed for 62 s after a
// 30 s partition). Now the agent's own credential admits it at once, and
// the old session ends at its next renewal without overwriting the record.
func TestAgentSessionMovesBetweenReplicas(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_POD_IP", "10.0.0.1") // this replica; the other is 10.0.0.2
	t0 := time.Date(2026, 10, 10, 1, 0, 0, 0, time.UTC)
	key := directory.GetDispatcherCacheKey("agent-1")
	owner := sessionCredential(hashAPIKey("the agent's key"))

	t.Run("admitted while the replica it left holds its record", func(t *testing.T) {
		d, cancel := newTestDispatcher(t)
		defer cancel()
		mc := newMockCache()
		mc.Data[key] = &directory.Value{PodIP: "10.0.0.2", Since: t0.UnixNano(), Credential: owner}
		inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
		if _, err := d.validateAgentConnection(context.Background(), "agent-1", owner, inv); err != nil {
			t.Fatalf("the agent's own reconnect was refused: %v", err)
		}
		for _, other := range []string{"", sessionCredential(hashAPIKey("another key"))} {
			if _, err := d.validateAgentConnection(context.Background(), "agent-1", other, inv); status.Code(err) != codes.AlreadyExists {
				t.Fatalf("owner %q took over another credential's agent: %v", other, err)
			}
		}
	})

	t.Run("the old session yields at its renewal", func(t *testing.T) {
		d, cancel := newTestDispatcher(t)
		defer cancel()
		mc := newMockCache()
		newer := &directory.Value{PodIP: "10.0.0.2", Since: t0.Add(time.Minute).UnixNano(), Credential: owner}
		mc.Data[key] = newer
		inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		old := NewHTTPStream("agent-1", nil, t.Context(), make(chan proto.Message), ticker)
		old.connectedAt, old.credential = t0, owner

		done := make(chan error, 1)
		go func() { done <- d.WatchAgentSession(t.Context(), "agent-1", old, inv, nil) }()
		select {
		case err := <-done:
			if !errors.Is(err, errSessionReplaced) {
				t.Fatalf("old session ended with %v, want errSessionReplaced", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the old session kept running after the agent moved")
		}
		if mc.Data[key] != newer {
			t.Fatalf("the old session overwrote the agent's new record: %+v", mc.Data[key])
		}
	})

	t.Run("the relay's own agent runs on every replica", func(t *testing.T) {
		d, cancel := newTestDispatcher(t)
		defer cancel()
		mc := newMockCache()
		relayKey := directory.GetStreamerCacheKey(rpcclient.RelayAgentID)
		mc.Data[relayKey] = &directory.Value{PodIP: "10.0.0.2", Since: t0.Add(time.Minute).UnixNano()}
		inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
		own := directory.Session{Since: t0}
		if err := d.refreshAgentInventoryOnInboundActivity(t.Context(), rpcclient.RelayAgentID, inv, own, nil, nil); err != nil {
			t.Fatalf("one replica's relay agent superseded another's: %v", err)
		}
	})

	t.Run("the current session keeps renewing", func(t *testing.T) {
		d, cancel := newTestDispatcher(t)
		defer cancel()
		mc := newMockCache()
		mc.Data[key] = &directory.Value{PodIP: "10.0.0.2", Since: t0.Add(-time.Minute).UnixNano(), Credential: owner} // a stale one
		inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		cur := NewHTTPStream("agent-1", nil, t.Context(), make(chan proto.Message), ticker)
		cur.connectedAt, cur.credential = t0, owner
		ctx, stop := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer stop()
		if err := d.WatchAgentSession(ctx, "agent-1", cur, inv, nil); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("current session ended with %v", err)
		}
		if got := mc.Data[key]; got.PodIP != "10.0.0.1" || got.Since != t0.UnixNano() {
			t.Fatalf("record = %+v, want the current session here", got)
		}
	})
}

// Replica clocks 5 minutes apart do not decide which of an agent's sessions
// is current: each claim takes the agent's next Seq from the shared store,
// and the stream renews with it. Before: the newer session on the replica
// whose clock was behind yielded to the older one, and the older session on
// the replica whose clock was ahead took the record back.
func TestAgentSessionOrderIgnoresClockSkew(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_POD_IP", "10.0.0.1") // this replica; the other is 10.0.0.2
	t0 := time.Date(2026, 10, 10, 1, 0, 0, 0, time.UTC)
	skew := 5 * time.Minute
	key := directory.GetDispatcherCacheKey("agent-1")
	owner := sessionCredential(hashAPIKey("the agent's key"))
	watch := func(t *testing.T, record *directory.Value, mine directory.Session) (*directory.Value, error) {
		t.Helper()
		d, cancel := newTestDispatcher(t)
		defer cancel()
		mc := newMockCache()
		mc.Data[key] = record
		inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		s := NewHTTPStream("agent-1", nil, t.Context(), make(chan proto.Message), ticker)
		s.connectedAt, s.seq, s.credential = mine.Since, mine.Seq, mine.Credential
		ctx, stop := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer stop()
		err := d.WatchAgentSession(ctx, "agent-1", s, inv, nil)
		return mc.Data[key], err
	}

	t.Run("the newer session's replica runs behind", func(t *testing.T) {
		newer := &directory.Value{PodIP: "10.0.0.2", Since: t0.Add(-skew).UnixNano(), Seq: 2, Credential: owner}
		got, err := watch(t, newer, directory.Session{Since: t0, Seq: 1, Credential: owner})
		if !errors.Is(err, errSessionReplaced) || got != newer {
			t.Fatalf("old session: %v, record %+v; want errSessionReplaced and the newer record kept", err, got)
		}
	})

	t.Run("the older session's replica runs ahead", func(t *testing.T) {
		older := &directory.Value{PodIP: "10.0.0.2", Since: t0.Add(skew).UnixNano(), Seq: 1, Credential: owner}
		got, err := watch(t, older, directory.Session{Since: t0, Seq: 2, Credential: owner})
		if !errors.Is(err, context.DeadlineExceeded) || got.PodIP != "10.0.0.1" || got.Seq != 2 {
			t.Fatalf("current session: %v, record %+v; want it renewing its record here", err, got)
		}
	})

	t.Run("claims take increasing seqs whatever the clocks", func(t *testing.T) {
		mc := newMockCache()
		var seqs []int64
		for _, pod := range []string{"10.0.0.2", "10.0.0.1"} {
			t.Setenv("TEST_POD_IP", pod)
			inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
			s, err := claimAgentRecord(t.Context(), inv, "agent-1", directory.Session{Since: t0.Add(-skew * time.Duration(len(seqs))), Credential: owner}, nil, nil, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			seqs = append(seqs, s.Seq)
		}
		if seqs[0] != 1 || seqs[1] != 2 || mc.Data[key].Seq != 2 {
			t.Fatalf("seqs = %v, record %+v; want 1, 2 and the record naming 2", seqs, mc.Data[key])
		}
	})
}
