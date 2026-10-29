// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/viper"
	"google.golang.org/grpc/status"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/platform/util"
)

type countingInventory struct {
	directory.IPInventory
	sets atomic.Int32
}

func (c *countingInventory) SetIPWithAgentInfo(context.Context, string, map[string]string, []string, time.Duration) error {
	c.sets.Add(1)
	return nil
}

// A TCP agent's inventory record (how other replicas find it) is renewed
// while its stream lives, not written once and left to expire.
func TestConnectTCPRenewsInventoryRecord(t *testing.T) {
	inv := &countingInventory{IPInventory: directory.NewNoOpInventory()}
	d := &Dispatcher{
		TCPDispatcher:       NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil),
		StreamerIPInventory: inv,
		Logger:              slog.New(slog.DiscardHandler),
		Upstreams:           &util.SyncMap[string, Streamer]{},
		Config:              DispatcherConfig{HeartbeatRecvThreshold: 90 * time.Millisecond},
	}
	d.ConfigureTCPRenew(d.TCPDispatcher)
	ctx, cancel := context.WithCancel(makeGRPCContext("renew-agent"))
	st := &fake.EndableStream{End: make(chan struct{}), Ctx: ctx}
	done := make(chan error, 1)
	go func() { done <- d.ConnectTCP(st) }()
	waitUntil(t, func() bool { return inv.sets.Load() >= 4 },
		func() string { return "the record was not renewed while the stream lived" })
	cancel()
	close(st.End)
	<-done
}

// A TCP agent's old stream, on a replica a partition cut off, ends at its
// next renewal once the agent connected to another replica, instead of
// pointing the agent's record back at itself (EOF on visits for up to 25 s
// after the partition, kind soak 2026-10-10).
func TestConnectTCPOldStreamYieldsToNewerSession(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	mc := newMockCache()
	inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	d := &Dispatcher{
		TCPDispatcher:       NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil),
		StreamerIPInventory: inv,
		Logger:              slog.New(slog.DiscardHandler),
		Upstreams:           &util.SyncMap[string, Streamer]{},
		Agents:              &util.SyncMap[string, *HTTPStream]{},
		Config:              DispatcherConfig{HeartbeatRecvThreshold: 90 * time.Millisecond},
	}
	d.ConfigureTCPRenew(d.TCPDispatcher)
	st := &fake.EndableStream{End: make(chan struct{}), Ctx: makeGRPCContext("moved-agent")}
	defer close(st.End)
	done := make(chan error, 1)
	go func() { done <- d.ConnectTCP(st) }()
	key := directory.GetStreamerCacheKey("moved-agent")
	waitUntil(t, func() bool { v, _ := mc.Get(context.Background(), key); return v != nil },
		func() string { return "ConnectTCP wrote no record" })
	newer := &directory.Value{PodIP: "10.0.0.2", Since: time.Now().Add(time.Second).UnixNano()}
	_, _ = mc.Set(context.Background(), key, *newer, time.Minute) // the agent connected there
	select {
	case err := <-done:
		if status.Code(err) != status.Code(errReplacedSessionStatus) {
			t.Fatalf("old stream ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the old stream kept running after the agent moved")
	}
	if v, _ := mc.Get(context.Background(), key); v == nil || v.PodIP != "10.0.0.2" {
		t.Fatalf("the old stream overwrote the agent's new record: %+v", v)
	}
}

// A rebalancing move's old TCP stream stays once the agent's new session
// took the record: the agent closes it, with the sessions on it, when it
// leaves its old connection (kind soak 2026-10-21, rollout: ending it at
// the next renewal cut a TCP visit).
func TestConnectTCPMovingAgentKeepsOldStream(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	mc := newMockCache()
	inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	d := &Dispatcher{
		TCPDispatcher:       NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil),
		StreamerIPInventory: inv,
		Logger:              slog.New(slog.DiscardHandler),
		Upstreams:           &util.SyncMap[string, Streamer]{},
		Agents:              &util.SyncMap[string, *HTTPStream]{},
		Config:              DispatcherConfig{HeartbeatRecvThreshold: 90 * time.Millisecond},
	}
	d.ConfigureTCPRenew(d.TCPDispatcher)
	st := &fake.EndableStream{End: make(chan struct{}), Ctx: makeGRPCContext("moved-agent")}
	done := make(chan error, 1)
	go func() { done <- d.ConnectTCP(st) }()
	key := directory.GetStreamerCacheKey("moved-agent")
	waitUntil(t, func() bool { v, _ := mc.Get(context.Background(), key); return v != nil },
		func() string { return "ConnectTCP wrote no record" })
	d.tcpMovingAway("moved-agent", true) // the session watch started a rebalancing move
	newer := &directory.Value{PodIP: "10.0.0.2", Since: time.Now().Add(time.Second).UnixNano()}
	_, _ = mc.Set(context.Background(), key, *newer, time.Minute) // its move's new session
	// A renewal that read the record before that write can still land
	// after it (PutSession's get-then-set, a race it accepts: the newer
	// session's next renewal corrects it; Linux runner, 5e63e44). The new
	// session renews, as it does every half threshold; from then on the
	// old stream, flagged, must write nothing.
	time.Sleep(d.Config.HeartbeatRecvThreshold)
	_, _ = mc.Set(context.Background(), key, *newer, time.Minute)
	select {
	case err := <-done:
		t.Fatalf("a moved agent's old stream ended before the agent closed it: %v", err)
	case <-time.After(300 * time.Millisecond): // more renewals, its HTTP session long gone
	}
	if v, _ := mc.Get(context.Background(), key); v == nil || v.PodIP != "10.0.0.2" {
		t.Fatalf("the old stream overwrote the agent's new record: %+v", v)
	}
	close(st.End) // the agent leaves its old connection
	<-done
}
