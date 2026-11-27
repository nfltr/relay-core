// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"github.com/nfltr/relay-core/platform/util"
)

// sessionInventory is the shared inventory (directory.NewIPInventory).
type sessionInventory interface {
	directory.IPInventory
	directory.SessionStore
	DeleteSession(context.Context, string, directory.Session) error
}

// claimGate holds the second ClaimSession, after its write, until
// release closes: the new stream has claimed the record, not registered.
type claimGate struct {
	sessionInventory
	claims  int
	claimed chan struct{}
	release chan struct{}
}

func (g *claimGate) ClaimSession(ctx context.Context, agentID string, s directory.Session, labels map[string]string, capabilities []string, ttl time.Duration) (directory.Session, error) {
	s, err := g.sessionInventory.ClaimSession(ctx, agentID, s, labels, capabilities, ttl)
	if g.claims++; g.claims == 2 {
		close(g.claimed)
		<-g.release
	}
	return s, err
}

// TestConnectTCPReconnectKeepsNewSessionRecord: an agent reconnects to the
// same replica; its old stream ends after the new one claimed the record
// and before it registered. The old stream's deferred delete leaves the
// new session's record (relay-server spec/README.md finding 1: other
// replicas answered "agent offline" until the new session's renewal).
func TestConnectTCPReconnectKeepsNewSessionRecord(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	mc := directory.NewMemoryCache()
	inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	gate := &claimGate{sessionInventory: inv.(sessionInventory), claimed: make(chan struct{}), release: make(chan struct{})}
	d := &Dispatcher{
		TCPDispatcher:       NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil),
		StreamerIPInventory: gate,
		Logger:              slog.New(slog.DiscardHandler),
		Upstreams:           &util.SyncMap[string, Streamer]{},
		Agents:              &util.SyncMap[string, *HTTPStream]{},
		Config:              DispatcherConfig{HeartbeatRecvThreshold: time.Minute}, // no renewal during the test
	}
	d.ConfigureTCPRenew(d.TCPDispatcher)
	const agent = "reconnecting-agent"
	connect := func() (*fake.EndableStream, chan error) {
		st := &fake.EndableStream{End: make(chan struct{}), Ctx: makeGRPCContext(agent)}
		done := make(chan error, 1)
		go func() { done <- d.ConnectTCP(st) }()
		return st, done
	}
	old, oldDone := connect()
	waitUntil(t, func() bool { return d.TCPDispatcher.agentStream(agent) != nil },
		func() string { return "the old stream did not register" })

	cur, curDone := connect()
	<-gate.claimed // the new session's record is written, its stream not registered
	close(old.End)
	<-oldDone // its deferred delete ran
	close(gate.release)
	waitUntil(t, func() bool { return d.TCPDispatcher.agentStream(agent) != nil },
		func() string { return "the new stream did not register" })

	v, _ := mc.Get(context.Background(), directory.GetStreamerCacheKey(agent))
	if v == nil || v.Seq != 2 {
		t.Errorf("the agent's record after the old stream ended: %+v, want the new session's (Seq 2)", v)
	}
	close(cur.End)
	<-curDone
}
