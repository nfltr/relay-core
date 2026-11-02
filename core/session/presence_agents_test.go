// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"github.com/nfltr/relay-core/platform/util"
)

// awaitPresence waits for an event of change (up, down) for id after
// cursor and returns the cursor past it.
func awaitPresence(t *testing.T, d *Dispatcher, cursor, id, change string) string {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		resp := d.Presence.Watch(t.Context(), cursor, time.Second, nil)
		cursor = resp.GetCursor()
		for _, ev := range resp.GetEvents() {
			if ev.GetAgentId() == id && presenceChangeName(ev) == change {
				return cursor
			}
		}
	}
	t.Fatalf("no %s of %s on the presence feed", change, id)
	return ""
}

// Review r2 F4 (spec/README.md finding 29): an HTTP-tunnel agent and a
// TCP-only agent, which myAgents lists (this replica's, every replica's
// through the TCP agent's record), publish an up when they connect and a
// down when they leave. Before, neither moved the feed: a watcher's
// snapshot held them, or missed them, with nothing to wake it.
func TestPresenceHTTPAndTCPAgentsPublish(t *testing.T) {
	t.Run("http", func(t *testing.T) {
		d, cancel := newTestDispatcher(t)
		defer cancel()
		cursor := d.Presence.Cursor()
		client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "http-agent"))
		ctx, stop := context.WithCancel(ctx)
		defer stop()
		s, err := client.ConnectHTTP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Header(); err != nil {
			t.Fatal(err)
		}
		cursor = awaitPresence(t, d, cursor, "http-agent", "up")
		stop()
		awaitPresence(t, d, cursor, "http-agent", "down")
	})
	t.Run("tcp", func(t *testing.T) {
		d := &Dispatcher{
			TCPDispatcher:       NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil),
			StreamerIPInventory: directory.NewNoOpInventory(),
			Logger:              slog.New(slog.DiscardHandler),
			Upstreams:           &util.SyncMap[string, Streamer]{},
			Agents:              &util.SyncMap[string, *HTTPStream]{},
			Config:              DispatcherConfig{HeartbeatRecvThreshold: time.Minute},
		}
		cursor := d.Presence.Cursor()
		ctx, cancel := context.WithCancel(makeGRPCContext("tcp-agent"))
		defer cancel()
		st := &fake.EndableStream{End: make(chan struct{}), Ctx: ctx}
		done := make(chan error, 1)
		go func() { done <- d.ConnectTCP(st) }()
		cursor = awaitPresence(t, d, cursor, "tcp-agent", "up")
		cancel()
		close(st.End)
		<-done
		awaitPresence(t, d, cursor, "tcp-agent", "down")
	})
}
