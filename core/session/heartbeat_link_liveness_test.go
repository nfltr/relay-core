// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"google.golang.org/protobuf/proto"
)

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// While an agent's link is up, its liveness is the agent's: the relay
// does not end its ConnectHTTP session for unanswered heartbeats, and
// writes no last-seen per heartbeat (it does at the disconnect). Once the
// link is down (the gRPC fallback), the heartbeats are required again.
// Before, a session without heartbeats ended errNoHeartbeat after the
// receive threshold, link or not.
func TestHeartbeatSilenceDoesNotEndAnAgentOnItsLink(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	l := newTestAgentLink(t)
	p := l.d.bound[agentLinkTestToken]
	p.link.asked = dataplane.AgentLinkTCP
	l.d.Link = l
	l.d.Streams["agent"] = p
	l.Conns[agentLinkTestToken] = map[io.Closer]linkConn{nopCloser{}: {Transport: dataplane.AgentLinkTCP}}
	d.TCPDispatcher = l.d
	d.Config.HeartbeatEnabled = true
	d.Config.HeartbeatRecvThreshold = 100 * time.Millisecond
	notified := 0
	d.OnAgentHeartbeat = func(string) { notified++ }

	ticker := time.NewTicker(d.Config.HeartbeatRecvThreshold)
	defer ticker.Stop()
	tracker := &mockHeartbeatTracker{agentID: "agent", heartbeats: make(chan proto.Message), ticker: ticker}
	inv := directory.NewNoOpInventory()

	ctx, stop := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer stop()
	if err := d.WatchAgentSession(ctx, "agent", tracker, inv, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("session with its link up ended %v; want it kept past the heartbeat threshold", err)
	}
	d.NotifyAgentHeartbeat("agent")
	if notified != 0 {
		t.Fatal("a heartbeat wrote last-seen for an agent on its link")
	}

	clear(l.Conns) // the link went down: heartbeats are required again
	ctx, stop = context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if err := d.WatchAgentSession(ctx, "agent", tracker, inv, nil); !errors.Is(err, errNoHeartbeat) {
		t.Fatalf("session without its link ended %v; want errNoHeartbeat", err)
	}
	d.NotifyAgentHeartbeat("agent")
	if notified != 1 {
		t.Fatal("a heartbeat of an agent without its link wrote no last-seen")
	}
}

// An agent whose link goes down gets a whole receive threshold to answer
// the relay's heartbeats again: until then the link vouched for it. The
// agent answers the next heartbeat once its link is down (a heartbeat
// interval later); before the fix a receive-threshold tick that came
// within that interval of the link's end ended a live agent
// errNoHeartbeat: about one link end in nine with the defaults (5s
// heartbeats, a 45s threshold), as when the relay retires an idle link.
func TestHeartbeatThresholdRestartsWhenTheLinkGoesDown(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	l := newTestAgentLink(t)
	p := l.d.bound[agentLinkTestToken]
	p.link.asked = dataplane.AgentLinkTCP
	l.d.Link = l
	l.d.Streams["agent"] = p
	l.Conns[agentLinkTestToken] = map[io.Closer]linkConn{nopCloser{}: {Transport: dataplane.AgentLinkTCP}}
	d.TCPDispatcher = l.d
	d.Config.HeartbeatEnabled = true
	const threshold = time.Second
	d.Config.HeartbeatRecvThreshold = threshold

	ticker := time.NewTicker(threshold)
	defer ticker.Stop()
	tracker := &mockHeartbeatTracker{agentID: "agent", heartbeats: make(chan proto.Message), ticker: ticker}
	start := time.Now()
	time.AfterFunc(threshold*3/2, func() { // just after a tick the link vouched at
		l.Mu.Lock()
		clear(l.Conns)
		l.Mu.Unlock()
	})
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	err := d.WatchAgentSession(ctx, "agent", tracker, directory.NewNoOpInventory(), nil)
	if !errors.Is(err, errNoHeartbeat) {
		t.Fatalf("session ended %v; want errNoHeartbeat once the agent answered none for a threshold after its link went down", err)
	}
	if took := time.Since(start); took < threshold*5/2 {
		t.Fatalf("session ended %v after its start, %v after its link went down: want a whole threshold (%v) after", took, took-threshold*3/2, threshold)
	}
}
