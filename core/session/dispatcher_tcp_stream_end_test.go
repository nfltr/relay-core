// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// waitingCtx signals once its Done is first asked for: an open waiting
// for its agent link selects on it as it starts to wait.
type waitingCtx struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *waitingCtx) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

// closerFunc is an agent-link connection whose Close runs f.
type closerFunc struct{ f func() }

func (c *closerFunc) Close() error { c.f(); return nil }

// TestOpenWokenByItsStreamsEndIsRefused: an open waiting for its agent's
// link wakes when the agent's primary stream ends (primaryEnded) and then
// takes the frame streams. It must not be placed on the stream that is
// ending: it is refused as the agent offline (no other stream here), never
// accepted and then lost with the stream. Before, the stream took sessions
// until unregister's CloseAll, after primaryEnded had woken the waiters:
// the open was accepted onto the dead stream and its client got
// "connection lost" (TestOpensAfterAgentPathRestored of a relay, once
// on the Linux runner). The ordering is forced: primaryEnded closes the
// link's connection (closerFunc) only once the woken open returned.
func TestOpenWokenByItsStreamsEndIsRefused(t *testing.T) {
	l := newTestAgentLink(t)
	d := l.d
	d.Link = l
	as := NewTCPAgentStream()
	as.agent, as.token = "agent", agentLinkTestToken
	as.link.asked = dataplane.AgentLinkTCP
	as.link.dial, as.link.dialBy = make(chan struct{}), time.Now().Add(time.Hour) // a dial request out: opens wait for it
	d.bound[as.token] = as
	d.Streams["agent"] = as
	d.All["agent"] = []*tcpAgentStream{as}

	ctx := &waitingCtx{Context: t.Context(), waiting: make(chan struct{})}
	var s peerlink.Session
	var err error
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		s, _, err = d.onward(ctx, TCPEndpoint{AgentID: "agent", TargetAddr: "echo"})
	}()
	<-ctx.waiting

	l.Mu.Lock()
	l.Conns[as.token] = map[io.Closer]linkConn{&closerFunc{func() { <-returned }}: {Transport: dataplane.AgentLinkTCP}}
	l.Mu.Unlock()
	d.unregister("agent", as) // the stream ended
	<-returned

	if err == nil {
		_, rerr := s.Read(make([]byte, 1))
		_ = s.Close()
		t.Fatalf("an open woken by its stream's end was placed on that stream (then %v); want it refused as the agent offline", rerr)
	}
	if !errors.Is(&DialError{AgentID: "agent", Err: err}, ErrOffline) {
		t.Fatalf("open refused %v; want the agent offline", err)
	}
}
