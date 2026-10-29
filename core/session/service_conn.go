// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/nfltr/link/peerlink"

	"github.com/nfltr/relay-core/core/identity"
)

// ServiceProvider names the principal the relay opens its own sessions to
// a service agent with (ServiceConn).
const ServiceProvider = "relay-service-route"

// ServiceConn opens a session, for one request of the relay's own, to the
// service agentID declares, as the agent's own principal (the operator
// named the agent; the caller checked its owner). It waits until the
// agent reached the service (Target.Answer), so an instance that is
// stopping or gone refuses here, before a request is sent. Closing the
// conn releases the session's accounting.
func (c *Core) ServiceConn(ctx context.Context, agentID, service string) (net.Conn, error) {
	opened, err := c.Open(ctx, identity.Principal{Provider: ServiceProvider, AgentID: agentID}, Target{Agent: agentID, Service: service, Answer: true})
	if err != nil {
		return nil, err
	}
	if cs, ok := opened.Session.(peerlink.ChainSession); ok && cs.Answers() {
		if err := cs.Answered(ctx); err != nil {
			_ = opened.Session.Close()
			if opened.Release != nil {
				opened.Release()
			}
			return nil, err
		}
	}
	return &openedConn{Conn: opened.Session, release: opened.Release}, nil
}

// openedConn releases the session's accounting when closed.
type openedConn struct {
	net.Conn
	release func()
	done    atomic.Bool
}

func (c *openedConn) Close() error {
	err := c.Conn.Close()
	if c.done.CompareAndSwap(false, true) && c.release != nil {
		c.release()
	}
	return err
}
