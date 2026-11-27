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
// named the agent; the caller checked its owner): its machine of its
// owner's account, which Reach lets reach its own agents. It waits until
// the agent reached the service (Target.Answer), so an instance that is
// stopping or gone refuses here, before a request is sent. Closing the
// conn releases the session's accounting.
func (c *Core) ServiceConn(ctx context.Context, agentID, service string) (net.Conn, error) {
	opened, err := c.Open(ctx, c.servicePrincipal(agentID), Target{Agent: agentID, Service: service, Answer: true})
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

// servicePrincipal is agentID's own principal: the machine of its ref, of
// the account the directory records as its owner (none known: the open is
// refused as the agent not being found).
func (c *Core) servicePrincipal(agentID string) identity.Principal {
	p := identity.Principal{Provider: ServiceProvider, AgentID: agentID}
	ref, err := identity.ParseAgentRef(agentID)
	if err != nil || c.Dispatcher == nil {
		return p
	}
	p.Machine, p.Subject = ref.Machine, string(ref.Machine)
	p.Owner, _ = c.Dispatcher.agentOwner(ref, agentID)
	return p
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
