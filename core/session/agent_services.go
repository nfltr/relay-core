// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// declaredServices reads the services a ConnectHTTP stream declares: nil
// for none, an InvalidArgument status for a malformed declaration (the
// session is refused, nothing is declared).
func declaredServices(ctx context.Context) ([]agentservices.Decl, error) {
	vals := metadata.ValueFromIncomingContext(ctx, agentservices.Header)
	switch len(vals) {
	case 0:
		return nil, nil
	case 1:
	default:
		return nil, status.Errorf(codes.InvalidArgument, "%s: declare the services once", agentservices.Header)
	}
	decls, err := agentservices.Parse(vals[0])
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s: %v", agentservices.Header, err)
	}
	return decls, nil
}

// AgentServices returns the services agentID's ConnectHTTP stream
// declared, and whether the agent is connected: here, or on another
// replica, whose agent record in the shared inventory carries the
// declaration (directory.Session.Services). A record without one declares
// none (also one a relay before v0.20.3 wrote).
func (d *Dispatcher) AgentServices(agentID string) ([]agentservices.Decl, bool) {
	if s, ok := d.Agents.Load(agentID); ok {
		return s.Services, true
	}
	store, ok := d.IPInventory.(directory.SessionStore)
	if !ok || agentID == "" {
		return nil, false
	}
	_, s, err := store.GetSession(context.Background(), agentID) // bounded by the inventory's op timeout
	if err != nil {
		return nil, false
	}
	if s.Services == "" {
		return nil, true
	}
	services, err := agentservices.Parse(s.Services)
	if err != nil { // written by a relay from a declaration it accepted: never
		return nil, true
	}
	return services, true
}
