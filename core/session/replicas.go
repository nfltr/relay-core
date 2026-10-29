// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/directory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The seams to the other replicas of a multi-replica relay. Each is nil on
// a single relay, whose registrations here are the only authority.

// Replicas are a Dispatcher's seams to its peers, set with PodDialer.
type Replicas struct {
	Owner Owner
	Mover Mover
}

// Owner names the replica holding an agent's session, by the cluster
// inventory.
type Owner interface {
	// OwnerOf is the replica agentID is recorded on ("" none) and whether
	// that is this one. An error is a failed lookup, directory.ErrAgentNotConnected
	// for an agent no replica holds.
	OwnerOf(ctx context.Context, agentID string) (replica string, local bool, err error)
}

// TCPLinkOpener opens a TCP session to an agent's replica on the replica
// link.
type TCPLinkOpener interface {
	OpenAgent(ctx context.Context, ep TCPEndpoint) (PeerSession, error)
}

// Mover moves agents between replicas (agent_move.go).
type Mover interface {
	// Move asks agentID's session here to connect to another replica;
	// false when it does not move (gone, moving already, or too young).
	Move(agentID string) bool
	// Rebalance takes the fleet's agents (total) over its replicas
	// (agent_rebalance.go); it returns how many it asked to move.
	Rebalance(total, replicas int) int
	// ReannounceAll wakes every agent session here to rewrite its record
	// now (inventory_epoch.go).
	ReannounceAll()
}

// PeerSession is a replica link session.
type PeerSession = peerlink.Session

// ErrPeerLinkOff: a multi-replica relay without peer_link configured
// cannot reach agents on other replicas.
var ErrPeerLinkOff = status.Error(codes.Unavailable, "replica link: not configured on this relay (peer_link.port and its CA): the agent is on another replica")

// PeerLinkCaller marks a stream context of a verified replica link peer
// (authorizeRelayTunnelCaller).
type PeerLinkCaller struct{}

// ReplicaLink is the replica link as the inventory epoch and the HTTP
// ingress use it (core/replicas.PeerLinks); nil when not configured.
type ReplicaLink interface {
	// Advertise writes this replica's link record, renewed by the epoch tick.
	Advertise(ctx context.Context, c directory.Cache, host string, ttl time.Duration)
	// HTTPDial is the reverse proxy's dialer toward a peer's link port.
	HTTPDial(linkPort string, dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error)
}

// LinkOffHTTPDial is HTTPDial of a relay without a replica link: a request
// for a peer's link port is refused with ErrPeerLinkOff.
func LinkOffHTTPDial(linkPort string, dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if _, port, err := net.SplitHostPort(addr); err != nil || port != linkPort {
			return dial(ctx, network, addr)
		}
		return nil, ErrPeerLinkOff
	}
}

// offLinks is a multi-replica relay's PodDialer and TCPLinkOpener until the
// replica link is wired (core/replicas.LinkPodDialer): every forward is
// refused with ErrPeerLinkOff.
type offLinks struct{}

func (offLinks) DialRelay(context.Context, string, *slog.Logger) (*PodRelayStream, error) {
	return nil, ErrPeerLinkOff
}

func (offLinks) OpenAgent(context.Context, TCPEndpoint) (PeerSession, error) {
	return nil, ErrPeerLinkOff
}

// PassCloseWrite is the CloseWrite of a conn wrapping c, which an
// embedded net.Conn hides. github.com/nfltr/link's graceful close (a
// drain) needs it: without it link closes the conn while the peer still
// sends, which resets a TCP connection under the bytes in flight to it.
func PassCloseWrite(c net.Conn) error {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

// ErrPeerUnreachable: a session refused without a dial, a dial to the
// peer having failed slowly less than peerUnreachableFor ago: a retry
// toward it waits for nothing new (DialAgentWithRetry stops).
var ErrPeerUnreachable = errors.New("replica link: peer unreachable")
