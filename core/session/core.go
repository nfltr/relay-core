// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"crypto/tls"
	"net"
	"sync/atomic"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
)

// Core is one replica's session core: its agents' sessions (Dispatcher,
// TCPDispatcher), the replica directory and the replica link, presence and
// the drain. Frontends open sessions through it (Open); the composition
// (package compose) sets its fields and runs it.
type Core struct {
	PodID string

	// PublishedDispatcher is Dispatcher for other goroutines: Run sets it
	// once the dispatcher is wired; Dispatcher never changes after.
	PublishedDispatcher atomic.Pointer[Dispatcher]
	Dispatcher          *Dispatcher
	// TCPDispatcher handles TCP sessions (agents' tcp services, ssh, the
	// connect sidecar); nil when tcp.enabled=false (the default).
	TCPDispatcher *TCPDispatcher

	// Directory is the replica directory (one replica: in process; several:
	// Redis); the inventories are its agent records.
	Directory             directory.Cache
	DispatcherIPInventory directory.IPInventory
	StreamerIPInventory   directory.IPInventory

	// PeerLinks is the replica link (core/replicas); nil when not configured.
	PeerLinks ReplicaLink
	// DrainBy is the drain's deadline, fixed by its first call (at the
	// drain's start) as that start plus the shutdown window; set by Run.
	DrainBy func() time.Time
	// routeBudget bounds Open's routing; 0: the const routeBudget (open.go).
	routeBudget time.Duration

	Broker               broker.MessageBroker
	PodIDTopicSubscriber broker.Consumer
	PodIDTopicProducer   broker.Producer

	// PresenceBus shares presence with peer replicas (presence.go).
	PresenceBus PresenceBus

	// AgentLinkUDP and AgentLinkTLS take agents' agent-link connections
	// over QUIC; nil: TCP only.
	AgentLinkUDP net.PacketConn
	AgentLinkTLS *tls.Config

	// Reach and DialPolicy are the composition's access rule and dial
	// policy for Open.
	Reach      Reach
	DialPolicy DialPolicy
}
