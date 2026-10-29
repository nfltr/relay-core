// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"google.golang.org/protobuf/proto"
)

const (
	// ProtocolHTTP names the HTTP protocol in an agent's request routing.
	ProtocolHTTP = "http"

	// AgentIDKey is the AgentID header (agent_id on the wire, where header
	// names ignore case), in its canonical form so a Get or Set of it costs
	// no allocation (TestHeaderNamesAreCanonical).
	AgentIDKey = "Agent_id"

	// TransactionIDHeader is a reserved request header injected by the proxy;
	// client-supplied values are stripped before forwarding to the agent.
	// Canonical form (X-RPC-Server-TransactionID on the wire before, the same
	// header: names ignore case): a Get, Set or Del of a name in another
	// form allocates its canonical copy, several per request
	// (docs/performance.md R4, TestHeaderNamesAreCanonical).
	TransactionIDHeader = "X-Rpc-Server-Transactionid"

	// RequestAtProxyHeader records the Unix-millisecond timestamp at which the
	// proxy received the request; it is stripped from client-supplied headers.
	// Canonical form, as TransactionIDHeader.
	RequestAtProxyHeader = "X-Rpc-Server-Requestatproxy"

	// RESERVED response header
	// following headers from the downstream agent will not be considered.
	// Canonical form, as TransactionIDHeader.
	MetadataHeader = "X-Rpc-Server-Metadata"

	// BrowsePathPrefix is the URL prefix for URL-based agent browsing.
	// Requests to /browse/<agentID>/... are routed to the named agent.
	BrowsePathPrefix = "/browse/"

	// BrowsePrefixHeader is forwarded to the agent so it can rewrite
	// body URLs to include the full /browse/<agentID> path.
	BrowsePrefixHeader = "X-Browse-Prefix"

	// SharePathPrefix is the URL prefix for public share-token access.
	// Requests to /share/<token>/... are routed to the agent encoded in
	// the token without requiring the caller to have an API key or session.
	SharePathPrefix = "/share/"
)

// StreamIO is a generic bidirectional stream interface.
type StreamIO[S, R proto.Message] interface {
	Send(S) error
	Recv() (R, error)
}

// Streamer wraps the gRPC bidirectional stream for sending/receiving pb.TunnelFrame.
type Streamer interface {
	StreamIO[*pb.TunnelFrame, *pb.TunnelFrame]
	Context() context.Context
}

// HeartbeatTracker provides heartbeat monitoring for agent streams.
type HeartbeatTracker interface {
	AgentID() string
	Heartbeats() chan proto.Message
	HeartbeatsRecvThresholdTicker() *time.Ticker
}

// HTTPStreamer wraps the gRPC bidirectional stream for the HTTP-based
// ConnectHTTP RPC.
type HTTPStreamer = StreamIO[*pb.HTTPProxyRequest, *pb.HTTPProxyResponse]

// Stream wraps a StreamIO with mutex-protected Send/Recv and heartbeat state.
type Stream[S, R proto.Message] struct {
	sendMut                       sync.Mutex
	recvMut                       sync.Mutex
	ID                            string
	routes                        []string             // service prefixes advertised by the agent
	Services                      []agentservices.Decl // named services under this identity (agent_services.go); nil for none
	labels                        map[string]string    // key-value labels for fleet selection
	labelsMu                      sync.RWMutex         // guards labels once the stream is published
	connectLabels                 map[string]string    // labels advertised at connect (an update never changes them)
	capabilities                  []string             // advertised capabilities (p2p, ssh, http, a2a)
	tlsFingerprint                string               // SHA-256 fingerprint of agent's E2EE TLS cert
	client                        agentwire.Client     // the client program that opened the stream
	IO                            StreamIO[S, R]
	streamCtx                     context.Context
	heartbeats                    chan proto.Message
	heartbeatsRecvThresholdTicker *time.Ticker
	// Per-agent stats; connectedAt, credential and account also name the
	// session in its inventory record (Session).
	connectedAt            time.Time
	seq                    int64
	credential             string
	account                string
	bytesSent              atomic.Int64
	bytesRecv              atomic.Int64
	requestCount           atomic.Int64
	lastOutboundDataSentAt atomic.Int64
	// hooked is a session hook's part in the session (session_hook.go);
	// nil for none. Set before the receive loop.
	hooked HookedSession
	// takeover lets a reconnect of the same agent replace this session
	// (ConnectHTTP sessions only; nil otherwise).
	takeover *sessionTakeover
	// draining: the agent is leaving and takes no new call (agent_drain.go);
	// guarded by sendMut.
	draining bool
	// moved is closed when an agent that moves (agent_move.go) reports its
	// new connection; nil for one that does not.
	moved     chan struct{}
	movedOnce sync.Once
	// moving: asked to move; its record belongs to the new session, so
	// inbound activity no longer renews it.
	moving atomic.Bool
	// rebalance is closed when this replica asks the agent to move to
	// rebalance the fleet (agent_rebalance.go; once: rebalanceAsked); nil
	// for an agent that does not move.
	rebalance      chan struct{}
	rebalanceAsked atomic.Bool
	// inflight holds the HTTP transactions handed to the agent and not yet
	// answered in full (ConnectHTTP sessions; dispatcher_http_inflight.go).
	inflight sync.Map
}

// SetLabels replaces the stream's advertised label set. Used by the
// Stage 1 inner-LLM label-publication channel
// (feedback_nfltr_is_pure_communication_layer): when a worker pushes
// MessageTypeWorkerLabelsUpdate the dispatcher's MergeAgentLabels
// builds the new map and calls SetLabels. Concurrency: a single
// goroutine drives this per agent (the orchestrator's recv loop), so
// no internal lock is needed here.
func (s *Stream[S, R]) SetLabels(labels map[string]string) {
	if s == nil {
		return
	}
	s.labelsMu.Lock()
	defer s.labelsMu.Unlock()
	s.labels = labels
}

// GRPCStream is a Stream specialized for gRPC pb.TunnelFrame bidirectional streams.
type GRPCStream = Stream[*pb.TunnelFrame, *pb.TunnelFrame]

// HTTPStream is a Stream specialized for HTTP subscribe bidirectional streams.
type HTTPStream = Stream[*pb.HTTPProxyRequest, *pb.HTTPProxyResponse]

// NewGRPCStream creates a new thread-safe GRPCStream wrapping a Streamer.
func NewGRPCStream(agentID string, stream Streamer, ctx context.Context, heartbeats chan proto.Message, heartbeatsRecvThresholdTicker *time.Ticker) *GRPCStream {
	return &GRPCStream{
		ID:                            agentID,
		IO:                            stream,
		streamCtx:                     ctx,
		heartbeats:                    heartbeats,
		heartbeatsRecvThresholdTicker: heartbeatsRecvThresholdTicker,
		connectedAt:                   time.Now(),
		client:                        streamClient(ctx),
	}
}

// NewHTTPStream creates a new thread-safe HTTPStream wrapping an HTTPStreamer.
func NewHTTPStream(agentID string, stream HTTPStreamer, ctx context.Context, heartbeats chan proto.Message, heartbeatsRecvThresholdTicker *time.Ticker) *HTTPStream {
	return &HTTPStream{
		ID:                            agentID,
		IO:                            stream,
		streamCtx:                     ctx,
		heartbeats:                    heartbeats,
		heartbeatsRecvThresholdTicker: heartbeatsRecvThresholdTicker,
		connectedAt:                   time.Now(),
		client:                        streamClient(ctx),
	}
}

// Send delivers a message to the underlying stream in a mutex-protected call.
func (a *Stream[S, R]) Send(x S) error {
	a.sendMut.Lock()
	defer a.sendMut.Unlock()
	if err := a.IO.Send(x); err != nil {
		return err
	}
	if !isOutboundHeartbeatMessage(x) {
		a.lastOutboundDataSentAt.Store(time.Now().UnixNano())
	}
	return nil
}

func isOutboundHeartbeatMessage(msg proto.Message) bool {
	switch m := msg.(type) {
	case *pb.TunnelFrame:
		return m == nil || proto.Size(m) == 0
	case *pb.HTTPProxyRequest:
		return m == nil || proto.Size(m) == 0
	default:
		return false
	}
}

// Recv reads the next message from the underlying stream in a mutex-protected call.
func (a *Stream[S, R]) Recv() (R, error) {
	a.recvMut.Lock()
	defer a.recvMut.Unlock()
	return a.IO.Recv()
}

// Context returns the context associated with the underlying stream.
func (a *Stream[S, R]) Context() context.Context {
	return a.streamCtx
}

// Heartbeats returns the channel on which heartbeat frames are delivered.
func (a *Stream[S, R]) Heartbeats() chan proto.Message {
	return a.heartbeats
}

// HeartbeatsRecvThresholdTicker returns the ticker that fires when the agent
// heartbeat receive threshold is reached, triggering a liveness check.
func (a *Stream[S, R]) HeartbeatsRecvThresholdTicker() *time.Ticker {
	return a.heartbeatsRecvThresholdTicker
}

// AgentID returns the agent identifier associated with this stream.
func (a *Stream[S, R]) AgentID() string {
	return a.ID
}

// OutboundDataSentWithin reports whether non-heartbeat data was sent within d.
func (a *Stream[S, R]) OutboundDataSentWithin(d time.Duration) bool {
	if a == nil || d <= 0 {
		return false
	}
	last := a.lastOutboundDataSentAt.Load()
	if last == 0 {
		return false
	}
	return time.Since(time.Unix(0, last)) <= d
}

// Routes returns the service prefixes advertised by the agent on connect.
func (a *Stream[S, R]) Routes() []string {
	return a.routes
}

// Labels returns the key-value labels the agent advertised on connect.
func (a *Stream[S, R]) Labels() map[string]string {
	a.labelsMu.RLock()
	defer a.labelsMu.RUnlock()
	return a.labels
}

// Client is the client program that opened the stream (agentwire).
func (a *Stream[S, R]) Client() agentwire.Client {
	return a.client
}

// streamClient reads a stream's client identity from its Hello (the gRPC
// server refused the stream if it had none).
func streamClient(ctx context.Context) agentwire.Client {
	if ctx == nil { // test fixtures
		return agentwire.Client{}
	}
	c, _ := agentwire.ParseHello(incomingKeys(ctx, agentwire.HelloKey))
	return c
}

// Capabilities returns the capabilities the agent advertised on connect.
func (a *Stream[S, R]) Capabilities() []string {
	return a.capabilities
}

// ConnectedAt returns when the agent connected.
func (a *Stream[S, R]) ConnectedAt() time.Time {
	return a.connectedAt
}

// Session is this agent session as its inventory record names it.
func (a *Stream[S, R]) Session() directory.Session {
	return directory.Session{Since: a.connectedAt, Seq: a.seq, Credential: a.credential, Account: a.account, Services: agentservices.Format(a.Services)}
}

// BytesSent returns the total bytes sent to this agent.
func (a *Stream[S, R]) BytesSent() int64 {
	return a.bytesSent.Load()
}

// BytesRecv returns the total bytes received from this agent.
func (a *Stream[S, R]) BytesRecv() int64 {
	return a.bytesRecv.Load()
}

// RequestCount returns the total requests proxied through this agent.
func (a *Stream[S, R]) RequestCount() int64 {
	return a.requestCount.Load()
}
