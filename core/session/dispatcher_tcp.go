// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/internal/platform/auth"
	"github.com/nfltr/relay-core/internal/platform/logging"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// tcpRelayBufSize is the read buffer size for server-side TCP relay loops:
// the largest data frame the relay sends an agent. A buffer stays pinned
// while its conn's Read waits, so it is sized for per-frame throughput
// (BenchmarkTCPTunnelThroughput), not for bulk memory.
const tcpRelayBufSize = 64 * 1024

// tcpSessionQueueBytes bounds the agent -> client bytes queued for one
// session on a gRPC stream without credit (an older agent's; a stream with
// credit queues its window: tcpDataWindow on a data connection,
// tcpGRPCWindow on a gRPC stream). The stream's receive loop waits only on
// a session holding this much unwritten data, so a slow client paces its
// siblings only past it, and relay memory per session is bounded in bytes.
const tcpSessionQueueBytes = 1 << 20

// TCPSessionStall is how long the receive loop of a gRPC stream without
// credit waits on a session whose queue is full while its client takes
// nothing; then the session is reset (tcpSession.deliver) and the stream's
// other sessions go on. A client still reading frees room with every write it takes, which
// restarts the wait: a client taking under tcpWriteBatch per tcpSessionStall
// (about 26 KB/s) while its agent sends faster is reset too.
const TCPSessionStall = 10 * time.Second

// tcpWriteBatch bounds the queued bytes a session writes to its conn at
// once (bytequeue.PopBatch).
const tcpWriteBatch = 256 << 10

const tcpHeartbeatControlSessionID = "_heartbeat"

// TCPRelayBufPool recycles read buffers used by TCP relay goroutines,
// reducing GC pressure during sustained high-throughput transfers.
var TCPRelayBufPool = sync.Pool{
	New: func() any { return make([]byte, tcpRelayBufSize) },
}

// TCPEndpoint maps a server listen address to an agent + backend target.
//
// Examples:
//
//	{ListenAddr: ":2222", AgentID: "mydevice",    TargetAddr: "localhost:22"}   // SSH
//	{ListenAddr: ":3389", AgentID: "mywinpc",     TargetAddr: "localhost:3389"} // RDP
//	{ListenAddr: ":2222", AgentID: "lan-gw",      TargetAddr: "192.168.1.42:22"} // LAN
type TCPEndpoint struct {
	// ListenAddr is the TCP address the server listens on (e.g. ":2222").
	ListenAddr string
	// AgentID is the target agent identity.
	AgentID string
	// TargetAddr is the host:port the agent dials.  May be any LAN-reachable
	// address, enabling whole-LAN access via a single gateway agent.
	TargetAddr string
	// E2EE signals that this session is an E2EE TLS passthrough.
	// When true, the agent routes bytes to its local TLS listener instead
	// of dialing TargetAddr directly.
	E2EE bool
	// Share, on an E2EE connection for a share whose credentials only the
	// agent checks, is the share the stream carrying it must enforce
	// (RouteConnection, e2ee_share_gate.go).
	Share string
	// Answer: an owner's /connect session, whose opener waits for the
	// agent's answer (pb.TCPData.Answer): link's own on a leg that
	// negotiated link.FeatureChain, in band on any other (session_chain.go).
	// The hop that cannot serve it answers its own refusal.
	Answer bool
	// ClientAddr, on an E2EE connection, is the visitor's IP address as
	// the relay accepted its connection (RouteConnection), sent to the
	// agent in the open (pb.TCPData.ClientAddr): the agent ends the
	// visitor's TLS and gives it to the backend as X-Forwarded-For and
	// X-Real-IP.
	ClientAddr string
}

// TCPDispatcher accepts raw TCP connections, multiplexes them over agents'
// ConnectTCP gRPC streams, and pumps bytes bidirectionally.
//
// Multi-pod support: when the target agent is not locally connected,
// TCPDispatcher opens a session on the replica link (peer_link.go) to the
// replica that has the agent and bridges the visitor over it.
//
//	+---------+  TCP  +--------------------+ ConnectTCP  +---------+
//	| Client  |------►| TCPDispatcher      |------------►| Agent   |
//	|(ssh/rdp)|       |                    |             |(on-prem)|
//	+---------+       +--------------------+             +---------+
//
// Cross-pod (multi-pod SaaS):
//
//	Client --> Pod A TCPDispatcher --> replica link --> Pod B TCPDispatcher --> Agent
type TCPDispatcher struct {
	Mu      sync.RWMutex
	Streams map[string]*tcpAgentStream // agentID -> its latest registered stream
	// All holds every registered stream of an agent that opened several
	// (the agent SDK's tcpStreamsHeader); new sessions are spread over them.
	All      map[string][]*tcpAgentStream
	nextPick atomic.Uint64
	// bound maps a primary stream's token to it (tcpBindHeader).
	bound   map[string]*tcpAgentStream
	AuxIdle time.Duration // tcpAuxIdle; tests shorten it
	// SessionStall is TCPSessionStall; tests shorten it (sessiontest).
	SessionStall time.Duration
	// dataPlane is set once data connections can reach the dispatcher
	// (DataPlaneListener): agents are then offered them.
	dataPlane atomic.Bool
	// Link is the agent link (agent_link.go), made with the data plane.
	Link *agentLink
	// registered is closed, and replaced, whenever a stream registers:
	// connections waiting for their agent's stream wake on it.
	registered chan struct{}
	logger     *slog.Logger
	nextSess   atomic.Int64

	heartbeatInterval time.Duration
	onHeartbeat       func(agentID string)

	// A primary stream's sender (on its handler goroutine) renews the
	// agent's inventory record every renewEvery and, once leaving closes,
	// lets the agent go at a jittered moment of drainWindow (drain.go).
	renewEvery  time.Duration
	renew       func(agentID string, s directory.Session) error
	renewNow    func() <-chan struct{} // closed when every record must be rewritten now
	leaving     <-chan struct{}
	drainWindow time.Duration
	// DrainBy is the relay's drain deadline (RPCServer.drainBy): the agent
	// link's sessions run until then once the gRPC port closed.
	DrainBy func() time.Time

	// podDialer is used for cross-pod routing when the target agent is on
	// a different pod.  Nil in single-pod deployments.
	podDialer PodDialer

	// Links opens TCP sessions to the agent's replica on the replica link;
	// nil on a single relay.
	Links TCPLinkOpener

	// streamerIPInventory registers each TCP agent's pod address so that
	// cross-pod routing can locate it.
	streamerIPInventory directory.IPInventory
}

// NewTCPDispatcher creates an empty TCP dispatcher.
// podDialer may be nil in single-pod deployments.
func NewTCPDispatcher(logger *slog.Logger, podDialer PodDialer, inv directory.IPInventory) *TCPDispatcher {
	return &TCPDispatcher{
		registered:          make(chan struct{}),
		Streams:             make(map[string]*tcpAgentStream),
		All:                 make(map[string][]*tcpAgentStream),
		bound:               make(map[string]*tcpAgentStream),
		AuxIdle:             tcpAuxIdle,
		SessionStall:        TCPSessionStall,
		logger:              logger,
		podDialer:           podDialer,
		streamerIPInventory: inv,
	}
}

// ConfigureHeartbeats enables lightweight control-plane keepalives for idle
// TCP streams so callers can refresh external liveness state from successful
// send operations without waiting for application traffic.
func (d *TCPDispatcher) ConfigureHeartbeats(interval time.Duration, onHeartbeat func(agentID string)) {
	d.heartbeatInterval = interval
	d.onHeartbeat = onHeartbeat
}

// ConfigureRenew makes every primary stream call renew for its agent every
// interval (its inventory record's TTL is longer).
func (d *TCPDispatcher) ConfigureRenew(interval time.Duration, renew func(agentID string, s directory.Session) error, now func() <-chan struct{}) {
	d.renewEvery, d.renew, d.renewNow = interval, renew, now
}

// ConfigureDrain lets every agent go at a jittered moment of the first
// half of window once leaving closes (the relay is shutting down).
func (d *TCPDispatcher) ConfigureDrain(leaving <-chan struct{}, window time.Duration) {
	d.leaving, d.drainWindow = leaving, window
}

// ConnectedAgentIDs returns the agentIDs of all agents with active TCP streams.
func (d *TCPDispatcher) ConnectedAgentIDs() []string {
	d.Mu.RLock()
	defer d.Mu.RUnlock()
	agentIDs := make([]string, 0, len(d.Streams))
	for k, as := range d.Streams {
		if as.http != httpSessionsOnly { // an HTTP agent's sessions are not a TCP tunnel
			agentIDs = append(agentIDs, k)
		}
	}
	return agentIDs
}

// ServeEndpoint listens on ep.ListenAddr and blocks until ctx is cancelled.
func (d *TCPDispatcher) ServeEndpoint(ctx context.Context, ep TCPEndpoint) error {
	ln, err := net.Listen("tcp", ep.ListenAddr)
	if err != nil {
		return fmt.Errorf("tcp dispatcher: listen %s: %w", ep.ListenAddr, err)
	}
	d.logger.Info("tcp tunnel endpoint listening",
		"addr", ep.ListenAddr, "agent_id", ep.AgentID, "target", ep.TargetAddr)
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("tcp dispatcher: accept %s: %w", ep.ListenAddr, err)
		}
		go d.HandleConn(ctx, conn, ep)
	}
}

// HandleConn handles a single inbound TCP connection.
// Uses the local agent stream when available; falls back to cross-pod relay.
func (d *TCPDispatcher) HandleConn(ctx context.Context, conn net.Conn, ep TCPEndpoint) {
	defer conn.Close() //nolint:errcheck

	start := time.Now()

	// The agent may be reconnecting after a brief network glitch or a
	// relay restart: wait for its stream to register.
	as := d.streamFor(ctx, ep.AgentID)
	if ctx.Err() != nil {
		return
	}

	if as == nil {
		d.forwardToPod(ctx, conn, ep, "cross-pod", start) // agent not on this pod
		return
	}

	_ = d.handleLocalConn(conn, ep, as) // a visitor conn: the caller closes it either way
}

// errTCPSessionRefused: handleLocalConn did not serve the connection (the
// agent's streams went, or the share is not enforced there).
var errTCPSessionRefused = errors.New("tcp session refused")

// handleLocalConn carries a visitor's TCP connection to the agent here: on
// its agent link (peerlink.Join), else multiplexed over its frame streams.
func (d *TCPDispatcher) handleLocalConn(conn net.Conn, ep TCPEndpoint, as *tcpAgentStream) error {
	if ep.Share != "" && !as.enforcesShare(ep.Share) {
		d.logger.Warn("tcp tunnel: share connection refused: its stream does not enforce the share's policy",
			"agent_id", ep.AgentID, "share", ep.Share, "remote", conn.RemoteAddr())
		shareUnenforced()
		return errTCPSessionRefused
	}
	open := &pb.TCPData{AgentId: ep.AgentID, TargetAddr: ep.TargetAddr, E2Ee: ep.E2EE, ClientAddr: ep.ClientAddr}
	if s, release, ok, err := d.Link.open(context.Background(), primaryOf(as), open); ok {
		if err != nil {
			return errTCPSessionRefused
		}
		// The link's connections count its bytes; its first byte is the
		// visitor's (agentLinkPath).
		_ = peerlink.Join(metrics.CountConn(conn, agentLinkPath(s), time.Now(), false, false), s)
		release()
		return nil
	}
	sessionID := fmt.Sprintf("%d", d.nextSess.Add(1))
	sess := newTCPSession()
	sess.abort = func() { resetConn(conn) }
	if as = d.attach(ep.AgentID, as, sessionID, sess); as == nil {
		return errTCPSessionRefused
	}
	defer as.removeSession(sessionID)
	// attach may have moved the session to another of the agent's streams.
	if ep.Share != "" && !as.enforcesShare(ep.Share) {
		d.logger.Warn("tcp tunnel: share connection refused: its stream does not enforce the share's policy",
			"agent_id", ep.AgentID, "share", ep.Share, "remote", conn.RemoteAddr())
		shareUnenforced()
		return errTCPSessionRefused
	}

	path := streamPath(as)
	began := metrics.TransferBegin(path)
	defer metrics.TransferEnd(path, began)
	firstByte := true

	// Send open frame so the agent dials ep.TargetAddr.
	as.send(&pb.TCPData{SessionId: sessionID, AgentId: ep.AgentID, TargetAddr: ep.TargetAddr, Open: true, E2Ee: ep.E2EE, ClientAddr: ep.ClientAddr})
	go d.pumpToAgent(conn, as, sessionID, sess)

	// Deliver everything the agent sent before its close: the close frame
	// trails the data on the stream, so the queue holds the whole tail. A
	// backlog goes out in one writev (on the TCP conn itself: net.Buffers
	// sees through no wrapper).
	w := conn
	if pc, ok := conn.(*PrefixConn); ok {
		w = pc.Conn
	}
	writeBatch := func(bufs [][]byte) error {
		nb := net.Buffers(bufs)
		_, err := nb.WriteTo(w)
		return err
	}
	var bufs [][]byte
	for {
		var ok bool
		if bufs, ok = sess.recvQ.PopBatch(bufs[:0], tcpWriteBatch); !ok {
			if sess.peerHalf.Load() { // the agent half-closed: so does the client's conn
				CloseWrite(conn)
				<-sess.closed // the client's bytes still go to the agent until it ends too
			}
			return nil
		}
		if firstByte {
			firstByte = false
			metrics.ObserveTransferFirstByte(path, time.Since(began))
		}
		err := writeBatch(bufs)
		clear(bufs)
		if err != nil {
			as.send(&pb.TCPData{SessionId: sessionID, Close: true})
			sess.markClosed()
			return nil
		}
	}
}

// pumpToAgent sends what a visitor's connection reads to the agent, then
// the session's close.
func (d *TCPDispatcher) pumpToAgent(conn net.Conn, as *tcpAgentStream, sessionID string, sess *tcpSession) {
	{
		buf := TCPRelayBufPool.Get().([]byte)
		defer TCPRelayBufPool.Put(buf) //nolint:staticcheck
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				cp := make([]byte, n)
				copy(cp, buf[:n])
				as.send(&pb.TCPData{SessionId: sessionID, Data: cp})
			}
			if err == io.EOF {
				as.closeWrite(sessionID, sess) // a half close: the agent's bytes keep coming
				return
			}
			if err != nil {
				as.send(&pb.TCPData{SessionId: sessionID, Close: true})
				sess.markClosed()
				return
			}
		}
	}
}

// RouteConnection routes a pre-accepted TCP connection to the named agent.
// Unlike handleConn, the caller already knows the agentID (e.g. from SNI
// extraction) — there is no static TCPEndpoint.  The target_addr sent to the
// agent is "__e2ee__" signaling E2EE TLS passthrough mode.
//
// RouteConnection closes conn when done.
//
// When share is set, the connection is for that share, whose credentials
// only the agent can check: it goes only to a local stream whose agent
// enforces the share's policy (e2ee_share_gate.go), and is refused otherwise.
//
// service, when set, is the agent's service the connection is for: the
// open frame names it in target_addr, which an agent with named services
// looks up in its table (agent_services.go); "" is the whole agent.
func (d *TCPDispatcher) RouteConnection(ctx context.Context, conn net.Conn, agentID, service, share string) {
	defer conn.Close() //nolint:errcheck

	ep := TCPEndpoint{
		AgentID:    agentID,
		TargetAddr: "__e2ee__",
		E2EE:       true,
		Share:      share,
		ClientAddr: remoteIP(conn),
	}
	if service != "" {
		ep.TargetAddr = service
	}

	start := time.Now()

	// Local agent lookup, waiting for a reconnecting agent (as handleConn).
	as := d.streamFor(ctx, agentID)
	if ctx.Err() != nil {
		return
	}
	// A share whose agent is on another replica is checked there (serveLinkTCP).
	if share != "" && as != nil && !as.enforcesShare(share) {
		d.logger.Warn("sni route: share connection refused: its agent does not enforce the share's policy",
			"agent_id", agentID, "share", share, "remote", conn.RemoteAddr())
		shareUnenforced()
		return
	}

	if as == nil {
		d.forwardToPod(ctx, conn, ep, "cross-pod-e2ee", start)
		return
	}

	_ = d.handleLocalConn(conn, ep, as) // a visitor conn: the caller closes it either way
}

// remoteIP is the IP address of conn's peer ("" when it has none): the
// visitor's own connection, nothing the visitor sent.
func remoteIP(conn net.Conn) string {
	if a, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		if ip, ok := netip.AddrFromSlice(a.IP); ok {
			return ip.Unmap().String()
		}
	}
	return ""
}

// forwardToPod forwards a visitor's connection to the replica holding its
// agent, on the replica link (peerlink.Join: a session the other replica
// resets cuts the visitor with an RST). A replica that cannot be reached
// on it (it advertises no link: older than v0.10.0, or without peer_link)
// gets the visitor cut at once, logged with the reason.
func (d *TCPDispatcher) forwardToPod(ctx context.Context, conn net.Conn, ep TCPEndpoint, path string, start time.Time) {
	logger := logging.With(d.logger, "agent_id", ep.AgentID, "remote", conn.RemoteAddr(), "path", path)
	s, err := d.openLink(ctx, ep)
	if err != nil {
		logger.Warn("tcp tunnel: cannot forward to the agent's replica", "error", err)
		metrics.TransferOpened(metrics.PathReplica, relayv1.ErrorReason_ERROR_REASON_TARGET_UNREACHABLE.String())
		return
	}
	began := metrics.TransferBegin(metrics.PathReplica)
	defer metrics.TransferEnd(metrics.PathReplica, start)
	_ = peerlink.Join(metrics.CountConn(conn, metrics.PathReplica, began, false, true), s)
}

// openLink opens a session on the replica link to ep's agent; ctx carries
// its link.Chain, if any.
func (d *TCPDispatcher) openLink(ctx context.Context, ep TCPEndpoint) (PeerSession, error) {
	if d.Links == nil {
		return nil, fmt.Errorf("agent %q is not connected to this relay", ep.AgentID)
	}
	return d.Links.OpenAgent(ctx, ep)
}

// ---------------------------------------------------------------------------
// gRPC handlers on Dispatcher
// ---------------------------------------------------------------------------

// ConnectTCP implements Dispatcher.ConnectTCP.  The agent opens this bidi
// stream once and keeps it alive -- similar to AgentTunnel.  The agent
// handles open/close/data frames by dialling ep.TargetAddr and relaying bytes.
func (d *Dispatcher) ConnectTCP(stream pb.Dispatcher_ConnectTCPServer) error {
	if d.TCPDispatcher == nil {
		return status.Error(codes.Unimplemented, "tcp tunneling is not enabled on this server")
	}

	agentID, err := auth.GetAgentIDFromContext(stream.Context())
	if err != nil {
		d.Logger.Error("ConnectTCP: cannot determine agentID", "error", err)
		return status.Error(codes.Unauthenticated, "cannot determine agentID")
	}
	if err := d.Config.Reserved.RefuseAgentName(agentID); err != nil {
		return err
	}

	// A busy agent's auxiliary connection: more capacity for its sessions,
	// bound to its primary stream, not an agent session of its own (no
	// inventory entry, not counted against the agent limit).
	if v := metadata.ValueFromIncomingContext(stream.Context(), TCPBindHeader); len(v) > 0 {
		d.Logger.Debug("agent bound an auxiliary ConnectTCP connection", "agent_id", agentID)
		return d.TCPDispatcher.RegisterAuxStream(agentID, v[0], stream)
	}

	// Register this pod's address in the streamer inventory so cross-pod TCP
	// routing can locate the agent.
	session := directory.Session{Since: time.Now(), Credential: streamCredential(stream.Context()), Account: PrincipalOwner(stream.Context())}
	if d.StreamerIPInventory != nil {
		recvThreshold := d.Config.HeartbeatRecvThreshold
		var setErr error
		if session, setErr = claimAgentRecord(stream.Context(), d.StreamerIPInventory, agentID, session, nil, nil, recvThreshold); setErr != nil {
			d.Logger.Error("ConnectTCP: failed to set streamer IP", "agent_id", agentID, "error", setErr)
			// Non-fatal: TCP still works locally; cross-pod routing cannot find this agent
			// (until a renewal writes the record; without a Seq it is ordered by its start time).
		}
		defer func() {
			if d.TCPDispatcher.agentStream(agentID) != nil {
				return // another of the agent's streams is still here
			}
			if _, ok := d.Upstreams.Load(agentID); ok {
				return // its AgentTunnel still holds the record
			}
			if delErr := d.StreamerIPInventory.DeleteIPs(context.Background(), agentID); delErr != nil {
				d.Logger.Error("ConnectTCP: failed to delete streamer IP", "agent_id", agentID, "error", delErr)
			}
		}()
	}

	d.Logger.Info("agent opened ConnectTCP stream", "agent_id", agentID)

	// Control frame handler for protocol-level signals from the agent.
	// Currently handles _fingerprint frames for E2EE certificate rotation.
	onControl := func(frame *pb.TCPData) {
		if frame.GetSessionId() == "_fingerprint" {
			newFP := string(frame.GetData())
			d.Logger.Info("agent rotated E2EE certificate",
				"agent_id", agentID, "fingerprint", newFP)
			if d.OnAgentConnect != nil {
				d.OnAgentConnect(agentID, newFP)
			}
		}
	}

	return d.TCPDispatcher.RegisterStream(agentID, stream, session, onControl)
}

// ---------------------------------------------------------------------------
// DialAgent — in-process session (browser terminal, test harnesses)
// ---------------------------------------------------------------------------

// DialAgentWithRetry wraps DialAgent with exponential backoff.  It retries
// up to maxRetries times when the agent is temporarily unreachable (e.g.
// it is reconnecting to another pod).  baseInterval controls the delay
// between attempts; it doubles each time, capped at 30 s, with jitter.
//
// A nil‐error conn is returned on the first successful dial.  If all
// attempts fail, the last error is returned.
func (d *TCPDispatcher) DialAgentWithRetry(ctx context.Context, agentID, targetAddr string, maxRetries int, baseInterval time.Duration) (net.Conn, error) {
	s, release, err := d.OnwardWithRetry(ctx, TCPEndpoint{AgentID: agentID, TargetAddr: targetAddr}, maxRetries, baseInterval)
	if err != nil {
		return nil, err
	}
	return newDialedSession(ctx, s, release), nil
}

// OnwardWithRetry is onward with DialAgentWithRetry's retries.
func (d *TCPDispatcher) OnwardWithRetry(ctx context.Context, ep TCPEndpoint, maxRetries int, baseInterval time.Duration) (peerlink.Session, func(), error) {
	agentID := ep.AgentID
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		s, release, err := d.onward(ctx, ep)
		if err == nil {
			return s, release, nil
		}
		if errors.Is(err, ErrAgentAnswers) || errors.Is(err, directory.ErrUnavailable) {
			return nil, nil, err // the agent is here, or the relay's directory failed: another attempt changes nothing now
		}
		lastErr = err
		if errors.Is(err, ErrPeerUnreachable) || noRoute(err) != nil {
			// The replica the inventory names (re-read on each attempt:
			// an agent that moved is found) did not answer a dial within
			// the last dial timeout, a partition or a replica gone, or the
			// replica link refused the session with no route: more
			// attempts would only wait; the caller answers at once.
			return nil, nil, fmt.Errorf("agent %q unreachable after %d attempts: %w", agentID, attempt, err)
		}

		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("context cancelled during dial retry %d: %w", attempt, ctx.Err())
		}

		if attempt < maxRetries {
			wait := retryBackoff(attempt, baseInterval)
			d.logger.Debug("tcp dial: retrying agent dial",
				"agent_id", agentID, "attempt", attempt, "max", maxRetries,
				"wait", wait, "error", err)
			// The next attempt starts as soon as the agent's stream
			// registers (a reconnecting agent), at the latest after wait.
			if d.waitAgentStream(ctx, agentID, wait); ctx.Err() != nil {
				return nil, nil, fmt.Errorf("context cancelled during dial retry %d: %w", attempt, ctx.Err())
			}
		}
	}
	return nil, nil, fmt.Errorf("agent %q unreachable after %d attempts: %w", agentID, maxRetries, lastErr)
}

// DialAgent opens an in-process TCP session to the agent identified by agentID,
// instructing the agent to dial targetAddr.  It returns a net.Conn that pumps
// raw bytes bidirectionally between the caller and the agent's upstream service.
//
// This is the server-side counterpart to ServeEndpoint: instead of a raw TCP
// listener, the session originates programmatically (e.g. from a WebSocket
// terminal handler or SSH proxy). Cancelling ctx ends the session.
//
// When the agent is on a different replica, DialAgent opens a session on
// the replica link (as handleConn does for TCP listeners): the returned
// net.Conn is that session.
func (d *TCPDispatcher) DialAgent(ctx context.Context, agentID, targetAddr string) (net.Conn, error) {
	s, release, err := d.onward(ctx, TCPEndpoint{AgentID: agentID, TargetAddr: targetAddr})
	if err != nil {
		return nil, err
	}
	return newDialedSession(ctx, s, release), nil
}

// ErrAgentAnswers is onward's refusal of an end-to-end encrypted /connect
// session to an agent that gives no answer: the client's TLS with it
// needs the agent's word that it took the session.
var ErrAgentAnswers = errors.New("the agent does not answer session opens")

func errNoAnswers(agentID string) error {
	return fmt.Errorf("agent %s runs a version that does not answer session opens: update it (%w)", agentID, ErrAgentAnswers)
}

// onward opens ep's session to its agent: on its agent link or frame
// streams when it is connected here, else on the replica link to its
// replica (ctx carries the session's link.Chain, if any). With ep.Answer
// the session answers with the agent's word (session_chain.go): link's
// own on a leg that negotiated link.FeatureChain, read in band on any
// other (inbandOut); an agent that gives none (nfltr v2.1.5 and older)
// takes a plain session without it, and refuses an E2EE one
// (errAgentAnswers). release, when not nil, runs once after the session
// ended.
func (d *TCPDispatcher) onward(ctx context.Context, ep TCPEndpoint) (peerlink.Session, func(), error) {
	as := d.agentStream(ep.AgentID)
	if as == nil {
		s, err := d.openLink(ctx, ep) // the agent's replica answers for it
		if err != nil {
			return nil, nil, err
		}
		return answering(s, ep.Answer), nil, nil
	}
	return d.onwardLocal(ctx, ep, as)
}

// onwardLocal is onward to the agent of stream as, here.
func (d *TCPDispatcher) onwardLocal(ctx context.Context, ep TCPEndpoint, as *tcpAgentStream) (peerlink.Session, func(), error) {
	if ep.Answer && !primaryOf(as).answers {
		if ep.E2EE {
			return nil, nil, errNoAnswers(ep.AgentID)
		}
		ep.Answer = false
	}
	open := &pb.TCPData{AgentId: ep.AgentID, TargetAddr: ep.TargetAddr, E2Ee: ep.E2EE, Answer: ep.Answer, ClientAddr: ep.ClientAddr}
	if s, release, ok, err := d.Link.open(ctx, primaryOf(as), open); ok {
		if err != nil {
			return nil, nil, err
		}
		noteAgentLinkLeg(ctx, s)
		return answering(s, ep.Answer), release, nil
	}

	sessionID := fmt.Sprintf("ws%d", d.nextSess.Add(1))
	sess := newTCPSession()
	if as = d.attach(ep.AgentID, as, sessionID, sess); as == nil {
		s, err := d.openLink(ctx, ep) // its streams went: it may be on another replica
		if err != nil {
			return nil, nil, err
		}
		return answering(s, ep.Answer), nil, nil
	}
	// attach may have moved the session to another of the agent's streams.
	if ep.Share != "" && !as.enforcesShare(ep.Share) {
		as.removeSession(sessionID)
		return nil, nil, errShareUnenforced
	}
	open.SessionId, open.Open = sessionID, true
	as.send(open)
	conn := &inProcConn{
		agentStream: as,
		sessionID:   sessionID,
		sess:        sess,
		cleanupFn: func() {
			as.removeSession(sessionID)
			sess.markClosed() // wakes a pending Read
		},
	}
	return answering(conn, ep.Answer), nil, nil
}

// errShareUnenforced: the agent's stream does not enforce the share's
// policy (e2ee_share_gate.go).
var errShareUnenforced = errors.New("the agent's stream does not enforce the share's policy")

// answering is s, read for its answer in band when the open asks for one
// and s does not answer link's own way.
func answering(s peerlink.Session, answer bool) peerlink.Session {
	if answer && !AnswersNatively(s) {
		return &inbandOut{Session: s}
	}
	return s
}

// resetConn closes c abortively (an RST on TCP): its peer sees the stream
// cut, not ended.
func resetConn(c net.Conn) {
	if s, ok := c.(interface{ Reset() error }); ok {
		_ = s.Reset() // a session, or a visitor's prefixConn
		return
	}
	if l, ok := c.(interface{ SetLinger(int) error }); ok {
		_ = l.SetLinger(0)
	}
	_ = c.Close()
}
