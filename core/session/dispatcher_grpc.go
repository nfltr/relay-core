// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
	"uuid"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/internal/platform/auth"
	"github.com/nfltr/relay-core/internal/platform/logging"
	"github.com/nfltr/relay-core/internal/platform/receiveloop"
	"github.com/nfltr/relay-core/internal/platform/txnregistry"
	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// retryBackoff returns an exponential backoff duration with jitter.
// baseInterval is doubled for each attempt (capped at 30s), then jittered ±25%.
func retryBackoff(attempt int, baseInterval time.Duration) time.Duration {
	backoff := float64(baseInterval) * math.Pow(2, float64(attempt-1))
	if backoff > float64(30*time.Second) {
		backoff = float64(30 * time.Second)
	}
	// ±25% jitter
	jitter := backoff * 0.25 * (2*rand.Float64() - 1) // #nosec G404 -- jitter is not security-critical
	return time.Duration(backoff + jitter)
}

// classifyDisconnectReason maps an errgroup error to a disconnect reason label.
func classifyDisconnectReason(err error) string {
	switch {
	case err == nil:
		return "clean"
	case errors.Is(err, errNoHeartbeat):
		return "heartbeat_timeout"
	case errors.Is(err, errServerRestarting):
		return "server_restart"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	default:
		return msgTypeError
	}
}

// cleanupPeerStream ends a stream to a peer replica that ended with an
// error (its podRelayStream's cleanup: the link session's reset).
func (d *Dispatcher) cleanupPeerStream(result *PodRelayStream, agentID string, retErr error, logger *slog.Logger) {
	if result == nil || !errors.Is(retErr, podToPodStreamErr) {
		return
	}
	if isExpectedStreamDisconnect(retErr) {
		logStreamLifecycleWithLogger(logger, "pod-to-pod stream closed during teardown", retErr)
		return
	}
	// The peer relay is healthy; only the agent behind it went away.
	if strings.Contains(retErr.Error(), errTargetAgentDisconnected.Error()) {
		logger.Info("pod-to-pod stream ended: target agent disconnected", "agent_id", agentID)
		return
	}
	logger.Error("error occurred during pod to pod streaming", "error", retErr)
	metrics.RegisterPodToPodConnectionFailure(result.targetPodIP)
	result.cleanup()
}

// dialPeerStream opens a RelayTunnel stream to the replica holding agentID
// (the replica link, PodDialer).
func (d *Dispatcher) dialPeerStream(ctx context.Context, agentID string, logger *slog.Logger) (*PodRelayStream, error) {
	if d.PodDialer == nil {
		return nil, errNoPodDialer
	}
	return d.PodDialer.DialRelay(ctx, agentID, logger)
}

// errNoPodDialer is what RelayTunnel answers on a single relay for a target
// that is not connected here.
var errNoPodDialer = status.Error(codes.NotFound, "pod dialer unavailable (not in k8s multi-pod mode)")

// relayTunnelStream is a relay tunnel's server end: a replica link session
// (linkRelayServer).
type relayTunnelStream interface {
	Context() context.Context
	Send(*pb.TunnelFrame) error
	Recv() (*pb.TunnelFrame, error)
}

// RelayTunnel serves a frame stream another replica forwards to an agent
// connected here (or, on a pod dialer, onward): replica <-> replica <-> agent.
func (d *Dispatcher) RelayTunnel(incomingStream relayTunnelStream) (retErr error) {
	callID := uuid.New().String()
	incomingStreamCtx := incomingStream.Context()
	downstreamStart := time.Now()

	var firstMessage *pb.TunnelFrame
	firstMessage, err := incomingStream.Recv()
	if err != nil {
		d.Logger.Error("failed to receive first message", "error", err)
		return err
	}

	if firstMessage.Routing == nil || firstMessage.Routing.AgentId == "" {
		d.Logger.Error("AgentID is required")
		return status.Errorf(codes.InvalidArgument, "AgentID is required")
	}

	firstMessage.Timestamps = &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()}
	if firstMessage.TransactionId == "" {
		firstMessage.TransactionId = uuid.New().String()
	}
	transactionID := firstMessage.TransactionId
	agentID := firstMessage.Routing.AgentId

	// For A2A cross-pod routing, TargetAgentId carries the intended recipient.
	// Use it for routing instead of AgentID (which identifies the source agent).
	targetAgentID := firstMessage.Routing.TargetAgentId
	if targetAgentID != "" {
		agentID = targetAgentID
	}

	if err := d.authorizeRelayTunnelCaller(incomingStreamCtx); err != nil {
		return err
	}

	// find the agent stream using the agentID
	nextStream, agentConnectedToThisPod := d.Upstreams.Load(agentID)
	if !agentConnectedToThisPod && d.DrainedAway(agentID) {
		return status.Error(codes.Unavailable, agentDrainingDetail)
	}
	route := "local"
	if !agentConnectedToThisPod {
		route = "remote"
	}

	// Build logger context all at once to reduce allocations
	var logger *slog.Logger
	if targetAgentID != "" {
		logger = logging.With(d.Logger,
			"AgentID", firstMessage.Routing.AgentId,
			"transactionID", transactionID,
			"callID", callID,
			"message_type", firstMessage.MessageType,
			"targetAgentID", targetAgentID,
			"flow", "a2a-cross-pod",
			"agentConnectedToThisPod", agentConnectedToThisPod,
		)
	} else {
		logger = logging.With(d.Logger,
			"AgentID", firstMessage.Routing.AgentId,
			"transactionID", transactionID,
			"callID", callID,
			"message_type", firstMessage.MessageType,
			"agentConnectedToThisPod", agentConnectedToThisPod,
		)
	}

	logger.Debug("received message from cluster")

	defer func() {
		if retErr != nil {
			logStreamLifecycleWithLogger(logger, "completed RelayTunnel", retErr)
		} else {
			logger.Debug("completed RelayTunnel")
		}
	}()
	var handle *txnregistry.TransactionHandle[*pb.TunnelFrame]
	var cleanup func()
	// relayErr captures the raw error from relay send/recv goroutines so
	// cleanupPeerStream can inspect the unwrapped chain. status.Errorf
	// (used for the gRPC return value) does not preserve Go error wrapping,
	// which would prevent errors.Is(retErr, podToPodStreamErr) from working.
	var relayErr error
	if !agentConnectedToThisPod {
		logger.Debug("not found agent stream")
		metrics.IncrActiveRemoteRelays()
		defer metrics.DecrActiveRemoteRelays()
		peerResult, dialErr := d.dialPeerStream(incomingStreamCtx, agentID, logger)
		if dialErr != nil {
			if status.Code(dialErr) != codes.NotFound {
				// codes.NotFound covers "no pod dialer configured" (single-pod
				// deployments) and is excluded, matching the identical
				// exclusion in routeAgentToAgentRemote (dispatcher_agent_routing.go)
				// — see that file's comment for why. Every other dial failure
				// here means a genuine cross-pod RelayTunnel proxy (the literal
				// "svc 1 <-> pod 1 <-> pod 2 <-> agent" path this function
				// implements) could never connect to the target pod. Found via
				// a live chaostoolkit fault-injection run
				// (docker/chaostoolkit/experiments) that showed this exact
				// branch — not routeAgentToAgentRemote's — is what a relay
				// tunnel to another replica exercises, and it registered no metric at all on dial
				// failure: the dialErr is returned directly to the caller
				// before cleanupPeerStream's deferred RegisterPodToPodConnectionFailure
				// call is even set up (that defer only runs for a stream that
				// dialed successfully and later broke).
				logger.Error("failed to dial peer for RelayTunnel target", "error", dialErr)
				metrics.RegisterPodToPodConnectionFailure("")
			}
			return dialErr
		}
		nextStream = peerResult.stream
		defer func() { d.cleanupPeerStream(peerResult, agentID, relayErr, logger) }()
		if cs, ok := nextStream.(interface{ CloseSend() error }); ok {
			defer func() { _ = cs.CloseSend() }()
		}
	} else {
		logger.Debug("found agent stream")
		handle, cleanup = d.TxnRegistry.Register(transactionID, incomingStreamCtx, logger)

		defer cleanup()
	}
	if agentConnectedToThisPod {
		err = sendNewCall(nextStream, firstMessage)
	} else {
		err = nextStream.Send(firstMessage)
	}
	if errors.Is(err, errAgentDraining) {
		return status.Error(codes.Unavailable, agentDrainingDetail)
	}
	if err != nil {
		logger.Error("failed to send first message to next stream", "error", err)
		metrics.RegisterGRPCDownstreamRequest(msgTypeError, route)
		metrics.RegisterGRPCDownstreamDuration(route, float64(time.Since(downstreamStart).Milliseconds()))
		relayErr = fmt.Errorf("send first message to next stream failed: %v: %w", err, podToPodStreamErr)
		return status.Errorf(codes.Internal, "error: %v", relayErr)
	}
	metrics.IncrGRPCDownstreamMessagesSent()
	logger.Debug("sent first message to next stream")
	var eg *errgroup.Group
	var egCtx context.Context
	agentBoundCtx := incomingStreamCtx
	if agentConnectedToThisPod {
		logger.Debug("errgroup ctx is using incoming stream ctx bound to the agent stream")
		agentBoundCtx = bindToAgentStream(incomingStreamCtx, nextStream)
		eg, egCtx = errgroup.WithContext(agentBoundCtx)
	} else {
		logger.Debug("errgroup ctx is using next stream ctx")
		eg, egCtx = errgroup.WithContext(nextStream.Context())
	}
	if !firstMessage.GetEom() {
		eg.Go(func() error {
			type recvResult struct {
				msg *pb.TunnelFrame
				err error
			}
			recvCh := make(chan recvResult, 1)

			go func() {
				defer close(recvCh)
				for {
					nextMessage, err := incomingStream.Recv()
					select {
					case recvCh <- recvResult{msg: nextMessage, err: err}:
						if err != nil {
							return
						}
					case <-egCtx.Done():
						return
					}
				}
			}()

			for {
				select {
				case <-egCtx.Done():
					logStreamLifecycleWithLogger(logger, "downstream context done", egCtx.Err())
					return egCtx.Err()
				case result, ok := <-recvCh:
					if !ok {
						return nil
					}
					if result.err != nil {
						if result.err == io.EOF {
							logger.Debug("received all messages from cluster (EOF)", "agent-connection", agentConnectedToThisPod)
							return nil
						}
						logger.Error("failed to receive message from cluster", "transactionID", transactionID, "agent-connection", agentConnectedToThisPod, "error", result.err)
						return result.err
					}
					nextMessage := result.msg
					logger.Debug("received message from cluster", "transactionID", transactionID)
					if agentConnectedToThisPod {
						nextMessage.TransactionId = transactionID
						nextMessage.Timestamps = &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()}
						logger.Debug("added metadata to message", "transactionID", transactionID)
					}
					err = nextStream.Send(nextMessage)
					if err != nil {
						logStreamLifecycleWithLogger(logger, "failed to send message to next stream", err, "isAgent", agentConnectedToThisPod)
						if !agentConnectedToThisPod {
							return fmt.Errorf("send message to next stream failed: %v: %w", err, podToPodStreamErr)
						} else {
							return err
						}
					}
					metrics.IncrGRPCDownstreamMessagesSent()
					if agentConnectedToThisPod {
						logger.Debug("sent message to agent")
					} else {
						logger.Debug("sent message to next stream")
					}
					if nextMessage.GetEom() {
						logger.Debug("received request EOM", "agent-connection", agentConnectedToThisPod)
						return nil
					}
				}
			}
		})
	} else {
		logger.Debug("received EOM - from cluster, first message only")
	}
	if agentConnectedToThisPod {
		eg.Go(func() error {
			if handle == nil {
				logger.Error("no transaction handle", "transactionID", transactionID)
				return txnregistry.ErrTransactionNotFound
			}

			for {
				select {
				case <-egCtx.Done():
					logStreamLifecycleWithLogger(logger, "downstream context done", egCtx.Err())
					return egCtx.Err()
				case <-handle.Stalled:
					return errRelayTunnelStalled
				case msg := <-handle.ResponseCh:
					if err := incomingStream.Send(msg); err != nil {
						logger.Error("failed to send response to cluster", "error", err)
						return err
					}
					metrics.IncrGRPCDownstreamMessagesReceived()
					logger.Debug("sent message to cluster")
					if msg.GetEom() {
						logger.Debug("received EOM - finished reading from response channel", "agent-connection", agentConnectedToThisPod)
						return nil
					}
				}
			}
		})
	} else {
		eg.Go(func() error {
			for {
				incomingMessage, err := nextStream.Recv()
				if err != nil {
					// Before the context check: egCtx derives from the next
					// stream's context, which ends as soon as the peer answers.
					if st, ok := peerAnsweredStatus(err); ok {
						return &peerAnswerError{st: st}
					}
					if egCtx.Err() != nil {
						return egCtx.Err()
					}
					logger.Error("failed to receive message from next pod", "error", err)
					return fmt.Errorf("receive message from next pod failed: %v: %w", err, podToPodStreamErr)
				}
				metrics.IncrGRPCDownstreamMessagesReceived()
				logger.Debug("received message from next pod")
				err = incomingStream.Send(incomingMessage)
				if err != nil {
					logger.Error("failed to send message to caller", "error", err)
					return err
				}
				logger.Debug("sent message to caller")
				if incomingMessage.GetEom() {
					logger.Debug("received EOM - from next pod, finished reading", "agent-connection", agentConnectedToThisPod)
					return nil
				} else {
					logger.Debug("waiting for more messages from next pod")
				}
			}
		})
	}

	err = eg.Wait()
	durationMs := float64(time.Since(downstreamStart).Milliseconds())
	if err != nil {
		logStreamLifecycleWithLogger(logger, "one of the goroutines finished with error", err)
		metrics.RegisterGRPCDownstreamRequest(msgTypeError, route)
		metrics.RegisterGRPCDownstreamDuration(route, durationMs)
		relayErr = err // preserve raw error for cleanupPeerStream
		if errors.Is(context.Cause(agentBoundCtx), errTargetAgentDisconnected) {
			return status.Error(codes.Unavailable, errTargetAgentDisconnected.Error())
		}
		if errors.Is(err, errRelayTunnelStalled) {
			return err
		}
		if isExpectedStreamDisconnect(err) {
			return status.Errorf(codes.Canceled, "Canceled")
		}
		// The next pod answered (e.g. NotFound): pass its typed status back
		// unchanged and keep the healthy peer's pooled connection.
		if answer, ok := errors.AsType[*peerAnswerError](err); ok {
			return answer.st.Err()
		}
		// When relaying and the peer stream context ended (server-2 died)
		// while the caller's stream is still alive, classify as a
		// pod-to-pod failure so cleanupPeerStream increments the metric.
		if !agentConnectedToThisPod && incomingStreamCtx.Err() == nil {
			relayErr = fmt.Errorf("relay stream ended: %v: %w", err, podToPodStreamErr)
		}
		return status.Errorf(codes.Internal, "error: %v", err)
	}
	metrics.RegisterGRPCDownstreamRequest("ok", route)
	metrics.RegisterGRPCDownstreamDuration(route, durationMs)
	return nil
}

// agent to grpc server
func (d *Dispatcher) AgentTunnel(stream pb.Dispatcher_AgentTunnelServer) error {
	// Get AgentID from context — supports API key or mTLS auth.
	// AgentID must be present for the agent to connect
	// AgentID must not present for the cluster connections
	agentID, err := auth.GetAgentIDFromContext(stream.Context())
	if err != nil {
		d.Logger.Error("error determining agentID from context", "error", err)
		return status.Error(codes.PermissionDenied, "Error determining agentID from context")
	}
	if err := d.Config.Reserved.RefuseAgentName(agentID); err != nil {
		return err
	}

	ctx, cancelCause := context.WithCancelCause(stream.Context()) // will cancel if `stream context` cancels
	tunnelCancel := &agentTunnelCancel{cancel: cancelCause}
	defer cancelCause(nil)
	registrationCtx := d.agentInventoryRegistrationContext(ctx)

	d.Logger.Info("received AgentTunnel call from agent", "AgentID", agentID)
	credential := streamCredential(stream.Context())
	release, err := d.validateAgentConnection(registrationCtx, agentID, credential, d.StreamerIPInventory)
	if err != nil {
		return err
	}
	defer release()

	metrics.RegisterUpstreamConnection("grpc")
	upstreamStart := time.Now()

	recvThreshold := d.Config.HeartbeatRecvThreshold

	// Parse agent labels and capabilities from gRPC metadata early so we
	// can store them in Redis.
	var agentLabels map[string]string
	var agentCapabilities []string
	var agentTLSFingerprint, keyBase string
	{
		md := incomingKeys(stream.Context(), agentwire.LabelsKey, "x-agent-capabilities", agentInstanceHeader,
			"x-tls-fingerprint")
		agentLabels = parseLabelsMetadata(md)
		if len(agentLabels) > 0 {
			d.Logger.Info("agent advertised labels", "AgentID", agentID, "labels", agentLabels)
		}
		agentCapabilities = parseCapabilitiesMetadata(md)
		if vals := md.Get(agentInstanceHeader); len(vals) > 0 {
			tunnelCancel.instance = vals[0]
		}
		if len(agentCapabilities) > 0 {
			d.Logger.Info("agent advertised capabilities", "AgentID", agentID, "capabilities", agentCapabilities)
		}
		// The pre-scoped key identity, for A2A target resolution and
		// ownership; registerAgent records it once this session holds the
		// agent.
		keyBase = keyBaseOf(stream.Context())
		// E2EE: read TLS certificate fingerprint from agent metadata.
		if vals := md.Get("x-tls-fingerprint"); len(vals) > 0 && vals[0] != "" {
			agentTLSFingerprint = vals[0]
			d.Logger.Info("agent advertised TLS fingerprint", "agent_id", agentID, "fingerprint", vals[0])
		}
	}

	ephemeral, err := d.admitAgentLifetime(agentID, agentLabels)
	if err != nil {
		return err
	}

	session, err := claimAgentRecord(registrationCtx, d.StreamerIPInventory, agentID, directory.Session{Since: time.Now(), Credential: credential, Account: PrincipalOwner(stream.Context())}, agentLabels, agentCapabilities, recvThreshold)
	if err != nil {
		d.Logger.Error("failed to set agentID dispatcher addr in cache", "AgentID", agentID, "error", err)
		return err
	}

	heartbeats, heartbeatsRecvThresholdTicker := newHeartbeatState(recvThreshold)
	defer heartbeatsRecvThresholdTicker.Stop()

	agentStream := NewGRPCStream(agentID, stream, stream.Context(), heartbeats, heartbeatsRecvThresholdTicker)
	agentStream.connectedAt, agentStream.seq, agentStream.credential, agentStream.account = session.Since, session.Seq, session.Credential, session.Account

	// Assign labels already parsed above.
	agentStream.labels = agentLabels
	agentStream.connectLabels = maps.Clone(agentLabels)
	agentStream.capabilities = agentCapabilities
	agentStream.tlsFingerprint = agentTLSFingerprint

	if ephemeral {
		// Held until this stream's row is registered (removeEphemeralRegistration).
		d.ephemeralMu.Lock()
	}
	d.agentTunnelMu.Lock()
	d.Upstreams.Store(agentID, agentStream)
	old, replaced := d.agentTunnelCancels.Swap(agentID, tunnelCancel)
	d.agentTunnelMu.Unlock()
	d.drainedAgents.Delete(agentID) // the id is back: no longer a drained departure
	d.wakeStaleAgentReaper()
	d.notifyUpstreamRegistered(agentID)
	d.touchAgentLastSeen(agentID)
	if replaced {
		// A newer connection of this agent ID took over (a reconnect after
		// a drop not noticed yet, or a second process with the same id):
		// the older session ends and its client is told why, instead of
		// staying connected but unroutable (first-user audit #2). Ended
		// after the Store above, so its teardown leaves this one's rows.
		prev := old.(*agentTunnelCancel)
		if prev.instance != "" && prev.instance == tunnelCancel.instance {
			// The same process reconnected (a drop the relay has not seen):
			// its older stream is dropped; nothing listens on it.
			d.Logger.Info("agent reconnected; dropping its previous AgentTunnel stream", "AgentID", agentID)
			prev.cancel(errSessionReconnected)
		} else {
			d.Logger.Info("another process connected with this agent id; ending the previous AgentTunnel session", "AgentID", agentID)
			prev.cancel(errSessionReplaced)
		}
	}
	if d.OnAgentMetadata != nil {
		d.OnAgentMetadata(AgentInfo{
			AgentID:      agentID,
			TunnelType:   "grpc",
			Capabilities: cloneStringSlice(agentCapabilities),
			Labels:       cloneStringMap(agentLabels),
			ConnectedAt:  agentStream.connectedAt,
		})
	}
	d.RegisterAgent(agentID, keyBase, agentStream.account, agentStream.tlsFingerprint)
	if ephemeral {
		d.ephemeralMu.Unlock()
	}
	// The accept, once the agent is registered (as ConnectHTTP): the
	// heartbeat the agent may watch and the admin message (if any).
	// Session hooks run only on ConnectHTTP.
	{
		responseMD := metadata.New(nil)
		if msg := d.Config.AdminMessage; msg != "" {
			responseMD.Append("x-admin-message", msg)
		}
		d.appendHeartbeat(responseMD, d.agentHeartbeat(stream.Context()))
		if sendErr := stream.SendHeader(responseMD); sendErr != nil {
			d.Logger.Warn("failed to send response headers", "error", sendErr)
		}
	}

	defer func(agentID string, currentStream *GRPCStream, currentLabels map[string]string) {
		d.agentTunnelCancels.CompareAndDelete(agentID, tunnelCancel)
		d.noteAgentDeparture(agentID, currentStream) // before the stream leaves upstreams
		if !d.DeregisterUpstreamAgent(agentID, currentStream, stream.Context().Err()) {
			return // replaced: its replacement holds the pool slot, record and labels
		}
		d.lastSeenByAgentID.Delete(agentID)
		if ephemeral {
			// Before DeleteIPs: until then another pod refuses this id
			// (AlreadyExists), so no reconnect there registers a row this
			// removal could take.
			d.removeEphemeralRegistration(agentID)
		}
		if err := d.StreamerIPInventory.DeleteIPs(context.Background(), agentID); err != nil {
			d.Logger.Error("failed to delete agentID dispatcher addr from cache", "AgentID", agentID, "error", err)
		} else {
			d.Logger.Debug("deleted agentID dispatcher addr from cache", "AgentID", agentID)
		}
		d.publishDeparture(agentID, currentStream)
	}(agentID, agentStream, agentLabels)

	sessionCtx := ctx
	eg, ctx := errgroup.WithContext(ctx)
	// Recv returns only when the gRPC stream ends, and the stream ends only
	// when this handler returns: the receive loop runs outside the group,
	// so a session ended from here (replaced, reaped) returns without
	// waiting for a Recv the client may never answer (as ConnectHTTP).
	received := make(chan error, 1) // watchAgentSession ends the session with it
	go func() { received <- d.receiveFromGRPCAgent(ctx, agentStream) }()

	eg.Go(func() error {
		return d.sendGRPCHeartbeats(ctx, agentID, agentStream)
	})

	err = d.runSession(eg, ctx, cancelCause, agentID, agentStream, d.StreamerIPInventory, received)

	// Record upstream connection duration with disconnect reason.
	disconnectReason := classifyDisconnectReason(err)
	metrics.RegisterUpstreamConnectionDuration(disconnectReason, time.Since(upstreamStart).Seconds())

	if err != nil {
		d.logStreamLifecycle("agent subscription is disconnected", err, "AgentID", agentID)
	}
	switch cause := context.Cause(sessionCtx); {
	case errors.Is(cause, errSessionReplaced):
		return errReplacedSessionStatus
	case errors.Is(cause, errSessionReconnected):
		return status.Error(codes.Canceled, errSessionReconnected.Error())
	}
	return d.classifyStreamError(err)
}

// sendGRPCHeartbeats periodically sends empty gRPC heartbeat messages to the agent.
// On send failure it cleans the IP inventory and returns.
// It takes the mutex-protected GRPCStream to avoid concurrent gRPC Send calls
// with A2A routing goroutines that share the same underlying stream.
func (d *Dispatcher) sendGRPCHeartbeats(ctx context.Context, agentID string, stream *GRPCStream) error {
	interval := d.agentHeartbeat(stream.streamCtx)
	heartbeatTicker := time.NewTicker(interval)
	defer heartbeatTicker.Stop()
	var bookkept time.Time
	var bookkeeping atomic.Bool
	logger := d.Logger.With("AgentID", agentID)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			if stream.OutboundDataSentWithin(interval) {
				logger.Debug("skipping heartbeat to agentID agent: outbound data recently sent")
				continue
			}
			if err := stream.Send(&pb.TunnelFrame{}); err != nil {
				d.onHeartbeatSendError(agentID, err, d.StreamerIPInventory)
				return err
			}
			logger.Debug("sent heartbeat to agentID agent")
			if time.Since(bookkept) >= heartbeatBookkeepingEvery {
				bookkept = time.Now()
				d.bookkeepHeartbeat(agentID, &bookkeeping)
			}
		}
	}
}

// errRelayTunnelStalled ends a RelayTunnel whose caller took none of the
// agent's responses for the stall bound (txnregistry.ErrTransactionStalled).
var errRelayTunnelStalled = status.Error(codes.ResourceExhausted, txnregistry.ErrTransactionStalled.Error())

// receiveFromGRPCAgent reads messages from a gRPC-mode agent stream and
// fans out each data message to the transaction registry for the waiting RelayTunnel caller.
func (d *Dispatcher) receiveFromGRPCAgent(ctx context.Context, stream *GRPCStream) error {
	recvThreshold := d.Config.HeartbeatRecvThreshold
	return receiveloop.ReceiveAndProcess(ctx, d.Config.RecvBufferSize, stream.ID, d.Logger,
		stream.Recv,
		func() { stream.heartbeatsRecvThresholdTicker.Reset(recvThreshold) },
		func(ctx context.Context, input *pb.TunnelFrame, receivedAt time.Time) error {
			return d.processGRPCInput(ctx, stream, input, receivedAt)
		},
	)
}

// processGRPCInput handles a single message received from a gRPC-mode agent.
// Heartbeats (zero-size or empty transaction ID) are forwarded to the heartbeat channel.
// Data messages are validated, enriched, and fanned out via the TransactionRegistry.
func (d *Dispatcher) processGRPCInput(ctx context.Context, stream *GRPCStream, input *pb.TunnelFrame, receivedAt time.Time) error {
	_ = d.refreshAgentInventoryOnInboundActivity(ctx, stream.ID, d.StreamerIPInventory, stream.Session(), stream.Labels(), stream.Capabilities()) // superseded: the session's watch ends it
	logger := logging.With(d.Logger, "AgentID", stream.ID)
	if input == nil || proto.Size(input) == 0 || len(input.TransactionId) == 0 {
		logger.Debug("received a heartbeat")
		metrics.RegisterUpstreamMessageProcessed("heartbeat")
		if ctx.Err() != nil {
			// Context cancelled; skip heartbeat delivery. Heartbeats are
			// best-effort and must not prevent the processor from draining
			// remaining data messages (e.g. the final EOM response that a
			// RelayTunnel caller is waiting for).
			logger.Debug("skipping heartbeat delivery: context cancelled")
			return nil
		}
		d.markAgentStreamAlive(stream, input)
		return nil
	}

	if input.GetMessageType() == agentDrainMessageType {
		return d.handleAgentDrain(stream, input, logger)
	}
	// Agent-to-agent: if the message carries a target_agent_id, route it to the
	// target agent instead of treating it as a RelayTunnel response.
	if isAgentToAgentRequest(input) {
		metrics.RegisterUpstreamMessageProcessed("a2a")
		d.dispatchAgentToAgent(ctx, stream, input)
		return nil
	}
	metrics.RegisterUpstreamMessageProcessed("data")
	if input.Timestamps == nil {
		return fmt.Errorf("missing timestamps in message for transactionID: %s", input.TransactionId)
	}
	input.Timestamps.ResponseAtGrpcServer = receivedAt.UnixMilli()
	// to be backward compatible with agents, set new fields to zero values
	if input.Routing == nil {
		input.Routing = &pb.RoutingInfo{}
	}
	// overrides agentID received from agent with the known stream agentID
	input.Routing.AgentId = stream.ID
	logger = logging.With(d.Logger, "message_type", input.GetMessageType(), "AgentID", input.Routing.AgentId, "transaction_id", input.TransactionId)
	now := time.Now()
	err := d.TxnRegistry.Fanout(input.TransactionId, input, logger)
	if err != nil {
		if errors.Is(err, txnregistry.ErrTransactionStale) {
			logger.Debug("ignoring stale transaction response", "transactionID", input.TransactionId, "message_type", input.GetMessageType(), "error", err)
			return nil
		}
		if errors.Is(err, txnregistry.ErrTransactionStalled) {
			// Its caller took nothing for the bound: the call is refused (its
			// consumer sees handle.Stalled), and the agent told to stop it.
			metrics.RegisterFanoutResponseDrop("stalled")
			go d.sendAgentToAgentCancel(stream, input, input.TransactionId, logger) // the agent's sender may be busy
			return nil
		}
		metrics.RegisterDownstreamTransactionTimeout()
		logger.Error("failed to fanout message", "transactionID", input.TransactionId, "error", err)
		if d.Config.DropAgentOnDownstreamErr {
			return err
		}
	} else {
		logger.Debug("fanout transaction took", "transaction_id", input.TransactionId, "duration", time.Since(now))
	}
	return nil
}

// authorizeRelayTunnelCaller admits a relay tunnel stream from another
// relay replica only (a replica link session: mTLS, the peer secret in its
// hello; peer_link_stream.go), which forwards a caller it authorized.
// Clients cannot open one: the public gRPC service has no RelayTunnel.
func (d *Dispatcher) authorizeRelayTunnelCaller(ctx context.Context) error {
	if d.ClusterRelayPeers || ctx.Value(PeerLinkCaller{}) != nil {
		return nil // clusterRelayPeers: tests standing in for a peer
	}
	return status.Error(codes.PermissionDenied, "relay tunnels come from relay replicas only")
}
