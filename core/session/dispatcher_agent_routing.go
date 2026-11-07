// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"
	"uuid"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/internal/platform/logging"
	"github.com/nfltr/relay-core/internal/platform/txnregistry"
	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay/client/handoff"
	"github.com/nfltr/relay/client/rpcclient"
	"github.com/nfltr/relay/proto/pb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Message type constants used in agent-to-agent protocol messages.
const (
	msgTypeCancel       = "cancel"
	msgTypeError        = "error"
	a2aSessionBufferCap = 16
	// a2aWindowCap is how many window updates (rpcclient.WindowMessageType)
	// of one call wait for its route: a caller has at most four on the way
	// (each is at least a quarter of its window: link.Credit.Consumed;
	// relay spec/CallCredit.tla GrantBound), so a peer that keeps to its
	// credit never fills it.
	a2aWindowCap = 4
)

// a2aSession tracks an active agent-to-agent routing session. The source
// agent's receive loop feeds request messages into requestCh; a background
// goroutine drains the channel and forwards them to the target agent.
//
// responseCh is a per-session buffered channel that replaces the shared
// TxnRegistry channel, providing backpressure isolation between sessions.
//
// The source stream's receive loop is requestCh's only sender and closer
// (dispatchAgentToAgent): it never waits on the session past handoff.Stall (a
// route that takes nothing is refused, not waited on) and never sends after
// the EOM or a refusal (ended).
type a2aSession struct {
	requestCh  chan *pb.TunnelFrame
	responseCh chan *pb.TunnelFrame
	// windows carries the caller's window updates to the route, also after
	// the request's EOM (they grant the responses' credit); never waited on.
	windows  chan *pb.TunnelFrame
	cancelFn context.CancelFunc // cancels the session context

	source *GRPCStream     // the stream that opened it; only it feeds or cancels it
	done   <-chan struct{} // the session context's: the route ended
	ended  bool            // nothing more is handed off (the source's receive loop only)
	// sourceDone: the source sent the call's EOM or cancel. A route that
	// ends before it leaves the transaction in its source's a2aEnded, so
	// the call's later frames are dropped rather than open another call.
	sourceDone atomic.Bool
	// meter counts the call's payload for the target's owner, set once
	// the call is routed (nil before: unmetered).
	meter *metrics.Meter
}

// add counts the call's payload to the target and from it.
func (s *a2aSession) add(toAgent, fromAgent int) {
	var m *metrics.Meter
	if s != nil {
		m = s.meter
	}
	metrics.AddBytes(metrics.PathA2A, m, toAgent, fromAgent)
}

// isAgentToAgentRequest returns true when the message from an agent is
// intended for another agent rather than being a response to a RelayTunnel call.
func isAgentToAgentRequest(msg *pb.TunnelFrame) bool {
	return msg.GetRouting().GetTargetAgentId() != ""
}

// isA2ACancelMessage returns true if the message signals cancellation of a
// running agent-to-agent transaction.
func isA2ACancelMessage(msg *pb.TunnelFrame) bool {
	return msg.GetMessageType() == msgTypeCancel
}

// dispatchAgentToAgent is called from processGRPCInput for every message that
// carries a target_agent_id. On the first message of a transaction it creates a
// new a2aSession and starts the routing goroutine; subsequent messages for the
// same transaction are forwarded into the existing session's request channel.
//
// A special "cancel" message type cancels an in-flight session.
func (d *Dispatcher) dispatchAgentToAgent(ctx context.Context, sourceStream *GRPCStream, msg *pb.TunnelFrame) {
	txnID := msg.TransactionId
	// The source identity is the relay-authenticated stream identity (the
	// key-scoped id the dispatcher registered), never the client-claimed
	// value: rpcclient stamps its raw --name, and targets key recovery and
	// reply routing on SourceAgentID(). Mirrors processGRPCInput's override.
	msg.Routing.AgentId = sourceStream.ID
	// The relay numbers the calling connection, so a call its newer
	// connection replaced applies to nothing (UpdateAgentLabels, finding 14).
	msg.Routing.SourceSession = sourceStream.serial

	session, loaded := d.a2aSessions.Load(txnID)
	if loaded && session.source != sourceStream {
		// Another stream's transaction: neither fed nor cancelled from here.
		d.Logger.Warn("agent-to-agent frame for another stream's transaction dropped", "AgentID", sourceStream.ID, "transactionID", txnID)
		return
	}

	if msg.GetMessageType() == rpcclient.WindowMessageType {
		d.handoffA2AWindow(sourceStream, session, loaded, msg)
		return
	}

	last := msg.GetEom() || isA2ACancelMessage(msg)
	if !loaded {
		if _, ended := sourceStream.a2aEnded.Load(txnID); ended {
			// A frame of a call whose route ended before the source's last:
			// dropped (it would start a call mid-request); the last one
			// forgets the call.
			if last {
				sourceStream.a2aEnded.Delete(txnID)
			}
			return
		}
	}
	if loaded && last {
		session.sourceDone.Store(true)
		sourceStream.a2aEnded.Delete(txnID) // in case the route just ended
	}

	// Handle cancellation messages for existing sessions.
	if isA2ACancelMessage(msg) {
		if loaded {
			session.cancelFn()
		}
		return
	}

	if !loaded {
		var sessionCtx context.Context
		session, sessionCtx = d.newA2ASession(ctx, sourceStream)
		session.sourceDone.Store(last)
		d.a2aSessions.Store(txnID, session)
		go d.routeAgentToAgent(sessionCtx, sourceStream, session, txnID)
	}
	if session.ended {
		return // after its EOM or a refusal: dropped, never sent on a closed channel
	}

	switch err := handoff.Send(session.requestCh, msg, session.done, d.handoffStall); {
	case err == nil:
		if msg.GetEom() {
			session.ended = true
			close(session.requestCh)
		}
	case errors.Is(err, handoff.ErrStalled):
		// The route took none of the call's frames for the bound (a stuck
		// target): refuse this call rather than hold the source's others.
		session.ended = true
		session.cancelFn()
		metrics.RegisterA2AError("request_stalled")
		logger := logging.With(d.Logger, "AgentID", sourceStream.ID, "transactionID", txnID)
		logger.Warn("agent-to-agent call refused: its route took none of its request for the stall bound")
		go d.sendAgentToAgentError(sourceStream, msg, codes.ResourceExhausted, errA2ARequestStalled, logger)
	default: // the route ended and answered the source itself
		session.ended = true
	}
}

// handoffA2AWindow gives a caller's window update to its call's route
// without waiting. One for no call (it ended) is dropped; one past
// a2aWindowCap (a caller that broke its credit) refuses the call.
func (d *Dispatcher) handoffA2AWindow(sourceStream *GRPCStream, session *a2aSession, loaded bool, msg *pb.TunnelFrame) {
	if !loaded {
		return
	}
	select {
	case session.windows <- msg:
		return
	case <-session.done:
		return
	default:
	}
	session.cancelFn()
	metrics.RegisterA2AError("window_overflow")
	logger := logging.With(d.Logger, "AgentID", sourceStream.ID, "transactionID", msg.GetTransactionId())
	logger.Warn("agent-to-agent call refused: its caller sent more window updates than its credit allows")
	go d.sendAgentToAgentError(sourceStream, msg, codes.ResourceExhausted, errA2AWindowOverflow, logger)
}

// errA2AWindowOverflow refuses a call whose caller sent window updates past
// its credit.
const errA2AWindowOverflow = "agent-to-agent call refused: its caller sent more window updates than its credit allows"

// errA2ARequestStalled refuses an agent-to-agent call whose route took none
// of its request frames for handoff.Stall.
const errA2ARequestStalled = "agent-to-agent call refused: its route took none of its request for the stall bound"

// a2aContextFromMsg derives a context with deadline from the first message's
// Metadata.DeadlineMs. If no deadline is set, the parent context is returned.
func a2aContextFromMsg(parent context.Context, msg *pb.TunnelFrame) (context.Context, context.CancelFunc) {
	deadlineMs := msg.GetRouting().GetDeadlineMs()
	if deadlineMs > 0 {
		deadline := time.UnixMilli(deadlineMs)
		return context.WithDeadline(parent, deadline)
	}
	return context.WithCancel(parent)
}

// routeAgentToAgent manages the full lifecycle of an agent-to-agent transaction.
// It reads the first message from the session to determine the target, applies
// any deadline from the metadata, then delegates to local or remote routing.
// newA2ASession makes a transaction's session, its buffers capped at
// a2aSessionBufferCap; its context is ctx's child.
func (d *Dispatcher) newA2ASession(ctx context.Context, sourceStream *GRPCStream) (*a2aSession, context.Context) {
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	bufferSize := d.Config.RecvBufferSize
	if bufferSize <= 0 || bufferSize > a2aSessionBufferCap {
		bufferSize = a2aSessionBufferCap
	}
	return &a2aSession{
		requestCh:  make(chan *pb.TunnelFrame, bufferSize),
		responseCh: make(chan *pb.TunnelFrame, bufferSize),
		windows:    make(chan *pb.TunnelFrame, a2aWindowCap),
		cancelFn:   sessionCancel,
		source:     sourceStream,
		done:       sessionCtx.Done(),
	}, sessionCtx
}

func (d *Dispatcher) routeAgentToAgent(ctx context.Context, sourceStream *GRPCStream, session *a2aSession, txnID string) {
	defer d.a2aSessions.Delete(txnID)
	defer func() { // before the delete: the source's next frame finds one or the other
		if !session.sourceDone.Load() {
			sourceStream.a2aEnded.Store(txnID, struct{}{})
			if session.sourceDone.Load() { // its last frame came meanwhile
				sourceStream.a2aEnded.Delete(txnID)
			}
		}
	}()
	// The session context is a child of the source stream's context; release
	// it with the transaction, or a long-lived stream retains one per call.
	defer session.cancelFn()

	metrics.IncrA2AActiveSessions()
	defer metrics.DecrA2AActiveSessions()

	// Peek first message to extract routing metadata.
	firstMsg, ok := <-session.requestCh
	if !ok {
		return
	}

	targetAgentID := firstMsg.Routing.TargetAgentId
	sourceAgentID := sourceStream.ID
	callID := uuid.New().String()
	routeStart := time.Now()

	logger := logging.With(d.Logger,
		"sourceAgentID", sourceAgentID,
		"targetAgentID", targetAgentID,
		"transactionID", txnID,
		"callID", callID,
		"flow", "agent-to-agent",
	)
	if a2aFrameDeadlineExpired(firstMsg, routeStart) {
		deadline := time.UnixMilli(firstMsg.GetRouting().GetDeadlineMs())
		logger.Info("agent-to-agent transaction deadline already exceeded", "deadline", deadline, "remaining", time.Until(deadline).Round(time.Millisecond))
		metrics.RegisterA2ARequest("pre_route", "deadline_exceeded")
		metrics.RegisterA2AError("deadline_exceeded")
		metrics.RegisterA2ALatency("pre_route", float64(time.Since(routeStart).Milliseconds()))
		d.sendAgentToAgentError(sourceStream, firstMsg, codes.DeadlineExceeded, "transaction deadline exceeded", logger)
		return
	}

	// Build logger with resolved target upfront
	logger = logging.With(d.Logger,
		"sourceAgentID", sourceAgentID,
		"targetAgentID", targetAgentID,
		"transactionID", txnID,
		"callID", callID,
		"flow", "agent-to-agent",
	)
	logger.Debug("routing agent-to-agent request")
	if sourceAgentID == targetAgentID {
		logger.Info("rejecting agent-to-agent self route")
		metrics.RegisterA2ARequest("local", "denied")
		metrics.RegisterA2AError("self_route")
		d.sendAgentToAgentError(sourceStream, firstMsg, codes.InvalidArgument, "target agent must differ from source agent", logger)
		return
	}

	// Only a source Reach allows reaches the target (agent_reach.go; the
	// target is an agent ref, never a name resolved here); a refusal is the
	// answer a target that is not connected gets.
	reaches, err := d.A2AReaches(sourceAgentID, targetAgentID)
	if err != nil {
		logger.Warn("agent-to-agent request refused: the directory is unavailable", "error", err)
		metrics.RegisterA2ARequest("local", "denied")
		metrics.RegisterA2AError("directory_unavailable")
		d.sendAgentToAgentError(sourceStream, firstMsg, codes.Unavailable, "the relay's directory is unavailable", logger)
		return
	}
	if !reaches {
		logger.Warn("agent-to-agent request refused: caller does not own the target")
		metrics.RegisterA2ARequest("local", "denied")
		metrics.RegisterA2AError("tenant_denied")
		d.sendAgentToAgentError(sourceStream, firstMsg, codes.NotFound, errA2ATargetNotConnected, logger)
		return
	}

	// The target's owner pays for the call (the source's: only the owner
	// reaches its agents).
	session.meter = d.OpenMeter(metrics.PathA2A, targetAgentID)
	defer session.meter.End()

	// Apply deadline from metadata. txnCtx is a child of the session
	// context (passed as ctx), so calling session.cancelFn() from a cancel
	// message propagates cancellation into txnCtx automatically.
	txnCtx, txnCancel := a2aContextFromMsg(ctx, firstMsg)
	defer txnCancel()

	if dl, ok := txnCtx.Deadline(); ok {
		logger.Debug("transaction deadline", "deadline", dl, "remaining", time.Until(dl).Round(time.Millisecond))
	}

	firstMsg.Timestamps = &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()}

	// Resolve target agent — check local pod first, then remote.
	targetStream, local := d.Upstreams.Load(targetAgentID)
	selfRecord := false
	if !local {
		// Its record may name this replica, its stream registering in a
		// moment (finding 45): wait for it; never dial this replica itself.
		if s, self := d.upstreamArriving(txnCtx, targetAgentID); s != nil {
			targetStream, local = s, true
		} else {
			selfRecord = self
		}
	}
	if !local && d.DrainedAway(targetAgentID) {
		metrics.RegisterA2ARequest("pre_route", "draining")
		d.sendAgentToAgentError(sourceStream, firstMsg, codes.Unavailable, agentDrainingDetail, logger)
		return
	}
	if local {
		d.routeAgentToAgentLocal(txnCtx, sourceStream, targetStream, session, firstMsg, txnID, sourceAgentID, targetAgentID, routeStart, logger)
		return
	}
	if selfRecord {
		metrics.RegisterA2ARequest("local", "target_disconnected")
		metrics.RegisterA2AError("not_found")
		d.sendAgentToAgentError(sourceStream, firstMsg, codes.NotFound, errA2ATargetNotConnected, logger)
		return
	}
	logger.Debug("target agent not on local pod, dialing peer")
	peerResult, dialErr := d.dialPeerStream(txnCtx, targetAgentID, logger)
	d.routeAgentToAgentRemote(txnCtx, sourceStream, session, firstMsg, targetAgentID, sourceAgentID, routeStart, logger, peerResult, dialErr)
}

func a2aFrameDeadlineExpired(msg *pb.TunnelFrame, now time.Time) bool {
	deadlineMs := msg.GetRouting().GetDeadlineMs()
	if deadlineMs <= 0 {
		return false
	}
	return !time.UnixMilli(deadlineMs).After(now)
}

// OwnerForAgent is the account owning agentID ("" unknown): the owner of
// the principal it connected here with (registerAgent), else the account
// its directory record names (an agent on another replica, or one with
// only a ConnectTCP stream here). A record an older replica wrote names
// none, nor does an in-process agent's. A directory it could not read
// (Redis unreachable) is an error wrapping directory.ErrUnavailable, never
// "" alone: callers answer the relay's own unavailability, not the
// agent's absence (TestOwnerForAgentReportsDirectoryErrors).
func (d *Dispatcher) OwnerForAgent(agentID string) (string, error) {
	if agentID == "" {
		return "", nil
	}
	if owner, ok := d.AgentOwners.Load(agentID); ok {
		return owner, nil
	}
	var failed error
	for _, inv := range []directory.IPInventory{d.StreamerIPInventory, d.IPInventory} {
		if store, ok := inv.(directory.SessionStore); ok {
			_, s, err := store.GetSession(context.Background(), agentID) // bounded by the inventory's op timeout
			switch {
			case err == nil && s.Account != "":
				return s.Account, nil
			case errors.Is(err, directory.ErrUnavailable):
				failed = err
			}
		}
	}
	return "", failed
}

// routeAgentToAgentLocal handles N:N streaming when the target agent is on the same pod.
// It uses a per-session response channel (session.responseCh) instead of the shared
// TxnRegistry to avoid backpressure from one session affecting other transactions.
func (d *Dispatcher) routeAgentToAgentLocal(
	ctx context.Context,
	sourceStream *GRPCStream,
	targetStream Streamer,
	session *a2aSession,
	firstMsg *pb.TunnelFrame,
	transactionID string,
	sourceAgentID, targetAgentID string,
	routeStart time.Time,
	logger *slog.Logger,
) {
	logger.Debug("target agent found on local pod")

	// Register with TxnRegistry so that the target agent's responses (arriving
	// via processGRPCInput → Fanout) are received. We drain the TxnRegistry
	// handle into the per-session responseCh for backpressure isolation.
	handle, cleanup := d.TxnRegistry.Register(transactionID, ctx, logger)
	defer cleanup()

	agentBoundCtx := bindToAgentStream(ctx, targetStream)
	eg, egCtx := errgroup.WithContext(agentBoundCtx)
	responsesDone := make(chan struct{}) // goroutine 3 returned

	// Goroutine 1: forward request messages to the target agent, and the
	// caller's window updates until the responses end.
	eg.Go(func() error {
		if err := sendNewCall(targetStream, firstMsg); err != nil {
			if !errors.Is(err, errAgentDraining) {
				logger.Error("failed to send first request to target agent", "error", err)
			}
			return err
		}
		logger.Debug("sent first request to target agent")
		session.add(len(firstMsg.GetPayload()), 0)
		requests := session.requestCh
		if firstMsg.GetEom() {
			requests = nil
		}
		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case <-responsesDone:
				return nil
			case msg := <-session.windows:
				msg.Timestamps = &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()}
				if err := targetStream.Send(msg); err != nil {
					logger.Error("failed to send window update to target agent", "error", err)
					return err
				}
			case msg, ok := <-requests:
				if !ok {
					requests = nil
					continue
				}
				msg.Timestamps = &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()}
				if err := targetStream.Send(msg); err != nil {
					logger.Error("failed to send request to target agent", "error", err)
					return err
				}
				logger.Debug("sent request to target agent")
				session.add(len(msg.GetPayload()), 0)
				if msg.GetEom() {
					requests = nil
				}
			}
		}
	})

	// Goroutine 2: drain TxnRegistry handle → per-session responseCh.
	// This decouples the shared TxnRegistry buffer from the relay goroutine,
	// preventing a slow source agent from blocking other transactions' Fanout.
	eg.Go(func() error {
		defer close(session.responseCh)
		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case <-handle.Stalled:
				return txnregistry.ErrTransactionStalled
			case resp, ok := <-handle.ResponseCh:
				if !ok {
					return nil
				}
				select {
				case session.responseCh <- resp:
				case <-egCtx.Done():
					return egCtx.Err()
				case <-handle.Stalled:
					return txnregistry.ErrTransactionStalled
				}
				if resp.GetEom() {
					return nil
				}
			}
		}
	})

	// Goroutine 3: relay responses from per-session channel back to source.
	eg.Go(func() error {
		defer close(responsesDone)
		for first := true; ; first = false {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case resp, ok := <-session.responseCh:
				if !ok {
					return nil
				}
				if err := sourceStream.Send(resp); err != nil {
					logger.Error("failed to relay response to source agent", "error", err)
					return err
				}
				d.markAgentStreamAlive(sourceStream, resp)
				session.add(0, len(resp.GetPayload()))
				metrics.A2AResponse(first, routeStart)
				logger.Debug("relayed response to source agent")
				if resp.GetEom() {
					logger.Debug("agent-to-agent transaction completed (EOM)")
					return nil
				}
			}
		}
	})

	if err := eg.Wait(); err != nil {
		latencyMs := float64(time.Since(routeStart).Milliseconds())
		if errors.Is(err, errAgentDraining) {
			logger.Info("agent-to-agent target agent is draining; call not delivered")
			metrics.RegisterA2ARequest("local", "draining")
			d.sendAgentToAgentError(sourceStream, firstMsg, codes.Unavailable, agentDrainingDetail, logger)
			return
		}
		// The target agent's stream ended (worker gone): the source must learn
		// it, as on the RelayTunnel route, not be treated as the canceller.
		if errors.Is(context.Cause(agentBoundCtx), errTargetAgentDisconnected) {
			logger.Info("agent-to-agent target agent disconnected")
			metrics.RegisterA2ARequest("local", "target_disconnected")
			metrics.RegisterA2AError("target_disconnected")
			metrics.RegisterA2ALatency("local", latencyMs)
			d.sendAgentToAgentError(sourceStream, firstMsg, codes.Unavailable, errTargetAgentDisconnected.Error(), logger)
			return
		}
		if errors.Is(err, context.Canceled) {
			// Source cancelled — propagate cancellation to target.
			metrics.RegisterA2ARequest("local", "cancelled")
			metrics.RegisterA2AError("cancelled")
			metrics.RegisterA2ALatency("local", latencyMs)
			d.sendAgentToAgentCancel(targetStream, firstMsg, transactionID, logger)
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			logger.Info("agent-to-agent transaction deadline exceeded")
			metrics.RegisterA2ARequest("local", "deadline_exceeded")
			metrics.RegisterA2AError("deadline_exceeded")
			metrics.RegisterA2ALatency("local", latencyMs)
			d.sendAgentToAgentError(sourceStream, firstMsg, codes.DeadlineExceeded, "transaction deadline exceeded", logger)
			d.sendAgentToAgentCancel(targetStream, firstMsg, transactionID, logger)
			return
		}
		if errors.Is(err, errInternalAgentLoopbackBackpressure) || errors.Is(err, txnregistry.ErrTransactionStalled) {
			logger.Warn("agent-to-agent response backpressure isolated", "error", err)
			metrics.RegisterA2ARequest("local", "response_backpressure")
			metrics.RegisterA2AError("response_backpressure")
			metrics.RegisterA2ALatency("local", latencyMs)
			d.sendAgentToAgentError(sourceStream, firstMsg, codes.ResourceExhausted, err.Error(), logger)
			d.sendAgentToAgentCancel(targetStream, firstMsg, transactionID, logger)
			return
		}
		logger.Error("agent-to-agent local routing error", "error", err)
		metrics.RegisterA2ARequest("local", "error")
		metrics.RegisterA2AError("routing_error")
		metrics.RegisterA2ALatency("local", latencyMs)
		d.sendAgentToAgentError(sourceStream, firstMsg, codes.Internal, "routing error", logger)
		return
	}

	// Success
	latencyMs := float64(time.Since(routeStart).Milliseconds())
	metrics.RegisterA2ARequest("local", "ok")
	metrics.RegisterA2ALatency("local", latencyMs)
}

// sendAgentToAgentError sends an error response back to the source agent
// when routing to the target agent fails. The error is encoded as a gRPC
// status in the message body. The status_code and status_detail fields in
// The status_code and status_detail fields provide structured error information matching gRPC semantics.
func (d *Dispatcher) sendAgentToAgentError(
	sourceStream *GRPCStream,
	originalMsg *pb.TunnelFrame,
	code codes.Code,
	detail string,
	logger *slog.Logger,
) {
	errStatus := status.New(code, detail)
	errBytes, _ := proto.Marshal(errStatus.Proto())

	errMsg := &pb.TunnelFrame{
		Payload:       errBytes,
		MessageType:   msgTypeError,
		TransactionId: originalMsg.GetTransactionId(),
		Timestamps:    &pb.Timestamps{ResponseAtGrpcServer: time.Now().UnixMilli()},
		Routing: &pb.RoutingInfo{
			AgentId:       originalMsg.GetRouting().GetTargetAgentId(),
			TargetAgentId: originalMsg.GetRouting().GetAgentId(),
		},
		Eom:          true,
		StatusCode:   int32(code), // #nosec G115 -- gRPC codes are 0..16
		StatusDetail: detail,
	}

	if err := sourceStream.Send(errMsg); err != nil {
		logger.Error("failed to send error response to source agent", "error", err)
	}
}

// sendAgentToAgentCancel sends a cancellation message to the target agent,
// propagating the source agent's context cancellation so the target can stop
// processing the transaction.
func (d *Dispatcher) sendAgentToAgentCancel(
	targetStream Streamer,
	originalMsg *pb.TunnelFrame,
	transactionID string,
	logger *slog.Logger,
) {
	cancelMsg := &pb.TunnelFrame{
		MessageType:   msgTypeCancel,
		TransactionId: transactionID,
		Timestamps:    &pb.Timestamps{ResponseAtGrpcServer: time.Now().UnixMilli()},
		Routing: &pb.RoutingInfo{
			AgentId:       originalMsg.GetRouting().GetAgentId(),
			TargetAgentId: originalMsg.GetRouting().GetTargetAgentId(),
		},
		Eom:          true,
		StatusCode:   int32(codes.Canceled),
		StatusDetail: "cancelled by source agent",
	}

	if err := targetStream.Send(cancelMsg); err != nil {
		logger.Error("failed to send cancellation to target agent", "error", err)
	} else {
		logger.Info("sent cancellation to target agent")
	}
}
