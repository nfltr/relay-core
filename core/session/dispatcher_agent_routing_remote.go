// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay/proto/pb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// routeAgentToAgentRemote handles N:N streaming when the target agent is on a
// different pod, over the peer stream the caller dialed (or its dial error).
func (d *Dispatcher) routeAgentToAgentRemote(
	ctx context.Context,
	sourceStream *GRPCStream,
	session *a2aSession,
	firstMsg *pb.TunnelFrame,
	targetAgentID string,
	sourceAgentID string,
	routeStart time.Time,
	logger *slog.Logger,
	peerResult *PodRelayStream,
	err error,
) {
	if err != nil {
		result := "error"
		if status.Code(err) == codes.NotFound {
			result = "target_disconnected" // the target is offline, as on the local path (ERROR_REASON_AGENT_OFFLINE)
			// codes.NotFound covers both "no pod dialer configured" (single-pod
			// deployments, e.g. the standalone GCE VM, where cross-pod relay is
			// never expected to work at all) and a genuinely unregistered
			// agent. Neither is a pod-to-pod connection failure worth
			// alerting on, so this stays Debug-only, matching the existing
			// severity split below.
			logger.Debug("failed to dial peer for target agent", "error", err)
		} else {
			// Every other error here means a real multi-pod dial genuinely
			// failed (e.g. the replica link could not reach a dead peer
			// pod) — found via a live
			// chaostoolkit fault-injection run
			// (docker/chaostoolkit/experiments) that showed
			// dispatcher_pod_to_pod_connection_failures_total never firing
			// for exactly this path: it is only ever registered when an
			// already-established peer stream breaks
			// (cleanupPeerStream / the response-relay goroutine in this
			// file), never when the initial dial itself fails to establish
			// one. A cross-pod relay that can never even connect is exactly
			// the failure this metric exists to surface.
			logger.Error("failed to dial peer for target agent", "error", err)
			metrics.RegisterPodToPodConnectionFailure()
		}
		metrics.RegisterA2ARequest("remote", result)
		metrics.RegisterA2AError("not_found")
		metrics.RegisterA2ALatency("remote", float64(time.Since(routeStart).Milliseconds()))
		d.sendAgentToAgentError(sourceStream, firstMsg, codes.NotFound, "target agent not connected", logger)
		return
	}
	peerStream := peerResult.stream
	defer func() {
		if cs, ok := peerStream.(interface{ CloseSend() error }); ok {
			_ = cs.CloseSend()
		}
	}()

	eg, egCtx := errgroup.WithContext(ctx)

	// Goroutine 1: forward request messages to the remote pod.
	eg.Go(func() error {
		if err := peerStream.Send(firstMsg); err != nil {
			if err == io.EOF {
				return nil // the peer ended the stream; Recv surfaces its status
			}
			logStreamLifecycleWithLogger(logger, "failed to send first request to remote pod", err)
			return fmt.Errorf("%v: %w", err, podToPodStreamErr)
		}
		logger.Debug("sent first request to remote pod")
		if firstMsg.GetEom() {
			return nil
		}
		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case msg, ok := <-session.requestCh:
				if !ok {
					return nil
				}
				if err := peerStream.Send(msg); err != nil {
					if err == io.EOF {
						return nil // the peer ended the stream; Recv surfaces its status
					}
					logStreamLifecycleWithLogger(logger, "failed to send request to remote pod", err)
					return fmt.Errorf("%v: %w", err, podToPodStreamErr)
				}
				logger.Debug("sent request to remote pod")
				if msg.GetEom() {
					return nil
				}
			}
		}
	})

	// Goroutine 2: relay responses from the remote pod back to the source agent.
	eg.Go(func() error {
		for {
			resp, err := peerStream.Recv()
			if err != nil {
				if err == io.EOF {
					logger.Debug("peer stream closed (EOF)")
					return nil
				}
				if egCtx.Err() != nil {
					return egCtx.Err()
				}
				if st, ok := peerAnsweredStatus(err); ok {
					return &peerAnswerError{st: st}
				}
				logStreamLifecycleWithLogger(logger, "failed to receive response from remote pod", err)
				if !isExpectedStreamDisconnect(err) {
					metrics.RegisterPodToPodConnectionFailure()
				}
				return fmt.Errorf("%v: %w", err, podToPodStreamErr)
			}
			if err := sourceStream.Send(resp); err != nil {
				logger.Error("failed to relay remote response to source agent", "error", err)
				return err
			}
			d.markAgentStreamAlive(sourceStream, resp)
			logger.Debug("relayed remote response to source agent")
			if resp.GetEom() {
				logger.Debug("agent-to-agent transaction completed via remote pod (EOM)")
				return nil
			}
		}
	})

	if err := eg.Wait(); err != nil {
		latencyMs := float64(time.Since(routeStart).Milliseconds())
		if errors.Is(err, context.Canceled) {
			// Cancellation is propagated via the peer stream context.
			metrics.RegisterA2ARequest("remote", "cancelled")
			metrics.RegisterA2AError("cancelled")
			metrics.RegisterA2ALatency("remote", latencyMs)
			return
		}
		if isExpectedStreamDisconnect(err) {
			logStreamLifecycleWithLogger(logger, "agent-to-agent remote route closed during teardown", err)
			metrics.RegisterA2ARequest("remote", "cancelled")
			metrics.RegisterA2ALatency("remote", latencyMs)
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			logger.Info("agent-to-agent remote transaction deadline exceeded")
			metrics.RegisterA2ARequest("remote", "deadline_exceeded")
			metrics.RegisterA2AError("deadline_exceeded")
			metrics.RegisterA2ALatency("remote", latencyMs)
			d.sendAgentToAgentError(sourceStream, firstMsg, codes.DeadlineExceeded, "transaction deadline exceeded", logger)
			return
		}
		if answer, ok := errors.AsType[*peerAnswerError](err); ok {
			// The peer relay answered (e.g. NotFound: the worker is not
			// there): relay its typed status as a local route would, and keep
			// the healthy peer's pooled connection.
			logger.Info("peer relay answered agent-to-agent request", "code", answer.st.Code(), "detail", answer.st.Message())
			metrics.RegisterA2ARequest("remote", "error")
			metrics.RegisterA2AError("peer_answered")
			metrics.RegisterA2ALatency("remote", latencyMs)
			d.sendAgentToAgentError(sourceStream, firstMsg, answer.st.Code(), answer.st.Message(), logger)
			return
		}
		logger.Error("agent-to-agent remote routing error", "error", err)
		metrics.RegisterA2ARequest("remote", "error")
		metrics.RegisterA2AError("remote_routing_error")
		metrics.RegisterA2ALatency("remote", latencyMs)
		d.cleanupPeerStream(peerResult, targetAgentID, fmt.Errorf("%v: %w", err, podToPodStreamErr), logger)
		d.sendAgentToAgentError(sourceStream, firstMsg, codes.Internal, "remote routing error", logger)
		return
	}

	// Success
	latencyMs := float64(time.Since(routeStart).Milliseconds())
	metrics.RegisterA2ARequest("remote", "ok")
	metrics.RegisterA2ALatency("remote", latencyMs)
}
