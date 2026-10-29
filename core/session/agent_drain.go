// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"log/slog"
	"time"

	"github.com/nfltr/relay/proto/pb"
)

// Agent drain: an
// agent leaving for good (an on-demand worker process that finished) sends
// agentDrainMessageType before it closes its stream. The relay then forwards
// no new call to it and answers one with agentDrainingDetail
// (codes.Unavailable), which tells the caller the call never reached the
// agent, so it can place the work elsewhere instead of treating the call as
// delivered and lost; it keeps doing so after the agent left, until the id
// connects again. The acknowledgement is sent on the agent's stream after
// every new call already forwarded, so the agent answers each of them
// before it closes. Wire constants mirror pkg/rpcclient (parity-tested).
const (
	agentDrainMessageType    = "agent.drain"
	agentDrainAckMessageType = "agent.drain.ack"
	agentDrainingDetail      = "target agent is draining: it takes no new call and is leaving"
	// drainedAgentTTL bounds how long a drained, departed agent id refuses
	// calls instead of waiting for it to register again.
	drainedAgentTTL = 10 * time.Minute
)

var errAgentDraining = errors.New(agentDrainingDetail)

// sendNewCall sends the first frame of a new call to target, refused with
// errAgentDraining once the target drains (checked and sent under the
// stream's send lock, so no first frame follows the drain acknowledgement).
func sendNewCall(target Streamer, frame *pb.TunnelFrame) error {
	if s, ok := target.(*GRPCStream); ok {
		s.sendMut.Lock()
		defer s.sendMut.Unlock()
		if s.draining {
			return errAgentDraining
		}
		if err := s.IO.Send(frame); err != nil {
			return err
		}
		s.lastOutboundDataSentAt.Store(time.Now().UnixNano())
		return nil
	}
	return target.Send(frame)
}

// drain marks the stream draining and sends ack under the send lock.
func (a *Stream[S, R]) drain(ack S) error {
	a.sendMut.Lock()
	defer a.sendMut.Unlock()
	a.draining = true
	return a.IO.Send(ack)
}

// isDraining reports whether the agent declared it is leaving.
func (a *Stream[S, R]) isDraining() bool {
	a.sendMut.Lock()
	defer a.sendMut.Unlock()
	return a.draining
}

// handleAgentDrain applies an agent's drain request and acknowledges it.
func (d *Dispatcher) handleAgentDrain(stream *GRPCStream, input *pb.TunnelFrame, logger *slog.Logger) error {
	logger.Info("agent draining: no new calls are routed to it")
	return stream.drain(&pb.TunnelFrame{
		MessageType: agentDrainAckMessageType, TransactionId: input.GetTransactionId(), Eom: true,
		Routing:    &pb.RoutingInfo{AgentId: stream.ID},
		Timestamps: &pb.Timestamps{ResponseAtGrpcServer: time.Now().UnixMilli()},
	})
}

// noteAgentDeparture remembers an agent that left after draining, so a call
// to it is refused at once instead of waiting for a registration that will
// not come; a connect of the id forgets it.
// ponytail: expired ids are pruned on each departure, O(departures in TTL).
func (d *Dispatcher) noteAgentDeparture(agentID string, stream *GRPCStream) {
	if cur, ok := d.Upstreams.Load(agentID); !ok || cur != Streamer(stream) || !stream.isDraining() {
		return // a newer connection of the id holds it
	}
	now := time.Now()
	d.drainedAgents.Range(func(k, v any) bool {
		if now.Sub(v.(time.Time)) > drainedAgentTTL {
			d.drainedAgents.Delete(k)
		}
		return true
	})
	d.drainedAgents.Store(agentID, now)
}

// DrainedAway reports an agent id that drained and left within the TTL
// (calls to it are refused as draining).
func (d *Dispatcher) DrainedAway(agentID string) bool {
	v, ok := d.drainedAgents.Load(agentID)
	return ok && time.Since(v.(time.Time)) <= drainedAgentTTL
}
