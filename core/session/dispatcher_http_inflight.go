// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// In-flight HTTP transactions of a ConnectHTTP session: a request handed to
// the agent's stream is in flight until the agent sends its last response
// chunk or the relay cancels it. When the session ends first (the agent
// closed, its stream died, a reconnect replaced it), the relay answers each
// one it still holds with an agent-gone response, so the visitor's response
// ends at once and visibly (Proxy: 502 before the headers, a cut connection
// after them), never by the idle timeout and never as a clean end of a
// truncated body.

import (
	"context"
	"net/http"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/chunking"
)

// agentGoneHeader marks the response the dispatcher writes for a
// transaction whose agent session ended before its last chunk. Only the
// relay writes it (on its own pod topic); an agent's copy is dropped
// (processHTTPInput).
const agentGoneHeader = "X-Nfltr-Relay-Agent-Gone"

// agentGoneDeliveryBound bounds publishing the agent-gone responses of an
// ended session.
const agentGoneDeliveryBound = 5 * time.Second

// trackRequest records a request handed to the agent: its first message
// starts the transaction, a cancel ends it.
func trackRequest(s *HTTPStream, req *pb.HTTPRequest) {
	h := req.GetHeaders()
	switch {
	case req.GetTransactionId() == "":
	case chunking.IsCancel(h):
		s.inflight.Delete(req.GetTransactionId())
	case chunking.IsControl(h):
	case chunking.GetChunkIndex(h) == 0:
		s.inflight.Store(req.GetTransactionId(), struct{}{})
	}
}

// endsTransaction reports whether resp is an agent's last response chunk.
func endsTransaction(resp *pb.HTTPResponse) bool {
	h := resp.GetHeaders()
	return !IsUploadAck(resp) && (!chunking.IsChunked(h) || chunking.IsLastChunk(h))
}

// failInflight answers every transaction s still holds with an agent-gone
// response.
func (d *Dispatcher) failInflight(s *HTTPStream) {
	ctx, cancel := context.WithTimeout(context.Background(), agentGoneDeliveryBound)
	defer cancel()
	s.inflight.Range(func(k, _ any) bool {
		s.inflight.Delete(k)
		txn := k.(string)
		if err := d.routeResponseToBroker(ctx, agentGoneResponse(s.ID, txn)); err != nil {
			d.Logger.Warn("agent-gone response not delivered", "AgentID", s.ID, "transaction_id", txn, "error", err)
		}
		return true
	})
}

// failQueued answers, with an agent-gone response, each request the
// session's closed consumer still holds: routed here as the session ended,
// it never reached the agent. Its visitor gets the answer at once, not the
// request timeout.
func (d *Dispatcher) failQueued(s *HTTPStream, queued <-chan broker.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), agentGoneDeliveryBound)
	defer cancel()
	for m := range queued {
		req, err := broker.DecodeBrokerMessage(m, &pb.HTTPProxyRequest{})
		h := req.GetRequest().GetHeaders()
		txn := req.GetRequest().GetTransactionId()
		if err != nil || txn == "" || chunking.IsControl(h) || chunking.IsCancel(h) || chunking.GetChunkIndex(h) != 0 {
			continue // a later chunk or a control frame: its transaction is answered once
		}
		s.inflight.Delete(txn)
		if err := d.routeResponseToBroker(ctx, agentGoneResponse(s.ID, txn)); err != nil {
			d.Logger.Warn("agent-gone response not delivered", "AgentID", s.ID, "transaction_id", txn, "error", err)
		}
	}
}

func agentGoneResponse(agentID, transactionID string) *pb.HTTPProxyResponse {
	return &pb.HTTPProxyResponse{
		Response: &pb.HTTPResponse{
			TransactionId: transactionID,
			StatusCode:    http.StatusBadGateway,
			Headers:       map[string]*pb.HeaderValueList{agentGoneHeader: {HeaderValues: []string{"1"}}},
		},
		Timestamps: &pb.Timestamps{},
		Routing:    &pb.RoutingInfo{AgentId: agentID},
	}
}

// IsAgentGone reports whether r is a dispatcher's agent-gone response.
func IsAgentGone(r *pb.HTTPResponse) bool {
	_, ok := r.GetHeaders()[agentGoneHeader]
	return ok
}
