// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/chunking"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

// ctxProducer delivers only while the send's ctx is live, as a broker that
// honours ctx does (the in-memory broker drops a send on an ended ctx
// without an error, at random).
type ctxProducer struct {
	fake.TestProducer
	fail bool
}

func (p *ctxProducer) SendProto(ctx context.Context, msg proto.Message) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if p.fail {
		return "", errors.New("broker down")
	}
	return p.TestProducer.SendProto(ctx, msg)
}

func lastChunk(txn string) *pb.HTTPProxyResponse {
	h := map[string]*pb.HeaderValueList{}
	chunking.SetChunkHeaders(h, 1, true)
	return &pb.HTTPProxyResponse{Response: &pb.HTTPResponse{TransactionId: txn, Headers: h, Payload: []byte("end")}, Timestamps: &pb.Timestamps{}}
}

// TestEndedSessionStillDeliversWhatItsAgentSent: the receive loop drains
// an ended session's last messages with its ended ctx; the agent's last
// chunk still reaches the waiting proxy (2026-10-15: dropped, the visitor
// waited for its timeout).
func TestEndedSessionStillDeliversWhatItsAgentSent(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()
	p := &ctxProducer{}
	d.podIDTopicProducer = p
	stream := NewHTTPStream("agentID-1", &mockHTTPStreamer{}, nil, make(chan proto.Message, 1), time.NewTicker(time.Minute))
	stream.inflight.Store("txn-1", struct{}{})
	ended, end := context.WithCancel(t.Context())
	end()
	if err := d.processHTTPInput(ended, stream, lastChunk("txn-1"), time.Now()); err != nil {
		t.Fatalf("not delivered: %v", err)
	}
	if got, _ := p.LastData.(*pb.HTTPProxyResponse); got.GetResponse().GetTransactionId() != "txn-1" {
		t.Fatalf("delivered %v", p.LastData)
	}
	if _, ok := stream.inflight.Load("txn-1"); ok {
		t.Fatal("a delivered last chunk left its transaction in flight")
	}
}

// TestUndeliveredLastChunkStaysInFlight: a last chunk that could not be
// delivered leaves its transaction to failInflight (agent-gone), so the
// visitor's response is cut, not left waiting.
func TestUndeliveredLastChunkStaysInFlight(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()
	d.podIDTopicProducer = &ctxProducer{fail: true}
	stream := NewHTTPStream("agentID-1", &mockHTTPStreamer{}, nil, make(chan proto.Message, 1), time.NewTicker(time.Minute))
	stream.inflight.Store("txn-1", struct{}{})
	if err := d.processHTTPInput(t.Context(), stream, lastChunk("txn-1"), time.Now()); err == nil {
		t.Fatal("delivered by a broker that is down")
	}
	if _, ok := stream.inflight.Load("txn-1"); !ok {
		t.Fatal("an undelivered last chunk ended its transaction: failInflight cannot answer it")
	}
}
