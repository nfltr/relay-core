// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The relay answers an agent-to-agent call NotFound only when it never
// delivered the call's first frame to its target: nfltr's hub reads NotFound
// before any reply on a first dispatch as "not delivered" and places the work
// on another worker, so a NotFound after delivery would run it twice.
// Each case drives
// a task stream (task.assign, request not ended) from the
// hub's agent on the source replica and reads the status the source gets.
func TestA2ANotFoundOnlyForUndeliveredCalls(t *testing.T) {
	cases := []struct {
		name string
		// remote: the target is on a peer replica (an in-process relay
		// tunnel, as the replica link carries one), else on the source's.
		remote, targetMissing, replyFirst bool
		// lose ends the call after delivery: the target's stream ends, or
		// the replica hop is cut.
		lose          string
		wantNotFound  bool
		wantDelivered bool
	}{
		{name: "undelivered/target on no replica", targetMissing: true, wantNotFound: true},
		{name: "undelivered/peer replica holds no target", remote: true, targetMissing: true, wantNotFound: true},
		{name: "delivered/same replica/target drops before replying", lose: "target", wantDelivered: true},
		{name: "delivered/same replica/target drops after a reply", replyFirst: true, lose: "target", wantDelivered: true},
		{name: "delivered/peer replica/target drops before replying", remote: true, lose: "target", wantDelivered: true},
		{name: "delivered/peer replica/target drops after a reply", remote: true, replyFirst: true, lose: "target", wantDelivered: true},
		{name: "delivered/peer replica/replica hop cut", remote: true, lose: "hop", wantDelivered: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			src, srcCancel := newTestDispatcher(t)
			defer srcCancel()
			defer viper.Reset()

			holder := src // the replica the target is on
			var cut chan struct{}
			if tc.remote {
				client, peer, cleanup := newA2ABufConnEnv(t)
				defer cleanup()
				cut = make(chan struct{})
				src.PodDialer = &cuttablePodDialer{inner: relayOnlyPodDialer{client: client, ctx: ctx}, cut: cut}
				holder = peer
			}
			targetCtx, dropTarget := context.WithCancel(ctx)
			defer dropTarget()
			target := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: targetCtx}
			if !tc.targetMissing {
				holder.Upstreams.Store("worker", newTestGRPCStream("worker", target, targetCtx))
			}

			source := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
			src.dispatchAgentToAgent(ctx, newTestGRPCStream("hub", source, ctx), &pb.TunnelFrame{
				Payload: []byte(`{"type":"task.assign"}`), MessageType: "a2a-request", TransactionId: "txn-" + t.Name(),
				Routing: &pb.RoutingInfo{TargetAgentId: "worker"},
			})

			if tc.wantDelivered {
				select {
				case f := <-target.sendCh:
					if f.GetTransactionId() != "txn-"+t.Name() {
						t.Fatalf("the target got %v", f)
					}
				case <-ctx.Done():
					t.Fatal("the call never reached its target")
				}
				if tc.replyFirst {
					if err := holder.TxnRegistry.Fanout("txn-"+t.Name(), &pb.TunnelFrame{MessageType: "a2a-response", TransactionId: "txn-" + t.Name(), Payload: []byte("accepted")}, holder.Logger); err != nil {
						t.Fatal(err)
					}
				}
				switch tc.lose {
				case "target":
					dropTarget()
				case "hop":
					close(cut)
				}
			}

			for {
				select {
				case f := <-source.sendCh:
					if f.GetMessageType() != msgTypeError {
						continue // the target's reply
					}
					code := codes.Code(f.GetStatusCode())
					t.Logf("source got %v %q", code, f.GetStatusDetail())
					if gotNotFound := code == codes.NotFound; gotNotFound != tc.wantNotFound {
						t.Fatalf("source got %v %q; want NotFound=%v (NotFound only for a call never delivered)", code, f.GetStatusDetail(), tc.wantNotFound)
					}
					return
				case <-ctx.Done():
					t.Fatal("the source got no status for its call")
				}
			}
		})
	}
}

// cuttablePodDialer dials as inner does; closing cut breaks every stream it
// dialed as a dead replica link does (linkRelayClient: Unavailable "stream
// cut"), after whatever the stream already carried.
type cuttablePodDialer struct {
	inner relayOnlyPodDialer
	cut   chan struct{}
}

func (d *cuttablePodDialer) DialRelay(ctx context.Context, agentID string, logger *slog.Logger) (*PodRelayStream, error) {
	ps, err := d.inner.DialRelay(ctx, agentID, logger)
	if err != nil {
		return nil, err
	}
	ps.stream = &cutStream{relayPipe: ps.stream.(*relayPipe), cut: d.cut}
	return ps, nil
}

type cutStream struct {
	*relayPipe
	cut chan struct{}
}

func (s *cutStream) Recv() (*pb.TunnelFrame, error) {
	type res struct {
		f   *pb.TunnelFrame
		err error
	}
	ch := make(chan res, 1)
	go func() { f, err := s.relayPipe.Recv(); ch <- res{f, err} }()
	select {
	case r := <-ch:
		return r.f, r.err
	case <-s.cut:
		return nil, status.Error(codes.Unavailable, "peer link: stream cut")
	}
}
