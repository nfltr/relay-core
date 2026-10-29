// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcfactory

import (
	"context"
	"net"
	"testing"

	"github.com/nfltr/relay/client/grpcclient"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// TestClientGenerationGate: with security.min_client_generation 2 a call
// without generation 2 (nfltr before v2.0.0: a Hello, no generation) is
// refused at connect with the update message and counted; a current client
// (grpcclient.ClientIdentity) is taken; with 0 both are taken and the old
// one is counted.
func TestClientGenerationGate(t *testing.T) {
	const method = "/test.Gen/Open"
	for _, min := range []int{0, 2} {
		s := grpc.NewServer(ProtocolOptions(min)...)
		s.RegisterService(&grpc.ServiceDesc{
			ServiceName: "test.Gen",
			HandlerType: (*any)(nil),
			Streams: []grpc.StreamDesc{{
				StreamName:    "Open",
				ClientStreams: true,
				Handler:       func(any, grpc.ServerStream) error { return nil },
			}},
		}, struct{}{})
		lis := bufconn.Listen(1 << 20)
		go func() { _ = s.Serve(lis) }()
		dial := []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
		call := func(ctx context.Context, opts ...grpc.DialOption) error {
			conn, err := grpc.NewClient("passthrough:///bufnet", append(dial, opts...)...)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close() //nolint:errcheck
			st, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true}, method)
			if err != nil {
				return err
			}
			_ = st.CloseSend()
			return st.RecvMsg(new(struct{}))
		}
		refused, taken := counter(t, "rpc_server_dispatcher_clients_refused_total", "reason", "generation"),
			counter(t, "rpc_server_dispatcher_old_generation_clients_total", "transport", "grpc")

		// A client before v2.0.0: its Hello, no generation.
		old := metadata.AppendToOutgoingContext(context.Background(), agentwire.Self().HelloPairs()...)
		err := call(old)
		switch {
		case min == 2 && (status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != agentwire.ClientGenerationTooOld):
			t.Errorf("min 2, old client: %v; want FailedPrecondition %q", err, agentwire.ClientGenerationTooOld)
		case min == 0 && status.Code(err) == codes.FailedPrecondition:
			t.Errorf("min 0, old client refused: %v", err)
		}
		if min == 2 && counter(t, "rpc_server_dispatcher_clients_refused_total", "reason", "generation") != refused+1 {
			t.Error("the refusal was not counted")
		}
		if min == 0 && counter(t, "rpc_server_dispatcher_old_generation_clients_total", "transport", "grpc") != taken+1 {
			t.Error("the old client was not counted")
		}
		// A current client.
		if err := call(context.Background(), grpcclient.ClientIdentity()...); status.Code(err) == codes.FailedPrecondition {
			t.Errorf("min %d, current client refused: %v", min, err)
		}
		s.Stop()
	}
}

// counter is the value of a counter with one label.
func counter(t *testing.T, name, label, value string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
