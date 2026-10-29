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
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// TestServerCountsStreamsByClient: a stream opened through a client dial
// (grpcclient.ClientIdentity) is counted under the client's name and
// version while it is open.
func TestServerCountsStreamsByClient(t *testing.T) {
	defer viper.Reset()
	const method = "/test.Client/Open"
	s := NewGRPCServer(testLogger())
	var open float64
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Client",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName:    "Open",
			ClientStreams: true,
			Handler: func(_ any, _ grpc.ServerStream) error {
				open = clientStreamsOpen(t, method, agentwire.Self())
				return nil
			},
		}},
	}, struct{}{})
	lis := bufconn.Listen(1 << 20)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	opts := append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, grpcclient.ClientIdentity()...)
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	st, err := conn.NewStream(context.Background(), &grpc.StreamDesc{ClientStreams: true}, method)
	if err != nil {
		t.Fatal(err)
	}
	_ = st.CloseSend()
	_ = st.RecvMsg(new(struct{}))
	if open != 1 {
		t.Fatalf("open streams of %+v while open = %v, want 1", agentwire.Self(), open)
	}
}

func clientStreamsOpen(t *testing.T, method string, c agentwire.Client) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "rpc_server_dispatcher_client_streams" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["method"] == method && labels["client_name"] == c.Name {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}
