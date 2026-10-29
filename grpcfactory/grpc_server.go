// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package grpcfactory builds the relay's gRPC server for agents
// (NewGRPCServer): the protocol handshake (ProtocolOptions), logging,
// request ids, validation, optional authentication with the core's
// provider chain (WithAuthenticator) and keepalive (TransportOptions).
package grpcfactory

import (
	"log/slog"
	"time"

	"github.com/nfltr/relay-core/grpcauth"

	"github.com/nfltr/relay-core/core/identity"

	"google.golang.org/grpc"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/keepalive"

	grpc_prom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/nfltr/relay-core/internal/platform/requestid"
	"github.com/nfltr/relay-core/platform/clientgen"
	appconfig "github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay/proto/wire/grpccodec"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
)

// Relay keepalive constants shared between the gRPC server and any client
// (e.g. worker/agent) so both sides stay in sync.
//
// The server's EnforcementPolicy.MinTime must be ≤ the client's ping
// interval.  iter-61 sCCC9 pings every 30 s, so MinTime is 25 s (5 s slack).
const (
	// RelayKeepaliveMinTime is the minimum interval the server requires
	// between successive client pings (EnforcementPolicy.MinTime).
	RelayKeepaliveMinTime = 25 * time.Second

	// RelayKeepaliveServerTime is the interval at which the server sends
	// its own keepalive pings to connected clients (ServerParameters.Time).
	RelayKeepaliveServerTime = 30 * time.Second

	// RelayKeepaliveServerTimeout is how long the server waits for a ping
	// ACK before closing the connection (ServerParameters.Timeout).
	RelayKeepaliveServerTimeout = 10 * time.Second
)

// GRPCServerOption configures the gRPC server created by NewGRPCServer.
type GRPCServerOption func(*grpcServerConfig)

type grpcServerConfig struct {
	auth      *identity.Authenticator
	peerCerts bool
}

// WithPeerCertificates gives handlers each connection's TLS client
// certificate (grpcauth.TLSStateCredentials), for an identity provider
// that authenticates certificates (mTLS).
func WithPeerCertificates() GRPCServerOption {
	return func(cfg *grpcServerConfig) { cfg.peerCerts = true }
}

// WithAuthenticator authenticates a call's credential with the relay core's
// provider chain and holds every stream under its credential
// (grpcauth.AuthStreamInterceptor).
func WithAuthenticator(auth *identity.Authenticator) GRPCServerOption {
	return func(cfg *grpcServerConfig) { cfg.auth = auth }
}

// NewGRPCServer creates a gRPC server with logging, request-ID, validation,
// optional API key auth, and keepalive configuration.
func NewGRPCServer(logger *slog.Logger, opts ...GRPCServerOption) *grpc.Server {
	var cfg grpcServerConfig
	for _, o := range opts {
		o(&cfg)
	}
	// Setup metrics — register in the default Prometheus registry so they are scraped.
	// Use Register (not MustRegister) to tolerate repeated calls in tests.
	// grpc_server_handling_seconds: a unary call's handling time, a
	// stream's lifetime (past 60 s in the +Inf bucket).
	srvMetrics := grpc_prom.NewServerMetrics(grpc_prom.WithServerHandlingTimeHistogram(grpc_prom.WithHistogramBuckets(metrics.SecondsBuckets)))
	if err := prometheus.Register(srvMetrics); err != nil {
		// If metrics were already registered (a relay started again in
		// this process), count into the registered collectors: a new,
		// unregistered one would leave this server's calls off /metrics.
		are, ok := err.(prometheus.AlreadyRegisteredError)
		if !ok {
			panic(err) // unexpected registration error
		}
		srvMetrics = are.ExistingCollector.(*grpc_prom.ServerMetrics)
	}

	unaryInterceptors := []grpc.UnaryServerInterceptor{
		// Credential authentication by the provider chain (a key, a
		// bearer token, a client certificate). No-op without one.
		grpcauth.AuthUnaryInterceptor(cfg.auth),

		// request-Id interceptor
		requestid.UnaryServerInterceptor(),

		// validation middleware
		grpc_validator.UnaryServerInterceptor(),
	}

	// DEV ONLY (security.trust_loopback): trust loopback peers as a local
	// principal with no credential. Runs FIRST so the injected agentID is
	// visible to every downstream interceptor and to
	// GetAgentIDFromContext. Off by default; loopback-only.
	if viper.GetBool("security.trust_loopback") {
		unaryInterceptors = append([]grpc.UnaryServerInterceptor{grpcauth.TrustLoopbackUnaryInterceptor()}, unaryInterceptors...)
	}

	unaryInterceptors = append(
		unaryInterceptors,

		// prometheus metrics
		srvMetrics.UnaryServerInterceptor(),
	)

	serverInterceptors := []grpc.StreamServerInterceptor{
		// Credential authentication, and the stream held under its credential (a
		// revocation or expiry ends it).
		grpcauth.AuthStreamInterceptor(cfg.auth),

		// request-Id interceptor
		requestid.StreamServerInterceptor(),

		// validation middleware
		grpc_validator.StreamServerInterceptor(),
	}

	// DEV ONLY (security.trust_loopback): stream counterpart — see the unary note.
	if viper.GetBool("security.trust_loopback") {
		serverInterceptors = append([]grpc.StreamServerInterceptor{grpcauth.TrustLoopbackStreamInterceptor()}, serverInterceptors...)
	}

	serverInterceptors = append(
		serverInterceptors,

		// prometheus metrics
		srvMetrics.StreamServerInterceptor(),

		// open streams by client name and version (after authentication, so
		// only authenticated clients name versions)
		clientStreamsInterceptor,
	)

	// Identity metadata is server-asserted: drop client copies before any
	// interceptor (trust-loopback, key validation) or handler reads it.
	unaryInterceptors = append([]grpc.UnaryServerInterceptor{grpcauth.StripAssertedIdentityUnaryInterceptor()}, unaryInterceptors...)
	serverInterceptors = append([]grpc.StreamServerInterceptor{grpcauth.StripAssertedIdentityStreamInterceptor()}, serverInterceptors...)

	// The protocol v1 Hello comes first of all: a pre-v1 client is told to
	// update, not refused for whatever else it sends.
	grpcOpts := append(ProtocolOptions(clientgen.Min()),
		grpc.ChainUnaryInterceptor(
			unaryInterceptors...,
		),
		grpc.ChainStreamInterceptor(
			serverInterceptors...,
		),
	)
	grpcOpts = append(grpcOpts, TransportOptions()...)
	if cfg.peerCerts {
		grpcOpts = append(grpcOpts, grpc.Creds(grpcauth.TLSStateCredentials()))
	}

	// Limit concurrent streams per connection to prevent stream-explosion DoS.
	if maxStreams := viper.GetUint32("grpc.max_concurrent_streams"); maxStreams > 0 {
		grpcOpts = append(grpcOpts, grpc.MaxConcurrentStreams(maxStreams))
	}

	// Always apply keepalive enforcement so the relay never rejects fast
	// client pings (e.g. iter-61 sCCC9 at 30 s) with RST_STREAM/too_many_pings.
	// Viper overrides are honoured when explicitly set; the relay-tuned
	// constants above are the fallback for all production deployments.
	minTime := RelayKeepaliveMinTime
	if d := viper.GetDuration("grpc.keepalive.enforcement_policy.min_time"); d > 0 {
		minTime = d
	}
	permitWithoutStream := true
	if viper.IsSet("grpc.keepalive.enforcement_policy.permit_without_stream") {
		permitWithoutStream = viper.GetBool("grpc.keepalive.enforcement_policy.permit_without_stream")
	}
	grpcOpts = append(grpcOpts,
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             minTime,
			PermitWithoutStream: permitWithoutStream,
		}),
	)

	srvTime := RelayKeepaliveServerTime
	if d := viper.GetDuration("grpc.keepalive.server_parameters.time"); d > 0 {
		srvTime = d
	}
	srvTimeout := RelayKeepaliveServerTimeout
	if d := viper.GetDuration("grpc.keepalive.server_parameters.timeout"); d > 0 {
		srvTimeout = d
	}
	grpcOpts = append(grpcOpts,
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    srvTime,
			Timeout: srvTimeout,
		}),
	)

	grpcServer := grpc.NewServer(
		grpcOpts...,
	)

	srvMetrics.InitializeMetrics(grpcServer)
	return grpcServer
}

// TransportOptions are the server's data-path transport settings (message
// size, codec, HTTP/2 flow-control windows). The TCP tunnel data-path
// harness (relay package tests) serves with exactly these, so its numbers
// track production.
func TransportOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(appconfig.GRPCMaxMessageSizeBytes()),
		grpc.MaxSendMsgSize(appconfig.GRPCMaxMessageSizeBytes()),
		// Body chunks (8 MiB) skip grpc-go's shared buffer pool, which kept
		// idle 8 MiB buffers per P (grpccodec).
		grpc.ForceServerCodecV2(grpccodec.NewLargeMessage()),
		experimental.BufferPool(grpccodec.Pool),
		// HTTP/2 receive windows: what an agent may send this relay
		// unacknowledged, so one agent stream moves at most window/RTT
		// (agent -> client: downloads, vhost responses). Static windows
		// turn off grpc-go's BDP estimator; at 40 ms RTT it measured no
		// better than 4 MiB static, and 8 MiB static measured 1.8x
		// (BenchmarkTCPTunnelThroughputRTT). A window is also what one
		// agent stream can make the relay buffer while its reader waits,
		// which is why the relay grants half of what the agent grants.
		grpc.InitialWindowSize(8 << 20),
		grpc.InitialConnWindowSize(32 << 20),
		// Collect up to 64 KiB per write (grpc-go's default is 32 KiB):
		// over TLS one batch is one write syscall (tlsbatch, which the
		// gRPC listener uses). grpc-go takes the buffer from a shared pool
		// for every flush, a heartbeat's included, so the pool grows to
		// the number of connections flushing at once: with 256 KiB, a
		// burst of 500 agents registering left up to 97 MiB of buffers
		// (about 45 KiB of relay memory per agent at 500) for 64 KiB at
		// no measured throughput cost (BenchmarkTCPTunnelThroughput).
		grpc.WriteBufferSize(64 << 10),
		// No read buffer of gRPC's own: over TLS the framer reads from
		// tls.Conn's record buffer, so a second 32 KiB per connection
		// (pinned for its life; the largest heap item per idle agent)
		// saved nothing. ponytail: a plaintext gRPC listener (loopback,
		// dev) then reads the socket once per frame header and payload;
		// give it a buffer if one serves real traffic.
		grpc.ReadBufferSize(0),
	}
}
