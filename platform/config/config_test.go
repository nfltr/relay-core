// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"strings"
	"testing"
	"time"

	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func TestSetDefaultFlags(t *testing.T) {
	SetDefaultFlags()
}

// TestNoCloudflareOriginPullFlags: nothing is Cloudflare-proxied, so the
// relay has no origin-pull listener to configure (no dual path, 2026-09-30).
func TestNoCloudflareOriginPullFlags(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	defer viper.Reset()
	SetDefaultFlags()
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "http.origin_pull") {
			t.Errorf("flag %s still registered: the Cloudflare origin-pull path is removed", f.Name)
		}
	})
}

// TestDeletedFeatureFlagsAreGone: features deleted from the relay core
// leave no flag behind: OPA, async dispatch and Pulsar, cross-environment
// forwarding, FleetInvoke, the admin API, the fileop timeout class, JWT
// (and application.id, which only named the OPA policy package), and the
// proxy cache's size and TTL (a bounded cache of peer replicas, no TTL).
func TestDeletedFeatureFlagsAreGone(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	defer viper.Reset()
	SetDefaultFlags()
	prefixes := []string{"authz.", "broker.", "dispatcher_type", "georeplication.", "current.env", "api.port",
		"peer_rpc.", "fleet.", "rpc.server.producers.", "admin.enable", "admin.token", "admin.allow_unauthenticated",
		"request.timeout.fileop", "security.jwt.", "application.id", "proxy_cache."}
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		for _, p := range prefixes {
			if strings.HasPrefix(f.Name, p) {
				t.Errorf("flag %s is registered; its feature is deleted", f.Name)
			}
		}
	})
	if flag.CommandLine.Lookup("admin.message") == nil {
		t.Error("admin.message (the agents' connect message) must stay")
	}
}

// TestSetDefaultFlags_GRPCKeepaliveDefaults locks in the server-side gRPC
// keepalive defaults that prevent long-idle worker streams from being torn
// down by the gRPC library's built-in TOO_MANY_PINGS enforcement (default
// MinTime=5m). The rpcclient's DefaultKeepaliveParams sends pings every 60s,
// so MinTime must be ≤60s. Server-initiated pings every 30s with a 10s
// timeout keep middleboxes (Cloudflare, NAT) from idling the TCP connection
// out and surface dead client streams within ~40s.
func TestSetDefaultFlags_GRPCKeepaliveDefaults(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	defer viper.Reset()
	SetDefaultFlags()

	if got, want := viper.GetDuration("grpc.keepalive.enforcement_policy.min_time"), 30*time.Second; got != want {
		t.Errorf("grpc.keepalive.enforcement_policy.min_time = %v, want %v (must be ≤60s to allow rpcclient default ping cadence)", got, want)
	}
	if !viper.GetBool("grpc.keepalive.enforcement_policy.permit_without_stream") {
		t.Fatal("grpc.keepalive.enforcement_policy.permit_without_stream = false; idle workers ping without active streams")
	}
	if got, want := viper.GetDuration("grpc.keepalive.server_parameters.time"), 30*time.Second; got != want {
		t.Errorf("grpc.keepalive.server_parameters.time = %v, want %v (server pings keep Cloudflare path warm)", got, want)
	}
	if got, want := viper.GetDuration("grpc.keepalive.server_parameters.timeout"), 10*time.Second; got != want {
		t.Errorf("grpc.keepalive.server_parameters.timeout = %v, want %v", got, want)
	}
}

// TestSetDefaultFlags_DownstreamFanoutErrorsDoNotDropAgentsByDefault locks in
// the orchestration-safe behavior: a missing/canceled downstream transaction
// should be observed and counted, but it should not sever a healthy long-lived
// worker stream. Operators can still opt into the stricter fail-closed mode.
func TestSetDefaultFlags_DownstreamFanoutErrorsDoNotDropAgentsByDefault(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	defer viper.Reset()
	SetDefaultFlags()

	if viper.GetBool("on_downstream_errors.drop_agent_connection") {
		t.Fatal("on_downstream_errors.drop_agent_connection default = true; want false so transient downstream/edge failures do not disconnect agents")
	}
}

// TestSetDefaultFlags_PageVisitsOffByDefault: public page visit counting is
// opt-in; a relay that does not turn it on never records visits.
func TestSetDefaultFlags_PageVisitsOffByDefault(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	defer viper.Reset()
	SetDefaultFlags()

	if viper.GetBool("usage.page_visits") {
		t.Fatal("usage.page_visits default = true; want false")
	}
}

// TestSetDefaultFlags_HeartbeatDefaults locks in the application-level
// heartbeat cadence. 15s send keeps consumer-NAT mappings warm (typical
// 30-300s NAT entry timeouts) and matches the kernel SO_KEEPALIVE interval
// configured in internal/adapters/outbound/grpcfactory. recv threshold = 3× send (45s) tolerates
// transient blips without disconnecting healthy agents.
func TestSetDefaultFlags_HeartbeatDefaults(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	defer viper.Reset()
	SetDefaultFlags()

	if !viper.GetBool("rpc.agent.heartbeat.enable") {
		t.Fatal("rpc.agent.heartbeat.enable default = false; heartbeats are required to detect silent disconnections")
	}
	if got, want := viper.GetDuration("rpc.agent.heartbeat.sendduration"), 5*time.Second; got != want {
		t.Errorf("rpc.agent.heartbeat.sendduration = %v, want %v (agents leave a relay silent for three: 15s)", got, want)
	}
	if got, want := viper.GetDuration("rpc.agent.heartbeat.recvthreshold"), 45*time.Second; got != want {
		t.Errorf("rpc.agent.heartbeat.recvthreshold = %v, want %v (an agent's echoes, tolerating missed beats)", got, want)
	}
}

func TestEnvK8sConstant(t *testing.T) {
	if EnvK8s != "k8s" {
		t.Errorf("EnvK8s = %q, want %q", EnvK8s, "k8s")
	}
}

func TestEnvConstants(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{"EnvLocal", EnvLocal, "local"},
		{"EnvStandalone", EnvStandalone, "standalone"},
		{"EnvCI", EnvCI, "CI"},
		{"EnvK8s", EnvK8s, "k8s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value != tt.want {
				t.Errorf("%s = %q, want %q", tt.name, tt.value, tt.want)
			}
		})
	}
}

func TestIsLocalEnv(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want bool
	}{
		{name: "local", env: EnvLocal, want: true},
		{name: "standalone", env: EnvStandalone, want: true},
		{name: "local uppercase", env: "LOCAL", want: true},
		{name: "standalone padded", env: "  standalone  ", want: true},
		{name: "k8s", env: EnvK8s, want: false},
		{name: "ci", env: EnvCI, want: false},
		{name: "unknown", env: "prod", want: false},
		{name: "empty", env: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsLocalEnv(tt.env); got != tt.want {
				t.Errorf("IsLocalEnv(%q) = %v, want %v", tt.env, got, tt.want)
			}
		})
	}
}

func TestIsK8sMultiPodEnv(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want bool
	}{
		{name: "k8s", env: EnvK8s, want: true},
		{name: "ci", env: EnvCI, want: true},
		{name: "ci lowercase", env: "ci", want: true},
		{name: "local", env: EnvLocal, want: false},
		{name: "standalone", env: EnvStandalone, want: false},
		{name: "empty", env: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsK8sMultiPodEnv(tt.env); got != tt.want {
				t.Errorf("IsK8sMultiPodEnv(%q) = %v, want %v", tt.env, got, tt.want)
			}
		})
	}
}

// TestDefaultLogLevelIsInfo guards the relay's built-in default: debug logs
// every agent-to-agent request, which on a small VM is CPU and journal I/O.
func TestDefaultLogLevelIsInfo(t *testing.T) {
	if defaultLogLevel != "info" {
		t.Fatalf("defaultLogLevel = %q, want info", defaultLogLevel)
	}
}
