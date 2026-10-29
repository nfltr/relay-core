// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
)

func TestRegisterAgentConnect(t *testing.T) {
	RegisterAgentConnect()
}

func TestRegisterAgentDisconnect(t *testing.T) {
	RegisterAgentConnect()
	RegisterAgentDisconnect()
}

func TestIncrQueuedRequests(t *testing.T) {
	IncrQueuedRequests()
}

func TestIncrDequeuedRequests(t *testing.T) {
	IncrDequeuedRequests()
}

func TestRegisterRequestsReceived(t *testing.T) {
	RegisterRequestsReceived()
}

func TestRegisterRequestsSentToAgents(t *testing.T) {
	RegisterRequestsSentToAgents()
}

func TestRegisterResponsesReceivedFromAgents(t *testing.T) {
	RegisterResponsesReceivedFromAgents()
}

func TestRegisterRequestsTotal(t *testing.T) {
	RegisterRequestsTotal("GET", "200", "wapi")
}

func TestRegisterTimeoutRequests(t *testing.T) {
	RegisterTimeoutRequests()
}

func TestRegisterRequestDuration(t *testing.T) {
	RegisterRequestDuration("GET", "200", 150.0)
}

func TestRegisterLatencyIntroduced(t *testing.T) {
	RegisterLatencyIntroduced("GET", "200", 42.0)
}

func TestRegisterRequestsFailedDueToAgentNotConnected(t *testing.T) {
	RegisterRequestsFailedDueToAgentNotConnected()
}

func TestRegisterRPCRequestPayloadSize(t *testing.T) {
	RegisterRPCRequestPayloadSize("GET", 1024.0)
}

func TestRegisterRPCResponsePayloadSize(t *testing.T) {
	RegisterRPCResponsePayloadSize("GET", "200", 2048.0)
}

func TestRegisterA2ARequest(t *testing.T) {
	RegisterA2ARequest("local", "ok")
	RegisterA2ARequest("remote", "ok")
	RegisterA2ARequest("local", "error")
	RegisterA2ARequest("remote", "deadline_exceeded")
	RegisterA2ARequest("local", "cancelled")
}

func TestRegisterA2ALatency(t *testing.T) {
	RegisterA2ALatency("local", 42.0)
	RegisterA2ALatency("remote", 150.0)
}

func TestRegisterA2AError(t *testing.T) {
	RegisterA2AError("deadline_exceeded")
	RegisterA2AError("cancelled")
	RegisterA2AError("not_found")
	RegisterA2AError("routing_error")
	RegisterA2AError("remote_routing_error")
}

func TestA2AActiveSessions(t *testing.T) {
	IncrA2AActiveSessions()
	IncrA2AActiveSessions()
	DecrA2AActiveSessions()
}

func TestRegisterPodToPodConnectionFailure(t *testing.T) {
	RegisterPodToPodConnectionFailure()
}

func TestRegisterBrokerMessageDecodeError(t *testing.T) {
	RegisterBrokerMessageDecodeError()
}

func TestRegisterFanoutResponseDrop(t *testing.T) {
	RegisterFanoutResponseDrop("context_cancelled")
}

func TestRegisterBrokerAckFailure(t *testing.T) {
	RegisterBrokerAckFailure()
}

func TestRegisterDownstreamTransactionTimeout(t *testing.T) {
	RegisterDownstreamTransactionTimeout()
}

func TestRegisterGRPCDownstreamRequest(t *testing.T) {
	RegisterGRPCDownstreamRequest("ok", "local")
	RegisterGRPCDownstreamRequest("error", "remote")
}

func TestRegisterGRPCDownstreamDuration(t *testing.T) {
	RegisterGRPCDownstreamDuration("local", 25.0)
	RegisterGRPCDownstreamDuration("remote", 150.0)
}

func TestIncrGRPCDownstreamMessagesSent(t *testing.T) {
	IncrGRPCDownstreamMessagesSent()
}

func TestIncrGRPCDownstreamMessagesReceived(t *testing.T) {
	IncrGRPCDownstreamMessagesReceived()
}

// --- Gap 1: AgentTunnel observability metrics tests ---

func TestRegisterUpstreamConnection(t *testing.T) {
	RegisterUpstreamConnection("grpc")
	RegisterUpstreamConnection("http")
}

func TestRegisterUpstreamConnectionDuration(t *testing.T) {
	RegisterUpstreamConnectionDuration("clean", 120.5)
	RegisterUpstreamConnectionDuration("heartbeat_timeout", 45.0)
	RegisterUpstreamConnectionDuration("server_restart", 300.0)
	RegisterUpstreamConnectionDuration("error", 10.0)
}

func TestRegisterUpstreamMessageProcessed(t *testing.T) {
	RegisterUpstreamMessageProcessed("heartbeat")
	RegisterUpstreamMessageProcessed("data")
	RegisterUpstreamMessageProcessed("a2a")
}

func TestRegisterUpstreamHeartbeatFailure(t *testing.T) {
	RegisterUpstreamHeartbeatFailure("send_failure")
	RegisterUpstreamHeartbeatFailure("recv_timeout")
}

// --- Gap 2: Connection pool metrics tests ---

// --- Gap 3: Broker publish latency tests ---

func TestRegisterBrokerPublishDuration(t *testing.T) {
	RegisterBrokerPublishDuration(3.5)
	RegisterBrokerPublishDuration(150.0)
}

// --- Gap 4: Transaction registry size tests ---

func TestSetTxnRegistrySize(t *testing.T) {
	SetTxnRegistrySize(42)
	SetTxnRegistrySize(0)
}

// --- Gap 5: Active remote relays tests ---

func TestActiveRemoteRelays(t *testing.T) {
	IncrActiveRemoteRelays()
	IncrActiveRemoteRelays()
	DecrActiveRemoteRelays()
}
