// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"testing"

	"github.com/nfltr/relay/client/rpcclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAgentDrainWireConstantsMatchRPCClient(t *testing.T) {
	if agentDrainMessageType != rpcclient.AgentDrainMessageType ||
		agentDrainAckMessageType != rpcclient.AgentDrainAckMessageType ||
		agentDrainingDetail != rpcclient.AgentDrainingDetail {
		t.Fatal("relay and rpcclient disagree on the agent drain wire constants")
	}
	if !rpcclient.IsAgentDraining(status.Error(codes.Unavailable, agentDrainingDetail)) {
		t.Fatal("the relay's draining refusal is not recognised by rpcclient.IsAgentDraining")
	}
}
