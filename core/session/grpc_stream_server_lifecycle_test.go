// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/spf13/viper"
)

func TestNewDispatcher_NoConfig(t *testing.T) {
	viper.Set("rpc.connected_agents_threshold", 10)
	defer viper.Reset()

	ctx := context.Background()
	b := broker.NewInMemoryMessageBroker()
	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second)
	if d == nil {
		t.Fatal("expected non-nil dispatcher")
	}
	if d.Config.ConnectedAgentsThreshold != 10 {
		t.Fatalf("expected threshold 10, got %d", d.Config.ConnectedAgentsThreshold)
	}
}
