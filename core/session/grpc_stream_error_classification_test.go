// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/spf13/viper"
)

// ===========================================================================

func TestClassifyStreamError_GlobalContextCanceled(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx, cancel := context.WithCancel(context.Background())

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})

	// Cancel global context, then pass context.Canceled → should classify as server-side cancel
	cancel()
	err := d.classifyStreamError(context.Canceled)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClassifyStreamError_ClientContextCanceled(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})

	// Global context still alive, context.Canceled → client disconnected, return nil
	err := d.classifyStreamError(context.Canceled)
	if err != nil {
		t.Fatalf("expected nil for client-side cancel, got: %v", err)
	}
}

func TestClassifyStreamError_DefaultError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})

	// Random error → default branch
	err := d.classifyStreamError(errors.New("something random"))
	if err == nil {
		t.Fatal("expected error for default branch")
	}
}

// ===========================================================================
// serveHTTP: sync mode
