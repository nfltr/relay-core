// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"io"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/spf13/viper"
)

func TestConnectHTTP_UnsubscribeError(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    2 * time.Second,
		HeartbeatEnabled:         false,
		RecvBufferSize:           10,
	})

	agentID := "oph-unsub-err"
	stream := &mockConnectHTTPServer{
		ctx:     makeGRPCContext(agentID),
		recvErr: io.EOF,
	}

	// Run ConnectHTTP. Close the broker after subscribe to make Unsubscribe fail.
	done := make(chan error, 1)
	go func() { done <- d.ConnectHTTP(stream) }()

	time.Sleep(100 * time.Millisecond)
	b.Close()

	select {
	case err := <-done:
		t.Logf("ConnectHTTP returned: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("ConnectHTTP didn't return")
	}
}
