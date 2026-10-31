// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/nfltr/relay-core/internal/platform/acceptloop/acceptlooptest"
)

// TestServeEndpointSurvivesAcceptError: an Accept error other than a
// closed listener (EMFILE) is retried, not the end of the endpoint.
func TestServeEndpointSurvivesAcceptError(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	ln := acceptlooptest.New()
	ctx, cancel := context.WithCancel(t.Context())
	served := make(chan error, 1)
	go func() { served <- d.serveEndpoint(ctx, ln, TCPEndpoint{ListenAddr: "test", AgentID: "a"}) }()
	select {
	case err := <-served:
		t.Fatalf("ServeEndpoint returned after one EMFILE: %v", err)
	case <-ln.Again:
	case <-time.After(10 * time.Second):
		t.Fatal("ServeEndpoint did not call Accept again")
	}
	cancel()
	if err := <-served; err != nil {
		t.Fatalf("ServeEndpoint after ctx end = %v, want nil", err)
	}
}
