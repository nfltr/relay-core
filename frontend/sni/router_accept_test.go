// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sni

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/acceptloop/acceptlooptest"
)

// TestRouterServeSurvivesAcceptError: an Accept error other than a closed
// listener (EMFILE) is retried, not the end of the SNI port.
func TestRouterServeSurvivesAcceptError(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	router := NewRouter("e2ee.test.local", nil, session.NewTCPDispatcher(logger, nil, directory.NewNoOpInventory()), logger)
	ln := acceptlooptest.New()
	ctx, cancel := context.WithCancel(t.Context())
	served := make(chan error, 1)
	go func() { served <- router.Serve(ctx, ln) }()
	select {
	case err := <-served:
		t.Fatalf("Serve returned after one EMFILE: %v", err)
	case <-ln.Again:
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not call Accept again")
	}
	cancel()
	if err := <-served; err != nil {
		t.Fatalf("Serve after ctx end = %v, want nil", err)
	}
}
