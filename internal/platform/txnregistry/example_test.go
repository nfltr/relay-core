// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package txnregistry_test

import (
	"context"
	"fmt"

	"log/slog"

	"github.com/nfltr/relay-core/internal/platform/txnregistry"
	"github.com/nfltr/relay/proto/pb"
)

// ExampleNewTransactionRegistry demonstrates the full lifecycle:
// register a transaction, fanout a response, and receive it.
func ExampleNewTransactionRegistry() {
	registry := txnregistry.NewTransactionRegistry[*pb.TunnelFrame]()
	logger := slog.Default()

	// Register a transaction — returns a handle with ResponseCh and a cleanup func
	handle, cleanup := registry.Register("txn-001", context.Background(), logger)
	defer cleanup()

	// Simulate agent sending a response through fanout
	go func() {
		msg := &pb.TunnelFrame{
			TransactionId: "txn-001",
		}
		_ = registry.Fanout("txn-001", msg, logger)
	}()

	// Receive the response
	response := <-handle.ResponseCh
	fmt.Println(response.TransactionId)
	// Output: txn-001
}
