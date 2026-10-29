// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package txnregistry

import (
	"context"
	"testing"

	"io"
	"log/slog"

	"github.com/nfltr/relay/proto/pb"
)

func benchLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func BenchmarkTransactionRegistry_Register(b *testing.B) {
	r := NewTransactionRegistry[*pb.TunnelFrame]()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Register("txn-id", ctx, benchLogger())
	}
}

func BenchmarkTransactionRegistry_Fanout(b *testing.B) {
	r := NewTransactionRegistry[*pb.TunnelFrame]()
	ctx := b.Context()
	logger := benchLogger()

	handle, cleanup := r.Register("txn-fanout", ctx, logger)
	defer cleanup()

	msg := &pb.TunnelFrame{
		TransactionId: "txn-fanout",
	}

	// Drain responses using context-select (channels are never closed)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-handle.ResponseCh:
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Fanout(msg.TransactionId, msg, logger)
	}
}

func BenchmarkTransactionRegistry_IsRegistered(b *testing.B) {
	r := NewTransactionRegistry[*pb.TunnelFrame]()
	r.Register("txn-check", context.Background(), benchLogger())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.IsRegistered("txn-check")
	}
}
