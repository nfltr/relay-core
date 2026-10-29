// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package txnregistry

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"io"
	"log/slog"

	"github.com/nfltr/relay/proto/pb"
	"github.com/stretchr/testify/require"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func TestTransactionRegistryRegister(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	ctx := context.Background()

	handle, _ := registry.Register("txn-1", ctx, newTestLogger())

	if handle == nil {
		t.Fatal("expected non-nil handle")
	}
	if handle.TransactionID != "txn-1" {
		t.Errorf("expected TransactionID 'txn-1', got '%s'", handle.TransactionID)
	}
	if handle.ResponseCh == nil {
		t.Error("expected non-nil ResponseCh")
	}
}

func TestTransactionRegistryRegisterMultiple(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	ctx := context.Background()

	handle1, _ := registry.Register("txn-1", ctx, newTestLogger())
	handle2, _ := registry.Register("txn-2", ctx, newTestLogger())

	if handle1.TransactionID == handle2.TransactionID {
		t.Error("handles should have different transaction IDs")
	}
	if handle1.ResponseCh == handle2.ResponseCh {
		t.Error("handles should have different response channels")
	}
}

func TestTransactionRegistryFanout_Success(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()
	ctx := t.Context()

	handle, _ := registry.Register("txn-1", ctx, newTestLogger())

	var received *pb.TunnelFrame
	done := make(chan struct{})
	go func() {
		received = <-handle.ResponseCh
		close(done)
	}()

	msg := &pb.TunnelFrame{
		TransactionId: "txn-1",
	}
	err := registry.Fanout(msg.TransactionId, msg, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-done:
		if received != msg {
			t.Error("received message doesn't match sent message")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestTransactionRegistryFanout_NotRegistered(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()

	msg := &pb.TunnelFrame{
		TransactionId: "non-existent",
	}

	err := registry.Fanout(msg.TransactionId, msg, logger)
	if err != ErrTransactionNotFound {
		t.Errorf("expected ErrTransactionNotFound, got %v", err)
	}
}

func TestTransactionRegistryUnregister_CleansUp(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()
	ctx := t.Context()

	_, cleanup := registry.Register("txn-1", ctx, logger)

	if !registry.IsRegistered("txn-1") {
		t.Error("should be registered after Register")
	}

	cleanup()

	if registry.IsRegistered("txn-1") {
		t.Error("should not be registered after cleanup")
	}
}

func TestTransactionRegistryFanout_ContextCancelled(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()
	ctx, cancel := context.WithCancel(context.Background())

	handle, _ := registry.Register("txn-1", ctx, newTestLogger())
	cancel()

	msg := &pb.TunnelFrame{
		TransactionId: "txn-1",
	}

	var fanoutErr error
	done := make(chan struct{})
	go func() {
		fanoutErr = registry.Fanout(msg.TransactionId, msg, logger)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Fanout blocked on cancelled context")
	}

	require.Error(t, fanoutErr)
	require.ErrorIs(t, fanoutErr, ErrTransactionStale)
	require.ErrorIs(t, fanoutErr, context.Canceled)

	select {
	case <-handle.ResponseCh:
	default:
	}
}

func TestTransactionRegistryFanout_CleanedUpTransactionIsStale(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()
	ctx := t.Context()

	_, cleanup := registry.Register("txn-stale", ctx, logger)
	cleanup()

	err := registry.Fanout("txn-stale", &pb.TunnelFrame{TransactionId: "txn-stale"}, logger)
	require.ErrorIs(t, err, ErrTransactionStale)
}

func TestTransactionRegistryConcurrentFanout(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()
	ctx := t.Context()

	handle, _ := registry.Register("txn-1", ctx, newTestLogger())

	var wg sync.WaitGroup
	var successCount atomic.Int32
	numMessages := 100

	receiverDone := make(chan struct{})
	go func() {
		count := 0
		for {
			select {
			case <-ctx.Done():
				close(receiverDone)
				return
			case <-handle.ResponseCh:
				count++
				if count >= numMessages {
					close(receiverDone)
					return
				}
			}
		}
	}()

	for range numMessages {
		wg.Go(func() {
			msg := &pb.TunnelFrame{
				TransactionId: "txn-1",
			}
			err := registry.Fanout(msg.TransactionId, msg, logger)
			if err == nil {
				successCount.Add(1)
			}
		})
	}

	wg.Wait()

	select {
	case <-receiverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for receiver")
	}

	if successCount.Load() != int32(numMessages) {
		t.Errorf("expected %d successful fanouts, got %d", numMessages, successCount.Load())
	}
}

func TestTransactionRegistryRaceCondition(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()

	for i := range 10 {
		ctx, cancel := context.WithCancel(context.Background())
		txnID := fmt.Sprintf("txn-race-test-%d", i)

		handle, cleanup := registry.Register(txnID, ctx, newTestLogger())

		// Start receiver using select (channels are never closed)
		receiverCtx, receiverCancel := context.WithCancel(context.Background())
		go func() {
			for {
				select {
				case <-receiverCtx.Done():
					return
				case <-handle.ResponseCh:
				}
			}
		}()

		var wg sync.WaitGroup

		for range 5 {
			wg.Go(func() {
				msg := &pb.TunnelFrame{
					TransactionId: txnID,
				}
				_ = registry.Fanout(msg.TransactionId, msg, logger)
			})
		}

		// Wait a tiny bit for fanouts to complete
		wg.Wait()

		cleanup()
		cancel()

		// Stop the receiver
		receiverCancel()
	}
}

func TestTransactionRegistryIsRegistered(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	ctx := context.Background()

	if registry.IsRegistered("txn-1") {
		t.Error("should not be registered before Register")
	}

	registry.Register("txn-1", ctx, newTestLogger())
	if !registry.IsRegistered("txn-1") {
		t.Error("should be registered after Register")
	}
}

func TestTransactionRegistryMultipleFanouts(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()
	ctx := t.Context()

	handle, _ := registry.Register("txn-1", ctx, newTestLogger())

	numMessages := 10
	receivedCount := 0
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-handle.ResponseCh:
				receivedCount++
				if receivedCount >= numMessages {
					close(done)
					return
				}
			}
		}
	}()

	for i := range numMessages {
		msg := &pb.TunnelFrame{
			TransactionId: "txn-1",
		}
		err := registry.Fanout(msg.TransactionId, msg, logger)
		if err != nil {
			t.Fatalf("fanout %d failed: %v", i, err)
		}
	}

	select {
	case <-done:
		if receivedCount != numMessages {
			t.Errorf("expected %d messages, received %d", numMessages, receivedCount)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout: only received %d of %d messages", receivedCount, numMessages)
	}
}

func TestTransactionRegistryUnregisterBeforeCancel(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()
	ctx := t.Context()

	_, cleanup := registry.Register("txn-1", ctx, logger)

	// cleanup removes the entry immediately
	cleanup()

	if registry.IsRegistered("txn-1") {
		t.Error("transaction should not be registered after cleanup")
	}
}

func TestTransactionRegistryMultipleTransactions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := NewTransactionRegistry[*pb.TunnelFrame]()
		logger := newTestLogger()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		numTransactions := 10
		handles := make([]*TransactionHandle[*pb.TunnelFrame], numTransactions)
		txnIDs := make([]string, numTransactions)

		// Register multiple transactions
		for i := range numTransactions {
			txnIDs[i] = fmt.Sprintf("txn-%d", i)
			handles[i], _ = registry.Register(txnIDs[i], ctx, newTestLogger())
		}

		// Verify all are registered
		for i := range numTransactions {
			if !registry.IsRegistered(txnIDs[i]) {
				t.Errorf("transaction %s should exist", txnIDs[i])
			}
		}

		// Start receivers first (unbuffered channels require receivers)
		var wg sync.WaitGroup
		received := make([]bool, numTransactions)
		for i := range numTransactions {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				select {
				case <-handles[idx].ResponseCh:
					received[idx] = true
				case <-time.After(2 * time.Second):
					t.Errorf("timeout waiting for message on transaction %d", idx)
				}
			}(i)
		}

		synctest.Wait()

		// Fanout to each
		for i := range numTransactions {
			msg := &pb.TunnelFrame{
				TransactionId: txnIDs[i],
			}
			err := registry.Fanout(msg.TransactionId, msg, logger)
			if err != nil {
				t.Errorf("fanout to %s failed: %v", txnIDs[i], err)
			}
		}

		wg.Wait()

		// Verify each received
		for i := range numTransactions {
			if !received[i] {
				t.Errorf("transaction %d was not received", i)
			}
		}
	})
}

func TestTransactionRegistryFanoutAfterUnregister(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()
	ctx := t.Context()

	_, cleanup := registry.Register("txn-1", ctx, logger)
	cleanup()

	msg := &pb.TunnelFrame{
		TransactionId: "txn-1",
	}

	err := registry.Fanout(msg.TransactionId, msg, logger)
	if err != ErrTransactionStale {
		t.Errorf("expected ErrTransactionStale after cleanup, got %v", err)
	}
}

func TestTransactionRegistryReRegister(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := NewTransactionRegistry[*pb.TunnelFrame]()
		logger := newTestLogger()
		ctx1, cancel1 := context.WithCancel(context.Background())

		handle1, cleanup1 := registry.Register("txn-1", ctx1, logger)

		// Drain handle1 in background using context-select (channels are never closed)
		go func() {
			for {
				select {
				case <-ctx1.Done():
					return
				case <-handle1.ResponseCh:
				}
			}
		}()

		cleanup1()
		cancel1()

		// Re-register same ID
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()

		handle2, _ := registry.Register("txn-1", ctx2, newTestLogger())

		// Should be a different handle
		if handle1.ResponseCh == handle2.ResponseCh {
			t.Error("re-registered transaction should have new response channel")
		}

		// Start receiver before fanout (unbuffered channel)
		done := make(chan struct{})
		go func() {
			select {
			case <-handle2.ResponseCh:
				close(done)
			case <-time.After(2 * time.Second):
			}
		}()

		synctest.Wait()

		// Fanout should work
		msg := &pb.TunnelFrame{
			TransactionId: "txn-1",
		}
		err := registry.Fanout(msg.TransactionId, msg, logger)
		if err != nil {
			t.Fatalf("fanout failed: %v", err)
		}

		select {
		case <-done:
			// Success
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for message on re-registered transaction")
		}
	})
}

func TestTransactionRegistryRapidRegisterUnregister(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()

	for range 100 {
		txnID := "rapid-txn"
		_, cleanup := registry.Register(txnID, context.Background(), newTestLogger())
		cleanup()
	}
	// Success if no panic
}

func TestTransactionRegistryConcurrentRegister(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()

	var wg sync.WaitGroup

	// Concurrent registration of different transactions
	for i := range 100 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx := context.Background()
			txnID := fmt.Sprintf("concurrent-txn-%d", idx)
			registry.Register(txnID, ctx, newTestLogger())
		}(i)
	}

	wg.Wait()
}

func TestTransactionRegistryConcurrentFanoutDifferentTransactions(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()
	ctx := t.Context()

	numTransactions := 10
	handles := make(map[string]*TransactionHandle[*pb.TunnelFrame])

	for i := range numTransactions {
		txnID := fmt.Sprintf("concurrent-%d", i)
		handles[txnID], _ = registry.Register(txnID, ctx, newTestLogger())
	}

	var wg sync.WaitGroup
	var successCount atomic.Int32

	// Start receivers
	for txnID, handle := range handles {
		wg.Add(1)
		go func(id string, h *TransactionHandle[*pb.TunnelFrame]) {
			defer wg.Done()
			select {
			case <-h.ResponseCh:
				successCount.Add(1)
			case <-time.After(2 * time.Second):
				t.Errorf("timeout waiting for %s", id)
			}
		}(txnID, handle)
	}

	// Concurrent fanouts
	for txnID := range handles {
		go func(id string) {
			msg := &pb.TunnelFrame{
				TransactionId: id,
			}
			_ = registry.Fanout(msg.TransactionId, msg, logger)
		}(txnID)
	}

	wg.Wait()

	if successCount.Load() != int32(numTransactions) {
		t.Errorf("expected %d successful receives, got %d", numTransactions, successCount.Load())
	}
}

func TestTransactionRegistryFanoutMessageIntegrity(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()
	ctx := t.Context()

	handle, _ := registry.Register("txn-1", ctx, newTestLogger())

	sentMsg := &pb.TunnelFrame{
		TransactionId: "txn-1",
		Eom:           true,
	}

	go func() {
		_ = registry.Fanout(sentMsg.TransactionId, sentMsg, logger)
	}()

	select {
	case receivedMsg := <-handle.ResponseCh:
		if receivedMsg != sentMsg {
			t.Error("received message should be the same pointer as sent")
		}
		if receivedMsg.TransactionId != "txn-1" {
			t.Error("transaction ID should be preserved")
		}
		if !receivedMsg.Eom {
			t.Error("EOM flag should be preserved")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestTransactionRegistryStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	logger := newTestLogger()

	var wg sync.WaitGroup
	numGoroutines := 50
	numOpsPerGoroutine := 50

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range numOpsPerGoroutine {
				txnID := fmt.Sprintf("stress-txn-%d-%d", id, j)
				ctx, cancel := context.WithCancel(context.Background())

				handle, cleanup := registry.Register(txnID, ctx, newTestLogger())

				// Start a receiver using context-select (channels are never closed)
				go func() {
					for {
						select {
						case <-ctx.Done():
							return
						case <-handle.ResponseCh:
						}
					}
				}()

				// Try to fanout
				msg := &pb.TunnelFrame{
					TransactionId: txnID,
				}
				_ = registry.Fanout(msg.TransactionId, msg, logger)

				cleanup()
				cancel()
			}
		}(i)
	}

	wg.Wait()
}

func TestTransactionRegistryHandleFields(t *testing.T) {
	registry := NewTransactionRegistry[*pb.TunnelFrame]()
	ctx := context.Background()

	handle, _ := registry.Register("test-txn", ctx, newTestLogger())

	if handle.TransactionID != "test-txn" {
		t.Errorf("expected TransactionID 'test-txn', got '%s'", handle.TransactionID)
	}

	if handle.ResponseCh == nil {
		t.Error("ResponseCh should not be nil")
	}
}
