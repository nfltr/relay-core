// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package receiveloop

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

const testData = "data"

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func TestReceiveAndProcess_BasicFlow(t *testing.T) {
	messages := []string{"msg-1", "msg-2", "msg-3"}
	idx := atomic.Int32{}

	recvFn := func() (string, error) {
		i := int(idx.Add(1)) - 1
		if i >= len(messages) {
			return "", errors.New("EOF")
		}
		return messages[i], nil
	}

	var onRecvCount atomic.Int32
	onRecv := func() {
		onRecvCount.Add(1)
	}

	var processed []string
	done := make(chan struct{})
	processFn := func(_ context.Context, data string, _ time.Time) error { //nolint:unparam // error is intentionally nil for success-path test
		processed = append(processed, data)
		if len(processed) == len(messages) {
			close(done)
		}
		return nil
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- ReceiveAndProcess(
			context.Background(), 10, "test-agentID", testLogger(),
			recvFn, onRecv, processFn,
		)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for processing")
	}

	err := <-errCh
	if err == nil || err.Error() != "EOF" {
		t.Errorf("expected EOF error, got: %v", err)
	}

	if len(processed) != 3 {
		t.Errorf("expected 3 processed items, got %d", len(processed))
	}
	if onRecvCount.Load() != 3 {
		t.Errorf("expected onRecv called 3 times, got %d", onRecvCount.Load())
	}
}

func TestReceiveAndProcess_RecvError(t *testing.T) {
	expectedErr := errors.New("connection lost")

	recvFn := func() (int, error) {
		return 0, expectedErr
	}

	err := ReceiveAndProcess(
		context.Background(), 10, "test-agentID", testLogger(),
		recvFn, func() {}, func(_ context.Context, _ int, _ time.Time) error { return nil },
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "connection lost" {
		t.Errorf("expected 'connection lost', got: %v", err)
	}
}

func TestReceiveAndProcess_ProcessError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		callCount := atomic.Int32{}
		recvFn := func() (string, error) {
			callCount.Add(1)
			time.Sleep(10 * time.Millisecond)
			return "data", nil
		}

		processErr := errors.New("process failed")
		processFn := func(_ context.Context, _ string, _ time.Time) error {
			return processErr
		}

		err := ReceiveAndProcess(
			context.Background(), 10, "test-agentID", testLogger(),
			recvFn, func() {}, processFn,
		)
		if err == nil {
			t.Fatal("expected error")
		}
		if !errors.Is(err, processErr) && !errors.Is(err, context.Canceled) {
			t.Errorf("expected processErr or context.Canceled, got: %v", err)
		}
	})
}

func TestReceiveAndProcess_ContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		recvFn := func() (string, error) {
			time.Sleep(100 * time.Millisecond)
			return testData, nil
		}

		cancel()

		err := ReceiveAndProcess(
			ctx, 10, "test-agentID", testLogger(),
			recvFn, func() {}, func(_ context.Context, _ string, _ time.Time) error { return nil },
		)
		if err == nil {
			t.Fatal("expected error on context cancellation")
		}
	})
}

func TestReceiveAndProcess_ReceivedAtTimestamp(t *testing.T) {
	before := time.Now()

	recvFn := func() (string, error) {
		return testData, nil
	}

	recvCount := atomic.Int32{}
	var receivedAt time.Time
	done := make(chan struct{})
	processFn := func(_ context.Context, _ string, ts time.Time) error {
		receivedAt = ts
		if recvCount.Add(1) == 1 {
			close(done)
			return errors.New("stop")
		}
		return nil
	}

	_ = ReceiveAndProcess(
		context.Background(), 10, "test-agentID", testLogger(),
		recvFn, func() {}, processFn,
	)

	if receivedAt.Before(before) {
		t.Errorf("receivedAt %v should be after test start %v", receivedAt, before)
	}
}

func TestReceiveAndProcess_OnRecvCalled(t *testing.T) {
	callCount := atomic.Int32{}
	recvIdx := atomic.Int32{}

	recvFn := func() (int, error) {
		i := recvIdx.Add(1)
		if i > 5 {
			return 0, errors.New("done")
		}
		return int(i), nil
	}

	onRecv := func() {
		callCount.Add(1)
	}

	_ = ReceiveAndProcess(
		context.Background(), 10, "test-agentID", testLogger(),
		recvFn, onRecv,
		func(_ context.Context, _ int, _ time.Time) error { return nil },
	)

	if callCount.Load() != 5 {
		t.Errorf("expected onRecv called 5 times, got %d", callCount.Load())
	}
}

func TestReceiveAndProcess_BufferSize(t *testing.T) {
	recvIdx := atomic.Int32{}
	recvFn := func() (string, error) {
		i := recvIdx.Add(1)
		if i > 3 {
			return "", errors.New("done")
		}
		return "msg", nil
	}

	var processCount atomic.Int32
	processFn := func(_ context.Context, _ string, _ time.Time) error {
		processCount.Add(1)
		return nil
	}

	_ = ReceiveAndProcess(
		context.Background(), 1, "test-agentID", testLogger(),
		recvFn, func() {}, processFn,
	)

	if processCount.Load() < 1 {
		t.Errorf("expected at least 1 processed item, got %d", processCount.Load())
	}
}

func TestReceiveAndProcess_ContextCancelDuringBufferSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		recvIdx := atomic.Int32{}
		recvFn := func() (string, error) {
			i := recvIdx.Add(1)
			if i > 1 {
				cancel()
				time.Sleep(50 * time.Millisecond)
			}
			return testData, nil
		}

		processFn := func(_ context.Context, _ string, _ time.Time) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		}

		err := ReceiveAndProcess(
			ctx, 0, "test-agentID", testLogger(),
			recvFn, func() {}, processFn,
		)

		if err == nil {
			t.Fatal("expected error on context cancellation")
		}
	})
}
