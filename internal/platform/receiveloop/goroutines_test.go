// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package receiveloop

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// TestReceiveAndProcess_ProcessesOnTheCallersGoroutine: the loop starts one
// goroutine (the reader) and processes on the caller's. It ran an errgroup
// with two while its caller only waited: a goroutine stack held for every
// idle agent session.
func TestReceiveAndProcess_ProcessesOnTheCallersGoroutine(t *testing.T) {
	recvBlocked := make(chan struct{})
	release := make(chan struct{})
	before := runtime.NumGoroutine()
	done := make(chan error, 1)
	go func() { // the caller's goroutine
		first := true
		done <- ReceiveAndProcess(context.Background(), 4, "a", testLogger(),
			func() (int, error) {
				if first {
					first = false
					close(recvBlocked)
				}
				<-release
				return 0, errors.New("stream ended")
			},
			func() {},
			func(context.Context, int, time.Time) error { return nil })
	}()
	<-recvBlocked
	time.Sleep(10 * time.Millisecond) // let every goroutine the loop starts start
	if got := runtime.NumGoroutine() - before; got != 2 {
		t.Errorf("an idle loop runs %d goroutines (its caller included), want 2: the caller and the reader", got)
	}
	close(release)
	if err := <-done; err == nil || err.Error() != "stream ended" {
		t.Fatalf("ReceiveAndProcess = %v, want the reader's error", err)
	}
}
