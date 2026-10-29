// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package receiveloop

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReceiveAndProcess is a generic receive loop that reads items from a
// stream and processes each one concurrently:
//   - Reader (its own goroutine): calls recvFn in a loop, pushes timestamped
//     items into a queue of at most bufSize items (it waits while the queue
//     is full). Calls onRecv after each successful receive (typically to
//     reset heartbeat tickers). The queue's storage grows with use: an idle
//     agent session holds none of it (a bufSize channel allocated all of it
//     up front, 8 KiB per agent at the relay's 1000).
//   - Processor (the caller's goroutine): reads from the buffer and invokes
//     processFn for each item.
//
// It returns the first error of either (the reader always ends with one),
// after the reader has returned. The processor runs on the caller's
// goroutine: a second one only waited, a stack held for every idle agent
// session.
func ReceiveAndProcess[T any](
	ctx context.Context,
	bufSize int,
	agentID string,
	logger *slog.Logger,
	recvFn func() (T, error),
	onRecv func(),
	processFn func(ctx context.Context, data T, receivedAt time.Time) error,
) error {
	type item struct {
		data       T
		receivedAt time.Time
	}
	items := newQueue[item](bufSize)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var first error
	var once sync.Once
	fail := func(err error) {
		if err != nil {
			once.Do(func() { first = err })
			cancel()
		}
	}
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer items.close()
		fail(func() error {
			for ctx.Err() == nil {
				data, err := recvFn()
				now := time.Now()
				if err != nil {
					if isExpectedDisconnect(err) {
						logger.Debug("receiving input from agent", "agent_id", agentID, "error", err)
					} else {
						logger.Error("receiving input from agent", "agent_id", agentID, "error", err)
					}
					return err
				}
				onRecv()
				if err := items.push(ctx, item{data: data, receivedAt: now}); err != nil {
					return err
				}
			}
			return ctx.Err()
		}())
	}()
	// pop drains all queued items before reporting the end, even after the
	// reader closed the queue. This prevents a race where ctx.Done() fires
	// and drops unprocessed messages that the reader already queued (e.g. the
	// final EOM response from an agent that closed its stream immediately
	// after sending).
	for it, ok := items.pop(); ok; it, ok = items.pop() {
		if err := processFn(ctx, it.data, it.receivedAt); err != nil {
			fail(err)
			break
		}
	}
	<-readerDone
	return first
}

func isExpectedDisconnect(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return status.Code(err) == codes.Canceled
}
