// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package receiveloop

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
)

func benchmarkLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func BenchmarkReceiveAndProcess(b *testing.B) {
	for _, bufSize := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("buf=%d", bufSize), func(b *testing.B) {
			count := 0
			recvFn := func() (int, error) {
				count++
				if count > b.N {
					return 0, fmt.Errorf("done")
				}
				return count, nil
			}

			_ = ReceiveAndProcess[int](
				context.Background(),
				bufSize,
				"bench-agentID",
				benchmarkLogger(),
				recvFn,
				func() {}, // onRecv no-op
				func(_ context.Context, _ int, _ time.Time) error {
					return nil
				},
			)
		})
	}
}

func BenchmarkReceiveAndProcess_HeavyProcessing(b *testing.B) {
	count := 0
	recvFn := func() (int, error) {
		count++
		if count > b.N {
			return 0, fmt.Errorf("done")
		}
		return count, nil
	}

	_ = ReceiveAndProcess[int](
		context.Background(),
		50,
		"bench-agentID",
		benchmarkLogger(),
		recvFn,
		func() {},
		func(_ context.Context, _ int, _ time.Time) error {
			// Simulate light work
			x := 0
			for i := range 100 {
				x += i
			}
			_ = x
			return nil
		},
	)
}
