// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package receiveloop_test

import (
	"context"
	"fmt"
	"time"

	"log/slog"

	"github.com/nfltr/relay-core/internal/platform/receiveloop"
)

// ExampleReceiveAndProcess demonstrates the generic receive-loop pattern
// with a simple integer stream that processes 5 items.
func ExampleReceiveAndProcess() {
	logger := slog.Default()
	count := 0

	err := receiveloop.ReceiveAndProcess[int](
		context.Background(),
		3, // buffer size
		"example-agentID",
		logger,
		func() (int, error) {
			count++
			if count > 5 {
				return 0, fmt.Errorf("stream ended")
			}
			return count, nil
		},
		func() {}, // onRecv hook (e.g. reset heartbeat)
		func(_ context.Context, data int, _ time.Time) error {
			fmt.Println(data)
			return nil
		},
	)
	_ = err
	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
}
