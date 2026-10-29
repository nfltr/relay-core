// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestConnectedAgentLimitIsAtomic guards --max-agents under concurrent
// connects: the limit check and the admission it grants are one step, so
// N agents connecting at once past the remaining room admit exactly that
// room; every other one is refused with FailedPrecondition naming the limit,
// and a session's release frees its room.
func TestConnectedAgentLimitIsAtomic(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	const limit, connecting = 3, 32
	d.Config.ConnectedAgentsThreshold = limit
	d.Agents.Store("existing", &HTTPStream{ID: "existing"})

	var (
		start    = make(chan struct{})
		wg       sync.WaitGroup
		mu       sync.Mutex
		admitted []func()
		errs     []error
	)
	for i := range connecting {
		wg.Go(func() {
			<-start
			id := fmt.Sprintf("agent-%d", i)
			release, err := d.validateAgentConnection(context.Background(), id, "", d.IPInventory)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			admitted = append(admitted, release)
		})
	}
	close(start)
	wg.Wait()

	if len(admitted) != limit-1 {
		t.Fatalf("admitted %d agents with room for %d (limit %d, 1 connected)", len(admitted), limit-1, limit)
	}
	for _, err := range errs {
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "limit of 3 connected agents") {
			t.Fatalf("refusal %v, want FailedPrecondition naming the limit", err)
		}
	}

	// A session that ends frees its room.
	admitted[0]()
	admitted[0]() // idempotent
	if _, err := d.validateAgentConnection(context.Background(), "late", "", d.IPInventory); err != nil {
		t.Fatalf("a connect after a session ended was refused: %v", err)
	}
	if _, err := d.validateAgentConnection(context.Background(), "later", "", d.IPInventory); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a connect past the limit: %v, want FailedPrecondition", err)
	}
}
