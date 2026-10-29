// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package receiveloop

import (
	"context"
	"errors"
	"testing"
	"time"
)

// An idle queue holds no storage however large its bound (a buffered
// channel allocated the whole bound up front: 8 KiB per idle agent session
// at the relay's 1000), and a drained one keeps at most keptSlots.
func TestQueueStorageGrowsWithUseOnly(t *testing.T) {
	q := newQueue[int](1000)
	if cap(q.buf) != 0 {
		t.Fatalf("new queue holds %d slots, want 0", cap(q.buf))
	}
	for i := range 500 {
		if err := q.push(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 500 {
		if v, ok := q.pop(); !ok || v != i {
			t.Fatalf("pop = %d, %v; want %d, true (FIFO)", v, ok, i)
		}
	}
	if cap(q.buf) > keptSlots {
		t.Fatalf("drained queue keeps %d slots, want <= %d", cap(q.buf), keptSlots)
	}
}

// push waits while the queue holds its bound and resumes when an item is
// taken; ctx ends the wait.
func TestQueueBoundsWhatTheReaderQueues(t *testing.T) {
	q := newQueue[int](2)
	ctx := context.Background()
	_ = q.push(ctx, 1)
	_ = q.push(ctx, 2)
	pushed := make(chan error, 1)
	go func() { pushed <- q.push(ctx, 3) }()
	select {
	case <-pushed:
		t.Fatal("push past the bound did not wait")
	case <-time.After(50 * time.Millisecond):
	}
	if v, _ := q.pop(); v != 1 {
		t.Fatalf("pop = %d, want 1", v)
	}
	if err := <-pushed; err != nil {
		t.Fatal(err)
	}

	cctx, cancel := context.WithCancel(ctx)
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	if err := q.push(cctx, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("push on a full queue after cancel = %v, want context.Canceled", err)
	}
}

// After close, pop returns what is queued, then false; a waiting pop wakes.
func TestQueueCloseDrainsThenEnds(t *testing.T) {
	q := newQueue[int](4)
	_ = q.push(context.Background(), 7)
	q.close()
	if v, ok := q.pop(); !ok || v != 7 {
		t.Fatalf("pop = %d, %v; want 7, true", v, ok)
	}
	if _, ok := q.pop(); ok {
		t.Fatal("pop on a closed, drained queue returned an item")
	}

	q = newQueue[int](4)
	ended := make(chan bool, 1)
	go func() { _, ok := q.pop(); ended <- ok }()
	time.Sleep(20 * time.Millisecond)
	q.close()
	if <-ended {
		t.Fatal("waiting pop returned an item from an empty closed queue")
	}
}
