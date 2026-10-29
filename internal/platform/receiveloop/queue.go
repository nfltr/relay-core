// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package receiveloop

import (
	"context"
	"sync"
)

// queue is a FIFO of at most max items for one pusher and one popper. Unlike
// a buffered channel it allocates storage as items arrive and lets it go
// when it drains, so an idle session holds none.
type queue[T any] struct {
	mu     sync.Mutex
	buf    []T // buf[head:] are queued
	head   int
	max    int
	closed bool
	added  chan struct{} // one pending wakeup: an empty queue got an item, or closed
	taken  chan struct{} // one pending wakeup: a full queue lost an item
}

// keptSlots is the storage a drained queue keeps, so that steady traffic
// does not allocate per item.
const keptSlots = 16

func newQueue[T any](n int) *queue[T] {
	return &queue[T]{max: max(n, 1), added: make(chan struct{}, 1), taken: make(chan struct{}, 1)}
}

func wake(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

// push adds v, waiting while the queue is full, until ctx is done.
func (q *queue[T]) push(ctx context.Context, v T) error {
	for {
		q.mu.Lock()
		if n := len(q.buf) - q.head; n < q.max {
			if q.head > 0 && q.head >= len(q.buf)/2 { // reuse the consumed front
				q.buf = q.buf[:copy(q.buf, q.buf[q.head:])]
				q.head = 0
			}
			q.buf = append(q.buf, v)
			q.mu.Unlock()
			if n == 0 { // pop waits only on an empty queue
				wake(q.added)
			}
			return nil
		}
		q.mu.Unlock()
		select {
		case <-q.taken:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// pop returns the oldest item, waiting for one; false once the queue is
// closed and empty.
func (q *queue[T]) pop() (T, bool) {
	for {
		q.mu.Lock()
		if n := len(q.buf) - q.head; n > 0 {
			v := q.buf[q.head]
			var zero T
			q.buf[q.head] = zero
			q.head++
			if q.head == len(q.buf) { // drained: keep a little storage, let a burst's go
				q.buf, q.head = q.buf[:0], 0
				if cap(q.buf) > keptSlots {
					q.buf = nil
				}
			}
			q.mu.Unlock()
			if n == q.max { // push waits only on a full queue
				wake(q.taken)
			}
			return v, true
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			var zero T
			return zero, false
		}
		<-q.added
	}
}

// close ends the queue: pop returns what is queued, then false.
func (q *queue[T]) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	wake(q.added)
}
