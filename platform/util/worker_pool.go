// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// WorkerPool runs a worker on every input of a channel, a bounded number at
// once.
type WorkerPool[T any] struct {
	inputs           <-chan T
	concurrencyLimit int
	worker           func(context.Context, T) error
}

// NewWorkerPool runs worker on inputs, concurrencyLimit goroutines at once
// (0 or less: one, in Run's goroutine).
func NewWorkerPool[T any](inputs <-chan T, concurrencyLimit int, worker func(context.Context, T) error) *WorkerPool[T] {
	return &WorkerPool[T]{
		inputs:           inputs,
		concurrencyLimit: concurrencyLimit,
		worker:           worker,
	}
}

// Run works until inputs is closed, ctx ends (its error) or a worker fails
// (its error, which ends the others).
func (pool *WorkerPool[T]) Run(ctx context.Context) error {
	run := func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case input, ok := <-pool.inputs:
				if !ok {
					return nil
				}
				if err := pool.worker(ctx, input); err != nil {
					return err
				}
			}
		}
	}
	if pool.concurrencyLimit <= 0 {
		return run(ctx)
	}

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < pool.concurrencyLimit; i++ {
		eg.Go(func() error {
			return run(ctx)
		})
	}

	return eg.Wait()
}
