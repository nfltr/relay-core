// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package txnregistry

import (
	"context"
	"sync/atomic"
	"time"

	"log/slog"

	"errors"

	"github.com/nfltr/relay-core/platform/util"
	"github.com/nfltr/relay/client/handoff"
)

// ErrTransactionNotFound is returned when a Fanout targets a transaction
// ID that has not been registered (or has already been cleaned up).
var ErrTransactionNotFound = errors.New("transaction not found")

// ErrTransactionStale is returned when a Fanout targets a transaction that
// has already completed, been cancelled, or otherwise expired.
var ErrTransactionStale = errors.New("stale transaction")

// ErrTransactionStalled is returned when a Fanout's consumer took no
// response for the registry's Stall: the transaction is refused (its
// handle's Stalled closes, later responses are stale) rather than let it
// hold the receive loop that serves every other transaction of its sender.
var ErrTransactionStalled = errors.New("transaction refused: its consumer took no response for the stall bound")

const transactionTombstoneTTL = 30 * time.Second

// txnEntry holds the per-transaction state stored in the single internal map.
type txnEntry[T any] struct {
	ch      chan T
	ctx     context.Context
	stalled chan struct{} // closed by the Fanout that removes a stalled entry
}

// TransactionHandle bundles the response channel for a single transaction.
// It is returned by Register and provides the caller with a receive-only
// channel to consume responses from.
type TransactionHandle[T any] struct {
	TransactionID string
	ResponseCh    <-chan T
	// Stalled closes when the transaction is refused because its consumer
	// took no response for the stall bound (ErrTransactionStalled): no more
	// responses come, and the consumer must not end its caller's stream as
	// if it were complete.
	Stalled <-chan struct{}
}

// TransactionRegistry manages the lifecycle of downstream/upstream transaction
// state. It maps transaction IDs to a buffered channel (size 1) and request context.
//
// The type parameter T is the element type carried on per-transaction response
// channels (e.g. *pb.TunnelFrame for gRPC, *pb.HTTPProxyResponse for HTTP).
//
// Design: a single sync map holds all per-transaction state. There are no
// per-transaction mutexes, no sync.Once, and channels are never closed by the
// registry. Cleanup simply deletes the map entry, which makes any subsequent
// Fanout return ErrTransactionNotFound. Callers read from ResponseCh via
// select with their own context, so an unclosed channel is safe.
type TransactionRegistry[T any] struct {
	entries    *util.SyncMap[string, *txnEntry[T]]
	tombstones *util.SyncMap[string, time.Time]
	// Stall bounds a Fanout's wait on a full transaction (handoff.Stall);
	// set before use, tests shorten it.
	Stall  time.Duration
	reaper *reaper // StartReaper's; nil: none
}

// reaper is StartReaper's configuration; running is set while its goroutine
// runs, which is only while the registry holds entries or tombstones.
type reaper struct {
	ctx      context.Context
	interval time.Duration
	logger   *slog.Logger
	onTick   func(size int)
	running  atomic.Bool
}

// NewTransactionRegistry creates a new TransactionRegistry with an empty map.
func NewTransactionRegistry[T any]() *TransactionRegistry[T] {
	return &TransactionRegistry[T]{
		entries:    &util.SyncMap[string, *txnEntry[T]]{},
		tombstones: &util.SyncMap[string, time.Time]{},
		Stall:      handoff.Stall,
	}
}

// Register creates a new transaction entry with a buffered response channel
// and associates it with the given request context. It returns a
// TransactionHandle (with a receive-only view of the channel) and a cleanup
// function that removes the entry from the registry.
//
// The channel buffer is sized to absorb short out-of-order bursts from
// concurrent fanout workers (default 4) without blocking, while capping
// per-transaction memory to ~64 MB (16 slots × 4 MB gRPC messages).
// Back-pressure propagates while the consumer keeps taking: full channel →
// waiting fanout worker → broker flow control; a consumer that takes nothing
// for Stall has its transaction refused (Fanout, ErrTransactionStalled).
//
// The channel is never closed by the registry. Callers should use select with
// their context to read from ResponseCh, rather than ranging over it.
//
//	handle, cleanup := registry.Register(txnID, ctx, logger)
//	defer cleanup()
func (r *TransactionRegistry[T]) Register(transactionID string, ctx context.Context, logger *slog.Logger) (*TransactionHandle[T], func()) {
	ch := make(chan T, 16)
	stalled := make(chan struct{})
	r.entries.Store(transactionID, &txnEntry[T]{ch: ch, ctx: ctx, stalled: stalled})
	r.wakeReaper()

	handle := &TransactionHandle[T]{
		TransactionID: transactionID,
		ResponseCh:    ch,
		Stalled:       stalled,
	}

	cleanup := func() {
		r.entries.Delete(transactionID)
		r.tombstones.Store(transactionID, time.Now().Add(transactionTombstoneTTL))
		r.wakeReaper()
		logger.Debug("cleaned up transaction", "transactionID", transactionID)
	}

	return handle, cleanup
}

// Fanout routes a message to the response channel for the given transaction ID.
// It respects the request context that was supplied during Register: if the
// context is already cancelled, the send is skipped and an error is returned.
//
// Returns ErrTransactionNotFound if the transaction ID was never registered,
// or ErrTransactionStale when a recent transaction has already completed and
// is still within the short tombstone window.
func (r *TransactionRegistry[T]) Fanout(transactionID string, msg T, logger *slog.Logger) error {
	entry, ok := r.entries.Load(transactionID)
	if !ok {
		if tombstoneUntil, stale := r.tombstones.Load(transactionID); stale {
			if time.Now().Before(tombstoneUntil) {
				logger.Debug("dropping response for stale transaction", "transactionID", transactionID)
				return ErrTransactionStale
			}
			r.tombstones.Delete(transactionID)
		}
		return ErrTransactionNotFound
	}

	// Fast-path: if the request context is already done, skip the send.
	// With a buffered channel the select below would non-deterministically
	// choose between send and ctx.Done; checking upfront makes this deterministic.
	if entry.ctx.Err() != nil {
		logger.Debug("dropping response for stale transaction", "transactionID", transactionID, "reason", entry.ctx.Err())
		return errors.Join(ErrTransactionStale, entry.ctx.Err())
	}

	// The caller's loop serves every transaction of its sender: it waits on
	// this one only while its consumer keeps taking, never past r.Stall.
	switch err := handoff.Send(entry.ch, msg, entry.ctx.Done(), r.Stall); {
	case err == nil:
		logger.Debug("loaded response to response channel", "transactionID", transactionID)
		return nil
	case errors.Is(err, handoff.ErrStalled):
		if r.entries.CompareAndDelete(transactionID, entry) { // once, whichever Fanout stalled first
			r.tombstones.Store(transactionID, time.Now().Add(transactionTombstoneTTL))
			r.wakeReaper()
			close(entry.stalled)
		}
		logger.Warn("transaction refused: its consumer took no response for the stall bound", "transactionID", transactionID, "stall", r.Stall)
		return ErrTransactionStalled
	default:
		logger.Debug("dropping response for stale transaction", "transactionID", transactionID, "reason", entry.ctx.Err())
		return errors.Join(ErrTransactionStale, entry.ctx.Err())
	}
}

// IsRegistered reports whether a transaction with the given ID is currently
// registered.
func (r *TransactionRegistry[T]) IsRegistered(transactionID string) bool {
	_, ok := r.entries.Load(transactionID)
	return ok
}

// RangeTransactionIDs iterates over all registered transaction IDs.
// If the callback returns false, iteration stops. Only keys are exposed;
// internal state remains encapsulated.
func (r *TransactionRegistry[T]) RangeTransactionIDs(fn func(key string) bool) {
	r.entries.Range(func(key string, _ *txnEntry[T]) bool {
		return fn(key)
	})
}

// Size returns the number of entries currently in the registry.
func (r *TransactionRegistry[T]) Size() int {
	var count int
	r.entries.Range(func(_ string, _ *txnEntry[T]) bool {
		count++
		return true
	})
	return count
}

// StartReaper sets up a background reaper that periodically removes entries
// whose request context has already been cancelled and expired tombstones.
// This is a safety net for entries that leak when a caller's defer-cleanup is
// skipped (e.g. due to a panic). Call it before Register. The reaper runs only
// while the registry holds entries or tombstones (Register starts it, it stops
// after a tick that leaves the registry empty), so an idle registry has no
// goroutine and no timer; it never starts again once ctx is cancelled.
// If onTick is non-nil, it is called after each reap with the current registry
// size (0 on the reaper's last tick).
func (r *TransactionRegistry[T]) StartReaper(ctx context.Context, interval time.Duration, logger *slog.Logger, onTick ...func(size int)) {
	rp := &reaper{ctx: ctx, interval: interval, logger: logger}
	if len(onTick) > 0 {
		rp.onTick = onTick[0]
	}
	r.reaper = rp
}

// wakeReaper starts the reaper's goroutine unless it runs (a load first:
// every Register calls it, and a running reaper is the common case).
func (r *TransactionRegistry[T]) wakeReaper() {
	if rp := r.reaper; rp != nil && !rp.running.Load() && rp.running.CompareAndSwap(false, true) {
		go r.reap(rp)
	}
}

// empty reports whether the registry holds no entry and no tombstone.
func (r *TransactionRegistry[T]) empty() bool {
	empty := true
	r.entries.Range(func(string, *txnEntry[T]) bool { empty = false; return false })
	r.tombstones.Range(func(string, time.Time) bool { empty = false; return false })
	return empty
}

func (r *TransactionRegistry[T]) reap(rp *reaper) {
	ticker := time.NewTicker(rp.interval)
	defer ticker.Stop()
	for {
		select {
		case <-rp.ctx.Done():
			return // running stays set: no restart after shutdown
		case <-ticker.C:
		}
		var reaped int
		r.entries.Range(func(key string, entry *txnEntry[T]) bool {
			if entry.ctx.Err() != nil {
				r.entries.Delete(key)
				r.tombstones.Store(key, time.Now().Add(transactionTombstoneTTL))
				reaped++
			}
			return true
		})
		r.tombstones.Range(func(key string, expiresAt time.Time) bool {
			if !time.Now().Before(expiresAt) {
				r.tombstones.Delete(key)
			}
			return true
		})
		if reaped > 0 {
			rp.logger.Info("transaction reaper: removed stale entries", "count", reaped)
		}
		if rp.onTick != nil {
			rp.onTick(r.Size())
		}
		if r.empty() {
			// Stop; a Register racing this either sees running cleared and
			// starts a new reaper, or stored its entry before the second
			// check and this one carries on.
			rp.running.Store(false)
			if r.empty() || !rp.running.CompareAndSwap(false, true) {
				return
			}
		}
	}
}
