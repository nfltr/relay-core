// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"context"
	"log/slog"
)

// With is logger.With(args...) that formats args only when a record is
// handled. slog's With formats them at once (a JSON handler renders them
// into its prefix), which a logger made per request or per frame paid on
// every one, though at the configured level it logged nothing; the output
// is the same. args are read when a record is handled: pass values that
// do not change (strings, numbers, addresses). docs/performance.md, R2.
func With(logger *slog.Logger, args ...any) *slog.Logger {
	if len(args) == 0 {
		return logger
	}
	return slog.New(&deferred{h: logger.Handler(), args: args})
}

// deferred is a handler with attributes still to add (args, as
// slog.Logger.With takes them).
type deferred struct {
	h    slog.Handler
	args []any
}

func (d *deferred) Enabled(ctx context.Context, l slog.Level) bool { return d.h.Enabled(ctx, l) }

func (d *deferred) Handle(ctx context.Context, r slog.Record) error {
	return d.resolve().Handle(ctx, r)
}

func (d *deferred) WithAttrs(as []slog.Attr) slog.Handler { return d.resolve().WithAttrs(as) }

func (d *deferred) WithGroup(name string) slog.Handler { return d.resolve().WithGroup(name) }

// resolve is the handler slog.Logger.With(args...) would have made.
func (d *deferred) resolve() slog.Handler { return slog.New(d.h).With(d.args...).Handler() }
