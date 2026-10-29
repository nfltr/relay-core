// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"testing"
)

// TestWithWritesWhatSlogWithWrites: every record, chained With and group
// comes out as slog.Logger.With's does.
func TestWithWritesWhatSlogWithWrites(t *testing.T) {
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}
	args := []any{"AgentID", "a1", "n", 7, "ok", true, "remote", addr, slog.Group("g", "k", "v"), "dangling"}
	run := func(l *slog.Logger) {
		l.Info("one", "x", 1)
		l.Debug("hidden")
		l.Error("two", "error", errors.New("boom"))
		l.With("more", "m").Warn("three")
		l.WithGroup("grp").Info("four", "y", 2)
	}
	var want, got bytes.Buffer
	opts := &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return a
	}}
	run(slog.New(slog.NewJSONHandler(&want, opts)).With(args...))
	run(With(slog.New(slog.NewJSONHandler(&got, opts)), args...))
	if want.String() != got.String() || want.Len() == 0 {
		t.Fatalf("got\n%s\nwant\n%s", got.String(), want.String())
	}
}

// TestWithFormatsNothingUnlogged is the guard of R2 (docs/performance.md):
// a per-request logger whose records are below the level costs no
// formatting, and at most 4 allocations (the logger, its handler, the
// args and the boxed string).
func TestWithFormatsNothingUnlogged(t *testing.T) {
	base := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)) // info
	id := string([]byte("transaction"))                         // not a constant: boxed per call
	allocs := testing.AllocsPerRun(1000, func() {
		l := With(base, "type", "proxy", "transaction_id", id)
		l.Debug("request completed", "n", 1)
	})
	if allocs > 4 {
		t.Fatalf("%v allocations per unlogged per-request logger, budget 4", allocs)
	}
	if With(base) != base {
		t.Fatal("With of no args is not the logger itself")
	}
	if !With(base, "k", "v").Handler().Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("not enabled at the base's level")
	}
}

func BenchmarkPerRequestLogger(b *testing.B) {
	base := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	id := string([]byte("transaction"))
	for _, c := range []struct {
		name string
		with func(*slog.Logger, ...any) *slog.Logger
	}{{"slog", (*slog.Logger).With}, {"deferred", With}} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				c.with(base, "AgentID", id, "type", "proxy", "transaction_id", id).Debug("request completed")
			}
		})
	}
}
