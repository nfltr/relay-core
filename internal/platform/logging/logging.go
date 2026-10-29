// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package logging provides opinionated slog initialization for the RPC server.
//
// It replaces the previous logrus-based logging with Go's standard log/slog,
// providing JSON-formatted structured logging suitable for production use.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// New creates a new *slog.Logger with JSON output.
// level is parsed from a string (debug, info, warn, error); defaults to info.
func New(level string, opts ...Option) *slog.Logger {
	cfg := &config{
		level:  parseLevel(level),
		writer: os.Stderr,
		source: false,
	}
	for _, o := range opts {
		o(cfg)
	}

	handlerOpts := &slog.HandlerOptions{
		Level:     cfg.level,
		AddSource: cfg.source,
	}

	handler := slog.NewJSONHandler(cfg.writer, handlerOpts)
	return slog.New(handler)
}

// config holds logger configuration.
type config struct {
	level  slog.Leveler
	writer io.Writer
	source bool
}

// Option configures a logger.
type Option func(*config)

// WithSource enables source file/line in log output.
func WithSource() Option {
	return func(c *config) { c.source = true }
}

// WithWriter sets the output writer.
func WithWriter(w io.Writer) Option {
	return func(c *config) { c.writer = w }
}

// WithLevelVar uses a *slog.LevelVar for dynamic level changes.
func WithLevelVar(lv *slog.LevelVar) Option {
	return func(c *config) { c.level = lv }
}

// parseLevel converts a level string to slog.Level.
// Accepts: debug, info, warn/warning, error. Defaults to info.
func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug", "trace":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error", "fatal", "panic":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// ParseLevel converts a string to slog.Level (exported for use in config).
func ParseLevel(s string) slog.Level {
	return parseLevel(s)
}
