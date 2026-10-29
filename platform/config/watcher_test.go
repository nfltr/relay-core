// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestNewConfigWatcher_LoadsInitialConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("logLevel: warn\n"), 0644); err != nil {
		t.Fatal(err)
	}

	level := new(slog.LevelVar)
	level.Set(slog.LevelDebug)
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: level}))
	w, err := NewConfigWatcher(path, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w.OnChange(ApplyLogLevel(level, logger))
	w.ApplyCurrent()

	cfg := w.Current()
	if cfg.LogLevel != "warn" {
		t.Fatalf("expected logLevel=warn, got %q", cfg.LogLevel)
	}
	if level.Level() != slog.LevelWarn {
		t.Fatalf("expected logger level Warn, got %s", level.Level())
	}
}

func TestNewConfigWatcher_MissingFileDoesNotError(t *testing.T) {
	logger := testLogger()
	w, err := NewConfigWatcher("/nonexistent/config.yaml", logger)
	if err != nil {
		t.Fatalf("expected nil error for missing file, got: %v", err)
	}
	cfg := w.Current()
	if cfg.LogLevel != "" {
		t.Fatalf("expected empty logLevel, got %q", cfg.LogLevel)
	}
}

func TestConfigWatcher_ReloadsOnFileChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("logLevel: info\n"), 0644); err != nil {
		t.Fatal(err)
	}

	level := new(slog.LevelVar)
	level.Set(slog.LevelDebug)
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: level}))
	w, err := NewConfigWatcher(path, logger)
	if err != nil {
		t.Fatal(err)
	}
	w.OnChange(ApplyLogLevel(level, logger))

	ctx := t.Context()

	go func() {
		_ = w.Run(ctx)
	}()

	// Give watcher time to start
	time.Sleep(200 * time.Millisecond)

	// Update the file
	if err := os.WriteFile(path, []byte("logLevel: error\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Wait for debounce + reload
	time.Sleep(2 * time.Second)

	cfg := w.Current()
	if cfg.LogLevel != "error" {
		t.Fatalf("expected logLevel=error after update, got %q", cfg.LogLevel)
	}
	if level.Level() != slog.LevelError {
		t.Fatalf("expected logger level Error, got %s", level.Level())
	}
}

// newWatcherWithLogLevel creates a ConfigWatcher from initialYAML, registers
// ApplyLogLevel, calls ApplyCurrent, and asserts the logger is at wantLevel.
func newWatcherWithLogLevel(t *testing.T, initialYAML string, wantLevel slog.Level) (*ConfigWatcher, *slog.LevelVar) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(initialYAML), 0644); err != nil {
		t.Fatal(err)
	}
	level := new(slog.LevelVar)
	level.Set(slog.LevelDebug)
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: level}))
	w, err := NewConfigWatcher(path, logger)
	if err != nil {
		t.Fatal(err)
	}
	w.OnChange(ApplyLogLevel(level, logger))
	w.ApplyCurrent()
	if level.Level() != wantLevel {
		t.Fatalf("expected %s, got %s", wantLevel, level.Level())
	}
	return w, level
}

func TestConfigWatcher_InvalidLevelKeepsCurrent(t *testing.T) {
	w, level := newWatcherWithLogLevel(t, "logLevel: info\n", slog.LevelInfo)

	// Write an invalid level
	if err := os.WriteFile(w.path, []byte("logLevel: invalid_level\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Force a reload
	_ = w.reload()

	// Logger level should remain info
	if level.Level() != slog.LevelInfo {
		t.Fatalf("expected logger level to remain Info after invalid value, got %s", level.Level())
	}
}

func TestConfigWatcher_EmptyLogLevelPreservesLevel(t *testing.T) {
	w, level := newWatcherWithLogLevel(t, "logLevel: warn\n", slog.LevelWarn)

	// Write empty log level
	if err := os.WriteFile(w.path, []byte("logLevel: \"\"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	_ = w.reload()

	// Should keep the previous level set by the initial load
	if level.Level() != slog.LevelWarn {
		t.Fatalf("expected Warn to be preserved, got %s", level.Level())
	}
}

func TestConfigWatcher_OnChangeCallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("logLevel: debug\n"), 0644); err != nil {
		t.Fatal(err)
	}

	logger := testLogger()

	var mu sync.Mutex
	var prevCfg, currCfg RuntimeConfig

	w, err := NewConfigWatcher(path, logger)
	if err != nil {
		t.Fatal(err)
	}
	// Register an additional callback
	w.OnChange(func(prev, curr RuntimeConfig) {
		mu.Lock()
		defer mu.Unlock()
		prevCfg = prev
		currCfg = curr
	})

	// Write new config and reload
	if err := os.WriteFile(path, []byte("logLevel: error\n"), 0644); err != nil {
		t.Fatal(err)
	}
	_ = w.reload()

	mu.Lock()
	defer mu.Unlock()
	if currCfg.LogLevel != "error" {
		t.Fatalf("expected OnChange callback to receive logLevel=error, got %q", currCfg.LogLevel)
	}
	if prevCfg.LogLevel != "debug" {
		t.Fatalf("expected OnChange callback to receive prev logLevel=debug, got %q", prevCfg.LogLevel)
	}
}

func TestConfigWatcher_MultipleCallbacks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("logLevel: info\n"), 0644); err != nil {
		t.Fatal(err)
	}

	logger := testLogger()
	w, err := NewConfigWatcher(path, logger)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var callOrder []int

	// Register two additional callbacks
	w.OnChange(func(_, _ RuntimeConfig) {
		mu.Lock()
		defer mu.Unlock()
		callOrder = append(callOrder, 1)
	})
	w.OnChange(func(_, _ RuntimeConfig) {
		mu.Lock()
		defer mu.Unlock()
		callOrder = append(callOrder, 2)
	})

	// Trigger a change
	if err := os.WriteFile(path, []byte("logLevel: warn\n"), 0644); err != nil {
		t.Fatal(err)
	}
	_ = w.reload()

	mu.Lock()
	defer mu.Unlock()
	// Callbacks 1 and 2 in registration order
	if len(callOrder) != 2 || callOrder[0] != 1 || callOrder[1] != 2 {
		t.Fatalf("expected callbacks called in order [1, 2], got %v", callOrder)
	}
}

func TestConfigWatcher_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("logLevel: info\n"), 0644); err != nil {
		t.Fatal(err)
	}

	logger := testLogger()
	w, err := NewConfigWatcher(path, logger)
	if err != nil {
		t.Fatal(err)
	}

	// Write invalid YAML
	if err := os.WriteFile(path, []byte("{{invalid yaml}}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	err = w.reload()
	if err == nil {
		t.Fatal("expected error on invalid YAML, got nil")
	}

	// Config should remain unchanged
	cfg := w.Current()
	if cfg.LogLevel != "info" {
		t.Fatalf("expected logLevel to remain info, got %q", cfg.LogLevel)
	}
}

func TestConfigWatcher_PollFallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	logger := testLogger()
	w, err := NewConfigWatcher(path, logger)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		// poll() is used when fsnotify can't watch. We test Run which
		// may fall back to polling if the file doesn't exist.
		_ = w.Run(ctx)
		close(done)
	}()

	// Cancel quickly — just verify it doesn't hang.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run didn't stop after context cancel")
	}
}

func TestConfigWatcher_RunStopsOnCancel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("logLevel: info\n"), 0644); err != nil {
		t.Fatal(err)
	}

	logger := testLogger()
	w, err := NewConfigWatcher(path, logger)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error on cancel, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run didn't stop after context cancel")
	}
}
