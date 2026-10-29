// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestNew(t *testing.T) {
	var buf bytes.Buffer
	logger := New("info", WithWriter(&buf))
	logger.Info("hello", "key", "val")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("expected JSON output, got: %s", buf.String())
	}
	if m["msg"] != "hello" {
		t.Errorf("expected msg=hello, got %v", m["msg"])
	}
	if m["key"] != "val" {
		t.Errorf("expected key=val, got %v", m["key"])
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"trace", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"fatal", slog.LevelError},
		{"unknown", slog.LevelInfo},
		{"", slog.LevelInfo},
	}
	for _, tt := range tests {
		got := ParseLevel(tt.input)
		if got != tt.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestWithSource(t *testing.T) {
	var buf bytes.Buffer
	logger := New("info", WithWriter(&buf), WithSource())
	logger.Info("source test")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("expected JSON output, got: %s", buf.String())
	}
	if _, ok := m["source"]; !ok {
		t.Error("expected source field in output")
	}
}

func TestDebugSuppressed(t *testing.T) {
	var buf bytes.Buffer
	logger := New("info", WithWriter(&buf))
	logger.Debug("should not appear")
	if buf.Len() > 0 {
		t.Errorf("debug message should be suppressed at info level, got: %s", buf.String())
	}
}

func TestWithLevelVar(t *testing.T) {
	var buf bytes.Buffer
	lv := &slog.LevelVar{}
	lv.Set(slog.LevelWarn)
	logger := New("debug", WithWriter(&buf), WithLevelVar(lv))

	logger.Info("should be suppressed")
	if buf.Len() > 0 {
		t.Errorf("info should be suppressed at warn level")
	}

	lv.Set(slog.LevelDebug)
	logger.Debug("now visible")
	if buf.Len() == 0 {
		t.Error("debug should be visible after level change")
	}
}
