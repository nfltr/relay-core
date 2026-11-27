// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
)

// RuntimeConfig holds settings that can be changed at runtime via a
// ConfigMap-mounted file without restarting the pod.
type RuntimeConfig struct {
	LogLevel string `yaml:"logLevel"`
}

// ChangeFunc is invoked whenever the runtime config changes.  It receives
// the previous and new configs so that listeners can react only to the
// fields they care about.
type ChangeFunc func(prev, curr RuntimeConfig)

// ConfigWatcher watches a YAML file (typically mounted from a ConfigMap) and
// applies runtime-tunable settings whenever the file changes.  Subsystems
// register interest via OnChange; callbacks are invoked in registration order.
type ConfigWatcher struct {
	path    string
	logger  *slog.Logger
	mu      sync.RWMutex
	current RuntimeConfig

	cbMu      sync.RWMutex
	callbacks []ChangeFunc
}

// NewConfigWatcher creates a watcher for the given file path.  It reads the
// file once synchronously so that the initial config is available before the
// background goroutine starts.
func NewConfigWatcher(path string, logger *slog.Logger) (*ConfigWatcher, error) {
	w := &ConfigWatcher{
		path:   path,
		logger: logger,
	}

	// Read initial config; if the file doesn't exist yet (e.g. ConfigMap not
	// mounted), start with an empty config and wait for it to appear.
	if err := w.reload(); err != nil {
		logger.Warn("config-watcher: could not read initial config, will use defaults", "path", path, "error", err)
	}
	return w, nil
}

// OnChange registers a callback that is invoked whenever the runtime config
// changes.  Callbacks are called in registration order.  It is safe to call
// OnChange before Run.
func (w *ConfigWatcher) OnChange(fn ChangeFunc) {
	w.cbMu.Lock()
	defer w.cbMu.Unlock()
	w.callbacks = append(w.callbacks, fn)
}

// ApplyCurrent forces all registered callbacks to run with the current config.
// Call this after registering callbacks to apply the initial configuration
// that was loaded during NewConfigWatcher.
func (w *ConfigWatcher) ApplyCurrent() {
	w.mu.RLock()
	curr := w.current
	w.mu.RUnlock()
	w.notifyCallbacks(RuntimeConfig{}, curr)
}

// Current returns a copy of the last-loaded runtime config.
func (w *ConfigWatcher) Current() RuntimeConfig {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.current
}

// Run starts watching the config file for changes.  It blocks until ctx is
// cancelled.  Kubernetes updates ConfigMap-mounted files by replacing the
// target symlink atomically, which produces CREATE/REMOVE events, so we
// watch for all event types.
func (w *ConfigWatcher) Run(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := watcher.Close(); closeErr != nil {
			w.logger.Warn("config-watcher: error closing fsnotify watcher", "error", closeErr)
		}
	}()

	if err := watcher.Add(w.path); err != nil {
		// The file may not exist yet when the pod starts.  In that case we
		// fall back to polling so we don't block forever.
		w.logger.Warn("config-watcher: cannot watch path, falling back to polling", "path", w.path, "error", err)
		return w.poll(ctx)
	}

	w.logger.Info("config-watcher: watching for runtime config changes", "path", w.path)

	// Debounce rapid events (Kubernetes does multiple writes atomically).
	var debounce <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("config-watcher: stopped")
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			w.logger.Debug("config-watcher: fsnotify event", "event", event.String())
			// Debounce: wait 500ms for rapid successive events.
			debounce = time.After(500 * time.Millisecond)
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			w.logger.Warn("config-watcher: fsnotify error", "error", err)
		case <-debounce:
			if err := w.reload(); err != nil {
				w.logger.Warn("config-watcher: failed to reload config", "error", err)
			}
		}
	}
}

// poll re-reads the config file every 10 seconds.  Used as a fallback when
// fsnotify cannot watch the file (e.g. file doesn't exist yet).
func (w *ConfigWatcher) poll(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("config-watcher: polling stopped")
			return nil
		case <-ticker.C:
			if err := w.reload(); err != nil {
				w.logger.Debug("config-watcher: poll reload failed", "error", err)
			}
		}
	}
}

// reload reads the YAML file from disk and applies the config.
func (w *ConfigWatcher) reload() error {
	data, err := os.ReadFile(w.path)
	if err != nil {
		return err
	}

	var cfg RuntimeConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}

	// Normalise values.
	cfg.LogLevel = strings.TrimSpace(strings.ToLower(cfg.LogLevel))

	w.mu.Lock()
	prev := w.current
	w.current = cfg
	w.mu.Unlock()

	if cfg != prev {
		w.logger.Info("config-watcher: runtime config changed", "prev", fmt.Sprintf("%+v", prev), "curr", fmt.Sprintf("%+v", cfg))
		w.notifyCallbacks(prev, cfg)
	}

	return nil
}

// notifyCallbacks invokes all registered change callbacks in order.
func (w *ConfigWatcher) notifyCallbacks(prev, curr RuntimeConfig) {
	w.cbMu.RLock()
	cbs := make([]ChangeFunc, len(w.callbacks))
	copy(cbs, w.callbacks)
	w.cbMu.RUnlock()

	for _, fn := range cbs {
		fn(prev, curr)
	}
}

// ApplyLogLevel is a ready-made ChangeFunc that adjusts the logger level.
// Register it via cfgWatcher.OnChange(config.ApplyLogLevel(logger)).
func ApplyLogLevel(level *slog.LevelVar, logger *slog.Logger) ChangeFunc {
	return func(_, curr RuntimeConfig) {
		if curr.LogLevel == "" {
			return
		}
		parsed, err := parseSlogLevel(curr.LogLevel)
		if err != nil {
			logger.Warn("config-watcher: invalid log level, keeping current level", "level", curr.LogLevel, "error", err)
			return
		}
		previous := level.Level()
		if parsed != previous {
			level.Set(parsed)
			logger.Info("config-watcher: log level changed", "from", previous.String(), "to", parsed.String())
		}
	}
}

// parseSlogLevel parses a log level string into an slog.Level.
func parseSlogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error": //nolint:goconst // log level label, not worth a constant
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unknown log level: %q", s)
	}
}
