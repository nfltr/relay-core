// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package acceptloop is an accept loop's answer to an Accept error: a
// closed listener ends the loop; anything else (EMFILE, ENFILE,
// ECONNABORTED) is counted, logged and retried after a backoff, as net/http
// does, so one error never stops a port.
package acceptloop

import (
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var acceptErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "nfltr_relay_accept_errors_total",
	Help: "Accept errors other than a closed listener (EMFILE, ENFILE, ECONNABORTED), by listener: each is retried after a backoff of 5 ms doubling to 1 s.",
}, []string{"listener"})

// Backoff paces one accept loop's retries. The zero value is ready.
type Backoff struct{ d time.Duration }

// Retry reports whether the loop calls Accept again after err: false for a
// closed listener (net.ErrClosed); otherwise it counts err under listener,
// logs it (logger may be nil) and sleeps the next backoff first.
func (b *Backoff) Retry(logger *slog.Logger, listener string, err error) bool {
	if errors.Is(err, net.ErrClosed) {
		return false
	}
	b.d = min(max(2*b.d, 5*time.Millisecond), time.Second)
	acceptErrors.WithLabelValues(listener).Inc()
	if logger != nil {
		logger.Warn("accept error; retrying", "listener", listener, "error", err, "backoff", b.d)
	}
	time.Sleep(b.d)
	return true
}

// Reset starts the backoff over after a successful Accept.
func (b *Backoff) Reset() { b.d = 0 }
