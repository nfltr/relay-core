// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package clientgen is the relay's client generation gate: every client of
// nfltr v2.0.0 or later sends its generation (agentwire.GenerationKey) on
// each gRPC call and tcp-connect request; security.min_client_generation
// (env SECURITY_MIN_CLIENT_GENERATION) refuses those below it. 0, the
// default, takes every client and counts and logs those below the current
// generation (agentwire.Generation: 3, link chaining; 2, nfltr v2.0.0 to
// v2.1.6, answers sessions in band), so an operator sees who still
// connects before raising it.
package clientgen

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/spf13/viper"
)

// Key is the setting.
const Key = "security.min_client_generation"

// Min is the setting's value (0: take every client). It reads viper: call
// it once, when a server starts.
func Min() int { return max(viper.GetInt(Key), 0) }

// Admit reports whether a client of generation gen is taken under min. A
// client below agentwire.Generation is counted (refused, or taken on transport:
// "grpc", "http") and logged at most once per logEvery. It reads no store:
// the refusal path stays cheap for old clients that retry.
func Admit(min, gen int, transport, client string) bool {
	if gen >= agentwire.Generation && gen >= min {
		return true
	}
	ok := gen >= min
	if ok {
		metrics.RecordOldGenerationClient(transport)
	} else {
		metrics.RecordGenerationRefusal()
	}
	logOld(ok, gen, min, transport, client)
	return ok
}

// logEvery bounds the log lines of old clients.
const logEvery = time.Minute

var (
	lastLog    atomic.Int64 // unix nanoseconds of the last line
	suppressed atomic.Int64 // old clients since it
)

func logOld(taken bool, gen, min int, transport, client string) {
	now, last := time.Now().UnixNano(), lastLog.Load()
	if last != 0 && now-last < int64(logEvery) || !lastLog.CompareAndSwap(last, now) {
		suppressed.Add(1)
		return
	}
	msg := fmt.Sprintf("client below generation %d refused (security.min_client_generation)", min)
	if taken {
		msg = fmt.Sprintf("client below generation %d connected: it is refused once security.min_client_generation is %d", agentwire.Generation, agentwire.Generation)
	}
	slog.Warn(msg, "generation", gen, "min", min, "transport", transport, "client", client, "more_since_last_line", suppressed.Swap(0))
}
