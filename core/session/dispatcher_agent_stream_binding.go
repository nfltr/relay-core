// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
)

// errTargetAgentDisconnected ends a transaction whose target agent's stream
// closed. Callers see a retryable transport error and recover (resume or
// redispatch) instead of waiting forever on replies that cannot come.
var errTargetAgentDisconnected = errors.New("target agent disconnected")

// bindToAgentStream returns ctx cancelled also when the target agent's own
// stream ends. A transaction routed to an agent is waiting on that agent's
// replies; when the agent disconnects (process killed, pod deleted) while this
// relay stays up, nothing else would end it. The binding's resources are
// released when ctx ends.
func bindToAgentStream(ctx context.Context, agent Streamer) context.Context {
	if agent == nil || agent.Context() == nil {
		return ctx
	}
	bound, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(agent.Context(), func() { cancel(errTargetAgentDisconnected) })
	context.AfterFunc(bound, func() { stop() })
	return bound
}
