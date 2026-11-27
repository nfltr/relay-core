// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"

	"github.com/nfltr/relay/proto/wire/agentwire"
	"golang.org/x/time/rate"
)

// An HTTP agent's LabelUpdate frames: at most labelUpdateRate a second,
// labelUpdateBurst at once; past it the stream's receive loop waits, so a
// flapping agent slows only its own stream (each update is one record
// write and one presence publish).
const (
	labelUpdateRate  rate.Limit = 5
	labelUpdateBurst            = 10
)

// applyHTTPLabelUpdate applies an HTTP agent's LabelUpdate frame to its
// stream (docs/decisions/http-agent-label-updates-2026-11-12.md): the
// agent's whole update set, its record the dispatcher record. An update
// past agentwire.CheckLabelUpdate is refused, applying nothing, and logged
// (a conforming agent checks first); one on a stream a newer session
// superseded, or whose teardown began, changes nothing.
func (d *Dispatcher) applyHTTPLabelUpdate(ctx context.Context, stream *HTTPStream, labels map[string]string) error {
	if err := agentwire.CheckLabelUpdate(labels); err != nil {
		d.Logger.Warn("label update refused", "agent_id", stream.ID, "error", err)
		return nil
	}
	if stream.labelRate != nil {
		if err := stream.labelRate.Wait(ctx); err != nil {
			return nil // the session ended: nothing to apply
		}
	}
	if _, ok := applyLabelUpdate(d, d.IPInventory, stream.ID, stream, labels, false); !ok {
		d.Logger.Debug("label update not applied: the session was superseded or ended", "agent_id", stream.ID)
	}
	return nil
}
