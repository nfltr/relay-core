// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nfltr/relay-core/core/directory"
)

// AgentLastSeen returns the last time inbound activity (including transport
// heartbeats) was observed from agentID, and whether any activity has been
// recorded for it at all. It reports the raw transport fact only — no
// liveness threshold is applied here.
func (d *Dispatcher) AgentLastSeen(agentID string) (time.Time, bool) {
	if d == nil || agentID == "" {
		return time.Time{}, false
	}
	v, ok := d.lastSeenByAgentID.Load(agentID)
	if !ok {
		return time.Time{}, false
	}
	return seenEpoch.Add(time.Duration(v.(*atomic.Int64).Load())), true
}

// WorkerPresent reports whether an agent is connected to the relay. An
// agent attached to this pod is present. In multi-pod mode (an Owner exists)
// a worker attached elsewhere is found through the same cluster inventory
// the pod dialer routes by: a record naming another pod means present, no
// record means absent, and an inventory failure means unknown so the caller
// never fences a live cross-pod session on a lookup error. A record naming
// this pod defers to the local maps above, which own this pod's connections.
// Without an Owner (single pod) the local maps stay the sole authority.
func (d *Dispatcher) WorkerPresent(workerID string) (present, known bool) {
	workerID = strings.TrimSpace(workerID)
	if d == nil || workerID == "" {
		return false, true
	}
	if d.workerAttachedHere(workerID) {
		return true, true
	}
	if d.Owner == nil {
		return false, true
	}
	peer, err := d.peerOwnsWorker(context.Background(), workerID)
	switch {
	case errors.Is(err, directory.ErrAgentNotConnected):
		return false, true
	case err != nil:
		return false, false
	}
	return peer, true
}

// peerOwnsWorker reports whether the cluster inventory (Owner) names another
// pod as workerID's owner. A record naming this pod is not peer ownership:
// this pod's own connection maps are authoritative for it.
func (d *Dispatcher) peerOwnsWorker(ctx context.Context, workerID string) (bool, error) {
	replica, local, err := d.Owner.OwnerOf(ctx, workerID)
	if err != nil {
		return false, err
	}
	return replica != "" && !local, nil
}

// workerAttachedHere reports whether workerID has a stream on this pod (HTTP,
// gRPC upstream or TCP).
func (d *Dispatcher) workerAttachedHere(workerID string) bool {
	if _, ok := d.Agents.Load(workerID); ok {
		return true
	}
	if _, ok := d.Upstreams.Load(workerID); ok {
		return true
	}
	return slices.Contains(d.GetTCPAgentAgentIDs(), workerID)
}
