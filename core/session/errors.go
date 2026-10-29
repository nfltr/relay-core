// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import "errors"

// Sentinel errors used across the Dispatcher.
var (
	podToPodStreamErr   = errors.New("pod to pod stream error") //nolint:staticcheck // ST1012: legacy name, widely referenced
	errServerRestarting = errors.New("server restarting")
	errNoHeartbeat      = errors.New("no heartbeat received from agent")
)

// errA2ATargetNotConnected is the detail an agent-to-agent request gets for
// a target that is not connected, and so for one the caller does not own.
const errA2ATargetNotConnected = "target agent not connected"
