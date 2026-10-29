// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"time"

	"github.com/nfltr/relay-core/core/directory"
)

// errorInventory implements IPInventory and always returns an error
type errorInventory struct {
	err error
}

func (e *errorInventory) GetIP(ctx context.Context, agentID string) (string, error) {
	return "", e.err
}

func (e *errorInventory) ListAgentIDs(ctx context.Context) ([]string, error) {
	return nil, e.err
}

func (e *errorInventory) ListAgentRecords(ctx context.Context) ([]directory.AgentRecord, error) {
	return nil, e.err
}

func (e *errorInventory) SetIP(ctx context.Context, agentID string, expiration time.Duration) error {
	return e.err
}

func (e *errorInventory) SetIPWithLabels(_ context.Context, _ string, _ map[string]string, _ time.Duration) error {
	return e.err
}

func (e *errorInventory) SetIPWithAgentInfo(_ context.Context, _ string, _ map[string]string, _ []string, _ time.Duration) error {
	return e.err
}

func (e *errorInventory) DeleteIPs(ctx context.Context, agentIDs ...string) error {
	return e.err
}
