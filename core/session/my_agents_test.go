// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"time"

	"github.com/nfltr/relay-core/core/directory"
)

type listOnlyInventory struct {
	agentIDs []string
	records  []directory.AgentRecord
	err      error
}

func (l *listOnlyInventory) GetIP(context.Context, string) (string, error) {
	return "", l.err
}

func (l *listOnlyInventory) ListAgentIDs(context.Context) ([]string, error) {
	if l.err != nil {
		return nil, l.err
	}
	if len(l.agentIDs) == 0 && len(l.records) > 0 {
		out := make([]string, 0, len(l.records))
		for _, rec := range l.records {
			out = append(out, rec.AgentID)
		}
		return out, nil
	}
	return append([]string(nil), l.agentIDs...), nil
}

func (l *listOnlyInventory) ListAgentRecords(context.Context) ([]directory.AgentRecord, error) {
	if l.err != nil {
		return nil, l.err
	}
	if len(l.records) > 0 {
		return append([]directory.AgentRecord(nil), l.records...), nil
	}
	records := make([]directory.AgentRecord, 0, len(l.agentIDs))
	for _, id := range l.agentIDs {
		records = append(records, directory.AgentRecord{AgentID: id, PodIP: "127.0.0.1"})
	}
	return records, nil
}

func (l *listOnlyInventory) SetIP(context.Context, string, time.Duration) error {
	return l.err
}

func (l *listOnlyInventory) SetIPWithLabels(context.Context, string, map[string]string, time.Duration) error {
	return l.err
}

func (l *listOnlyInventory) SetIPWithAgentInfo(context.Context, string, map[string]string, []string, time.Duration) error {
	return l.err
}

func (l *listOnlyInventory) DeleteIPs(context.Context, ...string) error {
	return l.err
}
