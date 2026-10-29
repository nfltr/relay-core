// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"maps"
	"testing"
)

// labelsTestDispatcher holds worker-1, connected with connect labels and
// carrying current labels (connect labels plus an earlier update).
func labelsTestDispatcher(t *testing.T, connect, current map[string]string) (*Dispatcher, *GRPCStream) {
	t.Helper()
	d, cancel := newTestDispatcher(t)
	t.Cleanup(cancel)
	stream := NewGRPCStream("worker-1", nil, nil, nil, nil)
	stream.connectLabels = maps.Clone(connect)
	stream.SetLabels(current)
	d.Upstreams.Store("worker-1", stream)
	return d, stream
}

// The labels an agent connected with stay exactly as advertised: an update
// can neither change nor drop them. A replace update drops the previous
// update's labels; a patch merges into the current set. Empty keys and
// values are dropped, the rest trimmed.
func TestUpdateAgentLabelsConnectLabelsWin(t *testing.T) {
	t.Parallel()
	connect := map[string]string{"flavor": "codex", "host": "a"}
	for _, tc := range []struct {
		name  string
		patch bool
		want  map[string]string
	}{
		{"replace", false, map[string]string{"flavor": "codex", "host": "a", "os": "linux"}},
		{"patch", true, map[string]string{"flavor": "codex", "host": "a", "old": "value", "os": "linux"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, stream := labelsTestDispatcher(t, connect, map[string]string{"flavor": "codex", "host": "a", "old": "value"})
			got, ok := d.UpdateAgentLabels("worker-1", map[string]string{"flavor": "cursor", " os ": " linux ", "": "orphan", "empty": " "}, tc.patch)
			if !ok || !maps.Equal(got, tc.want) || !maps.Equal(stream.Labels(), tc.want) {
				t.Fatalf("UpdateAgentLabels = %v, %v; stream %v; want %v", got, ok, stream.Labels(), tc.want)
			}
		})
	}
}

// An update publishes a presence labels event with the new set.
func TestUpdateAgentLabelsPublishesPresence(t *testing.T) {
	t.Parallel()
	d, _ := labelsTestDispatcher(t, nil, nil)
	base := d.Presence.Watch(t.Context(), "", 0, nil).GetCursor()
	if _, ok := d.UpdateAgentLabels("worker-1", map[string]string{"k": "v"}, false); !ok {
		t.Fatal("UpdateAgentLabels: agent not found")
	}
	evs := d.Presence.Watch(t.Context(), base, 0, nil).GetEvents()
	if len(evs) != 1 || evs[0].GetAgentId() != "worker-1" || !maps.Equal(evs[0].GetLabels().GetLabels(), map[string]string{"k": "v"}) {
		t.Fatalf("presence events = %v, want one labels event for worker-1 {k:v}", evs)
	}
}

// An unknown or empty agent id answers ok=false and changes nothing.
func TestUpdateAgentLabelsUnknownAgent(t *testing.T) {
	t.Parallel()
	d, stream := labelsTestDispatcher(t, nil, map[string]string{"k": "v"})
	for _, id := range []string{"ghost-worker", "   "} {
		if got, ok := d.UpdateAgentLabels(id, map[string]string{"a": "b"}, false); ok || got != nil {
			t.Errorf("UpdateAgentLabels(%q) = %v, %v; want nil, false", id, got, ok)
		}
		if _, ok := d.Upstreams.Load(id); ok {
			t.Errorf("UpdateAgentLabels(%q) stored a stream", id)
		}
	}
	if !maps.Equal(stream.Labels(), map[string]string{"k": "v"}) {
		t.Errorf("labels mutated: %v", stream.Labels())
	}
}
