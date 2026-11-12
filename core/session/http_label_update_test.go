// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/spf13/viper"
	"google.golang.org/grpc/metadata"
)

// labelTestDispatcher: a dispatcher whose records are readable (one
// memory store for both inventories).
func labelTestDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	viper.Set("pod_ip.env_var", "TEST_POD_IP") // records name a pod
	t.Cleanup(func() { viper.Set("pod_ip.env_var", "") })
	t.Setenv("TEST_POD_IP", "10.0.0.2")
	d, cancel := newTestDispatcher(t)
	t.Cleanup(cancel)
	mc := directory.NewMemoryCache()
	d.IPInventory, _ = directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	d.StreamerIPInventory, _ = directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	return d
}

// connectHTTPAgent connects agentID with connect labels and pg@db, and
// returns its stream once the relay accepted it.
func connectHTTPAgent(t *testing.T, d *Dispatcher, agentID string, labels map[string]string) pb.Dispatcher_ConnectHTTPClient {
	t.Helper()
	client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, agentID))
	ctx = metadata.AppendToOutgoingContext(ctx, agentservices.Header, pgAtDB, agentservices.ModesHeader, "1", agentwire.LabelsKey, agentwire.EncodeLabels(labels))
	hs, err := client.ConnectHTTP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h, err := hs.Header()
	if err != nil {
		t.Fatal(err)
	}
	if v := h.Get(agentwire.LabelUpdatesKey); len(v) != 1 || v[0] != "1" {
		t.Fatalf("accept %s = %v, want 1", agentwire.LabelUpdatesKey, v)
	}
	return hs
}

// sendLabels sends one LabelUpdate frame and waits for the presence labels
// event it publishes.
func sendLabels(t *testing.T, d *Dispatcher, hs pb.Dispatcher_ConnectHTTPClient, agentID string, labels map[string]string) map[string]string {
	t.Helper()
	base := d.Presence.Watch(t.Context(), "", 0, nil).GetCursor()
	if err := hs.Send(&pb.HTTPProxyResponse{Labels: &pb.LabelUpdate{Labels: labels}}); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		for _, ev := range d.Presence.Watch(t.Context(), base, time.Second, nil).GetEvents() {
			if ev.GetAgentId() == agentID && ev.GetLabels() != nil {
				return ev.GetLabels().GetLabels()
			}
		}
	}
	t.Fatal("no presence labels event")
	return nil
}

// A ConnectHTTP agent's LabelUpdate frame sets its labels on its stream,
// its dispatcher record (the one carrying its services, which other
// replicas read), presence and the instance pick's health read; the
// connect labels stay; the next update replaces the previous one.
func TestHTTPAgentLabelUpdate(t *testing.T) {
	d := labelTestDispatcher(t)
	connect := map[string]string{"zone": "a"}
	hs := connectHTTPAgent(t, d, "svc-agent", connect)

	want := map[string]string{"zone": "a", agentservices.HealthLabel("pg"): agentservices.HealthDown}
	if got := sendLabels(t, d, hs, "svc-agent", map[string]string{"zone": "b", agentservices.HealthLabel("pg"): agentservices.HealthDown}); !maps.Equal(got, want) {
		t.Fatalf("presence labels = %v, want %v", got, want)
	}
	stream, _ := d.Agents.Load("svc-agent")
	if !maps.Equal(stream.Labels(), want) || !maps.Equal(recordLabels(t.Context(), d.IPInventory, "svc-agent"), want) {
		t.Fatalf("stream %v, record %v; want %v", stream.Labels(), recordLabels(t.Context(), d.IPInventory, "svc-agent"), want)
	}
	if !d.serviceDown(t.Context(), "svc-agent", "pg") {
		t.Fatal("pg not down for the instance pick")
	}
	// The other replicas' read: no stream of the agent there, its record.
	d.Agents.Delete("svc-agent")
	if !d.serviceDown(t.Context(), "svc-agent", "pg") {
		t.Fatal("pg not down from the record")
	}
	d.Agents.Store("svc-agent", stream)

	if got := sendLabels(t, d, hs, "svc-agent", nil); !maps.Equal(got, connect) {
		t.Fatalf("after a cleared update: %v, want %v", got, connect)
	}
	if d.serviceDown(t.Context(), "svc-agent", "pg") {
		t.Fatal("pg still down after the update cleared it")
	}
}

// An update past agentwire.CheckLabelUpdate is refused: nothing of it is
// applied, and the stream's next update is.
func TestHTTPAgentLabelUpdatePastBounds(t *testing.T) {
	d := labelTestDispatcher(t)
	hs := connectHTTPAgent(t, d, "svc-agent", nil)
	many := map[string]string{}
	for i := range agentwire.MaxLabelUpdateEntries + 1 {
		many["k"+strconv.Itoa(i)] = "v"
	}
	if err := hs.Send(&pb.HTTPProxyResponse{Labels: &pb.LabelUpdate{Labels: many}}); err != nil {
		t.Fatal(err)
	}
	if got := sendLabels(t, d, hs, "svc-agent", pgDown); !maps.Equal(got, pgDown) {
		t.Fatalf("labels after a refused and a valid update = %v, want %v", got, pgDown)
	}
	if l := recordLabels(t.Context(), d.IPInventory, "svc-agent"); !maps.Equal(l, pgDown) {
		t.Fatalf("record labels %v, want %v", l, pgDown)
	}
}

// An update (or renewal) of a stream whose teardown began writes no record
// (Presence.tla NoResurrect: the receive loop drains frames after the
// session ended, and the release is the last write).
func TestLabelUpdateAfterTeardownWritesNothing(t *testing.T) {
	d := labelTestDispatcher(t)
	stream := NewHTTPStream("svc-agent", nil, nil, nil, nil)
	stream.connectedAt = time.Now()
	stream.markEnded()
	if got, ok := applyLabelUpdate(d, d.IPInventory, "svc-agent", stream, pgDown, false); ok || got != nil {
		t.Fatalf("update after teardown = %v, %v; want refused", got, ok)
	}
	if err := d.refreshAgentInventoryOnInboundActivity(t.Context(), "svc-agent", d.IPInventory, stream); err != nil {
		t.Fatal(err)
	}
	if l := recordLabels(t.Context(), d.IPInventory, "svc-agent"); l != nil {
		t.Fatalf("record %v written after teardown", l)
	}
	if _, err := d.IPInventory.GetIP(t.Context(), "svc-agent"); err == nil {
		t.Fatal("a record was written after teardown")
	}
}
