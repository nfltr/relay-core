// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/spf13/viper"
)

const presenceTestPodIPEnv = "NFLTR_TEST_PRESENCE_POD_IP"

// newPresenceTestDispatcher builds a multi-pod dispatcher (non-nil pod dialer)
// whose streamer inventory is the shared cluster inventory inv.
// inventoryOwner is the Owner of a test dispatcher whose StreamerIPInventory
// stands for the cluster's.
type inventoryOwner struct {
	inv  directory.IPInventory
	self string
}

func (o inventoryOwner) OwnerOf(ctx context.Context, agentID string) (string, bool, error) {
	replica, err := o.inv.GetIP(ctx, agentID)
	return replica, replica == o.self, err
}

// withOwner makes d a multi-replica dispatcher routing by its StreamerIPInventory.
func withOwner(d *Dispatcher) *Dispatcher {
	d.Owner = inventoryOwner{inv: d.StreamerIPInventory, self: d.Config.LoopbackAddr}
	return d
}

func newPresenceTestDispatcher(t *testing.T, inv directory.IPInventory) *Dispatcher {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b := broker.NewInMemoryMessageBroker()
	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), inv, time.Second,
		DispatcherConfig{LoopbackAddr: "127.0.0.1", SyncMapClearTimeout: time.Second})
	d.PodDialer = &failingPodDialer{}
	return withOwner(d)
}

// failingCache is the shared directory a second replica reads, its clock
// moved by the test and its reads failed on demand.
type failingCache struct {
	*directory.MemoryCache
	now time.Time
	err error
}

func newFailingCache() *failingCache {
	c := &failingCache{now: time.Now()}
	c.MemoryCache = directory.NewMemoryCacheClock(func() time.Time { return c.now })
	return c
}

func (c *failingCache) Get(ctx context.Context, key string) (*directory.Value, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.MemoryCache.Get(ctx, key)
}

// TestDispatcherWorkerPresentConsultsSharedInventory proves two relay pods
// sharing one inventory agree on a worker attached to pod B: pod A reports it
// present, reports it absent once B's inventory record expires, and reports
// "unknown" (never absent) when the inventory itself fails.
func TestDispatcherWorkerPresentConsultsSharedInventory(t *testing.T) {
	shared := newFailingCache()
	inv, err := directory.NewIPInventory(fake.TestLogger(), shared, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	if err != nil {
		t.Fatalf("NewIPInventory: %v", err)
	}
	prevEnvVar := viper.GetString("pod_ip.env_var")
	viper.Set("pod_ip.env_var", presenceTestPodIPEnv)
	t.Cleanup(func() { viper.Set("pod_ip.env_var", prevEnvVar) })

	podA := newPresenceTestDispatcher(t, inv)
	podB := newPresenceTestDispatcher(t, inv)
	const workerID = "worker-on-b"

	// Pod B accepts the worker: local stream plus inventory record at B's IP.
	t.Setenv(presenceTestPodIPEnv, "10.0.0.2")
	podB.Agents.Store(workerID, &HTTPStream{})
	if err := inv.SetIP(context.Background(), workerID, 5*time.Second); err != nil {
		t.Fatalf("SetIP: %v", err)
	}
	t.Setenv(presenceTestPodIPEnv, "10.0.0.1") // now reading as pod A

	assertPresence := func(d *Dispatcher, wantPresent, wantKnown bool) {
		t.Helper()
		present, known := d.WorkerPresent(workerID)
		if present != wantPresent || known != wantKnown {
			t.Fatalf("WorkerPresent = (%v, %v), want (%v, %v)", present, known, wantPresent, wantKnown)
		}
	}
	assertPresence(podB, true, true)
	assertPresence(podA, true, true)

	shared.err = errors.New("inventory unavailable")
	assertPresence(podA, false, false)
	shared.err = nil

	shared.now = shared.now.Add(6 * time.Second)
	assertPresence(podA, false, true)
}

// TestDispatcherWorkerPresentOwnPodRecordDefersToLocalMaps proves a stale
// inventory record pointing at this pod never overrides the pod's own
// authoritative connection maps.
func TestDispatcherWorkerPresentOwnPodRecordDefersToLocalMaps(t *testing.T) {
	shared := newFailingCache()
	inv, err := directory.NewIPInventory(fake.TestLogger(), shared, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	if err != nil {
		t.Fatalf("NewIPInventory: %v", err)
	}
	prevEnvVar := viper.GetString("pod_ip.env_var")
	viper.Set("pod_ip.env_var", presenceTestPodIPEnv)
	t.Cleanup(func() { viper.Set("pod_ip.env_var", prevEnvVar) })
	t.Setenv(presenceTestPodIPEnv, "10.0.0.1")

	podA := newPresenceTestDispatcher(t, inv)
	if err := inv.SetIP(context.Background(), "stale-worker", time.Minute); err != nil {
		t.Fatalf("SetIP: %v", err)
	}
	if present, known := podA.WorkerPresent("stale-worker"); present || !known {
		t.Fatalf("WorkerPresent = (%v, %v), want (false, true)", present, known)
	}
}

// TestDispatcherWorkerPresentSinglePodKeepsLocalAuthority proves a relay with
// no pod dialer (single pod, no-op inventory) answers exactly as before:
// local maps decide, and an absent worker is a known absence.
func TestDispatcherWorkerPresentSinglePodKeepsLocalAuthority(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	if present, known := d.WorkerPresent("missing"); present || !known {
		t.Fatalf("WorkerPresent(missing) = (%v, %v), want (false, true)", present, known)
	}
	d.Agents.Store("local", &HTTPStream{})
	if present, known := d.WorkerPresent("local"); !present || !known {
		t.Fatalf("WorkerPresent(local) = (%v, %v), want (true, true)", present, known)
	}
}
