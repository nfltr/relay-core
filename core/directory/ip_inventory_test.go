// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"log/slog"

	"github.com/spf13/viper"
)

// ---------------------------------------------------------------------------
// mock cache
// ---------------------------------------------------------------------------

type mockCache struct {
	store    map[string]*Value
	sets     map[string]map[string]bool // Redis SET emulation
	counters map[string]int64
	getErr   error
	setErr   error
	delErr   error
	ops      []string
}

func newMockCache() *mockCache {
	return &mockCache{
		store: make(map[string]*Value),
		sets:  make(map[string]map[string]bool),
	}
}

func (m *mockCache) Ping(ctx context.Context) (string, error) { return "PONG", nil }

func (m *mockCache) Get(ctx context.Context, key string) (*Value, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	v, ok := m.store[key]
	if !ok {
		return nil, nil
	}
	return v, nil
}

func (m *mockCache) Set(ctx context.Context, key string, value any, exp time.Duration) (string, error) {
	if m.setErr != nil {
		return "", m.setErr
	}
	if v, ok := value.(Value); ok {
		m.store[key] = &v
	}
	return "OK", nil
}

func (m *mockCache) Del(ctx context.Context, keys ...string) (int64, error) {
	m.ops = append(m.ops, "del")
	if m.delErr != nil {
		return 0, m.delErr
	}
	var n int64
	for _, k := range keys {
		if _, ok := m.store[k]; ok {
			delete(m.store, k)
			n++
		}
	}
	return n, nil
}

func (m *mockCache) Incr(ctx context.Context, key string, by int64, ttl time.Duration) (int64, error) {
	if m.setErr != nil {
		return 0, m.setErr
	}
	if m.counters == nil {
		m.counters = make(map[string]int64)
	}
	m.counters[key] += by
	return m.counters[key], nil
}

func (m *mockCache) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	if m.sets[key] == nil {
		m.sets[key] = make(map[string]bool)
	}
	var n int64
	for _, mem := range members {
		if !m.sets[key][mem] {
			m.sets[key][mem] = true
			n++
		}
	}
	return n, nil
}

func (m *mockCache) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	m.ops = append(m.ops, "srem")
	if m.sets[key] == nil {
		return 0, nil
	}
	var n int64
	for _, mem := range members {
		if m.sets[key][mem] {
			delete(m.sets[key], mem)
			n++
		}
	}
	return n, nil
}

func (m *mockCache) SInter(ctx context.Context, keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	// Start with the first set and intersect with subsequent sets.
	first := m.sets[keys[0]]
	if first == nil {
		return nil, nil
	}
	result := make(map[string]bool)
	for k := range first {
		result[k] = true
	}
	for _, key := range keys[1:] {
		s := m.sets[key]
		for k := range result {
			if !s[k] {
				delete(result, k)
			}
		}
	}
	out := make([]string, 0, len(result))
	for k := range result {
		out = append(out, k)
	}
	return out, nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

func TestNewIPInventory_Success(t *testing.T) {
	inv, err := NewIPInventory(testLogger(), newMockCache(), time.Second, "127.0.0.1", GetDispatcherCacheKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inv == nil {
		t.Fatal("expected non-nil inventory")
	}
}

func TestNewIPInventory_NilCache(t *testing.T) {
	_, err := NewIPInventory(testLogger(), nil, time.Second, "127.0.0.1", GetDispatcherCacheKey)
	if err == nil {
		t.Fatal("expected error for nil cache")
	}
}

func TestNewIPInventory_NilLogger(t *testing.T) {
	_, err := NewIPInventory(nil, newMockCache(), time.Second, "127.0.0.1", GetDispatcherCacheKey)
	if err == nil {
		t.Fatal("expected error for nil logger")
	}
}

// ---------------------------------------------------------------------------
// noOpInventory tests
// ---------------------------------------------------------------------------

func TestNoOpInventory(t *testing.T) {
	inv := NewNoOpInventory()
	ctx := context.Background()

	ip, err := inv.GetIP(ctx, "agent-1")
	if err != nil || ip != "" {
		t.Fatalf("expected empty string and nil error, got %q, %v", ip, err)
	}

	if err := inv.SetIP(ctx, "agent-1", time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := inv.DeleteIPs(ctx, "agent-1", "agent-2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ipInventory.GetIP tests
// ---------------------------------------------------------------------------

func TestGetIP_CacheError_RedisNil(t *testing.T) {
	mc := newMockCache()
	mc.getErr = ErrNotFound

	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)

	_, err := inv.GetIP(context.Background(), "agent-1")
	if err == nil {
		t.Fatal("expected error for redis nil")
	}
	if !errors.Is(err, ErrAgentNotConnected) {
		t.Fatalf("expected ErrAgentNotConnected, got %v", err)
	}
}

func TestGetIP_CacheError_KeyNotFound(t *testing.T) {
	mc := newMockCache()
	mc.getErr = fmt.Errorf("key not found")

	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)

	_, err := inv.GetIP(context.Background(), "agent-1")
	if err == nil {
		t.Fatal("expected error for key not found")
	}
	if errors.Is(err, ErrAgentNotConnected) {
		t.Fatalf("generic cache errors must not be classified as not connected: %v", err)
	}
}

func TestGetIP_CacheError_Other(t *testing.T) {
	mc := newMockCache()
	mc.getErr = fmt.Errorf("connection refused")

	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)

	_, err := inv.GetIP(context.Background(), "agent-1")
	if err == nil {
		t.Fatal("expected error")
	}
	// The relay's own failure, typed, its cause kept.
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrAgentNotConnected) || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("expected ErrUnavailable wrapping 'connection refused', got %v", err)
	}
}

func TestGetIP_NilValue(t *testing.T) {
	mc := newMockCache()
	// No entry in store — Get returns nil, nil

	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)

	_, err := inv.GetIP(context.Background(), "agent-1")
	if err == nil {
		t.Fatal("expected error for nil value")
	}
	if !errors.Is(err, ErrAgentNotConnected) {
		t.Fatalf("expected ErrAgentNotConnected, got %v", err)
	}
}

func TestGetIP_EmptyPodIP(t *testing.T) {
	mc := newMockCache()
	key := GetDispatcherCacheKey("agent-1")
	mc.store[key] = &Value{PodIP: ""}

	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)

	_, err := inv.GetIP(context.Background(), "agent-1")
	if err == nil {
		t.Fatal("expected error for empty PodIP")
	}
}

// ---------------------------------------------------------------------------
// ipInventory.GetIP tests
// ---------------------------------------------------------------------------

func TestGetIP_Delegates(t *testing.T) {
	mc := newMockCache()
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Setenv("POD_IP", "10.0.0.1")

	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)
	key := GetDispatcherCacheKey("agent-1")
	mc.store[key] = &Value{PodIP: "10.0.0.1"}

	ip, err := inv.GetIP(context.Background(), "agent-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ip != "127.0.0.1" {
		t.Fatalf("expected '127.0.0.1', got %q", ip)
	}
}

// ---------------------------------------------------------------------------
// ipInventory.SetIP tests
// ---------------------------------------------------------------------------

func TestSetIP_Success(t *testing.T) {
	mc := newMockCache()
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)

	err := inv.SetIP(context.Background(), "agent-1", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	key := GetDispatcherCacheKey("agent-1")
	if _, ok := mc.store[key]; !ok {
		t.Fatal("expected value to be stored in cache")
	}
	if !mc.sets["inventory:active:dipi:agentID:"]["agent-1"] {
		t.Fatal("expected agent to be indexed in active inventory set")
	}
}

func TestSetIPWithAgentInfo_StoresMetadata(t *testing.T) {
	mc := newMockCache()
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetStreamerCacheKey)

	labels := map[string]string{"flavor": "codex", "actor": "implementer"}
	caps := []string{"a2a", "worker"}
	if err := inv.SetIPWithAgentInfo(context.Background(), "agent-1", labels, caps, time.Minute); err != nil {
		t.Fatalf("SetIPWithAgentInfo: %v", err)
	}
	labels["flavor"] = "mutated"
	caps[0] = "mutated"

	value := mc.store[GetStreamerCacheKey("agent-1")]
	if value == nil {
		t.Fatal("expected value to be stored")
	}
	if value.Labels["flavor"] != "codex" || value.Labels["actor"] != "implementer" {
		t.Fatalf("labels not stored/cloned correctly: %#v", value.Labels)
	}
	if len(value.Capabilities) != 2 || value.Capabilities[0] != "a2a" || value.Capabilities[1] != "worker" {
		t.Fatalf("capabilities not stored/cloned correctly: %#v", value.Capabilities)
	}
}

func TestSetIP_CacheError(t *testing.T) {
	mc := newMockCache()
	mc.setErr = fmt.Errorf("set fail")
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)

	err := inv.SetIP(context.Background(), "agent-1", time.Minute)
	if err == nil {
		t.Fatal("expected error from cache Set")
	}
}

// ---------------------------------------------------------------------------
// ipInventory.DeleteIPs tests
// ---------------------------------------------------------------------------

func TestDeleteIPs_Success(t *testing.T) {
	mc := newMockCache()
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Setenv("POD_IP", "10.0.0.1")
	key := GetDispatcherCacheKey("agent-1")
	mc.store[key] = &Value{PodIP: "10.0.0.1"}
	mc.sets["inventory:active:dipi:agentID:"] = map[string]bool{"agent-1": true}

	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)
	err := inv.DeleteIPs(context.Background(), "agent-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := mc.store[key]; ok {
		t.Fatal("expected key to be deleted")
	}
	if mc.sets["inventory:active:dipi:agentID:"]["agent-1"] {
		t.Fatal("expected agent to be removed from active inventory set")
	}
}

// A pod deleting an agent that has meanwhile registered on another pod (a
// rolling restart) keeps the other pod's record.
func TestDeleteIPsKeepsAnotherPodsRecord(t *testing.T) {
	mc := newMockCache()
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Setenv("POD_IP", "10.0.0.1")
	key := GetDispatcherCacheKey("agent-1")
	mc.store[key] = &Value{PodIP: "10.0.0.2"}
	mc.sets["inventory:active:dipi:agentID:"] = map[string]bool{"agent-1": true}

	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)
	if err := inv.DeleteIPs(context.Background(), "agent-1"); err != nil {
		t.Fatal(err)
	}
	if mc.store[key] == nil || !mc.sets["inventory:active:dipi:agentID:"]["agent-1"] {
		t.Fatal("the old pod deleted the record the agent's new pod wrote")
	}
}

func TestListAgentIDs_ReturnsActiveInventorySet(t *testing.T) {
	mc := newMockCache()
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Setenv("POD_IP", "10.0.0.1")
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetStreamerCacheKey)
	if err := inv.SetIP(context.Background(), "agent-1", time.Minute); err != nil {
		t.Fatalf("SetIP agent-1: %v", err)
	}
	if err := inv.SetIP(context.Background(), "agent-2", time.Minute); err != nil {
		t.Fatalf("SetIP agent-2: %v", err)
	}

	got, err := inv.ListAgentIDs(context.Background())
	if err != nil {
		t.Fatalf("ListAgentIDs: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 active agents, got %d: %v", len(got), got)
	}
	want := map[string]bool{"agent-1": true, "agent-2": true}
	for _, id := range got {
		if !want[id] {
			t.Fatalf("unexpected active agent %q in %v", id, got)
		}
	}
}

func TestListAgentRecords_ReturnsLiveMetadataAndPrunesStaleSetMembers(t *testing.T) {
	mc := newMockCache()
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Setenv("POD_IP", "10.0.0.1")
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetStreamerCacheKey)
	if err := inv.SetIPWithAgentInfo(context.Background(), "agent-1",
		map[string]string{"flavor": "codex"},
		[]string{"worker", "a2a"},
		time.Minute,
	); err != nil {
		t.Fatalf("SetIPWithAgentInfo: %v", err)
	}
	if _, err := mc.SAdd(context.Background(), "inventory:active:sipi:agentID:", "stale-agent"); err != nil {
		t.Fatalf("SAdd stale: %v", err)
	}

	got, err := inv.ListAgentRecords(context.Background())
	if err != nil {
		t.Fatalf("ListAgentRecords: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 live record, got %d: %#v", len(got), got)
	}
	if got[0].AgentID != "agent-1" {
		t.Fatalf("AgentID = %q, want agent-1", got[0].AgentID)
	}
	if got[0].Labels["flavor"] != "codex" {
		t.Fatalf("Labels = %#v, want flavor=codex", got[0].Labels)
	}
	if len(got[0].Capabilities) != 2 || got[0].Capabilities[0] != "worker" || got[0].Capabilities[1] != "a2a" {
		t.Fatalf("Capabilities = %#v, want worker,a2a", got[0].Capabilities)
	}
	if mc.sets["inventory:active:sipi:agentID:"]["stale-agent"] {
		t.Fatal("expected stale active set member to be pruned")
	}
}

func TestDeleteIPs_RemovesActiveSetBeforeAddress(t *testing.T) {
	mc := newMockCache()
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetStreamerCacheKey)
	if err := inv.SetIPWithAgentInfo(context.Background(), "agent-1", nil, []string{"worker"}, time.Minute); err != nil {
		t.Fatalf("SetIPWithAgentInfo: %v", err)
	}
	mc.ops = nil

	if err := inv.DeleteIPs(context.Background(), "agent-1"); err != nil {
		t.Fatalf("DeleteIPs: %v", err)
	}
	if len(mc.ops) < 2 || mc.ops[0] != "srem" || mc.ops[1] != "del" {
		t.Fatalf("operation order = %v, want srem before del", mc.ops)
	}
	if mc.sets["inventory:active:sipi:agentID:"]["agent-1"] {
		t.Fatal("expected active set member removed")
	}
	if _, ok := mc.store["sipi:agentID:agent-1"]; ok {
		t.Fatal("expected address cache entry removed")
	}
}

func TestDeleteIPs_CacheError(t *testing.T) {
	mc := newMockCache()
	mc.delErr = fmt.Errorf("del fail")
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)

	err := inv.DeleteIPs(context.Background(), "agent-1")
	if err == nil {
		t.Fatal("expected error from cache Del")
	}
}

// ---------------------------------------------------------------------------
// Fleet label index tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Helper function tests
// ---------------------------------------------------------------------------

func TestGetDispatcherCacheKey(t *testing.T) {
	got := GetDispatcherCacheKey("abc")
	want := "dipi:agentID:abc"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestGetStreamerCacheKey(t *testing.T) {
	got := GetStreamerCacheKey("abc")
	want := "sipi:agentID:abc"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestGetPodIP(t *testing.T) {
	t.Setenv("MY_POD_IP", "192.168.1.1")
	t.Setenv("POD_IP", "192.168.1.1")

	// GetPodIP reads os.Getenv(viper.GetString("pod_ip.env_var"))
	// We just verify it returns a string without panicking
	_ = GetPodIP()
}
