// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/spf13/viper"
)

func TestIPInventory_SetAndGetIP(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	defer viper.Reset()

	mc := newMockCache()
	inv, err := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	err = inv.SetIP(ctx, "agentID-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	ip, err := inv.GetIP(ctx, "agentID-1")
	if err != nil {
		t.Fatal(err)
	}
	// PodIP 10.0.0.1 should be replaced with loopback 127.0.0.1
	if ip != "127.0.0.1" {
		t.Fatalf("expected 127.0.0.1, got %s", ip)
	}
}

func TestIPInventory_GetIP_NotFound(t *testing.T) {
	mc := newMockCache()
	inv, err := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	if err != nil {
		t.Fatal(err)
	}

	_, err = inv.GetIP(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent agentID")
	}
}

func TestIPInventory_GetIP_CacheError(t *testing.T) {
	mc := newMockCache()
	mc.Err = errors.New("redis: nil")
	inv, err := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	if err != nil {
		t.Fatal(err)
	}

	_, err = inv.GetIP(context.Background(), "agentID-1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestIPInventory_GetIP_GenericCacheError(t *testing.T) {
	mc := newMockCache()
	mc.Err = errors.New("connection refused")
	inv, err := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	if err != nil {
		t.Fatal(err)
	}

	_, err = inv.GetIP(context.Background(), "agentID-1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestIPInventory_GetIP_EmptyPodIP(t *testing.T) {
	mc := newMockCache()
	mc.Data["dipi:agentID:agentID-1"] = &directory.Value{PodIP: "", Env: "env"}
	inv, err := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	if err != nil {
		t.Fatal(err)
	}

	_, err = inv.GetIP(context.Background(), "agentID-1")
	if err == nil {
		t.Fatal("expected error for empty PodIP")
	}
}

func TestIPInventory_DeleteIPs(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Setenv("POD_IP", "10.0.0.1") // this pod: it deletes only its own records
	mc := newMockCache()
	mc.Data["dipi:agentID:o1"] = &directory.Value{PodIP: "10.0.0.1"}
	mc.Data["dipi:agentID:o2"] = &directory.Value{PodIP: "10.0.0.1"}
	inv, err := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	if err != nil {
		t.Fatal(err)
	}

	err = inv.DeleteIPs(context.Background(), "o1", "o2")
	if err != nil {
		t.Fatal(err)
	}
	if len(mc.Data) != 0 {
		t.Fatalf("expected empty cache, got %d entries", len(mc.Data))
	}
}

func newMockCache() *fake.MockCache {
	return &fake.MockCache{Data: make(map[string]*directory.Value)}
}
