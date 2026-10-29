// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spf13/viper"
)

// Sessions are ordered by their place in the agent's session order (Seq,
// from the shared store), not by the replicas' clocks: the newer session on
// a replica whose clock runs 5 minutes behind still wins, and an older
// session on a replica whose clock runs ahead yields. A record or session
// without a Seq (written by an older replica during a rolling
// upgrade) is ordered by the clocks, as those replicas order it.
func TestPutSessionRenewOrdersBySeqNotClock(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	t0 := time.Date(2026, 10, 10, 1, 0, 0, 0, time.UTC)
	skew := 5 * time.Minute
	key := GetDispatcherCacheKey("agent-1")

	for _, tc := range []struct {
		name   string
		mine   Session
		record *Value
		want   error
	}{
		{"newer session on a replica whose clock is behind", Session{Since: t0, Seq: 1, Credential: "o"},
			&Value{PodIP: "10.0.0.2", Since: t0.Add(-skew).UnixNano(), Seq: 2, Credential: "o"}, ErrSuperseded},
		{"older session on a replica whose clock is ahead", Session{Since: t0, Seq: 2, Credential: "o"},
			&Value{PodIP: "10.0.0.2", Since: t0.Add(skew).UnixNano(), Seq: 1, Credential: "o"}, nil},
		{"mixed versions: a newer record without seq", Session{Since: t0, Seq: 7, Credential: "o"},
			&Value{PodIP: "10.0.0.2", Since: t0.Add(time.Minute).UnixNano(), Credential: "o"}, ErrSuperseded},
		{"mixed versions: an older record without seq", Session{Since: t0, Seq: 7, Credential: "o"},
			&Value{PodIP: "10.0.0.2", Since: t0.Add(-time.Minute).UnixNano(), Credential: "o"}, nil},
		{"a session without seq orders by its clock", Session{Since: t0, Credential: "o"},
			&Value{PodIP: "10.0.0.2", Since: t0.Add(-time.Minute).UnixNano(), Seq: 9, Credential: "o"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc := newMockCache()
			mc.store[key] = tc.record
			inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)
			err := inv.(SessionStore).PutSession(context.Background(), "agent-1", tc.mine, true, nil, nil, time.Minute)
			if !errors.Is(err, tc.want) {
				t.Fatalf("PutSession = %v, want %v", err, tc.want)
			}
			got := mc.store[key]
			if tc.want != nil && got != tc.record {
				t.Fatalf("the older session overwrote the newer one's record: %+v", got)
			}
			if tc.want == nil && (got.PodIP != "10.0.0.1" || got.Seq != tc.mine.Seq) {
				t.Fatalf("record = %+v, want this pod's session", got)
			}
		})
	}
}

// A renewal by an agent's old session (its replica still holds it after the
// agent redialled elsewhere) leaves a newer session's record on another pod
// alone and reports ErrSuperseded; it overwrites an older session's record,
// a record an older relay wrote and its own.
func TestPutSessionRenewYieldsToNewerSessionOnAnotherPod(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	t0 := time.Date(2026, 10, 10, 1, 0, 0, 0, time.UTC)
	mine := Session{Since: t0, Credential: "o"}
	key := GetDispatcherCacheKey("agent-1")

	for _, tc := range []struct {
		name   string
		record *Value
		want   error
	}{
		{"newer session on another pod", &Value{PodIP: "10.0.0.2", Since: t0.Add(time.Minute).UnixNano(), Credential: "o"}, ErrSuperseded},
		{"older session on another pod", &Value{PodIP: "10.0.0.2", Since: t0.Add(-time.Minute).UnixNano(), Credential: "o"}, nil},
		{"record of an older relay", &Value{PodIP: "10.0.0.2"}, nil},
		{"own record", &Value{PodIP: "10.0.0.1", Since: t0.UnixNano(), Credential: "o"}, nil},
		{"no record (expired, Redis restarted)", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc := newMockCache()
			if tc.record != nil {
				mc.store[key] = tc.record
			}
			inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)
			err := inv.(SessionStore).PutSession(context.Background(), "agent-1", mine, true, nil, nil, time.Minute)
			if !errors.Is(err, tc.want) {
				t.Fatalf("PutSession = %v, want %v", err, tc.want)
			}
			got := mc.store[key]
			if tc.want != nil {
				if got != tc.record {
					t.Fatalf("the old session overwrote the newer one's record: %+v", got)
				}
				return
			}
			if got == nil || got.PodIP != "10.0.0.1" || got.Since != t0.UnixNano() || got.Credential != "o" {
				t.Fatalf("record = %+v, want this pod's session", got)
			}
		})
	}
}

// A claim (a new session) always writes, and GetSession reads it back with
// this pod as the loopback address.
func TestPutSessionClaimAndGetSession(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	mc := newMockCache()
	key := GetDispatcherCacheKey("agent-1")
	t0 := time.Date(2026, 10, 10, 1, 0, 0, 0, time.UTC)
	mc.store[key] = &Value{PodIP: "10.0.0.2", Since: t0.Add(time.Hour).UnixNano(), Credential: "o"}
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)
	store := inv.(SessionStore)
	if err := store.PutSession(context.Background(), "agent-1", Session{Since: t0, Credential: "o"}, false, nil, nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	pod, s, err := store.GetSession(context.Background(), "agent-1")
	if err != nil || pod != "127.0.0.1" || !s.Since.Equal(t0) || s.Credential != "o" {
		t.Fatalf("GetSession = %q %+v %v", pod, s, err)
	}
	delete(mc.store, key)
	if _, _, err := store.GetSession(context.Background(), "agent-1"); !errors.Is(err, ErrAgentNotConnected) {
		t.Fatalf("GetSession without a record = %v", err)
	}
}
