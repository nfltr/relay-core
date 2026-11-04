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
		// Finding R7 (relay-server spec/README.md, Replicas.tla seq-tie): a
		// store that restarted empty gave a new claim the Seq of a session
		// still live elsewhere; the later start wins, or neither yields.
		{"equal Seqs after a store loss: the later session holds", Session{Since: t0, Seq: 1, Credential: "o"},
			&Value{PodIP: "10.0.0.2", Since: t0.Add(time.Second).UnixNano(), Seq: 1, Credential: "o"}, ErrSuperseded},
		{"equal Seqs after a store loss: the earlier session yields", Session{Since: t0, Seq: 1, Credential: "o"},
			&Value{PodIP: "10.0.0.2", Since: t0.Add(-time.Second).UnixNano(), Seq: 1, Credential: "o"}, nil},
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

// TestRenewalNeverLowersSeqOnThisPod: an older session's renewal on the
// pod whose newer session holds the record writes nothing (relay-server
// spec/README.md finding 4: with the Seq-compared delete, the older one's
// end would then delete the record).
func TestRenewalNeverLowersSeqOnThisPod(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	key := GetStreamerCacheKey("agent-1")
	mc := newMockCache()
	newer := &Value{PodIP: "10.0.0.1", Seq: 5}
	mc.store[key] = newer
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetStreamerCacheKey)
	if err := inv.(SessionStore).PutSession(context.Background(), "agent-1", Session{Since: time.Now(), Seq: 3}, true, nil, nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	if got := mc.store[key]; got != newer {
		t.Fatalf("record = %+v, want the newer session's (Seq 5)", got)
	}
}

// TestDeleteSessionKeepsAnotherSessionsRecord: a session's delete removes
// the record only while it names this pod and that session.
func TestDeleteSessionKeepsAnotherSessionsRecord(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	key := GetStreamerCacheKey("agent-1")
	for _, tc := range []struct {
		name   string
		record *Value
		gone   bool
	}{
		{"a newer session here", &Value{PodIP: "10.0.0.1", Seq: 5}, false},
		{"another pod's", &Value{PodIP: "10.0.0.2", Seq: 3}, false},
		{"this session's", &Value{PodIP: "10.0.0.1", Seq: 3}, true},
		{"a renewal without a Seq here", &Value{PodIP: "10.0.0.1"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc := newMockCache()
			mc.store[key] = tc.record
			inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetStreamerCacheKey)
			if err := inv.(*ipInventory).DeleteSession(context.Background(), "agent-1", Session{Seq: 3}); err != nil {
				t.Fatal(err)
			}
			if _, ok := mc.store[key]; ok == tc.gone {
				t.Fatalf("record kept = %v, want %v", ok, !tc.gone)
			}
		})
	}
}

// Finding 13 (relay-core spec/README.md, Presence.tla renew-resurrects): a
// renewal that finds no record while the agent's Seq counter went past its
// session's (a newer session claimed it, then ended) is superseded instead
// of writing the agent back unannounced; at the counter it renews (its own
// record expired).
func TestRenewalAfterANewerSessionEndedIsSuperseded(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	key := GetStreamerCacheKey("agent-1")
	for _, tc := range []struct {
		seq  int64
		want error
	}{{4, ErrSuperseded}, {5, nil}} {
		mc := newMockCache()
		mc.counters = map[string]int64{seqKey(key): 5}
		inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetStreamerCacheKey)
		err := inv.(SessionStore).PutSession(context.Background(), "agent-1", Session{Since: time.Now(), Seq: tc.seq}, true, nil, nil, time.Minute)
		if !errors.Is(err, tc.want) && err != tc.want {
			t.Fatalf("Seq %d: err %v, want %v", tc.seq, err, tc.want)
		}
		if _, ok := mc.store[key]; ok != (tc.want == nil) {
			t.Fatalf("Seq %d: record written=%v", tc.seq, ok)
		}
	}
}

// Finding 34 (relay-core spec/README.md, Presence.tla shared-counter): an
// agent's dispatcher record (ConnectHTTP, ConnectAgentTunnel) and its
// streamer record (ConnectTCP) count their sessions apart. The streamer
// claim after the dispatcher one is no newer session of the dispatcher
// record: when that record lapsed (a late renewal, a store that lost it),
// its session's renewal writes it back instead of ending the agent.
func TestRenewalAfterTheOtherRecordsClaimWritesItBack(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	ctx := context.Background()
	mc := newMockCache()
	disp, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)
	strm, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetStreamerCacheKey)
	s, err := disp.(SessionStore).ClaimSession(ctx, "agent-1", Session{Since: time.Now()}, nil, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := strm.(SessionStore).ClaimSession(ctx, "agent-1", Session{Since: time.Now()}, nil, nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	delete(mc.store, GetDispatcherCacheKey("agent-1")) // the record lapsed
	if err := disp.(SessionStore).PutSession(ctx, "agent-1", s, true, nil, nil, time.Minute); err != nil {
		t.Fatalf("the live session's renewal: %v, want its record written back", err)
	}
	if _, ok := mc.store[GetDispatcherCacheKey("agent-1")]; !ok {
		t.Fatal("the dispatcher record was not written back")
	}
}

// Directory.tla seq-only (DeleteOwn): a store that restarted empty gave the
// agent's next session on this pod the Seq of a session still ending here;
// that older session's end must not delete the newer one's record, which
// names the same pod and Seq but another start time.
func TestDeleteSessionAfterAStoreLossKeepsTheNewerSessionsRecord(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	key := GetStreamerCacheKey("agent-1")
	t0 := time.Date(2026, 10, 10, 1, 0, 0, 0, time.UTC)
	mc := newMockCache()
	newer := &Value{PodIP: "10.0.0.1", Seq: 1, Since: t0.Add(time.Minute).UnixNano()}
	mc.store[key] = newer
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetStreamerCacheKey)
	if err := inv.(*ipInventory).DeleteSession(context.Background(), "agent-1", Session{Since: t0, Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if mc.store[key] != newer {
		t.Fatal("the older session's end deleted the newer session's record")
	}
}

// Finding 37 (relay-core spec/README.md, Directory.tla ClaimNoSeq): a
// ConnectTCP session whose claim got no Seq (the store unreachable) goes
// on, renewing and ending by its start time. Its end deletes only its own
// record (seqless-by-pod), and its record does not hide the counter from
// an older session's renewal (seqless-masks-counter).
func TestSeqlessSessionDeletesOnlyItsOwnRecord(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	key := GetStreamerCacheKey("agent-1")
	t1, t2 := time.Unix(100, 0), time.Unix(200, 0)
	for _, tc := range []struct {
		name   string
		record *Value
		gone   bool
	}{
		{"a newer session here", &Value{PodIP: "10.0.0.1", Seq: 5, Since: t2.UnixNano()}, false},
		{"another session without a Seq here", &Value{PodIP: "10.0.0.1", Since: t2.UnixNano()}, false},
		{"its own", &Value{PodIP: "10.0.0.1", Since: t1.UnixNano()}, true},
		{"a renewal without a session here", &Value{PodIP: "10.0.0.1"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc := newMockCache()
			mc.store[key] = tc.record
			inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetStreamerCacheKey)
			if err := inv.(*ipInventory).DeleteSession(context.Background(), "agent-1", Session{Since: t1}); err != nil {
				t.Fatal(err)
			}
			if _, ok := mc.store[key]; ok == tc.gone {
				t.Fatalf("record kept = %v, want %v", ok, !tc.gone)
			}
		})
	}
	t.Run("renewal past the counter under a record without a Seq", func(t *testing.T) {
		mc := newMockCache()
		mc.store[key] = &Value{PodIP: "10.0.0.1", Since: t1.UnixNano()}
		mc.counters = map[string]int64{seqKey(key): 5}
		inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetStreamerCacheKey)
		err := inv.(SessionStore).PutSession(context.Background(), "agent-1", Session{Since: t2, Seq: 4}, true, nil, nil, time.Minute)
		if !errors.Is(err, ErrSuperseded) {
			t.Fatalf("err %v, want ErrSuperseded", err)
		}
		if mc.store[key].Seq != 0 {
			t.Fatalf("record rewritten: %+v", *mc.store[key])
		}
	})
}
