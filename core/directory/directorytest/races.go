// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package directorytest is a conformance suite for directory.Cache
// implementations: the index sets beside the records (the nf1 rendezvous
// index, the service and active sets) stay consistent with the records
// under concurrent claims, deletes and read-side prunes, and the record
// names the newest session under concurrent claims and renewals (nfltr
// docs/decisions/nf1-index-atomic-2026-11-14.md; relay-core
// spec/Directory.tla IndexComplete, DeleteOwn). Run it with -race.
package directorytest

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/spf13/viper"
)

// Rounds is how many times each race runs.
const Rounds = 200

type deleter interface {
	DeleteSession(ctx context.Context, agentID string, s directory.Session) error
}

// RunIndexRaces runs the races against a fresh store from newCache each.
// It sets the pod address through viper and the environment, so its
// caller must not run in parallel.
func RunIndexRaces(t *testing.T, newCache func(t *testing.T) directory.Cache) {
	viper.Set("pod_ip.env_var", "DIRECTORYTEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("DIRECTORYTEST_POD_IP", "10.0.0.1")
	ctx := context.Background()
	open := func(t *testing.T) (directory.Cache, directory.IPInventory) {
		c := newCache(t)
		inv, err := directory.NewIPInventory(slog.New(slog.NewTextHandler(io.Discard, nil)), c, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
		if err != nil {
			t.Fatal(err)
		}
		return c, inv
	}
	claim := func(t *testing.T, inv directory.IPInventory, agent string, rid [16]byte, since time.Time) directory.Session {
		s, err := inv.(directory.SessionStore).ClaimSession(ctx, agent, directory.Session{Since: since, Services: Decl(rid)}, nil, nil, time.Minute)
		if err != nil {
			t.Error(err)
		}
		return s
	}
	listed := func(t *testing.T, inv directory.IPInventory, rid [16]byte) []string {
		got, err := inv.(directory.NF1Index).NF1Agents(ctx, rid)
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		return got
	}
	t0 := time.Unix(1_800_000_000, 0)

	// I2 with two registrants: concurrent adds of one rid by two agents,
	// then one's delete racing the other's re-claim.
	t.Run("two agents register one rid", func(t *testing.T) {
		_, inv := open(t)
		for r := range Rounds {
			rid := RID(r)
			a, b := fmt.Sprintf("a%d", r), fmt.Sprintf("b%d", r)
			var sa directory.Session
			race(func() { sa = claim(t, inv, a, rid, t0) }, func() { claim(t, inv, b, rid, t0) })
			if got := listed(t, inv, rid); !slices.Equal(got, []string{a, b}) {
				t.Fatalf("round %d: listed %v, want %s and %s", r, got, a, b)
			}
			race(func() {
				if err := inv.(deleter).DeleteSession(ctx, a, sa); err != nil {
					t.Error(err)
				}
			}, func() { claim(t, inv, b, rid, t0.Add(time.Second)) })
			if got := listed(t, inv, rid); !slices.Equal(got, []string{b}) {
				t.Fatalf("round %d: after %s's delete: listed %v, want %s", r, a, got, b)
			}
		}
	})

	// I3 and I2 on a takeover: the old session's delete racing the new
	// session's claim never deletes the new record nor its index entry.
	t.Run("delete races the next claim", func(t *testing.T) {
		_, inv := open(t)
		for r := range Rounds {
			rid, agent := RID(r), fmt.Sprintf("x%d", r)
			old := claim(t, inv, agent, rid, t0)
			var next directory.Session
			race(func() {
				if err := inv.(deleter).DeleteSession(ctx, agent, old); err != nil {
					t.Error(err)
				}
			}, func() { next = claim(t, inv, agent, rid, t0.Add(time.Second)) })
			_, s, err := inv.(directory.SessionStore).GetSession(ctx, agent)
			if err != nil || s.Seq != next.Seq {
				t.Fatalf("round %d: record %+v, %v; want the next session (Seq %d)", r, s, err, next.Seq)
			}
			if got := listed(t, inv, rid); !slices.Equal(got, []string{agent}) {
				t.Fatalf("round %d: listed %v, want %s", r, got, agent)
			}
		}
	})

	// The active set on a list's prune of a podless record: a claim
	// landing between the list's read and its removal keeps its entry
	// (ListAgentRecords; Directory.tla IndexComplete).
	t.Run("podless prune races a claim", func(t *testing.T) {
		c, inv := open(t)
		for r := range Rounds {
			rid, agent := RID(r), fmt.Sprintf("v%d", r)
			claim(t, inv, agent, rid, t0)
			podless(t, c, agent)
			race(func() { listedAgent(t, inv, agent) }, func() { claim(t, inv, agent, rid, t0.Add(time.Second)) })
			if !listedAgent(t, inv, agent) {
				t.Fatalf("round %d: %s not listed with its record live", r, agent)
			}
		}
	})

	// The record names the newest session after its renewal and its next
	// claim race (relay-server spec/README.md finding 3; spec/Directory.tla
	// NoResurrect, mutant renew-get-set): the renewal's check and write are
	// one store step.
	t.Run("renewal races the next claim", func(t *testing.T) {
		_, inv := open(t)
		for r := range Rounds {
			rid, agent := RID(r), fmt.Sprintf("z%d", r)
			old := claim(t, inv, agent, rid, t0)
			var next directory.Session
			race(func() {
				if err := inv.(directory.SessionStore).PutSession(ctx, agent, old, true, nil, nil, time.Minute); err != nil {
					t.Error(err)
				}
			}, func() { next = claim(t, inv, agent, rid, t0.Add(time.Second)) })
			named(t, r, inv, agent, next)
		}
	})

	// Two claims of one agent racing leave the newer one's record
	// (relay-server spec/README.md finding 2): a claim never writes over a
	// newer session's record.
	t.Run("two claims race", func(t *testing.T) {
		_, inv := open(t)
		for r := range Rounds {
			rid, agent := RID(r), fmt.Sprintf("w%d", r)
			var a, b directory.Session
			race(func() { a = claim(t, inv, agent, rid, t0) }, func() { b = claim(t, inv, agent, rid, t0.Add(time.Second)) })
			want := a
			if b.Newer(a) {
				want = b
			}
			named(t, r, inv, agent, want)
		}
	})

	// I2 on a prune: a read pruning a member whose record lapsed, racing
	// that agent's new claim, never removes the live record's entry.
	t.Run("prune races a claim", func(t *testing.T) {
		c, inv := open(t)
		for r := range Rounds {
			rid, agent := RID(r), fmt.Sprintf("y%d", r)
			claim(t, inv, agent, rid, t0)
			if _, err := c.Del(ctx, directory.GetDispatcherCacheKey(agent)); err != nil { // the record lapsed
				t.Fatal(err)
			}
			race(func() { _, _ = inv.(directory.NF1Index).NF1Agents(ctx, rid) }, func() { claim(t, inv, agent, rid, t0.Add(time.Second)) })
			if got := listed(t, inv, rid); !slices.Equal(got, []string{agent}) {
				t.Fatalf("round %d: listed %v with %s's record live", r, got, agent)
			}
		}
	})
}

// podless writes a record without a pod for agent (as no relay of this
// version writes one), its agent still in the active set.
func podless(t *testing.T, c directory.Cache, agent string) {
	t.Helper()
	if _, err := c.Set(context.Background(), directory.GetDispatcherCacheKey(agent), directory.Value{}, time.Minute); err != nil {
		t.Fatal(err)
	}
}

// named fails unless the agent's record names session want.
func named(t *testing.T, round int, inv directory.IPInventory, agent string, want directory.Session) {
	t.Helper()
	_, s, err := inv.(directory.SessionStore).GetSession(context.Background(), agent)
	if err != nil || s.Seq != want.Seq || !s.Since.Equal(want.Since) {
		t.Fatalf("round %d: the record names %+v (%v); want the newer session (Seq %d)", round, s, err, want.Seq)
	}
}

// listedAgent reports whether ListAgentRecords lists agent.
func listedAgent(t *testing.T, inv directory.IPInventory, agent string) bool {
	t.Helper()
	recs, err := inv.(interface {
		ListAgentRecords(context.Context) ([]directory.AgentRecord, error)
	}).ListAgentRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return slices.ContainsFunc(recs, func(r directory.AgentRecord) bool { return r.AgentID == agent })
}

// RID is round r's rendezvous id.
func RID(r int) [16]byte {
	h := sha256.Sum256([]byte(strconv.Itoa(r)))
	return [16]byte(h[:16])
}

// Decl is a service declaration under rid.
func Decl(rid [16]byte) string { return fmt.Sprintf("clip=tcp+e2ee;nf1=%x", rid) }

// race runs f and g at once.
func race(f, g func()) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, fn := range []func(){f, g} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			fn()
		}()
	}
	close(start)
	wg.Wait()
}
