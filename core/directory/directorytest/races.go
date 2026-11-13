// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package directorytest is a conformance suite for directory.Cache
// implementations: the index sets beside the records (the nf1 rendezvous
// index, the service and active sets) stay consistent with the records
// under concurrent claims, deletes and read-side prunes (nfltr
// docs/decisions/nf1-index-atomic-2026-11-14.md; relay-core
// spec/Directory.tla IndexComplete, DeleteOwn). Run it with -race.
package directorytest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
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

// RID is round r's rendezvous id.
func RID(r int) [16]byte { return [16]byte{0xab, byte(r >> 8), byte(r), 1} }

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
