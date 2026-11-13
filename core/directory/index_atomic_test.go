// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory_test

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/directory/directorytest"
	"github.com/spf13/viper"
)

// TestIndexRacesMemory: the index sets stay consistent with the records
// under concurrent claims, deletes and prunes on the one-replica store
// (relay-server runs the same suite on Redis).
func TestIndexRacesMemory(t *testing.T) {
	directorytest.RunIndexRaces(t, func(*testing.T) directory.Cache { return directory.NewMemoryCache() })
}

// hookCache runs hook once, just before the first store call that writes
// a key or set named in on, so another writer lands between a caller's
// read and its write (nfltr docs/decisions/nf1-index-atomic-2026-11-14.md).
type hookCache struct {
	directory.Cache
	on   func(key string) bool
	once sync.Once
	hook func()
}

func (h *hookCache) at(key string) {
	if h.on(key) {
		h.once.Do(h.hook)
	}
}

func (h *hookCache) Del(ctx context.Context, keys ...string) (int64, error) {
	h.at(keys[0])
	return h.Cache.Del(ctx, keys...)
}

func (h *hookCache) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	h.at(key)
	return h.Cache.SRem(ctx, key, members...)
}

func (h *hookCache) DelIf(ctx context.Context, key string, match func(*directory.Value) bool, member string, sets ...string) (bool, error) {
	h.at(key)
	for _, s := range sets {
		h.at(s)
	}
	return h.Cache.DelIf(ctx, key, match, member, sets...)
}

// TestIndexStepsAreAtomic: a claim landing between a delete's or a prune's
// read and its write is kept, with its index entry (Directory.tla
// DeleteOwn and IndexComplete; mutants delete-get-del, prune-get-srem).
func TestIndexStepsAreAtomic(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	ctx := context.Background()
	rid := directorytest.RID(1)
	const agent = "agent-1"
	key := directory.GetDispatcherCacheKey(agent)
	t0 := time.Unix(1_800_000_000, 0)
	setup := func(t *testing.T) (*directory.MemoryCache, *hookCache, directory.IPInventory, directory.IPInventory) {
		mc := directory.NewMemoryCache()
		h := &hookCache{Cache: mc}
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		hooked, _ := directory.NewIPInventory(logger, h, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
		plain, _ := directory.NewIPInventory(logger, mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
		return mc, h, hooked, plain
	}
	claim := func(t *testing.T, inv directory.IPInventory, since time.Time) directory.Session {
		s, err := inv.(directory.SessionStore).ClaimSession(ctx, agent, directory.Session{Since: since, Services: directorytest.Decl(rid)}, nil, nil, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	listed := func(t *testing.T, inv directory.IPInventory) []string {
		got, err := inv.(directory.NF1Index).NF1Agents(ctx, rid)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	t.Run("delete", func(t *testing.T) {
		_, h, hooked, plain := setup(t)
		old := claim(t, plain, t0)
		var next directory.Session
		h.on = func(string) bool { return true }
		h.hook = func() { next = claim(t, plain, t0.Add(time.Second)) }
		if err := hooked.(interface {
			DeleteSession(context.Context, string, directory.Session) error
		}).DeleteSession(ctx, agent, old); err != nil {
			t.Fatal(err)
		}
		if _, s, err := plain.(directory.SessionStore).GetSession(ctx, agent); err != nil || s.Seq != next.Seq {
			t.Fatalf("the next session's record was deleted: %+v, %v", s, err)
		}
		if got := listed(t, plain); !slices.Equal(got, []string{agent}) {
			t.Fatalf("listed %v, want %s", got, agent)
		}
	})

	t.Run("prune", func(t *testing.T) {
		mc, h, hooked, plain := setup(t)
		claim(t, plain, t0)
		if _, err := mc.Del(ctx, key); err != nil { // the record lapsed
			t.Fatal(err)
		}
		h.on = func(k string) bool { return k != key }
		h.hook = func() { claim(t, plain, t0.Add(time.Second)) }
		if got := listed(t, hooked); len(got) != 0 {
			t.Fatalf("listed %v before the claim", got)
		}
		if got := listed(t, plain); !slices.Equal(got, []string{agent}) {
			t.Fatalf("listed %v with the record live: the prune removed it", got)
		}
	})
}
