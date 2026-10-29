// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sharesecret

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// saturateHashes holds every hash slot so any cold verify fails closed;
// a Matches that still succeeds answered from the cache, without argon2.
func saturateHashes(t *testing.T) {
	t.Helper()
	for range maxConcurrentHashes {
		hashSlots <- struct{}{}
	}
	t.Cleanup(func() {
		for range maxConcurrentHashes {
			<-hashSlots
		}
	})
}

func TestVerifiedCacheSkipsArgon2ForARepeatedCredential(t *testing.T) {
	c := NewVerifiedCache(16, time.Minute)
	sealed := Seal("user:pw")
	if !c.Matches("swift-bay", sealed, "user:pw", "") {
		t.Fatal("cold verify of the right credential failed")
	}
	if c.Matches("swift-bay", sealed, "user:wrong", "") {
		t.Fatal("wrong credential matched")
	}
	if strings.Contains(fmt.Sprint(c.entries), "pw") {
		t.Fatalf("cache holds plaintext: %v", c.entries)
	}
	saturateHashes(t)
	if !c.Matches("swift-bay", sealed, "user:pw", "") {
		t.Fatal("repeated credential was hashed again (failed closed under a saturated limit)")
	}
	for name, miss := range map[string][3]string{
		"other code":     {"bright-lake", sealed, "user:pw"},
		"reissued share": {"swift-bay", Seal("user:pw"), "user:pw"}, // fresh salt
		"wrong password": {"swift-bay", sealed, "user:wrong"},       // never cached
	} {
		if c.Matches(miss[0], miss[1], miss[2], "") {
			t.Fatalf("%s: answered from the cache", name)
		}
	}
}

func TestVerifiedCacheForgetTTLAndShareExpiry(t *testing.T) {
	c := NewVerifiedCache(16, time.Minute)
	now := time.Now()
	c.now = func() time.Time { return now }
	sealed := Seal("tok")
	soon := now.Add(10 * time.Second).Format(time.RFC3339)
	for _, code := range []string{"a-revoked", "b-ttl", "c-expiring"} {
		expires := ""
		if code == "c-expiring" {
			expires = soon
		}
		if !c.Matches(code, sealed, "tok", expires) {
			t.Fatal("cold verify failed")
		}
	}
	c.Forget("a-revoked")
	saturateHashes(t)
	if c.Matches("a-revoked", sealed, "tok", "") {
		t.Fatal("a revoked share's credential was answered from the cache")
	}
	now = now.Add(30 * time.Second) // past the share's ExpiresAt, within the TTL
	if c.Matches("c-expiring", sealed, "tok", soon) {
		t.Fatal("an entry outlived its share's expiry")
	}
	if !c.Matches("b-ttl", sealed, "tok", "") {
		t.Fatal("entry within its TTL missed")
	}
	now = now.Add(time.Minute)
	if c.Matches("b-ttl", sealed, "tok", "") {
		t.Fatal("an entry outlived its TTL")
	}
}

func TestVerifiedCacheIsBounded(t *testing.T) {
	c := NewVerifiedCache(3, time.Minute)
	sealed := Seal("s")
	for i := range 10 {
		if !c.Matches(fmt.Sprint("code-", i), sealed, "s", "") {
			t.Fatal("cold verify failed")
		}
		if len(c.entries) > 3 {
			t.Fatalf("cache holds %d entries, max 3", len(c.entries))
		}
	}
}

// A browser's burst of requests with one credential shares one evaluation
// instead of overrunning the relay-wide limit into spurious 401s.
func TestVerifiedCacheConcurrentColdChecksShareOneHash(t *testing.T) {
	c := NewVerifiedCache(16, time.Minute)
	sealed := Seal("user:pw")
	var wg sync.WaitGroup
	var failed atomic.Int64
	for range 64 {
		wg.Go(func() {
			if !c.Matches("swift-bay", sealed, "user:pw", "") {
				failed.Add(1)
			}
		})
	}
	wg.Wait()
	if n := failed.Load(); n != 0 {
		t.Fatalf("%d of 64 concurrent requests with the right credential failed", n)
	}
}

// Before (SecretMatches per request) and after (VerifiedCache) for a valid
// credential, run in parallel as a relay serves a share: ns/op gives
// requests/s, failed/op the fail-closed 401s under the hash limit.
func BenchmarkShareValidCredential(b *testing.B) {
	sealed := Seal("user:pw")
	for _, bench := range []struct {
		name  string
		check func() bool
	}{
		{"before_argon2_per_request", func() bool { return SecretMatches(sealed, "user:pw") }},
		{"after_verified_cache", func() bool {
			return verifiedBench.Matches("swift-bay", sealed, "user:pw", "")
		}},
	} {
		b.Run(bench.name, func(b *testing.B) {
			var failed atomic.Int64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if !bench.check() {
						failed.Add(1)
					}
				}
			})
			b.ReportMetric(float64(failed.Load())/float64(b.N), "failed/op")
		})
	}
}

var verifiedBench = NewVerifiedCache(4096, time.Hour)
