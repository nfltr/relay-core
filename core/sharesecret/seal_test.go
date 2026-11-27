// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sharesecret

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/argon2"
)

// sealWith builds a sealed value with explicit parameters and key length.
func sealWith(secret string, mem, passes uint32, threads uint8, keyLen uint32) string {
	salt := []byte("0123456789abcdef")
	key := argon2.IDKey([]byte(secret), salt, passes, mem, threads, keyLen)
	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s", mem, passes, threads,
		b64.EncodeToString(salt), b64.EncodeToString(key))
}

func BenchmarkSeal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Seal("hunter2")
	}
}

func BenchmarkVerify(b *testing.B) {
	sealed := Seal("hunter2")
	for i := 0; i < b.N; i++ {
		SecretMatches(sealed, "hunter2")
	}
}

func TestSecretMatchesParameterBoundaries(t *testing.T) {
	if !SecretMatches(sealWith("s", maxSealMemKiB, maxSealTime, maxSealThreads, 32), "s") {
		t.Fatal("maximum in-bounds parameters (m=65536,t=10,p=8) must verify")
	}
	if SecretMatches(sealWith("s", maxSealMemKiB+1, 1, 1, 32), "s") {
		t.Fatal("m=65537 must be rejected")
	}
	if SecretMatches(sealWith("s", 8, maxSealTime+1, 1, 32), "s") {
		t.Fatal("t=11 must be rejected")
	}
	if SecretMatches(sealWith("s", 8, 1, maxSealThreads+1, 32), "s") {
		t.Fatal("p=9 must be rejected")
	}
}

func TestSecretMatchesKeyLengths(t *testing.T) {
	for _, n := range []uint32{16, 32, 64} {
		if !SecretMatches(sealWith("s", 8, 1, 1, n), "s") {
			t.Errorf("key length %d must verify", n)
		}
	}
	for _, n := range []uint32{4, 15, 65, 128} {
		if SecretMatches(sealWith("s", 8, 1, 1, n), "s") {
			t.Errorf("key length %d must be rejected", n)
		}
	}
}

func TestSecretMatchesTrailingJunkRejected(t *testing.T) {
	good := Seal("s")
	for name, v := range map[string]string{
		"v=19x":      strings.Replace(good, "v=19$", "v=19x$", 1),
		"p=1x":       strings.Replace(good, "p=1$", "p=1x$", 1),
		"m junk":     strings.Replace(good, "m=8192,", "m=8192x,", 1),
		"t junk":     strings.Replace(good, "t=2,", "t=2x,", 1),
		"plus sign":  strings.Replace(good, "m=8192,", "m=+8192,", 1),
		"extra":      strings.Replace(good, "p=1$", "p=1,q=2$", 1),
		"p overflow": strings.Replace(good, "p=1$", "p=257$", 1),
	} {
		if v == good {
			t.Fatalf("%s: test did not alter the value", name)
		}
		if SecretMatches(v, "s") {
			t.Errorf("%s: verified: %q", name, v)
		}
	}
}

func TestSecretMatchesEmptyValues(t *testing.T) {
	if SecretMatches(Seal("s"), "") {
		t.Fatal("empty presented value matched a sealed secret")
	}
	if !SecretMatches("", "") || SecretMatches("", "x") {
		t.Fatal("a policy value with no secret must match only an empty presented value")
	}
}

// Concurrent evaluations are bounded: with every slot held, a check fails
// closed after hashWait instead of allocating another argon2 lane.
func TestSecretMatchesFailsClosedWhenSaturated(t *testing.T) {
	sealed := Seal("s")
	for range maxConcurrentHashes {
		hashSlots <- struct{}{}
	}
	start := time.Now()
	got := SecretMatches(sealed, "s")
	for range maxConcurrentHashes {
		<-hashSlots
	}
	if got {
		t.Fatal("verification ran with every slot held")
	}
	if time.Since(start) < hashWait/2 {
		t.Fatal("saturated check did not wait for a slot")
	}
	if !SecretMatches(sealed, "s") {
		t.Fatal("verification must work again once slots free")
	}
}

func TestSecretMatchesConcurrentUse(t *testing.T) {
	sealed := Seal("s")
	var wg sync.WaitGroup
	for range 4 * maxConcurrentHashes {
		wg.Go(func() {
			if len(hashSlots) > maxConcurrentHashes {
				t.Error("slot bound exceeded")
			}
			SecretMatches(sealed, "wrong")
		})
	}
	wg.Wait()
}

func TestSealRoundTrip(t *testing.T) {
	sealed := Seal("hunter2")
	if !strings.HasPrefix(sealed, "$argon2id$v=19$m=8192,t=2,p=1$") {
		t.Fatalf("sealed = %q, want self-describing argon2id parameters", sealed)
	}
	if strings.Contains(sealed, "hunter2") {
		t.Fatal("sealed value contains the plaintext")
	}
	if !SecretMatches(sealed, "hunter2") {
		t.Fatal("the sealed secret does not verify")
	}
}

func TestSealWrongSecretRejected(t *testing.T) {
	sealed := Seal("hunter2")
	for _, bad := range []string{"", "hunter", "hunter22", "HUNTER2"} {
		if SecretMatches(sealed, bad) {
			t.Fatalf("SecretMatches accepted %q", bad)
		}
	}
}

func TestSealSaltDiffersPerSeal(t *testing.T) {
	a, b := Seal("same"), Seal("same")
	if a == b {
		t.Fatal("two seals of one secret are identical: salt is not random")
	}
	if !SecretMatches(a, "same") || !SecretMatches(b, "same") {
		t.Fatal("both seals must verify")
	}
}

func TestSecretMatchesOldFormatFailsClosed(t *testing.T) {
	// The former "sha256:<salt hex>:<digest hex>" seal of "hunter2".
	old := "sha256:00112233445566778899aabbccddeeff:" + strings.Repeat("ab", 32)
	if SecretMatches(old, "hunter2") {
		t.Fatal("old sha256 format must not verify")
	}
	if SecretMatches("hunter2", "hunter2") {
		t.Fatal("an unsealed value must not verify")
	}
}

func TestSecretMatchesMalformedFailsClosed(t *testing.T) {
	good := Seal("s")
	parts := strings.Split(good, "$") // "", argon2id, v, params, salt, key
	with := func(i int, v string) string {
		p := append([]string(nil), parts...)
		p[i] = v
		return strings.Join(p, "$")
	}
	for name, v := range map[string]string{
		"empty":            "",
		"prefix only":      "$argon2id$",
		"missing key":      strings.Join(parts[:5], "$"),
		"extra field":      good + "$x",
		"bad version":      with(2, "v=18"),
		"garbled version":  with(2, "v=x"),
		"bad params":       with(3, "m=x,t=2,p=1"),
		"missing params":   with(3, "m=8192"),
		"zero memory":      with(3, "m=0,t=2,p=1"),
		"zero time":        with(3, "m=8192,t=0,p=1"),
		"zero lanes":       with(3, "m=8192,t=2,p=0"),
		"memory too large": with(3, "m=4194304,t=2,p=1"),
		"time too large":   with(3, "m=8192,t=999,p=1"),
		"lanes too large":  with(3, "m=8192,t=2,p=200"),
		"bad salt base64":  with(4, "!!!"),
		"empty salt":       with(4, ""),
		"bad key base64":   with(5, "!!!"),
		"short key":        with(5, "AAAA"),
	} {
		if SecretMatches(v, "s") {
			t.Errorf("%s: malformed value verified: %q", name, v)
		}
	}
}
