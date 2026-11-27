// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package sharesecret seals a share's credentials (Basic Auth, Bearer
// token, required header values) and checks a presented one against its
// sealed value (SecretMatches, VerifiedCache). It links no store, so the
// core's share path (core/frontend) can check credentials without
// database/sql.
package sharesecret

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/argon2"
)

// A policy secret is kept as a PHC-style argon2id string,
// "$argon2id$v=19$m=<KiB>,t=<passes>,p=<lanes>$<salt>$<key>" (unpadded
// standard base64), with a fresh random salt per secret. The parameters travel
// in the value, so verification is self-describing. Stores hold only sealed
// secrets, so a copy of the database yields no share password, Bearer
// token or required header value, and each offline guess costs a memory-hard
// argon2id evaluation.
//
// Parameters: 8 MiB, 2 passes, 1 lane, 16-byte salt, 32-byte key. Verification
// runs on every proxied share request (about 10 ms and 8 MiB of transient
// memory per credential), so these sit below the OWASP baseline
// (19 MiB, t=2) on purpose; shares last at most 24 h. No older format is
// accepted: a value in any other shape (including the former "sha256:" form)
// never matches.
const (
	sealedPrefix = "$argon2id$"
	sealMemKiB   = 8 * 1024
	sealTime     = 2
	sealThreads  = 1
	sealSaltLen  = 16
	sealKeyLen   = 32

	// Bounds on parameters read back from a stored value, so a corrupt or
	// hostile row cannot make verification allocate without limit.
	maxSealMemKiB  = 64 * 1024
	maxSealTime    = 10
	maxSealThreads = 8
)

var b64 = base64.RawStdEncoding

// hashSlots bounds concurrent argon2 evaluations so a flood of credential
// checks holds at most maxConcurrentHashes*8 MiB of transient memory. A check
// that cannot get a slot within hashWait fails closed.
const (
	maxConcurrentHashes = 8
	hashWait            = 250 * time.Millisecond
)

var hashSlots = make(chan struct{}, maxConcurrentHashes)

// Seal is secret sealed: a PHC-style argon2id string with a fresh salt.
func Seal(secret string) string {
	salt := make([]byte, sealSaltLen)
	if _, err := rand.Read(salt); err != nil {
		panic("sharesecret: cannot read random bytes: " + err.Error())
	}
	key := argon2.IDKey([]byte(secret), salt, sealTime, sealMemKiB, sealThreads, sealKeyLen)
	return fmt.Sprintf("%sv=%d$m=%d,t=%d,p=%d$%s$%s", sealedPrefix, argon2.Version,
		sealMemKiB, sealTime, sealThreads, b64.EncodeToString(salt), b64.EncodeToString(key))
}

// SecretMatches reports, in constant time for a given sealed value, whether
// presented is the secret sealed into sealed. A value that is not sealed, or
// is malformed, or carries out-of-bounds parameters never matches: a policy
// that skipped Sealed fails closed. An empty sealed value means "no secret"
// and matches only an empty presented value; an empty presented value never
// matches a sealed secret and is rejected without hashing. Concurrent
// evaluations are bounded; when saturated the check fails closed.
func SecretMatches(sealed, presented string) bool {
	if sealed == "" {
		return presented == ""
	}
	if presented == "" {
		return false
	}
	rest, ok := strings.CutPrefix(sealed, sealedPrefix)
	if !ok {
		return false
	}
	parts := strings.Split(rest, "$")
	if len(parts) != 4 {
		return false
	}
	version, ok := parseUint(parts[0], "v=", 32)
	if !ok || version != argon2.Version {
		return false
	}
	mem, passes, threads, ok := parseParams(parts[1])
	if !ok {
		return false
	}
	if mem == 0 || mem > maxSealMemKiB || passes == 0 || passes > maxSealTime || threads == 0 || threads > maxSealThreads {
		return false
	}
	salt, err1 := b64.DecodeString(parts[2])
	want, err2 := b64.DecodeString(parts[3])
	if err1 != nil || err2 != nil || len(salt) == 0 || len(want) < 16 || len(want) > 64 {
		return false
	}
	timer := time.NewTimer(hashWait)
	defer timer.Stop()
	select {
	case hashSlots <- struct{}{}:
		defer func() { <-hashSlots }()
	case <-timer.C:
		return false
	}
	got := argon2.IDKey([]byte(presented), salt, uint32(passes), uint32(mem), uint8(threads), uint32(len(want))) // #nosec G115 -- each bounded above (maxSeal*, parseUint bit sizes, len(want) <= 64)
	return subtle.ConstantTimeCompare(got, want) == 1
}

// parseUint parses prefix followed by plain decimal digits and nothing else.
func parseUint(s, prefix string, bits int) (uint64, bool) {
	num, ok := strings.CutPrefix(s, prefix)
	if !ok || num == "" || num[0] < '0' || num[0] > '9' {
		return 0, false
	}
	n, err := strconv.ParseUint(num, 10, bits)
	return n, err == nil
}

// parseParams strictly parses "m=<n>,t=<n>,p=<n>".
func parseParams(s string) (mem, passes, threads uint64, ok bool) {
	fields := strings.Split(s, ",")
	if len(fields) != 3 {
		return 0, 0, 0, false
	}
	var ok1, ok2, ok3 bool
	mem, ok1 = parseUint(fields[0], "m=", 32)
	passes, ok2 = parseUint(fields[1], "t=", 32)
	threads, ok3 = parseUint(fields[2], "p=", 8)
	return mem, passes, threads, ok1 && ok2 && ok3
}
