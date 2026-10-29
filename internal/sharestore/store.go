// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package sharestore is an in-memory share issuer for this module's tests:
// the hosted relay's share store without its SQL half.
package sharestore

import (
	"errors"
	"sync"
	"time"

	"github.com/nfltr/relay-core/core/sharecode"
)

// Policy defines optional access control rules for a share URL. Its
// fields are core/frontend.SharePolicy's, the grant verifier's contract
// (the relay converts one to the other; a drift does not compile).
// An empty Policy means the URL is publicly accessible (the default).
// Multiple restrictions are ANDed: a request must satisfy all that are set.
type Policy struct {
	// BasicAuth, when non-empty, requires visitors to supply HTTP Basic Auth
	// credentials in the form "user:password". Browser-compatible: shows a
	// native login dialog.
	BasicAuth string `json:"basic_auth,omitempty"`
	// AllowedIPs, when non-empty, restricts access to the listed CIDR ranges
	// or exact IP addresses (e.g. "203.0.113.0/24" or "198.51.100.5").
	// An empty list means all IPs are allowed.
	AllowedIPs []string `json:"allowed_ips,omitempty"`
	// BearerToken, when non-empty, requires visitors to present a Bearer token
	// in the Authorization header: "Authorization: Bearer <token>".
	// API/curl access only — browsers do not provide a Bearer auth UI.
	// Share the token with recipients out-of-band.
	BearerToken string `json:"bearer_token,omitempty"`
	// RequiredHeaders, when non-empty, requires each listed header to be
	// present with its exact value. Useful for webhook signature headers,
	// custom API-key headers, or any arbitrary header-based gate.
	RequiredHeaders map[string]string `json:"required_headers,omitempty"`
	// Preset captures the share workflow chosen at issuance time so the
	// dashboard and CLI can describe the link consistently without exposing
	// underlying credentials.
	Preset string `json:"preset,omitempty"`
	// ExpiresAt mirrors the active server-side expiry in RFC3339 form so
	// catalog and CLI views can show the actual lifetime of a reused share URL.
	ExpiresAt string `json:"expires_at,omitempty"`
}

// Protected reports whether the policy has any access restriction configured.
func (p Policy) Protected() bool {
	return p.BasicAuth != "" || len(p.AllowedIPs) > 0 || p.BearerToken != "" || len(p.RequiredHeaders) > 0
}

// HasCredentialGate reports whether the policy requires explicit credentials
// (Basic Auth, Bearer token, or required headers) rather than only an IP gate.
func (p Policy) HasCredentialGate() bool {
	return p.BasicAuth != "" || p.BearerToken != "" || len(p.RequiredHeaders) > 0
}

// Store issues and validates share codes.
//
// A code is 128 random bits in lowercase base32 (see sharecode.Valid), used as a
// subdomain:
//
//	k3vq7mzt2xw4hn5rbsd6jyfa2e.rpc.example.com
//
// Two implementations: NewInMemory (tests, nfltr-relay without a data
// file) and NewSQL (nfltr-relay's SQLite file or SQL store, the accounts
// service). The hosted relay links neither: it reads the accounts
// service's codes through NewSQLReader.
type Store interface {
	Reader
	// Issue generates a unique code for agentID that expires after ttl.
	// The optional policy controls who may access the URL (Basic Auth, IP list).
	// Issuing for an agentID that already has an active code reuses that code and
	// updates its policy and TTL.
	Issue(agentID string, ttl time.Duration, policy Policy) string
	// Validate returns the agentID and access policy for a valid, non-expired code.
	// Returns ("", Policy{}, false) for unknown or expired codes.
	Validate(code string) (string, Policy, bool)
	// Touch extends the code's reservation window after a successful use. While a
	// reservation is active, the same agent can reissue the same code and the code
	// will not be assigned to any other agent.
	Touch(code string, ttl time.Duration)
	// Revoke removes a code immediately and for good: its reservation goes
	// with it, so Issue never hands a revoked code out again (audit F-40).
	Revoke(code string)
	// RevokeByAgentID revokes every code issued for agentID, and the agent's
	// reservation, and returns the count. The dashboard's share delete.
	RevokeByAgentID(agentID string) int
	// Close releases resources.
	Close()
}

// Reader is a share store's read side.
type Reader interface {
	// Lookup returns the subject and policy of a valid, non-expired code:
	// ErrUnknownCode for an unknown, expired or malformed code, another
	// error when the store could not answer (e.g. its database).
	Lookup(code string) (string, Policy, error)
	// GetCodesByAgentID returns all active (non-expired) share codes for agentID.
	GetCodesByAgentID(agentID string) []string
}

// ErrUnknownCode is Lookup's error for a code that is not (or no longer) a
// share: malformed, never issued, expired or revoked.
var ErrUnknownCode = errors.New("unknown share code")

// ── In-memory implementation ────────────────────────────────────────────────

type shareEntry struct {
	agentID string
	expires time.Time
	policy  Policy
}

type shareReservation struct {
	agentID       string
	reservedUntil time.Time
}

type inMemoryStore struct {
	mu                sync.Mutex
	entries           map[string]shareEntry
	reservations      map[string]shareReservation
	agentReservations map[string]string
	sweepEvery        time.Duration // 0: never
	lastSweep         time.Time
}

// NewInMemory returns a Store backed entirely by in-process memory. It is the
// default for single-process deployments and for tests.
func NewInMemory(cleanupInterval time.Duration) Store {
	s := &inMemoryStore{
		entries:           make(map[string]shareEntry),
		reservations:      make(map[string]shareReservation),
		agentReservations: make(map[string]string),
		sweepEvery:        cleanupInterval,
		lastSweep:         time.Now(),
	}
	return s
}

func (s *inMemoryStore) Issue(agentID string, ttl time.Duration, policy Policy) string {
	now := time.Now()
	expires := now.Add(ttl)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked(now)

	for code, entry := range s.entries {
		if entry.agentID != agentID {
			continue
		}
		if now.Before(entry.expires) && sharecode.Valid(code) {
			s.entries[code] = shareEntry{agentID: agentID, expires: expires, policy: policy}
			return code
		}
		delete(s.entries, code)
	}

	if code, ok := s.reservedCodeForAgentLocked(agentID, now); ok && sharecode.Valid(code) {
		s.entries[code] = shareEntry{agentID: agentID, expires: expires, policy: policy}
		return code
	}

	for {
		code := sharecode.New()
		if s.isCodeActiveLocked(code, now) || s.isCodeReservedByAnotherLocked(code, agentID, now) {
			continue
		}
		s.entries[code] = shareEntry{agentID: agentID, expires: expires, policy: policy}
		return code
	}
}

func (s *inMemoryStore) Validate(code string) (string, Policy, bool) {
	agentID, policy, err := s.Lookup(code)
	return agentID, policy, err == nil
}

func (s *inMemoryStore) Lookup(code string) (string, Policy, error) {
	if !sharecode.Valid(code) {
		return "", Policy{}, ErrUnknownCode
	}
	now := time.Now()
	s.mu.Lock()
	entry, ok := s.entries[code]
	if ok && !now.Before(entry.expires) {
		delete(s.entries, code)
		ok = false
	}
	s.mu.Unlock()
	if !ok {
		return "", Policy{}, ErrUnknownCode
	}
	return entry.agentID, entry.policy, nil
}

func (s *inMemoryStore) Touch(code string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked(now)
	entry, ok := s.entries[code]
	if !ok {
		return
	}
	if !now.Before(entry.expires) {
		delete(s.entries, code)
		return
	}
	if prevCode, ok := s.agentReservations[entry.agentID]; ok && prevCode != code {
		delete(s.reservations, prevCode)
	}
	s.agentReservations[entry.agentID] = code
	s.reservations[code] = shareReservation{agentID: entry.agentID, reservedUntil: now.Add(ttl)}
}

func (s *inMemoryStore) Revoke(code string) {
	s.mu.Lock()
	delete(s.entries, code)
	s.dropReservationLocked(code)
	s.mu.Unlock()
}

func (s *inMemoryStore) RevokeByAgentID(agentID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for code, entry := range s.entries {
		if entry.agentID == agentID {
			delete(s.entries, code)
			s.dropReservationLocked(code)
			n++
		}
	}
	if code, ok := s.agentReservations[agentID]; ok {
		s.dropReservationLocked(code)
	}
	return n
}

// dropReservationLocked forgets code's reservation. Caller holds s.mu.
func (s *inMemoryStore) dropReservationLocked(code string) {
	if r, ok := s.reservations[code]; ok {
		delete(s.reservations, code)
		if s.agentReservations[r.agentID] == code {
			delete(s.agentReservations, r.agentID)
		}
	}
}

func (s *inMemoryStore) GetCodesByAgentID(agentID string) []string {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var codes []string
	for code, entry := range s.entries {
		if !now.Before(entry.expires) {
			delete(s.entries, code)
			continue
		}
		if entry.agentID == agentID {
			codes = append(codes, code)
		}
	}
	return codes
}

func (s *inMemoryStore) Close() {}

// sweepLocked drops expired entries and reservations when sweepEvery has
// passed since the last sweep. Only Issue and Touch add either, so sweeping
// on them bounds the store as a ticker did, and an unused store runs nothing.
func (s *inMemoryStore) sweepLocked(now time.Time) {
	if s.sweepEvery <= 0 || now.Sub(s.lastSweep) < s.sweepEvery {
		return
	}
	s.lastSweep = now
	for code, entry := range s.entries {
		if now.After(entry.expires) {
			delete(s.entries, code)
		}
	}
	for code, reservation := range s.reservations {
		if !now.Before(reservation.reservedUntil) {
			delete(s.reservations, code)
			if s.agentReservations[reservation.agentID] == code {
				delete(s.agentReservations, reservation.agentID)
			}
		}
	}
}

func (s *inMemoryStore) reservedCodeForAgentLocked(agentID string, now time.Time) (string, bool) {
	code, ok := s.agentReservations[agentID]
	if !ok {
		return "", false
	}
	reservation, ok := s.reservations[code]
	if !ok || reservation.agentID != agentID || !now.Before(reservation.reservedUntil) {
		delete(s.agentReservations, agentID)
		if ok {
			delete(s.reservations, code)
		}
		return "", false
	}
	return code, true
}

func (s *inMemoryStore) isCodeActiveLocked(code string, now time.Time) bool {
	entry, ok := s.entries[code]
	if !ok {
		return false
	}
	if now.Before(entry.expires) {
		return true
	}
	delete(s.entries, code)
	return false
}

func (s *inMemoryStore) isCodeReservedByAnotherLocked(code, agentID string, now time.Time) bool {
	reservation, ok := s.reservations[code]
	if !ok {
		return false
	}
	if !now.Before(reservation.reservedUntil) {
		delete(s.reservations, code)
		if s.agentReservations[reservation.agentID] == code {
			delete(s.agentReservations, reservation.agentID)
		}
		return false
	}
	return reservation.agentID != agentID
}
