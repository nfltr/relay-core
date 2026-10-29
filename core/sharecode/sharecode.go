// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package sharecode is the shape of a share code and of the subject it is
// issued for: what the share issuer (internal/application/sharestore, the
// accounts service) writes and every grant verifier (core/frontend
// CachedGrants, the relay) reads. It links nothing but the standard
// library, so the core-only relay verifies shares without the stores.
package sharecode

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"strings"
	"time"
)

// A share code is 128 random bits (audit F-13) as lowercase RFC 4648 base32
// without padding: 26 characters of a-z and 2-7, a valid DNS label and URL
// path segment. It holds no words and no hyphen, so it can never equal a
// reserved name or a browse label ("b-<hash>").
const codeLen = 26

var encoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// legacyHexLen is the hex tail of codes issued before 2026-10-08,
// "<word>-<word>-<32 hex>", still accepted for links already handed out.
const legacyHexLen = 32

// New returns a fresh 26-character base32 code from crypto/rand.
func New() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("sharecode: cannot read random bytes: " + err.Error())
	}
	return strings.ToLower(encoding.EncodeToString(b))
}

// Valid reports whether code has the shape of a share code: the current
// 26-character base32 form, or the earlier "<word>-<word>-<32 hex>" form.
// Everything else, including codes from before audit F-13 (a bare word pair,
// or one with 24 random bits), is refused. It is the one parser of share
// codes; callers screening host names before a store lookup use it too.
func Valid(code string) bool {
	if len(code) == codeLen {
		// Decode, then re-encode: only the canonical lowercase spelling
		// (zero trailing bits) of 16 bytes round-trips.
		b, err := encoding.DecodeString(strings.ToUpper(code))
		return err == nil && len(b) == 16 && strings.ToLower(encoding.EncodeToString(b)) == code
	}
	words := strings.Split(code, "-")
	if len(words) != 3 || len(words[2]) != legacyHexLen || len(code) > 63 {
		return false
	}
	return lowerLetters(words[0]) && lowerLetters(words[1]) &&
		strings.Trim(words[2], "0123456789abcdef") == ""
}

func lowerLetters(s string) bool {
	return s != "" && strings.Trim(s, "abcdefghijklmnopqrstuvwxyz") == ""
}

// SubjectSep separates agent and service in a share's subject.
const SubjectSep = "/"

// SplitSubject is a share subject's agent and service ("" for the whole
// agent).
func SplitSubject(subject string) (agentID, service string) {
	agentID, service, _ = strings.Cut(subject, SubjectSep)
	return agentID, service
}

// GrantPath is the issuer's read of one share code: GET with ?code=, sent
// by the relay only (RelayHeader: "1", which the relay strips from every
// client's request); 200 with a Grant, 404 for no share.
const GrantPath = "/internal/v1/shares/grant"

// RelayHeader marks a request the relay itself sent a service agent (its
// share writes, grant reads and usage facts); the relay strips every
// client's copy, so a browser cannot set it.
const RelayHeader = "X-Nfltr-Relay-Facts"

// Grant is the issuer's answer at GrantPath: the subject and the share's
// policy (secrets sealed), as the issuer stores it.
type Grant struct {
	Subject string          `json:"subject"`
	Policy  json.RawMessage `json:"policy"`
}

// FactsPath is the issuer's: a relay reports there (POST, RelayHeader "1")
// what it saw that the issuer records, among them the share codes it
// granted (UsedReport).
const FactsPath = "/internal/v1/facts"

// JournalPath is a relay's: the issuer POSTs it (with its agent key) after
// it revoked a share code or a credential, and when it starts, so the relay
// drops what it cached.
const JournalPath = "/internal/v1/accounts/journal"

// Used is a share code granted to a visitor: the issuer keeps it reserved
// for its agent until Until.
type Used struct {
	Code  string    `json:"code"`
	Until time.Time `json:"until"`
}

// UsedReport is a relay's report of the share codes it granted, at
// FactsPath (one field of the issuer's facts).
type UsedReport struct {
	SharesUsed []Used `json:"shares_used,omitempty"`
}
