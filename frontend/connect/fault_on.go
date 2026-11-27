// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relayfault

package connect

// The relay fault hook, for test labs only (nfltr's direct-path NAT lab,
// tests/conformance/protocols/direct; docs/decisions/direct-paths-2026-11-11.md
// in nfltr): a shipped relay is built without the relayfault tag and has
// none (fault_off.go).
//
// RELAY_FAULT_WITHHOLD_FILE names a file. While it exists and holds a
// service name, a session to that service whose client's first byte opens
// a TLS record (a direct-path control exchange, service nfltr-direct) is
// reset at that byte, as a relay withholding the exchange would; one that
// starts otherwise (a path-layer OPEN: a relayed re-attach) passes. The
// file is read at each open, so a lab turns the fault on and off without
// a restart, and it outlives one in the relay's data directory.

import (
	"errors"
	"os"
	"strings"

	"github.com/nfltr/link/peerlink"
)

// tlsHandshake is a TLS record's first byte (content type handshake).
const tlsHandshake = 0x16

var errWithheld = errors.New("relayfault: session withheld")

func faultSession(service string, s peerlink.Session) peerlink.Session {
	path := os.Getenv("RELAY_FAULT_WITHHOLD_FILE")
	if path == "" {
		return s
	}
	b, err := os.ReadFile(path) // #nosec G304 -- the lab's own file, relayfault builds only
	if err != nil || strings.TrimSpace(string(b)) != service {
		return s
	}
	if cs, ok := s.(peerlink.ChainSession); ok {
		return &withheldChain{ChainSession: cs}
	}
	return &withheld{Session: s}
}

// firstByte resets via reset and fails when p is the session's first
// write and opens a TLS record. One writer per session (the join's
// direction toward the agent), so seen needs no lock.
func firstByte(seen *bool, p []byte, reset func() error) error {
	if *seen || len(p) == 0 {
		return nil
	}
	*seen = true
	if p[0] == tlsHandshake {
		_ = reset()
		return errWithheld
	}
	return nil
}

type withheld struct {
	peerlink.Session
	seen bool
}

func (w *withheld) Write(p []byte) (int, error) {
	if err := firstByte(&w.seen, p, w.Reset); err != nil {
		return 0, err
	}
	return w.Session.Write(p)
}

type withheldChain struct {
	peerlink.ChainSession
	seen bool
}

func (w *withheldChain) Write(p []byte) (int, error) {
	if err := firstByte(&w.seen, p, w.Reset); err != nil {
		return 0, err
	}
	return w.ChainSession.Write(p)
}
