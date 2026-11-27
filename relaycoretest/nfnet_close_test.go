// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package relaycoretest_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/nfltr/relay-core/relaycoretest"
	"github.com/nfltr/relay/client/nfnet"
)

// TestRelayedCloseThenExitDeliversTheTail (lane RELAYCLOSE, link
// spec/RelayedClose.tla): an accepted relayed Conn writes, closes and its
// program exits (its connections to the relay end) before the dialler
// read anything; the dialler then reads every byte and a clean end. A
// nil Close is the program's licence to exit. Before, the dialler read
// every byte, then a reset (the relay passed the agent link's end on), or
// a cut tail.
func TestRelayedCloseThenExitDeliversTheTail(t *testing.T) {
	var p *switchProxy
	s := newSDK(t, func(r *relaycoretest.Relay) string { p = newSwitchProxy(t, r.GRPC); return p.ln.Addr().String() })
	l := s.listen(t, nfnet.ListenOptions{})
	c, ac := s.pair(t, l)
	want := bytes.Repeat([]byte("0123456789abcdef"), 1<<16) // 1 MiB: within the windows
	if _, err := ac.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := ac.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	p.Switch("")                                            // the program exits: its connections end
	_ = c.SetReadDeadline(time.Now().Add(30 * time.Second)) // a bound against a hang only
	got, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("dialler read %d of %d bytes, then %v: want all, then EOF", len(got), len(want), err)
	}
}

// TestRelayedDialCloseThenExitDeliversTheTail: the same for the dialler:
// it writes, closes and its program exits (its connections to the relay
// end) before the accepted Conn read anything.
func TestRelayedDialCloseThenExitDeliversTheTail(t *testing.T) {
	s := newSDK(t, nil)
	p := newSwitchProxy(t, s.r.Proxy)
	s.cfg.Relay = p.ln.Addr().String()
	d, err := nfnet.NewDialer(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	s.d = d
	l := s.listen(t, nfnet.ListenOptions{})
	c, ac := s.pair(t, l)
	want := bytes.Repeat([]byte("0123456789abcdef"), 1<<16) // 1 MiB: within the windows
	if _, err := c.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	p.Switch("")                                             // the program exits: its connections end
	_ = ac.SetReadDeadline(time.Now().Add(30 * time.Second)) // a bound against a hang only
	got, err := io.ReadAll(ac)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("accepted Conn read %d of %d bytes, then %v: want all, then EOF", len(got), len(want), err)
	}
}
