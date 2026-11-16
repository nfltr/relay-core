// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package relaycoretest_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nfltr/relay-core/relaycoretest"
	"github.com/nfltr/relay/client/nf1"
	"github.com/nfltr/relay/client/nfnet"
)

// TestNF1AnonSDKEndToEnd (nfltr docs/decisions/nf1-anon-2026-11-16.md
// sections 1 and 2): the relay SDK's anonymous listener (nfnet.Listen with
// no credential) registers at a relay core, which echoes its declaration,
// and a dialer with no credential opens its address.
//
//   - Direct off: the dialer refuses the address's a1 answer itself,
//     CodeDirectRequired, and the listener accepts nothing.
//   - Direct on: the control exchange (C, relayed) and the punch move the
//     session to a direct path (host candidates on loopback), then 1 MiB
//     goes both ways, far over the relayed leg's budget and the source's
//     bucket: had a byte of it been relayed, the relay had cut the session.
func TestNF1AnonSDKEndToEnd(t *testing.T) {
	const budget = 32 << 10
	enableNF1Anon(t, anonSettings{legBytes: budget, bucket: 2 * budget})
	r := relaycoretest.New(t)
	nl, _, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	l, err := nfnet.Listen(ctx, nfnet.Config{Relay: r.Proxy, AgentRelay: r.GRPC, Agent: "send", Plaintext: true}, "send",
		nfnet.ListenOptions{Address: nl, Direct: nfnet.DirectOn})
	if err != nil {
		t.Fatalf("anonymous Listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	d, err := nfnet.NewDialer(nfnet.Config{Plaintext: true, DialRelay: func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", r.Proxy)
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close() //nolint:errcheck
	dial := func(mode nfnet.DirectMode) (*nfnet.Conn, error) {
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
			dctx, dcancel := context.WithTimeout(t.Context(), time.Minute)
			c, err := d.Dial(dctx, l.Address(), nfnet.DialOptions{Direct: mode})
			dcancel()
			if nfnet.CodeOf(err) != nfnet.CodeNotFound || time.Now().After(deadline) {
				return c, err
			}
		}
	}

	_, err = dial(nfnet.DirectOff)
	wantCode(t, "an anonymous address dialled with Direct off", err, nfnet.CodeDirectRequired)

	payload := bytes.Repeat([]byte("nf1anon!"), 1<<17) // 1 MiB
	served := make(chan error, 1)
	go func() {
		ac, err := l.Accept() // the first session the listener sees: the direct one
		if err != nil {
			served <- err
			return
		}
		defer ac.Close() //nolint:errcheck
		_ = ac.SetDeadline(time.Now().Add(time.Minute))
		got, err := io.ReadAll(ac)
		if err == nil && !bytes.Equal(got, payload) {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			_, err = ac.Write(got)
		}
		served <- err
	}()
	c, err := dial(nfnet.DirectOn)
	if err != nil {
		t.Fatalf("Direct on: %v", err)
	}
	defer c.Close() //nolint:errcheck
	_ = c.SetDeadline(time.Now().Add(time.Minute))
	go func() { _, _ = c.Write(payload); _ = c.CloseWrite() }()
	back, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(back, payload) {
		t.Fatalf("1 MiB back: %d bytes, %v", len(back), err)
	}
	if err := <-served; err != nil {
		t.Fatalf("the anonymous listener's session: %v", err)
	}
}
