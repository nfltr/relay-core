// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// chainPair is one link session with FeatureChain: its opener's end and
// its accepter's.
func chainPair(t *testing.T) (opener, accepter *link.Session) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	accepted := make(chan *link.Session, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		c, _, err := link.Server(context.Background(), nc, link.Config{Auth: link.NoAuth})
		if err != nil {
			return
		}
		m := link.NewMux(c)
		t.Cleanup(func() { _ = m.Close() })
		go func() { _ = m.Serve(func(_ []byte, s *link.Session) { accepted <- s }) }()
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c, err := link.Client(context.Background(), nc, link.Config{Auth: link.NoAuth})
	if err != nil {
		t.Fatal(err)
	}
	m := link.NewMux(c)
	t.Cleanup(func() { _ = m.Close() })
	go func() { _ = m.Serve(nil) }()
	if opener, err = m.Open(nil); err != nil {
		t.Fatal(err)
	}
	select {
	case accepter = <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("no session accepted")
	}
	t.Cleanup(func() { _, _ = opener.Close(), accepter.Close() })
	return opener, accepter
}

// The in-band edges (session_chain.go): a hop translates between a leg
// that answers in band and one that answers link's own way, both ways,
// accepted or refused, and the bytes follow the answer unchanged.
func TestChainInbandEdges(t *testing.T) {
	refused := dataplane.Refusal(relayv1.ErrorReason_ERROR_REASON_TARGET_REFUSED, "connection refused")
	for _, refuse := range []bool{false, true} {
		// An in-band opener (nfltr v2.1.6) through a hop to a far end that
		// answers link's own way.
		in, client := fake.PlainSessionPair(t)
		origin, far := chainPair(t)
		go Forward(slog.New(slog.DiscardHandler), ChainConnect, in, true, func(context.Context) (peerlink.Session, func(), error) {
			return origin, nil, nil
		})
		if refuse {
			_ = far.AbortWith(refused)
		} else {
			_ = far.Accept()
			_, _ = far.Write([]byte("hi"))
			_ = far.CloseWrite()
		}
		_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
		a, err := dataplane.ReadAnswer(client)
		switch {
		case err != nil:
			t.Fatalf("refuse %v: in-band answer: %v", refuse, err)
		case refuse && (a.GetReason() != relayv1.ErrorReason_ERROR_REASON_TARGET_REFUSED || a.GetMessage() != "connection refused"):
			t.Fatalf("in-band refusal %v", a)
		case !refuse && a.GetReason() != 0:
			t.Fatalf("in-band accept %v", a)
		}
		rest, err := io.ReadAll(client)
		if refuse && (len(rest) != 0 || err != nil) || !refuse && string(rest) != "hi" {
			t.Fatalf("refuse %v: after the answer %q, %v", refuse, rest, err)
		}

		// An opener that answers link's own way through a hop to a far end
		// that answers in band (the frame streams, a released agent).
		o, hopIn := chainPair(t)
		out, farConn := fake.PlainSessionPair(t)
		go Forward(slog.New(slog.DiscardHandler), chainReplica, hopIn, false, func(context.Context) (peerlink.Session, func(), error) {
			return answering(out, true), nil, nil
		})
		if refuse {
			_ = dataplane.WriteAnswer(farConn, relayv1.ErrorReason_ERROR_REASON_TARGET_REFUSED, "connection refused")
			_ = farConn.Close()
		} else {
			_, _ = farConn.Write(dataplane.AppendAnswer(nil, 0, ""))
			_, _ = farConn.Write([]byte("hi"))
			_ = farConn.CloseWrite()
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err = o.Answered(ctx)
		cancel()
		var ab *link.Abort
		switch {
		case refuse && (!errors.As(err, &ab) || answerReason(ab) != relayv1.ErrorReason_ERROR_REASON_TARGET_REFUSED || ab.Hops != 1):
			t.Fatalf("native refusal %v: want TARGET_REFUSED one hop beyond", err)
		case !refuse && err != nil:
			t.Fatalf("native accept %v", err)
		}
		if !refuse {
			if got, err := io.ReadAll(o); err != nil || string(got) != "hi" {
				t.Fatalf("after the native answer %q, %v: want the far end's bytes only", got, err)
			}
		}
	}
	// A refusal of the route itself reaches an in-band opener with its
	// reason; link's own codes with the nearest one.
	for _, tc := range []struct {
		err  error
		want relayv1.ErrorReason
	}{
		{dataplane.Refusal(relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND, "agent not found"), relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND},
		{link.ErrGoAway, relayv1.ErrorReason_ERROR_REASON_RELAY_DRAINING},
		{errors.New("no way"), relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE},
	} {
		in, client := fake.PlainSessionPair(t)
		go Forward(slog.New(slog.DiscardHandler), ChainConnect, in, true, func(context.Context) (peerlink.Session, func(), error) {
			return nil, nil, tc.err
		})
		_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
		if a, err := dataplane.ReadAnswer(client); err != nil || a.GetReason() != tc.want {
			t.Fatalf("route error %v: in-band answer %v, %v; want %v", tc.err, a, err, tc.want)
		}
	}
}
