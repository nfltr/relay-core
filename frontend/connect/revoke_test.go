// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/core/identity"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// TestRevokedSessionsEndTyped: a data connection's open sessions end with
// an abort of reason ERROR_REASON_UNAUTHENTICATED when the connection's
// credential is revoked or expires (the client reads why, instead of a
// bare reset it would retry), and are left alone when the hold ended for
// any other cause (the client left).
func TestRevokedSessionsEndTyped(t *testing.T) {
	for _, tc := range []struct {
		cause error
		typed bool
	}{{identity.ErrRevoked, true}, {identity.ErrExpired, true}, {context.Canceled, false}} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		var open sessionSet
		accepted := make(chan struct{})
		served := make(chan struct{})
		go func() {
			defer close(served)
			nc, err := ln.Accept()
			_ = ln.Close()
			if err != nil {
				return
			}
			defer nc.Close() //nolint:errcheck
			if _, err := http.ReadRequest(bufio.NewReader(nc)); err != nil {
				return
			}
			m, err := dataplane.Accept(t.Context(), nc, 1<<20, "allow")
			if err != nil {
				return
			}
			_ = m.Serve(func(_ *pb.TCPData, s *dataplane.Session) {
				open.add(s)
				accepted <- struct{}{}
				_, _ = io.Copy(io.Discard, s)
			})
		}()
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		m, _, err := dataplane.Upgrade(t.Context(), nc, "relay", "k", 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = m.Serve(nil) }()
		s, err := m.Open(&pb.TCPData{AgentId: "a", TargetAddr: "22"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		<-accepted

		open.abort(credentialAbort(tc.cause))
		wait := 200 * time.Millisecond // long enough for an abort that should not come
		if tc.typed {
			wait = 5 * time.Second
		}
		_ = s.SetReadDeadline(time.Now().Add(wait))
		_, err = s.Read(make([]byte, 1))
		a, isAbort := errors.AsType[*link.Abort](err)
		reason, _ := dataplane.AbortReason(a)
		switch {
		case tc.typed && (!isAbort || reason != relayv1.ErrorReason_ERROR_REASON_UNAUTHENTICATED):
			t.Errorf("%v: the session ended with %v, want an abort of reason ERROR_REASON_UNAUTHENTICATED", tc.cause, err)
		case !tc.typed && !errors.Is(err, context.DeadlineExceeded) && !isTimeout(err):
			t.Errorf("%v: the session ended with %v, want it left open", tc.cause, err)
		}
		_ = m.Close()
		<-served
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
