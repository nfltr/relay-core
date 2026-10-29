// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// overlapSession is a peerlink.Session whose Write records whether another
// Write was in progress: a link session takes one writer at a time.
type overlapSession struct {
	net.Conn
	in, overlaps atomic.Int32
}

func (s *overlapSession) Write(p []byte) (int, error) {
	if s.in.Add(1) > 1 {
		s.overlaps.Add(1)
	}
	time.Sleep(time.Millisecond)
	s.in.Add(-1)
	return len(p), nil
}
func (s *overlapSession) CloseWrite() error { return nil }
func (s *overlapSession) Reset() error      { return nil }
func (s *overlapSession) Close() error      { return nil }

// A link session handed out as a net.Conn (DialAgent's, the replica link's
// HTTP dial) is written by goroutines at once: net/http's transport hands
// an upgraded (101) connection to the caller while its write loop can
// still be inside the request's Write. The session underneath must still
// see one writer at a time.
func TestSessionConnSerialisesWrites(t *testing.T) {
	for name, wrap := range map[string]func(*overlapSession) net.Conn{
		"SessionConn":   func(s *overlapSession) net.Conn { return &SessionConn{Session: s} },
		"dialedSession": func(s *overlapSession) net.Conn { return newDialedSession(context.Background(), s, nil) },
	} {
		t.Run(name, func(t *testing.T) { testSerialisesWrites(t, wrap) })
	}
}

func testSerialisesWrites(t *testing.T, wrap func(*overlapSession) net.Conn) {
	s := &overlapSession{}
	d := wrap(s).(interface {
		net.Conn
		CloseWrite() error
	})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 5 {
				_, _ = d.Write([]byte("x"))
				_ = d.CloseWrite()
			}
		})
	}
	wg.Wait()
	if n := s.overlaps.Load(); n != 0 {
		t.Fatalf("%d overlapping Writes reached the session", n)
	}
}
