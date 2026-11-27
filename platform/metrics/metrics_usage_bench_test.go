// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"net"
	"testing"
	"time"
)

type discardConn struct{ net.Conn }

func (discardConn) Write(b []byte) (int, error) { return len(b), nil }
func (discardConn) Read(b []byte) (int, error)  { return len(b), nil }

// BenchmarkCountedConnWrite is the per-write cost a session's bytes pay on
// a frontend path: one counted write of 16 KiB, sessions in parallel.
func BenchmarkCountedConnWrite(b *testing.B) {
	buf := make([]byte, 16<<10)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		c := benchConn(discardConn{})
		for pb.Next() {
			_, _ = c.Write(buf)
		}
	})
}

// BenchmarkAddTransferBytes is a frontend path's byte count without a
// session's meter.
func BenchmarkAddTransferBytes(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			AddTransferBytes(PathConnect, 10, 20)
		}
	})
}

// benchConn is a session's counted conn with its meter, as a frontend
// opens one.
func benchConn(c net.Conn) net.Conn {
	EnableUsage()
	return CountConn(c, PathConnect, OpenMeter(PathConnect, "owner", "agent"), time.Now(), true, true)
}
