// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// CountConn is c, one side of a session on p, counting the session's
// payload bytes on p as they cross it and its first byte from the agent
// (since began). agentSide says c is the agent's side (what is read from
// it came from the agent); else it is the visitor's or client's (what is
// read from it goes to the agent). bytes false counts the first byte only:
// a transport path whose connections' own counters hold its bytes
// (TrackTransferConn). Every session whose bytes the relay copies itself
// goes through one (TestEveryOpenedPathCountsItsBytes, metricstest).
func CountConn(c net.Conn, p Path, began time.Time, agentSide, bytes bool) net.Conn {
	return &countedConn{Conn: c, p: p, began: began, agentSide: agentSide, bytes: bytes}
}

type countedConn struct {
	net.Conn
	p                Path
	began            time.Time
	agentSide, bytes bool
	first            atomic.Bool
}

func (c *countedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.count(n, c.agentSide)
	return n, err
}

func (c *countedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.count(n, !c.agentSide)
	return n, err
}

func (c *countedConn) count(n int, fromAgent bool) {
	if n <= 0 {
		return
	}
	if !fromAgent {
		if c.bytes {
			AddTransferBytes(c.p, n, 0)
		}
		return
	}
	if c.bytes {
		AddTransferBytes(c.p, 0, n)
	}
	if !c.first.Load() && c.first.CompareAndSwap(false, true) {
		ObserveTransferFirstByte(c.p, time.Since(c.began))
	}
}

// CloseWrite passes to c's CloseWrite (errors.ErrUnsupported without one).
func (c *countedConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

// Reset passes to c's Reset, else cuts a TCP conn with an RST.
func (c *countedConn) Reset() error {
	if r, ok := c.Conn.(interface{ Reset() error }); ok {
		return r.Reset()
	}
	if l, ok := c.Conn.(interface{ SetLinger(int) error }); ok {
		_ = l.SetLinger(0)
	}
	return c.Conn.Close()
}

// CountHTTP counts an HTTP request on p: its body's bytes to the agent,
// the response's from it, the response's header as its first byte (since
// began). The writer it returns unwraps (http.ResponseController) to w.
func CountHTTP(w http.ResponseWriter, r *http.Request, p Path, began time.Time) http.ResponseWriter {
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = &countedBody{ReadCloser: r.Body, p: p}
	}
	return &countedResponse{ResponseWriter: w, p: p, began: began}
}

type countedBody struct {
	io.ReadCloser
	p Path
}

func (b *countedBody) Read(buf []byte) (int, error) {
	n, err := b.ReadCloser.Read(buf)
	AddTransferBytes(b.p, n, 0)
	return n, err
}

type countedResponse struct {
	http.ResponseWriter
	p      Path
	began  time.Time
	header bool
}

func (w *countedResponse) WriteHeader(code int) {
	if !w.header && code >= 200 { // 1xx are interim
		w.header = true
		ObserveTransferFirstByte(w.p, time.Since(w.began))
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *countedResponse) Write(b []byte) (int, error) {
	if !w.header {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	AddTransferBytes(w.p, 0, n)
	return n, err
}

// Flush is for the writer's users that assert http.Flusher (the gRPC
// path's proxy, which flushes a streamed response's header before its
// body; TestCountHTTPWriterFlushes).
func (w *countedResponse) Flush() {
	if !w.header {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

// Unwrap is for http.ResponseController (Hijack).
func (w *countedResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
