// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/nfltr/relay-core/core/session"
)

// HTTP2 is the relay HTTP servers' HTTP/2 config. Every tunnel
// of a client shares its one connection, so the receive windows match the
// agents' gRPC ones (4 MiB per stream, 16 MiB per connection) instead of
// net/http's 1 MiB, which would cap a client's uploads at 1 MiB per round
// trip across all of its tunnels.
var HTTP2 = &http.HTTP2Config{
	MaxReceiveBufferPerStream:     4 << 20,
	MaxReceiveBufferPerConnection: 16 << 20,
	MaxReadFrameSize:              streamFrameSize,
}

// streamFrameSize is the largest HTTP/2 DATA frame the relay and
// tcp-connect accept from each other (net/http's default is 16 KiB). The
// TLS record is 16 KiB either way, but a 16 KiB frame plus its 9-byte
// header takes two records, and every record is a write syscall; larger
// frames also mean fewer body reads, each of which costs a WINDOW_UPDATE
// write back.
const streamFrameSize = 256 << 10

// IsStream reports whether r opens a /connect tunnel as an HTTP/2 stream.
func IsStream(r *http.Request) bool {
	return r.Method == http.MethodPost && r.ProtoMajor == 2 && strings.HasPrefix(r.URL.Path, Path)
}

// serveStream bridges a /connect tunnel carried as one HTTP/2 stream
// (POST, full duplex): the request body is client-to-agent bytes, the
// response body agent-to-client bytes. A client keeps one pooled HTTP/2
// connection to the relay and opens a stream per tunnel, so a new tunnel
// costs no TCP, TLS or WebSocket handshake (`nfltr tcp-connect` used to pay
// all three per local connection). The stream ends when either side does.
func serveStream(w http.ResponseWriter, r *http.Request, conn net.Conn) {
	// A tunnel idles as long as its user does (an SSH session), so it runs
	// on the server's own writer and body without stream deadlines; the
	// per-write deadline wrapper (writeProgressDeadline) would re-arm them.
	for {
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			break
		}
		w = u.Unwrap()
	}
	body := r.Body
	for {
		u, ok := body.(interface{ Unwrap() io.ReadCloser })
		if !ok {
			break
		}
		body = u.Unwrap()
	}
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		return
	}

	// The request body's end is the client's half close: it passes on, and
	// the agent's bytes keep coming until it ends too, which ends the
	// response (an HTTP/2 response cannot end before its handler returns).
	up := make(chan struct{})
	go func() {
		defer close(up)
		buf := session.TCPRelayBufPool.Get().([]byte)
		defer session.TCPRelayBufPool.Put(buf) //nolint:staticcheck
		if _, err := io.CopyBuffer(conn, body, buf); err != nil {
			_ = conn.Close() // the client went away: so does the agent's side
			return
		}
		session.CloseWrite(conn)
	}()
	fw := flushWriter{w, rc}
	if wt, ok := conn.(io.WriterTo); ok {
		_, _ = wt.WriteTo(fw)
	} else {
		buf := session.TCPRelayBufPool.Get().([]byte)
		_, _ = io.CopyBuffer(fw, struct{ io.Reader }{conn}, buf)
		session.TCPRelayBufPool.Put(buf) //nolint:staticcheck
	}
	_ = conn.Close()
	_ = body.Close()
	<-up
}

// flushWriter flushes each Write to the client: tunnel bytes must not wait
// in the response buffer.
type flushWriter struct {
	w  io.Writer
	rc *http.ResponseController
}

func (f flushWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if err == nil {
		err = f.rc.Flush()
	}
	return n, err
}
