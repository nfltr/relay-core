// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package wsconn is a minimal RFC 6455 WebSocket server side using only the
// Go standard library and wsframe: the WebSocket tunnels (tcp-connect, ssh)
// of the relay use it:
//
//   - Binary and text data frames (opcodes 0x1 and 0x2)
//   - Continuation frame assembly
//   - Ping / Pong (opcodes 0x9 / 0xA)
//   - Close frame (opcode 0x8), failing the connection with 1002/1007/1009
//     on a peer's protocol violation (wsframe.MessageReader)
//   - Client-to-server masking (required by RFC 6455 §5.3)
//   - Server-to-client unmasked frames
//   - Extended 16-bit and 64-bit payload lengths
//
// No external dependencies — uses net/http Hijacker for the upgrade.
package wsconn

import (
	"bufio"
	"crypto/sha1" // #nosec G505 -- SHA-1 is mandated by RFC 6455 for Sec-WebSocket-Accept
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/nfltr/relay/proto/wire/wsframe"
)

const (
	guid          = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	OpText   byte = 0x1
	OpBinary byte = 0x2
	OpClose  byte = 0x8
	OpPing   byte = 0x9
	OpPong   byte = 0xA

	// maxFramePayload caps the per-frame payload to protect against
	// memory-exhaustion from pathological WebSocket frames.
	maxFramePayload = 1 << 20 // 1 MiB
)

// ErrNotUpgrade is returned by Upgrade when the request is not a valid
// WebSocket upgrade.
var ErrNotUpgrade = errors.New("not a WebSocket upgrade request")

// Conn is a WebSocket connection built on top of a hijacked net.Conn.
// ReadMessage is not goroutine-safe; WriteMessage is goroutine-safe.
type Conn struct {
	conn      net.Conn
	rw        *bufio.ReadWriter
	mr        wsframe.MessageReader
	writeMu   sync.Mutex
	closeSent bool // under writeMu: no frame may follow a close frame
}

// Upgrade performs the RFC 6455 opening handshake and returns a *Conn.
//
// It validates the upgrade headers, computes Sec-WebSocket-Accept, hijacks
// the connection, and writes the 101 Switching Protocols response.
// The caller is responsible for calling Conn.Close when done.
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") ||
		!strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		return nil, ErrNotUpgrade
	}
	key := r.Header.Get("Sec-Websocket-Key")
	if key == "" {
		return nil, errors.New("missing Sec-WebSocket-Key header")
	}
	if r.Header.Get("Sec-Websocket-Version") != "13" {
		return nil, errors.New("unsupported WebSocket version (need 13)")
	}

	// Compute Sec-WebSocket-Accept per RFC 6455 §1.3.
	h := sha1.New() // #nosec G401 -- SHA-1 is mandated by RFC 6455 for Sec-WebSocket-Accept
	h.Write([]byte(key + guid))
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))

	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("server does not support connection hijacking")
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return nil, fmt.Errorf("hijack failed: %w", err)
	}

	// Write 101 response directly to the buffered writer.
	_, writeErr := fmt.Fprintf(rw,
		"HTTP/1.1 101 Switching Protocols\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Accept: %s\r\n"+
			"\r\n",
		accept,
	)
	if writeErr != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write upgrade response: %w", writeErr)
	}
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("flush upgrade response: %w", err)
	}

	return &Conn{conn: conn, rw: rw}, nil
}

// ReadMessage reads one complete WebSocket message, following continuation
// frames transparently, or a control frame (which may arrive between a
// message's fragments). Returns the opcode and payload. On a peer's protocol
// violation it fails the connection with the matching close code and
// returns the *wsframe.CloseError.
//
// Returns io.EOF or a net error when the connection is closed or reset.
func (c *Conn) ReadMessage() (opcode byte, payload []byte, err error) {
	opcode, payload, err = c.mr.ReadMessage(c.rw.Reader, maxFramePayload)
	if ce, ok := errors.AsType[*wsframe.CloseError](err); ok {
		_ = c.WriteMessage(OpClose, wsframe.ClosePayload(ce.Code))
	}
	return opcode, payload, err
}

// WriteMessage sends a single (non-fragmented) WebSocket frame to the client.
// Safe for concurrent use from multiple goroutines.
func (c *Conn) WriteMessage(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closeSent {
		return net.ErrClosed
	}
	c.closeSent = opcode == OpClose

	// Server frames are never masked.
	var h [10]byte
	if _, err := c.rw.Write(wsframe.AppendHeader(h[:0], opcode, len(payload), nil)); err != nil {
		return err
	}
	if _, err := c.rw.Write(payload); err != nil {
		return err
	}
	return c.rw.Flush()
}

// Close sends a normal-closure close frame (code 1000), unless a close frame
// was already sent, and closes the underlying connection. Safe to call
// multiple times.
func (c *Conn) Close() error {
	// Best-effort: ignoring write error since we're closing anyway.
	_ = c.WriteMessage(OpClose, wsframe.ClosePayload(wsframe.CloseNormal))
	return c.conn.Close()
}
