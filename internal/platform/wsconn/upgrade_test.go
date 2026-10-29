// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package wsconn

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// wsClientConn wraps a plain net.Conn as a minimal WebSocket client for tests.
// It sends unmasked frames (server→client direction, mask=0) to keep the test
// simple — real browsers always mask, but our readFrame implementation
// handles both masked and unmasked frames.
type wsClientConn struct {
	conn net.Conn
}

func (c *wsClientConn) writeUnmasked(opcode byte, payload []byte) error {
	var header []byte
	header = append(header, 0x80|opcode)
	n := len(payload)
	switch {
	case n < 126:
		header = append(header, byte(n))
	case n < 65536:
		header = append(header, 126, byte(n>>8), byte(n))
	}
	_, err := c.conn.Write(append(header, payload...))
	return err
}

func (c *wsClientConn) readBinary(buf []byte) (int, error) {
	// Read 2-byte header
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(c.conn, hdr); err != nil {
		return 0, err
	}
	length := int(hdr[1] & 0x7F)
	if length == 126 {
		ext := make([]byte, 2)
		if _, err := io.ReadFull(c.conn, ext); err != nil {
			return 0, err
		}
		length = int(ext[0])<<8 | int(ext[1])
	}
	return io.ReadFull(c.conn, buf[:length])
}

func TestWebSocketUpgradeAndEcho(t *testing.T) {
	t.Parallel()

	// Create a test HTTP server that upgrades to WS and echoes binary messages.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := Upgrade(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer ws.Close() //nolint:errcheck
		for {
			opcode, payload, err := ws.ReadMessage()
			if err != nil || opcode == OpClose {
				return
			}
			if opcode == OpPing {
				_ = ws.WriteMessage(OpPong, payload)
				continue
			}
			_ = ws.WriteMessage(OpBinary, payload) // echo
		}
	}))
	defer srv.Close()

	// Dial the test server and perform a WebSocket handshake manually.
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	// Send HTTP upgrade request.
	req := "GET /ws HTTP/1.1\r\n" +
		"Host: " + srv.Listener.Addr().String() + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write request: %v", err)
	}

	// Read the 101 response line.
	buf := make([]byte, 512)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if !strings.HasPrefix(string(buf[:n]), "HTTP/1.1 101") {
		t.Fatalf("expected 101 response, got: %q", string(buf[:n]))
	}

	client := &wsClientConn{conn: conn}

	// Send a binary echo frame.
	msg := []byte("ping-data")
	if err := client.writeUnmasked(OpBinary, msg); err != nil {
		t.Fatalf("writeUnmasked: %v", err)
	}

	reply := make([]byte, len(msg))
	if _, err := client.readBinary(reply); err != nil {
		t.Fatalf("readBinary: %v", err)
	}
	if !bytes.Equal(reply, msg) {
		t.Errorf("echo mismatch: got %q, want %q", reply, msg)
	}
}

func TestWebSocketUpgrade_RejectsNonUpgrade(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := Upgrade(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/ws") //nolint:noctx
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", resp.StatusCode)
	}
}
