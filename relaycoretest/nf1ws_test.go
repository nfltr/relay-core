// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package relaycoretest_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/nfltr/relay-core/relaycoretest"
	"github.com/nfltr/relay/client/capaddr"
	"github.com/nfltr/relay/client/nf1"
	"github.com/nfltr/relay/proto/wire/wsframe"
	"github.com/spf13/viper"
)

// wsClient is a browser's WebSocket as the extension's bundle uses it
// (nfltr docs/decisions/browser-peer-2026-11-16.md section 2): masked
// binary messages, the server's close code kept.
type wsClient struct {
	net.Conn
	br    *bufio.Reader
	mr    wsframe.MessageReader
	buf   []byte
	close uint16 // the server's close code, once read
}

// wsClosed is a read ended by the server's close frame.
type wsClosed struct{ code uint16 }

func (e wsClosed) Error() string { return fmt.Sprintf("websocket closed %d", e.code) }

func (c *wsClient) send(op byte, p []byte) error {
	var key [4]byte
	_, _ = rand.Read(key[:])
	_, err := c.Conn.Write(wsframe.AppendFrame(nil, op, p, &key))
	return err
}

func (c *wsClient) Write(p []byte) (int, error) {
	if err := c.send(0x2, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsClient) Read(p []byte) (int, error) {
	for len(c.buf) == 0 {
		op, msg, err := c.mr.ReadMessage(c.br, 1<<20)
		switch {
		case err != nil:
			return 0, err
		case op == 0x8:
			if len(msg) >= 2 {
				c.close = binary.BigEndian.Uint16(msg)
			}
			return 0, wsClosed{c.close}
		case op == 0x9:
			_ = c.send(0xA, msg)
		case op == 0x2:
			c.buf = msg
		}
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

// dialNF1WS upgrades a connection to proxy at /nf1/v1 with the
// subprotocols offered (none: no header) and an extension's Origin.
func dialNF1WS(t *testing.T, proxy string, protocols string) (*wsClient, *http.Response) {
	t.Helper()
	nc, err := net.Dial("tcp", proxy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	req, _ := http.NewRequest(http.MethodGet, "http://relay.test/nf1/v1", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Origin", "chrome-extension://abcdefghijklmnopabcdefghijklmnop")
	if protocols != "" {
		req.Header.Set("Sec-WebSocket-Protocol", protocols)
	}
	if err := req.Write(nc); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatal(err)
	}
	return &wsClient{Conn: nc, br: br}, resp
}

// openNF1WS opens addr over the relay's WebSocket: the rid message, then
// the shipped handshake on the WebSocket's bytes.
func openNF1WS(t *testing.T, proxy string, addr capaddr.Address) (*wsClient, net.Conn, error) {
	t.Helper()
	ws, resp := dialNF1WS(t, proxy, "nf1.v1")
	if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Sec-WebSocket-Protocol") != "nf1.v1" {
		t.Fatalf("upgrade: %s, protocol %q", resp.Status, resp.Header.Get("Sec-WebSocket-Protocol"))
	}
	if _, err := ws.Write(addr.RID[:]); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	c, err := nf1.Handshake(ctx, ws, addr)
	return ws, c, err
}

// enableNF1WS turns the WebSocket on for the relays the test starts
// (nf1.websocket), with idle as its idle bound (0: the default).
func enableNF1WS(t *testing.T, idle time.Duration) {
	viper.Set("nf1.websocket", true)
	if idle > 0 {
		viper.Set("nf1.websocket_idle", idle)
	}
	t.Cleanup(func() {
		viper.Set("nf1.websocket", false)
		viper.Set("nf1.websocket_idle", 10*time.Minute)
	})
}

// wsUntil opens addr over the WebSocket until it is served (the agent
// registers just after Connect) and under its caps (an ended session's
// count is freed once the relay's side of it ended too).
func wsUntil(t *testing.T, proxy string, addr capaddr.Address) (*wsClient, net.Conn) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(500 * time.Millisecond) {
		ws, c, err := openNF1WS(t, proxy, addr)
		if err == nil {
			return ws, c
		}
		if (ws.close != 4003 && ws.close != 4004) || time.Now().After(deadline) {
			t.Fatalf("open: %v (close %d)", err, ws.close)
		}
	}
}

// TestNF1WebSocket (nfltr docs/decisions/browser-peer-2026-11-16.md
// section 2): a browser with no credential opens an address over
// wss://R/nf1/v1 (subprotocol nf1.v1): its first message the rid, then
// the session's bytes, the shipped nf1 handshake end to end; an unknown
// rid closes 4003 (not served), another secret fails the proof (the
// relay sees no difference), a text message closes 1003, an upgrade
// without the subprotocol is 400.
func TestNF1WebSocket(t *testing.T) {
	enableNF1(t, 64)
	enableNF1WS(t, 0)
	r := relaycoretest.New(t)
	keyFile := filepath.Join(t.TempDir(), "key")
	writeKey(t, keyFile, r.Key)
	ag := startNF1Agent(t, r.GRPC, keyFile)

	_, c := wsUntil(t, r.Proxy, ag.l.Address())
	echoes(t, c)
	_ = c.Close()

	unknown := ag.l.Address()
	unknown.RID[0] ^= 0xff
	if ws, _, err := openNF1WS(t, r.Proxy, unknown); err == nil || ws.close != 4003 {
		t.Errorf("an unknown rid: %v, close %d, want 4003", err, ws.close)
	}

	other, _, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	wrongSecret := other.Address()
	wrongSecret.RID = ag.l.Address().RID // routed to the agent, whose pin it is not
	if _, _, err := openNF1WS(t, r.Proxy, wrongSecret); !errors.Is(err, capaddr.ErrPin) {
		t.Errorf("another key's address: %v, want ErrPin", err)
	}

	ws, _ := dialNF1WS(t, r.Proxy, "nf1.v1")
	if err := ws.send(0x1, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(ws); !errors.As(err, new(wsClosed)) || ws.close != 1003 {
		t.Errorf("a text message: %v, close %d, want 1003", err, ws.close)
	}

	if _, resp := dialNF1WS(t, r.Proxy, ""); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("no subprotocol: %s, want 400", resp.Status)
	}
	if _, resp := dialNF1WS(t, r.Proxy, "chat, other"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("another subprotocol: %s, want 400", resp.Status)
	}
}

// TestNF1WebSocketOff: nf1.websocket is off unless set; with nf1 on, the
// path answers 404 and opens nothing.
func TestNF1WebSocketOff(t *testing.T) {
	enableNF1(t, 64)
	r := relaycoretest.New(t)
	if _, resp := dialNF1WS(t, r.Proxy, "nf1.v1"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("nf1.websocket off: %s, want 404", resp.Status)
	}
}

// TestNF1WebSocketCapsAndIdle: the address's open-session cap closes a
// second WebSocket 4004 (refused, the CLI's exit 4) and frees its count
// with the first; a session with no byte either way for
// nf1.websocket_idle closes 1001.
func TestNF1WebSocketCapsAndIdle(t *testing.T) {
	enableNF1(t, 1)
	enableNF1WS(t, 2*time.Second)
	r := relaycoretest.New(t)
	keyFile := filepath.Join(t.TempDir(), "key")
	writeKey(t, keyFile, r.Key)
	ag := startNF1Agent(t, r.GRPC, keyFile)

	firstWS, first := wsUntil(t, r.Proxy, ag.l.Address())
	echoes(t, first)
	if ws, _, err := openNF1WS(t, r.Proxy, ag.l.Address()); err == nil || ws.close != 4004 {
		t.Fatalf("a second session over address_max: %v, close %d, want 4004", err, ws.close)
	}
	// Idle: the relay closes the first one.
	_ = first.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := first.Read(make([]byte, 1)); err == nil || firstWS.close != 1001 {
		t.Fatalf("an idle session: %v, close %d, want 1001", err, firstWS.close)
	}
	_, second := wsUntil(t, r.Proxy, ag.l.Address())
	echoes(t, second)
	_ = second.Close()
}
