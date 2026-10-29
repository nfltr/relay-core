// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package wsconn

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"slices"
	"testing"

	"github.com/nfltr/relay/proto/wire/wsframe"
)

// BenchmarkWSConnReadMessage: the relay reading a client's masked 32 KiB
// tunnel frames (tcp-connect over WebSocket, bridgeWSToConn).
func BenchmarkWSConnReadMessage(b *testing.B) {
	key := [4]byte{9, 8, 7, 6}
	frame := wsframe.AppendFrame(nil, OpBinary, bytes.Repeat([]byte{1}, 32<<10), &key)
	wire := bytes.Repeat(frame, 64)
	rd := bytes.NewReader(wire)
	c := &Conn{rw: bufio.NewReadWriter(bufio.NewReaderSize(rd, 64<<10), nil)}
	b.SetBytes(32 << 10)
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := c.ReadMessage(); err == io.EOF {
			rd.Reset(wire)
			c.rw.Reader.Reset(rd)
		} else if err != nil {
			b.Fatal(err)
		}
	}
}

// TestWSConnFailsViolationWithCloseCode: a client's reserved opcode fails
// the connection with one close frame carrying 1002 (Autobahn 4.x); the
// handler's deferred Close adds no second close frame.
func TestWSConnFailsViolationWithCloseCode(t *testing.T) {
	key := [4]byte{1, 2, 3, 4}
	var out bytes.Buffer
	c := &Conn{
		conn: discardNetConn{},
		rw: bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(wsframe.AppendFrame(nil, 0x3, nil, &key))),
			bufio.NewWriter(&out)),
	}
	_, _, err := c.ReadMessage()
	if ce, ok := errors.AsType[*wsframe.CloseError](err); !ok || ce.Code != wsframe.CloseProtocolError {
		t.Fatalf("err %v, want a 1002 close error", err)
	}
	_ = c.Close()
	want := wsframe.AppendFrame(nil, OpClose, wsframe.ClosePayload(wsframe.CloseProtocolError), nil)
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("wrote % x, want one 1002 close frame % x", out.Bytes(), want)
	}
}

// TestWSConnPingInsideFragmentedMessage guards a tunnel data loss: a ping
// between a binary message's fragments used to turn the message into a
// "ping" whose payload bridgeWSToConn answered as a pong instead of
// forwarding it to the agent.
func TestWSConnPingInsideFragmentedMessage(t *testing.T) {
	key := [4]byte{1, 2, 3, 4}
	first := wsframe.AppendFrame(nil, OpBinary, []byte("data-"), &key)
	first[0] &^= 0x80 // not FIN
	wire := slices.Concat(first, wsframe.AppendFrame(nil, OpPing, []byte("p"), &key),
		wsframe.AppendFrame(nil, 0x0, []byte("rest"), &key))
	c := &Conn{rw: bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(wire)), nil)}
	for _, want := range []struct {
		op      byte
		payload string
	}{{OpPing, "p"}, {OpBinary, "data-rest"}} {
		op, p, err := c.ReadMessage()
		if err != nil || op != want.op || string(p) != want.payload {
			t.Fatalf("got op %#x %q err %v, want op %#x %q", op, p, err, want.op, want.payload)
		}
	}
}

type discardNetConn struct{ net.Conn }

func (discardNetConn) Close() error { return nil }
