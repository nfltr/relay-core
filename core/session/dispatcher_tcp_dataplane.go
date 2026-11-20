// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// The gRPC port's sniff: a connection's first bytes tell the agent link
// (dataplane.AgentLinkMagic) from gRPC. Agent data connections (a link
// hello here, bound to a primary with its token) were retired in lane
// LINKV013 (nfltr docs/decisions/link-data-connections-retired-2026-11-20.md):
// a link hello is refused (refuseDataConn).

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"google.golang.org/grpc/metadata"

	"github.com/nfltr/relay-core/internal/platform/acceptloop"
	"github.com/nfltr/relay-core/platform/metrics"
)

// TCPDataWindow is the relay's window per session on the agent link and a
// /connect data connection: what it queues for one session's client at most, and so how much the
// agent may send it unacknowledged. A session moves at most a window per
// round trip of the whole pipeline (agent -> relay -> client conn); 1 MiB
// (the gRPC path's queue bound) measured iperf -P1 at -24% in docker, where the
// 1-CPU containers' scheduling stretches that round trip.
const TCPDataWindow = 4 << 20

// tcpGRPCWindow is the relay's window per session on a gRPC stream with
// credit (grpcCredit). HTTP/2 grows a stream's window with the path's
// bandwidth-delay product (grpc-go: to 16 MiB); a smaller credit window
// would cap a session there on a long path (BenchmarkTCPTunnelThroughputRTT:
// 4 MiB halved download at 40 ms).
const tcpGRPCWindow = 8 << 20

// DataPlaneSniffTimeout bounds the TLS handshake and first bytes of a
// connection to the gRPC port before it is handed to gRPC or the agent
// link.
const DataPlaneSniffTimeout = 20 * time.Second

// tcpFrameStream is what the dispatcher needs of an agent stream: a
// ConnectTCP gRPC stream.
type tcpFrameStream interface {
	Send(*pb.TCPData) error
	Recv() (*pb.TCPData, error)
}

// DataPlaneListener wraps the gRPC listener: gRPC connections come out of
// Accept as before, agent-link connections are served by d.
func (d *TCPDispatcher) DataPlaneListener(inner net.Listener) net.Listener {
	l := &dataPlaneListener{Listener: inner, d: d, conns: make(chan net.Conn), done: make(chan struct{})}
	d.Link = newAgentLink(d, inner.Addr())
	go func() { _ = d.Link.tcp.Serve(refuseAgentSessions) }()
	go l.acceptLoop()
	return l
}

type dataPlaneListener struct {
	net.Listener
	d     *TCPDispatcher
	conns chan net.Conn
	done  chan struct{}
	err   error // set before done closes
}

func (l *dataPlaneListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, l.err
	}
}

// acceptLoop sniffs every connection in a goroutine of its own: a slow
// handshake delays only itself.
func (l *dataPlaneListener) acceptLoop() {
	var backoff acceptloop.Backoff
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			if backoff.Retry(l.d.logger, "grpc_dataplane", err) {
				continue
			}
			l.err = err
			close(l.done)
			l.d.Link.drain() // the agent link's Serve ends with the port; its sessions run to the drain's deadline
			return
		}
		backoff.Reset()
		go l.route(c)
	}
}

// route reads a connection's first bytes: the agent link's magic, a link
// hello (refused), or anything else (an HTTP/2 preface), which goes to
// gRPC with the bytes replayed.
func (l *dataPlaneListener) route(c net.Conn) {
	_ = c.SetReadDeadline(time.Now().Add(DataPlaneSniffTimeout))
	head := make([]byte, len(dataplane.Magic))
	if _, err := io.ReadFull(c, head); err != nil {
		_ = c.Close()
		return
	}
	if link.IsHello(head) {
		l.d.refuseDataConn(&PrefixConn{Conn: c, Prefix: head})
		return
	}
	if string(head) == dataplane.AgentLinkMagic {
		_ = c.SetReadDeadline(time.Time{}) // the link's handshake sets its own
		l.d.Link.serveConn(c)
		return
	}
	if err := c.SetReadDeadline(time.Time{}); err != nil {
		_ = c.Close()
		return
	}
	select {
	case l.conns <- &PrefixConn{Conn: c, Prefix: head}:
	case <-l.done:
		_ = c.Close()
	}
}

// refuseDataConn answers a link hello, whose magic c replays, with link's
// refusal (reply status 1, the dialer's ErrRefused) and closes it: agent
// data connections are retired, and the relay never offers them.
func (d *TCPDispatcher) refuseDataConn(c net.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), DataPlaneSniffTimeout)
	defer cancel()
	_, _, err := link.Server(ctx, c, link.Config{Auth: retiredDataConns{}})
	d.logger.Debug("tcp data connection refused", "remote", c.RemoteAddr(), "error", err)
	_ = c.Close()
}

// retiredDataConns refuses every data connection's credential.
type retiredDataConns struct{}

func (retiredDataConns) Hello(context.Context, tls.ConnectionState) (byte, []byte, error) {
	return 0, nil, errDataConnsRetired
}

func (retiredDataConns) Verify(context.Context, tls.ConnectionState, byte, []byte) (link.Identity, error) {
	return link.Identity{}, errDataConnsRetired
}

var errDataConnsRetired = errors.New("data connections are retired: update nfltr")

// grpcCredit is a gRPC stream's per-session credit when its agent asked
// for it (dataplane.CreditHeader in its request metadata, its window), nil
// otherwise; send carries its window updates to the agent.
func grpcCredit(ctx context.Context, send func(*pb.TCPData)) *link.Credit {
	v := metadata.ValueFromIncomingContext(ctx, dataplane.CreditHeader)
	if len(v) == 0 {
		return nil
	}
	w, err := dataplane.ParseCreditHeader(v[0])
	if err != nil {
		return nil
	}
	cr := link.NewCredit(tcpGRPCWindow, w, func(sid string, n int) { send(dataplane.WindowFrame(sid, n)) })
	cr.SetObserver(metrics.TransferObserver(metrics.PathTCPGRPC, true))
	return cr
}

// grpcCreditFrame applies a frame received on a gRPC stream with credit:
// a window update grants, session data is charged. An agent that breaks
// the credit rules ends the stream.
func grpcCreditFrame(cr *link.Credit, f *pb.TCPData) error {
	switch sid := f.GetSessionId(); {
	case sid == dataplane.WindowSID:
		s, n, err := dataplane.ParseWindowFrame(f)
		if err != nil {
			return err
		}
		return cr.Grant(s, n)
	case isSessionData(f):
		return cr.Received(sid, len(f.GetData()))
	}
	return nil
}

// isSessionData reports whether f carries a session's data, which a
// stream with credit sends only on credit (control frames carry none).
func isSessionData(f *pb.TCPData) bool {
	return len(f.GetData()) > 0 && !strings.HasPrefix(f.GetSessionId(), "_")
}

// sendData queues a session's data in chunks the agent has credit for,
// waiting for it; it drops the rest once the session or the stream ended.
func (a *tcpAgentStream) sendData(f *pb.TCPData) {
	for data := f.GetData(); len(data) > 0; {
		n := min(len(data), dataplane.MaxChunk)
		if !a.cr.Acquire(f.GetSessionId(), n) {
			return
		}
		if traceOn {
			traceWrite(a, f.GetSessionId(), n)
		}
		chunk := f
		if n < len(f.GetData()) {
			chunk = &pb.TCPData{SessionId: f.GetSessionId(), Data: data[:n]}
		}
		a.enqueue(chunk)
		data = data[n:]
	}
}

// openCredit starts a session's credit on the stream; its queue holds a
// window and acknowledges what its client takes.
func (a *tcpAgentStream) openCredit(id string, s *tcpSession) {
	if traceOn {
		traceOpenSess(a, id, s)
	}
	a.cr.Open(id)
	s.recvQ.SetLimit(a.cr.Window())
	s.limit = int64(a.cr.Window())
	s.credit = func(n int) { a.cr.Consumed(id, n) }
}
