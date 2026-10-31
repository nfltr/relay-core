// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// The thin data plane (internal/platform/dataplane): a busy agent's further
// TCP tunnel streams on connections of their own that carry session frames
// without gRPC or HTTP/2. They arrive on the gRPC port, are told apart from
// gRPC connections by their first bytes, and are bound to the agent's
// primary ConnectTCP stream with its token (tcpBindHeader), as auxiliary
// gRPC connections are.

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
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

// A ConnectTCP response header says the relay takes data connections, one
// value per version (dataplane.Header, dataplane.Versions: "2" only since
// link v0.5.0 dropped version 1). An agent that speaks none of them binds
// auxiliary gRPC connections.

// TCPDataWindow is the relay's window per session on a data connection:
// what it queues for one session's client at most, and so how much the
// agent may send it unacknowledged. A session moves at most a window per
// round trip of the whole pipeline (agent -> relay -> client conn); 1 MiB
// (tcpSessionQueueBytes) measured iperf -P1 at -24% in docker, where the
// 1-CPU containers' scheduling stretches that round trip.
const TCPDataWindow = 4 << 20

// tcpGRPCWindow is the relay's window per session on a gRPC stream with
// credit (grpcCredit). HTTP/2 grows a stream's window with the path's
// bandwidth-delay product (grpc-go: to 16 MiB); a smaller credit window
// would cap a session there on a long path (BenchmarkTCPTunnelThroughputRTT:
// 4 MiB halved download at 40 ms).
const tcpGRPCWindow = 8 << 20

// DataPlaneSniffTimeout bounds the TLS handshake and first bytes of a
// connection to the gRPC port before it is handed to gRPC or served as a
// data connection.
const DataPlaneSniffTimeout = 20 * time.Second

// tcpFrameStream is what the dispatcher needs of an agent stream: a
// ConnectTCP gRPC stream or a data connection.
type tcpFrameStream interface {
	Send(*pb.TCPData) error
	Recv() (*pb.TCPData, error)
}

// DataPlaneListener wraps the gRPC listener: gRPC connections come out of
// Accept as before, data connections are served by d. From then on d offers
// agents the data plane (dataplane.Header).
func (d *TCPDispatcher) DataPlaneListener(inner net.Listener) net.Listener {
	l := &dataPlaneListener{Listener: inner, d: d, conns: make(chan net.Conn), done: make(chan struct{})}
	d.Link = newAgentLink(d, inner.Addr())
	go func() { _ = d.Link.tcp.Serve(refuseAgentSessions) }()
	go l.acceptLoop()
	d.dataPlane.Store(true)
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

// route reads a connection's first bytes: a data connection's Magic, or
// anything else (an HTTP/2 preface), which goes to gRPC with the bytes
// replayed.
func (l *dataPlaneListener) route(c net.Conn) {
	_ = c.SetReadDeadline(time.Now().Add(DataPlaneSniffTimeout))
	head := make([]byte, len(dataplane.Magic))
	if _, err := io.ReadFull(c, head); err != nil {
		_ = c.Close()
		return
	}
	if link.IsHello(head) {
		l.d.serveDataConn(&PrefixConn{Conn: c, Prefix: head})
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

// serveDataConn binds a data connection, whose Magic c replays, to the
// primary stream its token names, and serves it as an auxiliary stream of
// that agent until it ends. It speaks link version 2, with the agent's
// liveness; a version-1 hello (an agent before nfltr v2.0.0) gets the
// version reply and is counted (data_connections_total). The token
// is the connection's only credential: it is random, sent only in the
// primary's response header to the authenticated agent, and valid while
// the primary lives. Everything about the agent (identity, share policies)
// comes from the primary.
func (d *TCPDispatcher) serveDataConn(c net.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), DataPlaneSniffTimeout)
	defer cancel()
	lc, id, err := link.Server(ctx, c, link.Config{
		Window: TCPDataWindow, Auth: bindTokens{d},
		Observer: dataConnObserver{Observer: metrics.TransferObserver(metrics.PathTCPDataConn, false), logger: d.logger},
	})
	if err != nil {
		var ve *link.VersionError
		if errors.Is(err, link.ErrVersion) && !errors.As(err, &ve) { // a version-1 hello
			metrics.RecordDataConnection(1, "agent")
		}
		d.logger.Debug("tcp data connection refused", "remote", c.RemoteAddr(), "error", err)
		_ = c.Close()
		return
	}
	metrics.RecordDataConnection(lc.Version(), "agent")
	defer metrics.TrackTransferConn(metrics.PathTCPDataConn, lc.Stats)()
	dc := dataplane.Wrap(lc)
	defer dc.Close() //nolint:errcheck
	// Bound once the reply went out: a session placed here meanwhile would
	// find no connection. A primary that ended meanwhile, or took its last
	// auxiliary stream, ends this one.
	as, err := d.bindAux(id.Subject, id.Method, dc, dc.Credit())
	if err != nil {
		d.logger.Debug("tcp data connection not bound", "agent_id", id.Subject, "error", err)
		return
	}
	d.logger.Debug("agent bound a tcp data connection", "agent_id", id.Subject, "version", dc.Version())
	_ = d.serveAux(id.Subject, as, dc)
}

// bindTokens verifies a data connection's bind token: one a primary
// stream of this relay issued. The identity carries the agent (Subject)
// and the token (Method) for bindAux.
type bindTokens struct{ d *TCPDispatcher }

func (bindTokens) Hello(context.Context, tls.ConnectionState) (byte, []byte, error) {
	return 0, nil, errors.New("the relay does not dial data connections")
}

func (b bindTokens) Verify(_ context.Context, _ tls.ConnectionState, method byte, payload []byte) (link.Identity, error) {
	if method != link.MethodToken {
		return link.Identity{}, errors.New("a data connection binds with a token")
	}
	b.d.Mu.RLock()
	defer b.d.Mu.RUnlock()
	p := b.d.bound[string(payload)]
	if p == nil {
		return link.Identity{}, errors.New("no tcp stream of this agent to bind to")
	}
	if p.aux >= tcpAgentStreams-1 {
		return link.Identity{}, errors.New("the agent binds no more auxiliary tcp connections")
	}
	return link.Identity{Method: string(payload), Subject: p.agent}, nil
}

// dataConnObserver logs and counts a data connection the link ended
// because its agent fell silent (version 2 liveness).
type dataConnObserver struct {
	link.Observer // its ABORTs (its credit waits are in its Stats)
	logger        *slog.Logger
}

func (o dataConnObserver) ConnDown(id link.Identity, err error) {
	if errors.Is(err, link.ErrDeadPeer) {
		o.logger.Warn("tcp data connection: agent silent past its liveness", "agent_id", id.Subject)
		metrics.TransferError(metrics.PathTCPDataConn, metrics.TransferDeadPeer)
	}
}

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

// isSessionData reports whether f carries a session's data, which a data
// connection sends only on credit (control frames carry none).
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
	a.cr.Open(id)
	s.recvQ.SetLimit(a.cr.Window())
	s.limit = int64(a.cr.Window())
	s.credit = func(n int) { a.cr.Consumed(id, n) }
}
