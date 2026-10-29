// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/txnregistry"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/chunking"
	"golang.org/x/net/http/httpguts"
)

// A protocol upgrade (WebSocket) through the HTTP tunnel: the request goes
// to the agent as chunk 0 of a streamed upload, the agent answers with the
// backend's 101 as chunk 0 of a streamed response, and the relay then takes
// the visitor's connection over (hijack) and carries its bytes both ways as
// further chunks of the same transaction, under the upload and response
// windows (chunking.ReadUpgradeChunksWithContext has the wire contract).
// Without it a 101 was relayed as a response whose body never came: the
// visitor got nothing.

// upgradeRequest is one visitor's upgrade: its bytes after the switch reach
// the request's chunk reader through a pipe, fed once the relay holds the
// visitor's connection.
type upgradeRequest struct {
	visitor *io.PipeReader
	feed    *io.PipeWriter
}

// newUpgradeRequest returns nil when r asks for no protocol upgrade. r's
// headers are as the tunnel forwards them (stripHopByHopHeaders keeps the
// upgrade, as httputil.ReverseProxy does).
func newUpgradeRequest(r *http.Request) *upgradeRequest {
	if !httpguts.HeaderValuesContainsToken(r.Header["Connection"], "Upgrade") || r.Header.Get("Upgrade") == "" {
		return nil
	}
	pr, pw := io.Pipe()
	return &upgradeRequest{visitor: pr, feed: pw}
}

// close ends the visitor's side: a request whose backend did not switch, or
// a bridge that ended, leaves no reader waiting.
func (u *upgradeRequest) close() {
	if u != nil {
		_ = u.feed.Close()
	}
}

// bridge takes the visitor's connection over once the agent answered 101
// (first, chunk 0, whose headers are in w.Header() already) and relays the
// rest of the transaction both ways until either side ends; it reports
// whether the agent ended it (its last chunk), else the caller cancels the
// agent's side. pending holds the chunks that came before chunk 0;
// visitorDone closes when the visitor's bytes stopped reaching the agent.
func (p *Proxy) bridge(w http.ResponseWriter, r *http.Request, u *upgradeRequest, first *pb.HTTPProxyResponse, pending map[int]*pb.HTTPProxyResponse,
	handle *txnregistry.TransactionHandle[*pb.HTTPProxyResponse], rctx context.Context, visitorDone <-chan publishError, timeout time.Duration,
	agentID, transactionID string, logger *slog.Logger,
) bool {
	conn, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		logger.Warn("upgrade refused: the visitor's connection cannot be taken over", "error", err)
		http.Error(w, "this connection cannot switch protocols (HTTP/1.1 is required)", http.StatusBadGateway)
		return false
	}
	defer conn.Close()                //nolint:errcheck
	_ = conn.SetDeadline(time.Time{}) // the server's deadlines were for the request
	write := func(b []byte) error {
		_ = conn.SetWriteDeadline(time.Now().Add(timeout)) // a visitor that takes nothing is cut
		_, err := conn.Write(b)
		return err
	}
	head := &http.Response{StatusCode: http.StatusSwitchingProtocols, ProtoMajor: 1, ProtoMinor: 1, Header: w.Header(), Request: r}
	if err := head.Write(brw.Writer); err != nil || brw.Flush() != nil {
		return false
	}
	go func() { _, err := io.Copy(u.feed, brw.Reader); _ = u.feed.CloseWithError(err) }()
	if err := write(first.Response.Payload); err != nil {
		return false
	}
	p.ackChunk(rctx, agentID, transactionID, 0, logger)
	logger.Debug("upgraded: relaying the connection both ways")

	next := 1
	if pending == nil {
		pending = map[int]*pb.HTTPProxyResponse{}
	}
	for {
		for c, ok := pending[next]; ok; c, ok = pending[next] {
			delete(pending, next)
			if err := write(c.Response.Payload); err != nil {
				return false
			}
			p.ackChunk(rctx, agentID, transactionID, next, logger)
			next++
			if chunking.IsLastChunk(c.Response.Headers) {
				return true
			}
		}
		if len(pending) > chunking.ResponseWindow {
			logger.Warn("upgraded connection: chunks out of order beyond the response window", "pending", len(pending))
			return false
		}
		select {
		case data := <-handle.ResponseCh:
			if session.IsAgentGone(data.Response) {
				return false
			}
			pending[chunking.GetChunkIndex(data.Response.Headers)] = data
		case <-visitorDone:
			return false
		case <-handle.Stalled:
			return false
		case <-rctx.Done():
			return false
		}
	}
}
