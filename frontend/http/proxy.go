// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/relaystatus"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/logging"
	appconfig "github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay-core/platform/httpbody"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/chunking"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"log/slog"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/internal/platform/txnregistry"
	"github.com/nfltr/relay-core/platform/util"
	"github.com/nfltr/relay/proto/pb"
)

// Proxy forwards HTTP requests to the appropriate connected agent via the
// broker, matching each response by transaction ID.
type Proxy struct {
	logger *slog.Logger
	broker broker.MessageBroker

	txnRegistry *txnregistry.TransactionRegistry[*pb.HTTPProxyResponse]

	// relayCookie names the relay's own cookies, never forwarded.
	relayCookie func(name string) bool

	producers *broker.Producers[string] // agentID

	uploadAcks sync.Map // transaction ID → chan struct{}: acks of its request body's chunks
}

// NewProxy creates a Proxy, starts the transaction-registry reaper, and wires
// the provided broker for agent-bound message delivery.
func NewProxy(ctx context.Context, logger *slog.Logger, msgBroker broker.MessageBroker, relayCookie func(name string) bool) *Proxy {
	txnReg := txnregistry.NewTransactionRegistry[*pb.HTTPProxyResponse]()
	txnReg.StartReaper(ctx, 30*time.Second, logger, func(size int) {
		metrics.SetTxnRegistrySize(size)
	})

	proxy := &Proxy{
		logger:      logger,
		broker:      msgBroker,
		txnRegistry: txnReg,
		relayCookie: relayCookie,

		producers: broker.NewProducers(
			viper.GetInt("http.server.producers.cache_size"),
			viper.GetDuration("http.server.producers.cache_expiry"),
			func(o string, p broker.Producer) {
				logger.Debug("Closing producer", "agent_id", o)
				p.Close()
			},
		),
	}

	return proxy
}

// FanoutResponses decodes, fans out, then acks each message.
// FanoutResponses acks each message then fans out the response.
// Fanout failures (decode error, missing transaction, context cancelled) are
// deterministic — redelivery would fail identically — so we ack eagerly
// before attempting the fanout. This prevents the unbuffered channel send
// inside Fanout from delaying broker acknowledgements under load.
//
// Ack failures are logged but do NOT kill the worker pool. With multiple
// fanout workers a single transient ack failure (broker restart, network
// blip) would otherwise cancel all workers via errgroup and close the
// gRPC stream, causing EOF errors at every connected agent.
func (p *Proxy) FanoutResponses(ctx context.Context, podID string, podIDTopicConsumer broker.Consumer) error {
	consumerCh := podIDTopicConsumer.Chan()
	pool := util.NewWorkerPool(consumerCh, viper.GetInt("http.server.fanout_workers"), func(ctx context.Context, cmsg broker.Message) error {
		now := time.Now()
		// Ack eagerly: fanout failures are deterministic, redelivery won't help.
		ackErr := cmsg.Ack()
		if ackErr != nil {
			metrics.RegisterBrokerAckFailure()
			p.logger.Error("acking message failed (continuing)", "message", cmsg, "error", ackErr)
		}
		fanoutErr := p.fanoutResponse(cmsg, now)
		if fanoutErr != nil {
			if errors.Is(fanoutErr, txnregistry.ErrTransactionNotFound) {
				p.logger.Debug("failed faning out response", "error", fanoutErr)
			} else {
				p.logger.Warn("failed faning out response", "message", cmsg, "error", fanoutErr)
			}
		}
		return nil
	})
	return pool.Run(ctx)
}

func (p *Proxy) fanoutResponse(msg broker.Message, receivedAt time.Time) error {
	response, err := broker.DecodeBrokerMessage(msg, &pb.HTTPProxyResponse{})
	if err != nil {
		metrics.RegisterBrokerMessageDecodeError()
		return err
	}
	unmarshalledAt := time.Now()
	if session.IsUploadAck(response.GetResponse()) {
		// An agent consumed a chunk of an upload: not a response.
		if acks, ok := p.uploadAcks.Load(response.Response.TransactionId); ok {
			select {
			case acks.(chan struct{}) <- struct{}{}:
			default:
			}
		}
		return nil
	}

	// Build logger context upfront to reduce allocations
	logger := logging.With(p.logger,
		"AgentID", response.Routing.AgentId,
		"transaction_id", response.Response.TransactionId,
	)
	logger.Debug("time since message has left dispatcher", "duration", receivedAt.Sub(time.UnixMilli(response.Timestamps.ResponseAtGrpcServer)))
	logger.Debug("unmarshalling response took", "duration", unmarshalledAt.Sub(receivedAt))

	metrics.IncrDequeuedRequests()
	response.Timestamps.ResponseAtHttpServer = receivedAt.UnixMilli()
	transactionID := response.Response.TransactionId

	err = p.txnRegistry.Fanout(transactionID, response, logger)
	if err != nil {
		reason := "context_cancelled"
		switch {
		case errors.Is(err, txnregistry.ErrTransactionNotFound):
			reason = "transaction_not_found"
		case errors.Is(err, txnregistry.ErrTransactionStalled):
			// Its browser took nothing for the bound: the agent stops the
			// response now, not when the stuck handler finally returns.
			reason = "stalled"
			go p.cancelAgentRequest(response.Routing.GetAgentId(), transactionID, logger)
		}
		metrics.RegisterFanoutResponseDrop(reason)
		return err
	}
	metrics.RegisterResponsesReceivedFromAgents()
	logger.Debug("sending response to http handler took", "duration", time.Since(receivedAt))
	return nil
}

// ServeHTTP handles an inbound HTTP request by publishing it to the agent's
// broker topic and streaming the response back to the caller.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestAtDispatcher := time.Now()
	agentID := r.Header.Get(session.AgentIDKey)
	if len(agentID) == 0 {
		p.logger.Error("missing header in request", "header", session.AgentIDKey)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	requestAtProxyHeader := r.Header.Get(session.RequestAtProxyHeader)
	if len(requestAtProxyHeader) == 0 {
		p.logger.Error("missing header in request", "header", session.RequestAtProxyHeader)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	requestAtProxyValue, err := strconv.ParseInt(requestAtProxyHeader, 10, 64)
	if err != nil {
		p.logger.Error("parsing header", "header", session.RequestAtProxyHeader, "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	requestAtProxy := time.UnixMilli(requestAtProxyValue)

	UUID := r.Header.Get(session.TransactionIDHeader)
	if len(UUID) == 0 {
		p.logger.Error("missing header in request", "header", session.TransactionIDHeader)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	logger := logging.With(p.logger, "AgentID", agentID, "transaction_id", UUID)
	logger.Debug("Request took to reach dispatcher", "duration", time.Since(requestAtProxy))
	transactionID := UUID
	defer func() {
		logger.Debug("request took for completion", "duration", time.Since(requestAtDispatcher))
	}()

	logger.Debug("received request", "request", r)

	metrics.RegisterRequestsReceived()

	maxBodySize := viper.GetInt64("http.server.max_request_body_size")
	if maxBodySize > 0 && r.ContentLength > maxBodySize {
		// Refuse before a byte reaches the agent.
		http.Error(w, httpbody.TooLargeMessage(maxBodySize), http.StatusRequestEntityTooLarge)
		return
	}

	beforeRequestPreparation := time.Now()
	timeout := viper.GetDuration("default.request.timeout")
	deadline := time.Now().Add(timeout)
	agentReq, body, err := p.prepareAgentRequest(r, agentID, transactionID, deadline, requestAtProxy)
	if err != nil {
		logger.Error("preparing agent request", "error", err)
		http.Error(w, "Error: while reading request", http.StatusBadRequest)
		return
	}
	afterRequestPreparation := time.Now()
	logger.Debug("request preparation took", "duration", afterRequestPreparation.Sub(beforeRequestPreparation))

	// timeout bounds each wait for the agent (the response to start, or its
	// next chunk), not the whole response: a download that keeps making
	// progress is never cut. The timer is stopped while a chunk is written
	// to the client, whose own stalls the server's write deadline bounds.
	rctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	idle := time.AfterFunc(timeout, cancel)
	defer idle.Stop()
	// finished is set once the agent's whole response was relayed; any
	// other exit tells the agent to stop sending it.
	finished := false
	defer func() {
		if !finished {
			p.cancelAgentRequest(agentID, transactionID, logger)
		}
	}()

	handle, cleanup := p.txnRegistry.Register(transactionID, rctx, logger)
	defer cleanup()

	upgrade := newUpgradeRequest(r)
	defer upgrade.close()

	// Read the settings the publisher needs here: it may still run after
	// ServeHTTP returns, and must not read process configuration then
	// (TestProxyRequestBodyLimitEdge under -race).
	maxMessageSize := appconfig.GRPCMaxMessageSizeBytes()
	publishErrors := make(chan publishError)
	go func() {
		defer close(publishErrors)
		defer body.Close() //nolint:errcheck
		beforeRequestPublish := time.Now()
		defer func() {
			logger.Debug("request publish took", "duration", time.Since(beforeRequestPublish))
		}()
		// Stream the request body directly — never buffer the whole thing.
		// A body that ends in a read error is not complete: the agent must
		// not get its last chunk as if it were (httpbody.Body).
		read := &httpbody.Body{ReadCloser: body}
		var bodyReader io.Reader = read
		if maxBodySize > 0 {
			bodyReader = io.LimitReader(body, maxBodySize+1)
		}
		// Compute per-chunk payload budgets so each gRPC message uses
		// the full 4 MB limit.  The first message carries headers,
		// routing, timestamps, etc., so its payload budget is smaller.
		firstBudget := chunking.PayloadBudgetWithin(agentReq, maxMessageSize)
		restEnvelope := &pb.HTTPProxyRequest{
			Request: &pb.HTTPRequest{
				TransactionId: agentReq.Request.TransactionId,
				Headers:       make(map[string]*pb.HeaderValueList),
			},
			Timestamps: agentReq.Timestamps,
			Routing:    agentReq.Routing,
		}
		chunking.SetChunkHeaders(restEnvelope.Request.Headers, 999, true) // worst-case headers
		restBudget := chunking.PayloadBudgetWithin(restEnvelope, maxMessageSize)

		chunkCh := chunking.ReadChunksWithContext(rctx, bodyReader, firstBudget, restBudget)
		if upgrade != nil { // the request at once, the visitor's bytes once switched
			chunkCh = chunking.ReadUpgradeChunksWithContext(rctx, upgrade.visitor)
		}
		// A body of more than one chunk streams under the upload window:
		// chunk i waits for the ack of chunk i-UploadWindow.
		acks := make(chan struct{}, chunking.UploadWindow)
		p.uploadAcks.Store(transactionID, acks)
		defer p.uploadAcks.Delete(transactionID)
		totalBytes := 0
		index, reqSize, acked := 0, 0, 0
		for chunk := range chunkCh {
			totalBytes += len(chunk.Payload)
			if maxBodySize > 0 && int64(totalBytes) > maxBodySize && upgrade == nil {
				select {
				case <-rctx.Done():
				case publishErrors <- publishError{
					msg:          httpbody.TooLargeMessage(maxBodySize),
					err:          httpbody.ErrTooLarge,
					responseCode: http.StatusRequestEntityTooLarge,
				}:
					logger.Error("request body too large", "bytes_read", totalBytes, "limit", maxBodySize)
					cancel()
				}
				return
			}
			if chunk.Last && read.Failed() != nil {
				select {
				case <-rctx.Done():
				case publishErrors <- publishError{
					msg:          httpbody.Incomplete,
					err:          read.Failed(),
					responseCode: http.StatusBadRequest,
				}:
					logger.Warn("request body ended before it was complete", "bytes_read", totalBytes, "error", read.Failed())
					cancel()
				}
				return
			}
			var msg *pb.HTTPProxyRequest
			switch {
			case index == 0 && chunk.Last:
				// Small body — publish as a single message (most common).
				agentReq.Request.Payload = chunk.Payload
				msg = agentReq
				reqSize = proto.Size(agentReq) // the dispatcher may change it once published
			case index == 0:
				// First chunk of a multi-chunk request.
				agentReq.Request.Payload = chunk.Payload
				chunking.SetChunkHeaders(agentReq.Request.Headers, 0, false)
				msg = agentReq
				reqSize = proto.Size(agentReq)
			default:
				// Subsequent chunk.
				hdrs := make(map[string]*pb.HeaderValueList)
				chunking.SetChunkHeaders(hdrs, index, chunk.Last)
				msg = &pb.HTTPProxyRequest{
					Request: &pb.HTTPRequest{
						TransactionId: agentReq.Request.TransactionId,
						Headers:       hdrs,
						Payload:       chunk.Payload,
					},
					Timestamps: agentReq.Timestamps,
					Routing:    agentReq.Routing,
				}
			}

			if index > 0 || !chunk.Last {
				msg.Request.Headers[chunking.HeaderUploadAcks] = &pb.HeaderValueList{HeaderValues: []string{"1"}}
			}
			if index >= chunking.UploadWindow {
				select {
				case <-acks:
					acked++
				case <-rctx.Done(): // the idle timer bounds a stalled agent
					return
				}
			}
			if err := p.publishRequest(rctx, agentID, msg); err != nil {
				select {
				case <-rctx.Done():
				case publishErrors <- publishError{
					msg:          "Agent not connected",
					err:          err,
					responseCode: http.StatusServiceUnavailable,
				}:
					logger.Error("publishing request to agent", "error", err)
					cancel()
				}
				return
			}
			if upgrade == nil {
				idle.Reset(timeout) // the upload is making progress
			}
			index++
		}
		// Empty body (GET, DELETE, HEAD, etc.) — ReadChunks produces zero
		// chunks, so publish the original request as-is.
		if index == 0 {
			reqSize = proto.Size(agentReq)
			if err := p.publishRequest(rctx, agentID, agentReq); err != nil {
				select {
				case <-rctx.Done():
				case publishErrors <- publishError{
					msg:          "Agent not connected",
					err:          err,
					responseCode: http.StatusServiceUnavailable,
				}:
					logger.Error("publishing request to agent", "error", err)
					cancel()
				}
				return
			}
		}
		if index > 1 {
			logger.Debug("published chunked request", "chunks", index, "total_bytes", totalBytes)
		}
		metrics.RegisterRPCRequestPayloadSize(r.Method, float64(reqSize))
		metrics.IncrQueuedRequests()
		// The chunks still in flight reach a slow backend after the last
		// one was published: each ack is the upload making progress
		// (TestUploadDrainToSlowBackendIsProgress).
		for ; upgrade == nil && index > 1 && acked < index; acked++ {
			select {
			case <-acks:
				idle.Reset(timeout)
			case <-rctx.Done():
				return
			}
		}
	}()

	headersSent := false
	// pending buffers out-of-order chunks keyed by X-Chunk-Index.
	// With fanout_workers > 1, chunks for the same transaction may
	// arrive out of order in handle.ResponseCh. We reorder them here
	// using the chunk index so the HTTP response body is always correct.
	// maxPending caps the reorder buffer at 64 entries (~256 MB with 4 MB
	// chunks) to prevent unbounded memory growth if chunk 0 is severely
	// delayed.
	const maxPending = 64
	var pending map[int]*pb.HTTPProxyResponse
	for {
		select {
		case data := <-handle.ResponseCh:
			logger.Debug("response received")
			if session.IsAgentGone(data.Response) {
				metrics.RegisterRequestsFailedDueToAgentNotConnected()
				logger.Warn("the agent's session ended before its response")
				_ = copyMetadataHeaderToResponse(w, agentID, requestAtDispatcher.UnixMilli(), time.Now().UnixMilli(), nil)
				http.Error(w, "Agent not connected", http.StatusBadGateway)
				return
			}

			isChunked := chunking.IsChunked(data.Response.Headers)

			// Non-chunked single-message response: StatusCode is always
			// present on the only message in the stream.
			if !isChunked {
				if data.Response.StatusCode == 0 {
					logger.Warn("received response with zero status code, returning bad gateway")
					if !headersSent {
						http.Error(w, "bad gateway", http.StatusBadGateway)
					}
					return
				}

				util.CopyProtoHeadersToHTTPHeaders(data.Response.Headers, w.Header())
				if browsePrefix := frontend.BrowsePrefix(r.Context()); browsePrefix != "" {
					rewriteLocationHeader(w.Header(), browsePrefix)
				}
				err := copyMetadataHeaderToResponse(w, agentID, requestAtDispatcher.UnixMilli(), time.Now().UnixMilli(), data.Timestamps)
				if err != nil {
					logger.Error("copying metadata header to response", "error", err)
				}
				if w.Header().Get("Content-Length") == "" { // the agent's own length stands (HEAD)
					w.Header().Set("Content-Length", strconv.Itoa(len(data.Response.Payload)))
				}
				w.WriteHeader(int(data.Response.StatusCode))
				idle.Stop()
				totalBytes, _ := w.Write(data.Response.Payload)
				finished = true
				_ = http.NewResponseController(w).Flush()
				logger.Debug("response written to http",
					"status", data.Response.StatusCode,
					"bytes", totalBytes)
				metrics.RegisterRPCResponsePayloadSize(r.Method, strconv.Itoa(int(data.Response.StatusCode)), float64(totalBytes))
				return
			}

			// Chunked response: this message may not be chunk 0 when
			// fanout_workers > 1. Buffer non-zero chunks until chunk 0
			// arrives so we can write HTTP status and headers first.
			idx := chunking.GetChunkIndex(data.Response.Headers)
			if idx != 0 {
				if pending == nil {
					pending = make(map[int]*pb.HTTPProxyResponse)
				}
				if len(pending) >= maxPending {
					logger.Error("reorder buffer overflow, aborting chunked response",
						"pending", len(pending), "max", maxPending)
					if !headersSent {
						http.Error(w, "response too large to reorder", http.StatusBadGateway)
					}
					return
				}
				pending[idx] = data
				continue // keep reading for chunk 0
			}

			// ── chunk 0: write HTTP headers and status ──
			if data.Response.StatusCode == 0 {
				logger.Warn("received response with zero status code, returning bad gateway")
				if !headersSent {
					http.Error(w, "bad gateway", http.StatusBadGateway)
				}
				return
			}
			chunking.StripHeaders(data.Response.Headers)
			util.CopyProtoHeadersToHTTPHeaders(data.Response.Headers, w.Header())
			if browsePrefix := frontend.BrowsePrefix(r.Context()); browsePrefix != "" {
				rewriteLocationHeader(w.Header(), browsePrefix)
			}
			err := copyMetadataHeaderToResponse(w, agentID, requestAtDispatcher.UnixMilli(), time.Now().UnixMilli(), data.Timestamps)
			if err != nil {
				logger.Error("copying metadata header to response", "error", err)
			}
			if upgrade != nil && data.Response.StatusCode == http.StatusSwitchingProtocols {
				idle.Stop() // an upgraded connection lasts as long as both sides keep it
				finished = p.bridge(w, r, upgrade, data, pending, handle, rctx, publishErrors, timeout, agentID, transactionID, logger)
				return
			}
			w.WriteHeader(int(data.Response.StatusCode))
			headersSent = true
			idle.Stop()
			totalBytes, _ := w.Write(data.Response.Payload)
			idle.Reset(timeout)
			p.ackChunk(rctx, agentID, transactionID, 0, logger)

			// If chunk 0 is also the last, we're done.
			if chunking.IsLastChunk(data.Response.Headers) {
				finished = true
				_ = http.NewResponseController(w).Flush()
				logger.Debug("chunked response complete (single chunk)",
					"status", data.Response.StatusCode,
					"bytes", totalBytes)
				metrics.RegisterRPCResponsePayloadSize(r.Method, strconv.Itoa(int(data.Response.StatusCode)), float64(totalBytes))
				return
			}

			// Stream remaining chunks with reordering support. A streamed
			// response is long-lived: it ends when the key it was asked with
			// is revoked or expires (held_requests.go).
			logger.Debug("streaming chunked response")
			streamCtx := rctx
			if h, ok := identity.HoldCarried(rctx); ok {
				defer h.Release()
				streamCtx = h
			}
			// Through http.ResponseController: a writer the relay wraps w in
			// may only unwrap (TestProxyStreamedHeadReachesClientBeforeBody).
			flusher := http.NewResponseController(w)
			_ = flusher.Flush()
			chunks := 1
			nextIdx := 1

			// Flush any chunks that arrived before chunk 0 and are
			// now ready to write in sequence.
			for {
				buffered, ok := pending[nextIdx]
				if !ok {
					break
				}
				idle.Stop()
				n, writeErr := w.Write(buffered.Response.Payload)
				idle.Reset(timeout)
				totalBytes += n
				chunks++
				delete(pending, nextIdx)
				if writeErr != nil {
					logger.Error("writing buffered chunk to browser", "chunk", chunks, "error", writeErr)
					return
				}
				p.ackChunk(rctx, agentID, transactionID, nextIdx, logger)
				_ = flusher.Flush()
				if chunking.IsLastChunk(buffered.Response.Headers) {
					finished = true
					logger.Debug("chunked response complete",
						"status", data.Response.StatusCode,
						"chunks", chunks,
						"bytes", totalBytes)
					metrics.RegisterRPCResponsePayloadSize(r.Method, strconv.Itoa(int(data.Response.StatusCode)), float64(totalBytes))
					return
				}
				nextIdx++
			}

			for {
				select {
				case chunk := <-handle.ResponseCh:
					if session.IsAgentGone(chunk.Response) {
						logger.Warn("chunked response cut: the agent's session ended", "received", chunks, "bytes", totalBytes)
						panic(http.ErrAbortHandler) // cut, never a clean end of a truncated body
					}
					chunkIdx := chunking.GetChunkIndex(chunk.Response.Headers)
					if chunkIdx != nextIdx {
						// Out of order — buffer for later.
						if pending == nil {
							pending = make(map[int]*pb.HTTPProxyResponse)
						}
						if len(pending) >= maxPending {
							logger.Error("reorder buffer overflow during streaming",
								"pending", len(pending), "max", maxPending)
							panic(http.ErrAbortHandler) // cut, never a clean end of a truncated body
						}
						pending[chunkIdx] = chunk
						continue
					}
					// In order — write immediately.
					idle.Stop()
					n, writeErr := w.Write(chunk.Response.Payload)
					idle.Reset(timeout)
					totalBytes += n
					chunks++
					if writeErr != nil {
						logger.Error("writing chunk to browser", "chunk", chunks, "error", writeErr)
						return
					}
					p.ackChunk(rctx, agentID, transactionID, nextIdx, logger)
					_ = flusher.Flush()
					isLast := chunking.IsLastChunk(chunk.Response.Headers)
					nextIdx++

					// Drain any buffered chunks that are now consecutive.
					for {
						buffered, ok := pending[nextIdx]
						if !ok {
							break
						}
						idle.Stop()
						n, writeErr = w.Write(buffered.Response.Payload)
						idle.Reset(timeout)
						totalBytes += n
						chunks++
						delete(pending, nextIdx)
						if writeErr != nil {
							logger.Error("writing buffered chunk to browser", "chunk", chunks, "error", writeErr)
							return
						}
						p.ackChunk(rctx, agentID, transactionID, nextIdx, logger)
						_ = flusher.Flush()
						if chunking.IsLastChunk(buffered.Response.Headers) {
							isLast = true
						}
						nextIdx++
					}

					if isLast {
						finished = true
						logger.Debug("chunked response complete",
							"status", data.Response.StatusCode,
							"chunks", chunks,
							"bytes", totalBytes)
						metrics.RegisterRPCResponsePayloadSize(r.Method, strconv.Itoa(int(data.Response.StatusCode)), float64(totalBytes))
						return
					}
				case <-streamCtx.Done():
					logger.Warn("chunked response ended early: client gone, agent idle or key revoked", "received", chunks, "bytes", totalBytes, "cause", context.Cause(streamCtx))
					panic(http.ErrAbortHandler) // cut, never a clean end of a truncated body
				case <-handle.Stalled:
					logger.Warn("chunked response refused: the client took none of it for the stall bound", "received", chunks, "bytes", totalBytes)
					panic(http.ErrAbortHandler) // cut, never a clean end of a truncated body
				}
			}
		case <-handle.Stalled:
			logger.Warn("response refused: the client took none of it for the stall bound")
			if !headersSent {
				http.Error(w, "response refused: the client took none of it", http.StatusBadGateway)
				return
			}
			panic(http.ErrAbortHandler) // cut, never a clean end of a truncated body
		case publishErr, ok := <-publishErrors:
			if !ok {
				publishErrors = nil
				continue
			}
			if publishErr.responseCode == http.StatusServiceUnavailable {
				metrics.RegisterRequestsFailedDueToAgentNotConnected()
			}
			logger.Error("processing error", "error", publishErr.err)
			// finished stays false: the agent may hold part of the body, and
			// the deferred cancel stops its local request.
			if !headersSent {
				err := copyMetadataHeaderToResponse(w, agentID, requestAtDispatcher.UnixMilli(), time.Now().UnixMilli(), nil)
				if err != nil {
					logger.Error("copying metadata header to response", "error", err)
				}
				if publishErr.responseCode == http.StatusServiceUnavailable {
					// Not delivered: the agent's session here ended (or is
					// ending). Typed, so a client tells it from the agent's
					// own 503.
					relaystatus.WriteHTTP(w, http.StatusServiceUnavailable, codes.Unavailable, relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, publishErr.msg)
				} else {
					http.Error(w, publishErr.msg, publishErr.responseCode)
				}
			}
			return
		case <-rctx.Done():
			metrics.RegisterTimeoutRequests()
			logger.Warn("request timed out")
			if !headersSent {
				err := copyMetadataHeaderToResponse(w, agentID, requestAtDispatcher.UnixMilli(), time.Now().UnixMilli(), nil)
				if err != nil {
					logger.Error("copying metadata header to response", "error", err)
				}
				http.Error(w, "Request timed out", http.StatusGatewayTimeout)
			}
			return
		}
	}
}

// publishError is the request goroutine's failure, answered to the client.
type publishError struct {
	msg          string
	err          error
	responseCode int
}

func copyMetadataHeaderToResponse(w http.ResponseWriter, agentID string, requestAtDispatcher, responseAtDispatcher int64, timestamps *pb.Timestamps) error {
	m := Metadata{AgentID: agentID, RequestAtDispatcher: requestAtDispatcher, ResponseAtDispatcher: responseAtDispatcher}
	if timestamps != nil {
		m.RequestAtAgent = timestamps.RequestAtAgent
		m.ResponseAtAgent = timestamps.ResponseAtAgent
		m.RequestAtGRPCServer = timestamps.RequestAtGrpcServer
		m.ResponseAtGRPCServer = timestamps.ResponseAtGrpcServer
		m.RequestAtHTTPServer = timestamps.RequestAtHttpServer
		m.ResponseAtHTTPServer = timestamps.ResponseAtHttpServer
	}
	var buf [192]byte
	b, err := m.appendJSON(buf[:0])
	if err != nil {
		return err
	}
	w.Header().Set(session.MetadataHeader, string(b))
	return nil
}

// appendJSON appends json.Marshal(m)'s bytes to b: encoding/json's
// reflection cost every response 4 allocations (docs/performance.md R6;
// TestMetadataAppendJSONIsMarshal). An agent ID with a byte that JSON
// escapes goes through json.Marshal.
func (m *Metadata) appendJSON(b []byte) ([]byte, error) {
	b = append(b, `{"a":`...)
	plain := true
	for i := 0; i < len(m.AgentID) && plain; i++ {
		c := m.AgentID[i]
		plain = c >= 0x20 && c < 0x7f && c != '"' && c != '\\' && c != '<' && c != '>' && c != '&'
	}
	if plain {
		b = append(append(append(b, '"'), m.AgentID...), '"')
	} else {
		q, err := json.Marshal(m.AgentID)
		if err != nil {
			return nil, err
		}
		b = append(b, q...)
	}
	for _, f := range [...]struct {
		key string
		v   int64
	}{
		{`,"j":`, m.RequestAtHTTPServer}, {`,"k":`, m.RequestAtDispatcher}, {`,"l":`, m.RequestAtGRPCServer},
		{`,"m":`, m.RequestAtAgent}, {`,"n":`, m.ResponseAtAgent}, {`,"o":`, m.ResponseAtGRPCServer},
		{`,"p":`, m.ResponseAtDispatcher}, {`,"q":`, m.ResponseAtHTTPServer},
	} {
		b = strconv.AppendInt(append(b, f.key...), f.v, 10)
	}
	return append(b, '}'), nil
}

// ackChunk tells the agent chunk index of transactionID reached the client,
// which lets it send one more (chunking.ResponseWindow).
func (p *Proxy) ackChunk(ctx context.Context, agentID, transactionID string, index int, logger *slog.Logger) {
	if err := p.publishRequest(ctx, agentID, chunking.AckRequest(agentID, transactionID, index)); err != nil {
		logger.Debug("ack frame not delivered", "chunk", index, "error", err)
	}
}

// cancelAgentRequest tells the agent to stop sending transactionID's
// response: its caller is gone. Best effort; the request context is already
// done, so it gets a short context of its own.
func (p *Proxy) cancelAgentRequest(agentID, transactionID string, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.publishRequest(ctx, agentID, chunking.CancelRequest(agentID, transactionID)); err != nil {
		logger.Debug("cancel frame not delivered", "error", err)
	}
}

func (p *Proxy) publishRequest(rctx context.Context, agentID string, agentReq *pb.HTTPProxyRequest) error {
	logger := logging.With(p.logger, "AgentID", agentID, "transaction_id", agentReq.Request.TransactionId)
	now := time.Now()
	producer, release, err := p.producers.Get(agentID, p.broker, broker.ProducerOptions{
		Topic: agentID,
	})
	if err != nil {
		return fmt.Errorf("getting producer for agentID: %s: %w", agentID, err)
	}
	defer release()
	logger.Debug("publishRequest - getting producer took", "duration", time.Since(now))
	now = time.Now()
	_, err = producer.SendProto(rctx, agentReq)
	if err != nil {
		return fmt.Errorf("sending to agent for transactionID: %s: %w", agentReq.Request.TransactionId, err)
	}
	logger.Debug("publishRequest - publishing took", "duration", time.Since(now))
	return nil
}

// prepareAgentRequest builds the proto request envelope without reading the
// body. The caller streams r.Body directly through ReadChunks so the full
// payload is never buffered in memory. The returned io.ReadCloser is the
// request body; the caller must close it when done.
func (p *Proxy) prepareAgentRequest(r *http.Request, agentID string, transactionID string, deadline, requestAtProxyHTTPServer time.Time) (*pb.HTTPProxyRequest, io.ReadCloser, error) {
	agentURL := strings.TrimPrefix(r.URL.String(), "/")
	req := &pb.HTTPProxyRequest{
		Request: &pb.HTTPRequest{
			Deadline:      deadline.UnixMilli(),
			Headers:       util.HeadersToProto(withoutRelayCookies(r.Header, p.relayCookie), SkipHeaders),
			Method:        r.Method,
			Payload:       nil,
			TransactionId: transactionID,
			Url:           agentURL,
		},
		Timestamps: &pb.Timestamps{
			RequestAtHttpServer: requestAtProxyHTTPServer.UnixMilli(),
		},
		Routing: &pb.RoutingInfo{
			AgentId: agentID,
		},
	}

	return req, r.Body, req.Validate()
}

// rewriteLocationHeader prefixes absolute-path Location values with the
// browse prefix so that redirect responses stay within the /browse/<agentID>/
// namespace.  Only absolute paths (starting with /) are rewritten; fully
// qualified URLs and relative references are left as-is.
func rewriteLocationHeader(h http.Header, browsePrefix string) {
	loc := h.Get("Location")
	if loc == "" {
		return
	}
	// Only rewrite absolute-path references (e.g. "/page").
	// Skip fully-qualified URLs ("http://...") and relative references.
	if len(loc) > 0 && loc[0] == '/' && !strings.HasPrefix(loc, browsePrefix) {
		h.Set("Location", browsePrefix+loc)
	}
}
