// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/relaystatus"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/logging"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"golang.org/x/net/http/httpguts"
	"google.golang.org/grpc/codes"
)

// agentOfflineMessage is the 503 body for a known agent that is not
// connected, e.g. a share whose machine stopped sharing. Visitors see it, so
// it names no relay internals.
const agentOfflineMessage = "The machine behind this link is offline right now. Try again later."

// directoryUnavailableMessage is the 503 body when the relay cannot read
// where the agent is (its directory failed): the relay's fault.
const directoryUnavailableMessage = "The relay cannot route this request right now. Try again shortly."

// dispatch serves a request for the agent session.AgentIDKey names: in
// process when the agent is connected here (on an HTTP session, else as
// ConnectHTTP messages), over the replica link to the replica holding it,
// else 503 for a registered endpoint and 404 for an unknown one.
func (ing *Ingress) dispatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	UUID := r.Header.Get(session.TransactionIDHeader)
	logger := logging.With(ing.logger, "type", "proxy", "transaction_id", UUID)

	agentID := r.Header.Get(session.AgentIDKey)
	if len(agentID) == 0 {
		logger.Error("missing header in request", "header", session.AgentIDKey)
		http.Error(w, fmt.Sprintf("Error: Missing %s header", session.AgentIDKey), http.StatusBadRequest)
		return
	}
	dispatcherURL, err := ing.dispatcherURL(r.Context(), agentID)
	// Always advertise the relay pod that handled this request so
	// cross-pod routing failures are debuggable from a curl response.
	// In a multi-pod setup the same agent may route to a different
	// pod next request — having the pod_id on every reply means an
	// operator can correlate a flapping 503 to a specific pod
	// without pulling logs.
	podID := strings.TrimSpace(ing.core.PodID)
	if podID != "" {
		w.Header().Set("X-Relay-Pod", podID)
	}
	if errors.Is(err, directory.ErrUnavailable) {
		// Whether the agent is connected anywhere is unknown: the relay's
		// own failure, not the agent's absence.
		logger.Error("directory unavailable: request not routed", "agent_id", agentID, "error", err)
		w.Header().Set("Retry-After", "5")
		relaystatus.WriteHTTP(w, http.StatusServiceUnavailable, codes.Unavailable, relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE, directoryUnavailableMessage)
		return
	}
	if dispatcherURL == nil || err != nil {
		logger.Error("failed to get dispatcher proxy URL", "agent_id", agentID, "error", err)
		// If the agentID is pre-registered in the endpoint registry the agent is
		// known but temporarily disconnected — return 503 with Retry-After so the
		// caller knows to retry.  Unknown agentIDs continue to return 404.
		if ing.registered(r.Context(), agentID) {
			w.Header().Set("Retry-After", "30")
			metrics.RegisterEndpointRegistry503()
			// Share visitors read this body; operators have X-Relay-Pod.
			http.Error(w, agentOfflineMessage, http.StatusServiceUnavailable)
		} else {
			metrics.IncrEndpointRegistry404()
			msg := "agent not found"
			if podID != "" {
				msg = fmt.Sprintf("agent %q is not registered with this relay (handled by pod %q — check the AgentID, generate an API key, and start nfltr mcp)", agentID, podID)
			}
			http.Error(w, msg, http.StatusNotFound)
		}
		return
	}
	// On the success path, when the dispatcher resolved to a peer
	// pod (cross-pod forward), advertise both source and target so
	// the response trace shows the path. Loopback (same-pod) is
	// the common case and gets only X-Relay-Pod above.
	if dispatcherURL != ing.loopbackURL {
		if host := strings.TrimSpace(dispatcherURL.Host); host != "" {
			w.Header().Set("X-Relay-Pod-Target", host)
		}
	}
	logger.Debug("dispatching request", "agent_id", agentID, "dispatcher_url", dispatcherURL)
	if dispatcherURL == ing.loopbackURL {
		// The agent is connected to this process: hand the request to
		// the dispatcher's handler in process (no HTTP hop). The headers it
		// sees are the ones a peer's forward over the replica link sends
		// (httputil.ReverseProxy strips hop-by-hop headers). An agent
		// that serves HTTP sessions gets it on one (agent_http_link.go).
		if ing.link != nil && ing.core.TCPDispatcher.ServesHTTP(agentID) {
			metrics.RecordAgentHTTPRequest("link")
			ing.link.ServeHTTP(metrics.CountHTTP(w, r, metrics.PathHTTPLink, time.Now()), r)
			return
		}
		metrics.RecordAgentHTTPRequest("grpc")
		stripHopByHopHeaders(r.Header)
		ing.proxy.ServeHTTP(metrics.CountHTTP(w, r, metrics.PathHTTPGRPC, time.Now()), r)
		return
	}
	proxy := ing.peers.Get(dispatcherURL)
	logger.Debug("proxy lookup completed", "duration", time.Since(start))

	// If the request came through /browse/<agentID>/, wrap the
	// ResponseWriter so that Location headers in redirect responses
	// are rewritten to stay within the browse namespace.
	// The browse prefix is in the request context (the relay's routing)
	// and does not survive the httputil.ReverseProxy hop, so we intercept
	// it here before the proxy creates its own outgoing request.
	if browsePrefix := frontend.BrowsePrefix(r.Context()); browsePrefix != "" {
		w = &browseResponseWriter{ResponseWriter: w, browsePrefix: browsePrefix}
	}

	proxy.ServeHTTP(w, r)
}

// dispatcherURL is where agentID's request goes: loopbackURL when the agent
// is connected here, else the replica holding it, named by its pod IP and
// replica link port (the peer reverse proxies' link dialer carries it).
func (ing *Ingress) dispatcherURL(ctx context.Context, agentID string) (*url.URL, error) {
	logger := logging.With(ing.logger, "AgentID", agentID)
	if ing.core.Dispatcher.IsAgentConnected(agentID) {
		// agent is connected to current pod: served in process (dispatch)
		logger.Debug("agent is connected, serving in process", "agent_id", agentID)
		return ing.loopbackURL, nil
	}
	podIP, err := ing.core.DispatcherIPInventory.GetIP(ctx, agentID)
	if err != nil {
		return nil, err
	}
	dispatcherURL, err := url.Parse(fmt.Sprintf("http://%s:%s", podIP, ing.peerLinkPort))
	if err != nil {
		return nil, err
	}
	logger.Debug("dispatcher URL resolved", "url", dispatcherURL.String())
	return dispatcherURL, nil
}

// hopByHopHeaders are the headers httputil.ReverseProxy removes from a
// request it forwards (RFC 9110 7.6.1).
var hopByHopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

// stripHopByHopHeaders rewrites h as httputil.ReverseProxy rewrites the
// request it forwards: the headers Connection names and the hop-by-hop
// headers go, a protocol upgrade and "Te: trailers" stay.
func stripHopByHopHeaders(h http.Header) {
	upgrade := ""
	if httpguts.HeaderValuesContainsToken(h["Connection"], "Upgrade") {
		upgrade = h.Get("Upgrade")
	}
	trailers := httpguts.HeaderValuesContainsToken(h["Te"], "trailers")
	for _, f := range h["Connection"] {
		for sf := range strings.SplitSeq(f, ",") {
			if sf = textproto.TrimString(sf); sf != "" {
				h.Del(sf)
			}
		}
	}
	for _, k := range hopByHopHeaders {
		h.Del(k)
	}
	if trailers {
		h.Set("Te", "trailers")
	}
	if upgrade != "" {
		h.Set("Connection", "Upgrade")
		h.Set("Upgrade", upgrade)
	}
}

// browseResponseWriter wraps an http.ResponseWriter to rewrite Location
// headers in redirect responses so that they stay within the
// /browse/<agentID>/ namespace.
type browseResponseWriter struct {
	http.ResponseWriter
	browsePrefix string
}

func (bw *browseResponseWriter) WriteHeader(code int) {
	rewriteLocationHeader(bw.Header(), bw.browsePrefix)
	bw.ResponseWriter.WriteHeader(code)
}

// Flush implements http.Flusher — required for streaming responses
// through httputil.ReverseProxy.
func (bw *browseResponseWriter) Flush() {
	_ = http.NewResponseController(bw.ResponseWriter).Flush() // also through a writer that only unwraps
}

// Unwrap lets http.ResponseController reach the server's writer (the
// reverse proxy takes a protocol upgrade's connection over through it).
func (bw *browseResponseWriter) Unwrap() http.ResponseWriter { return bw.ResponseWriter }
