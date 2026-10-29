// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"
	"uuid"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay-core/platform/util"
	"github.com/spf13/viper"
)

// Metadata carries timing information across the HTTP proxy pipeline.
type Metadata struct {
	AgentID string `json:"a"`

	RequestAtHTTPServer  int64 `json:"j"` // request at proxy http server
	RequestAtDispatcher  int64 `json:"k"` // request at dispatcher http server
	RequestAtGRPCServer  int64 `json:"l"` // request at dispatcher grpc server
	RequestAtAgent       int64 `json:"m"` // request at agent
	ResponseAtAgent      int64 `json:"n"` // response at agent
	ResponseAtGRPCServer int64 `json:"o"` // response at dispatcher grpc server
	ResponseAtDispatcher int64 `json:"p"` // response at dispatcher http server
	ResponseAtHTTPServer int64 `json:"q"` // response at proxy http server
}

// front starts a request to an agent: its transaction ID and arrival time,
// no caching of agent content, and the latency metrics once answered.
func (ing *Ingress) front(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Agent content is never cached, except a site host's: public
		// static pages whose agent sets their caching.
		if !frontend.SiteRequest(r.Context()) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("CDN-Cache-Control", "no-store")
		}

		setForwardedHeaders(r)
		start := time.Now()
		UUID := uuid.New().String()
		r.Header.Set(session.TransactionIDHeader, UUID)
		rw := util.NewResponseWriter(w, frontRemovedHeaders)
		r.Header.Set(session.RequestAtProxyHeader, strconv.FormatInt(start.UnixMilli(), 10))

		next.ServeHTTP(rw, r)
		finish := time.Now()
		elapsed := finish.Sub(start)
		ing.logger.Debug("request completed", "type", "proxy", "transaction_id", UUID, "elapsed", elapsed)
		statusCode := rw.GetStatusCode()
		status := strconv.Itoa(statusCode)
		metrics.RegisterRequestsTotal(r.Method, status, apiType(r.URL.Path, ing.apiTypes))
		metadata := rw.GetRemovedHeaders().Get(session.MetadataHeader)
		if metadata == "" {
			// No agent answered (a share refused or rate limited, an agent
			// offline or unreached): counted above with its status.
			ing.logger.Debug("request not answered by an agent", "type", "proxy", "transaction_id", UUID, "status_code", statusCode)
			return
		}
		var m Metadata
		if err := json.Unmarshal([]byte(metadata), &m); err != nil {
			ing.logger.Error("failed to parse metadata", "type", "proxy", "transaction_id", UUID, "error", err)
			return
		}
		if statusCode == http.StatusGatewayTimeout || statusCode == http.StatusServiceUnavailable {
			ing.logger.Info("request failed", "AgentID", m.AgentID, "type", "proxy", "transaction_id", UUID, "metadata", metadata, "status_code", statusCode)
			return
		}
		latencyIntroduced := (m.RequestAtAgent - m.RequestAtHTTPServer) + (m.ResponseAtHTTPServer - m.ResponseAtAgent)

		metrics.RegisterLatencyIntroduced(r.Method, status, float64(latencyIntroduced))
		metrics.RegisterRequestDuration(r.Method, status, float64(elapsed.Milliseconds()))

		if ing.logger.Enabled(r.Context(), slog.LevelDebug) {
			logger := ing.logger.With("AgentID", m.AgentID, "type", "proxy", "transaction_id", UUID)
			logger.Debug("request reached dispatcher", "latency_ms", m.RequestAtDispatcher-start.UnixMilli())
			logger.Debug("response from dispatcher", "latency", finish.Sub(time.UnixMilli(m.ResponseAtDispatcher)))
			logger.Debug("total latency introduced", "latency_ms", latencyIntroduced)
		}
	})
}

// clientProxyHeaders are what a proxy tells its backend about the client.
// The relay is the visitor's first hop (nothing sits in front of its
// listeners), so a visitor's own copies are
// claims (another host, another address) and never reach the agent.
var clientProxyHeaders = []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-Ip"}

// setForwardedHeaders gives the agent's service what a reverse proxy at
// the edge forwards: the host the visitor asked for, the scheme it used and
// its address. The agent keeps them (and Origin, Referer) and by default
// sends X-Forwarded-Host as Host. Only the replica the visitor reached runs
// it: a peer replica's forward keeps them (peers.go).
func setForwardedHeaders(r *http.Request) {
	for _, k := range clientProxyHeaders {
		r.Header.Del(k)
	}
	// One backing array for the three values, an address validated without
	// an allocation: front's budget (TestFrontAllocBudget).
	v := make([]string, 3)
	v[0], v[1] = r.Host, "http"
	if r.TLS != nil {
		v[1] = "https"
	}
	r.Header["X-Forwarded-Host"], r.Header["X-Forwarded-Proto"] = v[0:1:1], v[1:2:2]
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if _, err := netip.ParseAddr(host); err == nil {
		v[2] = host
		r.Header["X-Forwarded-For"] = v[2:3:3]
	}
}

// frontRemovedHeaders are the response headers front takes off (one
// slice for every request: a literal per call was an allocation).
var frontRemovedHeaders = []string{session.MetadataHeader}

// GetAPIType returns the API type from the request URL path.
// The recognized prefixes are configurable via --http.api_type_prefixes (default: wapi,ibap).
func GetAPIType(r *http.Request) string {
	return apiType(r.URL.Path, viper.GetStringSlice("http.api_type_prefixes"))
}

// apiType is the prefix of prefixes that is path's first segment, else
// "other". front passes Ingress.apiTypes, read once: a viper read and a
// split of the path per request were 6 of front's allocations
// (docs/performance.md R5).
func apiType(path string, prefixes []string) string {
	_, rest, ok := strings.Cut(path, "/")
	if !ok {
		return "other"
	}
	first, _, _ := strings.Cut(rest, "/")
	for _, prefix := range prefixes {
		if first == prefix {
			return prefix
		}
	}
	return "other"
}
