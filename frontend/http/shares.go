// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/core/sharesecret"
	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay-core/platform/ratelimit"
	"github.com/nfltr/relay/proto/wire/agentservices"
)

// Shares serves a share's visitor (<code>.<share domain>) in every
// composition: the code's grant (Grants, which keeps it reserved), the
// share's policy (frontend.SharePolicy.Enforce, its credentials checked
// through Verified), and the visitor's budget of misses (Misses), then the
// request goes to the shared agent (or service) at its own path.
type Shares struct {
	Grants   frontend.GrantVerifier
	Verified *sharesecret.VerifiedCache
	// Misses bounds failed lookups and failed policy checks per client IP:
	// a client over its budget is refused before any lookup or
	// verification, so share codes cannot be enumerated (audit F-13).
	// Valid shares cost nothing. Nil: no budget.
	Misses *ratelimit.Limiter
	Log    *slog.Logger
	// Direct names the agent a label that is no share reaches directly
	// (the relay's direct agent subdomains); nil: none.
	Direct func(r *http.Request, label string) (agentID string, ok bool)
	// Path is a whole-agent share's path into its agent and the public
	// prefix the agent is told (the relay's capability aliases; handled:
	// answered already); nil: the path as is.
	Path func(w http.ResponseWriter, r *http.Request, agentID string, policy frontend.SharePolicy, path string) (proxyPath, publicPrefix string, handled bool)
	// Context marks the request's context as the share's (the relay's
	// browse context); nil: none.
	Context func(ctx context.Context, agentID string) context.Context
	// Realm is the realm a Basic Auth share's challenge names; "":
	// frontend.DefaultRealm.
	Realm string
	// RobotsTag is the X-Robots-Tag header a visitor's response carries
	// (e.g. "noindex, nofollow", so search engines do not index what an
	// agent serves); "": none.
	RobotsTag string
}

// Serve answers the visitor of share label code, or hands r to next with
// the agent named (session.AgentIDKey).
func (s *Shares) Serve(w http.ResponseWriter, r *http.Request, next http.Handler, code string) {
	clientIP := clientIP(r)
	if s.Misses != nil && s.Misses.Blocked(clientIP) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}
	g, grantErr := s.grant(code)
	ok := grantErr == nil
	agentID := g.AgentID
	if !ok && s.Direct != nil {
		agentID, ok = s.Direct(r, code)
	}
	if !ok && !errors.Is(grantErr, frontend.ErrNoGrant) {
		// The verifier could not answer: the share may well exist, and the
		// visitor missed nothing, so nothing is charged to its budget.
		s.Log.Warn("share: grant verifier unavailable", "error", grantErr)
		w.Header().Set("Retry-After", "5")
		http.Error(w, "share links are unavailable, try again shortly", http.StatusServiceUnavailable)
		return
	}
	if !ok {
		metrics.RegisterShareCodeValidation(false)
		s.miss(clientIP)
		http.Error(w, "invalid or expired share link", http.StatusUnauthorized)
		return
	}
	// A failed check is charged to the client's budget like a failed
	// lookup: each wrong credential costs an argon2 evaluation, and a
	// client over budget was already refused above.
	realm := s.Realm
	if realm == "" {
		realm = frontend.DefaultRealm
	}
	if !g.Policy.EnforceRealm(w, r, realm, func(sealed, presented string) bool {
		return s.Verified.Matches(code, sealed, presented, g.Policy.ExpiresAt)
	}) {
		s.miss(clientIP)
		return
	}

	requestPath := r.URL.Path
	if requestPath == "" {
		requestPath = "/"
	}
	// A service's share reaches that service only: the relay names it in
	// a header the agent routes by, and removes any copy a client sent.
	r.Header.Del(agentservices.RequestHeader)
	proxyPath, publicPrefix := requestPath, ""
	if g.Service != "" {
		r.Header.Set(agentservices.RequestHeader, g.Service)
	} else if s.Path != nil {
		var handled bool
		if proxyPath, publicPrefix, handled = s.Path(w, r, agentID, g.Policy, requestPath); handled {
			return
		}
	}
	r.URL.Path = proxyPath
	r.Header.Set(session.AgentIDKey, agentID)
	if publicPrefix != "" {
		r.Header.Set(session.BrowsePrefixHeader, publicPrefix)
	} else {
		// No path prefix rewriting for subdomain-based root access:
		// relative links stay on the same subdomain.
		r.Header.Del(session.BrowsePrefixHeader)
	}
	metrics.RegisterShareCodeValidation(true)
	if s.RobotsTag != "" {
		w.Header().Set("X-Robots-Tag", s.RobotsTag)
	}

	ctx := r.Context()
	if s.Context != nil {
		ctx = s.Context(ctx, agentID)
	}
	if publicPrefix != "" {
		ctx = frontend.WithBrowsePrefix(ctx, publicPrefix)
	}
	next.ServeHTTP(w, r.WithContext(ctx))
}

// NewShareVerified is a composition's verified-credential cache: entries
// are small (a digest, the code, a deadline), and a credential re-verifies
// after the TTL.
func NewShareVerified() *sharesecret.VerifiedCache {
	return sharesecret.NewVerifiedCache(4096, 5*time.Minute)
}

// grant is what code grants: frontend.ErrNoGrant for no share (or a
// whole-agent share to an agent that declares services), another error
// when the verifier could not answer.
func (s *Shares) grant(code string) (frontend.Grant, error) {
	if s.Grants == nil || !s.Grants.IsCode(code) {
		return frontend.Grant{}, frontend.ErrNoGrant
	}
	return s.Grants.Grant(code)
}

func (s *Shares) miss(clientIP string) {
	if s.Misses != nil {
		s.Misses.Allow(clientIP)
	}
}

// clientIP is the request's peer address, the address the relay's
// limiters key on (security.ClientIP).
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return net.ParseIP(host).String()
}
