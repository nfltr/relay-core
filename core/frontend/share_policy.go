// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package frontend

import (
	"net"
	"net/http"
	"strconv"
	"strings"
)

// DefaultRealm is the realm Enforce names in its Basic Auth challenge.
const DefaultRealm = "relay"

// Enforce checks all restrictions in p against the request.
// Checks are applied in order: IP → required headers → bearer token → Basic Auth.
// Returns true if access is permitted, false if the response has already been
// written with an appropriate error status. Its Basic Auth challenge names
// DefaultRealm.
func (policy SharePolicy) Enforce(w http.ResponseWriter, r *http.Request, matches func(sealed, presented string) bool) bool {
	return policy.EnforceRealm(w, r, DefaultRealm, matches)
}

// EnforceRealm is Enforce whose Basic Auth challenge names realm.
func (policy SharePolicy) EnforceRealm(w http.ResponseWriter, r *http.Request, realm string, matches func(sealed, presented string) bool) bool {
	// ― IP allowlist (silent 403 — no information leak) ―
	if len(policy.AllowedIPs) > 0 {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		if !policy.Admits(net.ParseIP(host)) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return false
		}
	}

	// ― Required Headers (silent 403 — checked before auth prompts) ―
	// Secrets in a stored policy are sealed (sharesecret, at issuance).
	for name, sealed := range policy.RequiredHeaders {
		if !matches(sealed, r.Header.Get(name)) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return false
		}
	}

	// ― Bearer Token (401 — API/curl clients) ―
	if policy.BearerToken != "" {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return false
		}
		if !matches(policy.BearerToken, strings.TrimPrefix(auth, "Bearer ")) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return false
		}
	}

	// ― Basic Auth (401 with WWW-Authenticate — browser-compatible) ―
	if policy.BasicAuth != "" {
		challenge := "Basic realm=" + strconv.Quote(realm)
		user, pass, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", challenge)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return false
		}
		if !matches(policy.BasicAuth, user+":"+pass) {
			w.Header().Set("WWW-Authenticate", challenge)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return false
		}
	}

	// The share's credential is the relay's, checked here: it goes no
	// further, as the relay's own cookies and keys do not.
	if policy.BearerToken != "" || policy.BasicAuth != "" {
		r.Header.Del("Authorization")
	}
	return true
}
