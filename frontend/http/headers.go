// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"net/http"
	"net/textproto"
	"strings"

	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/platform/util"
	"github.com/nfltr/relay/proto/wire/chunking"
)

// SkipHeaders is a map of headers that should not be forwarded to the agent
var SkipHeaders = map[string]util.SkipHeaderIf{
	textproto.CanonicalMIMEHeaderKey(session.AgentIDKey):           util.SkipHeaderAlways,
	textproto.CanonicalMIMEHeaderKey(session.TransactionIDHeader):  util.SkipHeaderAlways,
	textproto.CanonicalMIMEHeaderKey(session.RequestAtProxyHeader): util.SkipHeaderAlways,
	// The caller's relay credential is for the relay, never for the agent.
	textproto.CanonicalMIMEHeaderKey("X-API-Key"): util.SkipHeaderAlways,
	// Only the relay sends flow-control frames; a client cannot forge one.
	textproto.CanonicalMIMEHeaderKey(chunking.HeaderCancel):     util.SkipHeaderAlways,
	textproto.CanonicalMIMEHeaderKey(chunking.HeaderAck):        util.SkipHeaderAlways,
	textproto.CanonicalMIMEHeaderKey(chunking.HeaderUploadAcks): util.SkipHeaderAlways,
}

// withoutRelayCookies returns h with the relay's cookies (relayCookie, the
// host's) removed from its Cookie header, keeping the agent's own cookies:
// they authenticate the visitor to the relay, and a shared or browsed agent
// would otherwise receive the visitor's session.
func withoutRelayCookies(h http.Header, relayCookie func(name string) bool) http.Header {
	cookies := h.Values("Cookie")
	if len(cookies) == 0 {
		return h
	}
	var kept []string
	for _, line := range cookies {
		for part := range strings.SplitSeq(line, ";") {
			name, _, _ := strings.Cut(strings.TrimSpace(part), "=")
			if name != "" && !relayCookie(name) {
				kept = append(kept, strings.TrimSpace(part))
			}
		}
	}
	h = h.Clone()
	h.Del("Cookie")
	if len(kept) > 0 {
		h.Set("Cookie", strings.Join(kept, "; "))
	}
	return h
}
