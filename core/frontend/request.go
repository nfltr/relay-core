// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package frontend

import "context"

// What the relay's routing tells HTTP ingress about a request it hands
// over, beyond the agent it names (session.AgentIDKey).

type browsePrefixKey struct{}

type siteRequestKey struct{}

// WithBrowsePrefix marks ctx's request as served under prefix (a browse or
// endpoint-route path): the agent's absolute redirects are rewritten to stay
// under it.
func WithBrowsePrefix(ctx context.Context, prefix string) context.Context {
	return context.WithValue(ctx, browsePrefixKey{}, prefix)
}

// BrowsePrefix is ctx's request's prefix (WithBrowsePrefix); "" for none.
func BrowsePrefix(ctx context.Context) string {
	p, _ := ctx.Value(browsePrefixKey{}).(string)
	return p
}

// WithSiteRequest marks ctx's request as one for a site host's agent: its
// static pages keep the agent's caching headers.
func WithSiteRequest(ctx context.Context) context.Context {
	return context.WithValue(ctx, siteRequestKey{}, true)
}

// SiteRequest reports whether ctx's request is a site host's
// (WithSiteRequest).
func SiteRequest(ctx context.Context) bool {
	return ctx.Value(siteRequestKey{}) != nil
}
