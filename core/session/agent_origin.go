// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"strings"
	"time"
)

const (
	// BrowseLabelPrefix marks a browse origin; the rest of the label is
	// the first 128 bits of sha256(agent id), which fits any agent id into
	// one DNS label (the wildcard certificate covers one level).
	BrowseLabelPrefix = "b-"

	// BrowseGrantParam carries a short-lived browse grant from the
	// dashboard to the browse origin, which trades it for a host-only cookie.
	BrowseGrantParam = "nfltr_browse"

	// BrowseGrantHandoffTTL bounds the grant in the redirect URL.
	BrowseGrantHandoffTTL = time.Minute

	// AgentContentSecurityPolicy is added to every agent response. It only
	// restricts: another origin (a hostile share, or any other site) cannot
	// frame the agent, for example to clickjack an owner's browse view.
	// The agent's own policy still applies alongside it.
	AgentContentSecurityPolicy = "frame-ancestors 'self'"
)

// IsBrowseLabel reports whether label is a browse origin's DNS label:
// BrowseLabelPrefix and 32 hex characters.
func IsBrowseLabel(label string) bool {
	return strings.HasPrefix(label, BrowseLabelPrefix) && len(label) == len(BrowseLabelPrefix)+32
}
