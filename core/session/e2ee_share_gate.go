// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"slices"

	"google.golang.org/grpc/metadata"
)

// tcpSharePolicyHeader is the ConnectTCP request metadata in which an agent
// names each share whose credential policy it enforces on E2EE sessions
// (the agent SDK's sharePolicyHeader), one value per share code.
const tcpSharePolicyHeader = "x-nfltr-share-policy"

// sniErrShareUnenforced is the metric error type
// (dispatcher_sni_router_errors_total) of a share connection no agent
// stream enforcing the share could take (frontend/sni counts the others).
const sniErrShareUnenforced = "share_policy_unenforced"

// sharesEnforcedBy reads the shares an agent's ConnectTCP stream names.
func sharesEnforcedBy(ctx context.Context) []string {
	return metadata.ValueFromIncomingContext(ctx, tcpSharePolicyHeader)
}

// enforcesShare reports whether the stream's agent enforces share's policy;
// false for no stream (an agent on another pod cannot be checked). An
// auxiliary stream enforces what its primary declared: the metadata of the
// auxiliary connection itself is not trusted for it.
func (as *tcpAgentStream) enforcesShare(share string) bool {
	if as != nil && as.primary != nil {
		as = as.primary
	}
	return as != nil && slices.Contains(as.shares, share)
}
