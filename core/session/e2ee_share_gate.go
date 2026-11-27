// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"slices"

	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"google.golang.org/grpc/metadata"
)

// tcpSharePolicyHeader is the ConnectTCP request metadata in which an agent
// names each share whose credential policy it enforces on E2EE sessions
// (the agent SDK's sharePolicyHeader), one value per share code.
const tcpSharePolicyHeader = "x-nfltr-share-policy"

// shareUnenforced counts a share connection no agent stream enforcing the
// share could take, on sni (frontend/sni counts the other refusals).
func shareUnenforced() {
	metrics.TransferOpened(metrics.PathSNI, relayv1.ErrorReason_ERROR_REASON_SHARE_POLICY_UNENFORCED.String())
}

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
