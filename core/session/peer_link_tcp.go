// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// PodDialer opens a RelayTunnel stream (HTTP-agent and agent-to-agent
// calls, fleet invokes) to the replica holding an agent. A multi-replica
// relay's is a linkPodDialer; nil on a single relay.
type PodDialer interface {
	DialRelay(ctx context.Context, agentID string, logger *slog.Logger) (*PodRelayStream, error)
}

// PodRelayStream is a RelayTunnel stream to the replica at targetPodIP;
// cleanup ends it.
type PodRelayStream struct {
	stream      Streamer
	targetPodIP string
	cleanup     func()
}

// NewPodRelayStream is a RelayTunnel stream to the replica at targetPodIP;
// cleanup ends it.
func NewPodRelayStream(stream Streamer, targetPodIP string, cleanup func()) *PodRelayStream {
	return &PodRelayStream{stream: stream, targetPodIP: targetPodIP, cleanup: cleanup}
}

// ServeLinkTCP serves a session a peer forwarded on the link as a hop of
// its chain (session_chain.go): on to the agent here, as if its opener had
// connected here, answered with the agent's word. A session this replica
// cannot serve is refused: AGENT_OFFLINE when the agent is not here (it
// moved), SHARE_POLICY_UNENFORCED for a share its stream does not enforce.
func (d *TCPDispatcher) ServeLinkTCP(s PeerSession, ep TCPEndpoint) {
	Forward(d.logger, chainReplica, s, ep.Answer, func(ctx context.Context) (peerlink.Session, func(), error) {
		as := d.agentStream(ep.AgentID)
		if as == nil {
			return nil, nil, dataplane.Refusal(relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, fmt.Sprintf("agent %s is not connected", ep.AgentID))
		}
		if ep.Share != "" && !as.enforcesShare(ep.Share) {
			d.logger.Warn("tcp tunnel: share connection refused: its stream does not enforce the share's policy",
				"agent_id", ep.AgentID, "share", ep.Share)
			metrics.RegisterSNIRouterError(sniErrShareUnenforced)
			return nil, nil, dataplane.Refusal(relayv1.ErrorReason_ERROR_REASON_SHARE_POLICY_UNENFORCED, errShareUnenforced.Error())
		}
		out, release, err := d.onwardLocal(ctx, ep, as)
		if err != nil {
			reason := relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE
			switch {
			case errors.Is(err, ErrAgentAnswers):
				reason = relayv1.ErrorReason_ERROR_REASON_FEATURE_UNSUPPORTED
			case errors.Is(err, errShareUnenforced):
				reason = relayv1.ErrorReason_ERROR_REASON_SHARE_POLICY_UNENFORCED
			}
			return nil, nil, ErrFor(err, reason, "%v", err)
		}
		return out, release, nil
	})
}
