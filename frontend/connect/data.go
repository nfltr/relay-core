// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"github.com/nfltr/relay/proto/wire/tlsbatch"
)

// serveConnectData makes an authenticated /connect upgrade's connection a
// data connection (the relay protocol's dataplane): tcp-connect's tunnels as
// sessions with per-session credit, frames written in batches, no HTTP/2
// writer in between. It serves the connection until it ends. The 101 says
// the relay answers the sessions that ask and states its E2EE policy.
func (f *Frontend) serveData(w http.ResponseWriter, r *http.Request, caller identity.Principal, cfg Config) {
	if f.Core.TCPDispatcher == nil {
		http.Error(w, "TCP tunneling is not enabled (set tcp.enabled=true)", http.StatusServiceUnavailable)
		return
	}
	nc, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return
	}
	defer nc.Close() //nolint:errcheck
	// The client waits for the 101 before it sends its hello.
	if brw.Reader.Buffered() > 0 || nc.SetDeadline(time.Time{}) != nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), session.DataPlaneSniffTimeout)
	m, err := dataplane.Accept(ctx, tlsbatch.Batched(nc), session.TCPDataWindow, cfg.E2EEPolicy)
	cancel()
	if err != nil {
		return
	}
	metrics.RecordDataConnection(2, "connect")
	// Held under the upgrade's credential: revoking it ends the data
	// connection and its sessions (revocation.go).
	if f.Hold != nil {
		if held, release, ok := f.Hold(r.Context(), caller); ok {
			defer release()
			stop := context.AfterFunc(held, func() { _ = m.Close() })
			defer stop()
		}
	}
	if agentwire.ParseGeneration(r.Header.Values(agentwire.GenerationKey)) >= drainedGeneration {
		defer f.Clients.add(m)()
	}
	_ = m.Serve(func(open *pb.TCPData, s *dataplane.Session) { f.serveSession(r, caller, open, s) })
}

// serveConnectSession forwards one session of a data connection to its
// agent as a hop of its chain (core/session). The open runs every
// check an HTTP/2 tunnel does, the request's credential included (a key
// revoked since the upgrade opens nothing), then the operator's E2EE
// policy; a refusal carries its reason. Past them the session goes on to
// the agent, which answers it once its target is reached: the relay never
// answers for the agent. The client reads the answer link's own way where
// its data connection negotiated link.FeatureChain, in band where it did
// not and asked for one (nfltr v2.1.6); a client that asked for none
// (nfltr v2.1.5) gets a refused session reset.
func (f *Frontend) serveSession(r *http.Request, caller identity.Principal, open *pb.TCPData, s peerlink.Session) {
	e2ee := open.GetE2Ee()
	refuse := func(agentID string, reason relayv1.ErrorReason, msg string) error {
		metrics.RecordConnectSession(e2ee, reason.String())
		f.audit(r, agentID, open, "denied", reason.String()+": "+msg)
		return dataplane.Refusal(reason, msg)
	}
	session.Forward(f.Logger, session.ChainConnect, s, open.GetAnswer(), func(ctx context.Context) (peerlink.Session, func(), error) {
		// The agent's answer is the client's when it reads answers either
		// way; an owner's session either way (the agent's service table).
		t := session.Target{Agent: open.GetAgentId(), Service: open.GetTargetAddr(), E2EE: e2ee, Answer: open.GetAnswer() || session.AnswersNatively(s), Hold: true}
		out, err := f.Core.Open(ctx, caller, t)
		var refusal *session.Refusal
		var dial *session.DialError
		switch {
		case errors.As(err, &refusal):
			return nil, nil, refuse(refusal.AgentID, refusal.Reason, refusal.Msg)
		case errors.As(err, &dial):
			f.Logger.Warn("connect: dial agent failed", "agent_id", dial.AgentID, "target", dial.TargetAddr, "error", dial.Err)
			metrics.RegisterSSHProxyError("dial")
			if errors.Is(err, session.ErrAgentAnswers) {
				return nil, nil, refuse(dial.AgentID, relayv1.ErrorReason_ERROR_REASON_FEATURE_UNSUPPORTED, dial.Err.Error())
			}
			if errors.Is(err, session.ErrDirectoryUnavailable) {
				return nil, nil, refuse(dial.AgentID, relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE, "the relay's directory is unavailable")
			}
			metrics.RecordConnectSession(e2ee, relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE.String())
			f.audit(r, dial.AgentID, open, "denied", dial.Err.Error())
			return nil, nil, session.ErrFor(dial.Err, relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, "agent %s is not connected", dial.AgentID)
		case err != nil:
			return nil, nil, err
		}
		metrics.RecordConnectSession(e2ee, "open")
		f.audit(r, out.AgentID, open, "allowed", "")
		start := time.Now()
		metrics.RegisterSSHProxyConnection("local")
		return out.Session, func() {
			if out.Release != nil {
				out.Release()
			}
			metrics.DeregisterSSHProxyConnection()
			metrics.RegisterSSHProxyDuration(time.Since(start).Seconds())
		}, nil
	})
}
