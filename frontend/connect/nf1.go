// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"net/http"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/platform/connlimit"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"github.com/spf13/viper"
)

// The capability address's caps (nfltr
// docs/decisions/nf1-rendezvous-2026-11-13.md section 4), per replica,
// read from the nf1.* settings at first use. One account key each: conns
// and opens count per source address only, address per rid (and per rid
// and source).
type nf1Limits struct {
	conns, opens, address *connlimit.Accounts
}

const nf1Account = "nf1"

// nf1Limits are f's caps, read from the settings at its first nf1 use.
func (f *Frontend) nf1Limits() *nf1Limits {
	f.nf1Once.Do(func() { f.nf1 = newNF1Limits() })
	return &f.nf1
}

func newNF1Limits() nf1Limits {
	return nf1Limits{
		conns: connlimit.NewAccounts(connlimit.Limits{SourceMax: viper.GetInt("nf1.source_max"),
			SourceRate: viper.GetFloat64("nf1.source_rate"), SourceBurst: viper.GetInt("nf1.source_burst")}),
		opens: connlimit.NewAccounts(connlimit.Limits{SourceRate: viper.GetFloat64("nf1.open_rate"), SourceBurst: viper.GetInt("nf1.open_burst")}),
		address: connlimit.NewAccounts(connlimit.Limits{Max: viper.GetInt("nf1.address_max"),
			Rate: viper.GetFloat64("nf1.address_rate"), Burst: viper.GetInt("nf1.address_burst")}),
	}
}

func remoteIP(r *http.Request) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return nil
	}
	return net.ParseIP(host)
}

// admitAnonymous counts a data connection with no credential against its
// source: release, or false when over the source's limits.
func (f *Frontend) admitAnonymous(r *http.Request) (release func(), ok bool) {
	release, reason := f.nf1Limits().conns.Admit(nf1Account, remoteIP(r))
	return release, reason == ""
}

func limitRefusal(reason string) error {
	metrics.RecordConnectSession(true, relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED.String())
	return dataplane.Refusal(relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED, "nf1: over the relay's limit ("+reason+")")
}

// serveNF1Session forwards one session of a data connection that opens a
// capability address (pb.TCPData.nf1_rid) to the agent serving it, for any
// caller, with or without a credential: the address carries its own trust.
// Its refusals name no agent; unknown, offline and disabled are one answer
// (session.NF1NotServed).
func (f *Frontend) serveNF1Session(r *http.Request, open *pb.TCPData, s peerlink.Session, enabled bool) {
	session.Forward(f.Logger, session.ChainConnect, s, open.GetAnswer(), func(ctx context.Context) (peerlink.Session, func(), error) {
		refuse := func(reason relayv1.ErrorReason, msg string) error {
			metrics.RecordConnectSession(true, reason.String())
			return dataplane.Refusal(reason, msg)
		}
		rid := open.GetNf1Rid()
		switch {
		case !enabled || len(rid) != agentservices.RIDSize:
			return nil, nil, refuse(relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, session.NF1NotServed)
		case open.GetAgentId() != "" || open.GetTargetAddr() != "" || !open.GetE2Ee():
			return nil, nil, refuse(relayv1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, "nf1: an open of a capability address names no agent or target and is end to end encrypted")
		}
		lim := f.nf1Limits()
		ip := remoteIP(r)
		release, reason := lim.opens.Admit(nf1Account, ip) // a rate only: nothing stays counted
		if reason != "" {
			return nil, nil, limitRefusal(reason)
		}
		release()
		t := session.Target{NF1: [agentservices.RIDSize]byte(rid), E2EE: true, Answer: open.GetAnswer() || session.AnswersNatively(s)}
		t.Admit = func(agentID string) (func(), error) {
			addr, reason := lim.address.Admit(hex.EncodeToString(rid), ip)
			if reason != "" {
				return nil, limitRefusal("address_" + reason)
			}
			visitor, reason := f.Core.TCPDispatcher.AdmitVisitor(agentID, ip)
			if reason != "" {
				addr()
				return nil, limitRefusal(reason)
			}
			return func() { visitor(); addr() }, nil
		}
		out, err := f.Core.Open(ctx, identity.Principal{}, t)
		var refusal *session.Refusal
		var dial *session.DialError
		switch {
		case errors.As(err, &refusal):
			return nil, nil, refuse(refusal.Reason, refusal.Msg)
		case errors.As(err, &dial):
			f.Logger.Warn("connect: nf1 dial agent failed", "agent_id", dial.AgentID, "error", dial.Err)
			f.audit(r, dial.AgentID, open, "denied", dial.Err.Error())
			if errors.Is(err, session.ErrDraining) {
				return nil, nil, refuse(relayv1.ErrorReason_ERROR_REASON_RELAY_DRAINING, "the relay is draining")
			}
			return nil, nil, refuse(relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, session.NF1NotServed)
		case err != nil: // Admit's limitRefusal
			return nil, nil, err
		}
		metrics.RecordConnectSession(true, "open")
		f.audit(r, out.AgentID, open, "allowed", "nf1")
		// The serving agent's owner pays (the owner's decision 2026-11-13).
		session.MeterChain(ctx, f.Core.Dispatcher.OpenMeter(metrics.PathConnect, out.AgentID))
		began := metrics.TransferBegin(metrics.PathConnect)
		return out.Session, func() {
			if out.Release != nil {
				out.Release()
			}
			metrics.TransferEnd(metrics.PathConnect, began)
		}, nil
	})
}
