// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// QAD, QUIC address discovery (lane DIRECTPATH; nfltr
// docs/decisions/direct-paths-2026-11-11.md "QAD"): a connection on the
// agent link's UDP port that negotiates ALPN qadALPN is answered with the
// address its packets came from, as this relay saw it, and closed. An
// endpoint behind NAT learns its reflexive address there, from the same
// UDP socket its direct paths use. The answer is one unidirectional
// stream: the address as netip.AddrPort text ("203.0.113.5:4242",
// "[2001:db8::1]:4242"; an IPv4-mapped address unmapped), then FIN; the
// relay then waits for the caller to close, at most DataPlaneSniffTimeout.
// It needs no credential (the address is the caller's own) and the port's
// packet and handshake limits (connlimit) bound it. The answer is the
// relay's word: an endpoint uses it only as a candidate, which the direct
// handshake checks (link spec/DirectPath.tla, relay
// spec/proverif/e2ee_tcp.pv queries 21-24).

import (
	"errors"
	"net"
	"net/netip"
	"time"

	"github.com/nfltr/quic-go"
	"github.com/nfltr/relay/proto/wire/dataplane"

	"github.com/nfltr/relay-core/platform/metrics"
)

// qadALPN is the ALPN of a QAD connection (unexported until an endpoint
// asks: then it moves to relay proto wire/dataplane).
const qadALPN = "nfltr-qad/1"

// errQADAnswered ends a QAD connection: it names no peer.
var errQADAnswered = errors.New("qad: answered")

// answerQAD writes q's observed address on a unidirectional stream and
// waits for the caller's close.
func answerQAD(q *quic.Conn) error {
	addr, err := observedAddr(q.RemoteAddr())
	if err != nil {
		return err
	}
	st, err := q.OpenUniStream()
	if err != nil {
		return err
	}
	_ = st.SetWriteDeadline(time.Now().Add(DataPlaneSniffTimeout))
	if _, err := st.Write([]byte(addr.String())); err != nil {
		return err
	}
	if err := st.Close(); err != nil {
		return err
	}
	metrics.RecordAgentLinkConnection(dataplane.AgentLinkQUIC, "qad")
	t := time.NewTimer(DataPlaneSniffTimeout)
	defer t.Stop()
	select { // the caller's close: the answer reached it
	case <-q.Context().Done():
	case <-t.C:
	}
	return errQADAnswered
}

// observedAddr is a's IP and port, an IPv4-mapped IPv6 address unmapped.
func observedAddr(a net.Addr) (netip.AddrPort, error) {
	u, ok := a.(*net.UDPAddr)
	if !ok {
		return netip.AddrPort{}, errors.New("qad: not a UDP address: " + a.String())
	}
	ap := u.AddrPort()
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()), nil
}
