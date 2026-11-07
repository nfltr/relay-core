// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"net"

	"github.com/nfltr/relay-core/platform/connlimit"
)

// AdmitVisitor counts a visitor's connection from ip to agentID against the
// agent's account (Visitors: its plan's limits; spec/AccountLimit.tla):
// release frees it, or reason names the limit that refused it, reported to
// VisitorRefused. An agent whose owner is unknown is admitted.
func (d *TCPDispatcher) AdmitVisitor(agentID string, ip net.IP) (release func(), reason string) {
	v := d.Visitors
	if v == nil || v.Limits() == (connlimit.Limits{}) || d.ownerOf == nil {
		return func() {}, ""
	}
	owner, _ := d.ownerOf(agentID)
	release, reason = v.Admit(owner, ip)
	if reason != "" && d.VisitorRefused != nil {
		d.VisitorRefused(owner, agentID, reason)
	}
	return release, reason
}
