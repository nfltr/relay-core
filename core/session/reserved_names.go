// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"strings"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// reservedLabels are the product hostnames under http.share_domain: they
// never
// name an agent, a direct agent subdomain or a share, and the relay never
// serves agent content on them. This is the one built-in list; creation
// (agent connect) and routing (agentOriginLabel, dashboardHost) both read
// it through Reserved.
var reservedLabels = map[string]bool{
	"www": true, "api": true, "api-origin": true, "grpc": true,
	"relay": true, "orch": true, "docs": true, "app": true, "admin": true, "dashboard": true,
	"status": true, "mail": true, "smtp": true, "mx": true, "ns": true,
	"blog": true, "help": true, "support": true, "s": true,
}

// Reserved is one relay's reserved labels: the built-in product names and
// the labels its operator maps to sites (http.site_hosts), which get the
// same treatment. The zero value holds the built-ins only. Copies share
// the site labels: Set on one (a reload of the site hosts) is what every
// copy (the dispatcher's, the HTTP routes') reads from then on.
type Reserved struct {
	sites *atomic.Pointer[map[string]bool]
}

// NewReserved reserves siteLabels beside the built-in names.
func NewReserved(siteLabels ...string) Reserved {
	r := Reserved{sites: new(atomic.Pointer[map[string]bool])}
	r.Set(siteLabels...)
	return r
}

// Set replaces the site labels, for every copy of r, at once: a label added
// is refused to the next agent connect, a label removed is an ordinary name
// again. It panics on the zero value, which holds no site labels to replace.
func (r Reserved) Set(siteLabels ...string) {
	sites := make(map[string]bool, len(siteLabels))
	for _, l := range siteLabels {
		sites[strings.ToLower(strings.TrimSpace(l))] = true
	}
	r.sites.Store(&sites)
}

// Label reports whether a host label is reserved on this relay.
func (r Reserved) Label(label string) bool {
	label = strings.ToLower(strings.TrimSpace(label))
	if reservedLabels[label] {
		return true
	}
	if r.sites == nil {
		return false
	}
	return (*r.sites.Load())[label]
}

// AgentName reports whether an agent may not take name: a reserved label,
// or a browse label (b-<hash>), which would shadow an owner's browse origin.
func (r Reserved) AgentName(name string) bool {
	return r.Label(name) || IsBrowseLabel(strings.ToLower(strings.TrimSpace(name)))
}

// RefuseAgentName fails an agent connect under a reserved name with
// FailedPrecondition, which agents print and stop on instead of retrying.
// It also refuses an agent registered under such a name before the list
// grew: it cannot reconnect.
func (r Reserved) RefuseAgentName(agentID string) error {
	if !r.AgentName(agentID) {
		return nil
	}
	return status.Errorf(codes.FailedPrecondition, "agent name %q is reserved by the relay (product hostnames, site hosts and browse labels); connect with another name (--name)", agentID)
}
