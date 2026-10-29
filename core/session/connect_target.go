// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/platform/config"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentservices"
)

// TargetRules are the operator's rules for a session's target: the host a
// port is dialed on and the E2EE policy (tcp.e2ee_policy, tcp.e2ee_default).
type TargetRules struct {
	TargetHost  string // the host of a port target (ssh.proxy.target_host); "" localhost
	E2EEPolicy  string
	E2EEDefault bool
	// Managed is the address of agentID's managed capability serving port
	// ("" none); nil: no managed capabilities.
	Managed func(ctx context.Context, agentID, port string) string
}

// ResolveTarget is the core's access rule and target resolution for a
// caller whose bare agent names resolve under scopePrefix and who reaches
// what reaches says: ConnectTarget, then the operator's E2EE policy. It
// refuses with a *Refusal.
func (d *Dispatcher) ResolveTarget(ctx context.Context, scopePrefix string, reaches func(string) (bool, error), t Target, rules TargetRules) (string, string, error) {
	agentID, targetAddr, refusal := d.ConnectTarget(ctx, reaches, scopePrefix, t.Agent, t.Service, rules)
	if refusal != nil {
		return "", "", refusal
	}
	if reason, msg := d.e2eeAnswer(agentID, t.Service, t.E2EE, rules); reason != 0 {
		return "", "", &Refusal{Reason: reason, Msg: msg, AgentID: agentID}
	}
	return agentID, targetAddr, nil
}

// KeyReach is Core.Reach for callers holding a credential (a principal of
// an identity provider): they reach their own agents (PrincipalReaches),
// bare names resolving under their subject. No principal reaches nothing.
func (d *Dispatcher) KeyReach(rules func() TargetRules) Reach {
	return func(ctx context.Context, caller identity.Principal, t Target) (string, string, error) {
		if caller.Provider == "" {
			return "", "", &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_UNAUTHENTICATED, Msg: "authentication required", AgentID: t.Agent}
		}
		return d.ResolveTarget(ctx, caller.Subject, d.PrincipalReaches(caller), t, rules())
	}
}

// ConnectTarget resolves a session to agentID's port or service for a
// caller (reaches, scopePrefix): the scoped agent ID and the target
// address, or the refusal the caller gets. Every client session path runs
// it: WebSocket, HTTP/2 stream and data connection session.
func (d *Dispatcher) ConnectTarget(ctx context.Context, reaches func(string) (bool, error), scopePrefix, agentID, port string, rules TargetRules) (string, string, *Refusal) {
	refuse := func(reason relayv1.ErrorReason, msg string) (string, string, *Refusal) {
		return "", "", &Refusal{Reason: reason, Msg: msg, AgentID: agentID}
	}
	// Validate agentID — no path traversal.
	if agentID == "" || strings.Contains(agentID, "..") || strings.ContainsAny(agentID, "/\\") {
		return refuse(relayv1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, "invalid agent ID")
	}
	// Only the agent's owner reaches it; anyone else gets the answer a
	// nonexistent agent gets. An owner the directory could not read is
	// the relay's unavailability, not the agent's absence.
	agentID, err := d.scopedConnectAgent(reaches, scopePrefix, agentID)
	ok := false
	if err == nil {
		ok, err = reaches(agentID)
	}
	switch {
	case err != nil:
		return refuse(relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE, "the relay's directory is unavailable")
	case !ok:
		return refuse(relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND, "agent not found")
	}
	noTCP := d == nil || d.TCPDispatcher == nil
	// A service name in place of the port: the owner's session to that
	// service of the agent.
	if agentservices.ValidName(port) {
		if noTCP {
			return refuse(relayv1.ErrorReason_ERROR_REASON_FEATURE_UNSUPPORTED, "TCP tunneling is not enabled (set tcp.enabled=true)")
		}
		if r := d.connectService(agentID, port); r != nil {
			return "", "", r
		}
		return agentID, port, nil
	}
	if !ValidPort(port) {
		return refuse(relayv1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, "invalid port number")
	}
	if noTCP {
		return refuse(relayv1.ErrorReason_ERROR_REASON_FEATURE_UNSUPPORTED, "TCP tunneling is not enabled (set tcp.enabled=true)")
	}
	// A service agent's sessions name a service (agent_services.go); the
	// agent dials only its own table, so a port means nothing to it.
	if services, _ := d.AgentServices(agentID); len(services) > 0 {
		return refuse(relayv1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, fmt.Sprintf("agent %s serves named services (%s): connect to %s/<service>", agentID, agentservices.Format(services), agentID))
	}
	host := rules.TargetHost
	if host == "" {
		host = "localhost"
	}
	targetAddr := net.JoinHostPort(host, port)
	if rules.Managed != nil {
		if managed := rules.Managed(ctx, agentID, port); managed != "" {
			targetAddr = managed
		}
	}
	return agentID, targetAddr, nil
}

// scopedConnectAgent resolves the agent name a key's session names. A
// short name is scoped to the key's own namespace: `nfltr ssh-proxy
// --name raspberry-pi` finds "alice.raspberry-pi" registered with the same
// key, so the caller need not spell the prefix. The key's namespace wins
// while that agent is connected here; otherwise a full ID the caller's
// account owns (an agent of another key of the same account) is taken as
// is. Neither: the scoped name, which the ownership check then answers.
// No scoping for a session cookie (scopePrefix "") or a name already in
// the key's namespace.
func (d *Dispatcher) scopedConnectAgent(reaches func(string) (bool, error), scopePrefix, agentID string) (string, error) {
	if scopePrefix == "" || agentID == scopePrefix || strings.HasPrefix(agentID, scopePrefix+".") {
		return agentID, nil
	}
	scoped := scopePrefix + "." + agentID
	if d != nil && d.IsAgentConnected(scoped) {
		return scoped, nil
	}
	if ok, err := reaches(agentID); ok || err != nil {
		return agentID, err
	}
	return scoped, nil
}

// connectService resolves the owner's session to agentID's service: the
// agent must be connected (on any replica: a session entering another
// replica goes over the replica link) and declare a tcp service of that
// name.
func (d *Dispatcher) connectService(agentID, service string) *Refusal {
	services, connected := d.AgentServices(agentID)
	switch {
	case !connected:
		return &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, Msg: fmt.Sprintf("agent %s is not connected", agentID), AgentID: agentID}
	case agentservices.Lookup(services, service) != agentservices.TCP:
		return &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_UNKNOWN_SERVICE, Msg: fmt.Sprintf("agent %s has no tcp service %q", agentID, service), AgentID: agentID}
	}
	return nil
}

// e2eeAnswer is SessionE2EEAnswer of an owner's session to target (a
// port, or a service of agentID's declaration).
func (d *Dispatcher) e2eeAnswer(agentID, target string, e2ee bool, rules TargetRules) (relayv1.ErrorReason, string) {
	var svc *agentservices.Decl
	if agentservices.ValidName(target) {
		services, _ := d.AgentServices(agentID)
		if decl, ok := agentservices.Find(services, target); ok {
			svc = &decl
		}
	}
	return SessionE2EEAnswer(e2ee, svc, rules.E2EEPolicy, rules.E2EEDefault)
}

// SessionE2EEAnswer refuses a session whose mode (e2ee) conflicts with the
// policy or with its service (svc, when it names one): the answer's reason
// and message; reason UNSPECIFIED when the session may open.
func SessionE2EEAnswer(e2ee bool, svc *agentservices.Decl, policy string, def bool) (relayv1.ErrorReason, string) {
	switch {
	case e2ee && policy == config.E2EEForbid:
		return relayv1.ErrorReason_ERROR_REASON_E2EE_UNAVAILABLE, "this relay forbids end-to-end encrypted TCP sessions (tcp.e2ee_policy=forbid)"
	case !e2ee && policy == config.E2EERequire:
		return relayv1.ErrorReason_ERROR_REASON_E2EE_REQUIRED, "this relay requires end-to-end encrypted TCP sessions (tcp.e2ee_policy=require): pin the agent's certificate"
	case svc == nil:
		return 0, ""
	case e2ee && !ServiceE2EE(*svc, policy, def):
		return relayv1.ErrorReason_ERROR_REASON_E2EE_UNAVAILABLE, fmt.Sprintf("service %s is not end-to-end encrypted", svc.Name)
	case !e2ee && ServiceE2EE(*svc, policy, def):
		return relayv1.ErrorReason_ERROR_REASON_E2EE_REQUIRED, fmt.Sprintf("service %s takes end-to-end encrypted sessions only: pin the agent's certificate", svc.Name)
	}
	return 0, ""
}

// ValidPort reports whether s is a TCP port number (1-65535).
func ValidPort(s string) bool {
	if s == "" || len(s) > 5 {
		return false
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
		n = n*10 + int(c-'0')
	}
	return n >= 1 && n <= 65535
}
