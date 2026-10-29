// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"fmt"
	"slices"

	"github.com/nfltr/relay-core/platform/config"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"google.golang.org/grpc/codes"
)

// CheckServicesE2EE refuses a declaration with a tcp service whose own
// E2EE choice the policy forbids, as a FailedPrecondition the agent shows
// at startup.
func CheckServicesE2EE(decls []agentservices.Decl, policy string) error {
	for _, d := range decls {
		if d.Kind != agentservices.TCP {
			continue
		}
		switch {
		case policy == config.E2EERequire && d.E2EE == agentservices.E2EEOff:
			return RelayStatus(codes.FailedPrecondition, relayv1.ErrorReason_ERROR_REASON_E2EE_REQUIRED,
				fmt.Sprintf("service %s is declared e2ee: false, and this relay requires end-to-end encryption of TCP services (tcp.e2ee_policy=require)", d.Name)).Err()
		case policy == config.E2EEForbid && d.E2EE == agentservices.E2EEOn:
			return RelayStatus(codes.FailedPrecondition, relayv1.ErrorReason_ERROR_REASON_E2EE_UNAVAILABLE,
				fmt.Sprintf("service %s is declared e2ee: true, and this relay forbids end-to-end encryption of TCP services (tcp.e2ee_policy=forbid)", d.Name)).Err()
		}
	}
	return nil
}

// plainServices is the declaration of an agent that does not announce
// E2EE modes (agentservices.ModesHeader: nfltr before v2.1.6), which
// cannot terminate E2EE on a tcp service: its tcp services plain, or a
// FailedPrecondition it shows at startup where the policy requires E2EE.
func plainServices(decls []agentservices.Decl, policy string) ([]agentservices.Decl, error) {
	out := slices.Clone(decls)
	for i, d := range out {
		if d.Kind != agentservices.TCP || d.E2EE != agentservices.E2EEDefault {
			continue
		}
		if policy == config.E2EERequire {
			return nil, RelayStatus(codes.FailedPrecondition, relayv1.ErrorReason_ERROR_REASON_E2EE_REQUIRED,
				fmt.Sprintf("service %s: this relay requires end-to-end encryption of TCP services (tcp.e2ee_policy=require), which this nfltr version cannot serve: update nfltr (nfltr update)", d.Name)).Err()
		}
		out[i].E2EE = agentservices.E2EEOff
	}
	return out, nil
}

// ServiceE2EE is whether sessions to a tcp service are end to end
// encrypted: the agent's choice, else the policy's, else the default.
func ServiceE2EE(d agentservices.Decl, policy string, def bool) bool {
	switch {
	case d.E2EE == agentservices.E2EEOn:
		return true
	case d.E2EE == agentservices.E2EEOff:
		return false
	case policy == config.E2EERequire:
		return true
	case policy == config.E2EEForbid:
		return false
	}
	return def
}

// ResolveServicesE2EE returns decls with each tcp service's E2EE stated
// (serviceE2EE): the declaration the relay accepts and echoes, so the
// agent knows (and shows) the mode of every service.
func ResolveServicesE2EE(decls []agentservices.Decl, policy string, def bool) []agentservices.Decl {
	if decls == nil {
		return nil
	}
	out := make([]agentservices.Decl, len(decls))
	for i, d := range decls {
		if d.Kind == agentservices.TCP {
			on := ServiceE2EE(d, policy, def)
			d.E2EE = agentservices.E2EEOff
			if on {
				d.E2EE = agentservices.E2EEOn
			}
		}
		out[i] = d
	}
	return out
}
