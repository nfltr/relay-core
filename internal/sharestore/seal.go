// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sharestore

import (
	"maps"

	"github.com/nfltr/relay-core/core/sharesecret"
)

// Sealed returns p with its credentials (Basic Auth, Bearer token, required
// header values) sealed. The relay seals a policy once, at issuance, before
// any store sees it; the issuer gets the plaintext in the issue response.
func (p Policy) Sealed() Policy {
	if p.BasicAuth != "" {
		p.BasicAuth = sharesecret.Seal(p.BasicAuth)
	}
	if p.BearerToken != "" {
		p.BearerToken = sharesecret.Seal(p.BearerToken)
	}
	if len(p.RequiredHeaders) > 0 {
		p.RequiredHeaders = maps.Clone(p.RequiredHeaders)
		for name, value := range p.RequiredHeaders {
			if value != "" { // an empty value is "no secret": nothing to seal
				p.RequiredHeaders[name] = sharesecret.Seal(value)
			}
		}
	}
	return p
}
