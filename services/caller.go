// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package services holds what the relay and its service agents share: a
// stored feature (blobs, ...)
// runs as its own process, connected to the relay as an ordinary agent, and
// the relay carries a route's requests to it over a session it opens with
// Core.Open. The service never sees the caller's credential: the relay
// authenticates it and asserts its owner in CallerHeader.
package services

// CallerHeader carries the owner the relay authenticated for a request it
// carries to a service agent: the partition key of everything the service
// stores. The relay drops any copy a client sent (stripClientAssertedIdentityHeaders)
// and sets it on every request it forwards; a service trusts it because only
// its own owner's principals can open a session to it (the core's reach
// rule) and the operator holds that owner's credentials.
const CallerHeader = "X-Nfltr-Caller"

// CallerPrincipalHeader and CallerMethodHeader carry, beside the owner, the
// principal the relay authenticated and how (its X-Auth-Method: apikey,
// session, ...), for a service that records who sent what (the mailbox
// service: an envelope's sender). Stripped from clients and set by the relay
// as CallerHeader is.
const (
	CallerPrincipalHeader = "X-Nfltr-Caller-Principal"
	CallerMethodHeader    = "X-Nfltr-Caller-Method"
)
