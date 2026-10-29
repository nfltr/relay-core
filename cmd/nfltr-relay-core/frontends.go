// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/nfltr/relay-core/core/frontend"
	httpingress "github.com/nfltr/relay-core/frontend/http"
	"github.com/nfltr/relay-core/frontend/sni"
)

// frontends are the frontends nfltr-relay-core attaches to its core
// (frontend.CoreHost): HTTP ingress (a key's own requests to its agents)
// and SNI E2EE passthrough; WireGuard with the build tag wireguard
// (frontends_wireguard.go). The client sessions' frontend (frontend/connect)
// is served on the proxy port directly.
var frontends = []frontend.Frontend{httpingress.Frontend{}, sni.Frontend{}}
