// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package agentcliguard keeps the relay module's test binaries off every
// real relay: whatever relay address, API key or saved config the host or a
// node agent's environment holds, a test resolves a loopback trap that fails
// the binary on any dial (relaytrap.go, the same trap as the root module's
// agentcliguard). The relay never spawns agent CLIs, so this copy has no
// agent-CLI stubs. Imported only from _test.go files.
package agentcliguard

// Run is a complete TestMain body: os.Exit(agentcliguard.Run(m)).
func Run(m interface{ Run() int }) int {
	dials := isolateRelay()
	code := m.Run()
	if dials() > 0 && code == 0 {
		code = 1
	}
	return code
}
