// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relayfault

package connect

import "github.com/nfltr/link/peerlink"

// faultSession is s: the relay fault hook is in relayfault builds only
// (fault_on.go), never in a shipped relay.
func faultSession(_ string, s peerlink.Session) peerlink.Session { return s }
