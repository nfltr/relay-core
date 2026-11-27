// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"maps"
	"strings"

	"github.com/nfltr/relay/proto/wire/agentwire"
	"google.golang.org/grpc/metadata"
)

// parseLabelsMetadata extracts agent labels from gRPC incoming metadata: each
// agentwire.LabelsKey value, merged. Returns nil if there are none.
func parseLabelsMetadata(md metadata.MD) map[string]string {
	labels := make(map[string]string)
	for _, val := range md.Get(agentwire.LabelsKey) {
		maps.Copy(labels, agentwire.ParseLabels(val))
	}
	if len(labels) == 0 {
		return nil
	}
	return labels
}

// parseCapabilitiesMetadata extracts the "x-agent-capabilities" gRPC
// metadata header containing comma-separated capability identifiers
// (e.g. "p2p,ssh,http"). Returns nil if the header is absent or empty.
func parseCapabilitiesMetadata(md metadata.MD) []string {
	vals := md.Get("x-agent-capabilities")
	if len(vals) == 0 {
		return nil
	}
	var caps []string
	for _, val := range vals {
		for c := range strings.SplitSeq(val, ",") {
			c = strings.TrimSpace(c)
			if c != "" {
				caps = append(caps, c)
			}
		}
	}
	return caps
}
