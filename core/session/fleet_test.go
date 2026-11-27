// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestParseLabelsMetadata_ValidInput(t *testing.T) {
	tests := []struct {
		name   string
		input  []string
		expect map[string]string
	}{
		{
			name:   "single object",
			input:  []string{`{"fleet":"hvac"}`},
			expect: map[string]string{"fleet": "hvac"},
		},
		{
			name:   "multiple header values merge",
			input:  []string{`{"fleet":"hvac"}`, `{"zone":"building-a"}`},
			expect: map[string]string{"fleet": "hvac", "zone": "building-a"},
		},
		{
			// One encoding (relay-protocol-v1 P0): k=v pairs read as none.
			name:   "key=value pairs are not labels",
			input:  []string{"fleet=hvac,zone=building-a"},
			expect: nil,
		},
		{
			// rpcclient sends a JSON object: values keep ',' and '='.
			name:   "json object keeps commas and equals signs",
			input:  []string{`{"nfltr.machine.resources":"cpu:16,mem:64GB","note":"a=b"}`},
			expect: map[string]string{"nfltr.machine.resources": "cpu:16,mem:64GB", "note": "a=b"},
		},
		{
			name:   "empty string",
			input:  []string{""},
			expect: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			md := metadata.MD{
				"x-agent-labels": tc.input,
			}
			got := parseLabelsMetadata(md)
			if tc.expect == nil {
				if len(got) != 0 {
					t.Errorf("expected nil/empty, got %v", got)
				}
				return
			}
			if len(got) != len(tc.expect) {
				t.Fatalf("length mismatch: got %v, want %v", got, tc.expect)
			}
			for k, v := range tc.expect {
				if got[k] != v {
					t.Errorf("key %q: got %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestParseLabelsMetadata_NoHeader(t *testing.T) {
	md := metadata.MD{
		"x-agentID": {"agent-a"},
	}
	got := parseLabelsMetadata(md)
	if len(got) != 0 {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestAgentInfoIncludesLabels(t *testing.T) {
	labels := map[string]string{"fleet": "hvac", "zone": "building-a"}

	info := AgentInfo{
		AgentID: "sensor-1",
		Routes:  []string{"/api/v1/data"},
		Labels:  labels,
	}

	if info.Labels["fleet"] != "hvac" {
		t.Errorf("expected label fleet=hvac, got %v", info.Labels)
	}
	if info.Labels["zone"] != "building-a" {
		t.Errorf("expected label zone=building-a, got %v", info.Labels)
	}
}
