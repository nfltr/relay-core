// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"net/http"
	"net/textproto"
	"reflect"
	"testing"
)

func TestHeadersToProto(t *testing.T) {
	header := http.Header{
		"Content-Type":    []string{"application/json"},
		"X-Custom-Header": []string{"value1", "value2"},
		"Agent_id":        []string{"123"},
		"never_skip":      []string{"value1", "value2"},
	}

	header.Add("Authorization", "Bearer token")

	skipHeaders := map[string]SkipHeaderIf{
		textproto.CanonicalMIMEHeaderKey("agent_id"):   SkipHeaderAlways,
		textproto.CanonicalMIMEHeaderKey("never_skip"): func(_ string) bool { return false },
	}

	expectedHeaders := map[string][]string{
		"Authorization":   {"Bearer token"},
		"Content-Type":    {"application/json"},
		"X-Custom-Header": {"value1", "value2"},
		"never_skip":      {"value1", "value2"},
	}

	result := HeadersToProto(header, skipHeaders)

	for h, valueList := range result {
		if _, ok := skipHeaders[h]; ok {
			t.Errorf("Header %v should not be present in the result", h)
		}
		vals, ok := expectedHeaders[h]
		if !ok {
			t.Errorf("Header %v should be present in the result", h)
		}
		if !reflect.DeepEqual(valueList.GetHeaderValues(), vals) {
			t.Errorf("Header %v should have values %v, got %v", h, vals, valueList.GetHeaderValues())
		}
	}
}
