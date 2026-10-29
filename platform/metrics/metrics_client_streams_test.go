// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func TestTrackClientStream(t *testing.T) {
	end := TrackClientStream("/m/A", "nfltr", "v1.0.1")
	if got := open("/m/A", "nfltr", "v1.0.1"); got != 1 {
		t.Fatalf("open = %v, want 1", got)
	}
	end()
	if got := open("/m/A", "nfltr", "v1.0.1"); got != 0 {
		t.Fatalf("after end = %v, want 0", got)
	}
	TrackClientStream("/m/A", "", "")()
	// Client input cannot grow the series without bound.
	for i := range maxClientVersions + 10 {
		TrackClientStream("/m/B", "x", fmt.Sprint(i))()
	}
	if n := len(clientVersions.seen); n > maxClientVersions {
		t.Fatalf("%d client versions kept, bound %d", n, maxClientVersions)
	}
}

func open(labels ...string) float64 {
	var m dto.Metric
	_ = clientStreams.WithLabelValues(labels...).Write(&m)
	return m.GetGauge().GetValue()
}
