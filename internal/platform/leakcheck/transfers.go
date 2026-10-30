// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package leakcheck

import (
	"flag"
	"fmt"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// SkipTransfers turns uncountedTransfers off for a package whose tests
// count transfers without sessions (platform/metrics' own).
var SkipTransfers bool

// firstByteGaps are the paths whose sessions may move bytes with no first
// byte observed, and why.
var firstByteGaps = map[string]string{
	"connect": "a data connection's session spliced to an agent link session by link.Join: link reports its bytes (HopEnded) but no first byte; follow-up in github.com/nfltr/link",
}

// uncountedTransfers names the transfer paths (nfltr_relay_transfer_*) on
// which the package's tests opened sessions but counted none of their
// bytes, or counted bytes from the agent and never a first byte: a path
// that opens sessions and forgets to meter them. Only a whole run is
// judged (one -run test may open a session that moves nothing).
func uncountedTransfers() []string {
	if f := flag.Lookup("test.run"); SkipTransfers || f != nil && f.Value.String() != "" {
		return nil
	}
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return []string{"gather: " + err.Error()}
	}
	opens, bytes, from, first := map[string]float64{}, map[string]float64{}, map[string]float64{}, map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			l := labels(m)
			switch mf.GetName() {
			case "nfltr_relay_transfer_opens_total":
				if l["result"] == "ok" {
					opens[l["path"]] += m.GetCounter().GetValue()
				}
			case "nfltr_relay_transfer_bytes_total":
				bytes[l["path"]] += m.GetCounter().GetValue()
				if l["direction"] == "from_agent" {
					from[l["path"]] += m.GetCounter().GetValue()
				}
			case "nfltr_relay_transfer_first_byte_seconds":
				first[l["path"]] += float64(m.GetHistogram().GetSampleCount())
			}
		}
	}
	var out []string
	for p, n := range opens {
		switch {
		case n > 0 && bytes[p] == 0:
			out = append(out, fmt.Sprintf("%s: %.0f sessions opened, no bytes counted", p, n))
		case from[p] > 0 && first[p] == 0 && firstByteGaps[p] == "":
			out = append(out, fmt.Sprintf("%s: %.0f bytes from the agent, no first byte observed", p, from[p]))
		}
	}
	sort.Strings(out)
	return out
}

func labels(m *dto.Metric) map[string]string {
	l := map[string]string{}
	for _, p := range m.GetLabel() {
		l[p.GetName()] = p.GetValue()
	}
	return l
}
