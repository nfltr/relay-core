// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"net/http"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestNoIdentifyingLabels: no metric of this package labels its series with
// an agent, account or key, so /metrics names nobody and its series count
// does not grow with the agents a relay has seen. The rule is the one
// relaytest's TestMetricsNameNobody applies to every series the running
// relay serves: no label name containing any of these words.
func TestNoIdentifyingLabels(t *testing.T) {
	forbidden := regexp.MustCompile(`(?i)agent|owner|account|user|key|ip|addr|peer|share|subdomain|email|tenant|worker|pod_id|pool`)
	vars := regexp.MustCompile(`variableLabels: \{([^}]*)\}`)
	ch := make(chan *prometheus.Desc, 1024)
	all.Describe(ch)
	close(ch)
	n := 0
	for d := range ch {
		n++
		m := vars.FindStringSubmatch(d.String())
		if m == nil {
			t.Fatalf("cannot read the labels of %s", d)
		}
		for _, l := range strings.Split(m[1], ",") {
			if l = strings.Trim(l, " c()"); forbidden.MatchString(l) {
				t.Errorf("%s has identifying label %q", d, l)
			}
		}
	}
	if n < 50 {
		t.Fatalf("described %d metrics, want every metric of the package", n)
	}
}

// TestHTTPLabelsBounded: a request's method (client input) and the status
// an agent answered with become label values only as a method net/http
// names and a three-digit code, else "other".
func TestHTTPLabelsBounded(t *testing.T) {
	RegisterRequestsTotal("ALICE-HUB", "12345", "other")
	RegisterRequestDuration("ALICE-HUB", "-1", 1)
	RegisterLatencyIntroduced("ALICE-HUB", "2000", 1)
	RegisterRPCRequestPayloadSize("ALICE-HUB", 1)
	RegisterRPCResponsePayloadSize("ALICE-HUB", "20x", 1)
	ObserveHTTPRequest("proxy", "ALICE-HUB", "2xx", 0)
	RegisterRequestsTotal(http.MethodGet, "200", "other")
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "method" || l.GetName() == "status" {
					seen[l.GetValue()] = true
				}
			}
		}
	}
	for _, v := range []string{"ALICE-HUB", "12345", "-1", "2000", "20x"} {
		if seen[v] {
			t.Errorf("label value %q kept, want it bounded to \"other\"", v)
		}
	}
	for _, v := range []string{"other", http.MethodGet, "200"} {
		if !seen[v] {
			t.Errorf("label value %q missing", v)
		}
	}
}

func TestBuildInfo(t *testing.T) {
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "nfltr_relay_build_info" {
			continue
		}
		if len(f.GetMetric()) != 1 || f.GetMetric()[0].GetGauge().GetValue() != 1 {
			t.Fatalf("nfltr_relay_build_info = %v, want one series of value 1", f.GetMetric())
		}
		labels := map[string]string{}
		for _, l := range f.GetMetric()[0].GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["version"] == "" || labels["revision"] == "" || labels["go_version"] != runtime.Version() {
			t.Fatalf("nfltr_relay_build_info labels = %v", labels)
		}
		return
	}
	t.Fatal("nfltr_relay_build_info not exported")
}
