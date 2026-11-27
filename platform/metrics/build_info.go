// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/nfltr/relay-core/platform/buildinfo"
	"github.com/prometheus/client_golang/prometheus"
)

var buildInfoDesc = prometheus.NewDesc(
	"nfltr_relay_build_info",
	"Always 1, labelled with the relay's version, VCS revision and Go version.",
	[]string{"version", "revision", "go_version"}, nil,
)

// buildInfo reports nfltr_relay_build_info at each scrape, so a release
// stamp (-ldflags) is read when it is asked for, not in init.
type buildInfo struct{}

func (buildInfo) Describe(ch chan<- *prometheus.Desc) { ch <- buildInfoDesc }

func (buildInfo) Collect(ch chan<- prometheus.Metric) {
	i := buildinfo.Current()
	rev := i.GitSHA
	if rev == "" {
		rev = i.VCSRevision
	}
	ch <- prometheus.MustNewConstMetric(buildInfoDesc, prometheus.GaugeValue, 1, i.Version, rev, i.GoVersion)
}
