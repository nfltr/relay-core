// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package metrics holds the relay's Prometheus metrics, registered once
// with the default registry, and the functions that update them. No metric
// carries a label naming an agent, account or key.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// auto collects every metric of this package for the default registry. It
// is promauto's factory for the constructors this package uses. The metrics
// register as one collector in init rather than one Register each: the
// registry describes every collector on a goroutine of its own.
//
// No metric carries a label naming an agent, account or key: the relay's
// /metrics is the same for every operator who scrapes it
// (TestNoIdentifyingLabels).
var auto factory

// all is every metric auto made; init registers it once.
var all = collectors{buildInfo{}}

func init() { prometheus.MustRegister(all) }

type collectors []prometheus.Collector

func (cs collectors) Describe(ch chan<- *prometheus.Desc) {
	for _, c := range cs {
		c.Describe(ch)
	}
}

func (cs collectors) Collect(ch chan<- prometheus.Metric) {
	for _, c := range cs {
		c.Collect(ch)
	}
}

type factory struct{}

// NewCounter is promauto.NewCounter through auto. allow:wrapper (collects for the one registration)
func (factory) NewCounter(o prometheus.CounterOpts) prometheus.Counter {
	return register(prometheus.NewCounter(o))
}

// NewCounterVec is promauto.NewCounterVec through auto. allow:wrapper (collects for the one registration)
func (factory) NewCounterVec(o prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	return register(prometheus.NewCounterVec(o, labels))
}

// NewGauge is promauto.NewGauge through auto. allow:wrapper (collects for the one registration)
func (factory) NewGauge(o prometheus.GaugeOpts) prometheus.Gauge {
	return register(prometheus.NewGauge(o))
}

// NewGaugeVec is promauto.NewGaugeVec through auto. allow:wrapper (collects for the one registration)
func (factory) NewGaugeVec(o prometheus.GaugeOpts, labels []string) *prometheus.GaugeVec {
	return register(prometheus.NewGaugeVec(o, labels))
}

// NewHistogram is promauto.NewHistogram through auto. allow:wrapper (collects for the one registration)
func (factory) NewHistogram(o prometheus.HistogramOpts) prometheus.Histogram {
	return register(prometheus.NewHistogram(o))
}

// NewHistogramVec is promauto.NewHistogramVec through auto. allow:wrapper (collects for the one registration)
func (factory) NewHistogramVec(o prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	return register(prometheus.NewHistogramVec(o, labels))
}

func register[C prometheus.Collector](c C) C {
	all = append(all, c)
	return c
}
