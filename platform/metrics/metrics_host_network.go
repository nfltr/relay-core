// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// hostNetwork serves nfltr_relay_host_network_bytes_total, read from
// /proc/net/dev at each scrape (nothing runs between scrapes): the bytes
// the host's own network interfaces moved. process_network_* (the process
// collector) is every interface of the namespace, loopback included, so
// it counts the relay's local hops (services, the access agent, the TLS
// front) as traffic; this counts what left and reached the machine.
// Linux only: elsewhere it serves nothing.
var hostNetwork = register(&hostNetworkCollector{
	desc: prometheus.NewDesc("nfltr_relay_host_network_bytes_total",
		"Bytes the host's physical network interfaces received and transmitted (/proc/net/dev; loopback, bridges, veth, tun and WireGuard interfaces left out; where the relay sees no physical interface, as in a container, every interface but loopback). On a relay that is its machine's only service, transmit is its egress.",
		[]string{"direction"}, nil),
	procNetDev: "/proc/net/dev",
	physical: func(iface string) bool {
		_, err := os.Stat("/sys/class/net/" + iface + "/device")
		return err == nil
	},
})

type hostNetworkCollector struct {
	desc       *prometheus.Desc
	procNetDev string
	physical   func(iface string) bool
}

func (c *hostNetworkCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *hostNetworkCollector) Collect(ch chan<- prometheus.Metric) {
	f, err := os.Open(c.procNetDev)
	if err != nil {
		return // not Linux, or no procfs: no series
	}
	defer f.Close() //nolint:errcheck
	rx, tx, ok := hostNetBytes(f, c.physical)
	if !ok {
		return
	}
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.CounterValue, float64(rx), "receive")
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.CounterValue, float64(tx), "transmit")
}

// hostNetBytes sums /proc/net/dev's received and transmitted bytes over
// the physical interfaces, or over every interface but loopback when none
// is physical; ok is false when it lists no interface.
func hostNetBytes(r io.Reader, physical func(string) bool) (rx, tx uint64, ok bool) {
	var all, phys [2]uint64
	var any, anyPhys bool
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		name, rest, found := strings.Cut(sc.Text(), ":")
		if !found {
			continue // the two header lines
		}
		name = strings.TrimSpace(name)
		f := strings.Fields(rest)
		if name == "lo" || len(f) < 9 {
			continue
		}
		in, err1 := strconv.ParseUint(f[0], 10, 64)
		out, err2 := strconv.ParseUint(f[8], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		any = true
		all[0], all[1] = all[0]+in, all[1]+out
		if physical(name) {
			anyPhys = true
			phys[0], phys[1] = phys[0]+in, phys[1]+out
		}
	}
	if anyPhys {
		return phys[0], phys[1], true
	}
	return all[0], all[1], any
}
