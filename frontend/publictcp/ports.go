// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package publictcp

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// ParseRange reads a port range "lo-hi" or a single port "p" (1-65535).
func ParseRange(s string) (lo, hi int, err error) {
	a, b, isRange := strings.Cut(s, "-")
	if !isRange {
		b = a
	}
	lo, err1 := strconv.Atoi(strings.TrimSpace(a))
	hi, err2 := strconv.Atoi(strings.TrimSpace(b))
	if err := errors.Join(err1, err2); err != nil {
		return 0, 0, fmt.Errorf("public tcp: port range %q: %w", s, err)
	}
	if lo < 1 || hi > 65535 || lo > hi {
		return 0, 0, fmt.Errorf("public tcp: port range %q: want 1 <= lo <= hi <= 65535", s)
	}
	return lo, hi, nil
}

// CheckEphemeral refuses a range overlapping the kernel's ephemeral range
// [elo, ehi]: a port the relay's own outbound connections took would be
// held when the range is bound.
func CheckEphemeral(lo, hi, elo, ehi int) error {
	if lo <= ehi && elo <= hi {
		return fmt.Errorf("public tcp: ports %d-%d overlap the ephemeral range %d-%d", lo, hi, elo, ehi)
	}
	return nil
}

// EphemeralRange is Linux's ip_local_port_range; elsewhere an error.
func EphemeralRange() (lo, hi int, err error) {
	b, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return 0, 0, err
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		return 0, 0, fmt.Errorf("ip_local_port_range: %q", b)
	}
	return ParseRange(f[0] + "-" + f[1])
}

// Bind listens on every port of [lo, hi] at host, or on none: a port held
// by another process fails the whole range (no SO_REUSEPORT: one relay
// owns each port, spec PortLease.tla NoDoubleBind).
func Bind(host string, lo, hi int) ([]net.Listener, error) {
	if lo < 1 || hi > 65535 || lo > hi {
		return nil, fmt.Errorf("public tcp: bind %d-%d: invalid range", lo, hi)
	}
	lns := make([]net.Listener, 0, hi-lo+1)
	for p := lo; p <= hi; p++ {
		ln, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(p)))
		if err != nil {
			closeAll(lns)
			return nil, fmt.Errorf("public tcp: bind %d-%d: %w", lo, hi, err)
		}
		lns = append(lns, ln)
	}
	return lns, nil
}

func closeAll(lns []net.Listener) {
	for _, ln := range lns {
		_ = ln.Close()
	}
}
