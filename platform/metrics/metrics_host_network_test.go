// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strings"
	"testing"
)

const procNetDev = `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 9000000     100    0    0    0     0          0         0  9000000     100    0    0    0     0       0          0
  ens4: 1000        10    0    0    0     0          0         0     2000      20    0    0    0     0       0          0
docker0:  300         3    0    0    0     0          0         0      400       4    0    0    0     0       0          0
vethab12:  50         1    0    0    0     0          0         0       60       1    0    0    0     0       0          0
`

// TestHostNetBytesCountsPhysicalInterfacesOnly: loopback never counts; the
// physical interfaces do, bridges and veths only when none is physical.
func TestHostNetBytesCountsPhysicalInterfacesOnly(t *testing.T) {
	for name, tc := range map[string]struct {
		physical func(string) bool
		rx, tx   uint64
	}{
		"vm":        {func(i string) bool { return i == "ens4" }, 1000, 2000},
		"container": {func(string) bool { return false }, 1350, 2460},
	} {
		rx, tx, ok := hostNetBytes(strings.NewReader(procNetDev), tc.physical)
		if !ok || rx != tc.rx || tx != tc.tx {
			t.Errorf("%s: rx %d tx %d ok %v, want %d %d true", name, rx, tx, ok, tc.rx, tc.tx)
		}
	}
	if _, _, ok := hostNetBytes(strings.NewReader("Inter-|\n face |\n    lo: 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0\n"), func(string) bool { return true }); ok {
		t.Error("loopback alone: ok, want no series")
	}
}
