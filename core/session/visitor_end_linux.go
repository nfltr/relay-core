// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"golang.org/x/sys/unix"

	"github.com/nfltr/link"
)

// tcpEnd reads Linux's TCP_INFO: tcpi_bytes_acked covers the FIN once it
// reaches the bytes written + 1 (an RST never advances it; tcpi_unacked
// is 0 after an RST too).
type tcpEnd struct{}

func (tcpEnd) finAcked(fd uintptr, written int64) (bool, uint64, error) {
	ti, err := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	if err != nil {
		return false, 0, errEndUnknown
	}
	if ti.Bytes_acked >= uint64(written)+1 { // #nosec G115 -- written counts bytes written: never negative
		return true, ti.Bytes_acked, nil
	}
	if ti.State == tcpClose {
		return false, 0, link.ErrReset
	}
	return false, ti.Bytes_acked, nil
}

const tcpClose = 7 // include/net/tcp_states.h TCP_CLOSE
