// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"golang.org/x/sys/unix"

	"github.com/nfltr/link"
)

// netinet/tcp_fsm.h.
const (
	tcpsClosed   = 0
	tcpsLastAck  = 8
	tcpsFinWait2 = 9
	tcpsTimeWait = 10
)

// tcpEnd reads macOS's TCP_CONNECTION_INFO: FIN_WAIT_2 or TIME_WAIT once
// the FIN was acknowledged; CLOSED after an RST, and also from LAST_ACK
// on the ACK: there an ACK and an RST look alike (errEndUnknown).
type tcpEnd struct{ last uint8 }

func (t *tcpEnd) finAcked(fd uintptr, _ int64) (bool, uint64, error) {
	ci, err := unix.GetsockoptTCPConnectionInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_CONNECTION_INFO)
	if err != nil {
		return false, 0, errEndUnknown
	}
	last := t.last
	t.last = ci.State
	switch ci.State {
	case tcpsFinWait2, tcpsTimeWait:
		return true, 0, nil
	case tcpsClosed:
		if last == tcpsLastAck {
			return false, 0, errEndUnknown
		}
		return false, 0, link.ErrReset
	}
	return false, uint64(ci.Snd_sbbytes), nil // the send buffer drains: progress
}
