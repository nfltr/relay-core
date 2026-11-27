// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin

package session

// tcpEnd: this OS does not tell an ACK of the FIN (errEndUnknown).
type tcpEnd struct{}

func (tcpEnd) finAcked(uintptr, int64) (bool, uint64, error) { return false, 0, errEndUnknown }
