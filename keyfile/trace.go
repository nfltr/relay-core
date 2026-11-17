// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

// event is one step of spec/KeyAccount.tla as the recorder writes it
// (trace_on.go; spec/TraceKeyAccount.tla reads it): m a machine id, n its
// name, a its account ("" in a MintCode: its own), c a code's id, k a
// key's id, ok an Auth's answer.
type event struct {
	Ev string `json:"ev"`
	M  string `json:"m"`
	N  string `json:"n"`
	A  string `json:"a"`
	C  string `json:"c"`
	K  string `json:"k"`
	OK bool   `json:"ok"`
}
