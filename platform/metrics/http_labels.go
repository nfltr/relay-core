// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import "net/http"

// The HTTP method and status a request's labels carry are input: a client
// may send any token as its method, and an agent's response carries the
// status the agent wrote. Bounded here, where every HTTP label passes, so
// no caller can grow /metrics by sending new values.

// httpMethod is method as a label value: a method net/http names, else
// "other".
func httpMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions, http.MethodConnect, http.MethodTrace:
		return method
	}
	return "other"
}

// httpStatus is status as a label value: a three-digit code (100 to 999),
// else "other".
func httpStatus(status string) string {
	if len(status) == 3 && status[0] >= '1' && status[0] <= '9' &&
		status[1] >= '0' && status[1] <= '9' && status[2] >= '0' && status[2] <= '9' {
		return status
	}
	return "other"
}
