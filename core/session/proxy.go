// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"github.com/nfltr/relay/proto/wire/chunking"

	"github.com/nfltr/relay/proto/pb"
)

// IsUploadAck reports whether an agent message is an upload ack
// (chunking.AckResponse): no status, no chunk headers. A response whose
// backend happened to set the ack header has a status, so it is still
// relayed.
func IsUploadAck(r *pb.HTTPResponse) bool {
	h := r.GetHeaders()
	return r.GetStatusCode() == 0 && !chunking.IsChunked(h) && chunking.IsAck(h)
}
