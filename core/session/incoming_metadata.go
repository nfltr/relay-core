// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

// incomingKeys is ctx's request metadata limited to keys: those values
// copied, not the whole map with every value as metadata.FromIncomingContext
// copies it (TestReadsIncomingMetadataByKey).
func incomingKeys(ctx context.Context, keys ...string) metadata.MD {
	md := make(metadata.MD, len(keys))
	for _, k := range keys {
		k = strings.ToLower(k)
		if v := metadata.ValueFromIncomingContext(ctx, k); len(v) > 0 {
			md[k] = v
		}
	}
	return md
}

// firstIncoming is the first value of key in ctx's request metadata, or "".
func firstIncoming(ctx context.Context, key string) string {
	if v := metadata.ValueFromIncomingContext(ctx, key); len(v) > 0 {
		return v[0]
	}
	return ""
}

// FirstMD returns the first value of a metadata key, or "".
func FirstMD(md metadata.MD, key string) string {
	if vals := md.Get(key); len(vals) > 0 {
		return vals[0]
	}
	return ""
}
