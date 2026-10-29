// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package requestid

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func BenchmarkHandleRequestID_Existing(b *testing.B) {
	md := metadata.Pairs(DefaultRequestIDKey, "existing-request-id")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = handleRequestID(ctx)
	}
}

func BenchmarkHandleRequestID_Generate(b *testing.B) {
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = handleRequestID(ctx)
	}
}

func BenchmarkUnaryServerInterceptor(b *testing.B) {
	interceptor := UnaryServerInterceptor()
	md := metadata.Pairs(DefaultRequestIDKey, "test-req-id")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = interceptor(ctx, nil, info, handler)
	}
}
