// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"net/http"
	"testing"

	"github.com/nfltr/relay/proto/pb"
)

// --- SyncMap benchmarks ---

func BenchmarkSyncMap_Store(b *testing.B) {
	var m SyncMap[string, int]
	for i := 0; i < b.N; i++ {
		m.Store("key", i)
	}
}

func BenchmarkSyncMap_Load_Hit(b *testing.B) {
	var m SyncMap[string, int]
	m.Store("key", 42)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Load("key")
	}
}

func BenchmarkSyncMap_Load_Miss(b *testing.B) {
	var m SyncMap[string, int]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Load("missing")
	}
}

func BenchmarkSyncMap_StoreAndLoad(b *testing.B) {
	var m SyncMap[string, int]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				m.Store("key", i)
			} else {
				m.Load("key")
			}
			i++
		}
	})
}

func BenchmarkSyncMap_Range(b *testing.B) {
	var m SyncMap[string, int]
	for i := range 100 {
		m.Store(string(rune('a'+i)), i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Range(func(_ string, _ int) bool {
			return true
		})
	}
}

// --- WorkerPool benchmarks ---

func BenchmarkWorkerPool_SingleWorker(b *testing.B) {
	ch := make(chan int, 128)
	pool := NewWorkerPool(ch, 1, func(_ context.Context, _ int) error {
		return nil
	})

	go func() {
		for i := 0; i < b.N; i++ {
			ch <- i
		}
		close(ch)
	}()

	b.ResetTimer()
	_ = pool.Run(context.Background())
}

func BenchmarkWorkerPool_Parallel(b *testing.B) {
	for _, workers := range []int{1, 4, 8, 16} {
		b.Run(workerLabel(workers), func(b *testing.B) {
			ch := make(chan int, 128)
			pool := NewWorkerPool(ch, workers, func(_ context.Context, _ int) error {
				return nil
			})

			go func() {
				for i := 0; i < b.N; i++ {
					ch <- i
				}
				close(ch)
			}()

			b.ResetTimer()
			_ = pool.Run(context.Background())
		})
	}
}

func workerLabel(n int) string {
	switch n {
	case 1:
		return "workers=1"
	case 4:
		return "workers=4"
	case 8:
		return "workers=8"
	default:
		return "workers=16"
	}
}

// --- HeadersToProto benchmark ---

func BenchmarkHeadersToProto(b *testing.B) {
	headers := http.Header{
		"Content-Type":    {"application/json"},
		"Authorization":   {"Bearer token123"},
		"X-Request-Id":    {"abc-def-ghi"},
		"Accept":          {"text/html", "application/json"},
		"X-Custom-Header": {"value1"},
	}
	skip := map[string]SkipHeaderIf{
		"Authorization": SkipHeaderAlways,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HeadersToProto(headers, skip)
	}
}

func BenchmarkCopyProtoHeadersToHTTPHeaders(b *testing.B) {
	protoHeaders := map[string]*pb.HeaderValueList{
		"Content-Type": {HeaderValues: []string{"application/json"}},
		"X-Request-Id": {HeaderValues: []string{"abc-def-ghi"}},
		"Accept":       {HeaderValues: []string{"text/html", "application/json"}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := make(http.Header)
		CopyProtoHeadersToHTTPHeaders(protoHeaders, h)
	}
}

// --- BytesToMessage / ProtoToMessage benchmarks ---

func BenchmarkBytesToMessage(b *testing.B) {
	buf := make([]byte, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BytesToTunnelFrame(buf, "TestMessage", "agentID-123", false)
	}
}

func BenchmarkProtoToMessage(b *testing.B) {
	msg := &pb.HTTPProxyResponse{
		Timestamps: &pb.Timestamps{
			ResponseAtAgent: 12345,
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ProtoToTunnelFrame(msg, "HTTPProxyResponse", "agentID-123", false)
	}
}

// --- Cast benchmark ---

func BenchmarkCast_Hit(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Cast[int](42, true)
	}
}

func BenchmarkCast_Miss(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Cast[int](nil, false)
	}
}
