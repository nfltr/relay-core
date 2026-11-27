// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package util_test

import (
	"context"
	"fmt"

	"github.com/nfltr/relay-core/platform/util"
)

// ExampleSyncMap demonstrates typed concurrent map operations.
func ExampleSyncMap() {
	var m util.SyncMap[string, int]

	m.Store("counter", 1)
	m.Store("limit", 100)

	val, ok := m.Load("counter")
	fmt.Printf("counter=%d ok=%v\n", val, ok)

	m.Delete("counter")
	_, ok = m.Load("counter")
	fmt.Printf("after delete ok=%v\n", ok)

	m.Store("a", 10)
	m.Store("b", 20)
	sum := 0
	m.Range(func(_ string, v int) bool {
		sum += v
		return true
	})
	fmt.Printf("sum=%d\n", sum)
	// Output:
	// counter=1 ok=true
	// after delete ok=false
	// sum=130
}

// ExampleNewWorkerPool demonstrates parallel processing of work items.
func ExampleNewWorkerPool() {
	items := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		items <- i
	}
	close(items)

	results := make(chan int, 5)
	pool := util.NewWorkerPool(items, 2, func(_ context.Context, item int) error {
		results <- item * item
		return nil
	})

	err := pool.Run(context.Background())
	close(results)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	sum := 0
	for r := range results {
		sum += r
	}
	fmt.Println("sum of squares:", sum)
	// Output: sum of squares: 55
}
