// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory_test

import (
	"context"
	"fmt"

	"github.com/nfltr/relay-core/core/directory"
)

// ExampleGetDispatcherCacheKey shows the cache key format for dispatchers.
func ExampleGetDispatcherCacheKey() {
	key := directory.GetDispatcherCacheKey("abc-123")
	fmt.Println(key)
	// Output: dipi:agentID:abc-123
}

// ExampleGetStreamerCacheKey shows the cache key format for streamers.
func ExampleGetStreamerCacheKey() {
	key := directory.GetStreamerCacheKey("abc-123")
	fmt.Println(key)
	// Output: sipi:agentID:abc-123
}

// ExampleNewNoOpInventory demonstrates the no-op inventory for async mode.
func ExampleNewNoOpInventory() {
	inv := directory.NewNoOpInventory()

	ip, err := inv.GetIP(context.Background(), "any-agentID")
	fmt.Printf("ip=%q err=%v\n", ip, err)
	// Output: ip="" err=<nil>
}
