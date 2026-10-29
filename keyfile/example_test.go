// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/keyfile"
)

// A key file holds one "agentID:sha256:<hex of the key's SHA-256>" (or,
// legacy, "agentID:key") per line; the store keeps only the keys' hashes,
// and the provider authenticates a credential against it.
func Example() {
	dir, err := os.MkdirTemp("", "keyfile")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir) //nolint:errcheck
	path := filepath.Join(dir, "api-keys")
	sum := sha256.Sum256([]byte("rpc_examplekeyaaaaaaaaaaaaaaaaaaaaaaaaa"))
	if err := os.WriteFile(path, []byte("web:sha256:"+hex.EncodeToString(sum[:])+"\n"), 0o600); err != nil {
		panic(err)
	}

	store, err := keyfile.LoadAPIKeys(path)
	if err != nil {
		panic(err)
	}
	id, ok := store.Validate("rpc_examplekeyaaaaaaaaaaaaaaaaaaaaaaaaa")
	fmt.Println(id, ok)
	_, ok = store.Validate("rpc_wrong")
	fmt.Println(ok)

	p, err := keyfile.Provider{Store: store}.Authenticate(context.Background(), identity.Credential{Token: "rpc_examplekeyaaaaaaaaaaaaaaaaaaaaaaaaa"})
	fmt.Println(p.Subject, err)
	// Output:
	// web true
	// false
	// web <nil>
}

// ExampleAPIKeyStore_Add adds a key to an empty store.
func ExampleAPIKeyStore_Add() {
	store := keyfile.NewAPIKeyStore()
	store.Add("web", "rpc_examplekeyaaaaaaaaaaaaaaaaaaaaaaaaa")
	fmt.Println(store.Count())
	id, ok := store.Validate("rpc_examplekeyaaaaaaaaaaaaaaaaaaaaaaaaa")
	fmt.Println(id, ok)
	// Output:
	// 1
	// web true
}
