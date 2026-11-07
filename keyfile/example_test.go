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

// A key file holds one machine per line: "<machine id> <name>
// <service,...> sha256:<hex of the key's SHA-256>". The store keeps only
// the keys' hashes, and the provider authenticates a credential as its
// machine, which may register only its services.
func Example() {
	dir, err := os.MkdirTemp("", "keyfile")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir) //nolint:errcheck
	path := filepath.Join(dir, "api-keys")
	sum := sha256.Sum256([]byte("rpc_examplekeyaaaaaaaaaaaaaaaaaaaaaaaaa"))
	line := "m-aaaaaaaaaaaaaaaaaaaaaaaaaa web ssh,http sha256:" + hex.EncodeToString(sum[:]) + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		panic(err)
	}

	store, err := keyfile.LoadAPIKeys(path)
	if err != nil {
		panic(err)
	}
	m, ok := store.Validate("rpc_examplekeyaaaaaaaaaaaaaaaaaaaaaaaaa")
	fmt.Println(m.ID, m.Name, m.Services, ok)
	_, ok = store.Validate("rpc_wrong")
	fmt.Println(ok)

	p, err := keyfile.Provider{Store: store}.Authenticate(context.Background(), identity.Credential{Token: "rpc_examplekeyaaaaaaaaaaaaaaaaaaaaaaaaa"})
	fmt.Println(p.Machine, p.Names, err)
	// Output:
	// m-aaaaaaaaaaaaaaaaaaaaaaaaaa web [ssh http] true
	// false
	// m-aaaaaaaaaaaaaaaaaaaaaaaaaa [ssh http] <nil>
}
