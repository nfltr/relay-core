// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nfltr/relay-core/keyfile"
)

// A key file holds one machine per line: "<machine id> <name>
// spki:<hex of its public key's SHA-256>". A machine joins with a
// single-use code (KeyManager.MintCode, Enrol); the provider authenticates
// its certificate, signed by the data dir's machine CA, as that machine,
// whose agents are its own id's refs. The file holds no secret.
func Example() {
	dir, err := os.MkdirTemp("", "keyfile")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir) //nolint:errcheck
	path := filepath.Join(dir, "api-keys")
	sum := sha256.Sum256([]byte("the machine key's SubjectPublicKeyInfo"))
	line := "m-aaaaaaaaaaaaaaaaaaaaaaaaaa web spki:" + hex.EncodeToString(sum[:]) + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		panic(err)
	}

	store, err := keyfile.Load(path)
	if err != nil {
		panic(err)
	}
	fmt.Println(store.Count())
	// Output:
	// 1
}
