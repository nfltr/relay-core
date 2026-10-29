// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package repocheck

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestReadsIncomingMetadataByKey: the relay reads a stream's request
// metadata one key at a time (metadata.ValueFromIncomingContext), never by
// copying it whole to read a key: metadata.FromIncomingContext copies the
// map and every value (an agent stream's ~16 entries: a 1408-byte map plus
// a slice per key), and per-stream setup called it ~20 times. That garbage,
// allocated beside each agent's long-lived metadata, held a partly empty
// span per idle agent. Only the files below take the whole map: they
// enrich or strip it (a copy the stream keeps) or read many keys.
func TestReadsIncomingMetadataByKey(t *testing.T) {
	wholeMap := []string{
		"grpcauth/apikey_interceptor.go",            // enriches the metadata with the key's identity
		"grpcauth/asserted_identity_interceptor.go", // strips client-asserted identity
		"grpcauth/trust_loopback_interceptor.go",    // enriches with the loopback identity
		"internal/platform/tcprig/tcprig.go",        // the test rig strips headers an older relay or agent lacks
	}
	var copying []string
	err := filepath.WalkDir("../..", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "testdata" || strings.HasPrefix(d.Name(), ".") && d.Name() != "..") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(src), "metadata.FromIncomingContext(") {
			rel, _ := filepath.Rel("../..", path)
			copying = append(copying, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range copying {
		if !slices.Contains(wholeMap, p) {
			t.Errorf("%s copies the incoming metadata whole (metadata.FromIncomingContext): read the keys it needs with metadata.ValueFromIncomingContext", p)
		}
	}
	for _, p := range wholeMap {
		if !slices.Contains(copying, p) {
			t.Errorf("%s no longer copies the incoming metadata: drop it from the list", p)
		}
	}
}
