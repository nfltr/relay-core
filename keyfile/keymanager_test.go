// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testKeyManagerLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// ── KeyManager Generate ─────────────────────────────────────────────────────

func TestKeyManager_Generate(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "api-keys")

	store := NewAPIKeyStore()
	km := NewKeyManager(keyFile, store, testKeyManagerLogger())

	resp, err := km.Generate("agent-east")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.AgentID != "agent-east" {
		t.Fatalf("expected agentID 'agent-east', got %q", resp.AgentID)
	}
	if !strings.HasPrefix(resp.APIKey, "rpc_") {
		t.Fatalf("expected key to start with 'rpc_', got %q", resp.APIKey)
	}
	// Key should be 4 (prefix) + 96 (hex) = 100 chars
	if len(resp.APIKey) != 100 {
		t.Fatalf("expected key length 100, got %d", len(resp.APIKey))
	}

	// Key should be usable for authentication
	agentID, ok := store.Validate(resp.APIKey)
	if !ok {
		t.Fatal("generated key should be valid in store")
	}
	if agentID != "agent-east" {
		t.Fatalf("expected agentID 'agent-east', got %q", agentID)
	}

	// Key file should exist and contain the hashed entry (never plaintext)
	data, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatalf("read key file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "agent-east:sha256:") {
		t.Fatalf("expected key file to contain hashed entry, got: %s", content)
	}
	// The plaintext key should NOT appear in the file
	if strings.Contains(content, resp.APIKey) {
		t.Fatal("plaintext key should NOT appear in the key file")
	}
}

func TestKeyManager_Generate_EmptyAgentID(t *testing.T) {
	store := NewAPIKeyStore()
	km := NewKeyManager("", store, testKeyManagerLogger())

	_, err := km.Generate("")
	if err == nil {
		t.Fatal("expected error for empty agentID")
	}
}

// An agent ID the key file cannot hold (agentID:sha256:<hex> per line) is
// refused: ':' or a line break would re-key or add an agent on the next
// load, surrounding space is trimmed away by it, '#' starts a comment.
// Found by Schemathesis (tests/conformance/schemathesis.sh) on POST /api/v1/keys.
func TestKeyManager_Generate_RejectsAgentIDTheKeyFileCannotHold(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "api-keys")
	km := NewKeyManager(keyFile, NewAPIKeyStore(), testKeyManagerLogger())
	for _, id := range []string{"a:b", "victim:sha256:00\nx", "x\nvictim", " lead", "trail\t", "#comment", "nul\x00", "ok\u0085"} {
		if _, err := km.Generate(id); err == nil {
			t.Errorf("Generate(%q) accepted", id)
		}
	}
	if _, err := km.Generate("agent-east.v2@team_1"); err != nil {
		t.Fatalf("plain agent ID refused: %v", err)
	}
	if _, err := LoadAPIKeys(keyFile); err != nil {
		t.Fatalf("key file no longer loads: %v", err)
	}
}

func TestKeyManager_Generate_Duplicate(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "api-keys")

	store := NewAPIKeyStore()
	km := NewKeyManager(keyFile, store, testKeyManagerLogger())

	_, err := km.Generate("agent-east")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = km.Generate("agent-east")
	if err == nil {
		t.Fatal("expected error for duplicate agentID")
	}
	if !strings.Contains(err.Error(), "already has an API key") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

// ── KeyManager Revoke ───────────────────────────────────────────────────────

func TestKeyManager_Revoke(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "api-keys")

	store := NewAPIKeyStore()
	km := NewKeyManager(keyFile, store, testKeyManagerLogger())

	resp, _ := km.Generate("agent-east")
	apiKey := resp.APIKey

	// Verify key works
	_, ok := store.Validate(apiKey)
	if !ok {
		t.Fatal("key should be valid before revoke")
	}

	// Revoke
	if err := km.Revoke("agent-east"); err != nil {
		t.Fatalf("revoke error: %v", err)
	}

	// Key should no longer work
	_, ok = store.Validate(apiKey)
	if ok {
		t.Fatal("key should be invalid after revoke")
	}

	// File should not contain the entry
	data, _ := os.ReadFile(keyFile)
	if strings.Contains(string(data), apiKey) {
		t.Fatal("key file should not contain revoked key")
	}
}

func TestKeyManager_Revoke_NotFound(t *testing.T) {
	store := NewAPIKeyStore()
	km := NewKeyManager("", store, testKeyManagerLogger())

	err := km.Revoke("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent agentID")
	}
}

// ── KeyManager List ─────────────────────────────────────────────────────────

func TestKeyManager_List(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "api-keys")

	store := NewAPIKeyStore()
	km := NewKeyManager(keyFile, store, testKeyManagerLogger())

	_, _ = km.Generate("agent-east")
	_, _ = km.Generate("agent-west")

	records := km.List()
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	agentIDs := map[string]bool{}
	for _, r := range records {
		agentIDs[r.AgentID] = true
		if r.KeyPrefix == "" {
			t.Fatal("expected non-empty key prefix")
		}
	}
	if !agentIDs["agent-east"] || !agentIDs["agent-west"] {
		t.Fatal("expected both agent-east and agent-west in list")
	}
}

// ── APIKeyStore Remove ──────────────────────────────────────────────────────

func TestAPIKeyStore_Remove(t *testing.T) {
	store := NewAPIKeyStore()
	store.Add(testAgentID, "key-aaa")
	store.Add("agent-2", "key-bbb")

	store.Remove(testAgentID)

	_, ok := store.Validate("key-aaa")
	if ok {
		t.Fatal("expected key-aaa to be invalid after remove")
	}
	_, ok = store.Validate("key-bbb")
	if !ok {
		t.Fatal("expected key-bbb to still be valid")
	}
}

// ── HTTP Handlers ───────────────────────────────────────────────────────────

// ── End-to-end: generate → authenticate → revoke → fail auth ───────────────

func TestKeyManager_EndToEnd(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "api-keys")
	store := NewAPIKeyStore()
	km := NewKeyManager(keyFile, store, testKeyManagerLogger())

	// 1. Generate key for agent
	resp, err := km.Generate(testAgentID2)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	// 2. Agent authenticates
	agentID, ok := store.Validate(resp.APIKey)
	if !ok || agentID != testAgentID2 {
		t.Fatal("authentication should succeed")
	}

	// 3. Revoke
	if err := km.Revoke(testAgentID2); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	// 4. Authentication should fail
	_, ok = store.Validate(resp.APIKey)
	if ok {
		t.Fatal("authentication should fail after revocation")
	}

	// 5. Can generate a new key for the same AgentID
	resp2, err := km.Generate(testAgentID2)
	if err != nil {
		t.Fatalf("re-generate: %v", err)
	}
	if resp2.APIKey == resp.APIKey {
		t.Fatal("new key should be different from old key")
	}

	// 6. New key works
	agentID, ok = store.Validate(resp2.APIKey)
	if !ok || agentID != testAgentID2 {
		t.Fatal("new key should authenticate")
	}

	// 7. Verify persistence: reload from file
	store2, err := LoadAPIKeys(keyFile)
	if err != nil {
		t.Fatalf("reload key file: %v", err)
	}
	agentID, ok = store2.Validate(resp2.APIKey)
	if !ok || agentID != testAgentID2 {
		t.Fatal("reloaded store should contain the new key")
	}
}
