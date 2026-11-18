// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"strings"

	"github.com/nfltr/relay-core/core/identity"
)

// The guard binary's credential: one token for one machine, the binary's
// own provider (as examples/custom-auth's): the key file authenticates
// machine certificates only (nfltr/nfltr
// docs/decisions/api-key-cut-2026-11-15.md), which this binary's plain
// gRPC port cannot ask for. RELAY_CORE_TOKEN ("<machine id> <name>
// <token>") names it, else the last line of RELAY_CORE_TOKEN_FILE
// (default zeroTokenFile).
const (
	tokenEnv      = "RELAY_CORE_TOKEN"
	tokenFileEnv  = "RELAY_CORE_TOKEN_FILE"
	zeroTokenFile = "nfltr-relay-core.token"
)

// token authenticates its one secret as machine m (its own account).
type token struct {
	secret string
	m      identity.MachineID
}

func (token) Name() string { return "token" }

func (t token) Authenticate(_ context.Context, c identity.Credential) (identity.Principal, error) {
	if c.Token == "" || subtle.ConstantTimeCompare([]byte(c.Token), []byte(t.secret)) != 1 {
		return identity.Principal{}, identity.ErrUnknownCredential
	}
	return identity.Principal{Owner: string(t.m), Machine: t.m, Subject: string(t.m)}, nil
}

// tokenFile is the token file's path.
func tokenFile() string {
	if f := os.Getenv(tokenFileEnv); f != "" {
		return f
	}
	return zeroTokenFile
}

// loadToken reads RELAY_CORE_TOKEN, else the token file's last line.
func loadToken() (token, error) {
	v := os.Getenv(tokenEnv)
	if v == "" {
		data, err := os.ReadFile(tokenFile()) // #nosec G304 -- the operator's token file
		if err != nil {
			return token{}, fmt.Errorf("no token: set %s or %s: %w", tokenEnv, tokenFileEnv, err)
		}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		v = lines[len(lines)-1]
	}
	f := strings.Fields(v)
	if len(f) != 3 {
		return token{}, errors.New(`a token is "<machine id> <name> <token>"`)
	}
	id, err := identity.ParseMachineID(f[0])
	if err != nil {
		return token{}, err
	}
	return token{secret: f[2], m: id}, nil
}

// writeZeroToken creates the token file 0600 with one generated machine
// "quickstart" when it does not exist (a local agent and client read it).
func writeZeroToken(logger *slog.Logger) error {
	path := tokenFile()
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		return err // it exists (nil), or it cannot be read
	}
	secret := make([]byte, 24)
	if _, err := rand.Read(secret); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- the operator's token file path
	if err != nil {
		return err
	}
	id := identity.NewMachineID()
	_, err = fmt.Fprintf(f, "# nfltr-relay-core zero-config machine quickstart (%s): agents and clients send its token\n%s quickstart tok_%s\n", id, id, hex.EncodeToString(secret))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		logger.Info("generated a token", "file", path)
	}
	return err
}
