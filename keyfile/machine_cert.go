// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nfltr/relay-core/core/identity"
)

// Machine certificates of a relay without a database (lane L5, nfltr
// docs/decisions/api-key-cut-2026-11-15.md "L5 SELFHOST design";
// spec/KeyAccount.tla MintCode, Enrol, Reissue, Authenticates): a
// single-use code enrols a machine whose line binds the key its
// certificate certifies (spki:). The CA that signs lives with the relay
// (relay-server selfhost); this package keeps the lines and the codes.

// MachineTrustDomain is the SPIFFE trust domain of machine ids, as on the
// hosted relay (one URI SAN spiffe://<domain>/m/<machine id>).
const MachineTrustDomain = "nfltr.xyz"

// ErrEnrolRefused refuses a code that is unknown, spent or expired.
var ErrEnrolRefused = errors.New("enrol_refused: the enrolment code is unknown, used up or expired; mint another with nfltr-relay machines code")

// codesDir holds one file per outstanding code, named by the code's
// SHA-256: minting creates it (O_EXCL), enrolling removes it, and a remove
// succeeds once, so a code enrols at most one machine (EnrolCodeSingleUse)
// with no lock shared between the relay and `nfltr-relay machines code`.
func (km *KeyManager) codesDir() string {
	return filepath.Join(filepath.Dir(km.filePath), "enrol-codes")
}

func (km *KeyManager) codePath(code string) string {
	h := sha256.Sum256([]byte(strings.TrimSpace(code)))
	return filepath.Join(km.codesDir(), hex.EncodeToString(h[:]))
}

// MintCode makes a single-use code for a machine named name, in the
// account of member (a machine the file holds, by id or name) or, with
// member "", an account of its own; it lasts ttl from now. The code is
// returned once; only its SHA-256 is kept.
func (km *KeyManager) MintCode(name, member string, ttl time.Duration, now time.Time) (string, error) {
	if !identity.ValidMachineName(name) {
		return "", fmt.Errorf("%w: %q", ErrMachineName, name)
	}
	if ttl <= 0 {
		return "", errors.New("keyfile: a code's lifetime must be positive")
	}
	account := "-"
	if member != "" {
		km.mu.Lock()
		a, err := km.accountOfMember(member)
		km.mu.Unlock()
		if err != nil {
			return "", err
		}
		account = string(a)
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	code := "enr_" + hex.EncodeToString(b)
	if err := os.MkdirAll(km.codesDir(), 0o700); err != nil { // nosemgrep: incorrect-default-permission -- a directory: owner only, rwx
		return "", err
	}
	f, err := os.OpenFile(km.codePath(code), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- a file in the operator's data dir
	if err != nil {
		return "", err
	}
	_, werr := fmt.Fprintf(f, "%s %s %d\n", name, account, now.Add(ttl).Unix())
	if err := errors.Join(werr, f.Close()); err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	if account == "-" {
		account = ""
	}
	km.store.trace(event{Ev: "MintCode", C: codeID(code), N: name, A: account})
	return code, nil
}

// Enrol spends code and adds its machine with a fresh id, its line binding
// pub (spec Enrol): the code's file is removed first, so a crash after it
// leaves the code spent and no machine (fail closed: mint another). The
// caller then has the CA sign a certificate for the returned machine and
// pub.
func (km *KeyManager) Enrol(code string, pub crypto.PublicKey, now time.Time) (Machine, error) {
	spki, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return Machine{}, fmt.Errorf("keyfile: machine key: %w", err)
	}
	km.mu.Lock()
	defer km.mu.Unlock()
	path := km.codePath(code)
	raw, err := os.ReadFile(path) // #nosec G304 -- named by the code's hash in the operator's data dir
	if err != nil {
		return Machine{}, ErrEnrolRefused
	}
	f := strings.Fields(string(raw))
	if len(f) != 3 {
		return Machine{}, ErrEnrolRefused
	}
	until, err := strconv.ParseInt(f[2], 10, 64)
	if err != nil {
		return Machine{}, ErrEnrolRefused
	}
	m := Machine{ID: identity.NewMachineID(), Name: f[0], Cert: true}
	if f[1] != "-" {
		m.Account = identity.MachineID(f[1])
	}
	if err := validMachine(m); err != nil {
		return Machine{}, ErrEnrolRefused
	}
	for _, r := range km.records {
		if r.Name == m.Name && r.Account == m.AccountOf() {
			return Machine{}, fmt.Errorf("keyfile: a machine named %q exists in this account; revoke it first", m.Name)
		}
	}
	if err := os.Remove(path); err != nil { // the single use: one remove succeeds
		return Machine{}, ErrEnrolRefused
	}
	if now.Unix() > until {
		return Machine{}, ErrEnrolRefused
	}
	h := sha256.Sum256(spki)
	km.store.addHash(m, h, event{Ev: "Enrol", C: codeID(code), M: string(m.ID), K: keyID(h), N: m.Name, A: string(m.AccountOf())})
	if err := km.appendLine(m, h); err != nil {
		km.store.Remove(m.ID)
		return Machine{}, fmt.Errorf("persist machine: %w", err)
	}
	km.records[m.ID] = KeyRecord{MachineID: m.ID, Name: m.Name, Account: m.AccountOf(), KeyPrefix: spkiPrefix + hex.EncodeToString(h[:4]) + "...", CreatedAt: now.UTC()}
	km.logger.Info("security: enrolled machine", "machine_id", m.ID, "name", m.Name, "account", m.AccountOf())
	return m, nil
}

// Rebind makes machine id's line bind pub (spec Reissue): the same key
// renews, a new one rotates, and the old key's certificates authenticate
// nothing from then on (OnRevoke runs for id: its connections end). The caller has authenticated id's current
// certificate (MachineByCert) and checked the rotation proof.
func (km *KeyManager) Rebind(id identity.MachineID, pub crypto.PublicKey) error {
	spki, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return fmt.Errorf("keyfile: machine key: %w", err)
	}
	h := sha256.Sum256(spki)
	km.mu.Lock()
	defer km.mu.Unlock()
	km.store.mu.Lock()
	var m Machine
	var old [32]byte
	found := false
	for k, x := range km.store.keys {
		if x.ID == id && x.Cert {
			m, old, found = x, k, true
		}
	}
	if found && old != h {
		delete(km.store.keys, old)
		km.store.keys[h] = m
	}
	if found {
		km.store.trace(event{Ev: "Reissue", M: string(id), K: keyID(h)})
	}
	km.store.mu.Unlock()
	if !found {
		return identity.ErrUnknownCredential
	}
	if old == h {
		return nil
	}
	// The old key's connections end and the core's cached answers go, as
	// for a revoke: the machine reconnects with the new certificate.
	if km.onRevoke != nil {
		km.onRevoke(string(id))
	}
	return km.rewriteFile()
}

// MachineByCert is the certificate machine chain (leaf first) names: the
// chain verifies against roots at now for client auth, the leaf carries
// one machine id, and that machine's line binds the leaf's key (spec
// Authenticates). Anything else is identity.ErrUnknownCredential.
func (s *APIKeyStore) MachineByCert(roots *x509.CertPool, chain []*x509.Certificate, now time.Time) (Machine, error) {
	leaf := chain[0]
	inter := x509.NewCertPool()
	for _, c := range chain[1:] {
		inter.AddCert(c)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return Machine{}, identity.ErrUnknownCredential
	}
	id, ok := MachineIDOfCert(leaf)
	if !ok {
		return Machine{}, identity.ErrUnknownCredential
	}
	h := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
	s.mu.RLock()
	m, found := s.keys[h]
	ok = found && m.Cert && m.ID == id
	s.trace(event{Ev: "Auth", M: string(id), K: keyID(h), OK: ok}) // under the read lock: the answer's state
	s.mu.RUnlock()
	if !ok {
		return Machine{}, identity.ErrUnknownCredential
	}
	return m, nil
}

// MachineURI is id's SPIFFE id: the one URI SAN of its certificate.
func MachineURI(id identity.MachineID) *url.URL {
	return &url.URL{Scheme: "spiffe", Host: MachineTrustDomain, Path: "/m/" + string(id)}
}

// MachineIDOfCert is the machine id c carries: its one URI SAN, of the
// trust domain and the fixed path, and no other SAN.
func MachineIDOfCert(c *x509.Certificate) (identity.MachineID, bool) {
	if len(c.URIs) != 1 || len(c.DNSNames) != 0 || len(c.EmailAddresses) != 0 || len(c.IPAddresses) != 0 {
		return "", false
	}
	u := c.URIs[0]
	rest, ok := strings.CutPrefix(u.Path, "/m/")
	if u.Scheme != "spiffe" || u.Host != MachineTrustDomain || !ok || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return "", false
	}
	id, err := identity.ParseMachineID(rest)
	return id, err == nil
}

// addHash adds machine m under hash h (a key's, or a cert machine's SPKI);
// e is its trace step, in the same critical section.
func (s *APIKeyStore) addHash(m Machine, h [32]byte, e event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[h] = m
	s.trace(e)
}
