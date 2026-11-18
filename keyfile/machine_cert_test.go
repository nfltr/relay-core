// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/identity"
)

// testCA signs machine certificates as the self-hosted relay's data-dir CA.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test machine CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(c)
	return &testCA{cert: c, key: key, pool: pool}
}

func (ca *testCA) sign(t *testing.T, id identity.MachineID, pub *ecdsa.PublicKey) []*x509.Certificate {
	t.Helper()
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(24 * time.Hour),
		URIs: []*url.URL{{Scheme: "spiffe", Host: MachineTrustDomain, Path: "/m/" + string(id)}}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	return []*x509.Certificate{c}
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func certManager(t *testing.T) (*KeyManager, Provider, *testCA) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "api-keys")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	km := NewKeyManager(file, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ca := newTestCA(t)
	return km, Provider{Store: km.store, CAs: ca.pool}, ca
}

// enrolled is a machine a code enrolled, with its certificate.
type enrolled struct {
	Machine
	chain []*x509.Certificate
}

// enrol mints a code for name (in member's account, or its own) and
// enrols a fresh key with it; ca signs the certificate.
func enrol(t *testing.T, km *KeyManager, ca *testCA, name, member string) enrolled {
	t.Helper()
	code, err := km.MintCode(name, member, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	k := newKey(t)
	m, err := km.Enrol(code, &k.PublicKey, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return enrolled{m, ca.sign(t, m.ID, &k.PublicKey)}
}

func certPrincipal(p Provider, chain []*x509.Certificate) (identity.Principal, error) {
	return p.Authenticate(context.Background(), identity.Credential{PeerCerts: chain})
}

// TestEnrolCodeSingleUse: a code enrols one machine (spec
// EnrolCodeSingleUse, mutant code-reuse); an expired or unknown code
// enrols none.
func TestEnrolCodeSingleUse(t *testing.T) {
	km, _, _ := certManager(t)
	now := time.Now()
	code, err := km.MintCode("admin", "", time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := km.Enrol(code, &newKey(t).PublicKey, now); err != nil {
		t.Fatal(err)
	}
	if _, err := km.Enrol(code, &newKey(t).PublicKey, now); !errors.Is(err, ErrEnrolRefused) {
		t.Fatalf("second redemption: %v, want ErrEnrolRefused", err)
	}
	old, _ := km.MintCode("late", "", time.Minute, now)
	if _, err := km.Enrol(old, &newKey(t).PublicKey, now.Add(2*time.Minute)); !errors.Is(err, ErrEnrolRefused) {
		t.Fatalf("expired code: %v", err)
	}
	if _, err := km.Enrol(old, &newKey(t).PublicKey, now); !errors.Is(err, ErrEnrolRefused) {
		t.Fatalf("an expired code stays spent: %v", err)
	}
	if _, err := km.Enrol("enr_unknown", &newKey(t).PublicKey, now); !errors.Is(err, ErrEnrolRefused) {
		t.Fatalf("unknown code: %v", err)
	}
	if n := len(km.List()); n != 1 {
		t.Fatalf("%d machines, want 1", n)
	}
}

// TestCertBoundToLine: a certificate authenticates its machine while the
// line binds its key; after a rotation (Rebind to a new key) or a revoke it
// admits nothing (spec CertBoundToLine, mutants cert-any-key,
// cert-after-revoke). The line survives a reload of the file.
func TestCertBoundToLine(t *testing.T) {
	km, p, ca := certManager(t)
	now := time.Now()
	code, _ := km.MintCode("laptop", "", time.Hour, now)
	k1 := newKey(t)
	m, err := km.Enrol(code, &k1.PublicKey, now)
	if err != nil {
		t.Fatal(err)
	}
	c1 := ca.sign(t, m.ID, &k1.PublicKey)
	pr, err := certPrincipal(p, c1)
	if err != nil || pr.Machine != m.ID || pr.Owner != string(m.ID) || pr.NotAfter.IsZero() {
		t.Fatalf("certificate: %+v %v", pr, err)
	}
	if _, err := km.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, err := certPrincipal(p, c1); err != nil {
		t.Fatalf("after reload: %v", err)
	}
	// A certificate for another machine id with this key: not that machine's.
	if _, err := certPrincipal(p, ca.sign(t, identity.NewMachineID(), &k1.PublicKey)); !errors.Is(err, identity.ErrUnknownCredential) {
		t.Fatalf("other id: %v", err)
	}
	// Another CA's certificate.
	if _, err := certPrincipal(p, newTestCA(t).sign(t, m.ID, &k1.PublicKey)); !errors.Is(err, identity.ErrUnknownCredential) {
		t.Fatalf("other CA: %v", err)
	}
	var cut []string
	km.OnRevoke(func(s string) { cut = append(cut, s) })
	if err := km.Rebind(m.ID, &k1.PublicKey); err != nil || len(cut) != 0 {
		t.Fatalf("renewal (same key): %v, cut %v", err, cut)
	}
	k2 := newKey(t)
	if err := km.Rebind(m.ID, &k2.PublicKey); err != nil {
		t.Fatal(err)
	}
	if len(cut) != 1 || cut[0] != string(m.ID) {
		t.Fatalf("rotation cut %v, want the machine's connections", cut)
	}
	if _, err := certPrincipal(p, c1); !errors.Is(err, identity.ErrUnknownCredential) {
		t.Fatalf("rotated-away key: %v", err)
	}
	c2 := ca.sign(t, m.ID, &k2.PublicKey)
	if _, err := certPrincipal(p, c2); err != nil {
		t.Fatalf("rotated key: %v", err)
	}
	if err := km.Revoke(m.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := certPrincipal(p, c2); !errors.Is(err, identity.ErrUnknownCredential) {
		t.Fatalf("revoked: %v", err)
	}
}

// TestTokenIsNotACredential: the key file authenticates certificates
// only; a bearer token never authenticates, even one that is a machine's
// public key DER (the SPKI is public) or arrives beside its certificate
// (the API key cut: no key branch is left).
func TestTokenIsNotACredential(t *testing.T) {
	km, p, ca := certManager(t)
	code, _ := km.MintCode("box", "", time.Hour, time.Now())
	k := newKey(t)
	m, err := km.Enrol(code, &k.PublicKey, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	der, _ := x509.MarshalPKIXPublicKey(&k.PublicKey)
	if _, err := p.Authenticate(t.Context(), identity.Credential{Token: string(der)}); !errors.Is(err, identity.ErrUnknownCredential) {
		t.Fatalf("SPKI as a token: %v", err)
	}
	if _, err := p.Authenticate(t.Context(), identity.Credential{Token: "rpc_x", PeerCerts: ca.sign(t, m.ID, &k.PublicKey)}); !errors.Is(err, identity.ErrUnknownCredential) {
		t.Fatalf("a token beside a certificate: %v", err)
	}
}

// TestCodeJoinsAccount: a code minted with --account enrols into that
// machine's account; a name taken there is refused (spec EnrolledAsCoded,
// mutants code-join-outside, code-dup-name).
func TestCodeJoinsAccount(t *testing.T) {
	km, p, ca := certManager(t)
	now := time.Now()
	first := enrol(t, km, ca, "admin", "")
	if _, err := km.MintCode("node", "nobody", time.Hour, now); err == nil {
		t.Fatal("a code for an account the file does not hold was minted")
	}
	code, err := km.MintCode("node", "admin", time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	k := newKey(t)
	m, err := km.Enrol(code, &k.PublicKey, now)
	if err != nil {
		t.Fatal(err)
	}
	pr, err := certPrincipal(p, ca.sign(t, m.ID, &k.PublicKey))
	if err != nil || pr.Owner != string(first.ID) {
		t.Fatalf("joined machine: %+v %v, want owner %s", pr, err, first.ID)
	}
	dup, _ := km.MintCode("node", "admin", time.Hour, now)
	if _, err := km.Enrol(dup, &newKey(t).PublicKey, now); err == nil {
		t.Fatal("a second machine named node in the account")
	}
}

// TestWritersOfOneFileKeepEachOthersLines: two managers of one key file
// (the running relay and `nfltr-relay machines` in another process, each
// with the file as it loaded it) never drop a line the other wrote, nor
// bring back one the other removed (editFile: locked read-modify-write).
func TestWritersOfOneFileKeepEachOthersLines(t *testing.T) {
	relay, _, ca := certManager(t)
	cli := NewKeyManager(relay.filePath, nil, slog.New(slog.DiscardHandler)) // loaded the empty file
	x := enrol(t, relay, ca, "x", "")
	y := enrol(t, cli, ca, "y", "") // cli's memory holds no x
	if err := cli.Revoke(y.ID); err != nil {
		t.Fatal(err)
	}
	if err := relay.Rebind(x.ID, &newKey(t).PublicKey); err != nil { // relay's memory never saw y go
		t.Fatal(err)
	}
	got, err := Load(relay.filePath)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[identity.MachineID]bool{}
	for _, m := range got.Hashes() {
		ids[m.ID] = true
	}
	if !ids[x.ID] || ids[y.ID] || len(ids) != 1 {
		t.Fatalf("key file holds %v: want only x %s (y %s revoked)", ids, x.ID, y.ID)
	}
}
