// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package tlsconfig is the relay's server TLS without ACME: an operator's
// certificate files as a *tls.Config, the TLS listeners, a self-signed
// certificate, and the rule that a relay listens off loopback only with
// TLS (RefusePlaintext).
package tlsconfig

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	"github.com/nfltr/relay/proto/wire/tlsbatch"
)

// Server is the relay's server TLS for cert: TLS 1.2 or later, the AEAD
// suites with forward secrecy (Ciphers), h2 for gRPC.
func Server(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		CipherSuites: Ciphers(),
		NextProtos:   []string{"h2"},
	}
}

// Load is Server for the PEM certificate (chain) and key files.
func Load(certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("security: load TLS keypair: %w", err)
	}
	return Server(cert), nil
}

// Ciphers returns a list of strong TLS 1.2 cipher suites.
// TLS 1.3 cipher suites are always enabled and cannot be configured.
func Ciphers() []uint16 {
	return []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
	}
}

// RefusePlaintext is the error for listening on listen (an IP address)
// without TLS: nil on loopback, where only this machine reaches the port
// (or a TLS-terminating proxy in front of it).
func RefusePlaintext(listen string) error {
	if ip := net.ParseIP(listen); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("refusing to listen on %s without TLS", listen)
}

// WrapListener wraps a net.Listener with TLS if the config is non-nil. Its
// conns are *tls.Conn, as net/http needs; one an HTTP handler hijacks can
// batch its writes (tlsbatch.Batched).
func WrapListener(l net.Listener, tc *tls.Config) net.Listener {
	if tc == nil {
		return l
	}
	return tlsbatch.NewHTTPListener(l, tc)
}

// WrapGRPCListener is WrapListener for the gRPC server, which takes any
// net.Conn: each Write's TLS records go out in one write (tlsbatch).
func WrapGRPCListener(l net.Listener, tc *tls.Config) net.Listener {
	if tc == nil {
		return l
	}
	return tlsbatch.NewListener(l, tc)
}

// SelfSignedPEM creates a self-signed ECDSA P-256 server certificate named
// commonName, valid for validity, with each of names as a SAN (IP addresses
// as IP SANs, everything else as DNS names). It returns PEM-encoded
// certificate and private key.
func SelfSignedPEM(commonName string, names []string, validity time.Duration) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate ECDSA key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("generate serial: %w", err)
	}
	template := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{commonName}, CommonName: commonName},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	for _, name := range names {
		if ip := net.ParseIP(name); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, name)
		}
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create self-signed cert: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal ECDSA key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}
