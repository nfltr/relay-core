// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// TLSStateCredentials are the gRPC server's transport credentials when its
// listener already speaks TLS (WrapGRPCListener, the SNI mux): no second
// handshake, but each connection's TLS state, found below the listener's
// wrappers (a NetConn chain, tlsbatch.Conn), becomes its peer's AuthInfo,
// so a handler's context carries the client certificate
// (PeerCertificates). A plaintext connection gets no TLS state.
func TLSStateCredentials() credentials.TransportCredentials { return tlsStateCreds{} }

type tlsStateCreds struct{}

// tlsConn is a server conn's TLS: *tls.Conn and tlsbatch.Conn.
type tlsConn interface {
	ConnectionState() tls.ConnectionState
	HandshakeContext(context.Context) error
}

func (tlsStateCreds) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	for c := conn; c != nil; {
		if t, ok := c.(tlsConn); ok {
			if err := t.HandshakeContext(context.Background()); err != nil {
				return nil, nil, err
			}
			return conn, credentials.TLSInfo{State: t.ConnectionState(), CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}}, nil
		}
		u, ok := c.(interface{ NetConn() net.Conn })
		if !ok {
			break
		}
		c = u.NetConn()
	}
	return conn, plaintext{credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}}, nil
}

type plaintext struct{ credentials.CommonAuthInfo }

func (plaintext) AuthType() string { return "insecure" }

func (tlsStateCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("security: TLSStateCredentials are server-side only")
}

func (tlsStateCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (c tlsStateCreds) Clone() credentials.TransportCredentials { return c }

func (tlsStateCreds) OverrideServerName(string) error { return nil }

// PeerCertificates is the TLS client certificate chain of the connection a
// gRPC call came on (nil: none, or not TLS).
func PeerCertificates(ctx context.Context) []*x509.Certificate {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.PeerCertificates) == 0 {
		return nil
	}
	return info.State.PeerCertificates
}
