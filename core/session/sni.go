// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"io"
	"net"
	"time"
)

// SNIPeekTimeout bounds how long a client may take to send its ClientHello
// to a listener that peeks it (ExtractSNI).
const SNIPeekTimeout = 5 * time.Second

// SNI extraction sentinel errors.
var (
	// errNoSNI is returned when the ClientHello does not contain an SNI extension.
	errNoSNI = errors.New("no SNI in ClientHello")

	// errNotTLS is returned when the connection does not start with a TLS record.
	errNotTLS = errors.New("not a TLS handshake")

	// errSNITruncated is returned when the ClientHello is too short to parse.
	errSNITruncated = errors.New("truncated TLS ClientHello")
)

// PrefixConn wraps a net.Conn to prepend already-read bytes (the TLS
// ClientHello) before the underlying connection's data.
type PrefixConn struct {
	net.Conn
	Prefix []byte
	off    int
}

// CloseWrite closes the underlying connection's writing side when it has
// one.
func (c *PrefixConn) CloseWrite() error { return PassCloseWrite(c.Conn) }

// NetConn is the conn below the replayed prefix (a gRPC server reads its
// TLS state through it: the client certificate).
func (c *PrefixConn) NetConn() net.Conn { return c.Conn }

// Reset cuts the visitor with an RST (peerlink.Join aborts a Session so):
// it sees its stream cut, not ended.
func (c *PrefixConn) Reset() error {
	if l, ok := c.Conn.(interface{ SetLinger(int) error }); ok {
		_ = l.SetLinger(0)
	}
	return c.Conn.Close()
}

// Read returns the prefix first, then the underlying connection's data.
func (c *PrefixConn) Read(b []byte) (int, error) {
	if c.off < len(c.Prefix) {
		n := copy(b, c.Prefix[c.off:])
		c.off += n
		return n, nil
	}
	return c.Conn.Read(b)
}

// ExtractSNI reads the TLS ClientHello from conn and returns the SNI
// server_name extension value.  The bytes read from conn are returned as
// peeked so the caller can replay them.
//
// This does NOT terminate TLS — it only parses the unencrypted ClientHello
// record.
func ExtractSNI(conn net.Conn) (sni string, peeked []byte, err error) {
	// A TLS record is:  ContentType(1) + Version(2) + Length(2) + Fragment(N)
	// For ClientHello the max record is 16384 + 5 bytes of header.
	// In practice, ClientHello is usually <512 bytes.
	// We read in stages to avoid over-reading.

	// Stage 1: Read the TLS record header (5 bytes).
	// peeked is replayed to the next reader, so it holds exactly the bytes
	// read: a short read (EOF, the peek deadline) returns only those.
	header := make([]byte, 5)
	if n, err := io.ReadFull(conn, header); err != nil {
		return "", header[:n], errNotTLS
	}

	// ContentType 0x16 = Handshake.
	if header[0] != 0x16 {
		return "", header, errNotTLS
	}

	// Record length (big-endian uint16).
	recordLen := int(header[3])<<8 | int(header[4])
	if recordLen < 4 || recordLen > 16384 {
		return "", header, errSNITruncated
	}

	// Stage 2: Read the handshake record body.
	body := make([]byte, recordLen)
	if n, err := io.ReadFull(conn, body); err != nil {
		return "", append(header, body[:n]...), errSNITruncated
	}

	peeked = make([]byte, 0, len(header)+len(body))
	peeked = append(peeked, header...)
	peeked = append(peeked, body...)

	// Handshake header: type(1) + length(3).
	if len(body) < 4 {
		return "", peeked, errSNITruncated
	}
	if body[0] != 0x01 { // ClientHello
		return "", peeked, errNotTLS
	}

	// Parse ClientHello fields to reach extensions.
	// Skip: handshake header(4) + client_version(2) + random(32) = offset 38.
	pos := 4 + 2 + 32
	if pos >= len(body) {
		return "", peeked, errSNITruncated
	}

	// Session ID (variable length, 1-byte length prefix).
	sessionIDLen := int(body[pos])
	pos += 1 + sessionIDLen
	if pos+2 > len(body) {
		return "", peeked, errSNITruncated
	}

	// Cipher suites (variable length, 2-byte length prefix).
	cipherSuitesLen := int(body[pos])<<8 | int(body[pos+1])
	pos += 2 + cipherSuitesLen
	if pos+1 > len(body) {
		return "", peeked, errSNITruncated
	}

	// Compression methods (variable length, 1-byte length prefix).
	compressionLen := int(body[pos])
	pos += 1 + compressionLen
	if pos+2 > len(body) {
		// No extensions present.
		return "", peeked, errNoSNI
	}

	// Extensions (variable length, 2-byte length prefix).
	extensionsLen := int(body[pos])<<8 | int(body[pos+1])
	pos += 2
	end := pos + extensionsLen
	if end > len(body) {
		return "", peeked, errSNITruncated
	}

	// Walk extensions looking for SNI (type 0x0000).
	for pos+4 <= end {
		extType := int(body[pos])<<8 | int(body[pos+1])
		extLen := int(body[pos+2])<<8 | int(body[pos+3])
		pos += 4
		if pos+extLen > end {
			break
		}
		if extType == 0x0000 {
			// SNI extension: server_name_list_length(2) + entries.
			sniData := body[pos : pos+extLen]
			sni, err := parseSNIExtension(sniData)
			return sni, peeked, err
		}
		pos += extLen
	}

	return "", peeked, errNoSNI
}

// parseSNIExtension parses the SNI extension payload and returns the first
// host_name (type 0).
func parseSNIExtension(data []byte) (string, error) {
	if len(data) < 2 {
		return "", errSNITruncated
	}
	// server_name_list_length
	listLen := int(data[0])<<8 | int(data[1])
	pos := 2
	if pos+listLen > len(data) {
		return "", errSNITruncated
	}
	end := pos + listLen

	for pos+3 <= end {
		nameType := data[pos]
		nameLen := int(data[pos+1])<<8 | int(data[pos+2])
		pos += 3
		if pos+nameLen > end {
			break
		}
		if nameType == 0 { // host_name
			return string(data[pos : pos+nameLen]), nil
		}
		pos += nameLen
	}

	return "", errNoSNI
}
