// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
)

// FuzzExtractSNI feeds arbitrary first bytes of a connection to the
// ClientHello peek: no panic, it reads at most one record, and the bytes it
// returns for replay are exactly the bytes it consumed.
func FuzzExtractSNI(f *testing.F) {
	for _, name := range []string{"fresh-crest.nfltr.xyz", "abc.e2ee.nfltr.xyz", "grpc.nfltr.xyz", "a"} {
		f.Add(clientHello(name))
	}
	f.Add([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"))
	f.Add([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"))
	f.Add([]byte{0x16, 0x03, 0x01, 0x00, 0x05})
	f.Add([]byte{0x16, 0x03, 0x01, 0xff, 0xff})
	f.Add([]byte{0x16, 0x03})
	f.Fuzz(func(t *testing.T, wire []byte) {
		c := &fuzzConn{r: bytes.NewReader(wire)}
		_, peeked, _ := ExtractSNI(c)
		if c.n > maxClientHelloPeek {
			t.Fatalf("read %d bytes, limit %d", c.n, maxClientHelloPeek)
		}
		if !bytes.Equal(peeked, wire[:c.n]) {
			t.Fatalf("replays %x, consumed %x", peeked, wire[:c.n])
		}
	})
}

// TestExtractSNI_ShortReadReplaysOnlyBytesRead guards what FuzzExtractSNI
// found: a connection that ended (or hit the peek deadline) inside the
// record header or body had its replay padded with zero bytes it never
// sent, which the sni-mux then prepended to the stream it passed on.
func TestExtractSNI_ShortReadReplaysOnlyBytesRead(t *testing.T) {
	for _, wire := range [][]byte{{0x16, 0x03}, {0x16, 0x03, 0x01, 0x00, 0x05, 0x01}} {
		_, peeked, err := ExtractSNI(&fuzzConn{r: bytes.NewReader(wire)})
		if err == nil || !bytes.Equal(peeked, wire) {
			t.Errorf("%x: replays %x, %v", wire, peeked, err)
		}
	}
}

// FuzzExtractSNIFromTLSClient: for a real crypto/tls ClientHello that fits
// in one record, extractSNI returns the server_name the client sent.
func FuzzExtractSNIFromTLSClient(f *testing.F) {
	for _, name := range []string{"fresh-crest.nfltr.xyz", "abc.e2ee.nfltr.xyz", "x.", "127.0.0.1", "[::1]", ""} {
		f.Add(name)
	}
	f.Fuzz(func(t *testing.T, name string) {
		hello := clientHello(name)
		if len(hello) < 5 {
			return // crypto/tls refused the config before writing
		}
		want := sentSNI(name)
		sni, _, err := ExtractSNI(&fuzzConn{r: bytes.NewReader(hello)})
		oneRecord := int(hello[3])<<8|int(hello[4])+5 == len(hello)
		switch {
		case err == nil && sni != want:
			t.Fatalf("SNI %q, client sent %q", sni, want)
		case err != nil && oneRecord && want != "":
			t.Fatalf("SNI %q: %v", want, err)
		case want == "" && oneRecord && !errors.Is(err, errNoSNI):
			t.Fatalf("no SNI sent: got %q, %v", sni, err)
		}
	})
}

// FuzzParseSNIExtension: no panic on any extension body, and a name
// returned lies within it.
func FuzzParseSNIExtension(f *testing.F) {
	f.Add([]byte{0, 8, 0, 0, 5, 'a', '.', 'x', 'y', 'z'})
	f.Add([]byte{0, 3, 1, 0, 0})
	f.Add([]byte{0xff, 0xff})
	f.Add([]byte{0})
	f.Fuzz(func(t *testing.T, data []byte) {
		name, err := parseSNIExtension(data)
		if err == nil && (len(name) > len(data)-5 || !bytes.Contains(data, []byte(name))) {
			t.Fatalf("name %q not in %x", name, data)
		}
	})
}

// maxClientHelloPeek is the most extractSNI may read: a record header and
// one record of at most 16384 bytes.
const maxClientHelloPeek = 5 + 16384

// fuzzConn is a net.Conn reading r and counting the bytes read.
type fuzzConn struct {
	net.Conn
	r io.Reader
	n int
}

func (c *fuzzConn) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.n += n
	return n, err
}

// captureConn records what a TLS client writes, then ends its handshake.
type captureConn struct {
	net.Conn
	buf bytes.Buffer
}

// Write: records b. allow:wrapper
func (c *captureConn) Write(b []byte) (int, error) { return c.buf.Write(b) }

// Read: the handshake ends after the ClientHello. allow:wrapper
func (c *captureConn) Read([]byte) (int, error) { return 0, io.EOF }

// Close: nothing to close. allow:wrapper
func (c *captureConn) Close() error { return nil }

// clientHello returns the first flight crypto/tls writes for serverName.
func clientHello(serverName string) []byte {
	c := &captureConn{}
	_ = tls.Client(c, &tls.Config{ServerName: serverName, InsecureSkipVerify: true}).Handshake() //nolint:gosec // only the ClientHello is used
	return c.buf.Bytes()
}

// sentSNI is the server_name crypto/tls puts in its ClientHello for name
// (crypto/tls hostnameInSNI): none for an IP literal, trailing dots cut.
func sentSNI(name string) string {
	host := name
	if len(host) > 0 && host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1]
	}
	if i := strings.LastIndex(host, "%"); i > 0 {
		host = host[:i]
	}
	if net.ParseIP(host) != nil {
		return ""
	}
	return strings.TrimRight(name, ".")
}
