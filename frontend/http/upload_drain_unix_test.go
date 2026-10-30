// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package httpingress

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/viper"
)

// slowReadBackend reads a request body step bytes at a time with a pause
// after each step, and answers with how many it read; read and doneAt are
// how many it has read and when it read the last one. It serves on ln, or
// without one on connections with a 64 KiB receive buffer: what the agent
// has written to it and it has not read yet stays small, so its drain is
// what the agent holds and reports, not bytes in socket buffers, which no
// proxy sees.
func slowReadBackend(t *testing.T, ln net.Listener, step int, pause time.Duration) (srv *httptest.Server, read, doneAt *atomic.Int64) {
	read, doneAt = new(atomic.Int64), new(atomic.Int64)
	srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, step)
		var n int64
		for {
			m, err := io.ReadFull(r.Body, buf)
			n += int64(m)
			read.Store(n)
			if err != nil {
				break
			}
			time.Sleep(pause)
		}
		doneAt.Store(time.Now().UnixNano())
		_, _ = io.WriteString(w, strconv.FormatInt(n, 10))
	}))
	if ln == nil {
		ln = smallReadBuffer{srv.Listener}
	} else {
		_ = srv.Listener.Close()
	}
	srv.Listener = ln
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, read, doneAt
}

// TestUploadDrainToSlowBackendIsProgress: once the relay has published an
// upload's last chunk, the agent still feeds the chunks in flight to a
// backend that reads them slowly; each chunk the agent acks is progress
// on the request's timeout, so the request ends with the backend's
// answer, not a 504.
func TestUploadDrainToSlowBackendIsProgress(t *testing.T) {
	const timeout = 3 * time.Second
	// A chunk (chunking.MaxPayload, 1 MiB) read and acked every second,
	// two seconds inside the timeout for a loaded machine to move it; the
	// chunks in flight after the last one was published
	// (chunking.UploadWindow) drain for about 4 s, past it. One pause per
	// chunk: a loaded machine stretches it by one wake-up's lateness, not
	// by one per small read.
	backend, read, doneAt := slowReadBackend(t, smallWindowListener(t), 1<<20, time.Second)
	url, _ := startGRPCHTTPPath(t, backend.URL)
	viper.Set("default.request.timeout", timeout)

	const n = 5 << 20
	var sentAt atomic.Int64
	body := &endTimer{r: io.LimitReader(&patternReader{}, n), at: &sentAt}
	resp, err := http.Post(url+"/upload", "application/octet-stream", body)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(got) != strconv.Itoa(n) {
		t.Fatalf("%d %q %v after the client's last byte, the backend at %d of %d bytes", resp.StatusCode, got, time.Since(time.Unix(0, sentAt.Load())), read.Load(), n)
	}
	if drain := time.Duration(doneAt.Load() - sentAt.Load()); drain < timeout {
		t.Fatalf("the backend's drain after the client's last byte took %v, under the %v timeout: the test did not exercise it", drain, timeout)
	}
}

// TestUploadDrainToSlowBackendIsProgressLink is
// TestUploadDrainToSlowBackendIsProgress on an agent's HTTP session: after
// the relay wrote an upload's last byte, the agent still feeds what its
// receive window holds (up to 16 MiB) to a slow backend; each window
// update the agent sends as it reads is progress on the request's
// timeout.
func TestUploadDrainToSlowBackendIsProgressLink(t *testing.T) {
	const timeout = 5 * time.Second
	// The agent grants a quarter of its window, 4 MiB, at a time: the
	// backend's pauses take 2 s of each 4 MiB, which leaves 3 s inside the
	// timeout for a loaded machine to move them (make ci's parallel -race
	// packages took 2.2 s); it drains the agent's window for 8 s, past it.
	// The drain is four grants long whatever the pace, so the two margins
	// trade against each other.
	backend, read, doneAt := slowReadBackend(t, nil, 1<<20, 500*time.Millisecond)
	_, url := startAgentHTTPLink(t, backend.URL)
	viper.Set("default.request.timeout", timeout)

	const n = 16 << 20
	var sentAt atomic.Int64
	body := &endTimer{r: io.LimitReader(&patternReader{}, n), at: &sentAt}
	resp, err := http.Post(url+"/upload", "application/octet-stream", body)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(got) != strconv.Itoa(n) {
		t.Fatalf("%d %q %v after the client's last byte, the backend at %d of %d bytes", resp.StatusCode, got, time.Since(time.Unix(0, sentAt.Load())), read.Load(), n)
	}
	if drain := time.Duration(doneAt.Load() - sentAt.Load()); drain < timeout {
		t.Fatalf("the backend's drain after the client's last byte took %v, under the %v timeout: the test did not exercise it", drain, timeout)
	}
}

// smallWindowListener listens on loopback with a 16 KiB receive buffer and
// a 1400-byte MSS, both set before the handshake. The agent's side then
// buffers little too: Linux grows a sender's send buffer with its
// congestion window times its MSS, and loopback's 64 KiB MSS let the
// agent's kernel take about 3 MiB of an upload ahead of a slow backend, so
// the agent acked chunks the backend read seconds later
// (TestUploadDrainToSlowBackendIsProgress went 504 on Linux, not on macOS).
// A 16 KiB window paces bulk reads on a loaded machine, so only the test
// that needs it takes it.
func smallWindowListener(t *testing.T) net.Listener {
	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var err error
		if cerr := c.Control(func(fd uintptr) {
			err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 16<<10)
			// macOS refuses an MSS above its default before a connection;
			// its send buffers stay small without it.
			_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG, 1400)
		}); cerr != nil {
			return cerr
		}
		return err
	}}
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// endTimer records when its reader returned EOF.
type endTimer struct {
	r  io.Reader
	at *atomic.Int64
}

func (e *endTimer) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err == io.EOF && e.at.Load() == 0 {
		e.at.Store(time.Now().UnixNano())
	}
	return n, err
}

type smallReadBuffer struct{ net.Listener }

func (l smallReadBuffer) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(64 << 10)
	}
	return c, err
}
