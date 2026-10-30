// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/viper"
)

// slowReadBackend reads a request body step bytes at a time with a pause
// after each step, and answers with how many it read; read and doneAt are
// how many it has read and when it read the last one. Its connections
// take a small receive buffer: what the agent has written to it and it has
// not read yet stays small, so its drain is the agent's chunks in flight,
// whose acks the relay sees, not bytes in socket buffers, which no proxy
// sees.
func slowReadBackend(t *testing.T, step int, pause time.Duration) (srv *httptest.Server, read, doneAt *atomic.Int64) {
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
	srv.Listener = smallReadBuffer{srv.Listener}
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
	backend, read, doneAt := slowReadBackend(t, 1<<20, time.Second)
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

type smallReadBuffer struct{ net.Listener }

func (l smallReadBuffer) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(64 << 10)
	}
	return c, err
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
