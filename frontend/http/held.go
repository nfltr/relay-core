// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"io"
	"net/http"

	"github.com/nfltr/relay-core/core/identity"
)

// holdLongResponse holds a reverse-proxied response under the key its
// request carries once it is long-lived: a body of unknown length (a
// stream) or past identity.LongBodyBytes. A revocation or the key's expiry
// closes the body, which ends the copy (the reverse proxy then cuts the
// response). For a ReverseProxy's ModifyResponse.
func holdLongResponse(resp *http.Response) {
	// An upgrade's body must stay the connection (and its request was
	// held from the start: the relay's authMiddleware).
	if resp.StatusCode == http.StatusSwitchingProtocols || resp.ContentLength >= 0 && resp.ContentLength <= identity.LongBodyBytes {
		return
	}
	h, ok := identity.HoldCarried(resp.Request.Context())
	if !ok {
		return
	}
	body := resp.Body
	resp.Body = &heldBody{ReadCloser: body, h: h, stop: context.AfterFunc(h, func() { _ = body.Close() })}
}

// heldBody is a response body held under its request's key until it is
// closed.
type heldBody struct {
	io.ReadCloser
	h    *identity.Held
	stop func() bool
}

func (b *heldBody) Close() error {
	b.stop()
	err := b.ReadCloser.Close()
	b.h.Release()
	return err
}
