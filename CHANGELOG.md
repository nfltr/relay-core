# Changelog

Releases of `github.com/nfltr/relay-core` (tags `vX.Y.Z`). The module is
v0.x: a minor version (v0.N.0) may change the exported API, and every
such change is listed here with its migration; a patch version
(v0.N.M) does not change it. `make check-api` holds each release to the
changes listed (from the first tag on).

## v0.2.5 (2026-10-29)

- Requires github.com/nfltr/link v0.10.18 (senders waiting in a
  connection's write buffer are woken one at a time; the wire is
  unchanged), relay proto v0.19.4 and client v0.19.13. No API change.

## v0.2.4 (2026-10-29)

- Requires github.com/nfltr/relay client v0.19.12 (v0.19.11's agent ran
  its TCP tunnel on the closed connection after a reconnect). No API
  change.

## v0.2.3 (2026-10-29)

- The HTTP edge sets `X-Real-IP` to the visitor's address as accepted, the
  same value as `X-Forwarded-For`; a visitor's own `X-Real-IP` is still
  removed first. `core/session.Core.ServeAgents` treats a stop that comes
  before serving as a clean stop. Requires github.com/nfltr/relay client
  v0.19.11. No API change.

## v0.2.1 (2026-10-29)

- `scripts/check-publish.sh` holds its denylisted names as SHA-256
  digests; `scripts/check-api.sh` skips on a repository whose HEAD is
  its first commit. No API change.

## v0.2.0 (2026-10-29)

- Requires github.com/nfltr/link v0.10.17 (quiclink v0.4.17) and
  github.com/nfltr/relay proto v0.19.3, client v0.19.9.
- `frontend/http.Shares` takes `Realm` (the realm a Basic Auth share's
  challenge names; unset: `core/frontend.DefaultRealm`, "relay") and
  `RobotsTag` (the `X-Robots-Tag` header of a visitor's response; unset:
  none). Before, the realm was "nfltr" and every visitor's response carried
  `X-Robots-Tag: noindex, nofollow`. Migration: a composition that wants
  either sets the field. `nfltr-relay-core` sets neither.
- `core/frontend.SharePolicy.EnforceRealm`: `Enforce` naming a realm;
  `Enforce` names `DefaultRealm` (was "nfltr").
- The key file's header line names "the relay" (was an older server name).

## v0.1.1 (2026-10-29)

- `scripts/fuzz-short.sh` writes one `<package> <target> <exit status>` line
  per target to `FUZZ_OUT` when set, as the relay repositories' scripts do.
  No API change.

## v0.1.0 (2026-10-29)

The relay's session core, split out of the relay server module into a
module of its own, Apache-2.0. The relay server (`nfltr-relay`) is its
first user and uses only its exported API.

Public packages:

- `compose`: a relay composition: the core with its frontends attached and
  its ports served (`Serve`, `Composition`, `AgentPort`, `Listen`).
- `core/directory`, `core/frontend`, `core/identity`, `core/relaystatus`,
  `core/session`, `core/sharecode`, `core/sharesecret`: the session core,
  its in-process directory, the frontend plugin interface, the identity
  provider chain, share codes and sealed share secrets.
- `frontend/connect`, `frontend/http`, `frontend/sni`: client sessions
  (`/connect/`), HTTP ingress and SNI E2EE passthrough.
- `keyfile`: the API key file and its identity provider.
- `grpcauth`, `grpcfactory`: the agents' gRPC authentication and server.
- `tlsconfig`: the operator's certificate, plaintext refusal off loopback.
- `broker`, `services`, `platform/*` (`config`, `metrics`, `util`,
  `ratelimit`, `buildinfo`, `nondumpable`, `clientgen`).

New with the split:

- `compose.Serve` takes identity providers (`providers ...identity.Provider`):
  without any, the key file or the environment's key as before; with none of
  those either it returns an error wrapping `compose.ErrNoKeys`.
- `relaycoretest.New(t)`: an in-process relay core on loopback for tests.
- `nfltr-relay-core serve` without `--from-env` starts with no
  configuration: loopback ports, TCP sessions on, the key file
  `nfltr-relay-core.keys` created 0600 with a generated key when absent.
  Off loopback without TLS it refuses to start and names the settings to fix.

`cmd/nfltr-relay-core` runs the core alone (`serve`, `serve --from-env`); it
is not a release artifact.
