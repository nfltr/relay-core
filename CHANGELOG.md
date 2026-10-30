# Changelog

All notable changes to `github.com/nfltr/relay-core` (tags `vX.Y.Z`), newest
first. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

The module is v0.x: a minor version (v0.N.0) may change the exported API,
and every such change is listed here with its migration; a patch version
(v0.N.M) does not change it. `make check-api` holds each release to the
changes listed (from the first tag on).

## [Unreleased]

Nothing yet.

## [v0.4.1] - 2026-10-30

No API change.

### Fixed

- A slow backend behind an agent no longer turns an upload into a 504 while
  bytes are still moving. The relay hands an upload's last byte to the agent
  well before the backend has read it, so the request's idle bound
  (`default.request.timeout`) could run out while the agent was still
  feeding the backend. The bound now restarts on each sign of progress
  until the response begins:
  - on an agent's HTTP session over the agent link: each window update the
    agent sends for the session (link v0.11.1 `Session.OnGrant`). Sessions
    on QUIC carry no such signal yet.
  - on a ConnectHTTP stream: each chunk the agent acknowledges after the
    last one was published (before, the relay stopped reading those
    acknowledgements).

### Changed

- Requires github.com/nfltr/link v0.11.1, relay proto v0.19.6 and relay
  client v0.20.1.

## [v0.4.0] - 2026-10-30

### Removed

- The per-frontend metric families that the transfer families (v0.3.0)
  replace. There is no dual reporting; switch dashboards and alerts to
  `nfltr_relay_transfer_*` with the matching `path`:

  | Removed family | Now counted as |
  |---|---|
  | `rpc_server_dispatcher_tcp_tunnel_*` | the `tcp_*` path the session rides; a session forwarded to the replica holding its agent as path `replica`, and a replica it cannot reach as a refused open `ERROR_REASON_TARGET_UNREACHABLE` |
  | `rpc_server_sni_router_*` | path `sni`; refusals as `nfltr_relay_transfer_opens_total` results: parse `PROTOCOL_UNSUPPORTED`, resolve `AGENT_NOT_FOUND`, share store `DIRECTORY_UNAVAILABLE`, `SHARE_IP_REFUSED`, `SHARE_POLICY_UNENFORCED` |
  | `rpc_server_ssh_proxy_*` | path `connect` (every connect session) |
  | `rpc_server_proxy_request_duration_milliseconds` | `rpc_server_proxy_http_request_duration_seconds`, route `proxy` |

- API: `metrics.RegisterTCPTunnelConnection`, `DeregisterTCPTunnelConnection`,
  `RegisterTCPTunnelDuration`, `RegisterTCPTunnelError`,
  `RegisterSNIRouterConnection`, `DeregisterSNIRouterConnection`,
  `RegisterSNIRouterDuration`, `RegisterSNIRouterError`,
  `RegisterSSHProxyConnection`, `DeregisterSSHProxyConnection`,
  `RegisterSSHProxyDuration`, `RegisterSSHProxyError` and
  `RegisterRequestDuration`. Migration: call `TransferBegin`/`TransferEnd`
  and `TransferOpened` with the path. Guard: `TestReplacedFamiliesStayRetired`.

### Changed

- A released client's tunnel now counts every refusal and a failed dial
  (`AGENT_OFFLINE`) in `connect_sessions_total`.

## [v0.3.0] - 2026-10-30

### Added

- Transfer metric families: every data path reports the same shape,
  labelled by a closed `path` (`metrics.Path`, `metrics.PathNames`:
  `http_link`, `http_grpc`, `sni`, `connect`, `ssh`, `tcp_link`, `tcp_quic`,
  `tcp_dataconn`, `tcp_grpc`, `a2a`, `replica`, `wg`, `p2p_relay`, `mailbox`,
  `blob`, `accounts`):
  - `nfltr_relay_transfer_opens_total{path,result}`, `_active{path}`,
    `_duration_seconds{path}`, `_open_seconds`, `_first_byte_seconds`;
  - `_bytes_total{path,direction}`, counted while bytes flow (link
    connections' `Stats` and quiclink's `Node.Stats` are read at scrape;
    ended connections' counters are kept, so counters never go back);
  - `_errors_total{path,reason,code}` (`code` is a link ABORT's code);
  - `_stall_seconds_total`, `_stalls_total` (credit waits, the gRPC fallback
    streams' included) and `_queued_bytes{path}`.

  Nothing allocates per session or per byte and nothing runs between
  scrapes (guard: `TestTransferHotPathAllocs`). This module records the
  agent transports (`tcp_link`, `tcp_quic`, `tcp_dataconn`, `tcp_grpc`),
  `sni`, `connect`, the `http_link` and `http_grpc` opens, `a2a` and
  `replica` answers. A composition records its own paths through the same
  functions: `TransferBegin`/`TransferEnd`, `TransferOpened`,
  `TransferError`, `TransferAbort`, `AddTransferBytes`, `TrackTransferConn`,
  `TransferObserver`.
- `grpc_server_handling_seconds` (gRPC handling time; a stream's
  lifetime), with buckets `metrics.SecondsBuckets` (1 ms to 60 s, 12
  buckets).

### Changed

- API: `metrics.RegisterPodToPodConnectionFailure()` takes no argument, and
  `pod_to_pod_connection_failures_total` lost its `target_pod` label (a
  replica address that was never deleted, so its series grew without
  bound). Migration: drop the argument. The label guard now refuses any
  label naming a pod.
- Requires github.com/nfltr/link v0.11.0 (connection counters:
  `Conn.Stats`, `Observer.SessionAbort`, `Credit.SetObserver`),
  peerlink/quiclink v0.5.0 (`Node.Stats`), relay proto v0.19.5 and relay
  client v0.20.0.

## [v0.2.5] - 2026-10-29

No API change.

### Changed

- Requires github.com/nfltr/link v0.10.18 (senders waiting in a
  connection's write buffer are woken one at a time; the wire is
  unchanged), relay proto v0.19.4 and relay client v0.19.13.

## [v0.2.4] - 2026-10-29

No API change.

### Fixed

- Requires github.com/nfltr/relay client v0.19.12: v0.19.11's agent ran
  its TCP tunnel on the closed connection after a reconnect.

## [v0.2.3] - 2026-10-29

No API change.

### Changed

- The HTTP edge sets `X-Real-IP` to the visitor's address as accepted, the
  same value as `X-Forwarded-For`. A visitor's own `X-Real-IP` is still
  removed first and never becomes the value.
- Requires github.com/nfltr/relay client v0.19.11.

### Fixed

- `core/session.Core.ServeAgents` treats a stop that comes before serving
  begins as a clean stop. Before, it returned `grpc.ErrServerStopped` when
  the stop won that race.

## [v0.2.2] - 2026-10-29

No API change.

### Changed

- Requires github.com/nfltr/relay client v0.19.10.

## [v0.2.1] - 2026-10-29

No API change.

### Changed

- `scripts/check-publish.sh` holds its denylisted names as SHA-256 digests.
- `scripts/check-api.sh` skips on a repository whose HEAD is its first
  commit.

## v0.2.0 - 2026-10-29

Not tagged in this repository; listed for the API changes since v0.1.

### Changed

- API: `frontend/http.Shares` takes `Realm` (the realm a Basic Auth
  share's challenge names; unset: `core/frontend.DefaultRealm`, "relay")
  and `RobotsTag` (the `X-Robots-Tag` header of a visitor's response;
  unset: none). Before, the realm was "nfltr" and every visitor's response
  carried `X-Robots-Tag: noindex, nofollow`. Migration: a composition that
  wants either sets the field. `nfltr-relay-core` sets neither.
- API: `core/frontend.SharePolicy.EnforceRealm` is `Enforce` naming a
  realm; `Enforce` names `DefaultRealm` (was "nfltr").
- The key file's header line names "the relay".
- Requires github.com/nfltr/link v0.10.17 (quiclink v0.4.17) and
  github.com/nfltr/relay proto v0.19.3, client v0.19.9.

## v0.1.1 - 2026-10-29

Not tagged in this repository.

### Changed

- `scripts/fuzz-short.sh` writes one `<package> <target> <exit status>`
  line per target to `FUZZ_OUT` when set. No API change.

## v0.1.0 - 2026-10-29

Not tagged in this repository. The relay's session core, split out of the
relay server module into a module of its own, Apache-2.0. The relay server
(`nfltr-relay`) is its first user and uses only its exported API.

### Added

- Public packages:
  - `compose`: a relay composition: the core with its frontends attached
    and its ports served (`Serve`, `Composition`, `AgentPort`, `Listen`).
  - `core/directory`, `core/frontend`, `core/identity`,
    `core/relaystatus`, `core/session`, `core/sharecode`,
    `core/sharesecret`: the session core, its in-process directory, the
    frontend plugin interface, the identity provider chain, share codes
    and sealed share secrets.
  - `frontend/connect`, `frontend/http`, `frontend/sni`: client sessions
    (`/connect/`), HTTP ingress and SNI E2EE passthrough.
  - `keyfile`: the API key file and its identity provider.
  - `grpcauth`, `grpcfactory`: the agents' gRPC authentication and server.
  - `tlsconfig`: the operator's certificate; plaintext refused off
    loopback.
  - `broker`, `services`, `platform/*` (`config`, `metrics`, `util`,
    `ratelimit`, `buildinfo`, `nondumpable`, `clientgen`).
- `compose.Serve` takes identity providers (`providers ...identity.Provider`):
  without any, the key file or the environment's key as before; with none
  of those either it returns an error wrapping `compose.ErrNoKeys`.
- `relaycoretest.New(t)`: an in-process relay core on loopback for tests.
- `nfltr-relay-core serve` without `--from-env` starts with no
  configuration: loopback ports, TCP sessions on, the key file
  `nfltr-relay-core.keys` created 0600 with a generated key when absent.
  Off loopback without TLS it refuses to start and names the settings to
  fix. `cmd/nfltr-relay-core` runs the core alone (`serve`,
  `serve --from-env`); it is not a release artifact.

[Unreleased]: https://github.com/nfltr/relay-core/compare/v0.4.1...HEAD
[v0.4.1]: https://github.com/nfltr/relay-core/compare/v0.4.0...v0.4.1
[v0.4.0]: https://github.com/nfltr/relay-core/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/nfltr/relay-core/compare/v0.2.5...v0.3.0
[v0.2.5]: https://github.com/nfltr/relay-core/compare/v0.2.4...v0.2.5
[v0.2.4]: https://github.com/nfltr/relay-core/compare/v0.2.3...v0.2.4
[v0.2.3]: https://github.com/nfltr/relay-core/compare/v0.2.2...v0.2.3
[v0.2.2]: https://github.com/nfltr/relay-core/compare/v0.2.1...v0.2.2
[v0.2.1]: https://github.com/nfltr/relay-core/releases/tag/v0.2.1
