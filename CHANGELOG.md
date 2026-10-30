# Changelog

Releases of `github.com/nfltr/relay-core` (tags `vX.Y.Z`). The module is
v0.x: a minor version (v0.N.0) may change the exported API, and every
such change is listed here with its migration; a patch version
(v0.N.M) does not change it. `make check-api` holds each release to the
changes listed (from the first tag on).

## Unreleased

- An upload over a ConnectHTTP stream no longer gets a 504 while the
  agent still feeds the chunks in flight after the last one to a slow
  backend: each chunk the agent acks after the last one was published
  restarts `default.request.timeout`, as the acks before it did.
- The per-frontend families the transfer families replace are deleted
  (no dual reporting): `rpc_server_dispatcher_tcp_tunnel_*` (sessions
  count on the tcp_* path they ride; a session forwarded to the replica
  holding its agent on path replica, a replica it cannot reach as a
  refused open `ERROR_REASON_TARGET_UNREACHABLE`), `_sni_router_*` (path
  sni; its refusals as `nfltr_relay_transfer_opens_total` results:
  parse `PROTOCOL_UNSUPPORTED`, resolve `AGENT_NOT_FOUND`, share store
  `DIRECTORY_UNAVAILABLE`, `SHARE_IP_REFUSED`, `SHARE_POLICY_UNENFORCED`),
  `_ssh_proxy_*` (every connect session, path connect; a released
  client's tunnel now counts every refusal and a failed dial
  (`AGENT_OFFLINE`) in `connect_sessions_total` too) and
  `rpc_server_proxy_request_duration_milliseconds`
  (`rpc_server_proxy_http_request_duration_seconds`, route `proxy`).
  API change: `metrics.RegisterTCPTunnelConnection`,
  `DeregisterTCPTunnelConnection`, `RegisterTCPTunnelDuration`,
  `RegisterTCPTunnelError`, `RegisterSNIRouterConnection`,
  `DeregisterSNIRouterConnection`, `RegisterSNIRouterDuration`,
  `RegisterSNIRouterError`, `RegisterSSHProxyConnection`,
  `DeregisterSSHProxyConnection`, `RegisterSSHProxyDuration`,
  `RegisterSSHProxyError` and `RegisterRequestDuration` are removed;
  callers use `TransferBegin`/`TransferEnd` and `TransferOpened` with
  their path. Guard: `TestReplacedFamiliesStayRetired`.
- Requires github.com/nfltr/link v0.11.0 (connection counters:
  `Conn.Stats`, `Observer.SessionAbort`, `Credit.SetObserver`) and
  peerlink/quiclink v0.5.0 (`Node.Stats`).
- The transfer families: every data path reports the same shape, labelled
  by a closed `path` (`metrics.Path`, `metrics.PathNames`: http_link,
  http_grpc, sni, connect, ssh, tcp_link, tcp_quic, tcp_dataconn,
  tcp_grpc, a2a, replica, wg, p2p_relay, mailbox, blob, accounts):
  `nfltr_relay_transfer_opens_total{path,result}`, `_active{path}`,
  `_bytes_total{path,direction}` (counted while bytes flow: link
  connections' `Stats` and quiclink's `Node.Stats` read at scrape, ended
  connections kept, so counters never go back), `_errors_total{path,reason,code}`
  (a link ABORT's code), `_open_seconds`, `_first_byte_seconds`,
  `_duration_seconds{path}`, `_stall_seconds_total`, `_stalls_total`
  (credit waits, gRPC fallback streams' too) and `_queued_bytes{path}`.
  Nothing allocates per session or byte and nothing runs between scrapes
  (`TestTransferHotPathAllocs`). Recorded here: the agent transports
  (tcp_link, tcp_quic, tcp_dataconn, tcp_grpc), sni, connect, http_link
  and http_grpc opens, a2a, and replica answers; relay-server records its
  own paths through the same functions (`TransferBegin`/`TransferEnd`,
  `TransferOpened`, `TransferError`, `TransferAbort`, `AddTransferBytes`,
  `TrackTransferConn`, `TransferObserver`).
- `grpc_server_handling_seconds` (gRPC handling time; a stream's
  lifetime), buckets `metrics.SecondsBuckets` (1 ms to 60 s, 12).
- API change: `metrics.RegisterPodToPodConnectionFailure()` takes no
  argument; `pod_to_pod_connection_failures_total` lost its `target_pod`
  label (a replica address, never deleted). Callers drop the argument.

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
