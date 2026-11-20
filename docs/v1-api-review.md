# relay-core v1 API review

Lane RCAPI, 2026-11-20, on relay-core main `4bee3c6` (v0.17.0 the latest
tag). The rung above relay's review (github.com/nfltr/relay
`docs/v1-api-review.md`, lane RELAYAPI) and link's (github.com/nfltr/link
`docs/v1-api-review.md`), by the same method: every exported identifier of
the module's public packages (not `internal/`, `cmd/`, `examples/`), with
the questions

- does a consumer need it (relay-server, nfltr, hosted, send, pipe at
  their mains; another package of this module counts as "module", the
  identifier's own package does not),
- is it named well and do its docs name only what exists,
- are its failures and concurrency documented,
- is it safe to misuse (zero value, a second Close, concurrent use),

and, because they are surface too, its config keys and metric names.

Outcome: 21 package-level exports unexported and 7 deleted (none had a
consumer; one of them a config key nothing read), 53 doc references to
names that do not exist fixed (the guard `TestDocsNameNoGhosts` found 51), 7
concurrency statements added, `api/v0.18.0.txt` recorded (next release
**v0.19.0**: promote r30 took v0.18.0), 0 wire changes. **No consumer changes**: `go vet` of every
package that imports relay-core in relay-server (27), nfltr (12), hosted
(2), send (1) and pipe (1) passes against this branch (`replace` in a
copy of each go.mod, `-modfile`). Proposed, not made: the metric names'
two schemes and the config keys' spelling (below: "renames now or
never"), each with the consumers' dashboards, alerts and deploy files it
would touch.

## Method

- Uses resolved by type: the relay lane's scanner
  (`golang.org/x/tools/go/packages`, `TypesInfo.Uses` mapped to the
  package-level name, method or field) with relay-core as the prefix, over
  every package and test of each consumer, loaded against this checkout
  (`replace github.com/nfltr/relay-core => <clone>` in a copy of the
  consumer's go.mod, `-mod=mod -modfile=`): relay-server `ae08825`, nfltr
  `57786a6`, hosted `89ec44a`, send `2b53180`, pipe `c770462` (all pin
  v0.17.0). An implicit use through an interface does not count (see
  "Kept").
- Reachability: `deadcode` (x/tools v0.51.0) over every `main` package of
  relay-core, relay-server, hosted and nfltr (send and pipe link relay-core
  only in tests, through `relaycoretest`); a function is dead when every
  binary that links its package leaves it unreached. 27 are, 11 of them
  test doubles or helpers consumers' tests call (`NewNoOpInventory`,
  `FirstMD`, `ProtocolOptions`).
- Classes (package-level names plus methods and fields; definitions from
  non-test files):

| names | used by a consumer | used only by another package of relay-core | used by nothing outside its package |
|---|---|---|---|
| 1286 | 718 | 291 | 277 |

relay-server uses 711 names across 31 packages; nfltr 24 (`core/identity`,
`keyfile`, `platform/filewatch`, `tlsconfig`); hosted 3 (`core/identity`);
send and pipe 5 each (`relaycoretest`). The tables and tools are in the
lane's evidence (`~/nfltr-evidence/rcapi-2026-11-20/`: `class.tsv`, one
row per name with its class and in-module users; `*.uses`;
`deadall.txt`; `flags.txt`; `metrics.tsv`; the scanners `scan/` and
`metricscan/`).

## Findings by kind

### Unexported (used only by their own package)

| identifier | decision | consumers affected |
|---|---|---|
| `compose.ListenHost` | `listenHost` (Listen's bind host per port) | none |
| `core/session.ValidPort` | `validPort` | none |
| `core/nf1anon.Source` | `sourceOf` (the source bucket of an address) | none |
| `core/directory/directorytest.Rounds` | `rounds` (the race suite's own count) | none |
| `frontend/connect.RequestToken` | `requestToken`; `RequestPrincipal` is the API | none |
| `grpcauth.BalanceExhaustedStatus`, `CredentialEndedStatus`, `MetadataPrincipal`, `PeerCertificates` | unexported; the interceptors are the API, and their docs now state the codes (FailedPrecondition with `ERROR_REASON_BALANCE_EXHAUSTED`, Unauthenticated) | none |
| `platform/metrics.AddTransferStall`, `ObserveTransferOpen` | unexported; recorded through the transfer observer | none |
| `platform/util.Cast` | `cast` (`SyncMap`'s) | none |
| `frontend/http.New`, `Ingress`, `NewProxy`, `Proxy`, `HeaderPolicy`, `NewHeaderPolicyFromConfig`, `HeaderRewriteMiddleware`, `Metadata`, `SkipHeaders` | the HTTP ingress's internals; the package's API is `Frontend`, `Shares`, `NewShareVerified` | none |

### Deleted (no caller outside tests)

| identifier | why | consumers affected |
|---|---|---|
| `compose.MergeListeners` | no caller anywhere (the service child it was for listens through `Listen`) | none |
| `frontend/http.GetAPIType` | test only; the ingress reads `--http.api_type_prefixes` once (`apiType`) | none |
| `keyfile.KeyListResponse` | no caller (the key list is served by relay-server's API) | none |
| `platform/util.NewLogger` | test only | none |
| `platform/util.ProtoToTunnelFrame` (and its test seam `protoMarshal`) | test only; callers marshal and use `BytesToTunnelFrame` | none |
| `core/session/sessiontest.SetSessionStall` | no caller | none |
| `platform/config.GitOpsExec`, config key `gitops.type` | registered, never read (an orchestration leftover); no deploy sets it (nfltr `deploy/`, `helm/`, relay-server, hosted) | none: a relay started with `--gitops.type` now refuses the flag |

### Docs naming what does not exist (53, fixed)

51 found by the new guard, 2 by reading (a retired path named as live): doc comments
of public packages named unexported helpers or names gone with earlier
lanes. Each now names what a caller can use or states the value:
`portKeys` (the port order), `memorySweepEvery` (a minute),
`offlineHold` (500 ms), `agentStreamWait` (1.5 s), `staleAgentMaxIdle`
(180 s), `heartbeatBookkeepingEvery` (15 s), `tcpAuxIdle` (30 s),
`tcpAgentStreams` (4: 3 auxiliary streams), `tcpWriteBatch` (256 KiB),
`tcpBindHeader`/`tcpStreamsHeader` (`x-nfltr-tcp-bind`,
`x-nfltr-tcp-streams`), `errNoHeartbeat`/`errServerRestarting` (their
messages), `wsCloseCode` (the codes 4003, 4004, 4009, 1013, 1011),
`logEvery`, `addAttempts`, `writeFileAtomic`, `httpMethod`,
`stripClientAssertedIdentityHeaders`, `helloRequired`, `muxListener`,
`registerAgent`/`forgetAgent`/`ownerForAgent` (`OwnerForAgent`),
`refuseAttached`, `openService`, `serviceE2EE`
(`agentservices.Decl.E2EE`), `internalAgentLoopbackAgentSide`
(`InternalAgentLoopback.AgentSide`); and names that no longer exist in
this module: `linkPodDialer` and `peerUnreachableFor` (relay-server
`core/replicas` since the replica link moved), `dialPeerStream`,
`authorizeRelayTunnelCaller`, `sendGRPCHeartbeats`,
`relayRequestsToAgent`, `serveNF1Session`, `agentPrincipal`, "iter-61
sCCC9" (the relay SDK's 30 s ping). `ServeAgents` named the agents' TCP
data connections (retired); `data_connections_total`'s help named the
retired `agent` kind.

### Concurrency and misuse statements (added)

| type | statement |
|---|---|
| `core/session.Core` | fields set before it serves; methods safe from any goroutine |
| `core/session.Dispatcher` | exported fields are configuration set before it serves; methods safe from any goroutine (the gRPC server calls them per stream) |
| `core/session.TCPDispatcher` | methods safe from any goroutine; Configure methods and settings before it serves |
| `platform/ratelimit.Limiter`, `core/identity.Authenticator`, `keyfile.KeyManager` | safe for concurrent use (each holds its mutex; KeyManager serialises its file writes) |
| `frontend/connect.Clients` | safe from any goroutine; a second `Drain` drains what is left (nothing once the first returned) |

Already stated and kept: `InternalAgentLoopback.Close` and
`SessionListener.Close` (idempotent), `keyfile.Store`, `broker.Producers`,
`directory.MemoryCache`.

## Kept although no consumer uses them

Each for a stated reason (an export stays when a consumer uses it, or a
used API's documented use needs it):

- Sentinels callers test or that reach them wrapped: the `broker.Err*`,
  `compose.ErrNoProviders`, `core/session.ErrRefused`,
  `ErrUnknownService`, `frontend/connect.ErrNoCredential`,
  `keyfile.ErrMachineName`.
- Types in a used signature or field (`session.PodDialer`, `Mover`,
  `Reach`, `Owner`, `Opened`, `StreamIO`, `HTTPStreamer`, `GRPCStopper`,
  `ReplicaLink`, `TCPLinkOpener`, `Stream`, `DialPolicy`,
  `core/frontend.ShareLookup`, `identity.Live`, `config.ConfigWatcher`,
  `ChangeFunc`, `RuntimeConfig`, `metrics.AgentSource`, `util.WorkerPool`,
  `ResponseWriter`, `broker.Cacheable`, `ConsumerType`,
  `frontend/sni.Router`): a caller holds or implements them without
  naming them.
- Methods that satisfy an interface (the frontends' `Name`, `Settings`,
  `Attach`; `net.Conn`'s, `net.Listener`'s and `error`'s on
  `PrefixConn`, `SessionConn`, `SessionListener`, `Refusal`,
  `DialError`; the fakes' gRPC stream methods).
- Bounds and values a caller reads: `identity.MachineIDLen`,
  `ShareReadTTL` (grouped with `ShareMissTTL` and `ShareReadMax`),
  `config.DefaultGRPCMaxMessageSize`, `ratelimit.DefaultMaxKeys`,
  `filewatch.Settle`; the `connlimit.Reason*` and `metrics.Path*`,
  `Serves*`, `Transfer*` values are metric label values (surface, below);
  `metrics.PathTCPDataConn` is never recorded but kept, the `Path` order
  is append only.
- `ratelimit.Option`, `WithMaxKeys`, `WithNowFunc`: `New`'s option API.
- The test packages (`relaycoretest`, `sessiontest`, `sessiontest/fake`,
  `directorytest`): doubles for consumers' tests.

## Proposed, not changed (each needs the owner's decision)

1. **The 291 names used only by another relay-core package** (87 in
   `platform/metrics`, 41 in `core/session`, 24 in `core/frontend`, 21 in
   `core/nf1anon`, 18 in `platform/connlimit`, ...) are relay-core's own
   plumbing between its layers; candidates for `internal/` before v1, one
   package at a time, after the import-boundary tests
   (`TestCoreLinksNoFrontendOrStore`, `TestFrontendsImportOnlyTheCore`)
   say which moves keep the layering.
2. **`core/session` methods no other package names** (the
   `Dispatcher.UpdateAgentLabels`, `RunStaleAgentReaper`,
   `ResolveService`, `AgentLastSeen`, `ConfigureTCPRenew`,
   `TCPDispatcher.ConfigureDrain`, `ConfigureHeartbeats`,
   `ConfigureRenew`, `OpenMeter`, `UnmovedStreams`, `Core.RoutedAgents`,
   `StopGRPC`, and `DispatcherConfig`'s `AdminMessage`, `E2EEDefault`,
   `E2EEPolicy`, `NF1Hosts`): not changed, because "no use by type" is not
   "no use" here. relay-server calls `UpdateAgentLabels` through an
   interface of its own (`relayserver/relay_agent.go`), and its
   `relaytest/idle_test.go` names `(*Dispatcher).RunStaleAgentReaper` by
   string in its idle-goroutine table; unexporting either breaks it
   without a compile error in relay-core. Each needs a check of its
   interface and string uses before it moves.
3. **Product keys in the core's config**: `security.google.*`,
   `security.sql.*`, `security.users.*`, `security.session.*`,
   `mailbox.service`, `ui.dev_dir` are read by relay-server (the hosted
   product), never by relay-core. Done in lane RCNAMES: relay-server
   registers them (below, "Decided").

## Surface: config keys and metric names (renames now or never)

### Config keys (194, after `gitops.type`)

Registered by `platform/config.SetDefaultFlags`; set by flags, the
environment and the runtime config file. Every key is read by relay-core,
relay-server, hosted or nfltr (a scan of the key literals; `gitops.type`
was the one none read).

| prefix | keys | names |
|---|---|---|
| `accounts` | 2 | `accounts.read_dsn`, `accounts.service` |
| `admin` | 1 | `admin.message` |
| `agent_link` | 1 | `agent_link.quic.listen` |
| `cache` | 6 | `cache.connection_retry_count`, `cache.redis_master_name`, `cache.redis_replication`, `cache.redis_sentinel_addrs`, `cache.redis_url`, `cache.type` |
| `default` | 1 | `default.request.timeout` |
| `e2ee` | 3 | `e2ee.tls_passthrough.base_domain`, `e2ee.tls_passthrough.enabled`, `e2ee.tls_passthrough.port` |
| `endpoint` | 1 | `endpoint.registry.offline_prune_ttl` |
| `env` | 1 | `env` |
| `grpc` | 9 | `grpc.connection_timeout`, `grpc.keepalive.enforcement_policy.min_time`, `grpc.keepalive.enforcement_policy.permit_without_stream`, `grpc.keepalive.server_parameters.time`, `grpc.keepalive.server_parameters.timeout`, `grpc.max_concurrent_streams`, `grpc.max_connection_idle`, `grpc.max_message_size`, `grpc.proxying.downstream.sync_map.clear_timeout` |
| `http` | 27 | `http.api_type_prefixes`, `http.direct_agent_subdomains`, `http.redirect_port`, `http.request_headers.add`, `http.request_headers.remove`, `http.response_headers.add`, `http.response_headers.remove`, `http.server.enable`, `http.server.fanout_workers`, `http.server.idle_timeout`, `http.server.max_request_body_size`, `http.server.producers.cache_expiry`, `http.server.producers.cache_size`, `http.server.read_header_timeout`, `http.server.uploads.buffer_bytes`, `http.server.uploads.per_ip`, `http.server.uploads.wait`, `http.server.write_timeout`, `http.share_domain`, `http.share_reservation_ttl`, `http.share_store_file`, `http.share_ttl`, `http.site_apex`, `http.site_hosts`, `http.site_hosts_file`, `http.url_rewrite.pattern`, `http.url_rewrite.replacement` |
| `internal` | 5 | `internal.address`, `internal.enable`, `internal.health`, `internal.port`, `internal.readiness` |
| `listen` | 1 | `listen.address` |
| `logging` | 1 | `logging.level` |
| `loopback` | 1 | `loopback` |
| `mailbox` | 1 | `mailbox.service` |
| `metrics` | 2 | `metrics.enable`, `metrics.port` |
| `nf1` | 22 | `nf1.address_burst`, `nf1.address_max`, `nf1.address_rate`, `nf1.anon.bucket_bytes`, `nf1.anon.bucket_rate`, `nf1.anon.enabled`, `nf1.anon.leg_bytes`, `nf1.anon.leg_time`, `nf1.anon.lifetime`, `nf1.anon.max`, `nf1.anon.source_burst`, `nf1.anon.source_max`, `nf1.anon.source_rate`, `nf1.enabled`, `nf1.hosts`, `nf1.open_burst`, `nf1.open_rate`, `nf1.source_burst`, `nf1.source_max`, `nf1.source_rate`, `nf1.websocket`, `nf1.websocket_idle` |
| `on_downstream_errors` | 1 | `on_downstream_errors.drop_agent_connection` |
| `p2p` | 2 | `p2p.turn.ttl`, `p2p.turn.urls` |
| `peer_link` | 6 | `peer_link.ca_cert_file`, `peer_link.ca_key_file`, `peer_link.liveness`, `peer_link.port`, `peer_link.transport`, `peer_link.trust_domain` |
| `pod_id`, `pod_ip` | 2 | `pod_id.env_var`, `pod_ip.env_var` |
| `profiling` | 1 | `profiling.enable` |
| `proxy` | 5 | `proxy.dial_timeout`, `proxy.idle_conn_timeout`, `proxy.max_idle_conns_per_host`, `proxy.response_header_timeout`, `proxy.tcp_keepalive` |
| `ratelimit` | 5 | `ratelimit.burst`, `ratelimit.enable`, `ratelimit.rate`, `ratelimit.visitor_burst`, `ratelimit.visitor_rate` |
| `redis` | 5 | `redis.endpoint`, `redis.min_idle_conns`, `redis.ops_timeout`, `redis.pool_size`, `redis.port` |
| `rpc` | 6 | `rpc.agent.heartbeat.enable`, `rpc.agent.heartbeat.recvthreshold`, `rpc.agent.heartbeat.sendduration`, `rpc.connected_agents_threshold`, `rpc.recv_buffer_size`, `rpc.server.enable` |
| `runtime` | 1 | `runtime.config.path` |
| `security` | 51 | `security.admin_emails`, `security.audit.enable`, `security.conn_limit.*` (13), `security.google.*` (10), `security.ip_allowlist`, `security.machines.file`, `security.min_client_generation`, `security.mtls.*` (4), `security.oidc.*` (7), `security.session.*` (4), `security.sql.*` (6), `security.trust_loopback`, `security.users.*` (2) |
| `ssh` | 2 | `ssh.proxy.enabled`, `ssh.proxy.target_host` |
| `tcp` | 6 | `tcp.dial_max_retries`, `tcp.dial_retry_interval`, `tcp.e2ee_default`, `tcp.e2ee_policy`, `tcp.enabled`, `tcp.endpoints` |
| `termination` | 1 | `termination.grace.periodseconds` |
| `tls` | 12 | `tls.acme`, `tls.acme_cache_dir`, `tls.acme_dns01_cloudflare_token_file`, `tls.acme_domains`, `tls.acme_email`, `tls.auto`, `tls.cert_file`, `tls.enable`, `tls.grpc_only`, `tls.key_file`, `tls.san_dns`, `tls.web_domains` |
| `ui` | 1 | `ui.dev_dir` |
| `usage` | 2 | `usage.meter_interval`, `usage.page_visits` |

Spelling the owner may want fixed before v1 (the `enable` one is
decided and done, "Decided" below; the rest not changed: each rename
touches nfltr `deploy/relay-settings.yaml` (both deploy shapes render
from it, `make check-relay-settings`), the rendered `start.sh` and Helm
values `helm/rpc-server/values.yaml`, relay-server's `selfhost` and
`relaytest` env, hosted's `deploy/` and `helm/`, and every operator's own
config; a v1 that keeps them keeps them for v1.x):

- `enable` and `enabled` both mean on: `http.server.enable`,
  `internal.enable`, `metrics.enable`, `ratelimit.enable`,
  `rpc.server.enable`, `tls.enable`, `profiling.enable`,
  `security.*.enable` against `tcp.enabled`, `nf1.enabled`,
  `nf1.anon.enabled`, `e2ee.tls_passthrough.enabled`,
  `ssh.proxy.enabled`.
- Run-together words: `rpc.agent.heartbeat.recvthreshold`,
  `sendduration`, `termination.grace.periodseconds`.
- `rpc.*` names the agent port by an old name ("rpc server"); so does
  `helm/rpc-server`.

### Metric names (89)

Two schemes: 75 under `rpc_server_{proxy,dispatcher,broker}_` (18, 52,
5: the namespaces of the HTTP ingress, the session core and the broker,
named after a binary that no longer exists) and 14 under `nfltr_relay_`
(`build_info`, `connected_agents_by_serves`, `host_network_bytes_total`,
`presence_events_total`, `transfer_{active,bytes_total,duration_seconds,errors_total,first_byte_seconds,open_seconds,opens_total,queued_bytes,stall_seconds_total,stalls_total}`).
The full list with help texts is `metrics.tsv` in the evidence; per-agent
and per-account labels are summed away before storage (nfltr
`docs/guides/local-relay-metrics.md`).

Proposed for v1, decided and done (below): one prefix, `nfltr_relay_`, for all 89.
Serving both names for a release window would be the dual path the
governance rules forbid, so it is a hard cut in one ordered change:
relay-core's `platform/metrics` (the three namespace constants and the
`relaytest` assertions), then every consumer below in the same promote
round. What names them today:

- nfltr: `deploy/metrics/grafana` (5 dashboards), the alert rules in
  `deploy/metrics/`, `helm/rpc-server/values.yaml`, `cmd/tools/relay-soak`,
  `cmd/tools/cost-ledger`, `cmd/tools/docs-ref`,
  `tests/conformance/protocols`, `scripts/smoke-test-own-relay.sh`,
  `smoke-test-helm-services.sh`, `smoke-test-client-generation.sh`,
  `smoke-replica-link-cross-version.sh`, `relay-capacity-kind.sh`, and the
  blog post `web/blog/relay-metrics-locally-through-nfltr.md`.
- relay-server: `internal/adapters/inbound/relay` (8 files), the
  `relaytest` suites that read metrics (`usage_test.go`,
  `service_connected_test.go`, `nf1anon_test.go`,
  `metrics_labels_test.go`, `datagram_test.go`,
  `agent_link_resume_test.go`), `core/replicas/peer_link_metrics_test.go`,
  `spec/README.md`, three decision docs.
- hosted: `deploy/metrics/grafana`, `helm/rpc-server/{values.yaml,templates,dashboards}`,
  `cmd/tools/relay-soak`, `cmd/tools/cost-ledger`, its guides
  (`standalone-vm-deployment.md`, `standalone-release-runbook.md`,
  `relay-k8s-scaleout.md`).
- The production Prometheus's stored series (history under the old names
  ends at the cut).

Label values are surface too (`connlimit.Reason*`, `metrics.Path*`,
`Serves*`, `Transfer*`): frozen with the names.

### Decided (lead, owner-delegated, lane RCNAMES 2026-11-20)

Hard cuts, no aliases (CHANGELOG "Changed: config keys and metric names
settled for v1"):

1. One metric prefix: every `rpc_server_{proxy,dispatcher,broker}_*` is
   `nfltr_relay_{proxy,dispatcher,broker}_*` (75 names), and
   `..._rpc_server_agent_latency_milliseconds` is
   `nfltr_relay_proxy_agent_latency_milliseconds`. Guard
   `TestMetricNamesHaveOnePrefix` (`internal/repocheck`: every prometheus
   `*Opts` and `NewDesc` name in the module's code).
2. One boolean form: the 12 `*.enable` keys are `*.enabled` (9 here, 3
   with the product keys in relay-server); `config.CheckRenamed` refuses
   an old env name still set. Guard `TestKeysOneSpelling`.
3. The 23 product keys are registered by relay-server. Guard
   `TestKeysOneSpelling`.

Not decided, kept: the run-together words and the `rpc.*` prefix above.

## Consumers affected

None must change for this branch: no consumer at its main names a removed
export (`go vet` of each importing package, above). At their next bump to
v0.19.0:

- nfltr: `docs/reference/config.md` lists `gitops.type` (generated from
  the vendored source); `make docs-generate` in the bump drops its row,
  else `TestConfigReferenceDocIsCurrent` (`cmd/tools/docs-ref`) fails.
- relay-server: `spec/README.md`'s `Tick` row names
  `grpcauth.CredentialEndedStatus`, now unexported: name the answer
  (Unauthenticated "credential expired") in that bump. Prose only; no
  build or test reads it.
- hosted, send, pipe: nothing.

## Guards added

| guard | what fails |
|---|---|
| `TestDocsNameNoGhosts` (`internal/repocheck/docnames_test.go`; link's and relay's test, ported: one module, packages by directory, `frontend` and `core/frontend` share a package name so a `frontend.` name may be either's) | a doc comment of a public package naming an unexported identifier, a member its type lacks, or a name `api/*.txt` records as removed (planted `fooBar` in `MemoryCache`'s doc: fails) |
| `make check-api` (`scripts/check-api.sh`, already a CI step) | an API change since v0.18.0 that `api/v0.18.0.txt` does not list; it lists this lane's and RCNAMES' |

## What the next release makes

`api/v0.18.0.txt` lists every change since v0.18.0 (promote r30, which
shipped `PathBlob` and `ServesHTTP` removed, `DialHTTP` and `HTTPRoute`
added): this lane's 28 removals and RCNAMES' `config.CheckRenamed`
(re-recorded by lane R31PREP on the r31 stack). Names are removed, so
the release is a minor: **v0.19.0**. No consumer at its main names a removed one; relay-server's
`go test ./relaytest/` passes against this branch (the downstream check).

## What "v1 compatible" will mean for relay-core

| surface | v1 promise | guard |
|---|---|---|
| Go API of the public packages | Go 1 compatibility: no exported name removed or changed in v1.x | `make check-api`, `TestDocsNameNoGhosts`, `TestPublicPackagesAreDocumented` |
| config keys | names and meanings frozen; new keys additive; a removed key is refused, never ignored | `platform/config` tests; nfltr `make check-relay-settings` |
| metric names and label values | frozen; new series additive | relay-server's `relaytest` metric assertions |
| the wire (agent link, HTTP headers, `/connect`) | relay's `proto` and link's WIRE.md | those repositories' guards; the cross-version smokes |
