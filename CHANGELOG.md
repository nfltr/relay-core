# Changelog

All notable changes to `github.com/nfltr/relay-core` (tags `vX.Y.Z`), newest
first. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

The module is v0.x: a minor version (v0.N.0) may change the exported API,
and every such change is listed here with its migration; a patch version
(v0.N.M) does not change it. `make check-api` holds each release to the
changes listed (from the first tag on).

## [Unreleased]

### Added

- `platform/config.CheckNF1` and the setting `nf1.hosts` (env `NF1_HOSTS`),
  `core/session.DispatcherConfig.NF1Hosts`: the relay hosts a relay's
  capability addresses name. A relay calls `CheckNF1` before it starts: it
  refuses `nf1.enabled` without `nf1.hosts` (`compose.Serve` does).

### Changed

- An agent's declaration of a capability address (`nf1=` in
  `agentservices.Header`) is taken only with its server key's
  registration proof (`agentservices.RegHeader`, relay
  `agentservices.CheckRegistration`) for one of `nf1.hosts`; otherwise the
  agent's ConnectHTTP is refused InvalidArgument (a hard cut: addresses
  minted before relay client v0.21.36, whose rid is not bound to the key,
  are refused; nfltr docs/decisions/nf1-risks-2026-11-14.md, spec
  `NF1Claim.tla`). The proof is checked and dropped, never stored or
  echoed. Migration: agents send `nf1.Listener.RegistrationProof`
  (relay client v0.21.36's `nfnet` does), addresses are minted again, and
  a relay with `nf1.enabled` sets `nf1.hosts`.
- An nf1 open with several agents declaring its rid (the owner's servers)
  goes to the newest claim first and falls back to the next on a failed
  dial (`NF1Agents` returns its agents newest claim first); not served only
  when none is left. A directory failure during the dial answers
  DIRECTORY_UNAVAILABLE, not "not served".

### Changed (incompatible)

- `core/directory.Cache.SetAdd` is replaced by `SetAddIf` (and on
  `MemoryCache` and `sessiontest/fake.MockCache`): a session's claim and
  renewal check the holder and write in one store step (spec
  `Directory.tla`, mutants renew-get-set, delete-get-del, renew-yields).
  Migration: a `Cache` implementation adds `SetAddIf`, which applies the
  add only while the stated condition holds; callers of `SetAdd` call
  `SetAddIf` with that condition.

### Removed

- `frontend/publictcp` and `metrics.PathPublicTCP` (label `public_tcp`):
  public TCP endpoints are deleted (lane PUBDEL, nfltr
  docs/decisions/nf1-udp-2026-11-14.md section 5; they were never offered
  to users and production ran them off). Migration: drop the
  `public_tcp.*` settings and any `publictcp.Frontend` from the
  composition; the zero-account path is `nf1`.

### Added

- Capability addresses (`nf1...`, nfltr
  `docs/decisions/nf1-rendezvous-2026-11-13.md`): `session.Target.NF1`
  (and `Target.Admit`) opens the one live agent whose service declaration
  carries the rendezvous id (`directory.NF1Index`, kept beside the records
  like the service index); `connect.Config.NF1` makes an upgrade with no
  credential a data connection that opens `nf1` sessions only, under the
  `nf1.*` caps (per source, per address, then the serving account's
  visitor limits); `session.NF1NotServed` is the one answer for an unknown,
  offline or doubly declared address. Off by default (`nf1.enabled`).
- `platform/filewatch` (was internal): `Watch` calls back when a file
  changes (its directory watched, event-driven, `Settle`), and `Add`
  watches a directory with an fsnotify watcher without failing when an
  entry of it vanishes meanwhile: on macOS fsnotify's `Add` lstat's every
  entry and returned a vanished one's "no such file or directory", so a
  sqlite journal beside the API key file left the key file unwatched (a
  removed key valid until a restart). relay-server's watchers use it.
  Migration: none.
- Services by name (nfltr docs/decisions/service-discovery-2026-11-12.md):
  `session.ParseAgentAddress` reads `<service>@<group>` (`AgentAddress.Group`);
  `Dispatcher.ResolveService` picks one instance per open among the agents
  of the caller's account that publish the service under the organisation
  group (`agentservices.Decl.Group`) and `Reach` allows, minus
  `Target.Exclude` (`pb.TCPData.exclude`: agent refs or machine names),
  healthy first (`agentservices.HealthLabel`), the down ones when none is
  healthy, uniform random; none online is `AGENT_OFFLINE` (a held open
  waits). `Core.Open` picks again among the instances not yet tried when
  one cannot be reached or its agent refuses (target refused or
  unreachable), and answers the last refusal once every one refused.
  `directory.ServiceIndex` (the shared inventory's (account, group,
  service) index, kept by `PutSession`/`DeleteSession`, pruned on read);
  `session.PresenceServices` fills presence `AgentUp.services`, each service's
  health from the agent's current labels, as the pick reads it (relay-server's
  `ListAgents` fills `Agent.services` with it). Migration: `session.Target`
  is no longer comparable (`Exclude` is a slice): compare its fields (no
  caller in relay-server or nfltr does); the release is a minor one.
- The relay fault hook, for test labs only: a relay built with the
  `relayfault` tag (`frontend/connect/fault_on.go`; a shipped relay has
  none) resets a `/connect` session to the service named in the file
  `RELAY_FAULT_WITHHOLD_FILE` names at its client's first byte when that
  byte opens a TLS record (a direct-path control exchange), and passes one
  that starts otherwise (a relayed re-attach): nfltr's NAT lab withholds
  `nfltr-direct` with it (nfltr docs/decisions/direct-paths-2026-11-11.md).
  Migration: none.
- `grpc.max_connection_idle` (default 5 min, 0 = never): a gRPC connection
  with no stream for that long gets GOAWAY and is closed. A client that
  finished the HTTP/2 preface, answered pings and opened no stream (its
  identity comes with a stream's headers) was held for good
  (docs/decisions/grpc-max-connection-idle-2026-11-11.md). Migration: none;
  an idle client reconnects at its next call.
- `compose.Serve`'s proxy port closes a keep-alive connection idle for
  `http.server.idle_timeout` (default 120 s); it held one for good before.
  Migration: none.
- Relay failover, the endpoint's half (nfltr
  docs/decisions/relay-failover-2026-11-08.md, spec/Failover.tla `Accept`;
  lanes RFRELAYID, RFDEDUPE): `core/directory.RelayID`, the directory's
  relay id (one per directory, written once by the first replica that
  asks; needs a store with `SetNX`: `SetNXCache`, `MemoryCache.SetNX`);
  `session.Dispatcher.RelayID` and `grpcfactory.WithRelayID` send it in
  every Welcome (`Welcome.relay_id`; `compose.AgentPort` passes the
  dispatcher's, `compose.Serve` sets it). A connect (ConnectHTTP,
  AgentTunnel, ConnectTCP, so the agent link) whose
  `x-nfltr-agent-relays` names that id is refused `AlreadyExists`
  `ERROR_REASON_ALREADY_ATTACHED` before it takes a session over or
  claims the record. With `-tags relaytrace`: the `accept`/`refuse`
  recorder (`failover-<relay id>-<n>.ndjson`). Migration: a composition
  that sets no `RelayID` sends none and refuses nothing, as before; to
  opt in, set `Dispatcher.RelayID` from `directory.RelayID` (its store
  must implement `SetNX`). Requires github.com/nfltr/relay/proto with
  `Welcome.relay_id`.

- Share sign-in (nfltr docs/decisions/share-access-2026-11-08.md):
  `core/frontend`: `SharePolicy.Login`, `SharePolicy.LoginAllow` (a share
  only signed-in visitors the rule admits may visit); `frontend/http`:
  `Shares.SignIn`, the composition's sign-in check for such a share (none:
  503, fail closed). The SNI front never serves a sign-in share end to
  end: it goes to the relay's TLS, or an SNI router of its own refuses it.
  No migration: a policy without `Login` behaves as before.
- `platform/connlimit`: `Accounts`, an account's own connection limits
  past the per-source floor (spec/AccountLimit.tla): open connections and
  new ones a second, per account and per (account, source address), under
  `Limits` set per plan (`SetPlan`; the default from the new flags
  `security.conn_limit.account_*`, 0 = no limit). `core/session`:
  `TCPDispatcher.Visitors`, `VisitorRefused`, `AdmitVisitor`; an E2EE share
  connection over its account's limits is answered with a TLS
  `access_denied` alert and closed (refusals counted in
  `rpc_server_proxy_conn_refused_total{listener="sni",reason="account_*"}`).
  `platform/ratelimit`: `Limiter.Ready` (a bucket takes new limits).

- With `-tags relaytrace` only: `Authenticator.TraceRevocations(f)`, each
  revocation's two steps of relay-server `spec/Revocation.tla` in order,
  `cache` (its cached answers dropped) then `cut` (`Live.Cut`), for the
  revocation trace check. Without the tag nothing changes.

- `keyfile`: accounts of a key-file relay (spec/KeyAccount.tla). A line
  may carry a fourth field `account=<machine id>`: the machine belongs to
  that account (`Machine.Account`, `Machine.AccountOf`; `KeyRecord.Account`,
  `KeyGenerateResponse.Account`), and `Provider` gives it that owner, so the
  machines of one account reach each other's agents. `KeyManager.Join(name,
  member)` adds a machine to the account of a machine the file holds.
  `Policy` (session `Policy` and `NameResolver`) resolves
  `<machine name>.<service>` within the caller's account; `compose.Serve`
  uses it with the key file. A name is unique per account (was: per file);
  a reload that changes a kept machine's account is refused. Migration:
  none for a file without the field (every line stays its own account).

- `platform/ratelimit`: a limiter holds at most `DefaultMaxKeys` keys
  (`WithMaxKeys(n)` sets another cap); past it the least recently used
  key is evicted and counted by `platform/metrics.IncrRateLimitEvictions`
  (`proxy_ratelimit_evictions_total`). Buckets are `golang.org/x/time/rate`
  limiters in a `hashicorp/golang-lru/v2` LRU (audit F-26). No migration:
  `New` keeps its signature.

### Fixed

- `core/identity`: `Authenticator.Revoke` clears the credential's cached
  answers before it cuts its connections, so whoever sees a connection
  end at the cut finds no cached answer: the key's next connection is
  asked of its provider. Before, a connection opened right after the cut
  could take the cached answer, and its hold then ended it at once
  (relay-server `TestRevocationReachesEveryReplica`).
- `grpcauth`: under `security.trust_loopback` a loopback call carrying a
  forwarding header (`forwarded`, `x-forwarded-for`, `x-real-ip`) is not
  trusted: it came through a proxy on the relay's host (audit F-33).

### Changed

- Fleet tokens (deleted in relay-server v0.49.21) are gone from the words:
  `security.sql.enable`'s help ("API key and machine store"), an unknown
  credential's message ("invalid API key") and comments. The x-fleet-*
  metadata keys are still dropped from clients.
- The flood floor's defaults rise to a level only an attack reaches, now
  that each account has limits of its own past it (nfltr/nfltr
  docs/decisions/relay-plan-conn-limits-2026-11-08.md): per source
  address `security.conn_limit.per_ip_max` 5000 (was 2000),
  `per_ip_rate` 1000/s (was 200), `per_ip_burst` 5000 (was 2000).

### Changed (breaking)

- `directory.Cache` has two compound steps (nfltr
  `docs/decisions/nf1-index-atomic-2026-11-14.md`; spec/README.md finding
  47): `SetAdd` (write a record and add a member to sets, in one step) and
  `DelIf` (when a predicate holds for the record, remove a member from
  sets and delete the record, in one step). `PutSession`, `DeleteSession`,
  `DeleteIPs` and the index reads' prunes use them, so a claim landing
  between a read and its write is no longer lost (relay-server finding 1b).
  Migration: a `Cache` implementation adds both (Redis: MULTI/EXEC for
  `SetAdd`; WATCH, GET, then MULTI/EXEC for `DelIf`); run
  `directorytest.RunIndexRaces` (new) against it.
- Names resolve once at the edge (TENANTS3; relay-server spec/Reach.tla
  `Dial`, `DialOwn`, `BareNameIsOwnMachine`): `session.ParseAgentAddress`
  is the one parser of an agent address (a ref; a bare service, the
  caller's own machine's; `<machine name>.<service>`, resolved within the
  caller's account), `Dispatcher.ResolveAgent` resolves it, and a
  `Policy` that also implements `session.NameResolver` resolves machine
  names. `Dispatcher.ResolveTarget(ctx, reaches, t, rules)` becomes
  `ResolveTarget(ctx, caller, reaches, t, rules)`: pass the principal
  `Core.Reach` was called with. A2A routing still takes refs only.
- No bound services (TENANTSCERT; relay-server spec/Reach.tla
  `AgentIdOwned`, mutant `ref-other-machine`): a machine registers any
  valid service name, always as its own ref `<machine id>-<service>`, so
  no machine can take another's agent and the bound list protected
  nothing. Removed: `identity.Principal.Names`, `keyfile.Machine.Services`,
  `KeyRecord.Services`, `KeyGenerateResponse.Services`,
  `keyfile.ErrServiceName`, the flags `security.mtls.names` and
  `security.oidc.names_claim`; `KeyManager.Generate(name)` and
  `GenerateForOwner(name, owner)` take no services; `relaycoretest.New(tb)`
  takes none. `identity.ValidService` refuses a name that is itself an
  agent ref. A key file line is `<machine id> <name> sha256:<hex>` and an
  environment key `<machine id> <name> <key>`; a line with a services
  field is refused. Migration: drop the services field of each line.
- Machine identity (TENANTS2, nfltr docs/decisions/tenants-reach-policy-2026-11-07.md;
  relay-server spec/Reach.tla, spec/MachineAuth.tla). An agent is its
  agent ref `<machine id>-<service>` (`identity.AgentRef`,
  `identity.MachineID`: `m-` + 26 base32 characters); `identity.Principal`
  gains `Machine` and `Group`, its `Names` are exactly the machine's bound
  services, and a credential whose provider states no machine is refused
  with `identity.ErrMachineNotEnrolled` (gRPC Unauthenticated and HTTP 401
  with reason `ERROR_REASON_MACHINE_NOT_ENROLLED`). Removed with no
  replacement: `identity.AnyName`, the `Minting` interface, the
  `<provider>:<subject>` agent id prefix, `Dispatcher.AgentKeyBase`,
  `IsAgentConnected`, `PrincipalReaches`, `AgentCredentialReaches` and
  the A2A short-name resolution. Migration: providers state `Machine`;
  callers name agents by ref.
- One reach decision: `Dispatcher.Reach(p, agentID, Action)` (the
  account, then the injected `session.Policy`; no policy refuses, fail
  closed), asked by connect, A2A (the forwarding and the delivering
  replica), WireGuard exits and the serving replica's `ServeLinkTCP`
  (`TCPEndpoint.Caller`). `compose.Serve` sets `session.AccountIsOneGroup`.
  `RegisterAgent` drops its key base argument. A principal's session opens
  at the agent with its caller (`pb.TCPData.Caller`).
- `keyfile` is the self-hosted machine store: each line is
  `<machine id> <name> <service,...> sha256:<hex>` (`keyfile.Machine`,
  `Machine.Line`); `Add`, `Remove`, `Validate`, `Generate` and `Revoke`
  take or return machines; the old `<agent id>:<key>` lines are refused.
  Migration: re-add each machine.

- A share revocation names the share's id (`sharecode.ID`), never its code
  (a secret the issuer's revocation journal and the relays' revocation bus
  carried): `core/frontend.(*CachedGrants).ForgetID(id)` and
  `core/sharesecret.(*VerifiedCache).ForgetID(id)` drop the cached grant
  and verified credentials of the code with that id. `Forget(code)` on
  both is removed. Migration: call `ForgetID(sharecode.ID(code))`.
  `VerifiedCache` keeps the id in place of the code (no plaintext code in
  the cache). relay-server spec/Share.tla `Forget`, mutant
  `forget-by-key`.

### Fixed

- `core/frontend.CachedGrants`: a revocation applied while a lookup of the
  code was in flight was undone when that lookup cached what it read
  before the revoke (up to `ShareReadTTL` on the replica that applied it).
  A forget now bumps a generation; a lookup that saw one start caches
  nothing (`TestCachedGrantsForgetDuringLookup`; relay-server
  spec/Share.tla `CacheFill`, mutant `fill-after-forget`).
- `platform/connlimit` `TestPacketHandshakeFitsItsBurst` no longer fails
  on a loaded machine: it counts the long-header datagrams one handshake
  sends (3 on loopback, up to 5 when the client ACKs the server's flight
  in separate datagrams) and measures again when qlog shows a
  retransmission, instead of running a handshake through a bucket of 8
  that a few PTO retransmissions emptied. The flags' help now sizes
  `per_ip_handshake_packet_burst` at 3 to 8 datagrams a handshake
  (125 to 330 at once), retransmissions spending tokens the rate refills.
  No API change.
- A response whose handler writes no status (net/http sends the implicit
  200 with the header map as it is) no longer carries the headers
  `util.ResponseWriter` removes: front's agent metadata header went out
  with it, and the request was counted with status 0. The new
  `platform/util.(*ResponseWriter).Finish` writes that 200 through the
  writer; front calls it once its handler returns
  (`TestFrontDropsMetadataWithoutWriteHeader`). A caller that removes
  headers (relay-server's Set-Cookie on site hosts and mailbox routes)
  calls it the same way. Addition only: no migration.
- A request a peer replica forwarded that reaches the replica an agent
  just moved off goes on, once, to the replica the agent's record names
  (`Ingress.peer`; the hop is marked `X-Nfltr-Peer-Hop`), instead of 503
  while the agent is connected there (spec/Move.tla `MovedForwards`,
  spec/README.md finding 46). No API change.

### Added

- Usage by share: the meter carries a share's id (`sharecode.ID`, a hash
  of the code; the code itself is never metered or logged) and counts one
  visit each time a share meter opens (`platform/metrics.OpenShareMeter`,
  `Usage.Share`, `Usage.ShareVisits`; `session.(*Dispatcher).OpenShareMeter`,
  `session.(*TCPDispatcher).OpenShareMeter`; the HTTP share path passes it
  with `frontend.WithShare` / `frontend.Share`). Additions only: no
  migration.
- Per-source limits on the QUIC agent link's UDP socket
  (`agent_link.quic.listen`), applied by `TCPDispatcher.AgentLinkQUIC`
  before QUIC parses a datagram: `platform/connlimit.WrapPacketConn`
  (`PacketConfig`), set by `security.conn_limit.per_ip_packet_rate`
  (datagrams a second per IPv4 address or IPv6 /64, default 20000, burst
  `per_ip_packet_burst` 40000) and
  `security.conn_limit.per_ip_handshake_packet_rate` (of those, QUIC
  long-header datagrams: handshakes, 3 to 8 each, and version
  negotiation, default 50, burst `per_ip_handshake_packet_burst` 1000);
  0 turns one off, loopback
  is exempt. A `*net.UDPConn` keeps quic-go's batched reads, GSO and ECN
  (the wrap filters `ReadBatch`). Drops are counted in
  `rpc_server_proxy_conn_refused_total{listener="quic",reason="per_ip_packets"}`
  (`metrics.AddConnRefusals`). Measured: an Initial flood from one source
  no longer grows the relay's half-open handshakes without bound (nfltr
  docs/decisions/relay-flood-hardening-2026-11-05.md, DDOS2). Migration:
  none; a deployment whose QUIC clients share one address (NAT, a load
  balancer without the client address) raises or turns off the limits.
- `session.(*Dispatcher).RouteArriving` and `AddHTTPSession`: a request
  for an agent whose record names this replica, though its HTTP session
  has not registered here yet (a moving agent's new session claims the
  record first), waits up to 1.5 s for that registration instead of being
  forwarded to this replica's own replica-link port (HTTP ingress) or
  refused 503 (a peer's forward). A rolling restart lost a visit to it
  (spec/README.md finding 45). `AddHTTPSession` is how a session registers
  (it wakes the waiters); code that stored into `Dispatcher.Agents`
  directly keeps working, its waiters woken by the 1.5 s bound only.
  Migration: none.
- Per-source-IP connection limits on the public ports (proxy, gRPC, SNI
  router), checked at accept before any byte is read: `platform/connlimit`
  (`Wrap`, `Config`), set by `security.conn_limit.per_ip_max` (open
  connections, default 2000), `security.conn_limit.per_ip_rate` (new
  connections a second, default 200) and `security.conn_limit.per_ip_burst`
  (default 2000); 0 turns one off, loopback peers are exempt. Refusals are
  `rpc_server_proxy_conn_refused_total{listener,reason}`. And
  `grpc.connection_timeout` (default 10s; grpc-go's own is 120s): a gRPC
  connection must finish its TLS handshake and HTTP/2 preface by then. One
  machine's stalled-handshake flood ran a 1 GiB relay out of memory in
  12 s before (nfltr `docs/decisions/relay-flood-hardening-2026-11-05.md`).
  `ratelimit.visitor_rate` (default 20) and `ratelimit.visitor_burst`
  (default 100): one visitor IP's share and site visits to one agent, which
  relay-server checks before the agent's `ratelimit.rate`.
  Migration: a deployment whose clients reach the relay through a NAT or
  proxy that hides their addresses (one source IP for all) raises or turns
  off the per-IP limits.

- A typed balance refusal: `identity.ErrBalanceExhausted` (a provider's
  "balance exhausted"), `(*identity.Authenticator).RevokeCause(key, cause)`
  (a cut with its cause; `Revoke` is `RevokeCause(key, ErrRevoked)`),
  `identity.Ended(cause)`, `grpcauth.BalanceExhaustedStatus()`
  (`FailedPrecondition` + `ERROR_REASON_BALANCE_EXHAUSTED`), `Shares.Refuse`
  (the product refuses a share visit by the agent's owner) and
  `connect.ErrNoCredential`. gRPC answers the refusal with the reason, the
  connect and share paths 402 with it, a cut session aborts with it.
  Migration: none.

- `(*frontend.ShareUses).UsedEnding(code, end)` and
  `frontend.CachedGrants.UsedEnd`: a grant reports the code's end it read
  (`SharePolicy.ExpiresAt`), and a use within `After` of that end is sent
  to the issuer at once instead of at the next flush, so a share visited
  in its last flush interval no longer idle-expires (relay-server
  `spec/README.md` finding S2, `spec/Share.tla` `urgent`). The core-only
  relay (`compose`) sets it. Migration: none; a relay that reports uses
  sets `grants.UsedEnd = uses.UsedEnding` in place of `uses.Used`.

- `(directory.Session).Newer(other)`: whether this session is newer than
  `other` (higher `Seq`, then later `Since`), the order the directory
  already used. `session.TCPDispatcher` uses it to keep a newer session's
  primary when an older one registers a stream (finding 39). No migration.

- `(*session.Dispatcher).ServicesGo`: set with `AgentsGo` on a relay
  without peers, the relay's own services (`DispatcherConfig.Uncounted`)
  go when it closes instead of with the other agents, and an agent other
  than a service is refused a new session (`Unavailable`) once `AgentsGo`
  closed. relay-server sets it so that its last usage flush, which the
  accounts service takes, follows the agents' departure (relay-server
  `spec/README.md` finding M6). Nil: every agent goes at `AgentsGo`, as
  before.

- `(*session.Dispatcher).RouteHere(agentID)`: a route lease on the
  agent's HTTP session here (ok while this replica routes to it), ended
  by `release` once the request's first message is on the agent's topic.
  The HTTP frontend takes one for every request it serves in process; a
  move sends MOVE_DONE only once none is held (`spec/Move.tla`). An
  ingress of its own that routes to `Dispatcher.Agents` in process takes
  one the same way.
- `spec/AgentLink.tla` and `spec/Handoff.tla`: TLA+ models of the agent's
  frame streams with per-session credit and of the choice between the agent
  link and the frame streams (`make spec spec-mutants` in CI; liveness,
  simulation and trace validation in the scheduled job). With
  `-tags relaytrace` and `RELAYTRACE=<dir>` the dispatcher records each
  credit stream's steps for `make spec-traces`; without the tag nothing is
  compiled in. No API change.
- Agent seconds in the usage take: `metrics.AgentUp(account, agentID)`
  and `metrics.AgentDown(agentID)` keep one clock per connected agent id,
  however many sessions it holds; `TakeUsage` reports the whole seconds
  each was connected since the last take as a `Usage` with
  `AgentSeconds` (its Path and bytes zero). ConnectHTTP, AgentTunnel and
  primary ConnectTCP sessions call them; in-process agents do not. A
  caller of `TakeUsage` that sums bytes only needs no change (the new
  rows carry no bytes).
- `(*metrics.Meter).Live(moved)`: a take reads a session's bytes from
  `moved` while it lasts. A forwarded connect session (a data-connection
  session, `session.MeterChain`) is read from link's `HopEvent.Moved`, so
  its bytes show at every `TakeUsage`, not only once it ends: a relay that
  dies loses at most what it had not yet taken. Its end counts only what
  the last take had not seen; the transfer counter reads the same.
  Requires link with `HopEvent.Moved` (v0.12.0).
- `usage.meter_interval` (env `USAGE_METER_INTERVAL`, default 0 = off):
  the interval at which a relay that records usage takes it.
- Usage per payer: `metrics.EnableUsage`, `metrics.OpenMeter(path,
  account, agentID)`, `metrics.AddBytes`, `(*metrics.Meter).End` and
  `metrics.TakeUsage(func(metrics.Usage))`, the bytes each frontend path
  carried since the last take by the account that pays for them.
  `Dispatcher.OpenMeter` and `TCPDispatcher.OpenMeter` open a session's
  meter for its agent's owner (`OwnerForAgent`); connect, sni, http_link,
  http_grpc and a2a sessions are metered once usage is enabled, a
  forwarded connect session through `session.MeterChain`. A metered
  session's bytes cost one atomic add on its own meter and are summed into
  `nfltr_relay_transfer_bytes_total` at scrape, so the usage taken equals
  the path counters exactly (bytes without a meter are usage of no payer).
  Off until `EnableUsage`: nothing changes for a relay that does not call it.
- Every transfer path that opens sessions counts their bytes
  (`nfltr_relay_transfer_bytes_total`) and the first byte from the agent
  (`nfltr_relay_transfer_first_byte_seconds`): http_link, http_grpc, sni,
  connect, a2a and replica counted opens and no bytes, and no session on
  the agent link observed a first byte. `metrics.CountConn` and
  `metrics.CountHTTP` count a session where the relay copies it; a session
  `peerlink.Forward` splices (a connect client's data connection session,
  and its onward leg on the agent link: tcp_link, tcp_quic) takes its
  first byte from link's `HopEvent.FirstOut` (`metrics.RecordChainEnded`,
  `metrics.RecordChainFirstByte`); `metrics.A2AResponse` counts an A2A
  call's relayed response. A package's tests fail when
  they opened sessions on a path that counted no bytes, or bytes from the
  agent and no first byte (internal leakcheck).
- `nfltr_relay_host_network_bytes_total{direction}`: the bytes the host's
  physical network interfaces received and transmitted (/proc/net/dev at
  each scrape; Linux only). On a relay that is its machine's only service,
  transmit is its egress; `process_network_*` counts loopback too.
- `nfltr_relay_connected_agents_by_serves{serves="http|tcp|http_tcp|none"}`:
  connected agents by the kinds of services each declared at connect
  (none: it declared none, as a TCP-only agent does).
- `metrics.TrackAgents`: a relay's connected agents as a scrape counts them.
- The pre-push hook refuses a commit or tag not by the project's public
  identity or with a Co-Authored-By line
  (`scripts/check-identity.sh`; `make check` tests it).
- `session.ErrReplicaUnreachable`: an `Open` whose agent is connected to
  another replica of the relay that this one could not reach (the replica
  link's `link.CodeNoRoute` refusal) is a `*DialError` that `errors.Is`
  this, no longer `ErrOffline`.

### Fixed

- An agent-to-agent call whose target's record names this replica while
  the target's stream here has not registered yet (it claims the record
  first) waits up to 1.5 s for it, as HTTP requests do (`RouteArriving`):
  the call was dialled to this replica itself over the replica link, and a
  peer's `RelayTunnel` refused NotFound. A stale record of this replica
  answers NotFound, never a dial to itself (spec/README.md finding 45).
- `scripts/spec.sh traces` checks relay client/rpcclient's
  `calllifecycle-*.ndjson` against relay's `spec/TraceCallLifecycle.tla`;
  it sorted them into the AgentLink checker, which rejected them.

- Two sessions of an agent with the same Seq (a store that restarted
  empty gave a new claim the Seq of a session still live elsewhere) are
  ordered by their start times, so one yields; before, neither did and
  each renewal took the record back from the other until one stream ended
  (relay-server `spec/README.md` finding R7, `Replicas.tla` mutant
  `seq-tie`).

- An agent's dispatcher record (`ConnectHTTP`, `ConnectAgentTunnel`) and
  streamer record (`ConnectTCP`) take their session Seq from counters of
  their own (`nfltr:agent-session-seq:<record key>`). With one counter per
  agent, the other record's claim read as a newer session: once a live
  HTTP session's record lapsed, its renewal ended the session instead of
  writing the record back (`spec/README.md` finding 34; relay-server
  `TestAgentLinkSurvivesReconnect` failed 3-5 in 10). The counter keys
  change: a relay of this version takes the first Seq above the record's
  current one, as after a store that lost its counters.

- A presence change whose PUBLISH to peers failed is sent again (after a
  backoff of 100 ms doubling to 5 s), folded under the agent's newer
  changes; before, it was dropped and the peers, whose subscriptions did
  not break, never resynced (`spec/README.md` finding 25). Failed sends
  are counted as `nfltr_relay_presence_events_total{origin="failed"}`.
- Failed directory writes no longer change what other replicas list
  without a presence wake (`spec/README.md` finding 27): a label update
  whose write failed, or a record the store lost, is published again
  (an `up`) by the renewal that writes it next, and the failed update
  asks for the next epoch tick's rewrite (`RecordWriteFailed`); a release
  that failed publishes a second `down` once the record's TTL passed.
  `DeleteSession` returns the store's read error instead of reporting the
  record released.
- No presence `down` while a session of the agent is listed on this
  replica: an older session's teardown running after a newer one's `up`
  folded that up away for the peers (`spec/README.md` finding 28).
- HTTP-tunnel (`ConnectHTTP`) and TCP-only (`ConnectTCP`) agents publish
  their presence `up` and `down`: listed by `myAgents`, they never woke a
  watcher (`spec/README.md` finding 29).
- Requests a peer replica forwards take the route lease too
  (`Ingress.peer`): refused once this replica no longer routes to the
  agent, they are answered 503 `Retry-After` and never reach an agent
  that moved (it would run them after MOVE_DONE with the answer lost;
  `spec/README.md` finding 20).
- A drain's move bound ends once the replica stops routing to the old
  stream: a slow lease drain is no longer cut (finding 21).
- A moved agent's TCP primary leaves the rotation when its renewal finds
  the agent's new record: new TCP sessions go where the record points
  (finding 22).
- `carriesSessions` counts every primary of the agent here and the agent
  link's sessions by bind token, so a resumed link's sessions keep the
  agent from a rebalancing move (finding 18).
- A QUIC agent link's idle timeout only unbinds it; it no longer ends the
  agent's TCP primary and its frame-stream sessions (finding 17).

- On a TCP frame stream with the end acknowledgement, a session lost to
  the stream's end or aborted by the agent (its whole close before its
  half close) now reaches the client only as a reset: before, the
  client could get a FIN first (a held half close, or the agent's abort)
  and take a cut stream for a whole one. An in-process reader of an
  aborted session gets a `codes.Aborted` error instead of `io.EOF`
  (`spec/README.md` finding 15).
- Presence across replicas (`spec/Presence.tla`, findings 8, 10, 13): a
  label update (`UpdateAgentLabels`) renews its stream's own session
  record (Seq, credential, account and services kept) and is refused from
  a session a newer one superseded, instead of writing a record with no
  session that a stale session's end then deleted while the agent stayed
  connected; the renewal reads the labels and writes the record under the
  stream's record lock, so it never writes labels an update replaced; a
  renewal that finds no record while the agent's Seq counter went past its
  session's is superseded (`directory.ErrSuperseded`) instead of listing
  the agent again unannounced.

- A make-before-break move no longer sends MOVE_DONE past a request
  routed to the old stream (the 500 ms settle timer is gone: the route
  leases end, then the session's consumer sends MOVE_DONE after what it
  holds), and a rebalancing move given up whose agent's new session took
  the record after all goes on with the move instead of ending the old
  session with its requests (`spec/README.md` findings 6 and 7).
- A revocation that landed between `Authenticator.Authenticate`'s
  provider read and the caller's `Hold` left that connection held, never
  cut, and cached the revoked key's answer for its `CacheTTL` (a key
  file's: 30 s), so that replica accepted the key again. `Live` now
  counts revocations; `Authenticate` reads the count before the cache and
  the provider and keeps it in the principal, the cache insert is skipped
  and `Hold` (also `HoldCarried`) ends the connection at once with
  `ErrRevoked` when the key was revoked after that read. Holds every
  provider (accounts store, key file). Found by relay-server's
  `spec/Revocation.tla` (finding 5).
- An agent reconnecting to the same replica could lose its directory
  record: its old stream's deferred delete (`ConnectTCP`, `AgentTunnel`)
  ran after the new stream claimed the record and before it registered,
  and deleted the new session's record, so other replicas answered
  "agent offline" until its next renewal. The delete now removes the
  record only when it names this pod and the ending session's Seq, and a
  renewal no longer lowers the Seq over a newer session's record on its
  own pod. Found by relay-server's `spec/Replicas.tla` (findings 1, 4).

- An open waiting for its agent's link (a dial request out) that the end
  of the agent's primary stream woke was placed on that ending stream and
  then lost with it: its client got "connection lost" instead of the
  agent offline refusal (or the agent's other stream). A stream now takes
  no session from the moment it leaves the agent's rotation, before its
  link's waiters wake.
- The `http.share_ttl` help said a share code stops working once that
  long passes; it now says what the relay does since share codes expire
  idle: the longest idle lifetime of a code, each visit moving its end to
  the visit plus its lifetime, its URL unchanged.
- An SNI session's lifetime (`nfltr_relay_transfer_duration_seconds`,
  path sni) was timed from its accept, the ClientHello's peek included; it
  is timed from its routing.
- An Accept error other than a closed listener (EMFILE, ENFILE,
  ECONNABORTED) no longer ends the SNI router's `Serve` or a TCP
  endpoint's `ServeEndpoint`: every accept loop of the relay (those two,
  the SNI mux, the gRPC port's data-plane listener) retries it after a
  backoff of 5 ms doubling to 1 s, as net/http does, and counts it in
  `nfltr_relay_accept_errors_total{listener}`.
- The presence feed takes the bus's resync signal: an empty payload from
  `PresenceBus.Subscribe` (the subscription was re-established and may
  have missed peer changes) starts a new feed epoch, so every watcher's
  cursor answers reset and it re-lists, a waiting one at once
  (`nfltr_relay_presence_events_total{change="resync"}`). A replica that
  missed peers' changes during a bus outage kept a stale view until each
  agent's next change.
- An agent's rebalancing stay is forgotten after `stayFor` on every tick,
  also on a replica left alone after a scale-down (it kept its stays).
- `rpc_server_dispatcher_peer_link_conns_total`'s help names the
  `accept_error` event.
- The in-memory broker forgets a topic once its consumer and producer have
  closed (`Unsubscribe` is `Close`): every agent id the relay had seen kept
  its topic, its closed consumer with a 64-message channel and its closed
  producer, about 1.9 KB each, for the life of the process. The relay
  makes an agent's topic only when its session subscribes.
- The in-memory broker reports every message it does not deliver: a send
  to a topic with no consumer fails with `broker.ErrNoConsumer` and a send
  whose context ends first with the context's error (both returned nil
  and dropped the message before); a consumer's `Close` wakes a send
  blocked on its full channel (it waited for the send's context, forever
  with a background one); of concurrent `Subscribe`s to one topic exactly
  one wins; a consumer receives what is sent from `Subscribe` on, not
  from its first `Chan`. The package doc states the broker's contract.
- An agent that connected only over ConnectHTTP no longer leaves its
  last-seen time behind when it leaves (about 160 B per agent id, for the
  life of the process): the dispatcher forgets it with the agent's other
  state once no session of the id is left.
- `util.ResponseWriter` and the header policy's writer pass a 1xx other
  than 101 (103 Early Hints) through as informational: the final status
  still applies, with the headers to remove removed and the policy
  applied. A 1xx counted as final: a later 404 went out as an implicit
  200 carrying the session metadata header, and the policy's removals
  skipped the final response.
- Presence changes reach peer replicas in each agent's order, from one
  publisher per bus that holds at most one pending change per agent (a
  change not yet sent folds into the agent's next, counted as
  `nfltr_relay_presence_events_total{origin="folded"}`). Each change was
  published from a goroutine of its own: a slow bus held one goroutine
  per change, and an agent's quick down then up could reach peers as up
  then down.
- An agent-to-agent call whose route ended before the source's last frame
  (its target offline, refused or stalled) drops the source's later
  frames of that call. Each opened another call that started mid-request
  (a truncated call to a target that came back, or a second answer for
  one transaction).
- A response chunk for a transaction no visitor waits for any more tells
  the agent again to stop it: when the cancel sent as the visitor left was
  lost, the agent kept sending and the session counted the transaction in
  flight until it ended.

- `metrics.CountHTTP`'s writer is an `http.Flusher`: the gRPC HTTP
  path's proxy asserts one to send a streamed response's header before its
  body, and an SSE response's header waited for its first body byte.

- The header policy's writer (`http.response_headers`, `http.url_rewrite`)
  is an `http.Flusher` too, and the gRPC HTTP path's proxy,
  `util.ResponseWriter` and the browse writer flush through
  `http.ResponseController`, which reaches through a writer that only
  unwraps: with a header policy set, a streamed response's header on the
  gRPC HTTP path waited for its first body byte.

- HTTP to an agent connected to a replica this one cannot reach is answered
  503 with `Retry-After` ("connected to another replica of the relay, which
  could not be reached; try again shortly"), not 502 "bad gateway" (the
  replica link forward) or 502 "Agent not connected" (an HTTP session).
- A relay tunnel forwarded onward to such a replica ends `Unavailable` with
  the replica link's message, not an unclassified error.
- An agent-to-agent call to a target connected to an unreachable replica
  is answered `Unavailable` with the replica link's message ("connected to
  another replica of the relay, which could not be reached; try again"),
  not `NotFound` "target agent not connected", and is not counted as the
  agent offline. A target connected nowhere is still `NotFound` and
  counted as `ERROR_REASON_AGENT_OFFLINE` (it was counted `other`).
- A `/connect` session to such an agent is refused at once with that
  refusal instead of being retried through the dial policy's backoff, and
  is counted `replica_unreachable`, not offline.
- An upload over the agent link's QUIC transport no longer gets a 504
  while the agent still feeds what its receive window holds to a slow
  backend: the agent's window updates (its MAX_STREAM_DATA frames, through
  quiclink's `Session.OnGrant`) restart `default.request.timeout` until
  the response begins, as on the agent link over TCP since v0.4.1.

### Changed

- `connect.RequestPrincipal` returns `(Principal, header, error)` in place
  of `(Principal, header, bool)`, and `connect.Frontend.Caller` returns
  `(Principal, error)`: the provider's error (a balance refusal) reaches
  the answer. Migration: `ok` is `err == nil`; no credential is
  `connect.ErrNoCredential`.

- `(*session.Dispatcher).UpdateAgentLabels(agentID, session, labels,
  patch)`: takes the calling connection's number (`rpcclient.SourceSession`
  of the update's stream, the relay's `RoutingInfo.source_session`) and
  refuses an update from a connection the agent's current one here
  replaced (spec/README.md finding 14). Migration: pass
  `rpcclient.SourceSession(stream)`.

- `metrics.CountConn`, `metrics.CountHTTP` and `metrics.RecordChainEnded`
  take the session's `*metrics.Meter` (nil: none, as before); migration:
  pass `nil`, or `OpenMeter`'s meter. `metrics.A2AResponse(first, began)`
  no longer counts the response's bytes: count them with `AddBytes` on the
  call's meter.
- `rpc_server_dispatcher_connected_agents` counts every connected agent
  once, whatever streams it holds: agents with TCP streams only were
  missing from it (it counted HTTP streams only).
- Requires github.com/nfltr/link at main (`HopEvent.FirstOut`).
- Requires github.com/nfltr/link/peerlink/quiclink v0.5.2 (`Session.OnGrant`),
  github.com/nfltr/quic-go with `Stream.SetSendWindowCallback`, and
  github.com/nfltr/link v0.11.2 (peerlinktest's lighter
  volumes for the agent link's QUIC conformance run).

### Removed

- `broker.ConsumerTypeShared`: no caller used it, and the in-memory broker
  now has one consumer per topic. Subscribe with `ConsumerTypeExclusive`
  (the zero value).

- `metrics.RegisterAgentConnect` and `metrics.RegisterAgentDisconnect`:
  connected_agents is counted at scrape from the dispatcher
  (`metrics.TrackAgents`).

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
