# A new session reaches the agent's current connection, not the one it replaced (2026-11-26)

Lane AGENTREPLACE (design and spec; the code is the lanes under "Code
lanes"). Input: lane NF1ROUTE's sibling audit
(`docs/decisions/nf1-route-declaring-connection-2026-11-26.md`
"Follow-ups"), `spec/Takeover.tla`, `spec/NF1Claim.tla`, nfltr's
one-agent-per-machine design (`docs/decisions/one-agent-per-machine-2026-11-25.md`:
a restart or an upgrade replaces the agent id's connection).

## Gap

A new connection of an agent id (one relay SDK `connectOnce`: a process
restart, `nfltr update`, ONEAGENT's `--replace` hold, a reconnect after a
network drop the relay has not seen yet) replaces the old one in two steps:
its `ConnectHTTP` stream takes the session over (`takeOverSession`: the old
client is told, `errReplacedSessionStatus`), then its `ConnectTCP` stream
registers. The two run beside each other in the SDK (`runWithReconnect`
starts the TCP tunnel while `ConnectHTTP` waits up to
`sessionTakeoverWait` for the old session's teardown), so either can reach
the relay first. Every open chooses its stream in one place,
`TCPDispatcher.agentStreamOrSignal`: the agent id's newest primary. Between
the takeover and the new `ConnectTCP`, that is the replaced connection's
stream. Every caller lands there: onward (owner `/connect`, shares, SNI
E2EE hosts, A2A over TCP), `DialHTTP` (and the HTTP pool's `HTTPRoute`),
`ServeLinkTCP` (an open another replica forwarded), `attach`'s re-pick.
An E2EE open meets the replaced process's certificate, not the one the new
connection registered, and the client's pin refuses it (the nf1 instance:
`TestNF1RelayCap`, 8 of 20 runs under `-race`); any other open runs on a
process that is exiting and is cut when it closes (at most 30 s). NF1ROUTE
fixed nf1 opens only, by their declaration; a connection identity fixes
every caller at the one owner.

## Design

### The wire field

- **Key.** `x-nfltr-conn` (`agentwire.ConnKey`, relay proto
  `proto/wire/agentwire`): the connection's id.
- **Who sets it.** The relay SDK, `client/agent` `connectOnce`: 16 bytes
  of `crypto/rand`, hex, a fresh one per call, in the `ConnectHTTP`
  metadata. The primary `ConnectTCP` stream's context derives from that
  stream context (`runTCPTunnel(..., tcpStreamCtx, ...)`, every SDK version
  since the nf1 declaration), so it carries the same id with no second
  change. A move's new connection is a `connectOnce` too (`moveConnect`):
  its own id. Auxiliary streams and the agent link carry no id: they are
  bound to their primary by its token (`TCPBindHeader`,
  `agentLinkResumeHeader`) and take its id (`primaryOf`).
- **Uniqueness.** 128 random bits per connection. The relay compares ids
  only between connections of one agent id on one relay, and never for
  authorization: the credential already decides who may connect as the
  agent (`takeOverSession`: another credential is refused). A forged or
  repeated id can only misroute the sender's own agent's sessions onto its
  own connections. The relay refuses an id longer than 64 bytes or not
  printable ASCII (`InvalidArgument`).
- **Generation and cut.** Client generation 4 (`agentwire.Generation`):
  every generation-4 client sends the id on `ConnectHTTP` and its primary
  `ConnectTCP`; the relay refuses either stream of a generation 4 or later
  client without one (`InvalidArgument`: a protocol violation, no
  fallback). A client below 4 sends none, so both its streams carry the
  empty id: the rule below then sees one connection per agent, which is
  exactly today's routing, through the same code (no second path). Its
  window stays open until the relay refuses it: the hard cut is
  `security.min_client_generation = 4`, set on the hosted relay once a
  release ships generation-4 clients and
  `nfltr_relay_dispatcher_old_generation_clients_total` reads zero (as the
  generation 2 and 3 cuts, `clientgen`).

### What the relay keeps

Per agent id on `TCPDispatcher` (under `d.Mu`): `cur`, the id of the
agent's current connection here, and `replaced`, the ids of the
connections a commit replaced (at most 4; the oldest dropped: a stream of
a dropped id takes no session either, since it is not `cur`). Dropped
when the agent has no stream and no `ConnectHTTP` session here. Nothing is
persisted: a relay restart reconnects every agent with fresh ids.

### Commit (`takeOverSession`, Takeover.tla's `Connect`)

When a `ConnectHTTP` is accepted (a takeover or a first connect), in the
step that loads the live session and calls `replace()`, before waiting for
the old teardown:

1. `cur` becomes the new id; the old `cur`, when it differs, joins
   `replaced`.
2. Every primary of the replaced id **yields** (`TCPDispatcher.yield`, the
   cross-replica supersede's: out of rotation, `ending`, its open sessions
   and auxiliary streams run on, it ends once idle or at
   `replacedSessionsWait`, 30 s).
3. `d.Streams[agent]` becomes the newest primary whose id is `cur`, or
   none: the **replacement window** when the new `ConnectTCP` has not
   registered yet.

The commit is at `replace()`, not at the end of the wait: the old client
is told then, so it is gone either way (a timed-out wait returns
`Unavailable` and the client reconnects with a fresh id).

### Routing (`agentStreamOrSignal`, the one owner)

`d.Streams[agent]` is always a primary of `cur`'s connection, or absent
(spec `PlaceCurrent`, `CurrentNotYielded`). So `enterRotation` makes a
primary the agent's route only when the agent has no `cur` here (a
`ConnectTCP`-only client, before its first `ConnectHTTP`) or its id is
`cur`; `leaveRotationLocked`'s successor is the newest such primary. A
primary of a newer connection that registered before its commit waits in
`d.All`, taking nothing, and becomes the route at its commit (spec
`WitnessEarly`). Every reader of `d.Streams` inherits the rule unchanged:
`agentStream`, `attach`, `HTTPRoute` (so the HTTP pool's key changes at the
commit, `HTTPPool.tla` Key), the agent link's and rebalance's reads.

### The window

An open that finds no route while the agent has a stream here takes the
existing wait: `streamFor` (the inventory names this pod, since the new
connection's claim is here) waits for the next registration
(`waitAgentStream`: the registration signal, no poll) up to
`agentStreamWait` (1.5 s) and its context; the new primary registering, the
open goes on on it; the bound passing, the dial fails offline
(`DialError`), which each caller already answers (the next replica, an nf1
`NF1NotServed`, a `/connect` refusal). `DialHTTP` with no route goes the
same way through `DialAgent`. No new wait code: the window is the
"agent has no stream yet" case it already handles. An nf1 open then
checks the chosen stream's declaration once (`declaresNF1`): the current
connection not declaring the rid is final, no wait (NF1ROUTE's own wait
loop in `nf1Stream` is deleted).

### A replaced connection's late `ConnectTCP`

A primary `ConnectTCP` whose id is in `replaced` is refused before it
registers, with `errReplacedSessionStatus` (final for the client, as for
its `ConnectHTTP`). Without it, a live client that ignores its notice would
hold that stream forever (spec mutant `late-old-kept`, `ReplacedEnds`).

### Who closes the replaced connection, and when

- Its client: on `errReplacedSessionStatus` (`rpcclient.FatalReplaced`) it
  closes the connection once its sessions ended, at most 30 s (relay
  client v0.19.4+). Unchanged.
- The relay: the commit yields its primaries, so the relay ends each once
  it carries no session, or at `replacedSessionsWait` (its sessions reset,
  typed: `AgentLink.tla`). New for a replacement on one relay (today only
  a cross-replica supersede yields): a client that ignores the notice (an
  older SDK, a hung process) no longer keeps the stream (spec mutant
  `no-yield`, `ReplacedEnds`).
- A dead client: the stream's liveness (`DeadAfter`). Unchanged.

### The other replica

A replaced connection on replica A and the new one on B: A's renewal is
superseded and its primary yields (existing, relay-server
`spec/Replicas.tla`); an open on A finds no route, the inventory names B
and the open is forwarded; B's `ServeLinkTCP` routes it by B's rule above,
so it waits out B's window too. A's routing before its renewal notices the
supersede is relay-server's (Replicas.tla), unchanged by this design.

## Invariants (spec `AgentReplace.tla`) and the test that fails if each breaks

| invariant | meaning | test (code lane L3) |
|---|---|---|
| `PlaceCurrent` | a new session reaches only the current connection's stream (before the agent's first commit here: a connection no commit replaced) | `TestOpenInTheWindowTakesNoReplacedStream` (core/session: the new `ConnectTCP` held, an open of each caller class, none on the old stream); relaycoretest `TestTakeoverWindowOpensReachTheNewConnection` (an agent restarted under its id with a new E2EE certificate, `/connect` opens in a loop across the restart, none refused by the pin; `-race -count=50`) |
| `CurrentNotYielded` | the current connection's stream is never out of rotation | `TestEarlyStreamOfANewerConnectionTakesSessionsAtItsCommit` |
| `NoFalseOffline` | an open answers offline only when no live connection of the agent was coming | `TestOpenWaitsForTheNewConnectionsStream` (the open in the window served once the stream registers within the bound) |
| `UpIsCurrent` | one `ConnectHTTP` is current | `TestConnectHTTPTakeover` (existing) |
| `OpenAnswers` (liveness) | every open answers, served or offline | `TestOpenInTheWindowAnswersOfflineAtTheBound` (the new connection never registers its stream) |
| `ReplacedEnds` (liveness) | a replaced connection's streams end, its client obeying or not | `TestReplacedPrimaryEndsAtTheBound` (a client that ignores its notice), `TestLateConnectTCPOfAReplacedConnectionIsRefused` |

No session is lost silently: an open is placed only on a registered stream
in the same step (`attach` under the stream's lock), every open answers
(`OpenAnswers`), and a stream that ends resets the sessions it carries
(`AgentLink.tla`).

## Failure model

- **The old connection never closes** (a hung or old client, or one dead
  and unseen): after the commit it takes no new session; the relay ends it
  at `replacedSessionsWait` (live) or liveness (dead); its open sessions run
  until then.
- **The new connection never completes** (its `ConnectTCP` never
  registers; the process dies after its commit): opens wait
  `agentStreamWait` and answer offline. They are not sent back to the old
  connection: it was told it was replaced and is exiting, so a session
  there is the bug this design removes. The next connection's commit
  restores routing; the agent is unreachable here meanwhile, visibly.
- **The new `ConnectTCP` arrives before its commit:** it takes nothing until
  the commit; the old connection, still current, serves.
- **The takeover's wait times out** (`Unavailable`): the commit stands (the
  old was told); the client reconnects with a fresh id, a new commit.
- **Two processes under one id on two machines** (a copied config):
  Takeover.tla's rule; the loser is told and exits; its streams yield.
- **A generation-3 client:** today's behaviour (one id, empty) until the
  cut.

## Spec

`spec/AgentReplace.tla` (new; Takeover.tla models the processes and their
machine lock, this one the streams of their connections; its `Connect` is
Takeover.tla's). Three connections of one agent id: `Start` (connectOnce),
`Connect` (the commit), `Register` (the primary `ConnectTCP`, before or
after the commit; refused for a replaced id), `Stop`, `Die`, `Detect`,
`RelayEnd` (the yield's end), `Clear`, and one open at a time from every
caller class (`Open`, `Place`, `Wait`, `Timeout`, `Reset`).

TLC (`AgentReplace.cfg`, three connections): 218,598 states, 39,626
distinct, every action taken (`-coverage 1`; the one branch never taken is
the `no-wait` mutant's), no invariant violated; `AgentReplaceLive.cfg`:
`OpenAnswers` and `ReplacedEnds` hold. Mutants, each caught
(`scripts/spec.sh mutants`): `route-by-id` (the code before:
`PlaceCurrent`, 7 states deep), `no-wait` (`NoFalseOffline`), `yield-all`
(`CurrentNotYielded`), `no-yield` and `late-old-kept` (`ReplacedEnds`).
Witnesses, each violated: `WitnessWindow`, `WitnessEarly`,
`WitnessLateRefused`, `WitnessOffline`, `WitnessServedAfterReplace`,
`WitnessCleared`. Details: `spec/README.md` "AgentReplace.tla".

## Prior art

- SIP Outbound (RFC 5626): a UA's registration carries its instance id and
  a `reg-id` per flow; a newer registration with the same pair replaces
  the flow, and requests go over the flow the registration was made on.
  The agent id is the instance, `x-nfltr-conn` the flow.
- QUIC connection IDs (RFC 9000 section 5.1): the binding is the
  connection's id, not its address.
- MQTT session takeover (MQTT 5.0 section 3.1.4): a new connection with the
  client's id disconnects the existing one (reason "Session taken over")
  and the new connection is the session's from then on.

Nothing is invented: a random per-connection token compared for equality.

## Library first

No library applies: one string comparison on metadata the relay already
reads; the id is `crypto/rand` (standard library).

## Simple, reliable, true

Adds: one metadata key and one client generation. Deletes: `nf1Stream`'s
own wait loop (the window is the general wait), NF1ROUTE's named gap (a
replaced connection that declared the same rid taking opens), and the
"its TCP stream stays, the newest takes every new session" contract in
`takeOverSession`'s comment. Nothing else can go: the id is the only thing
that tells two connections of one credential apart (the peer address was
rejected by NF1ROUTE: a proxy multiplexes agents on one upstream).

## Dogfood

The first nfltr-internal user: nfltr's own agents restarted under their
name, ONEAGENT's `--replace` hold and `nfltr update` on the owner's fleet.
Step: after the release, upgrade the fleet's agents while a `nfltr cat`
loop runs against an E2EE one; zero pin refusals, and the window metric
(below) shows each restart's waits served.

## Observability

A counter of opens that met the window, by result
(`nfltr_relay_agent_replace_waits_total{result="served|offline"}`), and the
commit's log line names the replaced and the new connection ids.

## Environment assumptions

| assumption | source |
|---|---|
| both streams of one connection reach the same relay | one `grpc.ClientConn`, one HTTP/2 connection; the relay's load balancers balance connections (TCP/TLS passthrough), not streams |
| a live connection's `ConnectTCP` registers within `agentStreamWait` of its commit | measured as the time between two streams of one connection (NF1ROUTE); past it the open answers offline and the next is served |
| the relay SDK's TCP stream context derives from its `ConnectHTTP` context | relay `client/agent` `runWithReconnect` (`drainContexts(ctx, streamCtx, ...)`) |

## Open decisions (recommendation first)

1. **Cut:** generation 4, the relay refusing a generation-4 stream without
   the id, hosted `min_client_generation = 4` after the release.
   Recommended. Rejected: the key optional with no generation (the window
   stays for every old client with nothing to show it).
2. **Commit point:** at `replace()`. Recommended (the old client is told
   then). Rejected: after the old teardown (a window as long as
   `sessionTakeoverWait`, 10 s, with the old already told).
3. **The relay's bound on a replaced primary on one relay** (the yield,
   30 s): recommended; a v0.19.4+ client closes within the same bound, so
   only old or hung clients see a change.
4. **The window's answer:** a bounded wait, then offline. Recommended (as
   NF1ROUTE; milliseconds in practice). Rejected: a typed "reconnecting"
   refusal the client retries (a round trip and a client change for the
   same result).
5. **`replaced` memory:** 4 ids per agent. Recommended; a longer chain of
   replacements within one registration window leaves a stream that takes
   no session and ends by its client or liveness.

## Code lanes (bottom up)

1. **relay proto:** `agentwire.ConnKey`, `Generation = 4`; tag
   `proto/vX.Y.Z`.
2. **relay client:** `connectOnce` sets the id; test that `ConnectHTTP` and
   the primary `ConnectTCP` carry the same id and each `connectOnce`
   (reconnect, move) a fresh one; tag `client/vX.Y.Z`.
3. **relay-core:** the state and commit (`takeOverSession` calling
   `TCPDispatcher`), `enterRotation`/`leaveRotationLocked`, the
   `RegisterStream` refusal and id check, the yield at commit, the window
   metric; `ConnectTCP`'s `departed` defer must test `d.All`, not
   `agentStream` (in the window it is nil while streams are here); delete
   `nf1Stream`'s loop and update `NF1Claim.tla` (its `pend`/`Waits` become
   this design's); the trace recorder (`trace_agentreplace_on.go`,
   `spec/TraceAgentReplace.tla`, a rejected fixture per invariant); the
   tests above; before and after `go test -race -run TestTCPTunnel
   ./core/session/` and `BenchmarkTCPTunnelThroughput` (one comparison
   per registration and per commit, none per open beyond today's).
4. **relay-server:** bump; `go test ./relaytest/` and a cross-replica
   variant of `TestOpenWaitsForTheNewConnectionsStream`; NF1ROUTE's
   `peerTCPOpen` field can ride.
5. **nfltr:** bump and vendor; ONEAGENT's hold relies on it; release notes;
   then hosted's `min_client_generation = 4` (decision 1).
