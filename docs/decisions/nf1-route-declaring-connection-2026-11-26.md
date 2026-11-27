# An nf1 open reaches only a connection that declared the address (2026-11-26)

Lane NF1ROUTE. Found by lane R42PREP: relay-core `relaycoretest`
`TestNF1RelayCap` failed 8 of 20 runs under `-race -count=20` at main
(3 of 20 at the r42 candidate) with `dial: nf1: server key does not match
the address's pin`. The r42 stopgap (R42FIX2, `b5ba80b`) gave the test's
second agent its own id; this change restores the original shape.

## Gap

A new connection of an agent id replaces the old one in two steps: its
`ConnectHTTP` stream takes over the session and writes the agent's record
(its service declaration, the `nf1` rids in it), then its `ConnectTCP`
stream registers. Between the two, `openNF1` reads the new record (the
address is the new connection's) and dials whatever stream the agent id has
here: the replaced connection's, the newest primary in `TCPDispatcher`
(`agentStream`). When that connection served another address (the owner
restarted the agent with a fresh key under the same name, or a test's
second agent under one id), its agent answers the TLS with the old key and
the client's pin refuses. That is safe (no wrong peer is ever accepted) but
every open in every same-id replacement window fails: a process restart,
nfltr's one-agent-per-machine hold (lane ONEAGENT).

## Design

- **Ownership of a declaration.** A connection declares its services once,
  in its stream metadata (`agentservices.Header`, with the rids'
  registration proofs, `agentservices.RegHeader`). The relay SDK opens the
  connection's `ConnectTCP` stream with the same metadata as its
  `ConnectHTTP` stream (relay client `connect.go`: the TCP tunnel's stream
  context derives from the ConnectHTTP one, every version since the nf1
  declaration exists). The relay reads it on the primary `ConnectTCP`
  stream too, with the same check (`declaredServices`: a declared rid
  counts only with its proof for one of `nf1.hosts`), and keeps the rids on
  the stream (`tcpAgentStream.nf1`); an auxiliary stream has its
  primary's, never its own (as `enforcesShare`).
- **Routing rule.** An nf1 open (`TCPEndpoint.NF1`, set by `openNF1`) takes
  the agent's newest primary here only when that primary declared the rid.
  Never an older primary (a replaced connection, a connection a move
  leaves): those keep the sessions they carry and take no new one, as for
  every open.
- **The replacement window.** While the newest primary has not declared
  the rid, the open waits for the agent's next registration (the
  dispatcher's registration signal, no poll), bounded by
  `agentStreamWait` (1.5 s) and the open's route budget. A primary that
  declared it registering, the open proceeds on it; the bound passing, or
  the agent leaving this relay, the dial fails as offline and `openNF1`
  tries the next server (the fallback NF1Claim.tla already has), else
  answers not served (the one answer, `NF1NotServed`).
- **Who closes what.** The open closes nothing. The replaced connection
  keeps its sessions until they end (`takeOverSession`: the replaced client
  closes its connection once its sessions ended, at most 30 s); an nf1
  session placed on the new primary ends with that primary's `CloseAll` or
  its own end. A session `attach` moved to another stream (its stream
  retiring) is checked again after the move: one that no longer declares
  the rid gives the session back and fails as offline (as the share gate
  does, `errShareUnenforced`).
- **The other replica.** `ServeLinkTCP` applies the same rule to a session
  another replica forwarded (it calls `onwardLocal`). The replica link's
  open frame is relay-server's (`core/replicas/peer_link.go`
  `peerTCPOpen`): until it carries `TCPEndpoint.NF1`, a forwarded nf1 open
  is routed as before there (follow-up below).

Rejected: a typed "agent reconnecting" refusal the client retries. The
window is the time between two streams of one connection (milliseconds),
so a wait answers the open itself; a retry would cost a round trip and a
client change for the same result. Rejected: pairing the TCP stream with
its ConnectHTTP stream by the peer address (a proxy that multiplexes
agents on one upstream connection breaks it).

## Invariants

1. An nf1 open of rid R reaches only a primary stream (or its auxiliary
   streams, or its agent link) whose connection declared R with its proof
   (`NF1Claim.tla` `RouteSound`, strengthened: `op.via = "rid"`).
2. An open in the replacement window of a key holder whose new connection
   declared R is served once that connection's stream registers, within
   the bound (`NoFalseNotServed`, unchanged; mutant `no-wait`).
3. Every open answers once, served or not served (`OneAnswer`, unchanged).

Environment assumption (spec README): a connection's `ConnectTCP` stream
registers within `agentStreamWait` of its record; past it, an open in the
window answers not served and the client's next open is served.

## Failure model

- The replaced connection's agent is dead and not yet noticed: it is never
  dialled for an address it did not declare, so no open waits on it.
- The new connection dies before its TCP stream registers: its record ends
  (or expires); the waiting open's bound passes and the next server (or
  not served) answers.
- A replaced connection that declared the same rid (a restart that kept
  its key file) is a holder of the key: the rule lets it take the open
  until the new stream registers, as every non-nf1 open does today. Its
  answer satisfies the pin; a dead one fails the dial and the retry waits
  for the registration. Named gap below.

## Spec

`spec/NF1Claim.tla` gains the agent id's connections: `strm` (what the
agent's newest stream's connection declared), `pend` (what its newest
connection, its stream not yet registered, declared), actions `Replace`
(a new connection declaring another address) and `Register`, `Routes`,
`Waits` in `Dial`. TLC (`NF1Claim.cfg`): 2,737,655 states (472,095
distinct), depth 20, 7 s, every action taken (`-coverage 1`). Mutants,
each caught: `route-by-id` (the code before: `RouteSound`), `no-wait`
(the window as a failed dial: `NoFalseNotServed`), and the five earlier
ones. Witness `WitnessWindow` (an open's candidate in the window) reached.

## Prior art

Routing by the connection that holds the binding, not the name: the
flow rule of SIP Outbound (RFC 5626: a request to a registered UA is
routed over the flow the registration was made on) and QUIC's connection
IDs (RFC 9000 section 5.1: the endpoint, not the address, owns the
binding). Nothing new is invented: the check is the share gate's
(`enforcesShare`) on another declared value.

## Library first

No library applies: the rule is one membership check on values the relay
already parses (`agentservices.Parse`, `CheckRegistration`).

## Tests

- `TestNF1OpenWaitsForTheDeclaringConnection` (core/session): a replaced
  primary declaring another rid takes no nf1 session; the open waits and
  lands on the new primary once it registers; with no declaring stream it
  fails offline at its bound.
- `TestNF1RelayCap` (relaycoretest), its original shape (the second agent
  under the same id), `-race -count=50`.
- Before and after: relay-core `go test -race -run TestTCPTunnel
  ./core/session/` and the nf1 tests.

## Dogfood

The first nfltr-internal user is nfltr's own `nfltr cat serve`/`nf1`
agents restarted under their name (ONEAGENT's hold, a process restart):
the relaycoretest restores the same-id replacement it exercises. No
separate dogfood step: the change is the relay's routing of an existing
flow.

## Follow-ups

- relay-server `core/replicas/peer_link.go`: carry `TCPEndpoint.NF1` in
  `peerTCPOpen` (one field each way), so a forwarded nf1 open gets the
  rule on the agent's replica. Gate: relay-server `go test ./relaytest/`
  plus a cross-replica variant of `TestNF1OpenWaitsForTheDeclaringConnection`.
- Sibling routes with the same assumption (audit 2026-11-26, every caller
  of `TCPDispatcher.agentStream`): `onward` (owner `/connect`, shares, SNI
  E2EE hosts, A2A over TCP), `DialHTTP` (HTTP sessions on the agent's
  streams) and `ServeLinkTCP` place a new session on the agent id's newest
  primary, which in the replacement window is the replaced connection's.
  An E2EE open there meets the replaced agent's certificate, not the one
  the new connection registered (`RegisterAgent`, `OnAgentConnect`): a
  client that pins the new one refuses it, the same failure class as this
  one (not reproduced in this lane); any other open runs on the dying
  process and is cut when it closes (its 30 s bound). The nf1 rule
  above cannot cover them: it pairs streams by the nf1 declaration, which
  only nf1 opens have. The general fix is one owner, the stream choice
  (`agentStreamOrSignal`): a per-connection value both streams carry (the
  relay SDK adds one metadata key to its ConnectHTTP metadata, which its
  ConnectTCP stream inherits), the relay pairing the current `HTTPStream`
  with its TCP stream and holding new opens for it as `nf1Stream` does.
  It changes the relay SDK, the takeover contract and `Takeover.tla`
  (a `Register` step after `Connect`, invariant: a new session reaches
  only the current connection's stream), so it is its own lane, spec
  first (designed and specified: `agent-replacement-routing-2026-11-26.md`,
  `spec/AgentReplace.tla`). Gate that fails until then: a relaycoretest
  `TestTakeoverWindowOpensReachTheNewConnection` (an agent restarted
  under its id with a new E2EE certificate, `/connect` opens in a loop
  across the restart: none refused by the pin).
- NF1Claim.tla has no trace recorder yet (open before this lane).

No performance-ledger line: the change claims no performance or resource
gain (a non-nf1 open pays one array comparison).
