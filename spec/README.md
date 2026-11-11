# A formal model of the agent's frame streams and their handoff

`AgentLink.tla` is a TLA+ model of the TCP data path between the relay
(`core/session`, `TCPDispatcher`) and an agent (github.com/nfltr/relay
`client/agent`, `tcpTunnel`) over one frame stream: a ConnectTCP gRPC
stream whose two sides keep per-session credit (`x-nfltr-tcp-credit`,
link phase 1b). `Handoff.tla` models which transport a session takes: the
agent link (`x-nfltr-agent-link`, a peerlink Link on connections bound
with the primary stream's token), else the frame streams, and what a
primary stream's end, a resumed link and a silent link do to the sessions.
Both are model checked with TLC (`make spec`); `Trace.tla` checks recorded
runs of the real relay against `AgentLink` (`make spec-traces`).

This file is the design; it came before the specs and the recorder
(docs/decisions/formal-specs-tla-2026-11-02.md in nfltr/nfltr, follow-up
3). The spec is here, not in github.com/nfltr/relay, because relay-core
owns the larger part: the relay half of both credit ends, the stream
registry, the session placement, the agent link's relay side
(`agent_link.go`: bind, resume, silent, primaryEnded) and the in-process
harness that runs the real agent against the real dispatcher
(`internal/platform/tcprig`, `go test -run TestTCPTunnel ./core/session/`).
The agent half is read from relay `client/agent` (`tcp.go`,
`tcp_dataplane.go`, `agent_link.go`); its Handoff steps are traced by a
recorder there (`TraceHandoffAgent.tla`, "Agent-side Handoff traces").

Out of scope, each covered elsewhere: the bytes of a session on the agent
link or on a data connection (link's own `spec/Link.tla`: a data
connection is a link v2 connection, `dataplane.Conn`); the replicas and
the directory across replicas (relay-server `spec/Replicas.tla`; the
records' own semantics are `Directory.tla` here); revocation (relay-server
`spec/Revocation.tla`); `client/handoff`, which hands a gRPC-only loop's
frames over without per-session state (no session state machine);
agent-to-agent calls, whose per-call credit ends at the two agents
(relay `spec/CallCredit.tla`; the relay's part is below, "Agent-to-agent
calls").

## AgentLink.tla: one frame stream with credit

### What is modelled

Two sides, `r` (the relay) and `a` (the agent), joined by one frame
stream: an ordered queue of frames each way (`ch[p]`, what `p` sent and the
peer has not received). The queue stands for the sender's outbox
(`tcpAgentStream.SendCh`, `tcpTunnel.sendCh`), its one sender goroutine
(`sendLoop`, `runSender`), gRPC and the network: one FIFO per direction is
exact, since every frame of a stream goes through its one outbox.

Per side `p` and session `s` (sessions `Sess`, opened by the relay):

| variable | Go | meaning |
|---|---|---|
| `st[p][s]` | relay `tcpAgentStream.Sessions`, agent `tcpTunnel.sessions` | `none`, `open`, `gone` |
| `cr[p][s]` | `link.Credit.sessions[s]` (`grpcCredit`, `streamCredit`) | `on`, `n` (send credit), `held`, `cons` |
| `pend[p][s]` | a send between `Credit.Acquire` and `enqueue` (`sendData`) | credit taken, frame not yet queued |
| `q`, `rcv`, `wr` | `tcpSession.recvQ` / the agent's `bytequeue.Queue`; counts | bytes queued unread, received, written by the app |
| `wfin`, `rfin` | relay `selfHalf`/`peerHalf`, agent `peerHalf`; whole closes | this side's close (`half`, `whole`), the peer's |
| `finAt[p][s]` | the bytes written before this side's close | what the peer must have received at its EOF |
| `why[p][s]` | `tcpSession.endErr`, agent `lost` | the reader's end: `eof` (the peer's half close released to the app: relay `recvQ` closed, agent hold released), `lost` (`errTCPSessionLost`, an RST), `reset` (`errTCPSessionStalled`; relay side: `errTCPSessionAborted`, the agent's abort, finding 15) |
| `clean[p][s]` | both halves closed, queue drained; or an EOF released after this side's half close | the app's connection ended cleanly (FIN both ways) |
| `waits[p][s]` | relay `selfHalf` before `peerClosedWrite`/`recvAck` closed `recvQ` | the app half-closed before the peer's half close was released to it: the EOF it then gets is a clean end |
| `acked[p][s]` | relay `tcpSession.acked`, agent `halfEnd.acked` | the peer acknowledged this side's half close (`EndAck`, finding 2) |
| `apend[p][s]` | the `go enqueue` of the ack frame | an ack of the peer's half close decided, not yet queued |
| `closing[s]` | `inProcConn.Close` called (finding 3) | the relay's in-process conn is being closed |
| `alive`, `down[p]` | the stream; `unregister`+`CloseAll`, `closeAll` | the stream broke; side `p` noticed |

### What is abstracted

- Bytes are counts; a data frame carries its offset (`off`, the bytes its
  sender wrote before it), so a receiver sees loss, duplication and
  reordering as an offset other than what it received. A close carries
  `at`, the bytes written before it.
- Small sizes (`MCAgentLink.tla`): both windows 2, at most 2 bytes per
  frame, 2 bytes each way per session, two sessions; the grant threshold is
  `max(1, W div 4)` (`Credit.Consumed`: a quarter window). Trace
  validation runs with the real sizes (8 MiB relay window, 16 MiB agent
  window, `dataplane.MaxChunk`).
- A read pops and grants in one step (`Credit.Consumed` then `grant`'s
  enqueue); link's model splits them, nothing here depends on it.
- One stream: auxiliary gRPC streams have their own credit and outbox and
  carry disjoint sessions, so they are copies of this one. Heartbeats,
  control frames other than window updates, the open's answer and the
  agent's refusals other than a close are not modelled.
- The apps: the relay's client connection (`handleLocalConn` writer,
  `pumpToAgent` reader, or an in-process `inProcConn`) and the agent's
  target (`pump`) write, half close, abort (a whole close: a write error,
  `sessionConn.Close`) and read at any time, or never (a stalled reader).
- A stream ends in two logged steps: `Break` (the stream fails: the frames
  on the way are lost) and `Down(p)` (side `p`'s receive loop ended:
  relay `unregister` -> `CloseAll`, agent `closeAll`).

### Actions and the Go they model

| action | Go |
|---|---|
| `Open(s)` | `handleLocalConn`: `attach` (`addSession`, `openCredit`), then the open frame |
| `Write(p, s, n)` | relay `pumpToAgent`/`inProcConn.Write` -> `send` -> `sendData`'s `Acquire`; agent `pump` -> `send` -> `sendData` |
| `SendData(p, s)` | `sendData`'s `enqueue` (into `SendCh`/`sendCh`; dropped once the stream ended: `<-a.Done`, `sendDone`) |
| `CloseWrite(p, s)` | relay `closeWrite` (client EOF), agent `pump`'s half close (target EOF) |
| `StartClose(s)` | `inProcConn.Close` begins: it marks the session closed, then takes `wmu` for its close frame (finding 3) |
| `Abort(p, s)` | relay `handleLocalConn`/`pumpToAgent` error -> close frame, `markClosed`; agent `sessionConn.Close`, `pump`'s read error -> close frame (the session ends at `Finish`) |
| `Read(p, s, k)` | a pop of the session queue (`PopBatch`) -> `Credit.Consumed` -> window frame |
| `SendWin(p, s)` | the window frame `link.NewCredit`'s grant callback queues (`dataplane.WindowFrame`) |
| `Recv(p)`, `RecvData`, `RecvWin`, `RecvClose`, `Release` | relay `recvLoop` (`grpcCreditFrame`, then `Dispatch`); agent `serveTCPStream`'s receive loop: a data frame into the session queue (`PushWithin`), a window frame to `Credit.Grant` (`ParseWindowFrame`), a close frame (`closeFrame`), the half close released to the reader at once or at the ack (`recvAck`) |
| `SendAck(p, s)` | the ack frame (`dataplane.EndAckFrame`) queued by its own goroutine: the receive loop never waits for its outbox |
| `Finish(p, s)` | relay `removeSession` (`handleLocalConn` returned: both halves closed, the peer's whole close, or its own); agent `closeSession` (`pump` after `<-written`, `closeFrame`) |
| `Stall(p)` | without credit only: `deliver`/`PushWithin` time out, `resetSession` |
| `Break` | the gRPC stream fails (network, relay restart, `as.silent`, revocation) |
| `Down(p)` | relay `unregister` (`CloseAll`, `Credit.Close`); agent `closeAll` |

### Invariants

1. `CreditBounds`, `Conservation`: a sender never takes more than the
   peer's window: while both sides know a session, `n + pend + data in
   flight + held at the peer + window updates in flight = W` of the peer;
   `0 <= n <= W`. A receiver never sees data past its window or a window
   update past its own (`ErrCredit`, which ends the stream with every
   session on it, never happens: `NoProtocolError`).
2. `NoHOL`: no session's stalled reader blocks the stream's receive loop:
   with credit, the frame at the head of a stream always fits its
   session's queue (`deliver` and the agent's `Push` never wait).
   `BufferBound`: a queue holds at most its window.
3. `NoProtocolError` also records: data of a session after the peer's half
   close (dropped by a closed queue: `afterfin`); data out of order, lost
   or duplicated (`order`).
4. `TypedEnd`: a reader that ends with EOF received every byte the peer
   wrote before its close; every other end is typed (`lost`, `reset`).
5. `DownEndsAll`: once a side noticed the stream ended, no session is open
   there.
6. `NoSilentLoss`: a session that ended cleanly at a side (`clean`: its
   end, or an EOF released after its own half close: a TCP client sees
   FIN both ways) had every byte
   that side wrote, and its close, received by the peer once the stream is
   gone. It holds with `EndAck = TRUE` (finding 2: a half close is
   acknowledged by an `ack` frame, and both halves end a session cleanly
   only once its own was acknowledged); a stream without it (an older
   agent or relay) is `EndAck = FALSE`, where it fails
   (`race:clean-tail-lost`).
7. `AbortTyped`: the agent's whole close before its half close (an
   abort) reaches the relay's reader as a reset, never an EOF (`EndAck`);
   the relay's abort of a client's session (`RelayAbort`: TCPData.abort)
   reaches the agent's target as one (finding 15).

Witnesses (`make spec-mutants`, each must be violated, so the invariants
above guard reachable states): `WitnessAbort` (an agent abort reaches the
relay), `WitnessRelayAbort` (a relay abort reaches the agent,
`AgentLinkAbort.cfg`), `WitnessHeldEof` (an app that half-closed first
gets a released EOF).

### Liveness (weak fairness on every step that is not an app's)

- `EndsAfterBreak`: after the stream broke, every session ends at both
  sides (`AgentLinkBreak.cfg`).
- `WriteReturns`: an in-process conn's Write returns once its Close was
  called, though the agent reads nothing more and its stream stays up:
  the relay's outbox (`SendCh`) is full, `OutCap` frames
  (`AgentLinkWrite.cfg`, in `make spec`: only the relay side is fair).
- `Isolation`: a session whose apps write, close and read (fairly)
  completes at both sides although the other session's readers never read
  (`AgentLinkLive.cfg`; with credit no stall reset is needed, `NoHOL`).

Every step makes progress (bytes and frames are bounded and nothing is
undone), so a behaviour ends; weak fairness on the union of the protocol
steps (`Protocol`) is then as strong as on each, and much cheaper to check.
The apps of session `Fair` write, half-close and read fairly (`FairApps`);
the other sessions' apps may stop. `Aborts = FALSE` leaves out the
whole close in a config whose property needs the apps to finish.

### Mutants (`make spec-mutants`)

- `fin-overtakes`: a close goes ahead of the data queued before it
  (`TypedEnd`).
- `grant-early`: a window update counts bytes not yet read
  (`Conservation`).
- `no-credit`: an older agent's stream: the receive loop waits for a full
  queue (`NoHOL`).
- `lose-as-eof`: `CloseAll` ends a session the agent had not closed as a
  clean EOF (`TypedEnd`).
- `lose-unacked-clean`: `CloseAll` ends a session whose halves are both
  closed but whose own was not acked as a clean end (`lose` without the
  ack check; `NoSilentLoss`).
- `inproc-closewrite`: an inProcConn's Write queues its chunk after a
  CloseWrite on another goroutine (`NoProtocolError`: `afterfin`; finding
  1, fixed).
- `inproc-enqueue-blocks`: an inProcConn's Write waits for outbox room
  holding `wmu`, and Close waits for `wmu` (`WriteReturns`; finding 3,
  fixed).
- `no-hold`: the peer's half close is released to an app whose own half
  close is not acked yet (`NoSilentLoss`; finding 2's hold, review AL-2).
- `abort-as-eof`: the agent's abort reaches the relay's reader as an EOF
  (`AbortTyped`; finding 15).
- `relay-abort-as-eof`: the relay's abort of a client's session reaches
  the agent's target as a FIN (`AbortTyped` on `AgentLinkAbort.cfg`, and
  link's interface alone; finding 15).
- `no-credit` runs with 3 bytes (one past the window).
- a stream without the end acknowledgement (finding 2: an older agent or
  relay): `race:clean-tail-lost` (`EndAck = FALSE`, `NoSilentLoss`).

## The link session interface (LinkSession.tla, lane LINKIFACE)

`LinkSession.tla` is github.com/nfltr/link's `spec/LinkSession.tla`, the
guarantees a layer above may assume of a link session (link
`spec/README.md` "The session interface"; link's `Link.tla` refines it).
`scripts/spec.sh` reads it from the required link module (`go list -m`,
TLC's `TLA-Library`), so a link spec change cannot strand relay-core
(link v0.12.2 or later; relay-core keeps no copy). relay-core uses link's
sessions in three places and models them only by these guarantees:

| where | which guarantees | spec |
|---|---|---|
| sessions on the agent link (`agentLink.open`, `ServeLinkTCP`, `peerlink.Join`) | a connection's end ends its sessions `lost` (`GConn`: Handoff's `cut`); an open either opens or is refused before any byte (`GSessions`); a dead link connection is detected within Interval + Timeout + 2 x Late, a live one never (link `Liveness.tla`: Handoff's `Silent`) | `Handoff.tla` (`LinkDrop`, `Silent`) |
| bytes of a session on a data connection (`dataplane.Conn`, link v2) | ordered, credited, half close, typed ends (`GWrites`, `GReads`, `GEnds`) | none here: link's |
| AgentLink's frame streams | none: a frame stream is gRPC, not a link session; it uses link's `Credit` (modelled in `AgentLink.tla`'s `cr`, `Thr`: the same algorithm as link's `Link.tla`) | `AgentLink.tla` refines the interface (below) |

`AgentLink.tla` does not run on link sessions: it builds sessions of its
own on a gRPC frame stream. `MCAgentLink.tla` maps it onto
`LinkSession` and `AgentLinkIface.cfg` checks `Refines` (`make spec`, one
byte each way: on `AgentLink.cfg` it tripled CI's run; `AgentLink.cfg` and
`AgentLinkLarge.cfg` check it in `make spec-nightly`, lane SPECTRIM), so a
session placed on the frame streams gives what one placed on the agent
link gives, and a layer above (`Handoff.tla`) can treat the two alike.
The mapping: `w` the bytes written before this side's close (`finAt`
once it closed: a send that held credit before a whole close queues its
frame after it, and the peer, which takes the close first, drops it),
`d` the bytes received (`rcv`), `c` those read (`rcv - q`), `rend` the
reader's end (`why`), `shut` this side's whole close; the relay's whole
close is a Close for an inProcConn (the agent's target reads EOF) and an
abort for a client's session (`RelayAbort`: the target is reset, finding
15), and the agent's an abort (the relay's reader reads a reset); a
stream has no
GOAWAY (`ga` FALSE: its end is `Break`). `make spec-mutants` checks that
`fin-overtakes`, `grant-early`, `lose-as-eof`, `abort-as-eof`,
`relay-abort-as-eof` and `inproc-closewrite` each fail the interface alone (`interface:<mutant>`,
every AgentLink invariant left out).

Frame streams give one guarantee link's interface does not: a session
that ended cleanly at a side had its tail and FIN received by the peer
(`NoSilentLoss`, the end acknowledgement of finding 2). Sessions on the
agent link get it from link's `peerlink.Join` (finding 23, fixed in link).

## Handoff.tla: agent link or frame streams

### What is modelled

The relay's primary streams of one agent `1..MaxGen` (a reconnect is the
next one) with their bind tokens; the agent link's connections bound with a
token (`agentLink.Conns`, one per token suffices); the agent's lazy link
(`lazyLink`: none, dialling, up, parked between primaries: `linkPark`) and
its adoption of a link the relay resumed; the agent's TCP path (the frame
streams and link/TCP ride it, link/QUIC rides UDP); client sessions `Sess`
placed by `handleLocalConn`/`agentLink.open` on the link or on a primary's
frame streams, and their ends; what the relay counts as the agent carrying
sessions (`carriesSessions`, which a rebalance asks before moving it).

| action | Go |
|---|---|
| `Register(g)` | `RegisterStream`: `resumeToken` (a parked link's token), `bound`, the header (`TCPBindHeader`), `dialNow` |
| `Adopt`, `AdoptRefused` | relay `client/agent` `tcp.go` after `stream.Header()`: `lazyLink.adopt`; refused while dialling, or the park's TTL first: `drainNow` |
| `Dial`, `Bind` | the agent's `lazyLink.open`/`run` on `AgentLinkDial`; `agentLink.verify`+`bind` |
| `AgentEnd(g)`, `Park` | the agent's primary's context ends: `lazyLink.end` takes the up link off it, then parks it (`linkPark.park`); `runTCPTunnel` returns, and the agent's next primary connects (`Register`), only after both (the bug before: `end` ran in `context.AfterFunc`'s goroutine, finding 43) |
| `PrimEnd(g)` | `unregister` -> `primaryEnded` (closes the token's connections unless resumed), `CloseAll` |
| `LinkDrop(t)`, `DropAs` | a link connection closes (park TTL, agent stop, network); the agent's up link redials it (its peerlink Node, `linkBackoff`: finding 30) |
| `Silent(t, tr)` | a link connection dead past its liveness: link/TCP `agentLinkObserver.ConnDown` -> `agentLink.silent` (ends the primary bound to the token); QUIC's idle timeout (`quicPeer`): `unbind` only |
| `PathDown` | the agent's TCP path dies (the frame streams' heartbeats, `WatchAgentSession`, take over once no link vouches) |
| `Arrive(s)`, `Place(s)`, `Wake(s)`, `OnLinkOr`, `Attach` | `handleLocalConn` -> `agentLink.open` (link, or wait `agentLinkDialWait` for a requested dial; `node.Open` failing falls back: `OnLinkOr`), else `attach` (`addSession`; an ending primary's: the newest one's, or refused) |
| `End(s)` | a session's apps end it |

### Invariants and properties (`Handoff.cfg`, `make spec`, 82k states)

- `PlacedOnLive`: a session is placed on frame streams only of a primary
  not ended (`attach`'s `addSession` refuses an ending stream:
  `TestOpenWokenByItsStreamsEndIsRefused`), and on the link only through a
  connection bound with the token of an up primary; also after a failed
  link open (finding 19).
- `NoOrphanLink`: a relay link connection exists only for a token an up
  primary holds.
- `CarriesExact`: the relay counts the agent as carrying sessions exactly
  when one is open (finding 18): every primary here, its frame streams'
  sessions and the link sessions of the token it holds, whichever primary
  they opened through.
- `SilenceOnDeadPath`: a link's silence ends a primary only when the
  agent's TCP path is dead (finding 17).
- `ParkOffered`: the agent asks every primary after the one whose end
  parked its link to resume it (finding 43).
- `LinkOpensAnswered`: an open the relay placed on the link is answered
  by the agent, also on a link in the agent's park between two primaries;
  only an open with no stream for it is refused (finding 42).

A session's end is one of `done` (its apps), `lost` (its primary ended
under it), `cut` (its link connection closed) or `refused` (no stream);
how its bytes end is AgentLink's and link's.

Liveness (`HandoffLive.cfg`): `Placed`: an arriving session is placed or
refused (the dial wait is bounded); `SilentEnds`: a primary whose link
went silent ends (finding 16).

Mutants (`make spec-mutants`): `attach-ended` (`PlacedOnLive`),
`keep-conns` (`NoOrphanLink`), `wait-forever` (`Placed`),
`count-per-primary`, `carries-newest` (`CarriesExact`, finding 18: the
code before), `quic-silent-ends` (`SilenceOnDeadPath`, finding 17: the
code before), `park-primary-ctx` (`LinkOpensAnswered`, finding 42: the
agent's code before), `end-async` (`ParkOffered`, finding 43: the
agent's code before). Witnesses, each violated: `WitnessLinkOpen`,
`WitnessFallback` (`PlacedOnLive`), `WitnessResumed` (`NoOrphanLink`),
`WitnessCarryOld` (`CarriesExact`: a link session outlives the primary it
opened through), `WitnessSilent` (`SilenceOnDeadPath`), `WitnessParkedOpen`
(`LinkOpensAnswered`: a session opens on a parked link),
`WitnessEndUnparked` (`ParkOffered`: the agent saw its primary end and
has not parked the link yet). `COVERAGE=1`
takes every action.

Trace validation (`make spec-traces`, `TraceHandoff.tla`): see "Trace
validation" below.

One agent: `resumeToken` refusing another agent's token is not in the
model (tokens are secrets; defence in depth); its guard is
`TestResumeTokenRefusesAnotherAgent`.

### Environment assumptions

| assumption | source |
|---|---|
| A bind token is a 128-bit secret only its primary's agent holds | `newBindToken` (crypto/rand); the header goes only to that stream |
| One agent process per agent id; a second is a takeover | `ConnectAgentTunnel` (relay-server `spec/Replicas.tla`) |
| link/TCP and the frame streams share the agent's TCP path; link/QUIC rides UDP, a path that fails apart (UDP blocked, shorter NAT UDP timeouts) | RFC 9000 §10.1 (QUIC idle timeout), RFC 4787 REQ-5 (UDP mapping timeout) |
| A dead peer is detected by link's ping liveness on link/TCP, QUIC's idle timeout on QUIC, heartbeats on the frame streams | link `Liveness`; quic-go `MaxIdleTimeout`; `HeartbeatRecvThreshold` |
| `node.Open` can fail (GOAWAY drain, close, `agentLinkDialWait`); the caller then attaches | peerlink `Node.Open`; `agentLink.open` |
| An agent whose primary ended registers its next one on a working connection (`Register` is not forced: the model refuses opens offline meanwhile, as designed) | relay `client/agent` `runWithReconnect` (each cycle's TCP tunnel on that cycle's connection: `TestAgentTCPTunnelFollowsReconnect`, finding 42) |

### Code map (drift check)

`scripts/check-spec-drift.sh` reads each code map's last column: a file
of this repository (a path with `/`); another repository's is named
`repo:path` and not checked.

| what the spec models | file |
|---|---|
| `resumeToken`, `verify`, `bind`, `unbind`, `silent`, `quicPeer`, `agentLinkObserver`, `open` (the link-session count), `primaryEnded`, `dialNow` | `core/session/agent_link.go` |
| `RegisterStream`, `unregister`, `leaveRotationLocked`, `carriesSessions`, `carries`, `yield`, `sendLoop`'s silent and supersede cases | `core/session/dispatcher_tcp_streams.go` |
| `handleLocalConn`, `onwardLocal`, `attach` | `core/session/dispatcher_tcp.go` |
| the Handoff trace recorder (`TraceHandoff.tla`) | `core/session/trace_handoff_on.go`, `trace_off.go` |
| the agent's Handoff trace recorder (`TraceHandoffAgent.tla`) | `relay:client/agent/trace_handoff_on.go`, `trace_handoff_off.go` |
| `moveAgent`'s `carriesSessions` check | `core/session/agent_rebalance.go` |
| `lazyLink` (`open`, `end`, `adopt`, `drainNow`), `linkPark`, the resume header | `relay:client/agent/agent_link.go`, `tcp.go` |

### QAD on the agent link's UDP port (lane DIRECTPATH, 2026-11-11)

`quicPeer` answers a connection that negotiates ALPN `nfltr-qad/1`
(`core/session/qad.go`) with the address its packets came from, on one
unidirectional stream, and closes it; it binds nothing, so the agent
link's model above is unchanged (no token, no `bind`, no session). It has
no state of its own to model: one request, one answer, the connection's
end. What the answer may do is in the direct-path design (nfltr
`docs/decisions/direct-paths-2026-11-11.md`): it is the relay's word, an
endpoint uses it only as a candidate, and link's `spec/DirectPath.tla`
(`Offer` with another holder, `OnlyPeer`, mutant `no-bind`) and relay's
`spec/proverif/e2ee_tcp.pv` (queries 21-24) show a forged or withheld
answer costs the direct path only, never a byte or the peer. Candidate
exchange is end to end, inside the session (the design's "Layers"), so
relay-core holds no candidate state either.

## Move.tla: moving an agent's HTTP session to another replica

`Move.tla` models one make-before-break move (`pb.Move`) of an agent's
ConnectHTTP session off replica A (a drain or a rebalance) to replica B,
with the requests visitors send meanwhile, through A's own ingress or a
peer replica's (over the replica link), the drain's bound, and the
agent's TCP primary on A beside the HTTP session. Sources: `agent_move.go`,
`agent_rebalance.go`, `dispatcher_session_watch.go`
(`WatchAgentSession`), `dispatcher_subscribe.go`
(`relayRequestsToAgent`), `dispatcher_tcp_streams.go` (`sendLoop`'s
supersede, `streamFor`), `frontend/http` (`dispatch.go` `dispatcherURL`,
`frontend.go` `peer`, `proxy.go`), and the agent's half in
github.com/nfltr/relay `client/agent` (`move.go`, `connect.go`, `run.go`
`closeWhenAnswered`). The streams are FIFO queues.

| action | Go |
|---|---|
| `Pick` | `dispatcherURL`: A while `RouteHere` gives a route lease, else the record |
| `PickRemote`, `ArriveA` | a peer's `dispatcherURL` reading the record (A); the forward arriving at A: `Ingress.peer` takes `RouteHere` too; refused while the record names B's session, it goes on to B once (`peerHopHeader`, finding 46); else 503 `Retry-After`, the request reached no agent (the visitor's retry is `new` again) |
| `Publish` | `Proxy.publishRequest` of a transaction's first message (on the consumer's channel, or `ErrNoConsumer`), then the lease's release |
| `Unlease` | the lease ends with no message published: the visitor went away first, or the request went over the agent link (`dispatch.go`); it reached no agent here (`gone`) |
| `Forward` | `relayRequestsToAgent`: the channel's next message onto the stream |
| `Ask` | `startMove`: `MOVE_RECONNECT`, the record no longer renewed |
| `GiveUp` | a rebalance's `moveBound` (`giveUp`), also after `MOVE_CONNECTED` while the record check has not seen B's record; none in a drain |
| `StopRouting` | `recordMoved`, `stopRouting` |
| `Refresh` | the renewal after `GiveUp` finding B's record (`directory.ErrSuperseded`): `resumeMove` |
| `Done` | `routeLeases` idle, then `relayRequestsToAgent`'s `moveDone`: the channel drained, `MOVE_DONE` |
| `StartDrain`, `DrainBound` | `stopping`; `stopNow` (`moveBound` from `MOVE_RECONNECT`, disarmed once A stopped routing): the agent let go (`errServerRestarting`) |
| `RecvA`, `EndA` | `receiveFromHTTPAgent`; the agent's EOF ends the session (`failInflight`, `failQueued`) |
| `ServeB`, `FailB` | B serving a request that reached it (its own session, not modelled); B's new session ending after it took the record (MV-5) |
| `PickB`, `ArriveB`, `RegisterB` | B's own `dispatcherURL` (B when B routes, else the record: A, or B itself); a request for B at B (B's own pick, or a peer's forward at `Ingress.peer`): `RouteArriving` waits while the record names B's session that has not registered; B's session registering (`d.Agents.Store` after `claimAgentRecord`, `dispatcher_subscribe.go`) (finding 45) |
| `TCPSupersede`, `PlaceTCP` | `sendLoop`'s superseded renewal of the agent's TCP primary (`yield` when `movingAway`); `streamFor` placing a TCP session |
| `RecvAgent`, `ConnectB`, `DialFails`, `Answer`, `CloseOld` | `processStream`, `moveConnect`, a handler's answer, `closeWhenAnswered` |

Invariants (`Move.cfg`, `make spec`, 11k states): `NoLoss` (no request
fails, unless a drain let the agent go before its new connection took the
record: `lg`, the documented bounded loss), `RunOnce` (no request runs on
the agent without its answer reaching the visitor; the same bound),
`OneOwner` (once A stopped routing the record names B),
`NoTCPAfterMove` (no TCP session is placed on A's TCP primary once it
found the agent's record moved), `OwnerServes` (the replica the record
names never refuses a request while that session lives: between B's claim
and its registration a request at B waits for the registration; finding
45), `MovedForwards` (a peer's forward that reaches A after A stopped
routing goes on to B while the record names B's session, never refused;
finding 46). Liveness (`MoveLive.cfg`, no drain, weak
fairness on every step but the `GiveUp` timer and a peer's pick): every
request answered, and A's old session ends once the agent connected B.
`COVERAGE=1` takes every action.

The design (`Lease = TRUE`): the ingress takes a route lease on the
stream when it picks A, atomically with A still routing, and so does a
request a peer forwarded when it arrives at A; the lease ends once the
transaction's first message is on the consumer's channel (or failed);
`MOVE_DONE` is sent by `relayRequestsToAgent` itself, once A stopped
routing, no lease is held and the channel is drained, so every
transaction the old stream carries precedes it (a later message is a
continuation: a body chunk, an ack, a cancel). A renewal superseded by
B's record after a rebalance gave up goes on with the move instead of
ending the session. A drain's bound covers only the move's first half
(until A stops routing); the settle and the agent's close run under the
relay's shutdown. A TCP primary whose renewal finds the agent moved leaves
the rotation (its open sessions run on).

Mutants (`make spec-mutants`): `race:settle-timer` (`Lease = FALSE`,
finding 6), `race:supersede-ends` (finding 7), `done-ignores-leases`,
`done-overtakes` (`MOVE_DONE` sent past the channel), `close-early` (the
agent closes the old stream with requests unanswered), `peer-no-lease`
(finding 20), `drain-bound-after-stop` (finding 21),
`tcp-stays-in-rotation` (finding 22), `no-arrival-wait` (finding 45: the
code before `RouteArriving`). Witnesses, each violated:
`WitnessHeld` (requests held as A stops routing), `WitnessLetGo` (the
bounded loss is reachable), `WitnessRemote` (a peer's forward in flight
after A stopped routing), `WitnessOwner`, `WitnessTCP` (a TCP session on
A after the record moved: the window before the renewal finds it, which
the agent's `waitIdle` bounds), `WitnessBGone` (A stopped routing while
the record names B's session that ended: MV-5), `WitnessArriving` (a
request at B while the record names B and B does not route yet: finding 45).

`FailB` (MV-5, lane W5-RCORE; `FailBOn`, off in `MoveLive.cfg`, whose
`AllAnswered` it would break by design): B's new session ends after it
took the record. A has stopped renewing and refuses the agent, so the
record names an ended session until its TTL; a request picked meanwhile
is answered 503 Retry-After before any agent ran it (it stays `new`), and
`NoLoss`, `RunOnce` and `OneOwner` (the record never names A again) hold
with it. The agent's next connection is a new session, outside the model.

### Environment assumptions

| assumption | source |
|---|---|
| The in-memory broker's send returns once the message is on the consumer's channel | `broker/broker.go` contract |
| The directory fences records by Seq; the newest session wins; a read may be stale by one write | relay-server `spec/Replicas.tla` |
| A peer replica routes by the record it read; its forward can arrive any time later | `dispatcherURL`, the replica link (relay-server `core/replicas`) |
| B's session may fail after taking the record (`FailB`): the agent reconnects elsewhere, as a new session; requests answer 503 meanwhile (not loss) | finding MV-5 (review) |
| B's session registers within `agentStreamWait` (1.5 s) of its claim, unless it ends: only local steps (the topic, the subscription, the claim's own `SAdd`) lie between; so `ArriveB` has no timeout step while the record names B's live session | `dispatcher_subscribe.go`, `RouteArriving` (finding 45) |
| HTTP requests on the agent link do not ride the topic: MOVE_DONE does not order them; they end with the old connection (park TTL 10 s) | `dispatch.go` (link branch releases the lease), relay `client/agent/agent_link.go` |
| A rebalancing move skips an agent the relay counts as carrying TCP sessions (`carriesSessions`, exact by `Handoff.tla` `CarriesExact`); sessions placed after that check are bounded by the agent's `waitIdle` (30 s) | `moveAgent`; relay `client/agent/move.go` |

### Code map (drift check)

| what the spec models | file |
|---|---|
| `routeLeases`, `RouteHere`, `startMove`, `recordMoved`, `stopRouting`, `resumeMove` | `core/session/agent_move.go` |
| `WatchAgentSession`: `stopping`, `stopNow`, `ask`, `giveUp`, `moved`, `check`, `settle` | `core/session/dispatcher_session_watch.go` |
| `moveAgent`, `stays` | `core/session/agent_rebalance.go` |
| `relayRequestsToAgent`, `moveDone` | `core/session/dispatcher_subscribe.go` |
| `sendLoop`'s superseded renewal (`movingAway`, `yield`), `streamFor` | `core/session/dispatcher_tcp_streams.go` |
| `dispatcherURL`, `peer`, `routeLease`, the first publish | `frontend/http/dispatch.go`, `frontend.go`, `proxy.go` |
| `RouteArriving`, `upstreamArriving` (the A2A paths), the claim before the registration (`RegisterB`) | `core/session/route_arriving.go`, `core/session/dispatcher_subscribe.go`, `core/session/dispatcher_grpc.go`, `core/session/dispatcher_agent_routing.go` |
| the recorder of A's steps (`TraceMove.tla`, "Trace validation") | `core/session/trace_move_on.go` |
| `moveConnect`, `waitIdle`, `closeWhenAnswered` | `relay:client/agent/move.go`, `connect.go`, `run.go` |

## Presence.tla: the presence feed across replicas

`Presence.tla` models the presence feed (`presence.go`) of one agent on
two replicas sharing the directory and a Redis pub/sub bus
(relay-server `redisbus`), with the label update (`UpdateAgentLabels`),
the record renewal (`dispatcher_inventory_refresh.go`) and one watcher
(github.com/nfltr/relay `client/presenceclient` `Wait`, then a re-list:
the cursor first, then the snapshot from relay-server `myAgents`, through
presence `ListAgents` or `/api/v1/my/agents`, which the nfltr hub's slot
wait reads; `Snap` is either). The
feed is a wake signal, not an event log: no consumer applies events, a
woken watcher re-lists. So the guarantee checked is `NoSilentStale`: a
watcher whose snapshot differs from what a list answers now is owed a
wake (its cursor is behind the feed, a change is still to be published or
delivered, its replica's subscription is down and its resync comes, or its
replica is down). Event order across replicas does not matter for it.
The agent's sessions are ordered by Seq as `Move.tla` and relay-server
`Replicas.tla` give them (the newest session holds the record; an older
one ends); the bus loses every message in flight with the subscription
(Redis drops a slow subscriber's buffer, restarts), and the
resubscription is the resync (a new epoch: every cursor answers reset).
The cursor is abstracted to `due` (the feed moved since the watcher took
it), exact for the wake: the watcher only asks "anything after my
cursor?". One watcher suffices (watchers only observe); with it on `r1`
the agent's stream is local or remote by where it connects.

Scope: every agent `myAgents` lists. `ConnectAgentTunnel` agents (the
agents the nfltr hub places work on), `ConnectHTTP` agents and TCP-only
(`ConnectTCP`) agents take the same `Connect` and `End` actions: each
writes its record, then publishes its up; each releases, then publishes
its down once no session of it is listed here (finding 29).
The relay-core fixes of findings 8, 10, 13 and 25-27 reach the hosted
relay with the next promote-chain round (relay-server requires
relay-core v0.7.2, which has none of them; review r2 F8).

| action | Go |
|---|---|
| `Connect`, `End`, `Supersede` | `ConnectAgentTunnel`: `claimAgentRecord`, `notifyUpstreamRegistered`; `ConnectHTTP` (`Presence.up` after `Agents.Store`; `DeregisterAgent`), `ConnectTCP`; their teardowns `releaseAgentRecord`, `departed`, `publishDeparture`; a renewal's `directory.ErrSuperseded` |
| `Renew` (`RenewWrite`) | `refreshAgentInventoryOnInboundActivity`, `renewAgentRecord`, `PutSession` renew |
| `Update` (`UpdateWrite`), `Apply`, `UpdateRecv`, `UpdateApply` | `UpdateAgentLabels` (relay-server `serveUpdateLabels`); with `InFlight` the request received on a stream (`rpcclient.SourceSession`), applied under the stream's record lock only to that session (finding 14) |
| `Publish`, `PublishFail`, `Deliver` | `presenceOutbox` (`publish`, `requeue` on a failed send), `BindBus`'s subscriber: `bump` |
| `Drop`, `Resubscribe` | Redis dropping a subscriber, go-redis resubscribing; redisbus's empty payload, `presenceFeed.resync` |
| `Wake`, `Cursor`, `Snap` | `presenceFeed.Watch` answering, `ListAgents` (`Presence.Cursor`, `myAgents`) or `handleMyAgents` |
| `UpdateFail`, `EndFail`, `Lose` | a failed record write (`UpdateAgentLabels`, `releaseAgentRecord`); the store losing a live session's record (Redis restarted empty, renewals failing past the TTL; relay-server `inventory_epoch.go`: `reannounceAll`) |
| `LateDown` | `departed`'s second down a TTL after a failed release |
| `Sweep` | relay-server `inventory_epoch.go` `noteReplicas`, `presence_resync.go` `resyncBus` |
| `CrashR`, `Expire` | a replica gone without its teardown; a record's TTL (its replica gone, or a failed release's leftover) |
| `OtherClaim` | mutant `shared-counter` only: a claim of the agent's other record taking the same counter (finding 34, the code before) |

The design (`Presence.cfg`, `make spec`, 17k states): a label update is a
fenced renewal of the stream's own session (refused when superseded), and
a renewal and an update write the record under the stream's record lock;
every directory write that changes a listing is published after it (a
write that failed, or a record the store lost, once a later renewal
lands; a release that failed, again once its record's TTL passed); a
renewal finding no record while the agent's Seq counter is past its
session's is superseded (it does not write back a record a newer session
left), and that counter is the record's own, so only a newer session of
this record moves it (finding 34: `SupersededByNewer`); a list whose directory read fails answers Unavailable, not the
local agents only, on either list (presence `ListAgents` and
`/api/v1/my/agents`); a publish that fails stays pending and is sent
again (`PublishFail` is a stutter in the design); the bus never drops a
payload it could not hand over without a resync. Witnesses, each violated, so `NoSilentStale` is not vacuous:
`WitnessStale` (a watcher's snapshot stale, owed a wake),
`WitnessRemoteUpdate` (a label update made on the other replica reaches
the watcher's snapshot); `WitnessSuperseded` (a stream superseded, so
`SupersededByNewer` is not vacuous); `WitnessReplaced` (an action
property: a stream replaced by a newer session on its own replica).
`Connect` on a replica that has the agent's stream replaces it (F9): the
old stream's teardown releases by its own session and publishes no down
while a session of the agent is listed here (finding 28). The order of
the old teardown's steps against the new connect changes only the
events' contents (an up folded into a down), never whether a wake is
owed, so the model keeps one stream per replica and the contents are
the consumer's (finding 28, F6). Finding 14 (an update in flight from
the replaced connection, applied after the replacement) is `InFlight`'s
`UpdateRecv` and `UpdateApply`, fenced by the session the request came on
(`OwnUpdate`). `PresenceLive.cfg` (`make spec-liveness`, 57 s) checks `Woken`
under weak fairness on the feed's steps (`Publish`, `Deliver`,
`Resubscribe`, `Wake`, `Cursor`, `Snap`): a stale watcher is woken, or
the listing comes back to its snapshot. Mutants (`make spec-mutants`),
each caught: `unfenced-label-write` (finding 8), `partial-list` (9),
`partial-my-agents` (24), `renew-race` (10), `drop-no-resync` (11),
`renew-resurrects` (13), `publish-drop` (25), `publish-before-write`,
`resync-early` (the resync before the subscription is back),
`resync-ignored` (the feed ignoring the bus's resync signal: the bug
a0154c2 fixed), `silent-rewrite`, `no-late-down` (27),
`shared-counter` (34), `release-by-agent` (an agent replacing its
stream on the same replica, review r2 F9: the old stream's teardown
released the agent's record whatever session it named, the code before
2a1cf33; the new session's renewal wrote it back unannounced);
on `PresenceLive.cfg` (`Woken`): `subscriber-stops` and `outbox-stops`
(the subscriber or the outbox gone while the replica drains and serves:
the safety config cannot see them, finding 26) and `publish-drop`;
`no-crash-resync` (`Crash = TRUE`, finding 12). `PresenceLarge.cfg`
(`make spec-nightly`) checks the design with a crash. `COVERAGE=1` takes
every action but those only a mutant or a larger config enables
(`PublishFail`, `RenewWrite`, `UpdateWrite`; `CrashR`, `Sweep` in
`PresenceLarge.cfg`). Trace validation (`make spec-traces`,
`TracePresence.tla`): see "Presence traces".

### Environment assumptions

| assumption | source |
|---|---|
| Redis pub/sub is at most once: a subscriber gets a message only while subscribed; nothing is kept for one that was away | Redis docs, "Pub/Sub" (delivery semantics) |
| Messages of one publisher reach a subscriber in publish order | Redis docs, "Pub/Sub" (one connection, in order) |
| PUBLISH can fail or time out on the publisher alone while peers stay subscribed, and succeeds again eventually (`PresenceLive.cfg`) | go-redis `Publish` error; redisbus `Bus.Publish` |
| A dropped subscription is followed by a resubscription, whose confirmation redisbus turns into a resync (empty payload), in order before later messages | go-redis `PubSub` reconnect; redisbus `Resync` |
| A directory write may fail (`MaxFail`); the store may lose a live session's record; relay-server's epoch tick (every 5 s) then has every session here rewrite its record (`Lose` sets `stale` at once: the 5 s gap is unmodelled) | relay-server `inventory_epoch.go`, `Replicas.tla` |
| A record expires at its TTL after its last write: a timer one TTL after a failed release fires after it (`LateDown` waits for `Expire`) | Redis `EXPIRE`; `departed` |
| The newest session (by Seq) holds the record; an older one is superseded at its next renewal | `Move.tla`, relay-server `Replicas.tla` |
| The Seq counter is the record's own: the dispatcher record (`ConnectAgentTunnel`, `ConnectHTTP`) and the streamer record (`ConnectTCP`) count apart (finding 34) | `core/directory/session.go` `seqKey` |
| A replica crash takes its streams with no teardown; a graceful stop runs the teardowns with the outbox and the subscriber still bound (until the composition's `After`, finding 26), so it is a sequence of `End`s | `CrashR`; compose `Composition.Run`, relay-server `Composition` |
| A crashed replica's marker expires within 30 s (`inventoryEpochTTL`); its records within their TTL of its last renewal, so a resync one TTL after another replica sees the marker gone follows them (`Sweep` waits for `Expire`); a replica starting resyncs once a TTL after its first look | relay-server `inventory_epoch.go` |
| The watcher's cursor, its long-poll and its re-list reach one replica (another one answers reset: a re-list, safe since the write precedes the publish) | presence.proto; review F10 |
| A watcher re-lists on any wake (no event is applied); `ListAgents` takes the cursor before the snapshot | relay `client/presenceclient` `Wait`; relay-server `my_agents.go` |

### Code map (drift check)

| what the spec models | file |
|---|---|
| `presenceFeed` (`bump`, `resync`, `Watch`, `Cursor`), `BindBus`, `presenceOutbox` (`add`, `run`, `publish`, `requeue`), `notifyUpstreamRegistered` | `core/session/presence.go` |
| `UpdateAgentLabels`, `publishDeparture`, `DeregisterAgent` (`ConnectHTTP`'s End) | `core/session/dispatcher.go` |
| `claimAgentRecord`, `renewAgentRecord`, `releaseAgentRecord`, `refreshAgentInventoryOnInboundActivity`, `rewriteRecord`, `departed` | `core/session/dispatcher_inventory_refresh.go` |
| `ConnectAgentTunnel`: its record, its up, its teardown | `core/session/dispatcher_grpc.go` |
| `ConnectHTTP`'s Connect (`Presence.up` after `Agents.Store`) | `core/session/dispatcher_subscribe.go` |
| `ConnectTCP`'s Connect and End (`claimAgentRecord`, `departed`) | `core/session/dispatcher_tcp.go` |
| the reannounce case (`rewriteRecord`) | `core/session/dispatcher_session_watch.go` |
| `disconnectAgent` (the reaper's End) | `core/session/dispatcher_stale_agents.go` |
| the presence trace recorder (`TracePresence.tla`) | `core/session/trace_presence_on.go` |
| `PutSession`, `DeleteSession` | `core/directory/session.go` |
| the bus, the gateway, `ListAgents`, `handleMyAgents`, the bus's context, `noteReplicas`, `resyncBus`, the watcher | relay-server `redisbus`, `presence.go`, `my_agents.go`, `server_run.go`, `inventory_epoch.go`, `presence_resync.go`; `relay:client/presenceclient` |

## Directory.tla: an agent's session records in the directory

`Directory.tla` models the records of `core/directory/session.go` as one
agent's sessions on two pods see them: `ClaimSession` (the key's next Seq
from its own counter, `seqKey`, above the record's; the record written),
`PutSession` with renew (`Renew`: yield with `ErrSuperseded` to a newer
session's record on another pod, write nothing under a newer one on its
own pod, yield when there is no record but the counter is past its Seq,
else write), `DeleteSession` (`End`: delete the record only while it
names this pod and this session), a record's TTL passing (`Expire`) and,
once, the store restarting empty (`StoreLoss`: records and counters gone);
and a claim whose counter step fails (`ClaimNoSeq`: the store unreachable;
ConnectTCP goes on with a session without a Seq, which renews and ends by
its start time alone, finding 37).
| action | Go |
|---|---|
| `Claim(p, k)`, `ClaimNoSeq(p, k)` | `ClaimSession` (`nextSeq`: `Incr` on `seqKey`, above the record's Seq; then `PutSession` without renew); its `Incr` failing |
| `Renew(i)`, `RenewWith` | `PutSession` with renew (`renewAgentRecord`), as one step |
| `End(i)`, `EndWith` | `DeleteSession` (`releaseAgentRecord`), as one step |
| `Read(i, op)`, `Done(x)` | `TwoStep` only: the GET of `PutSession`'s renew or `DeleteSession`, then its SET or DEL |
| `Expire(k)` | the record's TTL (`MemoryCache.liveLocked`, Redis `EXPIRE`) |
| `StoreLoss` | the store restarting empty |

Both record keys (the dispatcher's, the streamer's) are in it, so finding
34 is. Each store call is atomic. `PutSession`'s renew and
`DeleteSession` are two calls (a GET, then a SET or DEL); `Directory.cfg`
takes each as one step, and `DirectoryHeal.cfg` (`TwoStep`) splits them
(`Read`, then `Done` deciding on what was read, finding 38). The
replicas, the epoch and the re-announce are relay-server `Replicas.tla`'s.

Invariants (`Directory.cfg`, `make spec`, 492,852 states (129,090
distinct), 4 s): `SupersededByNewer` (a session yields only to
a newer session of its own record), `NoResurrect` (until the store loses
its counters, a record never names a session older than another claimed
one of its key), `DeleteOwn` (a record goes only with its own session's
end, its TTL or the store), `NoPingPong` (of two live sessions of a key,
one yields to the other: no two renewals overwriting each other). Each
has a witness TLC must violate (`WitnessSuperseded`,
`WitnessResurrectSetup`, `WitnessEndKeeps`, `WitnessTie`). Mutants
(`make spec-mutants`), each caught: `renew-resurrects` (finding 13),
`shared-counter` (34), `seq-only` (36: the code before its fix),
`seq-tie` (relay-server finding R7, the Seq compared alone),
`delete-any`, `seqless-by-pod` and `seqless-masks-counter` (37: the code
before its fix). `NoResurrect` judges records of sessions with a Seq: a
session without one cannot know a newer one ended. With the two calls
apart (`DirectoryHeal.cfg`, 1,126,085 states (320,277 distinct), 7 s) the
race does break `DeleteOwn` (`race:renew-get-set`: a claim between an
end's GET and its DEL is deleted) and `NoResurrect` (an older session's
SET over a newer one's record), and `Heals` holds: the agent's latest
session's next renewal writes its own record, whatever was read or
written meanwhile (mutant `heal:renew-yields`: a renewal that yields to
any other record never heals). `COVERAGE=1` takes every action and every branch of
`Renew` and `End`.

Model-based test: `scripts/spec.sh walks` writes 60 random walks of
`Directory.cfg` (`-simulate`, depth 12, seed 1; the walk is written by the
config-only invariant `WalkDump` through `TLCExt!Trace` and
`ndJsonSerialize`) to `spec/testdata/directory-walks.ndjson`, one state a
line; core/directory `TestDirectoryModelWalks` replays each against the
real store (`MemoryCache` behind the dispatcher's and the streamer's
inventories, each step on the pod the walk names) and compares the
records (pod, Seq, start time), the Seq counters and every session's Seq
and state after every step. It fails unless the walks take every action
and supersede a session; `TestDirectoryModelWalksCatchPlantedBugs`
replays them against a store with finding 34's bug (one counter) and
finding 13's (a renewal blind to the counter) and must diverge. The
replay found finding 36. Regenerate the walks when the spec changes.

### Environment assumptions

| assumption | source |
|---|---|
| Each store call (GET, SET, DEL, INCRBY) is atomic and linearizable; `PutSession`'s renew and `DeleteSession` are two calls each, modelled apart in `DirectoryHeal.cfg` (finding 38) | Redis docs, "Transactions" (single commands are atomic); `MemoryCache` (one mutex) |
| A key expires at its TTL after its last write; a counter (`seqTTL`, 30 days) outlives every record | Redis `EXPIRE`; `MemoryCache.liveLocked` |
| A store that restarts empty loses records and counters together (`StoreLoss`, once) | Redis without persistence; relay-server finding R7 |
| Start times (`Since`) of an agent's sessions are distinct; their order only breaks a Seq tie, so a skewed clock picks which of two tied sessions yields, never whether one does | `Session.Newer` |
| A session has a Seq unless its claim's counter step failed (`ClaimNoSeq`, the streamer's record only: ConnectTCP's claim is not fatal); a record with neither Seq nor start time (the relay's own agent's `SetIP`) is not modelled | `ClaimSession`, `dispatcher_tcp.go` |

### Code map (drift check)

| what the spec models | file |
|---|---|
| `ClaimSession`, `nextSeq`, `PutSession` (renew), `DeleteSession`, `Session.Newer`, `seqKey` | `core/directory/session.go` |
| `MemoryCache` (`Get`, `Set`, `Del`, `Incr`, expiry): the store the walks run on | `core/directory/memory.go` |

## KeyAccount.tla: accounts of a key-file relay

`KeyAccount.tla` models the key file of a relay without a database
(`keyfile`, lane KEYACCT, nfltr/nfltr
`docs/decisions/tenants-reach-policy-2026-11-07.md`): each line is a
machine (an id the file assigns, never reused), a name and an account. A
machine generated alone is its own account, whose id is that first
machine's; `keys generate --account X` joins the account of X, a machine
the file holds. The provider's owner is the line's account, so relay-core
Reach (owner == owner) lets the machines of one account, and only they,
reach each other's agents; a name resolves within the caller's account.
A reload swaps keys and names in one step and is refused when it would
change a kept machine's account; revoking an account's first machine
leaves the account to the others.

| action | Go |
|---|---|
| `Generate(m, n)` | `KeyManager.Generate` / `GenerateForOwner` (`generate` with no member) |
| `Join(m, n, j)` | `KeyManager.Join` (`accountOfMember`: j by id, or a name one machine holds) |
| `Revoke(m)` | `KeyManager.Revoke` |
| `Reload(m, n, a)` | `KeyManager.Reload` (the edited file: `readAPIKeys`; refused when a kept machine's account changed) |

`Owner(m)` is `Provider.Authenticate`'s owner (`Machine.AccountOf`) and
`Resolve(c, n)` is `Policy.ResolveMachine` (`APIKeyStore.resolve`).
Invariants (`KeyAccount.cfg`, `make spec`, 4 ids, 2 names: 53,273 states
(10,329 distinct), 1 s; `COVERAGE=1` takes every action): `AccountStable`
(a machine's account never changes after generate), `AccountRooted` (every
account is the id of a machine generated as its own), `NamesUnique` (per
account, among live machines), `SameAccountReaches` (one owner exactly for
one account), `ResolveOwnAccount` (a name resolves to at most one machine,
of the caller's account). Mutants (`make spec-mutants`), each caught:
`join-outside` (`--account` naming a machine the file does not hold),
`dup-name`, `revoke-reassigns` (the first machine's revoke orphaning the
others), `reload-account` (a reload changing an account), `owner-own-id`
(every line its own account: the key file before KEYACCT),
`resolve-global` (a name looked up across accounts). Witnesses, each
violated: `WitnessOutlivesFirst` (two machines of an account whose first
machine was revoked), `WitnessNameReused` (a revoked machine's name taken
again in its account at once), `WitnessNameInTwoAccounts`. Tests:
keyfile `TestKeyFileAccounts`, `TestLoadAPIKeysRefuses`. No trace
recorder yet (each manager call is one locked step of one process).

### Code map (drift check)

| what the spec models | file |
|---|---|
| `Generate`, `Join`, `Revoke`, the line's account | `keyfile/keymanager.go`, `keyfile/apikey.go` |
| `Reload` | `keyfile/keyfile_reload.go` |
| `Owner`, `Resolve` | `keyfile/provider.go` |

## AccountLimit.tla: an account's own connection limits

`AccountLimit.tla` models one relay replica's limiter of visitor
connections per account (`platform/connlimit` `Accounts`, lane CONNLIMIT,
nfltr/nfltr `docs/decisions/relay-plan-conn-limits-2026-11-08.md`), past
the per-source flood floor (`connlimit.Wrap`): once an E2EE share's agent's
owner is known, a connection counts against that account in total and
against (account, source address): open connections and a token bucket of
new ones, under the limits of the plan revision the replica holds. The plan
is one value in the accounts store (relay-server's global settings); a
change is a new revision that the replica applies when the accounts
journal says so (relay-server `spec/Journal.tla` bounds when). Counts are
per replica, in memory; a replica is modelled alone, as two replicas are
two copies of this model sharing only the plan.

| action | Go |
|---|---|
| `ChangePlan(p)` | the accounts service saves the global settings' `conn_limits` (relay-server `UpsertGlobalSettings`, `GlobalSettingsChanged`) |
| `Apply` | relay-server `reloadGlobalSettings` -> `Accounts.SetPlan` |
| `Refill(a)`, `RefillSource(k)` | time passing in a `rate.Limiter` (`platform/ratelimit`) |
| `Admit(a, s)` | `Accounts.Admit` admitting (`TCPDispatcher.AdmitVisitor` from `frontend/sni` `routeE2EE`); `Limiter.Ready` gives a bucket the held plan's rate and burst first |
| `Refuse(a, s)` | `Accounts.Admit` refusing: `refuseTLS` (a TLS alert), `VisitorRefused` |
| `Close(a, s)` | the release `Admit` returned (`sync.OnceFunc`), deferred in `routeE2EE` |

Invariants (`AccountLimit.cfg`, `make spec`: 2 accounts behind 1 source,
limits 1..2, 2 revisions: 1,304,504 states (175,146 distinct), 6 s;
`COVERAGE=1` takes every action; `Refuse` only stutters in the correct
model): `LimitHolds` (every admission is within the held revision's
limits: open per account, open per account and source, both buckets),
`NoAccountStarvesAnother` (a refusal is explained by the refused account's
own counts), `CountsMatch` (the limiter's counts are the live connections).
Liveness (`AccountLimitLive.cfg`, `make spec-liveness`): `PlanReaches`
(every revision is applied, under weak fairness of `Apply`). Mutants
(`make spec-mutants`), each caught: `count-by-ip-only` (the per-source key
without the account: `NoAccountStarvesAnother`), `cache-stale-plan` (a
bucket keeps the burst it was made with: `LimitHolds`), `release-twice`
(a second Close releases again: `CountsMatch`). Witnesses, each violated:
`WitnessRefused`, `WitnessLowered` (a plan lowered under more open
connections than its new limit: they stay), `WitnessShared` (both
accounts' visitors behind one address at once). Tests: `connlimit`
`TestAccountsOpenLimits`, `TestAccountsNoAccountStarvesAnother`,
`TestAccountsPlanReachesBuckets`, `TestAccountsRefusalSpendsNothing`;
`core/session` `TestAdmitVisitorCountsTheAgentsAccount`. No trace recorder
yet (each `Admit` and release is one step under one mutex of one process;
open item of the lane).

Environment assumptions:

| assumption | source |
|---|---|
| a `rate.Limiter` whose burst is lowered caps its tokens at the new burst at its next read | golang.org/x/time/rate `advance` (`tokens > burst`) |
| `sync.OnceFunc` runs its function once however often called | Go `sync` docs |
| the global settings change reaches every replica (journal poll, bus, the service's POST) | relay-server `spec/Journal.tla` |

### Code map (drift check)

| what the spec models | file |
|---|---|
| `Admit`, `Refuse`, `Close`, `Apply` (`SetPlan`) | `platform/connlimit/account.go` |
| a bucket's limits replaced (`Ready`) | `platform/ratelimit/ratelimit.go` |
| `AdmitVisitor`, the account | `core/session/visitors.go` |
| the refusal at the share | `frontend/sni/gate.go` |

## Failover.tla: an agent and its clients with several relays

`Failover.tla` is a design (lane RELAYFAILOVER, nfltr/nfltr
`docs/decisions/relay-failover-2026-11-08.md`). The endpoint's half is
code (lanes RFRELAYID, RFDEDUPE: `Accept`, the Welcome's `relay_id`, and
its recorder, code map below); the agent's and the client's are not yet
(RFAGENT, RFCLIENT: the Go column names what those lanes will write), and
no `TraceFailover.tla` checks the recorder yet (RFTRACE). It is here because relay-core owns what the
design rests on: the directory record a connect claims (`Directory.tla`'s
guarantees: the newest claim wins, an older session's renewal yields), the
connect's admission, where an endpoint's ingress routes an open
(`dispatcherURL`, the replica link), and the share gate; the agent's and
the client's halves are github.com/nfltr/relay `client/agent` and
`client/rpcclient`, modelled here as `Move.tla` and `Handoff.tla` model
theirs.

The topology is fixed: the list `a, b1, b2`, where `a` is an independent
relay (cluster A, nothing shared) and `b1`, `b2` are two endpoints of one
relay (cluster B: replicas or regions sharing one directory). The agent
learns an endpoint's relay from its Welcome (`relay_id`). It dials one
endpoint at a time; each connect names the relays it holds links to, and
an endpoint of a named relay refuses (`ALREADY_ATTACHED`) before it claims
anything. Faults (at most `MaxFaults`): an endpoint crashes (its record
stays until `Lapse`) and restarts, or the agent's path to it breaks while
clients still reach it (`Cut`, `Heal`). A client session walks the list:
`Skip` (unreachable before sending), `Send`, `Arrive` (no route: a typed
NOT_FOUND, next entry; a delivering link: the agent runs it; a stale
registration or a dead endpoint: swallowed), `Answer`, `Timeout` (no
answer is coming: reported unknown, never resent), `Rewalk` or `HoldEnds`
after a full walk. One share, issued by cluster B (`Share = TRUE`).

| action | Go |
|---|---|
| `Dial(e)`, `DialFails(e)` | relay `client/agent` relay dialer: one connect in flight, `x-nfltr-agent-relays` naming the relay ids held |
| `Detect(e)` | the agent's link liveness (`agentwire` Liveness presets), or the relay's end of the stream |
| `Accept(e)` | `core/session` connect admission: refuse `ERROR_REASON_ALREADY_ATTACHED` when the header names this relay's id, else `ClaimSession` and register; `Welcome.relay_id` |
| `Notice(e)` | the session watch ending a stale session: liveness timeout, or renewal `ErrSuperseded` (`Directory.tla` `Renew`), `DeleteSession` of its own record |
| `Lapse(c)` | the record's TTL (`Directory.tla` `Expire`) |
| `Crash(e)`, `Restart(e)`, `Cut(e)`, `Heal(e)` | the environment |
| `Skip(s)`, `Send(s)`, `Arrive(s)`, `Answer(s)`, `Timeout(s)`, `Rewalk(s)`, `HoldEnds(s)` | relay `client/rpcclient` data-connection open walk (nfltr `transportcli` dial, mappings, `tcp-connect`), `frontend` routing (`dispatcherURL`) |
| `Visit(e)`, `Revoke` | the share gate (relay-server shares) |

Invariants (`Failover.cfg`, one session, two faults, `make spec`: 712,785
states (159,931 distinct), depth 25, 38 s at load 4, 4 min at load 80;
`FailoverShare.cfg`, the share, no sessions: 206,961 (45,028), depth 20,
20 s; `COVERAGE=1` takes every action across the two: `Failover.cfg` all
but `Visit` and `Revoke`, `FailoverShare.cfg` all but the session's): `OneLinkPerCluster` (no split
brain: at most one up link per relay), `RecordFollowsAgent` (a relay's
record names every link of the agent that delivers: no lost handover),
`RunOnce` (no open run twice), `NotFoundTrue` (a `not_found` session was
never run and every entry of its last walk refused it or was unreachable),
`OpenedRan`, `ShareOneCluster` and `RevokedNotServed` (the share served by
its issuing relay only, never after its revocation). Liveness
(`FailoverLive.cfg`, `make spec-liveness`, the same 159,931 distinct
states, 8 min at load 80): `Reattaches` (once the faults
are over the agent holds a link to every relay it can reach), `StaleEnds`
(no stale registration outlives its link), `Settles` (every session ends
opened or reported: none lost silently).

Mutants (`make spec-mutants`), each caught: `failover:parallel-connect`
(duplicate registration: connects in flight together, each naming only
the relays already up), `failover:claim-before-dedupe` (the endpoint
claims the record, then refuses), `failover:renew-overwrites` (lost
handover: the left endpoint's renewal writes the record back),
`failover:stale-alt` (lost handover: an endpoint once refused as an
alternate is never dialled again, caught by `Reattaches`),
`failover:share-everywhere` (two relays serving one share),
`failover:retry-ambiguous` (an unanswered open sent to the next relay:
run twice), `failover:first-only` (reported `not_found` after the first
relay's refusal); 7 mutants and 8 witnesses: about 8 min at load 80
(`MUTANTS=failover`). Witnesses, each violated: `WitnessRefused`,
`WitnessHandover` (the agent on b2 while b1 still holds its stale
registration), `WitnessReturn` (back on b1 after it once refused as an
alternate), `WitnessRevoked`, `WitnessWalk` (opened after the first entry
refused), `WitnessAmbiguous` (reported unknown after the agent ran it),
`WitnessStale` (an open swallowed by a live endpoint's stale
registration), `WitnessNotFound`.

What modelling it settled (each now a rule of the design doc): the
endpoint cannot refuse a second session it sees in its own directory (it
cannot tell a stale one from a live one, and failover needs the newest
claim to win), so the agent names its relays; the connect check must come
before the claim; the agent must not dial in parallel while an endpoint's
relay is unknown; alternates are skipped only while their relay's link is
up; either side's liveness may pass first, and the relay's end of a
session must reach the agent (a link the relay ended delivers nothing,
`Detect` on `~reg`), else after a healed partition the agent holds a link
the relay no longer routes to (the first check's counterexample).

Environment assumptions:

| assumption | source |
|---|---|
| a cluster's directory: the newest claim names the record, an older session's renewal yields, a record goes with its own session's end or its TTL | `Directory.tla` (here) |
| an endpoint's id and certificate are authenticated by the entry's key and pin, so a relay cannot claim another's id | `nfltr/nfltr docs/guides/self-hosted-relay-identity.md` |
| an open is answered at most once, by the agent or by the hop that refused it | nfltr `docs/guides/connect-sidecar.md` ("The open is answered before any byte"), link chaining |
| a client reaches every live endpoint (client-side partitions are `Skip`'s unreachable) | modelling choice |

`Accept` assumes every endpoint of a cluster reports one id: the
directory's (`directory.RelayID`, one key written once with SET NX). A
store that loses its records (`Directory.tla` `StoreLoss`, not modelled
here) loses the id too, and a replica that starts after that reads a new
one: until the old ones restart, the cluster has two ids and an agent may
hold a link through each. A move's reconnect (`Move.tla`) is a connect of
the agent's link to this cluster: the agent must not name the cluster it
moves within (RFAGENT), or the new endpoint refuses it.

### Code map (drift check)

| what the spec models | file |
|---|---|
| `Accept`: the refusal before any claim (`refuseAttached`), called first by every connect | `core/session/connect_attached.go`, `dispatcher_subscribe.go`, `dispatcher_grpc.go`, `dispatcher_tcp.go` |
| `Accept`'s `known'`: the Welcome's `relay_id` | `core/directory/relay_id.go`, `grpcfactory/client_streams.go` |
| the recorder of `Accept` (`accept`, `refuse`; for RFTRACE) | `core/session/trace_failover_on.go` |
| `Dial`, `DialFails`, `Detect` | `relay:client/agent` (RFAGENT) |

## Layer contract: what the layer above may assume

relay-server (its `spec/Replicas.tla`), the relay's clients and the agent
SDK may assume these of relay-core, and only these; each is checked by
the named invariant or property (`make spec`, `spec-liveness`), its
mutants (`make spec-mutants`) and, where it says so, traces of the real
code (`make spec-traces`). relay-core assumes of link only its
`spec/README.md` layer contract (ordered, credited sessions; typed ends;
GOAWAY drain), not its internals.

| guarantee | invariant or property | traced |
|---|---|---|
| A TCP session's bytes arrive in order, once each, while its stream lives; a sender never exceeds its peer's window | `AgentLink` `Conservation`, `CreditBounds`, `NoProtocolError` | yes (`Trace.tla`) |
| One stalled reader never blocks another session on its stream | `NoHOL`, `BufferBound`; `Isolation` (live) | yes |
| An EOF means every byte the peer wrote before its close arrived; any other end is typed (reset, lost) | `TypedEnd`, `NoSilentLoss` (with `EndAck`) | yes |
| An abort on either side reaches the other side as a reset, never as an EOF | `AbortTyped` | yes (`TestTCPTunnelClientResetResetsTargetEndToEnd`) |
| Once a stream ended, no session stays open on it, and after a break every session ends at both sides | `DownEndsAll`; `EndsAfterBreak` (live) | yes |
| A session is placed only on a live primary's frame streams or on a link connection bound to an up primary's token; a placement that fails falls back or is refused, never hangs | `Handoff` `PlacedOnLive`, `NoOrphanLink`; `Placed` (live) | yes (`TraceHandoff.tla`) |
| `carriesSessions` is exactly "a session of the agent is open here" | `CarriesExact` | yes |
| A link's silence ends a primary only when the agent's TCP path is dead | `SilenceOnDeadPath`; `SilentEnds` (live) | yes |
| A make-before-break move loses no request except the documented drain bound; no request runs without its answer reaching the visitor; once A stops routing the record names B | `Move` `NoLoss`, `RunOnce`, `OneOwner`; `AllAnswered` (live) | yes (`TraceMove.tla`) |
| After a move, no TCP session is placed on A's old primary | `NoTCPAfterMove` | yes |
| The replica an agent's record names never refuses (nor forwards to itself) a request while that session lives; it waits for its registration | `OwnerServes` | no (B's steps are not recorded; guards `TestDispatchWaitsForAnArrivingAgent`, `TestPeerForwardWaitsForAnArrivingAgent`) |
| A presence watcher whose snapshot is stale is owed a wake, and is woken (or the listing returns to its snapshot) | `Presence` `NoSilentStale`; `Woken` (live) | yes (`TracePresence.tla`) |
| A label update lands only on the session it came on | `OwnUpdate` | yes (the update; the fence by `TestUpdateAgentLabelsFromAReplacedConnection`) |
| A session yields only to a newer session of its own record; a record never names a session older than a claimed one (until the store loses its counters); a record goes only with its own session, its TTL or the store | `Directory` `SupersededByNewer`, `NoResurrect`, `DeleteOwn`, `NoPingPong` | by model-based test (`TestDirectoryModelWalks`) |
| With `PutSession`'s renew and `DeleteSession` as two store calls (as today), a lost or stale record is rewritten by the agent's latest session at its next renewal | `DirectoryHeal.cfg` `Heals` | no (finding 38) |

Code against model, both ways: `Directory` by replaying TLC's walks on
the store (`TestDirectoryModelWalks`); `AgentLink`, `Handoff`, `Move` and
`Presence` by trace validation of the real code's runs (a replay of
their walks would drive gRPC streams, timers and the agent from outside;
the traces check the same steps where they happen). relay-server's
`Replicas.tla` (the directory records and the dispatcher's routing across
replicas) by replaying its walks on this module's code
(`TestReplicasModelWalks`, below). `scripts/spec.sh
actions` (in `make spec`) keeps the action tables complete both ways and
fails on an event a trace module takes that no Go records.

What the layer above must provide (relay-core's environment assumptions,
the tables above): single store commands atomic and linearizable, TTL
expiry, Redis pub/sub at most once and in order per publisher with a
resync after a resubscription, one agent process per agent id, bind
tokens secret.

## Trace validation

With `-tags relaytrace` and `RELAYTRACE=<dir>`, each relay frame stream
(`tcpAgentStream`) with credit appends one JSON line per relay-side step
of `AgentLink` to `<dir>/<n>.ndjson` under one process-wide lock, at the
state change and under the lock that ordered it: `Open`, `Write`
(`Acquire`), `SendData` (`enqueue`), `CloseWrite`, `Abort`, `SendAck`,
`Read` (`Consumed`, with the grant), `Recv` (data, window, close, ack),
`Finish`, `Down`. The first line (`Up`) carries the windows and, as `a`,
whether the stream acks half closes; `scripts/spec.sh` sets `EndAck` from
it. The agent's steps are not logged: `Trace.tla` lets TLC choose
them, each data frame and window update the agent sends sized as the
relay later receives it (the stream is FIFO), so the search stays linear.
Without the tag the recorder is a constant `false` and every call is
compiled away (`TestTraceOffCostsNothing`).

The agent's steps are searched only where the relay's next logged step
needs them: before a receive (for that session only) and, with the
stream's failure, before the relay's Down; and its receive loop first
takes everything the relay sent before. Receiving earlier only gives the
agent more credit, data to read and open sessions, so this loses no
explanation of a trace and keeps TLC from searching the orders of steps
that commute. For the same reason the agent sends the frame the relay
receives next only once the relay took the one before. With both, a
172-line trace of 8 concurrent sessions checks in 6 s; without the second
it ran past 5 minutes with a growing queue.

`make spec-traces` runs the tests named in `scripts/spec.sh` (the
in-process harness: concurrent sessions, half close, close after data,
stream loss, a stuck target) and checks each stream's trace, after
checking that each `spec/testdata/reject-*.ndjson` is rejected:
`reject-data-after-fin` (data sent after the relay's half close),
`reject-eof-after-loss` (the client got the EOF of an agent half close
held for the ack after the stream ended: finding 15's race). The
recorder's `Eof` (`traceEof`: `handleLocalConn`'s FIN to the client, an
inProcConn's `io.EOF`) is logged after `Down` too, and the model takes it
only for a released half close.

The recorder logs a relay-side `Write` when `Acquire` returns and the
`SendData` when the frame is enqueued; between `Acquire` and the
recorder's line a session can be finished by its reader, and a `Write`
logged after its session's `Finish` would order the `Acquire` after the
removal it preceded. The recorder leaves such a chunk's `Write` and its
`SendData` out (`traceWrite`, `traceSent`): the chunk is dropped as after
the session's end, a stutter of the model's state (its credit is gone).
Guard: `TestTraceWriteAfterFinishIsLeftOut` (relaytrace; failed before,
lane CONFORM-relay-core). A relay `SendAck` after its own `Abort` (a
client error: the whole close is queued, then the ack goroutine's frame)
is a behaviour of the model: `Abort` keeps the ack already decided
(`apend`), and the agent takes the whole close first (FIFO), so nothing
ends cleanly; a trace with it is accepted (checked by hand, lane
CONFORM-relay-core: an earlier note here said the model rejects it).

Every trace check lists `TraceNotDone` last under `INVARIANTS`: TLC
reports the first violated invariant in listed order, and a trace is
accepted when that is `TraceNotDone`, so with it first a trace whose
last step broke a real invariant was accepted (relay finding CL3, lane
TRACEORDER). `make spec-mutants` checks
`spec/testdata/mutant-hagent-park-primary-ctx-last.ndjson` (an open on
the parked link as its last line) with `Mutant = "park-primary-ctx"`:
rejected for `LinkOpensAnswered`; with `TraceNotDone` first it was
accepted. With `Mutant = "none"` it is a behaviour (the open answered),
so it is no `reject-*` fixture.

### Handoff traces

With the same tag, a TCPDispatcher whose agent link is on records each
agent's relay-side steps of `Handoff.tla` to
`<dir>/handoff-<agent>-<n>.ndjson` (`trace_handoff_on.go`): `Register`
(the primary's number and its token's), `PrimEnd` (it left the
rotation), `Bind` and `LinkDrop` (the token's link went from no
connection to one and back), `Silent` (with its transport), `Arrive`,
`Wait`, `Open` (link: the token; grpc: the primary), `Refused`, `End`,
and `Carries` (what `carriesSessions` answered). Each is written under
`traceMu` inside the lock that orders its change (`d.Mu`, the link's
`Mu`; `traceMu` is the innermost lock). `TraceHandoff.tla` checks a
trace against the model with Handoff's four invariants on every state:
the agent's steps (`Dial`, `AgentEnd`, `Adopt`, `AdoptRefused`), a
refused or second connection's bind and the TCP path's failure are not
logged and TLC chooses them; a `LinkDrop` of a connection the agent
already closed (its park), a `Silent` logged after its connection's
unbind, and an `End` of a session the model already lost, cut or refused
are taken as they are; a step that names no action fails the trace.
`make spec-traces` runs the agent-link tests in `HANDOFF_TESTS` and checks
every Handoff trace (14 traces, 7-177 lines, each a few seconds), after
checking that each `spec/testdata/reject-handoff-*.ndjson` is rejected:
`attach-ended` (a session on an ended primary's frame streams:
`PlacedOnLive`), `keep-conns` (a link connection bound for a primary that
ended: `NoOrphanLink`), `carries-stale` (`carriesSessions` false with a
link session open: `CarriesExact`), `quic-silent` (a QUIC silence ending
the primary: `SilenceOnDeadPath`). `COVERAGE=1` lists the trace module's
actions no trace takes. The resume (`UAdopt`) is
`TestAgentLinkResumedByTheNextPrimary`'s: the real agent's primary ends on
its side while the relay's goes on (a test seam holds the relay's stream:
its path broke towards the agent only), the next primary resumes the link
by its token and the session open on it goes on; its `refused` case has
the relay notice first (finding 5: a new token, the parked link's session
ends, a new one works). `TCarries` is `TestAgentLinkCarriesSessions`'s
(a session open on the link, then closed; lane CONFORM-relay-core);
`UAdoptRefused` and `TLinkDropGone` are taken too (`COVERAGE=1`,
2026-11-04: 18 traces). Not taken, each a race no test can order without
a seam inside the step: a bind not logged (`UBind`: a second connection
of a token while its first is still bound, a redial racing the old
connection's unbind, or a refused one), a wait skipped (`USkipWait`: the
dial binds between `Arrive` and the wait), a silence logged after its
unbind (`TSilentAfterDrop`); `TUnknown` is the rejecting step (a line
that names no action) and never takes a line.

### Agent-side Handoff traces (`TraceHandoffAgent.tla`)

The other half of the same steps, recorded by the real agent: with
`-tags relaytrace` and `RELAYTRACE` set, github.com/nfltr/relay
`client/agent` (`trace_handoff_on.go`; zero cost without the tag) writes
the agent's steps of `Handoff.tla` for one agent run (its `linkPark`; a
`lazyLink` without a park is a run of one primary) to
`<dir>/hagent-<n>.ndjson`:

| line | agent code | `Handoff.tla` |
|---|---|---|
| `Register` (g, t) | the primary's header read (`runTCPTunnel`'s header goroutine, or `lazyLink.open` first): its token's number | `Register(g)`, `tok'[g] = t` |
| `Dial` (g) | `lazyLink.open` starts a dial | `Dial(g)` |
| `Bind` (t), `GiveUp` | `lazyLink.run`: the dial connected, or failed | `Bind` (bound, or refused) |
| `AgentEnd` (g) | `lazyLink.end` (parks an up link), or `close` once the primary ended without a park | `AgentEnd(g)` |
| `Adopt` (t), `AdoptRefused` (t) | `lazyLink.adopt` took the parked link, or drained it | `Adopt`, `AdoptRefused` |
| `Drain` (t) | the agent closed a link: the park's TTL, a park the next primary did not resume, `lazyLink.close` | `LinkDrop(t)` without a redial; or nothing when the relay closed it first |
| `Open` (s), `Refused` (s) | `linkSessions.accept` took a session; refused it for want of a context (finding 42) | `Place(s)`/`Wake(s)` on the link; the open refused |

Each line is written under the lock that orders its change
(`lazyLink.mu`, `linkPark.mu`; the recorder's lock innermost). The
relay's steps (`PrimEnd`, a bind of the Node's own redial, drops,
silences), the path's failure, the park of the link `AgentEnd` took off
its primary (`Park`, finding 43) and the arrival (and dial wait) of the
session the agent takes next (also before a `Dial` line: the relay asks
for a dial only for a waiting session) are not logged: TLC chooses them. `make
spec-traces` checks every agent trace of the `HANDOFF_TESTS` (the real
agent against the real relay) with Handoff's five invariants, after
checking that `spec/testdata/reject-hagent-adopt-after-end.ndjson`
(finding 44: an ended primary drained the park) and
`spec/testdata/reject-hagent-park-primary-ctx.ndjson` are rejected; the
latter is an open on a parked link refused, finding 42's agent bug (with
`Mutant = "park-primary-ctx"` the same trace is a behaviour that breaks
`LinkOpensAnswered`). First run (2026-11-04, lane HANDOFFREC): 18 traces
(6-45 lines, a few seconds each), all accepted, among them the resume
(`Adopt`, then a session on the adopted link) and the refused resume
(the parked link drained, a new dial); `COVERAGE=1`: `AAdoptRefused`,
`AGiveUp` and `ARefused` are taken by no test (`ARefused` by the reject
fixture only; `AUnknown` is the rejecting step). nfltr-relay's
`TestAgentLinkSurvivesReconnect` (relay-server `relaytest`, the
RECONNFLAKE test), run once by hand with this recorder, gave a trace of
57 lines with two sessions opened on the parked link between `AgentEnd`
and the next `Register` (finding 42's case), accepted.

The relay client's own tests (fake relays, `CLIENT_TESTS`) are in `make
spec-traces` too (2026-11-04, lane HANDOFF2): they take, every run, the
steps the real relay pair leaves to chance: an open on the parked link
and its resume (`TestTCPTunnelReturnsAfterTheLinkIsParked`, finding 42's
case), a refused adoption while the new primary dials
(`TestTCPTunnelAdoptRefusedWhileDialling`), a dial that gives up
(`TestLazyAgentLinkGivesUp`), and `TestLazyAgentLink`,
`TestAgentLinkUpOnlyWhileBound`: 8 traces (4-9 lines), all accepted.
`TestLazyAgentLink`'s traces were rejected before: its fake relay retired
the link (`dataplane.AgentLinkClose`), which no relay sends (finding 4);
the message is deleted. `COVERAGE=1`: only `ARefused` (the reject
fixture; the fixed agent never refuses an open on its link) and
`AUnknown` are taken by no trace.

### Moves (`TraceMove.tla`)

With the same tag and `RELAYTRACE`, the HTTP session of an agent that
moves (`trace_move_on.go`) writes A's steps of `Move.tla` to
`<dir>/move-<agent>-<n>.ndjson`, one file per session, each line under
the same lock held across the state change: `Pick` (`RouteHere`; `ok`:
the lease taken, refused while the session is still in `movedAway`),
`Release` (the lease's end: after its `Publish`, else `Unlease`),
`Publish` (`tracehook.Publish`: a transaction's first message onto the
consumer's channel, `ok`; or `ErrNoConsumer`), `Forward` (the first
message of a published transaction sent on the stream), `Ask`, `GiveUp`,
`StopRouting`, `Refresh` (`resumeMove`), `Done` (`MOVE_DONE` sent),
`StartDrain`, `DrainBound`, `RecvA` (`CONNECTED`, the last answer of a
forwarded transaction, the agent's `EOF` after `MOVE_DONE`). Requests are
named by transaction id; a pick names none, and TLC chooses which new
request it is (`TraceMoveLog.tla` adds one request per pick). The agent's
steps, and with `ConnectB` the record, are not logged: TLC chooses them,
only before a step that reads them (a receive, `StopRouting`, `Refresh`,
a refused pick). A session that ends outside the model (the agent leaves
before `MOVE_DONE`, a takeover, a heartbeat timeout, the relay's own stop)
ends its trace there. `make spec-traces` runs the move tests of
`agent_move_test.go`, `agent_move_drain_test.go` and
`agent_rebalance*_test.go`, checks each move trace, and rejects
`reject-move-noloss` (`MOVE_DONE` while a lease is held),
`reject-move-runonce` (a request forwarded after `MOVE_DONE`),
`reject-move-oneowner` (routing stopped before the new connection took
the record) and `reject-move-notcp` (a TCP session placed on A's primary
after it found the move). `COVERAGE=1` lists the move steps no trace took
(2026-11-04, 66 traces: every one but a `Publish` that finds no consumer,
which only a publish between the consumer's end and the session leaving
`movedAway` takes, a window no test orders; later the session is gone and
the publish is not A's step. A `PlaceTCP` that places elsewhere is not
logged: it is not A's step).
A peer replica's forward is `ArriveA` (frontend/http `peer` takes the
lease through `tracehook.RoutePeer`; TLC chooses the peer's unlogged
`PickRemote`, no more of them than forwards still to arrive). The TCP
steps go to the trace of the agent's moving HTTP session
(`tcpMovingAway` maps its TCP primary to it): `TCPSupersede` when the
primary leaves the rotation (`yield`) and `PlaceTCP` (ok) when a session
is added to it, each under `traceMu` taken before the stream's `Mu` (the
lock order of `traceFinish`), which orders the two; a session placed
elsewhere is not logged. `TestMovedAgentsTCPStreamTakesNoNewSessions`
takes both (lane W5-RCORE). `tracePublish` holds `traceMu` across the
send onto the consumer's channel, so `Publish` precedes its `Forward` in
the file; a traced test that fills a channel (64 messages) would hang
there, loudly (the test's timeout), never write a wrong trace. Decided
(lane W5-RCORE): kept, the tag is test-only and no move test queues
that many. After a
rebalancing move gave up, a drain's `stopNow` asks the agent again
(`startMove` with `mv` nil): `Ask` from `gaveUp` while draining (finding
25); no test takes it yet.

### Presence traces (`TracePresence.tla`)

With the same tag and `RELAYTRACE`, the replicas of an in-process test
deployment joined to one trace (`tracePresenceStart`,
`tracePresenceJoin`: `trace_presence_on.go`) write one agent's steps of
`Presence.tla` to `<dir>/presence-<agent>-<n>.ndjson`, one file for every
replica, each line under `traceMu` inside the lock that orders its
change (the feed's `Mu`, the stream's record lock), so one process gives
a total order and no vector clock is needed: `Connect`
(`notifyUpstreamRegistered`, its labels), `Renew` (a renewal that wrote
the record), `Update` (`ok`: its write landed, else `UpdateFail`),
`Superseded` (a renewal or update refused for a newer session: the model's
`Superseded(r)` must hold), `End` (`departed`; `ok`: the release landed,
else `EndFail`), `LateDown`, `Publish` and `Deliver` (the feed's revision
of a local or a peer's change), `Resync` (the bus's empty payload), and
the watcher's `Wake` (`Watch` answered the agent's events or reset),
`Cursor` and `Snap` (its listing: on, labels; the listing is
relay-server's `myAgents`, so the test that plays the watcher records
it). Labels are mapped by the trace module: the first `Connect`'s set is
`l0`, any other `l1`. A local change's `Publish` is logged at the feed's
revision; the outbox's send to the peers follows unlogged (the model's
`Publish` is both), so a resync on a peer between the two would reject a
correct trace (no test does that). Not logged: the bus's `Drop` (TLC takes
it only before a `Resync`) and a record's `Expire` (only before a
`LateDown`); crashes, `Lose`, the epoch's `Sweep` and HTTP-tunnel and
TCP-only sessions are not recorded. A step naming no action fails the
trace. `make spec-traces` runs `PRESENCE_TESTS`
(`TestPresenceAcrossReplicas`: connect on b, an update there, a renewal,
the bus's resync, a newer session on a superseding b's, both ends;
`TestPresenceAcrossReplicasFailedWrites`: an update and a release whose
writes fail, the renewal that publishes, the down after the TTL) and
checks each trace (2 traces, 30-33 lines, under a second each), after
checking that each `spec/testdata/reject-presence-*.ndjson` is rejected:
`partial-list` (a re-list that left out the other replica's agent:
`NoSilentStale`, findings 9 and 24), `superseded-own` (a session ended as
superseded with no newer one: `SupersededByNewer`, finding 34),
`unknown-step` (a step that is no action). `TypeOK` has no fixture: the
trace module maps every label set into `Lab` and takes the replicas from
the trace. `COVERAGE=1` lists the presence steps no trace took (none
today). Open: a multi-process trace of relay-server's `relaytest`
replicas (vector clocks merged with CommunityModules `VectorClocks`), and
a recorder for the real bus's `Drop` and the epoch's `Sweep`, are
relay-server's.

### Agent-to-agent call credit traces (relay `TraceCallCredit.tla`)

Per-call credit of agent-to-agent calls is relay's model
(`spec/CallCredit.tla`, `CallCreditBoth.tla` there; relay
`spec/README.md` "CallCredit traces"); its recorder is relay
`client/rpcclient/trace_callcredit_on.go`, and the two-agent runs that feed
it are here: `make spec-traces` runs `CALLCREDIT_TESTS`
(`TestInternalAgentRouting_.*`: the saturated call beside a healthy one,
and `StreamsPastTheWindows`, local and cross-replica through
`RelayTunnel`) and checks each `callcredit-*.ndjson` with relay's
`scripts/spec.sh cctraces`, fetched with `CallCredit.tla` and
`TraceCallCredit.tla` from github.com/nfltr/relay at the relay/client
version go.mod requires (`relayspec`; `RELAY_DIR=<checkout>` for
co-development): one check per call and direction, 8 (2026-11-05), all
accepted, 4-6 s each.

## Measurements (2026-11-02, M-series Mac, load 10-19, `taskpolicy -b`)

| config | what | states (distinct) | depth | time |
|---|---|---|---|---|
| `AgentLink.cfg` (CI) | invariants, 1 session, window 2, 2 bytes, 1 a frame, in-process relay end, `EndAck` (2026-11-02, after findings 1-2) | 2,905,774 (601,711) | | 28 s |
| `AgentLinkLarge.cfg` (`make spec-nightly`) | as the CI config was: 3 bytes, 2 a frame, not in-process | 5,774,765 (1,217,770) before `EndAck` | 39 | 81 s before `EndAck` |
| `AgentLinkWrite.cfg` (CI) | `WriteReturns`, 1 in-process session, relay window 1, outbox 1 frame, the agent may stop reading | 557,944 (141,165) | 36 | 51 s |
| `Handoff.cfg` (CI) | invariants, 2 sessions, 2 primaries, the link's own redial (finding 30) | 341,113 (81,647) | | 2-4 s |
| `Move.cfg` (CI) | invariants, 2 requests, 1 TCP session, drain, B failing (findings 31, 32, MV-5), B's arrival (finding 45), onward forwards (finding 46) | 271,505 (51,545) | 25 | 6 s at load 25 (2026-11-06; 135,307 states before finding 45) |
| `Presence.cfg` (CI) | `NoSilentStale`, `OwnUpdate`, 2 replicas, 1 watcher, 2 sessions, 1 update in flight, 1 resubscription, 1 failed write | 953,324 (159,269) | | 8 s (2026-11-04, `InFlight`; 607,118 states before) |
| `Directory.cfg` (CI) | `SupersededByNewer`, `NoResurrect`, `DeleteOwn`, `NoPingPong`, 2 pods, 2 record keys, 3 claims, a store loss, claims without a Seq | 492,852 (129,090) | | 4 s (2026-11-04; 186,958 states before `ClaimNoSeq`) |
| `DirectoryHeal.cfg` (CI) | `Heals`, the renew and delete as two store calls, no store loss | 1,126,085 (320,277) | | 7 s (2026-11-04) |
| `AgentLinkLive.cfg` | `Isolation`, 2 sessions, window 1 | 2,161,261 (531,955) | 41 | 2 min 3 s |
| `AgentLinkBreak.cfg` | `EndsAfterBreak`, stream breaks | 296,341 (74,783) | 36 | 42 s |
| `HandoffLive.cfg` | `Placed` | | | 7 s |
| `PresenceLive.cfg` | `Woken`, as `Presence.cfg` with a crash and the update in flight | 2,320,252 (402,327) | 24 | 1 min 46 s (2026-11-04) |
| `PresenceLarge.cfg` (`make spec-nightly`) | `NoSilentStale`, as `Presence.cfg` with a crash | 1,438,047 (244,286) | | 26 s (2026-11-03) |
| `make spec-mutants` | 9 mutants, each caught (on the CI config) | | 5-11 | 14 s |
| `Failover.cfg`, `FailoverShare.cfg` (CI, 2026-11-08) | invariants: one session, two faults; the share, no sessions | 712,785 (159,931); 206,961 (45,028) | 25; 20 | 38 s; 20 s (load 4) |
| `FailoverLive.cfg` (2026-11-08) | `Reattaches`, `StaleEnds`, `Settles` | 712,785 (159,931) | 25 | 8 min 20 s (load 80) |
| `make spec-traces` | 16 AgentLink, 64 Move, 12 Handoff traces, all accepted; 14 reject fixtures rejected | | | 6 min 38 s (load 15-35) |
| `make spec-traces`, agent-side Handoff (2026-11-04) | 18 agent traces accepted; 1 reject fixture rejected | | | 1-5 s a trace |
| `make spec-traces`, presence only (2026-11-03) | 2 Presence traces accepted; 3 reject fixtures rejected | | | under 10 s at load 120-140 |
| `make spec-traces`, CallCredit (2026-11-05) | 8 call directions (4 calls) accepted; relay's 2 reject fixtures rejected there | up to 813 | up to 265 | 4-6 s each (load 9) |

`COVERAGE=1 scripts/spec.sh check` takes every action of both models
except `Stall`: with credit (`Credit = TRUE`) no queue fills, so the stall
reset is never needed (that is `NoHOL`); the `no-credit` mutant takes it.

## Findings

What writing the models and checking them against the code found
(2026-11-02). Findings 1, 2 and 3 are fixed (their races are the mutants
`inproc-closewrite`, `race:clean-tail-lost`, a stream without the end
acknowledgement, and `inproc-enqueue-blocks`).

1. **An in-process conn's Write racing its CloseWrite sends data after
   the FIN** (code; TLC, `race:inproc-closewrite`). `inProcConn.Write`
   and `WriteOwned` (`core/session/dispatcher_tcp_inproc.go`) check
   `writeEnded`, then `send` -> `sendData` takes credit (waiting for it)
   and queues the frame without looking again; a `CloseWrite` from
   another goroutine in between queues the half close first. The agent
   has closed the session's queue (`closeFrame`): it charges the bytes
   and drops them, and the `Write` returned their count and no error.
   The same race link had (link `spec/README.md` finding 1). Severity:
   low: today's callers (`peerlink.Join` over `DialAgent`/`ServeLinkTCP`)
   write and half-close from one goroutine, but `net.Conn` allows the two
   at once. **Fixed** (lane AGENTFIX): `inProcConn.send` takes each
   chunk's credit, then under the session's `wmu` (which `closeWrite` and
   `Close` hold around their frame) looks again; after a close it gives
   the credit back (`Credit.Grant`) and returns the bytes sent before with
   `io.ErrClosedPipe`, as link's `Conn.sendData`. Guard:
   `TestInProcWriteRacingCloseWriteSendsNothingAfterFIN` (failed before).
2. **A session ends cleanly at one side while its tail is still on the
   way** (design; TLC, `race:clean-tail-lost`). Once both halves are
   closed a side ended the session at once: the relay's `closeWrite`
   marked it closed when the agent had half-closed first (`peerHalf`), so
   `handleLocalConn` returned and the client's connection was closed with
   a FIN, while the client's last bytes and its half close were still in
   `SendCh`, gRPC or the network; the agent's `pump` did the same to its
   target. If the stream failed then, the peer reset its end (`lost`), but
   the side that ended first had already told its app the session ended
   cleanly: its last bytes were lost with no error at that app. The window
   is one round trip plus the outbox, only after the peer half-closed, and
   only when the stream fails inside it. Severity: low-medium (silent from
   the app's point of view). **Fixed** (lane ENDACK, 2026-11-02) by an end
   acknowledgement (the model's `EndAck`):
   - Wire (github.com/nfltr/relay `proto/wire/dataplane`): a control frame
     `EndAckFrame(sid)` (session id `EndAckSID` = `_endack`, its data the
     acked session's id), sent only on a gRPC frame stream whose both
     sides announced it in `EndAckHeader` (`x-nfltr-tcp-end-ack: 1`): the
     agent in its ConnectTCP request metadata, the relay in its response
     header (`RegisterStream`, `RegisterAuxStream`). A data connection or
     the agent link never carries it: link ends its sessions itself
     (link `spec/Link.tla`).
   - Ordering: a side that takes the peer's half close of a session it
     has not closed whole (relay `Dispatch` -> `peerClosedWrite`, agent
     `closeFrame` with half) queues the ack from its own goroutine (`go
     enqueue`, the model's `SendAck`: a receive loop never waits for its
     outbox). The stream is FIFO, so the ack says every byte before the
     half close was received. Once this side's queue dropped them (its
     whole close) it sends none.
   - Who closes what: both halves closed no longer end a session. A side
     ends it cleanly (removes it; the app's connection gets the peer's
     FIN, a reader its EOF) once its own half close was acked and its
     queue drained. While its own half close waits for the ack, the peer's
     half close is held: the relay keeps `recvQ` open (the client, or an
     inProcConn's reader, gets no FIN/EOF yet), the agent keeps the
     target's queue open; the bytes before it are delivered meanwhile. A
     peer half close that came first is passed on at once (the app may
     still write): this side's own half close then waits for the ack
     before the session ends (relay `closeWrite` does not `markClosed`,
     agent `pump` waits on `halfEnd.done` before closing the target). A
     whole close needs no ack; the agent's reaches the relay's reader as
     a reset, the relay's abort reaches the agent's target as one
     (finding 15).
   - Failure: a stream that ends while a side waits for its ack ends the
     session with a reset at that app, never a clean end: relay `lose`
     counts a session whose own half close was not acked as lost (the
     client's connection is cut: `abort`, an inProcConn reader gets
     `errTCPSessionLost`); agent `closeAll` marks it lost (the target is
     reset: `SetLinger(0)`), and a `pump` waiting after its target's half
     close resets the target. Bound: one round trip plus the outbox.
   - Cross-version: without the header on both sides the code is as
     before (no ack is sent, none awaited): an older agent never announces
     it, so a new relay never waits for an ack; an older relay never
     echoes it, so a new agent never waits either.
   - Invariant: `NoSilentLoss` (holds with `EndAck`, `make spec`);
     mutants `race:clean-tail-lost` (`EndAck = FALSE`: a stream without
     the header) and `lose-unacked-clean` (`lose` without the ack check).
   - Guards: relay-core `TestTCPTunnelEndAckStreamLossResetsHalfClosedClient`
     (a visitor's connection and an in-process session: the client writes
     its tail and half-closes, the agent's half close crosses it, the
     stream ends before the agent's ack: the client's connection is reset,
     an in-process reader gets `errTCPSessionLost`, not EOF; failed
     before), `TestTCPTunnelEndAckEndsCleanOnceAcked`,
     `TestTCPTunnelEndAckOldAgentEndsAtOnce` (an agent without the header:
     the session ends at both half closes, no ack sent), and
     `make spec-traces` (the agent of the traced runs acks); relay
     `TestTCPTunnelEndAckHoldsTargetEOFUntilAcked` and
     `TestTCPTunnelEndAckStreamLossResetsTarget` (agent side, failed
     before), `TestTCPTunnelEndAckOldRelay` (no header: as before).
3. **An in-process Write waiting for the outbox never returned once its
   conn closed** (code; relay-server's leakcheck, then TLC:
   `inproc-enqueue-blocks`). Finding 1's fix enqueues each chunk under
   the session's `wmu`; `enqueue` waits for room in `SendCh` or the
   stream's end (`Done`) only. With an agent that reads nothing more
   while its stream stays up (relay-server's test agents at a test's end;
   a stuck agent), the Write waited holding `wmu`, and `Close` waited for
   `wmu` before it could mark the session closed: the Write (a link
   session's drain) never returned. Severity: medium (a goroutine and
   its session held until the stream ends; forever in the tests).
   **Fixed** (lane ENDACK): `Close` marks the session closed before it
   takes `wmu`, and an in-process chunk's enqueue also gives up once the
   session ended (`sess.closed`: Close, the stream's `CloseAll`, the
   agent's whole close), giving its credit back and returning
   `io.ErrClosedPipe`. Guard: `TestInProcWriteReturnsOnceClosed` (failed
   before), liveness `WriteReturns` (`AgentLinkWrite.cfg`).
4. **The relay never sends `dataplane.AgentLinkClose`** (review). The
   agent still acts on it (`lazyLink.control`), and the relay keeps a
   primary's link for the primary's whole life (`agent_link.go`); the
   agent-side branch is dead against this relay. Severity: info.
   Deleted (lane HANDOFF2): the constant from relay proto `dataplane`,
   the agent's branch and the `retired` flag from `lazyLink`.
5. **A resumed link needs the new primary before the relay notices the
   old one ended** (model, `Handoff.tla`). `resumeToken` resumes only a
   primary that is still up; when the relay's `unregister` runs first,
   `primaryEnded` closes the link's connections and its sessions are cut
   (typed, link's `CodeConnLost`). As designed (the agent then
   reconnects); the window is the relay's detection of the old stream's
   end (gRPC keepalive while the link is up). Severity: info.

6. **`MOVE_DONE` could overtake a request** (model, `Move.tla`
   `race:settle-timer`). `MOVE_DONE` went out from the watch goroutine
   `moveSettle` (500 ms) after the routes stopped, past requests still on
   the consumer's channel and past a request the ingress had picked A for
   but not yet published (its first body chunk is read in between: a
   slow upload). The agent closes the old stream once what it received
   is answered; a request after `MOVE_DONE` then runs on the agent and
   its visitor gets the 502 agent-gone (a retry runs it twice). Severity:
   medium. Fixed by the design above.
7. **A late `MOVE_CONNECTED` ended the old session with its requests**
   (model, `race:supersede-ends`). A rebalance gives up after
   `moveBound` (10 s); the agent's last try of its new connection may be
   accepted later (its own bound is 8 s per move, from its receipt of
   `MOVE_RECONNECT`). B's session then holds the record, A renews it
   again, finds it superseded and ends the old session
   (`errSessionReplaced`): every request in flight there failed 502.
   Severity: low. Fixed: a superseded renewal of a session asked to move
   goes on with the move (`resumeMove`). Guards (both failed before):
   `TestAgentMovesBeforeTheReplicaLetsItGo` (finding 6),
   `TestRebalanceMoveLateConnectedGoesOn` (finding 7).
8. **A label update wrote the agent's record without its session**
   (model, `Presence.tla` `unfenced-label-write`). `UpdateAgentLabels`
   wrote `SetIPWithAgentInfo`: a record with no Seq, credential, account
   or services, unfenced. From the stream of a session a newer one on
   another replica superseded (a move, a duplicate), it pointed the
   record back at the stale replica; that session's end then deleted the
   record by pod (no Seq), so every other replica listed the agent as
   gone while it was connected, and the next renewal put it back with no
   wake: watchers believed it offline until some other change. Also,
   until the next renewal, other replicas saw the agent with no account
   (scoping) and no services. Severity: high. Fixed: the update is a
   fenced renewal of the stream's session (`renewAgentRecord`), refused
   when superseded. Guard: `TestPresenceLabelUpdateKeepsTheSession`.
9. **A list whose directory read failed answered the local agents only**
   (model, `partial-list`; relay-server `myAgents`). The other replicas'
   agents were missing from a snapshot taken with a valid cursor: the
   watcher believed them offline with nothing more to wake it. Severity:
   medium. Fixed in relay-server: presence `ListAgents` answers
   Unavailable. Guard: relay-server `TestPresenceListFailsWhenTheDirectoryDoes`.
10. **A renewal could write labels an update had replaced** (model,
    `renew-race`). The renewal read the stream's labels before its
    throttle and wrote them later; an update writing and publishing in
    between was overwritten in the directory with no wake, so watchers on
    other replicas kept the stale labels. Severity: medium. Fixed: both
    write under the stream's record lock, the renewal reading the labels
    under it. Guard: `TestPresenceRenewalNeverWritesReplacedLabels`.
11. **The bus could drop a payload with its subscription up** (model,
    `drop-no-resync`; relay-server `redisbus`). go-redis drops a message,
    or even a subscribe confirmation (the resync), it could not hand to a
    reader blocked 60 s. Severity: low. Fixed in relay-server: a
    resyncing bus never blocks go-redis; a payload it cannot hand over is
    replaced by a resync. Guard: relay-server `TestBusOverflowIsAResync`.
12. **A crashed replica's agents vanish without a wake** (model,
    `no-crash-resync`, was `race:crash-expiry`). A replica that dies
    publishes no down; its records expire at their TTL, publishing
    nothing; a watcher that listed the agent kept it until some other
    change. Severity: medium (an agent that reconnects elsewhere is
    announced by its new up). Fixed in relay-server (lane PRESFIX): every
    replica that knew it (its epoch marker lived at its last look,
    whichever replica pruned it) resyncs its own feed one record TTL
    after it is gone, and once a TTL after its own first look; the resync
    is an empty payload into its bus subscription (`resyncBus`). Model:
    `Sweep`, `PresenceLarge.cfg` (nightly). Guard: relay-server
    `TestEpochResyncsPresenceAfterAReplicaIsGone`. A record whose
    renewals failed past its TTL on a live replica is finding 27's.
13. **An older session could write back a record a newer one left**
    (model, `renew-resurrects`). A session superseded by a newer one on
    another replica renews when that newer one has already ended and
    removed the record: no record, so nothing superseded it, and the
    agent reappeared in every other replica's list with no wake.
    Severity: low. Fixed: a renewal that finds no record while the
    agent's Seq counter is past its session's is superseded
    (`PutSession`). Guard: `TestRenewalAfterANewerSessionEndedIsSuperseded`.
14. **An update in flight from a replaced connection applies to its
    replacement** (review; fixed in code, lane W5-RELAY, 2026-11-03).
    `serveUpdateLabels` named the agent (`SourceAgentID`), not the
    session: a request received on a stream a newer connection of the
    agent on the same replica replaced was applied to the newer one's
    labels. Severity: low (a window of one request). Fix: the relay
    stamps each A2A frame with the calling connection's number
    (`RoutingInfo.source_session`, field 5, github.com/nfltr/relay proto;
    `Stream.serial`, random and never 0, replacing any value the agent
    sent); `rpcclient.SourceSession` reads it; `UpdateAgentLabels` takes
    it and refuses an update whose connection is not the agent's current
    one here. Version plan: relay-core and relay-server ship in one binary,
    so the field is written and read by one process; an agent never sees
    it except as an opaque number on calls it receives (older agents skip
    it). Guards: `TestUpdateAgentLabelsFromAReplacedConnection`,
    `TestDispatcher_dispatchAgentToAgent_StampsAuthenticatedSourceAgentID`,
    relay-server `TestServeUpdateLabels*`. Modelled (lane
    CONFORM-relay-core): with `InFlight` (`Presence.cfg`) an update is
    received on a stream (`UpdateRecv`, the session it came on) and
    applied later (`UpdateApply`), only to that session's stream; a
    `Connect` between them replaces the stream and the update is
    refused. Invariant `OwnUpdate` (a stream's labels change only by an
    update its own session sent); mutant `update-by-agent` (the code
    before: applied to whatever stream of the agent is here); witness
    `WitnessUpdateReplaced`. Traces keep the atomic `Update`
    (`InFlight = FALSE`: the recorder logs the applied update).
15. **A lost or aborted session reached the client as a clean end** (code,
    Linux runner flake of
    `TestTCPTunnelEndAckStreamLossResetsHalfClosedClient/endpoint`, and
    review AL-1/AL-2; lane ENDACKFIX, 2026-11-02). (a) Race: `CloseAll`'s
    `lose` closes the session's queue, then aborts the client (RST); the
    drained queue woke `handleLocalConn`'s writer, which sent the agent's
    held half close to the client as a FIN first. A client that had
    half-closed read EOF: a clean end while its tail was lost (finding 2's
    guarantee broken on that interleaving; 21 of 200 runs at
    GOMAXPROCS=2 under load). The spec was right (a held half close is
    never released by `Down`); the model did not have the hold itself
    (AL-2): it now releases a half close (`Release`) at receipt or at the
    ack, `waits` marks an app that half-closed before, and `NoSilentLoss`
    covers that EOF (mutant `no-hold`). (b) The agent's whole close
    before its half close (target read error, stall reset, refused open)
    reached the client as a FIN and an in-process reader as `io.EOF`: a
    cut stream looked whole (AL-1). Severity: medium (silent truncation).
    **Fixed** on streams with the end acknowledgement: `Dispatch` marks
    such a session `aborted` (`errTCPSessionAborted`, `codes.Aborted`),
    and `handleLocalConn`'s writer resets the client itself for every
    typed end (`endErr` not `io.EOF`), never a FIN first; the bytes before
    are delivered. `AbortTyped`, mutant `abort-as-eof`; the recorder's
    `Eof` and `spec/testdata/reject-eof-after-loss.ndjson`. Guards:
    `TestTCPTunnelEndAckStreamLossResetsHalfClosedClient` (deterministic
    now: the test's connection resets late), `TestTCPTunnelEndAckAgentAbortResetsClient`.
    (c) **Fixed** (lane W5-RELAY, 2026-11-03): the relay's whole close
    reached the agent's target as a FIN, both for a client's failure and
    for a normal `inProcConn.Close`, which the agent could not tell apart.
    Wire: `TCPData.abort` (field 11, github.com/nfltr/relay proto): a
    whole close that says the session failed; on a link leg it is link's
    reset (`SendReset`, CLOSE code 2), which `dataplane.Conn.Recv` reads
    back as `abort`. The relay sets it when the client's connection fails
    to read or write (`handleLocalConn`, `pumpToAgent`) and on a stall
    reset (`resetSession`); `inProcConn.Close` stays a close. The agent
    (`closeFrame`) resets its target after what is queued (an RST, also
    after an earlier half close), and sets it on its own aborts. Version
    plan: no negotiation; a peer that does not know the field closes with
    a FIN as before (protobuf skips an unknown field; an older link peer
    without the reset feature gets a plain close), so old and new agents
    and relays mix freely and a new pair gets the reset. Model:
    `RelayAbort`, `WholeT`, `AbortTyped` both sides, `AgentLinkAbort.cfg`
    (in `make spec`; 110,149 distinct states with `Refines`), mutant
    `relay-abort-as-eof`, witness `WitnessRelayAbort`. Guards: relay
    `TestTCPTunnelRelayAbortResetsTarget` (client/agent),
    `TestFramesRoundTripV2` (dataplane), relay-core
    `TestTCPTunnelClientResetAbortsAgentSession`.

Findings 16-22 come from an independent review of `Handoff.tla` and
`Move.tla` against the code (2026-11-02, lane RCREVIEW).

16. **`Silent` and `Adopt` were in the README, not in `Handoff.tla`**
    (spec drift; low). Silence was only a `LinkDrop`, nothing obliged its
    primary to end, and the agent's adoption of a resumed link was atomic
    with `Register`. Now `Silent(t, tr)` (liveness `SilentEnds`) and
    `Register` / `Adopt` / `AdoptRefused` apart, so a refused adopt (a dial
    of the new primary out, or the park's TTL first) is explored: it cuts
    the parked link's sessions, which the invariants allow.
17. **A QUIC link's idle timeout ended the agent's TCP primary** (code;
    low-medium; TLC, `quic-silent-ends`). `quicPeer` called
    `agentLink.silent` on QUIC's idle timeout, ending the gRPC primary and
    its frame-stream sessions (`lost`) though only the UDP path had died.
    **Fixed**: a silent QUIC connection only unbinds; once no link vouches
    for the agent, its frame streams' heartbeats judge the TCP path again
    (`sendLoop`, `WatchAgentSession`). `SilenceOnDeadPath`; guard
    `TestAgentLinkSilentEndsPrimary` (QUIC's idle timeout is not silence).
18. **`carriesSessions` missed link sessions after a resume and sessions
    on an older primary** (code; medium; TLC, `count-per-primary`,
    `carries-newest`). A link session was counted on the primary it opened
    through; a resume hands the link (and its sessions) to the next
    primary, and the old one's end took the count with it. And only the
    newest primary was looked at. A rebalance then moved an agent carrying
    TCP sessions, which its agent cuts 30 s later (the kind soak's
    `ConnLost`). **Fixed**: the link-session count belongs to the bind
    token (a resumed primary shares its predecessor's), counted by the
    primary the token is bound to; `carriesSessions` looks at every
    primary of the agent here. `CarriesExact`; guards
    `TestCarriesSessionsAfterResume`, `TestCarriesSessionsOnOlderPrimary`.
19. **A failed link open's fallback onto the frame streams was not
    modelled** (spec weaker; low). `OnLinkOr`: `node.Open` may fail and
    the session attaches; `PlacedOnLive` holds (`WitnessFallback`).
20. **Requests a peer replica forwarded skipped the route lease** (code;
    medium; TLC, `peer-no-lease`). `Ingress.Mount`'s `Peer` was the proxy
    itself: a forward whose ingress read the record before it moved
    published to A's topic with no lease and no "A still routes" check;
    after `MOVE_DONE` the agent ran it after `CloseSend` and the visitor got
    a 502 (a retry runs it twice: `RunOnce`). Finding 6 was open for
    cross-replica traffic. **Fixed**: `Ingress.peer` takes `RouteHere`;
    refused, it answers 503 `Retry-After` (`ERROR_REASON_AGENT_OFFLINE`)
    before any message is published. Guard
    `TestPeerForwardAfterMoveIsRefusedRetryable`.
21. **A drain's 10 s move bound ran past `stopRouting`** (code;
    medium-low; TLC, `drain-bound-after-stop`). `stopNow` was armed at
    `MOVE_RECONNECT` and cleared only in `settle`: a slow lease drain (a
    slow upload holds its lease until its first chunk is read) ended the
    session with requests held although the agent was connected to both
    replicas; a drain starting after a rebalancing move's `MOVE_DONE` armed
    it again. **Fixed**: once A stopped routing the bound is disarmed and a
    later `stopping` leaves it so; the relay's shutdown bounds the rest.
    `NoLoss` with the bounded let-go (`lg`); guard
    `TestDrainBoundEndsOnceRoutingStopped`.
22. **A moved agent's TCP primary stayed in rotation** (code; medium;
    TLC, `tcp-stays-in-rotation`). On a superseded renewal with
    `movingAway`, `sendLoop` went on without `yield`: `streamFor` kept
    placing new TCP sessions on a stream the agent closes after
    `waitIdle`. **Fixed**: it leaves the rotation (its open sessions run
    on, as a takeover's). `NoTCPAfterMove`; guard
    `TestConnectTCPMovingAgentKeepsOldStream`.

23. **A session on the agent link can end cleanly at the client while its
    tail is lost** (design; stating link's interface, LINKIFACE). Link's
    sessions do not guarantee `NoSilentLoss` (link `spec/README.md` "The
    session interface": `CleanEndReceived` is violated by a crash in link's
    `Link.cfg`): `peerlink.Join` passes the agent's FIN to the client and
    ends the client's connection cleanly once both directions ended, while
    the client's last bytes and FIN may still be in the link connection's
    write buffer; if that connection dies then, the agent's target is cut
    (`lost`) and the client saw a clean end. Finding 2 for the frame
    streams, which their end acknowledgement fixed, open for the agent
    link. Severity: low-medium (silent from the client's point of view;
    the window is one round trip plus the write buffer, only when the
    connection dies inside it). **Fixed in link** (lane SPECTRIM; the code
    is link's `peerlink.Join`, so the design and model are link's
    `spec/Join.tla`, README "Join's clean end"): after the session's
    CloseWrite the joint syncs (`Session.Sync`: a ping after the FIN,
    answered by the peer's Mux; no wire change), holds the agent's FIN
    from the client meanwhile, and resets the client when the connection
    dies first; the agent's target gets the same. `NoSilentLoss` there;
    mutants `no-sync` (the code before) and `no-hold`; guard link
    `TestJoinCleanEndOnlyOnceThePeerReadTheFIN`. In effect here once
    relay-core and relay's client require that link version (promote-chain).
30. **The agent's link redials a dropped connection itself** (spec
    weaker than the code; low; found by a trace,
    `TestAgentLinkUpForThePrimarysLife`). The model had a dropped link
    connection end the agent's lazy link (`DropBody`), so a `Bind` needed
    a dial the relay asked for; the agent's peerlink Node redials a
    connection of an up link on its own (`linkBackoff`), and the relay
    binds it with no request out. **Fixed in the spec**: `LinkDrop` may
    leave the lazy link dialling again (`DropAs`); every invariant,
    mutant and witness holds as before.
31. **Move.tla had no lease ending without a publish** (spec weaker
    than the code; medium for the model's liveness; found by the move
    traces). A route lease also ends when the visitor goes away before
    the first message, or when the request takes the agent link
    (`dispatch.go` releases it): the model kept `MOVE_DONE` back for it
    forever while the code sends it. **Fixed in the spec**: `Unlease`
    (the request ends `gone`, reaching no agent here); `AllAnswered`
    counts `gone` as done.
32. **Move.tla had no second ask after a rebalancing move gave up**
    (spec missing a code path; low; found by reading the trace
    recorder's gaps). A drain starting after `giveUp` arms `stopNow` with
    no move, and `startMove` asks the agent again; the model allowed
    `Ask` only from `routing`. **Fixed in the spec**: `Ask` from `gaveUp`
    while draining; every invariant, mutant and witness holds. No test
    takes the path yet.
33. **A test ended a primary for a QUIC link's silence** (test; low;
    found by a Handoff trace, `TestAgentLinkSilentEndsPrimary/quic`
    rejected: `SilenceOnDeadPath`). Production calls `agentLink.silent`
    only from link/TCP's observer, but `silent` took a transport and the
    test called it for QUIC, asserting what finding 17 removed. **Fixed**:
    `silent` is link/TCP's alone (no transport argument); the test's QUIC
    case stops at the unbind. Mutant `quic-silent-ends`, rejected trace
    `reject-handoff-quic-silent`.

The spec's `GiveUp` required no `MOVE_CONNECTED`; the code gives up on
the timer regardless (review MV-3). Dropping the guard keeps every
invariant (TLC): `Refresh` resumes the move when B's record is found.

Checked by review and holding for the feed itself: a revision only grows
within a process; a cursor of another epoch (a resync, another process)
or past the revision answers reset; one older than the kept history
answers reset with what is kept; `ListAgents` takes the cursor before the
snapshot; every local change is published after the directory write
that makes it visible (`claimAgentRecord` before `notifyUpstreamRegistered`,
`releaseAgentRecord` before `publishDeparture`); the resync is the
subscribe confirmation, which Redis sends before any message published
after it.


Checked and holding: per-session credit never goes negative or past a
window and the receive loops never wait on one session (`NoHOL`): with
credit a stalled reader cannot hold the stream's other sessions; no
data is lost, duplicated or reordered within a session, and a FIN never
overtakes data; a session the stream ended under ends `lost` (an RST)
unless the peer had closed its side; a session is never placed on a
primary that ended (`TestOpenWokenByItsStreamsEndIsRefused`'s rule) and a
relay link connection never outlives every primary holding its token.
24. **The hub's re-list answered the local agents only when the directory
    read failed** (review r2 F1; model `partial-my-agents`; relay-server
    `handleMyAgents`). Finding 9's fix covered presence `ListAgents`, but
    the nfltr hub's slot wait re-reads `/api/v1/my/agents` after
    `presenceclient.Wait`, and that answered 200 with this replica's
    agents (the inventory error dropped; the endpoint registry's error
    not even reported): the hub believed the other replicas' workers gone
    with its cursor already advanced. Severity: high. Fixed in
    relay-server: `/api/v1/my/agents` answers 503 when the directory or
    the endpoint registry does not answer. Guard: relay-server
    `TestHandleMyAgents_FailsWhenTheDirectoryDoes`.
25. **A failed PUBLISH was dropped with no resync** (review r2 F2; model
    `publish-drop`). The outbox took the change before the send and
    ignored its error; peers' subscriptions never broke, so none
    resynced: their watchers stayed stale until an unrelated change.
    Severity: high. Fixed: a failed send goes back into the outbox
    (folded under any newer change of the agent) and is sent again after
    a backoff (`requeue`). Guard: `TestPresenceOutboxRepublishesAFailedSend`.
27. **Failed directory writes changed listings with no wake** (review r2
    F5; model `UpdateFail`, `EndFail`, `Lose`; mutants `silent-rewrite`,
    `no-late-down`). A label update whose write failed was published
    while the record kept the old labels, and the renewal that later
    wrote them published nothing; a record the store lost (Redis
    restarted empty, renewals failing past the TTL) was written back by
    the next renewal with no wake; a release that failed published its
    down while the record still listed the agent everywhere until its TTL,
    whose expiry publishes nothing. Severity: medium. Fixed: a failed
    write or a lost record (relay-server's reannounce) marks the stream's
    record stale, and the renewal that lands next publishes an up; a
    failed update also asks for the reannounce (`RecordWriteFailed`); a
    failed release publishes a second down one TTL later (`departed`).
    Guards: `TestPresenceRecordRewriteIsPublished`,
    `TestPresenceFailedReleaseDownsAgainAfterTheTTL`.
26. **Liveness was not checked** (review r2 F7): `Owed` was a promise
    nothing discharged; a design whose publish or subscription never
    resumes passed. Fixed in the model: `PresenceLive.cfg` (`Woken`),
    mutants `subscriber-stops`, `outbox-stops` and `publish-drop` on it.
    Its code case (review r2 F3): at a graceful stop the outbox and the
    subscriber ended with the run's context (SIGTERM), so downs and label
    changes during the drain reached no peer and the draining replica's
    watchers stopped hearing peers (the code was `outbox-stops` and
    `subscriber-stops` for that window). Severity: medium. Fixed in
    relay-server: the bus is bound to a context that ends at the
    composition's `After`. Guard: relay-server
    `TestPresenceBusOutlivesTheDrain`.
28. **An older session's down folded a newer session's up away** (review
    r2 F6, code; not in the model: one stream per replica, review F9).
    On a same-replica reconnect the older session's `CompareAndDelete`
    can precede the newer one's `Store`, so the feed got the newer up and
    then the older down for a connected agent; the outbox folded the pair
    into a down for every peer, and a peer's nfltr hub that applies ups
    (`hubArrivals`, `awaitLaunched`) missed the arrival until its
    deadline. Severity: medium. Fixed: no down while a session of the
    agent is listed here (`publishDeparture`). Guard:
    `TestPresenceReplacedSessionPublishesNoDown`. F6, decided (lane
    W5-RCORE, owner's delegation): the feed's contract stays a wake
    (`Presence.tla`'s `NoSilentStale`); an event's contents are a hint a
    consumer may not apply as the agent's state, since an older session's
    up delayed in one replica's outbox can reach a peer after a newer
    one's from another. Recommended over carrying the session Seq in the
    event (a relay proto change every consumer would then have to order
    by) because the listing already orders sessions. Follow-up in nfltr:
    `hubArrivals` (`handlers_hub_tools.go`) treats an up as a wake and
    takes the agent's labels and E2EE peer facts from
    `/api/v1/my/agents`, as `hubInventory` does. The model gained
    same-replica replacement (F9, mutant `release-by-agent`); finding 14
    is fixed in code (the relay proto's `source_session`), its mutant open.
29. **HTTP-tunnel and TCP-only agents never moved the feed** (review r2
    F4, code: these agents are the model's `Connect` and `End`, which
    their code did not take). `myAgents` lists them (the TCP agent through
    its record on every replica), but neither published an up or a down,
    so a watcher's snapshot held a departed one, or missed an arrived
    one, with nothing to wake it. Severity: medium. Fixed: `ConnectHTTP`
    publishes an up once its stream is in `Agents` and a down in
    `DeregisterAgent`; `ConnectTCP` an up after its record and a down
    through `departed` (with finding 27's second down); no down while
    another session of the agent is listed here. Guard:
    `TestPresenceHTTPAndTCPAgentsPublish`. An HTTP agent whose release
    failed published no second down (the code skipped the model's
    `LateDown`); fixed in lane CONFORM-relay-core: `ConnectHTTP`'s failed
    release takes `lateDown` as `departed` does. Guard:
    `TestPresenceHTTPAgentFailedReleaseDownsAgainAfterTheTTL` (failed
    before).
34. **A live session was superseded by its own agent's other record**
    (code, found by relay-server's `TestAgentLinkSurvivesReconnect`
    failing 3-5 in 10 against relay-core main, lane RSFOLLOW; model
    `shared-counter`, invariant `SupersededByNewer`). An agent's
    `ConnectHTTP` or `ConnectAgentTunnel` session writes the dispatcher
    record and its `ConnectTCP` session the streamer record, but both took
    their Seq from one counter per agent. Finding 13's check (no record
    and the counter past this session's Seq: superseded) then read the
    other record's claim as a newer session: once the HTTP session's
    record lapsed (a renewal later than the TTL, the store losing it), its
    next renewal ended the agent's live session instead of writing the
    record back, and opens were refused offline until the agent
    reconnected. Severity: high on relay-core main (unreleased; v0.7.2
    has no finding 13 check). Fixed: the counter is the record's own
    (`seqKey` of the record key). Guard:
    `TestRenewalAfterTheOtherRecordsClaimWritesItBack`.
35. **A repeated half close started an ack goroutine each** (code, lane
    ENDACKRC's low, lane W5-RCORE; not in the model: its agent half-closes
    a session once, so TLA+ has no state to add). With the end
    acknowledgement the relay acks the agent's half close from a goroutine
    (a receive loop never waits for its outbox); an agent that repeated
    the frame while it kept its stream open but stopped reading left one
    goroutine per repeat waiting until the stream ended. Severity: low
    (bounded by the stream's life; an agent can only hurt its own
    stream). Fixed: only the first half close of a session is acked.
    Guard: `TestTCPTunnelEndAckRepeatedHalfCloseAcksOnce`.
36. **An older session's end could delete a newer session's record**
    (code, found by `TestDirectoryModelWalks` replaying a `Directory.tla`
    walk; model `seq-only`, invariant `DeleteOwn`). `DeleteSession`
    compared the record's pod and Seq with the ending session's; a store
    that restarted empty gives the agent's next session on the same pod
    the Seq of a session still ending there, so that end deleted the new
    session's record, and the agent was unlisted on every other replica
    until the new session's next renewal wrote it back. Severity: low (a
    store loss, the same pod and the same Seq; healed at the next
    renewal). Fixed: the start time (`Since`) must match too. Guard:
    `TestDeleteSessionAfterAStoreLossKeepsTheNewerSessionsRecord`.
37. **A session without a Seq deleted any record of its pod** (code,
    lane CONFORM-relay-core, the conformance pass's `DeleteIPs` gap; model
    `ClaimNoSeq`, mutants `seqless-by-pod` (`DeleteOwn`) and
    `seqless-masks-counter` (`NoResurrect`)). ConnectTCP goes on after a
    failed claim; when the counter step failed its session has no Seq.
    `DeleteSession` sent such a session to `DeleteIPs`, which deletes the
    record whatever session of this pod it names, so its end could unlist
    a newer session of the agent here; and once that session's renewal
    wrote a record without a Seq, an older session's renewal no longer saw
    the counter past it and wrote itself back (finding 13 again). Severity:
    low (a store unreachable at the claim, then the agent reconnecting to
    the same pod). Fixed: a session's end deletes only its own record (Seq
    and start time; a record with neither, the relay's own agent's, by its
    pod), and a renewal under a record without a Seq checks the counter as
    with no record. Guards: `TestSeqlessSessionDeletesOnlyItsOwnRecord`
    (failed before), `TestDirectoryModelWalks` (its `ClaimNoSeq` walks
    diverge from the code before).
38. **`PutSession`'s renew and `DeleteSession` decide on a GET they act on
    later** (design, re-judged by lane CONFORM-relay-core; model
    `DirectoryHeal.cfg`, `TwoStep`; `race:renew-get-set`). The two
    `ponytail:` windows were "accepted" without a model. With the calls
    apart TLC shows both: a claim landing between an end's GET and its DEL
    is deleted (`DeleteOwn`; an agent reconnecting to another pod while
    its old stream ends is the likely case), and an older session's SET
    over a newer record (`NoResurrect`, until the older one's next renewal
    yields). `Heals` holds: the agent's latest session's next renewal
    (on inbound activity, throttled to the heartbeat) writes its own
    record and its active-set entry, whatever was read or written
    meanwhile; mutant `heal:renew-yields` must break it. Decided: kept,
    bounded by one renewal interval (the agent unlisted, routes to it
    refused, no data lost, no session left open). Closing it needs a
    compare-and-set or compare-and-delete step in `directory.Cache` (a
    Redis script in relay-server's store), a contract change for the
    relay-server lane; `DirectoryHeal.cfg` then drops `TwoStep`.
39. **An older session's primary that registered last took the agent's
    new sessions on its replica** (code, low; relay-server
    `spec/README.md` finding R9, lane XREPO3). Found by
    `TestReplicasModelWalks` (core/session): relay-server
    `spec/ReplicasWalk.tla`'s walks (`spec/testdata/replicas-walks.ndjson`,
    70 walks, 1,871 steps, made by relay-server's `scripts/spec.sh walks`)
    replayed on two replicas' `TCPDispatcher`s and streamer records over
    one `MemoryCache`, each record SET and DEL held to its own step as in
    the model; after every step the record, the Seq counter, every
    connection's Seq, phase and pending write, each replica's routing
    (`agentStream`) and every session's outcome must be the model's. Two
    connections of the agent to one replica, the older one's claim slower:
    `RegisterStream` made it the stream new sessions go to, the
    connection the agent was replacing. Fixed: `enterRotation` keeps the
    primaries in session order (`directory.Session.Newer`, exported for
    it; a tie by registration, as before). The replay fails on the code
    before. Not driven by the replay: `HandleConn`/`ServeLinkTCP` I/O,
    `streamFor`'s wait, the replica link and `sendLoop`'s timers (their
    decisions come from `agentStream`, `GetIP` and `yield`, which it
    runs). The replay also found relay-server finding R8 (its spec's
    renewal missed this module's finding 13 counter read).
40. **A share visited in its last flush interval idle-expired** (code,
    low; relay-server `spec/README.md` finding S2, lane XREPO3).
    `ShareUses` reported every use at the next flush (30 s) and the
    issuer extends active codes only: a visit in the code's last 30 s
    reached it after the end. Design checked in relay-server
    `spec/Share.tla` (`urgent`, mutants `S2:no-urgent` and
    `S2:touch-ended-row`, the second the refuted design of extending an
    ended row). Fixed: `CachedGrants.UsedEnd` hands `ShareUses.UsedEnding`
    the end the grant read, and a use within `After` of it is sent at
    once (`TestShareUsesReportsAtOnceNearTheEnd`, which fails without
    it). `compose` sets it; relay-server's `shareReader` still uses `Used`
    until it requires this version.
41. **A failed share use report lost its uses** (code, low; relay-server
    `spec/README.md` finding 3, lane RSFOLLOW). `ShareUses.Flush` sent
    once and dropped the uses on an error, so an issuer outage of one
    flush let a visited share idle-expire. Design checked in relay-server
    `spec/Share.tla` (`FailReport`, `Resend`, mutant `drop-on-fail`).
    Fixed: a failed send's uses are put back (a later use's reservation
    wins) and the next flush is armed; a kept use whose reservation has
    passed is not sent again (`TestShareUsesKeepsUsesAFailedSendDid`,
    which fails without it).
42. **An open on a parked agent link was refused** (model gap, agent code,
    fixed; lane RECONNFLAKE). relay-server `relaytest`
    `TestAgentLinkSurvivesReconnect` went red on the Linux runner 2026-10-28
    and 10-29 (4 runs): (a) an open the relay placed on the QUIC link after
    the agent's primary ended was refused, application code 40, "dial tcp:
    operation was canceled": the agent's link sessions dialled their target
    in the ended primary's tunnel context (`Config.Drain`); (b) the agent's
    next primary never registered ("agent is not connected after 500ms"):
    `runWithReconnect` shadowed its client, so the next cycle's TCP tunnel
    ran on the closed connection. `Handoff.tla` reached neither as a
    violation: it took every placed link open as answered, and leaves the
    agent's reconnect to the environment. Both fixed in relay client by
    v0.19.12 (`newLinkSessions` dials in `park.ctx`:
    `TestLinkSessionsDialPastThePrimary`; `TestAgentTCPTunnelFollowsReconnect`);
    0 of 242 relay-server runner runs red on it since. The model now states
    (a): `OnLinkOr`'s answer, `LinkOpensAnswered`, mutant
    `park-primary-ctx`, witness `WitnessParkedOpen`; (b) is the environment
    assumption "an agent whose primary ended registers its next one".
43. **A fast reconnect lost its link's resume** (model gap, agent code,
    fixed; lane HANDOFF2). relay client's `lazyLink.end` ran in
    `context.AfterFunc`'s goroutine, so `runTCPTunnel` could return and the
    agent's next primary read `linkPark.parked()` for its resume header
    before the end parked the link: the next primary asked for no resume,
    the relay closed the old primary's connections, the parked link
    drained at its TTL and its sessions were cut. `Handoff.tla` took
    `AgentEnd` (end seen) and the park as one step, so it could not reach
    this; found by HANDOFFREC's action map, never by a trace. The model now
    splits them (`AgentEnd` takes the link off the primary into `pend`,
    `Park` parks it, `Register` waits for `pend` empty), states
    `ParkOffered`, mutant `end-async`, witness `WitnessEndUnparked`.
    Fixed: `runTCPTunnel` returns only after the end ran
    (`TestTCPTunnelReturnsAfterTheLinkIsParked`).
44. **A primary that ended before reading its header drained the resumed
    link** (agent code, fixed; lane HANDOFF2). `runTCPTunnel`'s header
    goroutine could take the park after its primary ended (and after the
    next primary had asked to resume it): `lazyLink.adopt` refused it for
    the ended context and drained it, so the next primary found no link to
    adopt. `Handoff.tla` forbids it (`AgentEnd` clears `hdr`: an ended
    primary adopts nothing, the park keeps the link); a trace of it is
    rejected (`spec/testdata/reject-hagent-adopt-after-end.ndjson`).
    Fixed: the goroutine leaves the park alone once the primary's end
    began, `runTCPTunnel` waits for one that already took it, and
    `adopt` puts the link back in the park when its primary ended
    (`TestTCPTunnelLeavesTheParkToTheNextPrimary`).
45. **A request at the replica an agent was moving to failed in the moment
    between its claim and its registration** (code, fixed; medium; TLC,
    `no-arrival-wait`; lane DRAINFIX, 2026-11-05, from VMSCALE1's rolling
    deploy: 1 failed visit, nfltr `docs/decisions/multi-vm-scaleout-2026-11-05.md`
    follow-up 4). The new session on B claims the agent's record
    (`claimAgentRecord`) before it registers in B's `Agents`
    (`dispatcher_subscribe.go`). A visit at B in between found no route
    (`RouteHere`), then a record naming B itself, which `GetIP` returns as
    the loopback address: `dispatcherURL` forwarded the request to
    `<loopback>:<peer link port>`, B's own replica link, and failed (the
    replica's certificate names its pod address, not the loopback one:
    "no route; try again"); a peer's forward arriving at B in the same
    moment was refused by `Ingress.peer` (503). The VM log read as a drain
    ordering gap (the draining replica's link stopped early); it is not:
    A routes until the record names B (`WaitAgentsMoved` before `Leave`).
    `Move.tla` modelled B as `ServeB` alone; it now has B's claim and
    registration apart (`RegisterB`), B's own ingress (`PickB`) and a
    request's arrival at B (`ArriveB`), `OwnerServes`, mutant
    `no-arrival-wait`, witness `WitnessArriving`. Fixed: `RouteArriving`
    (the TCP path's `streamFor` already waited for an arriving stream):
    when the record names this replica, the request waits up to
    `agentStreamWait` for the session's registration, woken by it, then
    is served, or answered as an offline agent (503 `Retry-After`); it is
    never forwarded to this replica itself. Guards
    `TestDispatchWaitsForAnArrivingAgent`,
    `TestPeerForwardWaitsForAnArrivingAgent`.
    The agent-to-agent paths had the same gap on the gRPC stream (its
    claim in `ConnectAgentTunnel` before `Upstreams.Store`): a call
    (`routeAgentToAgent`) dialled this replica itself over the replica
    link, a peer's `RelayTunnel` was refused NotFound (lane DRAIN2,
    2026-11-06). Same abstraction in `Move.tla` (a request at B while the
    record names B's session not yet registered: `ArriveB`, `RegisterB`
    on `Upstreams`), so no model change: `upstreamArriving` waits as
    `RouteArriving` does; a stale record of this replica is NotFound,
    never a dial to itself. Guards `TestA2ACallWaitsForAnArrivingTarget`,
    `TestA2ACallToAStaleRecordOfThisReplicaIsNotDialled`,
    `TestRelayTunnelWaitsForAnArrivingAgent`.
46. **A peer's forward that reached the replica an agent had just moved
    off was refused** (model gap, code, fixed; medium; TLC,
    `peer-refuses-moved`; lane DRAIN3, 2026-11-06, from relay-server's
    rolling-drain e2e: 1 site visit answered offline in 4,960). A visitor's
    replica read the record naming A and forwarded the request; A stopped
    routing the moment the record named B (`stopRouting`), and
    `Ingress.peer` answered the forward 503 `Retry-After` although the
    agent was connected to B. `Move.tla` let `ArriveA` refuse it ("the
    visitor sends it again"): `NoLoss` held, a visitor still saw a 503.
    Now `ArriveA` sends it on to B (`toB`, then `ArriveB`) while the record
    names B's session, `MovedForwards` (`refA = {}`), mutant
    `peer-refuses-moved` (8 states deep). Fixed: `Ingress.peer`, refused,
    reads the record once: this replica: `RouteArriving` (finding 45);
    another replica: the request goes on there, once (`peerHopHeader`
    marks it; that replica serves it or refuses it, never forwards it
    again); none: 503 as before. No agent ran it on A, so it runs at most
    once (`RunOnce`). Guard
    `TestPeerForwardAfterMoveGoesOnToTheAgentsReplica`. relay-server's site
    path had the same gap twice: its `siteRecheck` on A answered the
    offline page, and its visitor's replica read the record twice (naming
    A, then B before B's session registered) and answered offline
    (`TestSiteHostReadsTheRecordOnce`,
    `TestSiteRecheckPassesAMovedAgentsVisitOn` there).

## Agent-to-agent calls (relay spec/CallCredit.tla)

An agent-to-agent call's credit is end to end between the two rpcclient
agents (relay `spec/README.md` "CallCredit.tla", lane CALLCREDIT); the
relay is the model's `RelayIn`, `RelayOut` and `RelayGrant`: per-call
pipes that keep a call's frames in order, and the caller's window updates
(`rpcclient.WindowMessageType`) forwarded also after the call's request
EOM, since they grant the responses' credit (mutant `CC:drop-after-eom`:
the relay before, which ended a call's request direction at its EOM).
They go from the caller's receive loop into a per-call channel of
`a2aWindowCap` (4: `GrantBound`) without waiting; one past it refuses
the call. Tests: `TestInternalAgentRouting_StreamsPastTheWindows` (local
and cross-replica; both stop at 256 responses without the change),
`TestInternalAgentRouting_SaturatedTransactionDoesNotBlockHealthyTransaction`.

A property of any windowed stream, as of gRPC's: a call whose two sides
both send past the other's window without reading (a handler sending
more than 256 responses before reading the requests, a caller sending
more than 16 requests before reading the responses) waits until its
deadline; before, the receive loop cut it after `handoff.Stall`.

### Code map (drift check)

| what the spec models | file |
|---|---|
| `RelayGrant`: `handoffA2AWindow`; `RelayIn`/`RelayOut` of a local call | `core/session/dispatcher_agent_routing.go` |
| `RelayOut` and `RelayGrant` of a call to another replica | `core/session/dispatcher_agent_routing_remote.go` |
| `RelayTunnel`: an agent-to-agent call's frames forwarded until its stream ends | `core/session/dispatcher_grpc.go` |
