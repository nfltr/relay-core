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
`tcp_dataplane.go`, `agent_link.go`); it is not traced (a recorder there
would need a release of that module).

Out of scope, each covered elsewhere: the bytes of a session on the agent
link or on a data connection (link's own `spec/Link.tla`: a data
connection is a link v2 connection, `dataplane.Conn`); the replicas and
the directory (relay-server `spec/Replicas.tla`); revocation (relay-server
`spec/Revocation.tla`); `client/handoff`, which hands a gRPC-only loop's
frames over without per-session state (no session state machine).

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
| `Recv(p)` | relay `recvLoop` (`grpcCreditFrame`, then `Dispatch`); agent `serveTCPStream`'s receive loop |
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
7. `AbortTyped` (`EndAck`): the agent's whole close before its half close
   (an abort) reaches the relay's reader as a reset, never an EOF
   (finding 15).

Witnesses (`make spec-mutants`, each must be violated, so the invariants
above guard reachable states): `WitnessAbort` (an agent abort reaches the
relay), `WitnessHeldEof` (an app that half-closed first gets a released
EOF).

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
- `no-credit` runs with 3 bytes (one past the window).
- a stream without the end acknowledgement (finding 2: an older agent or
  relay): `race:clean-tail-lost` (`EndAck = FALSE`, `NoSilentLoss`).

## The link session interface (LinkSession.tla, lane LINKIFACE)

`LinkSession.tla` is github.com/nfltr/link's `spec/LinkSession.tla`, the
guarantees a layer above may assume of a link session (link
`spec/README.md` "The session interface"; link's `Link.tla` refines it).
The copy here must equal the link module's once the required link
version carries it (`make spec` compares them). relay-core uses link's
sessions in three places and models them only by these guarantees:

| where | which guarantees | spec |
|---|---|---|
| sessions on the agent link (`agentLink.open`, `ServeLinkTCP`, `peerlink.Join`) | a connection's end ends its sessions `lost` (`GConn`: Handoff's `cut`); an open either opens or is refused before any byte (`GSessions`); a dead link connection is detected within Interval + Timeout + 2 x Late, a live one never (link `Liveness.tla`: Handoff's `Silent`) | `Handoff.tla` (`LinkDrop`, `Silent`) |
| bytes of a session on a data connection (`dataplane.Conn`, link v2) | ordered, credited, half close, typed ends (`GWrites`, `GReads`, `GEnds`) | none here: link's |
| AgentLink's frame streams | none: a frame stream is gRPC, not a link session; it uses link's `Credit` (modelled in `AgentLink.tla`'s `cr`, `Thr`: the same algorithm as link's `Link.tla`) | `AgentLink.tla` refines the interface (below) |

`AgentLink.tla` does not run on link sessions: it builds sessions of its
own on a gRPC frame stream. `MCAgentLink.tla` maps it onto
`LinkSession` and `AgentLink.cfg` checks `Refines` (`make spec`), so a
session placed on the frame streams gives what one placed on the agent
link gives, and a layer above (`Handoff.tla`) can treat the two alike.
The mapping: `w` the bytes written before this side's close (`finAt`
once it closed: a send that held credit before a whole close queues its
frame after it, and the peer, which takes the close first, drops it),
`d` the bytes received (`rcv`), `c` those read (`rcv - q`), `rend` the
reader's end (`why`), `shut` this side's whole close; the relay's whole
close is a Close (the agent's target reads EOF: finding 15, open) and the
agent's an abort (the relay's reader reads a reset); a stream has no
GOAWAY (`ga` FALSE: its end is `Break`). `make spec-mutants` checks that
`fin-overtakes`, `grant-early`, `lose-as-eof`, `abort-as-eof` and
`inproc-closewrite` each fail the interface alone (`interface:<mutant>`,
every AgentLink invariant left out).

Frame streams give one guarantee link's interface does not: a session
that ended cleanly at a side had its tail and FIN received by the peer
(`NoSilentLoss`, the end acknowledgement of finding 2). Sessions on the
agent link do not have it (finding 23).

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
| `AgentEnd(g)` | the agent's primary's context ends: `lazyLink.end` (park) |
| `PrimEnd(g)` | `unregister` -> `primaryEnded` (closes the token's connections unless resumed), `CloseAll` |
| `LinkDrop(t)` | a link connection closes (park TTL, agent stop, network) |
| `Silent(t, tr)` | a link connection dead past its liveness: link/TCP `agentLinkObserver.ConnDown` -> `agentLink.silent` (ends the primary bound to the token); QUIC's idle timeout (`quicPeer`): `unbind` only |
| `PathDown` | the agent's TCP path dies (the frame streams' heartbeats, `WatchAgentSession`, take over once no link vouches) |
| `Arrive(s)`, `Place(s)`, `Wake(s)` | `handleLocalConn` -> `agentLink.open` (link, or wait `agentLinkDialWait` for a requested dial; `node.Open` failing falls back), else `attach` |
| `End(s)` | a session's apps end it |

### Invariants and properties (`Handoff.cfg`, `make spec`, 43k states)

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
code before). Witnesses, each violated: `WitnessLinkOpen`,
`WitnessFallback` (`PlacedOnLive`), `WitnessResumed` (`NoOrphanLink`),
`WitnessCarryOld` (`CarriesExact`: a link session outlives the primary it
opened through), `WitnessSilent` (`SilenceOnDeadPath`). `COVERAGE=1`
takes every action.

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

### Code map (drift check)

| file | what the spec models |
|---|---|
| `core/session/agent_link.go` | `resumeToken`, `verify`, `bind`, `unbind`, `silent`, `quicPeer`, `agentLinkObserver`, `open` (the link-session count), `primaryEnded`, `dialNow` |
| `core/session/dispatcher_tcp_streams.go` | `RegisterStream`, `unregister`, `leaveRotationLocked`, `carriesSessions`, `carries`, `yield`, `sendLoop`'s silent and supersede cases |
| `core/session/dispatcher_tcp.go` | `handleLocalConn`, `attach` |
| `core/session/agent_rebalance.go` | `moveAgent`'s `carriesSessions` check |
| relay `client/agent/agent_link.go`, `tcp.go` | `lazyLink` (`open`, `end`, `adopt`, `drainNow`), `linkPark`, the resume header |

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
| `PickRemote`, `ArriveA` | a peer's `dispatcherURL` reading the record (A); the forward arriving at A: `Ingress.peer` takes `RouteHere` too, refused: 503 `Retry-After`, the request reached no agent (the visitor's retry is `new` again) |
| `Publish` | `Proxy.publishRequest` of a transaction's first message (on the consumer's channel, or `ErrNoConsumer`), then the lease's release |
| `Forward` | `relayRequestsToAgent`: the channel's next message onto the stream |
| `Ask` | `startMove`: `MOVE_RECONNECT`, the record no longer renewed |
| `GiveUp` | a rebalance's `moveBound` (`giveUp`), also after `MOVE_CONNECTED` while the record check has not seen B's record; none in a drain |
| `StopRouting` | `recordMoved`, `stopRouting` |
| `Refresh` | the renewal after `GiveUp` finding B's record (`directory.ErrSuperseded`): `resumeMove` |
| `Done` | `routeLeases` idle, then `relayRequestsToAgent`'s `moveDone`: the channel drained, `MOVE_DONE` |
| `StartDrain`, `DrainBound` | `stopping`; `stopNow` (`moveBound` from `MOVE_RECONNECT`, disarmed once A stopped routing): the agent let go (`errServerRestarting`) |
| `RecvA` | `receiveFromHTTPAgent`; the agent's EOF ends the session (`failInflight`, `failQueued`) |
| `TCPSupersede`, `PlaceTCP` | `sendLoop`'s superseded renewal of the agent's TCP primary (`yield` when `movingAway`); `streamFor` placing a TCP session |
| `RecvAgent`, `ConnectB`, `DialFails`, `Answer`, `CloseOld` | `processStream`, `moveConnect`, a handler's answer, `closeWhenAnswered` |

Invariants (`Move.cfg`, `make spec`, 11k states): `NoLoss` (no request
fails, unless a drain let the agent go before its new connection took the
record: `lg`, the documented bounded loss), `RunOnce` (no request runs on
the agent without its answer reaching the visitor; the same bound),
`OneOwner` (once A stopped routing the record names B),
`NoTCPAfterMove` (no TCP session is placed on A's TCP primary once it
found the agent's record moved). Liveness (`MoveLive.cfg`, no drain, weak
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
`tcp-stays-in-rotation` (finding 22). Witnesses, each violated:
`WitnessHeld` (requests held as A stops routing), `WitnessLetGo` (the
bounded loss is reachable), `WitnessRemote` (a peer's forward in flight
after A stopped routing), `WitnessOwner`, `WitnessTCP` (a TCP session on
A after the record moved: the window before the renewal finds it, which
the agent's `waitIdle` bounds).

### Environment assumptions

| assumption | source |
|---|---|
| The in-memory broker's send returns once the message is on the consumer's channel | `broker/broker.go` contract |
| The directory fences records by Seq; the newest session wins; a read may be stale by one write | relay-server `spec/Replicas.tla` |
| A peer replica routes by the record it read; its forward can arrive any time later | `dispatcherURL`, the replica link (relay-server `core/replicas`) |
| B's session may fail after taking the record: the agent reconnects elsewhere; requests answer 503 meanwhile (not loss) | finding MV-5 (review) |
| HTTP requests on the agent link do not ride the topic: MOVE_DONE does not order them; they end with the old connection (park TTL 10 s) | `dispatch.go` (link branch releases the lease), relay `client/agent/agent_link.go` |
| A rebalancing move skips an agent the relay counts as carrying TCP sessions (`carriesSessions`, exact by `Handoff.tla` `CarriesExact`); sessions placed after that check are bounded by the agent's `waitIdle` (30 s) | `moveAgent`; relay `client/agent/move.go` |

### Code map (drift check)

| file | what the spec models |
|---|---|
| `core/session/agent_move.go` | `routeLeases`, `RouteHere`, `startMove`, `recordMoved`, `stopRouting`, `resumeMove` |
| `core/session/dispatcher_session_watch.go` | `WatchAgentSession`: `stopping`, `stopNow`, `ask`, `giveUp`, `moved`, `check`, `settle` |
| `core/session/agent_rebalance.go` | `moveAgent`, `stays` |
| `core/session/dispatcher_subscribe.go` | `relayRequestsToAgent`, `moveDone` |
| `core/session/dispatcher_tcp_streams.go` | `sendLoop`'s superseded renewal (`movingAway`, `yield`), `streamFor` |
| `frontend/http/dispatch.go`, `frontend.go`, `proxy.go` | `dispatcherURL`, `peer`, `routeLease`, the first publish |
| relay `client/agent/move.go`, `connect.go`, `run.go` | `moveConnect`, `waitIdle`, `closeWhenAnswered` |

## Presence.tla: the presence feed across replicas

`Presence.tla` models the presence feed (`presence.go`) of one agent on
two replicas sharing the directory and a Redis pub/sub bus
(relay-server `redisbus`), with the label update (`UpdateAgentLabels`),
the record renewal (`dispatcher_inventory_refresh.go`) and one watcher
(github.com/nfltr/relay `client/presenceclient` `Wait`, then relay-server
`ListAgents`: the cursor first, then the snapshot from `myAgents`). The
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

| action | Go |
|---|---|
| `Connect`, `End`, `Supersede` | `ConnectAgentTunnel`: `claimAgentRecord`, `notifyUpstreamRegistered`; its teardown `releaseAgentRecord`, `publishDeparture`; a renewal's `directory.ErrSuperseded` |
| `Renew` (`RenewWrite`) | `refreshAgentInventoryOnInboundActivity`, `renewAgentRecord`, `PutSession` renew |
| `Update` (`UpdateWrite`) | `UpdateAgentLabels` (relay-server `serveUpdateLabels`) |
| `Publish`, `Deliver` | `presenceOutbox`, `BindBus`'s subscriber: `bump` |
| `Drop`, `Resubscribe` | Redis dropping a subscriber, go-redis resubscribing; redisbus's empty payload, `presenceFeed.resync` |
| `Wake`, `Cursor`, `Snap` | `presenceFeed.Watch` answering, `ListAgents` (`Presence.Cursor`, `myAgents`) |
| `CrashR`, `Expire` | a replica gone without its teardown; its records' TTL |

The design (`Presence.cfg`, `make spec`, 17k states): a label update is a
fenced renewal of the stream's own session (refused when superseded), and
a renewal and an update write the record under the stream's record lock;
every directory write that changes a listing is published after it; a
renewal finding no record while the agent's Seq counter is past its
session's is superseded (it does not write back a record a newer session
left); a list whose directory read fails answers Unavailable, not the
local agents only; the bus never drops a payload it could not hand over
without a resync. Mutants (`make spec-mutants`), each caught:
`unfenced-label-write` (finding 8), `partial-list` (9), `renew-race`
(10), `drop-no-resync` (11), `renew-resurrects` (13),
`publish-before-write`, `resync-early` (the resync before the
subscription is back); `race:crash-expiry` (`Crash = TRUE`, finding 12,
open). `COVERAGE=1` takes every action but those only a mutant or the
race enables (`RenewWrite`, `UpdateWrite`, `CrashR`, `Expire`).

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
`SendData` when the frame is enqueued; between the two a session can be
finished by its reader, and such a trace would be rejected although the
code did what the model allows (the data is dropped as after a whole
close). No run hit it; it would show as a rejected trace naming a `Write`
after its session's `Finish`. Likewise a relay `SendAck` can follow its own
`Abort` (a client error: the whole close is queued, then `markClosed`):
the model sends no ack after a whole close; the agent takes the whole
close first (FIFO) and resets its target, so nothing ends cleanly.

## Measurements (2026-11-02, M-series Mac, load 10-19, `taskpolicy -b`)

| config | what | states (distinct) | depth | time |
|---|---|---|---|---|
| `AgentLink.cfg` (CI) | invariants, 1 session, window 2, 2 bytes, 1 a frame, in-process relay end, `EndAck` (2026-11-02, after findings 1-2) | 2,905,774 (601,711) | | 28 s |
| `AgentLinkLarge.cfg` (`make spec-nightly`) | as the CI config was: 3 bytes, 2 a frame, not in-process | 5,774,765 (1,217,770) before `EndAck` | 39 | 81 s before `EndAck` |
| `AgentLinkWrite.cfg` (CI) | `WriteReturns`, 1 in-process session, relay window 1, outbox 1 frame, the agent may stop reading | 557,944 (141,165) | 36 | 51 s |
| `Handoff.cfg` (CI) | invariants, 2 sessions, 2 primaries | 35,056 (11,526) | 20 | 4 s |
| `Presence.cfg` (CI) | `NoSilentStale`, 2 replicas, 1 watcher, 2 sessions, 1 update, 1 resubscription | 90,105 (16,971) | | 4 s CPU (12 s at load 15) |
| `AgentLinkLive.cfg` | `Isolation`, 2 sessions, window 1 | 2,161,261 (531,955) | 41 | 2 min 3 s |
| `AgentLinkBreak.cfg` | `EndsAfterBreak`, stream breaks | 296,341 (74,783) | 36 | 42 s |
| `HandoffLive.cfg` | `Placed` | | | 7 s |
| `make spec-mutants` | 9 mutants, each caught (on the CI config) | | 5-11 | 14 s |
| `make spec-traces` | 8 traces (2-172 lines), all accepted | | | 72 s |

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
     a reset (finding 15), the relay's reaches the agent's target as a
     FIN (open, finding 15).
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
    `race:crash-expiry`, open). A replica that dies publishes no down;
    its records expire at their TTL (as does a record whose renewals
    failed past it), publishing nothing; a watcher that listed the agent
    keeps it until some other change. Severity: medium (an agent that
    reconnects elsewhere is announced by its new up). Next: publish on a
    record's expiry (Redis keyspace `expired` notifications into the bus,
    or the replica link's peer loss plus a resync a TTL later).
13. **An older session could write back a record a newer one left**
    (model, `renew-resurrects`). A session superseded by a newer one on
    another replica renews when that newer one has already ended and
    removed the record: no record, so nothing superseded it, and the
    agent reappeared in every other replica's list with no wake.
    Severity: low. Fixed: a renewal that finds no record while the
    agent's Seq counter is past its session's is superseded
    (`PutSession`). Guard: `TestRenewalAfterANewerSessionEndedIsSuperseded`.
14. **An update in flight from a replaced connection applies to its
    replacement** (review, open). `serveUpdateLabels` names the agent
    (`SourceAgentID`), not the session: a request received on a stream
    a newer connection of the agent on the same replica replaced is
    applied to the newer one's labels. Severity: low (a window of one
    request). Next: the source session on the agent-to-agent call (relay
    proto and `rpcclient`, a release of github.com/nfltr/relay).
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
    **Open**: the relay's whole close reaches the agent's target as a FIN
    (agent `closeFrame`, github.com/nfltr/relay): the relay sends whole
    both for a client's error and for a normal `inProcConn.Close`, so the
    agent cannot tell them apart without a frame flag (relay proto,
    `client/agent`). The model keeps `AbortTyped` to the relay side.

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
    connection dies inside it). Next: a design here first (Handoff or a
    model of `Join` over `LinkSession`): either link's interface gains an
    acknowledged end (a session's `Done` after the peer read its FIN), or
    the relay holds the agent's FIN from the client until the agent's
    answer.

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
