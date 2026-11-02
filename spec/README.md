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
| `why[p][s]` | `tcpSession.endErr`, agent `lost` | the reader's end: `eof`, `lost` (`errTCPSessionLost`, an RST), `reset` (`errTCPSessionStalled`) |
| `clean[p][s]` | both halves closed, queue drained | the app's connection ended cleanly (FIN both ways) |
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
6. `NoSilentLoss`: a session that ended cleanly at a side had every byte
   that side wrote, and its close, received by the peer once the stream is
   gone. It holds with `EndAck = TRUE` (finding 2: a half close is
   acknowledged by an `ack` frame, and both halves end a session cleanly
   only once its own was acknowledged); a stream without it (an older
   agent or relay) is `EndAck = FALSE`, where it fails
   (`race:clean-tail-lost`).

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
- `no-credit` runs with 3 bytes (one past the window).
- a stream without the end acknowledgement (finding 2: an older agent or
  relay): `race:clean-tail-lost` (`EndAck = FALSE`, `NoSilentLoss`).

## Handoff.tla: agent link or frame streams

### What is modelled

The relay's primary streams of one agent `1..MaxGen` (a reconnect is the
next one) with their bind tokens; the agent link's connections bound with a
token (`agentLink.Conns`, one per token suffices); the agent's lazy link
(`lazyLink`: none, dialling, up, parked between primaries: `linkPark`);
client sessions `Sess` placed by `handleLocalConn`/`agentLink.open` on the
link or on the current primary's frame streams, and their ends.

| action | Go |
|---|---|
| `Register(g)` | `RegisterStream`: `resumeToken` (a parked link's token), `bound`, `dialNow` |
| `Dial`, `Bind` | the agent's `lazyLink.open`/`run` on `AgentLinkDial`; `agentLink.verify`+`bind` |
| `Adopt` | the agent's next primary takes the parked link the relay resumed (`lazyLink.adopt`) |
| `PrimEnd(g)` | `unregister` -> `primaryEnded` (closes the token's connections unless resumed), `CloseAll`; agent `lazyLink.end` (park) |
| `Silent` | a link connection dead past its liveness timeout: `agentLink.silent` ends the primary bound to its token |
| `LinkDrop` | a link connection closes (park TTL, agent stop, network) |
| `Arrive(s)`, `Place(s)`, `Wake(s)` | `handleLocalConn` -> `agentLink.open` (link, or wait `agentLinkDialWait` for a requested dial), else `attach` |
| `End(s)` | a session's apps end it |

### Invariants

- `PlacedOnLive`: a session is placed on frame streams only of a primary
  not ended (`attach`'s `addSession` refuses an ending stream:
  `TestOpenWokenByItsStreamsEndIsRefused`), and on the link only through a
  connection bound with the token of an up primary.
- `NoOrphanLink`: a relay link connection exists only for a token an up
  primary holds.

A session's end is one of `done` (its apps), `lost` (its primary ended
under it), `cut` (its link connection closed) or `refused` (no stream);
how its bytes end is AgentLink's and link's.

Liveness: `Placed`: an arriving session is placed or refused (the dial
wait is bounded; `HandoffLive.cfg`).

Mutants: `attach-ended` (`PlacedOnLive`), `keep-conns` (`NoOrphanLink`),
`wait-forever` (`Placed`).

## Move.tla: moving an agent's HTTP session to another replica

`Move.tla` models one make-before-break move (`pb.Move`) of an agent's
ConnectHTTP session off replica A (a drain or a rebalance) to replica B,
with the requests visitors send meanwhile. Sources: `agent_move.go`,
`agent_rebalance.go`, `dispatcher_session_watch.go`
(`WatchAgentSession`), `dispatcher_subscribe.go`
(`relayRequestsToAgent`), `frontend/http` (`dispatch.go`
`dispatcherURL`, `proxy.go`), and the agent's half in
github.com/nfltr/relay `client/agent` (`move.go`, `connect.go`,
`run.go` `closeWhenAnswered`). The streams are FIFO queues; the agent
link's sessions (TCP, HTTP on link) are not modelled: a rebalancing move
skips an agent carrying TCP sessions (`moveAgent`), and the old
connection closes once they ended (`waitIdle`).

| action | Go |
|---|---|
| `Pick` | `dispatcherURL`: A while A's `Agents` holds the stream (with a route lease, `RouteHere`), else the record |
| `Publish` | `Proxy.publishRequest` of a transaction's first message: on the consumer's channel, or `ErrNoConsumer` |
| `Forward` | `relayRequestsToAgent`: the channel's next message onto the stream |
| `Ask` | `startMove`: `MOVE_RECONNECT`, the record no longer renewed |
| `GiveUp` | the rebalance's `moveBound` without `MOVE_CONNECTED` (`giveUp`) |
| `StopRouting` | `recordMoved`, `stopRouting`; also after `GiveUp` (a late `MOVE_CONNECTED`) |
| `Refresh` | the renewal after `GiveUp` finding B's record (`directory.ErrSuperseded`) |
| `Done` | `MOVE_DONE` |
| `RecvA` | `receiveFromHTTPAgent`; the agent's EOF ends the session (`failInflight`, `failQueued`) |
| `RecvAgent`, `ConnectB`, `DialFails`, `Answer`, `CloseOld` | `processStream`, `moveConnect`, a handler's answer, `closeWhenAnswered` |

Invariants (`Move.cfg`, `make spec`): `NoLoss` (the agent stays
connected, so no request fails), `RunOnce` (no request runs on the agent
without its answer reaching the visitor), `OneOwner` (once A stopped
routing the record names B). Liveness (`MoveLive.cfg`, weak fairness on
every step but the `GiveUp` timer): every request answered, and A's old
session ends once the agent connected B.

The design (`Lease = TRUE`): the ingress takes a route lease on the
stream when it picks A, atomically with A still routing, and ends it
once the transaction's first message is on the consumer's channel (or
failed); `MOVE_DONE` is sent by `relayRequestsToAgent` itself, once A
stopped routing, no lease is held and the channel is drained, so every
transaction the old stream carries precedes it (a later message is a
continuation: a body chunk, an ack, a cancel). A renewal superseded by
B's record after a rebalance gave up goes on with the move instead of
ending the session.

Mutants (`make spec-mutants`): `race:settle-timer` (`Lease = FALSE`,
finding 6), `race:supersede-ends` (finding 7), `done-ignores-leases`,
`done-overtakes` (`MOVE_DONE` sent past the channel), `close-early` (the
agent closes the old stream with requests unanswered).

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
checking that `spec/testdata/reject-data-after-fin.ndjson` (data sent
after the relay's half close) is rejected.

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
     whole close needs no ack (the reader sees a reset either way).
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
   goes on with the move.


Checked and holding: per-session credit never goes negative or past a
window and the receive loops never wait on one session (`NoHOL`): with
credit a stalled reader cannot hold the stream's other sessions; no
data is lost, duplicated or reordered within a session, and a FIN never
overtakes data; a session the stream ended under ends `lost` (an RST)
unless the peer had closed its side; a session is never placed on a
primary that ended (`TestOpenWokenByItsStreamsEndIsRefused`'s rule) and a
relay link connection never outlives every primary holding its token.
