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
| `Abort(p, s)` | relay `handleLocalConn`/`pumpToAgent` error -> close frame, `markClosed`, `removeSession`; agent `closeSession` + close frame |
| `Read(p, s, k)` | a pop of the session queue (`PopBatch`) -> `Credit.Consumed` -> window frame |
| `Recv(p)` | relay `recvLoop` (`grpcCreditFrame`, then `Dispatch`); agent `serveTCPStream`'s receive loop |
| `Finish(p, s)` | relay `markClosed` + `removeSession` once both halves closed; agent `pump` after `<-written`, `closeSession` |
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
6. `NoSilentLoss` (expected to fail, see Findings): a session that ended
   cleanly at a side had every byte that side wrote, and its close,
   received by the peer once the stream is gone.

### Liveness (weak fairness on every step that is not an app's)

- `EndsAfterBreak`: after the stream broke, every session ends at both
  sides.
- `Isolation`: a session whose apps write, close and read (fairly)
  completes at both sides although the other session's readers never read
  (with credit, no stall reset needed; without credit only by `Stall`).

### Mutants (`make spec-mutants`)

- `fin-overtakes`: a close goes ahead of the data queued before it
  (`TypedEnd`).
- `grant-early`: a window update counts bytes not yet read
  (`BufferBound`).
- `no-credit`: an older agent's stream: the receive loop waits for a full
  queue (`NoHOL`).
- `lose-as-eof`: `CloseAll` ends a session the agent had not closed as a
  clean EOF (`TypedEnd`).
- races of the code as it is (Findings): `race:inproc-closewrite`
  (`NoProtocolError`), `race:clean-tail-lost` (`NoSilentLoss`).

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
  primary holds, or one whose resume is under way.
- `TypedEnd`: a session ends `done` (its apps), `lost` (its primary ended
  under it), `cut` (its link connection closed) or `refused` (no stream).

Liveness: `Placed`: an arriving session is placed or refused (the dial
wait is bounded); `SessionsEnd`: once every primary ended and the link is
gone, every session ended.

Mutants: `attach-ended` (`PlacedOnLive`), `keep-conns` (`NoOrphanLink`),
`wait-forever` (`Placed`).

## Trace validation

With `-tags relaytrace` and `RELAYTRACE=<dir>`, each relay frame stream
(`tcpAgentStream`) with credit appends one JSON line per relay-side step
of `AgentLink` to `<dir>/<n>.ndjson` under one process-wide lock, at the
state change and under the lock that ordered it: `Open`, `Write`
(`Acquire`), `SendData` (`enqueue`), `CloseWrite`, `Abort`, `Read`
(`Consumed`, with the grant), `Recv` (data, window, close), `Finish`,
`Down`. The agent's steps are not logged: `Trace.tla` lets TLC choose
them, each data frame and window update the agent sends sized as the
relay later receives it (the stream is FIFO), so the search stays linear.
Without the tag the recorder is a constant `false` and every call is
compiled away (`TestTraceOffCostsNothing`).

`make spec-traces` runs the tests named in `scripts/spec.sh` (the
in-process harness: concurrent sessions, half close, close after data,
stream loss, a stuck target) and checks each stream's trace.
