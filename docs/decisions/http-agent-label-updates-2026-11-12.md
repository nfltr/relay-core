# A ConnectHTTP agent updates its own labels (2026-11-12)

Lane HEALTHLABEL, for service discovery's health label (nfltr
`docs/decisions/service-discovery-2026-11-12.md` sections 2-3: a service is
down while its agent advertises `nfltr.health.<service>=down`,
`agentservices.HealthLabel`).

## Gap

Only an `AgentTunnel` agent could change its labels after connect
(`nfltr.presence.v1 UpdateLabels`, an A2A call to the relay's own agent,
applied by `Dispatcher.UpdateAgentLabels` to the `*GRPCStream` in
`Upstreams` whose serial sent it). `nfltr up` connects through
`ConnectHTTP` (an `HTTPStream` in `Agents`, never in `Upstreams`) and has no
`AgentTunnel` to call from; opening one with the same agent id would take
over the agent's session. Two reads also missed an HTTP agent's labels on
another replica: the instance pick (`agentLabels`) read `Upstreams`, then
the streamer record (the agent's `ConnectTCP` record, written without
labels), and relay-server `myAgents` listed other replicas' agents from
streamer records only, so a re-listing presence watcher never saw an HTTP
agent's labels there.

## Design

- **Wire: one frame on the agent's own stream.** `HTTPProxyResponse` gains
  `LabelUpdate labels = 5` (`map<string,string> labels = 1`), sent on a
  frame without a response. It is the agent's whole update set (not a
  patch): the labels it advertises beyond its connect labels, so a resend
  is idempotent and a reconnect resends the same frame. A relay that applies
  it says so in the accept (`x-nfltr-label-updates: 1`,
  `agentwire.LabelUpdatesKey`); a relay before it reads such a frame as a
  heartbeat, and the agent does not send it there.
- **Authority: the stream, never an agent id.** The frame carries no agent
  id; the relay applies it to the stream it arrived on (that stream's
  session, under its own credential). Another key cannot reach the stream,
  so it cannot set another agent's labels.
- **Which labels.** As `UpdateLabels`: the labels the agent connected with
  stay as advertised (an update can neither change nor drop them); any other
  key may be set. No reserved prefix: connect labels are already immutable,
  and a prefix would make the two update paths differ.
- **Bounds.** At most 64 labels, keys and values non-empty and at most 256
  bytes (`agentwire.CheckLabelUpdate`, used by the agent before it sends and
  by the relay before it applies; the relay refuses an update past them,
  applying nothing, and logs it: a conforming agent never sends one; it
  does not end the stream, whose receive loop waits on the agent's next
  frame). At most 5 updates a
  second per stream, burst 10 (`golang.org/x/time/rate`): past it the
  stream's receive loop waits, so a flapping agent slows only its own
  stream; every update is one record write and one presence publish.
- **Apply: the label update's existing path.** `UpdateAgentLabels`'s body,
  shared by both stream kinds (`applyLabelUpdate`): under the stream's
  record lock, the fenced renewal of the stream's own session with the new
  labels (`directory.ErrSuperseded`: nothing written), the stream's labels,
  then the presence `labels` publish. HTTP: the dispatcher record (the one
  carrying the stream's services, `IPInventory`).
- **Ordering vs disconnect.** The receive loop drains frames after the
  session ended (`processHTTPInput` on an ended context). The stream's
  teardown marks it ended under the record lock before it releases the
  record; an update (or a renewal) that finds it ended writes and publishes
  nothing. So no update writes back the record of a session that left (no
  stale `down` or `up` after the agent left), and an update on a replaced
  stream changes nothing the newer stream owns (the newer session holds the
  record; the fenced write is refused).
- **Cross-replica.** The record write reaches every replica's reads; the
  `labels` publish reaches every replica's watchers over the presence bus.
  Reads now follow the record that carries the services:
  `agentLabels` (the instance pick and `presenceServices`) reads the HTTP
  stream here, else the `AgentTunnel` stream here, else the dispatcher
  record, else the streamer record; relay-server `myAgents` lists other
  replicas' dispatcher records before their streamer records, so a re-list
  shows a remote HTTP agent's labels and its services' health.
- **Reconnect.** A new stream starts with its connect labels; the agent
  sends its update set as the first frame after the accept. Until it lands
  the agent's services count as up (fail open: the agent's answer to an
  open still reports a dead target).

Client API (github.com/nfltr/relay `client/agent`):

```go
// SetLabels replaces the labels the agent advertises beyond its connect
// labels (WithLabels), on its current connection and every later one.
SetLabels(labels map[string]string) error
```

It returns `agentwire.CheckLabelUpdate`'s error for labels past the bounds,
`ErrLabelUpdatesUnsupported` when the relay of the current connection does
not apply updates, else nil (sent on the current stream, or kept for the
next connection while none is up).

## TLA+

`spec/Presence.tla` already models label updates (the fenced `Apply`, the
request received on a session and applied only to that session's stream:
`UpdateRecv`, `UpdateApply`). The HTTP frame is that request: received on
the stream, applied in the stream's receive loop. Extended first: a ghost
`ended` (the sessions whose stream ended by `End`) and invariant
`NoResurrect` (the record never names a session whose stream ended), with
mutant `update-after-end` (the drained frame applied after the stream
ended: the code without the ended mark) caught. ProVerif: unaffected (no
E2EE binding changes).

## Tests

relay-core `core/session/http_label_update_test.go`: an HTTP agent's update
reaches its stream, record, presence and the instance pick; connect labels
stay; an update after the stream ended writes nothing; an update past the
bounds applies nothing (the next one applies). relay
`client/agent`: `SetLabels` sends on the current stream, resends after a
reconnect, refuses past the bounds and on a relay without the header.
relay-server `relaytest` `TestReplicasHTTPAgentLabels`: an `nfltr up`-style
agent on one replica sets `nfltr.health.<svc>=down`; the other replica's
presence lists it, and a down service is not picked while an up one is;
another agent's key cannot change it (it has no stream of that agent).

## Dogfood

The DOGFOOD lane's "stop one backend" step on linux-node: `nfltr up`
(SVCCLIENT2's wiring of `SetLabels`) marks the stopped backend's service
down and `nfltr services` shows it.
