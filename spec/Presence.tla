------------------------------ MODULE Presence ------------------------------
(* The presence feed across relay replicas (core/session presence.go, the  *)
(* label update and record renewal in dispatcher.go and                    *)
(* dispatcher_inventory_refresh.go; relay-server's gateway presence.go,    *)
(* my_agents.go and the Redis bus redisbus). spec/README.md "Presence.tla".*)
(*                                                                          *)
(* The feed is a wake signal: a watcher (presenceclient.Wait) long-polls    *)
(* from a cursor; on any event or reset it takes a new cursor and re-lists *)
(* (ListAgents: the cursor first, then the snapshot). So the guarantee is   *)
(* not an event order but this: a watcher whose snapshot differs from what *)
(* a list would answer now is owed a wake. One agent; its streams on the  *)
(* replicas are its sessions, ordered by Seq (Move.tla's guarantees: the   *)
(* newest session holds the record, an older one ends; the move itself is  *)
(* not modelled). The bus is lossy: Redis drops a subscriber's messages    *)
(* only with its subscription, and the resubscription is a resync (a new   *)
(* feed epoch: every cursor of the old one answers reset).                 *)
EXTENDS Naturals

CONSTANTS R,          \* the replicas
          W,          \* the replicas with a watcher (one is enough: watchers only observe)
          MaxSeq,     \* sessions the agent opens
          MaxUpd,     \* label updates
          MaxResync,  \* resubscriptions in all
          Crash,      \* a replica may crash (its records expire, finding 12)
          MaxFail,    \* directory writes that fail (finding 27)
          Mutant      \* "none", or a bug put back (spec/README.md)

Lab == {"l0", "l1"}       \* l0: the connect labels; l1: an update
None == [on |-> FALSE, lab |-> "-"]

VARIABLES
    live,   \* replica -> up
    st,     \* replica -> the agent's stream there: [on, seq, lab]
    seqs,   \* sessions opened (the store's Seq counter)
    upds,   \* label updates sent
    rec,    \* the directory record: [on, pod, seq, lab]; seq 0: written without a session
    rd,     \* replica -> labels a renewal read, not yet written ("-": none)
    pw,     \* replica -> labels an update published, not yet written ("-": none)
    pub,    \* replica -> a local change written, its publish pending (presenceOutbox)
    due,    \* replica -> its feed moved past its watcher's cursor (a revision
            \* or a new epoch since the cursor was taken: Watch answers)
    bus,    \* replica -> peer messages in flight to it
    sub,    \* replica -> its bus subscription: "up", "broken", "signalled"
    rs,     \* resubscriptions so far
    wpc,    \* replica -> its watcher: "idle", "cursor", "snap" (re-listing)
    view,   \* replica -> the watcher's snapshot of the agent [on, lab]
    stale,  \* replica -> its stream's record write failed or the store lost
            \* it: the next renewal that lands publishes (finding 27)
    late,   \* replica -> a release failed: a down is due once the record's
            \* TTL has passed (finding 27)
    fails,  \* directory writes failed so far
    gone    \* the replicas whose feed resyncs once a crashed replica's
            \* records expired (finding 12)

vars == <<live, st, seqs, upds, rec, rd, pw, pub, due, bus, sub, rs, wpc, view, stale, late, fails, gone>>
recv == <<rd, pw, stale, late, fails, gone>>
feedv == <<due, bus, sub, rs>>
watchv == <<wpc, view>>

\* What ListAgents on r answers for the agent now: its stream here, else
\* the directory record (myAgents).
Local(r) == IF st[r].on THEN [on |-> TRUE, lab |-> st[r].lab] ELSE None
Listing(r) == IF st[r].on THEN Local(r)
              ELSE IF rec.on THEN [on |-> TRUE, lab |-> rec.lab] ELSE None

Init ==
    /\ live = [r \in R |-> TRUE]
    /\ st = [r \in R |-> [on |-> FALSE, seq |-> 0, lab |-> "-"]]
    /\ seqs = 0 /\ upds = 0
    /\ rec = [on |-> FALSE, pod |-> "-", seq |-> 0, lab |-> "-"]
    /\ rd = [r \in R |-> "-"] /\ pw = [r \in R |-> "-"]
    /\ pub = [r \in R |-> FALSE]
    /\ due = [r \in R |-> FALSE]
    /\ bus = [r \in R |-> 0] /\ sub = [r \in R |-> "up"] /\ rs = 0
    /\ wpc = [r \in W |-> "idle"]
    /\ view = [r \in W |-> None]
    /\ stale = [r \in R |-> FALSE] /\ late = [r \in R |-> FALSE] /\ fails = 0
    /\ gone = {}

(* ---------------- the feed and the bus ----------------------------------- *)

\* publish on r: a local revision, and a message to every peer subscribed
\* now (a peer whose subscription is down misses it: it resyncs later).
\* Mutant outbox-stops (with subscriber-stops, finding 26): the outbox
\* (the subscriber) ended at SIGTERM while the replica drained and served.
Publish(r) ==
    /\ pub[r] /\ live[r] /\ Mutant # "outbox-stops"
    /\ pub' = [pub EXCEPT ![r] = FALSE]
    /\ due' = [due EXCEPT ![r] = TRUE]
    /\ bus' = [q \in R |-> IF q # r /\ live[q] /\ sub[q] = "up" THEN bus[q] + 1 ELSE bus[q]]
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, sub, rs, watchv>>

\* PUBLISH fails on the publisher only (a timeout under load, its own
\* connection reset or partitioned while the peers keep theirs): no peer
\* hears it and none resubscribes. The design keeps the change pending and
\* publishes it again (presenceOutbox requeue): the failure is a stutter,
\* and Publish is enabled again (PresenceLive.cfg: Redis eventually takes
\* a publish). Mutant publish-drop (the code, review F2): the change was
\* taken from the outbox before the send and its error ignored.
PublishFail(r) ==
    /\ Mutant = "publish-drop" /\ pub[r] /\ live[r]
    /\ pub' = [pub EXCEPT ![r] = FALSE]
    /\ due' = [due EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, bus, sub, rs, watchv>>

\* a peer's change: appended under a local revision (BindBus).
Deliver(q) ==
    /\ bus[q] > 0 /\ live[q] /\ Mutant # "subscriber-stops"
    /\ bus' = [bus EXCEPT ![q] = @ - 1] /\ due' = [due EXCEPT ![q] = TRUE]
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, pub, sub, rs, watchv>>

\* Redis drops a subscriber's messages with its subscription (a slow
\* subscriber's output buffer, a restart): all in flight are lost. The
\* code: a payload go-redis could not hand to a reader blocked 60 s was
\* dropped while the subscription stayed (finding 11): mutant drop-no-resync.
Drop(q) ==
    /\ live[q] /\ rs < MaxResync /\ sub[q] = "up"
    /\ IF Mutant = "drop-no-resync"
       THEN /\ bus[q] > 0 /\ bus' = [bus EXCEPT ![q] = @ - 1] /\ UNCHANGED sub
       ELSE /\ bus' = [bus EXCEPT ![q] = 0] /\ sub' = [sub EXCEPT ![q] = "broken"]
    /\ rs' = rs + 1
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, pub, due, watchv>>

\* The resubscription: the subscribe confirmation is the resync (an empty
\* payload, redisbus Resync), in order before any later message: a new
\* epoch. Mutant resync-early: the resync signalled before the
\* subscription is back; resync-ignored: the feed ignores the signal (the
\* code before a0154c2: no new epoch, the watchers kept their views).
Resubscribe(q) ==
    /\ live[q]
    /\ \/ /\ sub[q] = "broken" /\ Mutant # "resync-early"
          /\ sub' = [sub EXCEPT ![q] = "up"]
          /\ due' = IF Mutant = "resync-ignored" THEN due ELSE [due EXCEPT ![q] = TRUE]
       \/ /\ sub[q] = "broken" /\ Mutant = "resync-early"
          /\ sub' = [sub EXCEPT ![q] = "signalled"] /\ due' = [due EXCEPT ![q] = TRUE]
       \/ /\ sub[q] = "signalled"
          /\ sub' = [sub EXCEPT ![q] = "up"] /\ UNCHANGED due
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, pub, bus, rs, watchv>>

(* ---------------- the agent's sessions ----------------------------------- *)

\* ConnectAgentTunnel on r: claimAgentRecord (the next Seq: the newest
\* session holds the record), then the stream is listed and up published.
Connect(r) ==
    /\ live[r] /\ ~st[r].on /\ seqs < MaxSeq
    /\ seqs' = seqs + 1
    /\ st' = [st EXCEPT ![r] = [on |-> TRUE, seq |-> seqs + 1, lab |-> "l0"]]
    /\ rec' = [on |-> TRUE, pod |-> r, seq |-> seqs + 1, lab |-> "l0"]
    /\ pub' = [pub EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<live, upds, recv, feedv, watchv>>

\* The stream on r ends (closed, reaped, or its renewal found a newer
\* session): releaseAgentRecord removes the record only when it names this
\* session (DeleteSession; a record without a Seq: by its pod), then down.
End(r) ==
    LET mine == rec.on /\ rec.pod = r /\ (rec.seq = 0 \/ rec.seq = st[r].seq)
    IN /\ live[r] /\ st[r].on
       /\ st' = [st EXCEPT ![r] = [on |-> FALSE, seq |-> 0, lab |-> "-"]]
       /\ rec' = IF mine THEN [on |-> FALSE, pod |-> "-", seq |-> 0, lab |-> "-"] ELSE rec
       /\ rd' = [rd EXCEPT ![r] = "-"] /\ pw' = [pw EXCEPT ![r] = "-"]
       /\ pub' = [pub EXCEPT ![r] = TRUE]
       /\ stale' = [stale EXCEPT ![r] = FALSE]
       /\ UNCHANGED <<live, seqs, upds, late, fails, feedv, watchv, gone>>

\* The release fails (finding 27): the record stays, naming an ended
\* session, until its TTL; every replica lists the agent meanwhile. The
\* down is published now, and again once the TTL has passed (LateDown).
\* Mutant no-late-down: only the first.
EndFail(r) ==
    /\ live[r] /\ st[r].on /\ fails < MaxFail
    /\ rec.on /\ rec.pod = r /\ rec.seq = st[r].seq
    /\ fails' = fails + 1
    /\ st' = [st EXCEPT ![r] = [on |-> FALSE, seq |-> 0, lab |-> "-"]]
    /\ rd' = [rd EXCEPT ![r] = "-"] /\ pw' = [pw EXCEPT ![r] = "-"]
    /\ pub' = [pub EXCEPT ![r] = TRUE]
    /\ stale' = [stale EXCEPT ![r] = FALSE]
    /\ late' = [late EXCEPT ![r] = Mutant # "no-late-down"]
    /\ UNCHANGED <<live, seqs, upds, rec, feedv, watchv, gone>>

\* A record naming no stream of a live replica (a failed release's) is
\* gone once its TTL passed.
Leftover == rec.on /\ rec.pod \in R /\ (~st[rec.pod].on \/ st[rec.pod].seq # rec.seq)

LateDown(r) ==
    /\ late[r] /\ live[r] /\ ~(Leftover /\ rec.pod = r)
    /\ late' = [late EXCEPT ![r] = FALSE] /\ pub' = [pub EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<live, st, seqs, upds, rec, rd, pw, stale, fails, feedv, watchv, gone>>

\* The store loses a live session's record (Redis restarted empty, or the
\* renewals failed past its TTL): the replica's epoch tick (5 s) sees the
\* marker gone or the failed write and its sessions rewrite their
\* records (reannounceAll), each publishing once it lands.
Lose ==
    LET p == rec.pod
    IN /\ fails < MaxFail /\ rec.on /\ p \in R /\ live[p] /\ st[p].on /\ st[p].seq = rec.seq
       /\ fails' = fails + 1
       /\ rec' = [on |-> FALSE, pod |-> "-", seq |-> 0, lab |-> "-"]
       /\ stale' = [stale EXCEPT ![p] = TRUE]
       /\ UNCHANGED <<live, st, seqs, upds, rd, pw, late, pub, feedv, watchv, gone>>

\* renewAgentRecord's check: a newer session on another pod holds the
\* record, or (the design) no record is there and the store's Seq counter
\* went past this session's: a newer session claimed it and has ended, so
\* this one must not write it back unannounced (finding 13; mutant
\* renew-resurrects).
Superseded(r) ==
    \/ rec.on /\ rec.pod # r /\ rec.seq > st[r].seq
    \/ ~rec.on /\ seqs > st[r].seq /\ Mutant # "renew-resurrects"

\* The renewal (refreshAgentInventoryOnInboundActivity): it writes the
\* stream's labels for its session, publishing nothing. The design reads
\* the labels and writes under the stream's record lock, as the label
\* update does. Mutant renew-race (the code): the read and the write are
\* two steps an update can fall between (finding 10).
\* A renewal landing after a failed write or a lost record publishes (an
\* up: the agent as now recorded). Mutant silent-rewrite: it does not.
Renew(r) ==
    /\ live[r] /\ st[r].on /\ rd[r] = "-" /\ ~Superseded(r)
    /\ IF Mutant = "renew-race"
       THEN /\ rd' = [rd EXCEPT ![r] = st[r].lab] /\ UNCHANGED <<rec, pub, stale>>
       ELSE /\ rec' = [on |-> TRUE, pod |-> r, seq |-> st[r].seq, lab |-> st[r].lab]
            /\ stale' = [stale EXCEPT ![r] = FALSE]
            /\ pub' = [pub EXCEPT ![r] = pub[r] \/ (stale[r] /\ Mutant # "silent-rewrite")]
            /\ UNCHANGED rd
    /\ UNCHANGED <<live, st, seqs, upds, pw, late, fails, feedv, watchv, gone>>

RenewWrite(r) ==
    /\ live[r] /\ st[r].on /\ rd[r] # "-"
    /\ rd' = [rd EXCEPT ![r] = "-"]
    /\ rec' = IF Superseded(r) THEN rec
              ELSE [on |-> TRUE, pod |-> r, seq |-> st[r].seq, lab |-> rd[r]]
    /\ UNCHANGED <<live, st, seqs, upds, pw, pub, stale, late, fails, feedv, watchv, gone>>

\* The renewal finding a newer session elsewhere ends the stream (End).
Supersede(r) == live[r] /\ st[r].on /\ Superseded(r) /\ End(r)

\* UpdateLabels from the agent's stream on r (UpdateAgentLabels). The
\* design: under the stream's record lock, the fenced renewal write with
\* the new labels (refused when superseded), the stream's labels, then the
\* publish. Mutant unfenced-label-write (the code): SetIPWithAgentInfo, a
\* record naming no session (finding 8). Mutant publish-before-write.
Update(r, l) ==
    /\ live[r] /\ st[r].on /\ upds < MaxUpd /\ pw[r] = "-"
    /\ upds' = upds + 1
    /\ CASE Mutant = "unfenced-label-write" ->
              /\ rec' = [on |-> TRUE, pod |-> r, seq |-> 0, lab |-> l]
              /\ st' = [st EXCEPT ![r].lab = l] /\ pub' = [pub EXCEPT ![r] = TRUE]
              /\ UNCHANGED pw
         [] Mutant = "publish-before-write" ->
              /\ ~Superseded(r)
              /\ st' = [st EXCEPT ![r].lab = l] /\ pub' = [pub EXCEPT ![r] = TRUE]
              /\ pw' = [pw EXCEPT ![r] = l] /\ UNCHANGED rec
         [] OTHER ->
              /\ ~Superseded(r)
              /\ rec' = [on |-> TRUE, pod |-> r, seq |-> st[r].seq, lab |-> l]
              /\ st' = [st EXCEPT ![r].lab = l] /\ pub' = [pub EXCEPT ![r] = TRUE]
              /\ UNCHANGED pw
    /\ UNCHANGED <<live, seqs, rd, stale, late, fails, feedv, watchv, gone>>

\* The update's write fails (finding 27): the stream's labels change and
\* are published (this replica lists them), the record keeps the old ones
\* until a renewal lands, which publishes (stale).
UpdateFail(r, l) ==
    /\ live[r] /\ st[r].on /\ upds < MaxUpd /\ pw[r] = "-" /\ fails < MaxFail /\ ~Superseded(r)
    /\ upds' = upds + 1 /\ fails' = fails + 1
    /\ st' = [st EXCEPT ![r].lab = l] /\ pub' = [pub EXCEPT ![r] = TRUE]
    /\ stale' = [stale EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<live, seqs, rec, rd, pw, late, feedv, watchv, gone>>

UpdateWrite(r) ==
    /\ live[r] /\ pw[r] # "-" /\ ~pub[r]
    /\ pw' = [pw EXCEPT ![r] = "-"]
    /\ rec' = [on |-> TRUE, pod |-> r, seq |-> st[r].seq, lab |-> pw[r]]
    /\ UNCHANGED <<live, st, seqs, upds, rd, pub, stale, late, fails, feedv, watchv, gone>>

\* A replica crashes: its streams vanish with no release and no down; its
\* records expire at their TTL (Expire), publishing nothing (finding 12).
\* The design: every other replica sees its marker expire (relay-server
\* inventory_epoch.go) and resyncs its own feed once the records it may
\* have left expired too (Sweep). Mutant no-crash-resync: none does.
CrashR(r) ==
    /\ Crash /\ live[r] /\ \A q \in R : q # r => live[q]
    /\ live' = [live EXCEPT ![r] = FALSE]
    /\ st' = [st EXCEPT ![r] = [on |-> FALSE, seq |-> 0, lab |-> "-"]]
    /\ pub' = [pub EXCEPT ![r] = FALSE] /\ rd' = [rd EXCEPT ![r] = "-"] /\ pw' = [pw EXCEPT ![r] = "-"]
    /\ stale' = [stale EXCEPT ![r] = FALSE] /\ late' = [late EXCEPT ![r] = FALSE]
    /\ gone' = IF Mutant = "no-crash-resync" THEN gone ELSE gone \cup (R \ {r})
    /\ UNCHANGED <<seqs, upds, rec, fails, feedv, watchv>>

\* A replica's resync a record TTL after it saw a crashed replica's marker
\* expire: a new feed epoch, every cursor answers reset.
Sweep(q) ==
    /\ q \in gone /\ live[q] /\ ~(rec.on /\ rec.pod \in R /\ ~live[rec.pod])
    /\ gone' = gone \ {q} /\ due' = [due EXCEPT ![q] = TRUE]
    /\ UNCHANGED <<live, st, seqs, upds, rec, rd, pw, pub, stale, late, fails, bus, sub, rs, watchv>>

Expire ==
    /\ rec.on /\ rec.pod \in R /\ (~live[rec.pod] \/ Leftover)
    /\ rec' = [on |-> FALSE, pod |-> "-", seq |-> 0, lab |-> "-"]
    /\ UNCHANGED <<live, st, seqs, upds, recv, pub, feedv, watchv>>

(* ---------------- the watcher (presenceclient.Wait + ListAgents) --------- *)

\* Watch answers events or reset: the cursor is behind, or of another epoch.
Due(r) == due[r]

Wake(r) ==
    /\ live[r] /\ wpc[r] = "idle" /\ Due(r)
    /\ wpc' = [wpc EXCEPT ![r] = "cursor"]
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, pub, feedv, view>>

\* The re-list: the cursor first, then the snapshot. A watcher re-lists
\* through presence ListAgents or through /api/v1/my/agents (the nfltr
\* hub's slot wait: presenceclient.Wait, then the inventory); both answer
\* from myAgents. A directory read that fails answers Unavailable / 503
\* (the watcher lists again). Mutants: the snapshot without the other
\* replicas' agents, partial-list on ListAgents (finding 9) and
\* partial-my-agents on /api/v1/my/agents (review F1: the hub's path).
Cursor(r) ==
    /\ live[r] /\ wpc[r] = "cursor"
    /\ due' = [due EXCEPT ![r] = FALSE]
    /\ wpc' = [wpc EXCEPT ![r] = "snap"]
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, pub, bus, sub, rs, view>>

Snap(r) ==
    /\ live[r] /\ wpc[r] = "snap"
    /\ \/ view' = [view EXCEPT ![r] = Listing(r)]
       \/ Mutant \in {"partial-list", "partial-my-agents"} /\ view' = [view EXCEPT ![r] = Local(r)]
    /\ wpc' = [wpc EXCEPT ![r] = "idle"]
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, pub, feedv>>

Next ==
    \/ \E r \in R : \/ Publish(r) \/ PublishFail(r) \/ Deliver(r)
                    \/ EndFail(r) \/ LateDown(r) \/ UpdateFail(r, "l1") \/ Sweep(r) \/ Drop(r) \/ Resubscribe(r)
                    \/ Connect(r) \/ End(r) \/ Renew(r) \/ RenewWrite(r)
                    \/ Supersede(r) \/ UpdateWrite(r) \/ CrashR(r)
                    \/ Update(r, "l1")
    \/ \E r \in W : Wake(r) \/ Cursor(r) \/ Snap(r)
    \/ Expire \/ Lose

Spec == Init /\ [][Next]_vars

\* The feed's own steps are fair (Redis eventually takes a publish and
\* resubscribes; the watcher keeps polling); the agent, the bus's drops
\* and crashes are the environment's.
Fair ==
    /\ \A r \in R : WF_vars(Publish(r)) /\ WF_vars(Deliver(r)) /\ WF_vars(Resubscribe(r))
                   /\ WF_vars(Renew(r)) /\ WF_vars(LateDown(r)) /\ WF_vars(Sweep(r))
    /\ WF_vars(Expire)
    /\ \A r \in W : WF_vars(Wake(r)) /\ WF_vars(Cursor(r)) /\ WF_vars(Snap(r))
LiveSpec == Spec /\ Fair


(* ---------------- properties --------------------------------------------- *)

TypeOK ==
    /\ \A r \in R : st[r].on => st[r].seq \in 1..MaxSeq /\ st[r].lab \in Lab
    /\ rec.on => rec.pod \in R /\ rec.lab \in Lab
    /\ \A r \in W : wpc[r] \in {"idle", "cursor", "snap"}
    /\ \A r \in R : sub[r] \in {"up", "broken", "signalled"}

\* A wake is owed to r's watcher: its cursor is behind the feed, a change
\* is still to be published or delivered, its replica's subscription is
\* down (its resync comes), or its replica is down (the watch fails, the
\* client sees the error).
Owed(r) ==
    \/ Due(r) \/ bus[r] > 0 \/ sub[r] # "up" \/ ~live[r] \/ r \in gone
    \/ \E q \in R : pub[q] \/ stale[q] \/ late[q]

\* No silent staleness: a watcher whose snapshot differs from what its
\* list would answer now is owed a wake.
Stale(r) == wpc[r] = "idle" /\ view[r] # Listing(r)

NoSilentStale == \A r \in W : Stale(r) => Owed(r)

\* Witnesses (make spec-mutants): each must be violated, so the states
\* NoSilentStale guards are reached: a watcher's snapshot stale (owed a
\* wake), and a label update made on the other replica in its snapshot.
WitnessStale == \A r \in W : ~Stale(r)
WitnessRemoteUpdate == ~\E r \in W : ~st[r].on /\ view[r].on /\ view[r].lab = "l1"

\* The wake is paid (PresenceLive.cfg): a stale watcher is woken, or the
\* listing comes back to its snapshot (or its replica is gone: its client
\* sees the error). Review F7.
Woken == \A r \in W : Stale(r) ~> (wpc[r] # "idle" \/ view[r] = Listing(r) \/ ~live[r])
=============================================================================
