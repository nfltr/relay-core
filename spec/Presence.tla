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
    view    \* replica -> the watcher's snapshot of the agent [on, lab]

vars == <<live, st, seqs, upds, rec, rd, pw, pub, due, bus, sub, rs, wpc, view>>
recv == <<rd, pw>>
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

(* ---------------- the feed and the bus ----------------------------------- *)

\* publish on r: a local revision, and a message to every peer subscribed
\* now (a peer whose subscription is down misses it: it resyncs later).
Publish(r) ==
    /\ pub[r] /\ live[r]
    /\ pub' = [pub EXCEPT ![r] = FALSE]
    /\ due' = [due EXCEPT ![r] = TRUE]
    /\ bus' = [q \in R |-> IF q # r /\ live[q] /\ sub[q] = "up" THEN bus[q] + 1 ELSE bus[q]]
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, sub, rs, watchv>>

\* a peer's change: appended under a local revision (BindBus).
Deliver(q) ==
    /\ bus[q] > 0 /\ live[q]
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
\* subscription is back.
Resubscribe(q) ==
    /\ live[q]
    /\ \/ /\ sub[q] = "broken" /\ Mutant # "resync-early"
          /\ sub' = [sub EXCEPT ![q] = "up"] /\ due' = [due EXCEPT ![q] = TRUE]
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
       /\ UNCHANGED <<live, seqs, upds, feedv, watchv>>

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
Renew(r) ==
    /\ live[r] /\ st[r].on /\ rd[r] = "-" /\ ~Superseded(r)
    /\ IF Mutant = "renew-race"
       THEN /\ rd' = [rd EXCEPT ![r] = st[r].lab] /\ UNCHANGED rec
       ELSE /\ rec' = [on |-> TRUE, pod |-> r, seq |-> st[r].seq, lab |-> st[r].lab]
            /\ UNCHANGED rd
    /\ UNCHANGED <<live, st, seqs, upds, pw, pub, feedv, watchv>>

RenewWrite(r) ==
    /\ live[r] /\ st[r].on /\ rd[r] # "-"
    /\ rd' = [rd EXCEPT ![r] = "-"]
    /\ rec' = IF Superseded(r) THEN rec
              ELSE [on |-> TRUE, pod |-> r, seq |-> st[r].seq, lab |-> rd[r]]
    /\ UNCHANGED <<live, st, seqs, upds, pw, pub, feedv, watchv>>

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
    /\ UNCHANGED <<live, seqs, rd, feedv, watchv>>

UpdateWrite(r) ==
    /\ live[r] /\ pw[r] # "-" /\ ~pub[r]
    /\ pw' = [pw EXCEPT ![r] = "-"]
    /\ rec' = [on |-> TRUE, pod |-> r, seq |-> st[r].seq, lab |-> pw[r]]
    /\ UNCHANGED <<live, st, seqs, upds, rd, pub, feedv, watchv>>

\* A replica crashes: its streams vanish with no release and no down; its
\* records expire at their TTL (Expire), publishing nothing (finding 12).
CrashR(r) ==
    /\ Crash /\ live[r] /\ \A q \in R : q # r => live[q]
    /\ live' = [live EXCEPT ![r] = FALSE]
    /\ st' = [st EXCEPT ![r] = [on |-> FALSE, seq |-> 0, lab |-> "-"]]
    /\ pub' = [pub EXCEPT ![r] = FALSE] /\ rd' = [rd EXCEPT ![r] = "-"] /\ pw' = [pw EXCEPT ![r] = "-"]
    /\ UNCHANGED <<seqs, upds, rec, feedv, watchv>>

Expire ==
    /\ rec.on /\ rec.pod \in R /\ ~live[rec.pod]
    /\ rec' = [on |-> FALSE, pod |-> "-", seq |-> 0, lab |-> "-"]
    /\ UNCHANGED <<live, st, seqs, upds, recv, pub, feedv, watchv>>

(* ---------------- the watcher (presenceclient.Wait + ListAgents) --------- *)

\* Watch answers events or reset: the cursor is behind, or of another epoch.
Due(r) == due[r]

Wake(r) ==
    /\ live[r] /\ wpc[r] = "idle" /\ Due(r)
    /\ wpc' = [wpc EXCEPT ![r] = "cursor"]
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, pub, feedv, view>>

\* ListAgents: the cursor first, then the snapshot. A directory read that
\* fails answers Unavailable (the watcher lists again); mutant partial-list
\* (the code, finding 9): the snapshot without the other replicas' agents.
Cursor(r) ==
    /\ live[r] /\ wpc[r] = "cursor"
    /\ due' = [due EXCEPT ![r] = FALSE]
    /\ wpc' = [wpc EXCEPT ![r] = "snap"]
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, pub, bus, sub, rs, view>>

Snap(r) ==
    /\ live[r] /\ wpc[r] = "snap"
    /\ \/ view' = [view EXCEPT ![r] = Listing(r)]
       \/ Mutant = "partial-list" /\ view' = [view EXCEPT ![r] = Local(r)]
    /\ wpc' = [wpc EXCEPT ![r] = "idle"]
    /\ UNCHANGED <<live, st, seqs, upds, rec, recv, pub, feedv>>

Next ==
    \/ \E r \in R : \/ Publish(r) \/ Deliver(r) \/ Drop(r) \/ Resubscribe(r)
                    \/ Connect(r) \/ End(r) \/ Renew(r) \/ RenewWrite(r)
                    \/ Supersede(r) \/ UpdateWrite(r) \/ CrashR(r)
                    \/ Update(r, "l1")
    \/ \E r \in W : Wake(r) \/ Cursor(r) \/ Snap(r)
    \/ Expire

Spec == Init /\ [][Next]_vars


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
    \/ Due(r) \/ bus[r] > 0 \/ sub[r] # "up" \/ ~live[r]
    \/ \E q \in R : pub[q]

\* No silent staleness: a watcher whose snapshot differs from what its
\* list would answer now is owed a wake.
NoSilentStale ==
    \A r \in W : wpc[r] = "idle" /\ view[r] # Listing(r) => Owed(r)
=============================================================================
