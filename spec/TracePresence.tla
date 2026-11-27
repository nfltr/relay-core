---------------------------- MODULE TracePresence ----------------------------
(* Trace validation of Presence.tla (spec/README.md "Presence traces"): the *)
(* steps of one agent's sessions on the replicas of an in-process test      *)
(* deployment and of a watcher, recorded by the real dispatchers and feeds  *)
(* (-tags relaytrace), must be a behaviour of Presence. A local change's    *)
(* Publish is logged at the feed's revision (the outbox's send to the peers *)
(* follows unlogged: the model's Publish is both). The bus dropping a       *)
(* subscription (Drop) is not logged: TLC takes it only before the resync   *)
(* that follows it; a record's expiry only before a LateDown. Superseded is *)
(* a renewal or update refused for a newer session: Superseded(r) must hold.*)
(* Snap carries the watcher's listing, which must be Listing(r). The trace  *)
(* is accepted when TLC reaches its end: TraceNotDone is then violated.     *)
EXTENDS TracePresenceLog, Presence, TLC

VARIABLE l   \* the next line

traceVars == <<vars, l>>

E == Log[l]

Seen(e) == IF e.on THEN [on |-> TRUE, lab |-> Lbl(e.lab)] ELSE None

Step ==
    CASE E.ev = "Connect" -> Lbl(E.lab) = "l0" /\ Connect(E.r)
      [] E.ev = "Publish" -> Publish(E.r)
      [] E.ev = "Deliver" -> Deliver(E.r)
      [] E.ev = "Renew" -> Renew(E.r)
      [] E.ev = "Superseded" -> st[E.r].on /\ Superseded(E.r) /\ UNCHANGED vars
      [] E.ev = "Update" -> Lbl(E.lab) = "l1" /\ IF E.ok THEN Update(E.r, "l1") ELSE UpdateFail(E.r, "l1")
      [] E.ev = "End" -> IF E.ok THEN End(E.r) ELSE EndFail(E.r)
      [] E.ev = "LateDown" -> LateDown(E.r)
      [] E.ev = "Resync" -> sub[E.r] = "broken" /\ Resubscribe(E.r)
      [] E.ev = "Wake" -> Wake(E.r)
      [] E.ev = "Cursor" -> Cursor(E.r)
      [] E.ev = "Snap" -> Snap(E.r) /\ view'[E.r] = Seen(E)
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

Unlogged ==
    /\ l <= Len(Log)
    /\ \/ E.ev = "Resync" /\ Drop(E.r)
       \/ E.ev = "LateDown" /\ Expire

TraceInit == Init /\ l = 1

TraceNext ==
    \/ l <= Len(Log) /\ Step /\ l' = l + 1
    \/ Unlogged /\ UNCHANGED l

\* Violated once every line was taken: the trace is a behaviour.
TraceNotDone == l <= Len(Log)
=============================================================================
