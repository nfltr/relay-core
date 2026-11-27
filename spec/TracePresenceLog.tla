--------------------------- MODULE TracePresenceLog ---------------------------
(* A recorded presence trace, read once (as TraceLog.tla): its replicas,   *)
(* the watcher's, how many sessions, updates, resyncs and failed writes it *)
(* names, and its labels: the first Connect's are the connect labels       *)
(* (Presence.tla's l0), any other set is an update's (l1).                 *)
EXTENDS Json, Sequences, Naturals, FiniteSets

CONSTANT TraceFile

Log == ndJsonDeserialize(TraceFile)

Lines(evs) == {i \in 1..Len(Log) : Log[i].ev \in evs}

TraceR == {Log[i].r : i \in 1..Len(Log)}
TraceW == {Log[i].r : i \in Lines({"Wake", "Cursor", "Snap"})}
TraceMaxSeq == Cardinality(Lines({"Connect"}))
TraceMaxUpd == Cardinality(Lines({"Update"}))
TraceMaxResync == Cardinality(Lines({"Resync"}))
TraceMaxFail == Cardinality({i \in Lines({"Update", "End"}) : ~Log[i].ok})

L0 == IF Lines({"Connect"}) = {} THEN ""
      ELSE Log[CHOOSE i \in Lines({"Connect"}) : \A j \in Lines({"Connect"}) : i <= j].lab
Lbl(x) == IF x = L0 THEN "l0" ELSE "l1"
=============================================================================
