----------------------------- MODULE TraceTakeover -----------------------------
(* Trace validation of Takeover.tla (spec/README.md "Takeover.tla"): the  *)
(* relay's steps for one agent ID, recorded by the real dispatcher's      *)
(* ConnectHTTP (-tags relaytrace, core/session trace_takeover_on.go),     *)
(* must be a behaviour of Takeover. Connect is a session the relay takes  *)
(* (q: the live session it replaces and tells, or ""), Refuse one it      *)
(* refuses (q: the holder); Notice a replaced session's end, End any      *)
(* other end (Stop). A process's Start (the machine lock, nfltr's half)   *)
(* is not logged: TLC takes it before the process's first line. The trace *)
(* is accepted when TLC reaches its end: TraceNotDone is then violated.   *)
EXTENDS TraceTakeoverLog, Takeover, TLC

VARIABLE l   \* the next line

E == Log[l]

Q == IF E.q = "" THEN "none" ELSE E.q

Step ==
    CASE E.ev = "Connect" -> rel = Q /\ Connect(E.p) /\ ps'[E.p] = "serving"
      [] E.ev = "Refuse" -> rel = Q /\ Connect(E.p) /\ ps'[E.p] = "refused"
      [] E.ev = "Notice" -> Notice(E.p)
      [] E.ev = "End" -> Stop(E.p)
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

Unlogged ==
    /\ l <= Len(Log)
    /\ E.ev \in {"Connect", "Refuse"}
    /\ ps[E.p] = "idle"
    /\ \E r \in BOOLEAN : Start(E.p, r)

TraceInit == Init /\ l = 1

TraceNext ==
    \/ l <= Len(Log) /\ Step /\ l' = l + 1
    \/ Unlogged /\ UNCHANGED l

\* Violated once every line was taken: the trace is a behaviour.
TraceNotDone == l <= Len(Log)
=============================================================================
