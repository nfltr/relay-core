------------------------------ MODULE TraceNF1Cap ------------------------------
(* Trace validation of NF1Cap.tla (spec/README.md "NF1Cap traces"): the     *)
(* steps one address's counted legs take on a relay in the core/nf1cap      *)
(* tests and relaycoretest's TestNF1RelayCap (-tags relaytrace), recorded   *)
(* by core/nf1cap, must be a behaviour of NF1Cap. The relay sees only its   *)
(* side: every leg it counts is a stranger's session (the owner's are not   *)
(* counted, so not recorded); a leg the peers end (a punch, the transfer    *)
(* done) is End, the model's Punch; the owner's label change is not logged  *)
(* (SetCap, taken before the Learn that reads it). A Relay line of n bytes  *)
(* is n of the model's Relay steps of that leg (RelayN), u the relay's     *)
(* count after them (CountedOnce). The trace is                            *)
(* accepted when TLC reaches its end: TraceNotDone is then violated.        *)
EXTENDS TraceNF1CapLog, NF1Cap, TLC

VARIABLE l   \* the next line

E == Log[l]

\* k consecutive Relay(s) steps that carry (none cut): the guard holds at
\* each because it holds for the last, and every counter grows by k.
RelayN(s, k) ==
    /\ st[s] = "relayed" /\ sent[s] + k <= MaxUnits
    /\ Counted(s) => used + k <= CapOf(s)
    /\ used' = IF Counted(s) THEN used + k ELSE used
    /\ rel' = [rel EXCEPT ![s] = @ + k]
    /\ sent' = [sent EXCEPT ![s] = @ + k]
    /\ act' = [s |-> "Relay", x |-> s]
    /\ UNCHANGED <<ocap, rcap, cli, st, att, scap, why, hi, n>>

Step ==
    CASE E.ev = "Learn" -> Learn /\ rcap' = E.cap
      [] E.ev = "NewDay" -> NewDay
      [] E.ev = "Open" -> Open(E.s, "stranger") /\ (st'[E.s] = "relayed") = ("ok" \in DOMAIN E)
      [] E.ev = "Relay" -> RelayN(E.s, E.n) /\ used' = E.u
      [] E.ev = "Cut" -> Relay(E.s) /\ act'.s = "Cut"
      [] E.ev = "End" -> Punch(E.s)
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

\* The owner's label change the next Learn reads, not logged.
Unlogged == \E c \in Caps : SetCap(c)

TraceInit == Init /\ l = 2 /\ Log[1].ev = "Limits" /\ Log[1].cap = CapMax

TraceNext ==
    \/ l <= Len(Log) /\ Step /\ l' = l + 1
    \/ Unlogged /\ UNCHANGED l

\* Violated once every line was taken: the trace is a behaviour.
TraceNotDone == l <= Len(Log)
=============================================================================
