----------------------------- MODULE TraceNF1Anon -----------------------------
(* Trace validation of NF1Anon.tla (spec/README.md "NF1Anon traces"): the   *)
(* steps one source's anonymous registrations and their sessions' relayed  *)
(* legs take on a relay in the relaycoretest TestNF1Anon* tests (-tags      *)
(* relaytrace), recorded by core/nf1anon, must be a behaviour of NF1Anon.   *)
(* The relay sees only its side: a leg the peers end (a punch, the client   *)
(* giving up, the transfer done) is End, any of Punch, GiveUp or Done; a    *)
(* registration that ends early (the agent left, or its next connection    *)
(* took it over) is Leave, the model's Expire after the Ticks it skipped,   *)
(* which are not logged. A Relay line of n bytes is n of the model's Relay   *)
(* steps of that leg (RelayN). The trace is accepted when TLC reaches its   *)
(* end: TraceNotDone is then violated.                                     *)
EXTENDS TraceNF1AnonLog, NF1Anon, TLC

VARIABLE l   \* the next line

traceVars == <<vars, l>>

E == Log[l]

\* n consecutive Relay(s) steps that carry (none cut): the guard holds at
\* each because it holds for the last, and every counter grows by n.
RelayN(s, n) ==
    /\ st[s] = "relayed" /\ sent[s] + n <= MaxUnits
    /\ Budgeted(s) => leg[s] + n <= LegBudget /\ used + n <= AddrBudget
    /\ leg' = [leg EXCEPT ![s] = @ + n]
    /\ used' = IF Budgeted(s) THEN used + n ELSE used
    /\ arel' = [arel EXCEPT ![s] = @ + n]
    /\ sent' = [sent EXCEPT ![s] = @ + n]
    /\ act' = [s |-> "Relay", x |-> s]
    /\ UNCHANGED <<reg, srv, route, st, att, why>>

Step ==
    CASE E.ev = "Register" -> Register
      [] E.ev \in {"Expire", "Leave"} -> Expire
      [] E.ev = "Open" -> Open(E.s, E.srv, "nf1")
      [] E.ev = "Relay" -> RelayN(E.s, E.n)
      [] E.ev = "Cut" -> Relay(E.s) /\ act'.s = "Cut" /\ why'[E.s].a = E.why
      [] E.ev = "TimeCut" -> TimeCut(E.s) /\ why'[E.s].a = E.why
      [] E.ev = "End" -> \/ Punch(E.s) \/ GiveUp(E.s) \/ Done(E.s)
                         \/ st[E.s] = "failed" /\ UNCHANGED vars \* its registration ended under it (Expire)
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

\* The registration's ticks before its end, and a cut's end at the client
\* (Deliver or JoinEnd: the join's), not logged.
Unlogged ==
    \/ l <= Len(Log) /\ E.ev \in {"Expire", "Leave"} /\ Tick
    \/ \E s \in Sess : Deliver(s) \/ JoinEnd(s)

TraceInit == Init /\ l = 2 /\ Log[1].ev = "Limits" /\ Log[1].rate = 0

TraceNext ==
    \/ l <= Len(Log) /\ Step /\ l' = l + 1
    \/ Unlogged /\ UNCHANGED l

\* Violated once every line was taken: the trace is a behaviour.
TraceNotDone == l <= Len(Log)
=============================================================================
