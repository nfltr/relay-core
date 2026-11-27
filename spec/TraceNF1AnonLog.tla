--------------------------- MODULE TraceNF1AnonLog ---------------------------
(* A recorded NF1Anon trace (core/nf1anon trace_on.go), read once: its     *)
(* first line the relay's limits (the model's budgets; a refilling bucket  *)
(* is no trace of the model), then its steps. Its legs, registrations and  *)
(* relayed bytes size the model's constants.                               *)
EXTENDS Json, Sequences, Naturals, FiniteSets

CONSTANT TraceFile

Log == ndJsonDeserialize(TraceFile)

Lines(evs) == {i \in 1..Len(Log) : Log[i].ev \in evs}

RECURSIVE SumN(_)
SumN(S) == IF S = {} THEN 0 ELSE LET i == CHOOSE x \in S : TRUE IN Log[i].n + SumN(S \ {i})

TraceSess == {Log[i].s : i \in Lines({"Open"})}
TraceLegBudget == Log[1].leg
TraceAddrBudget == Log[1].addr
TraceRegs == Cardinality(Lines({"Register"}))
\* More than every byte the trace relayed: a cut stays possible (Relay's guard).
TraceUnits == SumN(Lines({"Relay"})) + 1
=============================================================================
