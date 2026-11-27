---------------------------- MODULE TraceNF1CapLog ----------------------------
(* A recorded NF1Cap trace (core/nf1cap trace_on.go), read once: its first *)
(* line the relay's default cap (the model's initial cap), then one       *)
(* address's steps on one relay. Its legs, caps, days and relayed bytes    *)
(* size the model's constants.                                             *)
EXTENDS Json, Sequences, Naturals, FiniteSets

CONSTANT TraceFile

Log == ndJsonDeserialize(TraceFile)

Lines(evs) == {i \in 1..Len(Log) : Log[i].ev \in evs}

RECURSIVE SumN(_)
SumN(S) == IF S = {} THEN 0 ELSE LET i == CHOOSE x \in S : TRUE IN Log[i].n + SumN(S \ {i})

TraceSess == {Log[i].s : i \in Lines({"Open"})}
TraceCaps == {Log[1].cap} \cup {Log[i].cap : i \in Lines({"Learn"})}
TraceSets == Cardinality(Lines({"Learn"}))
TraceDays == Cardinality(Lines({"NewDay"}))
\* More than every byte the trace relayed: a cut stays possible (Relay's guard).
TraceUnits == SumN(Lines({"Relay"})) + 1
=============================================================================
