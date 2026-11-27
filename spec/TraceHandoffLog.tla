---------------------------- MODULE TraceHandoffLog ----------------------------
(* A recorded Handoff trace, read once (as TraceLog.tla): its sessions and  *)
(* how many primaries registered.                                           *)
EXTENDS Json, Sequences, Naturals, FiniteSets

CONSTANT TraceFile

Log == ndJsonDeserialize(TraceFile)

TraceSess == {Log[i].s : i \in 1..Len(Log)} \ {""}
TraceMaxGen == Cardinality({i \in 1..Len(Log) : Log[i].ev = "Register"})
=============================================================================
