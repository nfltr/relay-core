--------------------------- MODULE TraceTakeoverLog ---------------------------
(* A recorded takeover trace, read once (as TraceLog.tla): its processes  *)
(* (each ConnectHTTP of the agent ID on one dispatcher), each one's       *)
(* credential (every line carries it), and a machine per process: the     *)
(* relay sees no machines, so the machine lock (nfltr's half) is left to  *)
(* TLC, which takes Start unlogged before a process's first line.         *)
EXTENDS Json, Sequences, Naturals

CONSTANT TraceFile

Log == ndJsonDeserialize(TraceFile)

TraceProcs == {Log[i].p : i \in 1..Len(Log)}
TraceCred == [p \in TraceProcs |-> Log[CHOOSE i \in 1..Len(Log) : Log[i].p = p].c]
TraceMach == [p \in TraceProcs |-> p]
=============================================================================
