------------------------------- MODULE TraceLog -------------------------------
(* The recorded trace, read once: TLC evaluates a module's constant          *)
(* definitions before those of the modules extending it, so Trace.tla's     *)
(* definitions over Log see the value instead of reading the file again.   *)
EXTENDS Json, Sequences, Naturals

CONSTANT TraceFile

Log == ndJsonDeserialize(TraceFile)

\* The sessions, the windows (the first line, Up), and per session the sizes
\* of the data frames and window updates the relay received from the agent,
\* in order: what the agent sent (the stream is FIFO).
TraceSess == {Log[i].s : i \in 2..Len(Log)} \ {""}
TraceWin == [p \in {"r", "a"} |-> IF p = "r" THEN Log[1].n ELSE Log[1].w]
Recvd(s, t) ==
    LET R == SelectSeq(Log, LAMBDA e : e.ev = "Recv" /\ e.t = t /\ e.s = s)
    IN [i \in 1..Len(R) |-> R[i].n]
AgentData == [s \in TraceSess |-> Recvd(s, "data")]
AgentWins == [s \in TraceSess |-> Recvd(s, "win")]
=============================================================================
