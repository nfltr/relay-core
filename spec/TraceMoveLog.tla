----------------------------- MODULE TraceMoveLog -----------------------------
(* A move's recorded trace, read once (TraceLog.tla says why), and the     *)
(* requests it names: the transactions it published, forwarded or got an   *)
(* answer to, and one for each pick (a pick names no transaction: TLC      *)
(* chooses which new request it is).                                       *)
EXTENDS Json, Sequences, Naturals, FiniteSets, TLC

CONSTANT TraceFile

Log == ndJsonDeserialize(TraceFile)

\* One request for each forward a peer replica sent (ArriveA names none).
ArriveReq == {"arrive" \o ToString(i) : i \in 1..Cardinality({j \in 1..Len(Log) : Log[j].ev = "ArriveA"})}

TraceReq ==
    ({Log[i].r : i \in {j \in 1..Len(Log) : Log[j].ev \in {"Publish", "Forward"}}}
     \cup ({Log[i].m : i \in {j \in 1..Len(Log) : Log[j].ev = "RecvA"}} \ {"CONNECTED", "EOF"}))
    \cup {"pick" \o ToString(i) : i \in 1..Cardinality({j \in 1..Len(Log) : Log[j].ev = "Pick"})}
    \cup ArriveReq


\* The client TCP sessions it placed.
TraceTSess == {Log[i].r : i \in {j \in 1..Len(Log) : Log[j].ev = "PlaceTCP"}}
=============================================================================
