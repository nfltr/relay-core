--------------------------- MODULE TraceHandoffAgent ---------------------------
(* Trace validation of Handoff.tla's agent side (spec/README.md "Agent-side *)
(* Handoff traces"): the agent's steps of one agent run, recorded by the    *)
(* real agent (github.com/nfltr/relay client/agent, -tags relaytrace,       *)
(* trace_handoff_on.go), must be a behaviour of Handoff. The relay's steps  *)
(* (its primaries' ends, binds the agent did not dial, drops, silences),    *)
(* the path's failure and the sessions' arrivals and placements are not     *)
(* logged: TLC chooses them between two logged lines (a session's only    *)
(* for the next one the agent takes: a link session stays open until its  *)
(* link drops, its apps' end changes nothing here). Accepted when TLC      *)
(* reaches the trace's end: TraceNotDone is violated.                       *)
EXTENDS TraceHandoffLog, Handoff, TLC

VARIABLE l \* the next line

E == Log[l]

Logged(ev) == l <= Len(Log) /\ E.ev = ev /\ l' = l + 1

\* The agent read primary E.g's header: the relay registered it with the
\* token of primary E.t.
ARegister == Logged("Register") /\ Register(E.g) /\ tok'[E.g] = E.t
ADial == Logged("Dial") /\ Dial(E.g)
\* The dial connected: the relay bound it (Bind), or refused it.
ABind == Logged("Bind") /\ lz.t = E.t /\ Bind /\ lz'.st = "up"
AGiveUp == Logged("GiveUp") /\ Bind /\ lz'.st = "none"
AAgentEnd == Logged("AgentEnd") /\ AgentEnd(E.g)
AAdopt == Logged("Adopt") /\ hdr = E.t /\ Adopt
AAdoptRefused == Logged("AdoptRefused") /\ hdr = E.t /\ AdoptRefused
\* The agent closed the link of token E.t (its park's TTL, a park the next
\* primary did not resume, the primary's end without a park).
ADrain == Logged("Drain") /\ LinkDrop(E.t) /\ ~conn'[E.t] /\ lz'.st # "dialing"
\* ... one the relay closed already (its primary's end).
ADrainGone == Logged("Drain") /\ ~conn[E.t] /\ UNCHANGED vars
\* The agent took a link session: the relay placed it on the link.
AOpen == Logged("Open") /\ (Place(E.s) \/ Wake(E.s)) /\ via'[E.s] = "link"
\* The agent refused it for want of a context (finding 42).
ARefused == Logged("Refused") /\ sst[E.s] = "refused" /\ via[E.s] = "link" /\ UNCHANGED vars
AUnknown ==
    /\ l <= Len(Log)
    /\ E.ev \notin {"Register", "Dial", "Bind", "GiveUp", "AgentEnd", "Adopt", "AdoptRefused", "Drain", "Open", "Refused"}
    /\ Print(<<"unknown event", E>>, FALSE)

\* Steps no line records: the relay's, the network's and the apps'.
Unl == l <= Len(Log) /\ UNCHANGED l
UPrimEnd == Unl /\ \E g \in Gens : PrimEnd(g)
UBind == Unl /\ Bind /\ lz'.st = "up" \* the Node's redial (DropAs redial)
ULinkDrop == Unl /\ \E t \in Gens : LinkDrop(t) \/ \E tr \in {"tcp", "quic"} : Silent(t, tr)
UPathDown == Unl /\ PathDown
\* lazyLink.end parks the link it took off its ended primary (linkPark.park).
UPark == Unl /\ Park
\* The session the agent takes next arrives, and waits for a dial; the
\* trace's sessions are all link sessions, so none other is placed. Before
\* a dial the agent logs, the next session it takes may arrive and wait:
\* the relay asks for a dial only for a waiting session (Place).
Opens == {i \in l..Len(Log) : Log[i].ev = "Open"}
NextOpen == IF Opens = {} THEN "" ELSE Log[CHOOSE i \in Opens : \A j \in Opens : i <= j].s
Waits(s) == s = IF E.ev = "Dial" THEN NextOpen ELSE IF E.ev = "Open" THEN E.s ELSE ""
UArrive == Unl /\ \E s \in Sess : Waits(s) /\ Arrive(s)
UWait == Unl /\ \E s \in Sess : Waits(s) /\ Place(s) /\ sst'[s] = "wait"

TraceInit == Init /\ l = 1

TraceNext ==
    \/ ARegister \/ ADial \/ ABind \/ AGiveUp \/ AAgentEnd \/ AAdopt \/ AAdoptRefused
    \/ ADrain \/ ADrainGone \/ AOpen \/ ARefused \/ AUnknown
    \/ UPrimEnd \/ UBind \/ ULinkDrop \/ UPathDown \/ UPark \/ UArrive \/ UWait

TraceNotDone == l <= Len(Log)
=============================================================================
