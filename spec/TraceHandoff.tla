----------------------------- MODULE TraceHandoff -----------------------------
(* Trace validation of Handoff.tla (spec/README.md "Trace validation"): the  *)
(* relay's steps for one agent with the agent link on, recorded by a run of *)
(* the real relay and agent (-tags relaytrace, trace_handoff_on.go), must   *)
(* be a behaviour of Handoff. The agent's steps (Dial, AgentEnd, Park,      *)
(* Adopt, AdoptRefused), a refused or second connection's bind, and its     *)
(* TCP path's failure are not logged: TLC chooses them between two logged   *)
(* ones. The trace is accepted when TLC reaches its end: TraceNotDone is    *)
(* violated.                                                                *)
EXTENDS TraceHandoffLog, Handoff, TLC

VARIABLE l \* the next line

E == Log[l]

\* The session's placement after the step is the one logged.
AsLogged(e) ==
    IF e.ev = "Refused" THEN sst'[e.s] = "refused"
    ELSE /\ sst'[e.s] = "open" /\ via'[e.s] = e.via
         /\ on'[e.s] = IF e.via = "link" THEN e.t ELSE e.g

\* Line l is ev; the step takes it.
Logged(ev) == l <= Len(Log) /\ E.ev = ev /\ l' = l + 1

TRegister == Logged("Register") /\ Register(E.g) /\ tok'[E.g] = E.t
TPrimEnd == Logged("PrimEnd") /\ PrimEnd(E.g)
TBind == Logged("Bind") /\ Bind /\ lz.t = E.t /\ ~conn[E.t] /\ conn'[E.t]
TLinkDrop == Logged("LinkDrop") /\ LinkDrop(E.t)
\* A connection the agent closed (its park: AgentEnd, AdoptRefused) left
\* the model's view of the link already.
TLinkDropGone == Logged("LinkDrop") /\ ~conn[E.t] /\ UNCHANGED vars
\* The relay ended the primary for its link's silence on transport E.via.
TSilent == Logged("Silent") /\ Silent(E.t, E.via) /\ bound[E.t] # NoTok /\ sil'[bound[E.t]]
\* The silence logged after its connection's unbind.
TSilentAfterDrop ==
    /\ Logged("Silent") /\ E.via = "tcp" /\ ~conn[E.t] /\ ~tcpUp /\ bound[E.t] # NoTok
    /\ sil' = [sil EXCEPT ![bound[E.t]] = TRUE]
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, hdr, tcpUp, sst, via, on, by>>
\* carriesSessions answered (a rebalance's check): what the model counts.
TCarries == Logged("Carries") /\ Carries = (E.via = "yes") /\ UNCHANGED vars
TArrive == Logged("Arrive") /\ Arrive(E.s)
TWait == Logged("Wait") /\ Place(E.s) /\ sst'[E.s] = "wait"
TPlace == (Logged("Open") \/ Logged("Refused")) /\ Place(E.s) /\ AsLogged(E)
TWake == (Logged("Open") \/ Logged("Refused")) /\ Wake(E.s) /\ AsLogged(E)
TEnd == Logged("End") /\ End(E.s)
\* The code ends a session lost or cut under it, or refused, too.
TEndGone == Logged("End") /\ sst[E.s] \in {"lost", "cut", "refused"} /\ UNCHANGED vars
TUnknown ==
    /\ l <= Len(Log)
    /\ E.ev \notin {"Register", "PrimEnd", "Bind", "LinkDrop", "Silent", "Carries", "Arrive", "Wait", "Open", "Refused", "End"}
    /\ Print(<<"unknown event", E>>, FALSE)

\* Steps no line records: the agent's, a refused or second connection's
\* bind, the path's failure; and a wait the code skipped (its primary
\* ending) before the placement logged next.
Unl == l <= Len(Log) /\ UNCHANGED l
UDial == Unl /\ \E g \in Gens : Dial(g)
UAgentEnd == Unl /\ \E g \in Gens : AgentEnd(g)
UPark == Unl /\ Park
UAdopt == Unl /\ Adopt
UAdoptRefused == Unl /\ AdoptRefused
UPathDown == Unl /\ PathDown
UBind == Unl /\ Bind /\ UNCHANGED conn
USkipWait == Unl /\ E.ev \in {"Open", "Refused"} /\ sst[E.s] = "arrived" /\ Place(E.s) /\ sst'[E.s] = "wait"

TraceInit == Init /\ l = 1

TraceNext ==
    \/ TRegister \/ TPrimEnd \/ TBind \/ TLinkDrop \/ TLinkDropGone \/ TSilent \/ TSilentAfterDrop
    \/ TCarries \/ TArrive \/ TWait \/ TPlace \/ TWake \/ TEnd \/ TEndGone \/ TUnknown
    \/ UDial \/ UAgentEnd \/ UPark \/ UAdopt \/ UAdoptRefused \/ UPathDown \/ UBind \/ USkipWait

\* Violated once every line was taken: the trace is a behaviour.
TraceNotDone == l <= Len(Log)
=============================================================================
