----------------------------- MODULE MCAgentLink -----------------------------
(* Small sizes for model checking (spec/README.md "What is abstracted").    *)
(* Refines: a frame stream's sessions give link's session interface         *)
(* (LinkSession.tla), so a layer above models a session on the agent link   *)
(* and one on the frame streams alike (spec/README.md "The link session     *)
(* interface").                                                             *)
EXTENDS AgentLink
MCWin == [p \in {"r", "a"} |-> 2]
MCWin1 == [p \in {"r", "a"} |-> 1]
MCWinW == [p \in {"r", "a"} |-> IF p = "r" THEN 1 ELSE 2]  \* AgentLinkWrite.cfg: the relay's writes

-----------------------------------------------------------------------------
(* The refinement mapping. The relay's whole close is a Close (the agent's  *)
(* target reads EOF: finding 15, open); the agent's is an abort (the        *)
(* relay's reader reads a reset). Bytes a send that held credit queues      *)
(* after this side's close are not written in the interface's terms: the    *)
(* peer takes the close first (FIFO) and drops them.                        *)
WEnd(p, s) == CASE wfin[p][s] = "half" -> "fin"
                [] wfin[p][s] = "whole" /\ p = "r" -> "close"
                [] wfin[p][s] = "whole" -> "reset"
                [] OTHER -> ""

\* p sent a FIN: its half close, or the relay's whole close. The agent's
\* whole close after its half close keeps the FIN (on its way or taken).
HalfOnWay(p, s) == \E i \in DOMAIN ch[p] : ch[p][i].t = "half" /\ ch[p][i].s = s
Fin(p, s) ==
    \/ wfin[p][s] = "half"
    \/ wfin[p][s] = "whole" /\ (p = "r" \/ rfin[Peer(p)][s] = "half" \/ HalfOnWay(p, s))

LS == INSTANCE LinkSession WITH
    Sides <- P,
    Sids <- Sess,
    Owner <- [s \in Sess |-> "r"],
    w <- [p \in P |-> [s \in Sess |-> IF wfin[p][s] # "" THEN finAt[p][s] ELSE wr[p][s]]],
    d <- rcv,
    c <- [p \in P |-> [s \in Sess |-> rcv[p][s] - q[p][s]]],
    wend <- [p \in P |-> [s \in Sess |-> WEnd(p, s)]],
    fin <- [p \in P |-> [s \in Sess |-> Fin(p, s)]],
    shut <- [p \in P |-> [s \in Sess |-> wfin[p][s] = "whole"]],
    rend <- why,
    ga <- [p \in P |-> FALSE],
    up <- [p \in P |-> ~down[p]],
    fault <- err # {}

Refines == LS!Spec
=============================================================================
