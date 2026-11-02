------------------------------ MODULE TraceMove ------------------------------
(* Trace validation of Move.tla (spec/README.md "Trace validation"): the   *)
(* replica-side steps of one HTTP session of an agent that moves, recorded *)
(* by a run of the real dispatcher (-tags relaytrace), must be a behaviour *)
(* of Move. The agent's steps (RecvAgent, ConnectB, Answer, CloseOld) and   *)
(* with them the directory's record are not logged: TLC chooses them      *)
(* between two logged ones. A lease's end (Release) is the step after its *)
(* Publish (pl counts those), or Unlease. The trace is accepted when TLC   *)
(* reaches its end: TraceNotDone is then violated (spec.sh).               *)
EXTENDS TraceMoveLog, Move

VARIABLES
    l,   \* the next line
    pl   \* leases whose request was published and that have not ended

traceVars == <<vars, l, pl>>

E == Log[l]

Step ==
    CASE E.ev = "Pick" -> routes = E.ok /\ (\E r \in Req : Pick(r)) /\ UNCHANGED pl
      [] E.ev = "Release" -> \/ pl > 0 /\ pl' = pl - 1 /\ UNCHANGED vars
                             \/ (\E r \in Req : Unlease(r)) /\ UNCHANGED pl
      [] E.ev = "Publish" -> (rel # "ended") = E.ok /\ Publish(E.r) /\ pl' = pl + 1
      [] E.ev = "Forward" -> cq # <<>> /\ Head(cq) = E.r /\ Forward /\ UNCHANGED pl
      [] E.ev = "Ask" -> Ask /\ UNCHANGED pl
      [] E.ev = "GiveUp" -> GiveUp /\ UNCHANGED pl
      [] E.ev = "StopRouting" -> StopRouting /\ UNCHANGED pl
      [] E.ev = "Refresh" -> Refresh /\ UNCHANGED pl
      [] E.ev = "Done" -> Done /\ UNCHANGED pl
      [] E.ev = "StartDrain" -> StartDrain /\ UNCHANGED pl
      [] E.ev = "DrainBound" -> DrainBound /\ UNCHANGED pl
      [] E.ev = "RecvA" -> upA # <<>> /\ Head(upA) = E.m /\ RecvA /\ UNCHANGED pl
      [] E.ev = "TCPSupersede" -> TCPSupersede /\ UNCHANGED pl
      [] E.ev = "PlaceTCP" -> tin = E.ok /\ PlaceTCP(E.r) /\ UNCHANGED pl
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

\* The agent, and with its new connection the record: only what the next
\* logged step can need. Its receive loop runs whenever the relay sent it
\* something; it connects B, answers, and closes only before a receive of
\* A (RecvA) or a step that reads the record (StopRouting, Refresh,
\* TCPSupersede, a refused Pick). B's serving (ServeB) and a failed dial
\* change nothing a logged step reads: not searched.
Unlogged ==
    /\ l <= Len(Log)
    /\ UNCHANGED pl
    /\ \/ RecvAgent
       \/ E.ev \in {"RecvA", "StopRouting", "Refresh", "TCPSupersede", "Pick"} /\ ConnectB
       \/ E.ev = "RecvA" /\ (CloseOld \/ \E r \in Req : Answer(r))

TraceInit == Init /\ l = 1 /\ pl = 0

TraceNext ==
    \/ l <= Len(Log) /\ Step /\ l' = l + 1
    \/ Unlogged /\ UNCHANGED l

\* Violated once every line was taken: the trace is a behaviour.
TraceNotDone == l <= Len(Log)
=============================================================================
