-------------------------------- MODULE Trace --------------------------------
(* Trace validation (spec/README.md "Trace validation"): the relay's steps *)
(* of one gRPC frame stream with credit, recorded by a run of the real      *)
(* relay and agent (-tags relaytrace), must be a behaviour of AgentLink.    *)
(* The agent's steps and the stream's failure are not logged: TLC chooses  *)
(* them between two logged ones, each data frame and window update the     *)
(* agent sends sized as the relay later receives it. The trace is accepted *)
(* when TLC reaches its end: TraceNotDone is then violated (spec.sh).      *)
EXTENDS TraceLog, AgentLink, TLC

VARIABLES
    l,   \* the next line
    nd,  \* [TraceSess -> Nat]: the agent's data frames sent per session
    nw   \* [TraceSess -> Nat]: its window updates decided per session

traceVars == <<vars, l, nd, nw>>

E == Log[l]

\* The relay's frame at the head of what the agent sent is the one logged.
Got(e) ==
    /\ ch["a"] # <<>>
    /\ Head(ch["a"]).t = e.t /\ Head(ch["a"]).s = e.s
    /\ e.t \in {"data", "win"} => Head(ch["a"]).n = e.n

Step ==
    CASE E.ev = "Open" -> Open(E.s)
      [] E.ev = "Write" -> Write("r", E.s, E.n)
      [] E.ev = "SendData" -> pend["r"][E.s] = E.n /\ SendData("r", E.s)
      [] E.ev = "CloseWrite" -> CloseWrite("r", E.s)
      [] E.ev = "Abort" -> Abort("r", E.s)
      [] E.ev = "SendWin" -> gpend["r"][E.s] = E.n /\ SendWin("r", E.s)
      [] E.ev = "Read" -> Read("r", E.s, E.n)
      [] E.ev = "Recv" -> Got(E) /\ Recv("r")
      [] E.ev = "Finish" -> Finish("r", E.s)
      [] E.ev = "Down" -> Down("r")
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

\* The agent: its receive loop, its app's writes sized as the relay
\* receives them, reads sized as the window updates the relay receives (or
\* below a grant), closes, and the stream's failure.
AgentWrite(s) ==
    /\ nd[s] < Len(AgentData[s])
    /\ Write("a", s, AgentData[s][nd[s] + 1])
    /\ nd' = [nd EXCEPT ![s] = @ + 1]

AgentRead(s) ==
    \/ /\ nw[s] < Len(AgentWins[s])
       /\ LET k == AgentWins[s][nw[s] + 1] - cr["a"][s].cons IN
            /\ k >= 1
            /\ Read("a", s, k)
            /\ gpend'["a"][s] = gpend["a"][s] + AgentWins[s][nw[s] + 1]
       /\ nw' = [nw EXCEPT ![s] = @ + 1]
    \/ /\ q["a"][s] > 0 /\ cr["a"][s].cons + q["a"][s] < Thr("a")
       /\ Read("a", s, q["a"][s])
       /\ UNCHANGED nw
    \/ /\ q["a"][s] > 0 /\ ~cr["a"][s].on  \* forgotten: no grant
       /\ Read("a", s, q["a"][s])
       /\ UNCHANGED nw

\* The agent's steps are needed only for what the relay receives next, so
\* they are taken only before the relay's next receive (and only for its
\* session) or, with the stream's failure, before its Down. Its receive
\* loop first takes everything the relay sent before (receiving earlier
\* only gives the agent more credit, data to read and sessions open), and
\* it sends the frame the relay receives next only once the relay took the
\* one before (later it has no less credit, and its frames keep their
\* order): the order of steps that commute is not searched.
Unlogged ==
    /\ l <= Len(Log)
    /\ \/ E.ev \in {"Recv", "Down"} /\ Recv("a") /\ UNCHANGED <<nd, nw>>
       \/ E.ev = "Recv" /\ ch["r"] = <<>> /\ ch["a"] = <<>> /\ LET s == E.s IN
            \/ AgentWrite(s) /\ UNCHANGED nw
            \/ AgentRead(s) /\ UNCHANGED nd
            \/ (SendData("a", s) \/ SendWin("a", s) \/ CloseWrite("a", s) \/ Abort("a", s) \/ Finish("a", s))
               /\ UNCHANGED <<nd, nw>>
       \/ E.ev = "Down" /\ (Break \/ Down("a")) /\ UNCHANGED <<nd, nw>>

TraceInit ==
    /\ Init
    /\ l = 2
    /\ nd = [s \in Sess |-> 0]
    /\ nw = [s \in Sess |-> 0]

TraceNext ==
    \/ l <= Len(Log) /\ Step /\ l' = l + 1 /\ UNCHANGED <<nd, nw>>
    \/ Unlogged /\ UNCHANGED l

\* Violated once every line was taken: the trace is a behaviour.
TraceNotDone == l <= Len(Log)
=============================================================================
