---------------------------- MODULE AgentReplace ----------------------------
(* Which stream a new session of an agent ID takes on this relay while a  *)
(* new connection of the agent replaces the old one (lane AGENTREPLACE,    *)
(* docs/decisions/agent-replacement-routing-2026-11-26.md, spec/README.md *)
(* "AgentReplace.tla"). A connection is one connectOnce of the relay SDK:  *)
(* its ConnectHTTP stream and its primary ConnectTCP stream, both carrying *)
(* its connection id (agentwire.ConnKey). The two reach the relay in       *)
(* either order: the SDK starts the TCP tunnel beside ConnectHTTP, and     *)
(* ConnectHTTP's takeover waits for the replaced session's teardown.       *)
(* Connect is Takeover.tla's Connect (takeOverSession commits): the agent's*)
(* current connection becomes c. Every open (onward /connect, shares, SNI  *)
(* E2EE, A2A, DialHTTP, ServeLinkTCP, nf1) is one Open: they all choose    *)
(* their stream in TCPDispatcher.agentStreamOrSignal. The open's checks    *)
(* after the choice (nf1's declaration, a share's gate) are NF1Claim.tla's *)
(* and Failover.tla's.                                                     *)
EXTENDS Naturals, FiniteSets

CONSTANTS Conns,  \* the agent ID's connections in a behaviour (one connectOnce each)
          Mutant  \* "none", or a bug put back (spec/README.md)

None == "none"
Offline == "offline" \* the open's typed answer: the agent is not reachable here now

VARIABLES
    hs,    \* conn -> its ConnectHTTP: idle, dial (open, takeover not committed), up (current), gone
    ts,    \* conn -> its primary ConnectTCP: idle, dial, reg (in rotation), yield (out of it), closed
    alive, \* conn -> its client process holds it
    cur,   \* the connection id of the agent's current ConnectHTTP here, None
    rep,   \* the ids of connections a Connect replaced (the relay remembers them)
    op,    \* the open in progress
    act    \* the last step, for the witnesses

vars == <<hs, ts, alive, cur, rep, op, act>>

IdleOp == [pc |-> "idle", res |-> None, ok |-> TRUE, alt |-> FALSE]

TypeOK ==
    /\ hs \in [Conns -> {"idle", "dial", "up", "gone"}]
    /\ ts \in [Conns -> {"idle", "dial", "reg", "yield", "closed"}]
    /\ alive \in [Conns -> BOOLEAN]
    /\ cur \in Conns \cup {None}
    /\ rep \subseteq Conns
    /\ op.pc \in {"idle", "route", "wait", "done"}
    /\ op.res \in Conns \cup {None, Offline}
    /\ op.ok \in BOOLEAN /\ op.alt \in BOOLEAN

Init ==
    /\ hs = [c \in Conns |-> "idle"]
    /\ ts = [c \in Conns |-> "idle"]
    /\ alive = [c \in Conns |-> FALSE]
    /\ cur = None
    /\ rep = {}
    /\ op = IdleOp
    /\ act = [s |-> "Init", c |-> None]

\* A stream may take a new session: in rotation and of the current
\* connection (any, before the agent's first Connect here: a ConnectTCP-only
\* client). "route-by-id" is the code before: the agent ID's newest primary,
\* whatever its connection (no ids, no yield, no refusal).
Routable(c) == ts[c] = "reg" /\ (cur \in {None, c} \/ Mutant = "route-by-id")

\* A connection that is or will be the agent's, live, its stream not ended.
Pending(c) == alive[c] /\ hs[c] \in {"dial", "up"} /\ ts[c] \in {"dial", "reg"} /\ c \notin rep

\* The relay SDK's connectOnce: a fresh connection id, its ConnectHTTP and
\* ConnectTCP opened.
Start(c) ==
    /\ hs[c] = "idle"
    /\ hs' = [hs EXCEPT ![c] = "dial"]
    /\ ts' = [ts EXCEPT ![c] = "dial"]
    /\ alive' = [alive EXCEPT ![c] = TRUE]
    /\ act' = [s |-> "Start", c |-> c]
    /\ UNCHANGED <<cur, rep, op>>

\* takeOverSession commits (Takeover.tla Connect, same credential): c is
\* the agent's current connection; the one it replaces is told
\* (errReplacedSessionStatus) and its primaries yield (out of rotation,
\* their sessions run on, ended once idle or at replacedSessionsWait).
\* "yield-all": every other connection's primary yields, a newer one's that
\* registered before its own Connect too. "no-yield": none yields.
Connect(c) ==
    /\ hs[c] = "dial" /\ alive[c]
    /\ LET old == cur
           r == IF old \in Conns /\ old # c /\ Mutant # "route-by-id" THEN rep \cup {old} ELSE rep
       IN /\ hs' = [x \in Conns |-> IF x = c THEN "up" ELSE IF x = old /\ hs[x] = "up" THEN "gone" ELSE hs[x]]
          /\ cur' = c
          /\ rep' = r
          /\ ts' = [x \in Conns |->
                       IF ts[x] = "reg" /\ Mutant \notin {"no-yield", "route-by-id"}
                          /\ (x \in r \/ (Mutant = "yield-all" /\ x # c))
                       THEN "yield" ELSE ts[x]]
    /\ act' = [s |-> "Connect", c |-> c]
    /\ UNCHANGED <<alive, op>>

\* The connection's primary ConnectTCP registers (enterRotation), before
\* or after its Connect. A replaced connection's is refused
\* (errReplacedSessionStatus). "late-old-kept": it enters as any other.
Register(c) ==
    /\ ts[c] = "dial" /\ alive[c]
    /\ IF c \in rep /\ Mutant # "late-old-kept"
       THEN /\ ts' = [ts EXCEPT ![c] = "closed"]
            /\ act' = [s |-> "RefusedLate", c |-> c]
       ELSE /\ ts' = [ts EXCEPT ![c] = "reg"]
            /\ act' = [s |-> "Register", c |-> c]
    /\ UNCHANGED <<hs, alive, cur, rep, op>>

\* The client closes its connection: Ctrl+C, or the replaced client once
\* its sessions ended (FatalReplaced; at most 30 s, relay client v0.19.4+).
\* No fairness: an old or hung client may never.
Stop(c) ==
    /\ alive[c]
    /\ alive' = [alive EXCEPT ![c] = FALSE]
    /\ hs' = [hs EXCEPT ![c] = "gone"]
    /\ ts' = [ts EXCEPT ![c] = "closed"]
    /\ act' = [s |-> "Stop", c |-> c]
    /\ UNCHANGED <<cur, rep, op>>

\* The client dies unseen (a crash, a lost network): a stream not yet
\* registered never will; a registered one stays until liveness. Its
\* ConnectHTTP's end (by liveness too) is taken here.
Die(c) ==
    /\ alive[c]
    /\ alive' = [alive EXCEPT ![c] = FALSE]
    /\ hs' = [hs EXCEPT ![c] = "gone"]
    /\ ts' = [ts EXCEPT ![c] = IF @ = "dial" THEN "closed" ELSE @]
    /\ act' = [s |-> "Die", c |-> c]
    /\ UNCHANGED <<cur, rep, op>>

\* The stream's liveness ends a dead client's stream.
Detect(c) ==
    /\ ~alive[c] /\ ts[c] \in {"reg", "yield"}
    /\ ts' = [ts EXCEPT ![c] = "closed"]
    /\ act' = [s |-> "Detect", c |-> c]
    /\ UNCHANGED <<hs, alive, cur, rep, op>>

\* The relay ends a yielded primary: once it carries no session, or at
\* replacedSessionsWait (its sessions reset, typed: AgentLink.tla).
RelayEnd(c) ==
    /\ ts[c] = "yield"
    /\ ts' = [ts EXCEPT ![c] = "closed"]
    /\ act' = [s |-> "RelayEnd", c |-> c]
    /\ UNCHANGED <<hs, alive, cur, rep, op>>

\* The agent left this relay whole: its current entry is dropped (memory
\* bound; its replaced ids are kept while any of its streams is here).
Clear ==
    /\ cur # None /\ hs[cur] = "gone"
    /\ \A x \in Conns : ts[x] \in {"idle", "closed"} /\ hs[x] # "dial"
    /\ cur' = None
    /\ act' = [s |-> "Clear", c |-> None]
    /\ UNCHANGED <<hs, ts, alive, rep, op>>

\* A new session's open (every caller class).
Open ==
    /\ op.pc = "idle"
    /\ op' = [IdleOp EXCEPT !.pc = "route"]
    /\ act' = [s |-> "Open", c |-> None]
    /\ UNCHANGED <<hs, ts, alive, cur, rep>>

\* agentStreamOrSignal returns a stream: the session is placed on it (attach).
\* ok: whether it is the current connection's, read in the same step.
Place(c) ==
    /\ op.pc \in {"route", "wait"} /\ Routable(c)
    /\ op' = [pc |-> "done", res |-> c, alt |-> FALSE,
              ok |-> (cur = c \/ (cur = None /\ c \notin rep))]
    /\ act' = [s |-> "Place", c |-> c]
    /\ UNCHANGED <<hs, ts, alive, cur, rep>>

\* No stream may take it: the open waits for the next registration
\* (streamFor, waitAgentStream: the registration signal, agentStreamWait).
\* "no-wait": the open answers offline at once.
Wait ==
    /\ op.pc = "route" /\ \A c \in Conns : ~Routable(c)
    /\ IF Mutant = "no-wait"
       THEN op' = [op EXCEPT !.pc = "done", !.res = Offline, !.alt = \E c \in Conns : Pending(c)]
       ELSE op' = [op EXCEPT !.pc = "wait"]
    /\ act' = [s |-> "Wait", c |-> None]
    /\ UNCHANGED <<hs, ts, alive, cur, rep>>

\* The wait's bound passes with no stream: offline (the caller's next
\* replica or its typed refusal). The bound is an assumption (spec/README.md
\* "AgentReplace.tla"): a live connection's streams complete within it, so
\* the bound passes only once none is pending.
Timeout ==
    /\ op.pc = "wait"
    /\ \A c \in Conns : ~Routable(c) /\ ~Pending(c)
    /\ op' = [op EXCEPT !.pc = "done", !.res = Offline, !.alt = FALSE]
    /\ act' = [s |-> "Timeout", c |-> None]
    /\ UNCHANGED <<hs, ts, alive, cur, rep>>

\* The open's answer went back; the next open starts.
Reset ==
    /\ op.pc = "done"
    /\ op' = IdleOp
    /\ act' = [s |-> "Reset", c |-> None]
    /\ UNCHANGED <<hs, ts, alive, cur, rep>>

Next ==
    \/ \E c \in Conns : Start(c) \/ Connect(c) \/ Register(c) \/ Stop(c) \/ Die(c) \/ Detect(c) \/ RelayEnd(c) \/ Place(c)
    \/ Clear \/ Open \/ Wait \/ Timeout \/ Reset

Spec == Init /\ [][Next]_vars

\* The relay's and the live clients' steps are fair; Stop and Die are not
\* (a client may ignore its notice, or never die).
LiveSpec == Spec
    /\ \A c \in Conns : (WF_vars(Connect(c)) /\ WF_vars(Register(c)) /\ WF_vars(Detect(c)) /\ WF_vars(RelayEnd(c)))
    /\ WF_vars(\E x \in Conns : Place(x)) /\ WF_vars(Wait) /\ WF_vars(Timeout)

-----------------------------------------------------------------------------
(* Invariants. *)

\* A new session reaches only the current connection's stream (before the
\* agent's first Connect here: a connection no Connect replaced): never a
\* replaced connection's, never a newer one's before its takeover commits.
PlaceCurrent == op.res \in Conns => op.ok

\* The current connection's stream is never out of rotation.
CurrentNotYielded == cur # None => ts[cur] # "yield"

\* An open answers offline only when no live connection of the agent was
\* coming: never for the replacement window itself.
NoFalseOffline == op.res = Offline => ~op.alt

\* One ConnectHTTP is current.
UpIsCurrent == \A c \in Conns : hs[c] = "up" => cur = c

\* Liveness (LiveSpec): every open answers (served or offline, never
\* stuck), and a replaced connection's streams end, its client obeying or
\* not.
OpenAnswers == op.pc \in {"route", "wait"} ~> op.pc = "done"
ReplacedEnds == \A c \in Conns : c \in rep ~> ts[c] = "closed"

\* Witnesses: each must be violated (its state reached).
\* An open waits out the window: the old stream yielded, the new not yet registered.
WitnessWindow == ~(op.pc = "wait" /\ cur # None /\ ts[cur] = "dial" /\ \E x \in Conns : ts[x] = "yield")
\* A connection's stream registered before its takeover committed, another current.
WitnessEarly == ~\E c \in Conns : hs[c] = "dial" /\ ts[c] = "reg" /\ cur \notin {None, c}
\* A replaced connection's ConnectTCP arrived after its replacement.
WitnessLateRefused == act.s # "RefusedLate"
WitnessOffline == op.res # Offline
\* An open served after a replacement.
WitnessServedAfterReplace == ~(op.res \in Conns /\ rep # {})
\* The agent left whole after a replacement.
WitnessCleared == ~(cur = None /\ rep # {})
=============================================================================
