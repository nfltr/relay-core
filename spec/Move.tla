------------------------------- MODULE Move -------------------------------
(* A make-before-break move of one agent's ConnectHTTP session off replica  *)
(* A to replica B (core/session agent_move.go, agent_rebalance.go,          *)
(* dispatcher_session_watch.go; the agent's half: github.com/nfltr/relay    *)
(* client/agent move.go, connect.go, run.go). spec/README.md "Move.tla".    *)
(*                                                                          *)
(* A request reaches the agent's stream on A in two steps: the ingress      *)
(* picks A (frontend/http dispatcherURL: the agent is in A's Agents) and    *)
(* later publishes its first message to the agent's topic (proxy.go; the    *)
(* first chunk of the body is read in between). A peer replica's ingress    *)
(* reads the record instead and forwards the request over the replica      *)
(* link, where it arrives later (PickRemote, ArriveA). The topic's consumer *)
(* (relayRequestsToAgent) sends it on the stream. Lease = TRUE is the       *)
(* design: the pick, or the forwarded request's arrival on A, takes a route *)
(* lease on the stream, released once the first message is on the          *)
(* consumer's channel; MOVE_DONE goes out in band, after the routes         *)
(* stopped, every lease ended and the channel drained. Lease = FALSE is the *)
(* code before it: MOVE_DONE a settle timer after the routes stopped (any   *)
(* time, here). The agent's TCP stream on A beside it (TCPSupersede,        *)
(* PlaceTCP) and a drain's bound (StartDrain, DrainBound) are modelled; the *)
(* agent link's sessions are not.                                           *)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Req,     \* the visitors' requests
          TSess,   \* client TCP sessions to the agent
          Lease,   \* the route lease and the in-band MOVE_DONE (the design)
          Drain,   \* the replica may start draining during the move
          Mutant   \* "none", or a bug put back (spec/README.md)

VARIABLES
    rel,     \* A's watch of the old session: routing, asked, gaveUp, stopped, done, ended
    routes,  \* A's Agents holds the old stream (IsAgentConnected)
    conn,    \* A received MOVE_CONNECTED
    rec,     \* the directory's record: "A" or "B"
    ag,      \* the agent: old, dialing, failed, both, closing, closed
    rq,      \* request -> new, picked, remote, fwd, onA, agentA, atB, answered, failed, gone
    exec,    \* requests the agent ran
    infl,    \* requests the agent received on the old stream, not answered
    cq,      \* the topic consumer's channel on A (published, not yet sent)
    chA,     \* the old stream, relay -> agent
    upA,     \* the old stream, agent -> relay
    drn,     \* A drains (stopping): its bound replaces the rebalance's GiveUp
    lg,      \* A let the agent go before it stopped routing (the drain's bound)
    tin,     \* A's TCP primary of the agent is in its rotation (streamFor takes it)
    tsup,    \* A's TCP primary found its record renewal superseded (the agent moved)
    ts,      \* TCP session -> new, onA, onB
    late     \* TCP session -> placed on A's TCP primary after it found the move

vars == <<rel, routes, conn, rec, ag, rq, exec, infl, cq, chA, upA, drn, lg, tin, tsup, ts, late>>
tcpVars == <<tin, tsup, ts, late>>

Init ==
    /\ rel = "routing" /\ routes = TRUE /\ conn = FALSE /\ rec = "A"
    /\ ag = "old" /\ rq = [r \in Req |-> "new"] /\ exec = {} /\ infl = {}
    /\ cq = <<>> /\ chA = <<>> /\ upA = <<>>
    /\ drn = FALSE /\ lg = FALSE
    /\ tin = TRUE /\ tsup = FALSE /\ ts = [s \in TSess |-> "new"] /\ late = [s \in TSess |-> FALSE]

Pending(s) == {s[i] : i \in 1..Len(s)}

\* A's session ends (the agent closed the old stream, or A ended it):
\* every request it holds is answered agent-gone (failInflight,
\* failQueued), and a later publish finds no consumer.
EndA ==
    /\ rel' = "ended" /\ routes' = FALSE
    /\ rq' = [r \in Req |-> IF rq[r] \in {"onA", "agentA"} THEN "failed" ELSE rq[r]]
    /\ cq' = <<>> /\ chA' = <<>>

(* ---------------- the ingress and the topic (frontend/http) ------------- *)

\* dispatcherURL: here while A routes (with a lease in the design), else
\* the replica the record names.
Pick(r) ==
    /\ rq[r] = "new"
    /\ IF routes
       THEN rq' = [rq EXCEPT ![r] = "picked"]
       ELSE IF rec = "B" THEN rq' = [rq EXCEPT ![r] = "atB"]
       ELSE rq' = [rq EXCEPT ![r] = "failed"] \* the record names A, which does not route
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* A peer replica's dispatcherURL: the record names A; the request goes
\* over the replica link.
PickRemote(r) ==
    /\ rq[r] = "new" /\ rec = "A"
    /\ rq' = [rq EXCEPT ![r] = "remote"]
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* The forwarded request reaches A (Ingress.peer): it takes a route lease
\* (RouteHere) as A's own ingress does; refused (A no longer routes to the
\* agent), it is answered 503 Retry-After before it reached any agent, and
\* the visitor sends it again (new). Without the lease (the bug, finding
\* MV-1) it is published whenever it arrives (fwd).
ArriveA(r) ==
    /\ rq[r] = "remote"
    /\ rq' = [rq EXCEPT ![r] = IF Mutant = "peer-no-lease" THEN "fwd"
                                ELSE IF routes THEN "picked" ELSE "new"]
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* The first message published to the agent's topic on A.
Publish(r) ==
    /\ rq[r] \in {"picked", "fwd"}
    /\ IF rel # "ended"
       THEN /\ cq' = Append(cq, r) /\ rq' = [rq EXCEPT ![r] = "onA"]
       ELSE /\ rq' = [rq EXCEPT ![r] = "failed"] /\ UNCHANGED cq \* ErrNoConsumer
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* The lease ends without a message published: the visitor went away
\* before its first chunk, or the request went over the agent link
\* (dispatch.go). It reached no agent here and is no loss (the visitor's
\* own end); without this step a lease that ends so kept MOVE_DONE back
\* forever in the model, where the code sends it (trace validation).
Unlease(r) ==
    /\ rq[r] = "picked" /\ rq' = [rq EXCEPT ![r] = "gone"]
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* relayRequestsToAgent: the consumer's next message onto the stream.
Forward ==
    /\ cq # <<>> /\ rel # "ended"
    /\ chA' = Append(chA, Head(cq)) /\ cq' = Tail(cq)
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, infl, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* B serves what reaches it (its own session; not this model's).
ServeB(r) ==
    /\ rq[r] = "atB" /\ rq' = [rq EXCEPT ![r] = "answered"] /\ exec' = exec \cup {r}
    /\ UNCHANGED <<rel, routes, conn, rec, ag, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars

(* ---------------- replica A (WatchAgentSession) ------------------------- *)

\* startMove: MOVE_RECONNECT; A stops renewing the record.
\* After a rebalancing move gave up, a drain asks again (its stopNow:
\* startMove with the move given up; finding 26).
Ask ==
    /\ \/ rel = "routing"
       \/ rel = "gaveUp" /\ drn
    /\ rel' = "asked"
    /\ chA' = Append(chA, "RECONNECT")
    /\ UNCHANGED <<routes, conn, rec, ag, rq, exec, infl, cq, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* A rebalance's moveBound passed (the giveUp timer, also after
\* MOVE_CONNECTED while the record check has not yet seen B's record):
\* the agent stays, A renews its record again. A drain has no giveUp.
GiveUp ==
    /\ rel = "asked" /\ ~drn /\ rel' = "gaveUp"
    /\ UNCHANGED <<routes, conn, rec, ag, rq, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* stopRouting: the record names the new session; A routes no more.
StopRouting ==
    /\ rel = "asked"
    /\ conn /\ rec = "B"
    /\ rel' = "stopped" /\ routes' = FALSE
    /\ UNCHANGED <<conn, rec, ag, rq, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* A's record renewal (routing, or after giving up): superseded by B's
\* record. The code ended the session (errSessionReplaced: its requests
\* fail); in the design a session A asked to move takes it as the move
\* done.
Refresh ==
    /\ rel = "gaveUp" /\ rec = "B"
    /\ IF Mutant # "supersede-ends"
       THEN /\ rel' = "stopped" /\ routes' = FALSE
            /\ UNCHANGED <<rq, cq, chA>>
       ELSE EndA
    /\ UNCHANGED <<conn, rec, ag, exec, infl, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* MOVE_DONE. The design: in band, by the consumer's goroutine, once no
\* lease is held and the channel is drained. The code: a timer after
\* stopRouting, sent past the channel.
Done ==
    /\ rel = "stopped"
    /\ Lease => (/\ \A r \in Req : rq[r] # "picked" \/ Mutant = "done-ignores-leases"
                 /\ cq = <<>> \/ Mutant = "done-overtakes")
    /\ rel' = "done" /\ chA' = Append(chA, "DONE")
    /\ UNCHANGED <<routes, conn, rec, ag, rq, exec, infl, cq, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* The replica starts draining (stopping), at any point of a move.
StartDrain ==
    /\ Drain /\ ~drn /\ drn' = TRUE
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, infl, cq, chA, upA, lg>>
    /\ UNCHANGED tcpVars

\* The drain's moveBound (stopNow): an agent that did not get its new
\* connection to take the record in time is let go (errServerRestarting):
\* the requests A holds fail. It is armed from MOVE_RECONNECT (or the
\* drain's start after a rebalance gave up) until A stops routing; once
\* the move is past that, only the relay's own shutdown ends it (finding
\* MV-2: the bound stayed armed and cut a settling move's requests).
DrainBound ==
    /\ drn
    /\ \/ rel \in {"asked", "gaveUp"}
       \/ rel \in {"stopped", "done"} /\ Mutant = "drain-bound-after-stop"
    /\ EndA
    /\ lg' = (rel \in {"asked", "gaveUp"})
    /\ UNCHANGED <<conn, rec, ag, exec, infl, upA, drn>>
    /\ UNCHANGED tcpVars

\* A receives from the old stream.
RecvA ==
    /\ upA # <<>> /\ rel # "ended" /\ upA' = Tail(upA)
    /\ CASE Head(upA) = "CONNECTED" ->
                /\ conn' = TRUE /\ UNCHANGED <<rel, routes, rq, cq, chA>>
         [] Head(upA) = "EOF" -> EndA /\ UNCHANGED conn
         [] OTHER ->
                /\ rq' = [rq EXCEPT ![Head(upA)] = "answered"]
                /\ UNCHANGED <<rel, routes, conn, cq, chA>>
    /\ UNCHANGED <<rec, ag, exec, infl, drn, lg>>
    /\ UNCHANGED tcpVars

(* ---------------- the agent's TCP stream on A --------------------------- *)

\* A TCP client session: streamFor takes A's primary while it is in the
\* rotation, else the replica the record names.
PlaceTCP(s) ==
    /\ ts[s] = "new"
    /\ IF tin THEN /\ ts' = [ts EXCEPT ![s] = "onA"]
                   /\ late' = [late EXCEPT ![s] = tsup]
              ELSE /\ ts' = [ts EXCEPT ![s] = "onB"] /\ UNCHANGED late
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, infl, cq, chA, upA, drn, lg, tin, tsup>>

\* sendLoop's renewal of A's TCP primary finds B's record: the agent moved
\* (movingAway), so it closes this stream with its old connection. The
\* stream leaves the rotation (yield), its open sessions run on; the bug
\* (finding MV-4) left it in rotation, so new sessions kept landing on a
\* stream the agent is about to close.
TCPSupersede ==
    /\ rec = "B" /\ ~tsup
    /\ tsup' = TRUE
    /\ tin' = (Mutant = "tcp-stays-in-rotation" /\ tin)
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, infl, cq, chA, upA, drn, lg, ts, late>>

(* ---------------- the agent (connect.go, run.go) ------------------------ *)

RecvAgent ==
    /\ chA # <<>> /\ chA' = Tail(chA)
    /\ LET m == Head(chA) IN
       CASE m = "RECONNECT" ->
                /\ ag' = IF ag = "old" THEN "dialing" ELSE ag
                /\ UNCHANGED <<rq, exec, infl>>
         [] m = "DONE" ->
                /\ ag' = IF ag = "both" THEN "closing" ELSE ag
                /\ UNCHANGED <<rq, exec, infl>>
         [] OTHER ->
                \* run even after CloseSend: its answer can no longer go out
                /\ exec' = exec \cup {m} /\ rq' = [rq EXCEPT ![m] = "agentA"]
                /\ infl' = IF ag = "closed" THEN infl ELSE infl \cup {m}
                /\ UNCHANGED ag
    /\ UNCHANGED <<rel, routes, conn, rec, cq, upA, drn, lg>>
    /\ UNCHANGED tcpVars

\* moveConnect: B accepts the new connection, its session takes the
\* record; MOVE_CONNECTED on the old stream.
ConnectB ==
    /\ ag = "dialing" /\ ag' = "both" /\ rec' = "B"
    /\ upA' = Append(upA, "CONNECTED")
    /\ UNCHANGED <<rel, routes, conn, rq, exec, infl, cq, chA, drn, lg>>
    /\ UNCHANGED tcpVars

\* Every try refused: the agent stays on the old connection.
DialFails ==
    /\ ag = "dialing" /\ ag' = "failed"
    /\ UNCHANGED <<rel, routes, conn, rec, rq, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars

Answer(r) ==
    /\ r \in infl /\ infl' = infl \ {r} /\ upA' = Append(upA, r)
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, cq, chA, drn, lg>>
    /\ UNCHANGED tcpVars

\* closeWhenAnswered: CloseSend once every request received is answered.
CloseOld ==
    /\ ag = "closing" /\ (infl = {} \/ Mutant = "close-early")
    /\ ag' = "closed" /\ upA' = Append(upA, "EOF")
    /\ UNCHANGED <<rel, routes, conn, rec, rq, exec, infl, cq, chA, drn, lg>>
    /\ UNCHANGED tcpVars

Next ==
    \/ \E r \in Req : Pick(r) \/ PickRemote(r) \/ ArriveA(r) \/ Publish(r) \/ Unlease(r) \/ ServeB(r) \/ Answer(r)
    \/ Forward \/ Ask \/ GiveUp \/ StopRouting \/ Refresh \/ Done \/ RecvA
    \/ StartDrain \/ DrainBound \/ TCPSupersede \/ \E s \in TSess : PlaceTCP(s)
    \/ RecvAgent \/ ConnectB \/ DialFails \/ CloseOld

Spec == Init /\ [][Next]_vars

\* Every step that stays enabled is taken (MoveLive.cfg); GiveUp,
\* StartDrain and DrainBound are timers and events, not duties.
Fairness ==
    /\ \A r \in Req : WF_vars(Pick(r)) /\ WF_vars(ArriveA(r)) /\ WF_vars(Publish(r)) /\ WF_vars(ServeB(r)) /\ WF_vars(Answer(r))
    /\ WF_vars(Forward) /\ WF_vars(StopRouting) /\ WF_vars(Refresh) /\ WF_vars(Done)
    /\ WF_vars(RecvA) /\ WF_vars(RecvAgent) /\ WF_vars(ConnectB) /\ WF_vars(CloseOld)

LiveSpec == Spec /\ Fairness

(* ---------------- properties -------------------------------------------- *)

TypeOK ==
    /\ rel \in {"routing", "asked", "gaveUp", "stopped", "done", "ended"}
    /\ rec \in {"A", "B"}
    /\ ag \in {"old", "dialing", "failed", "both", "closing", "closed"}
    /\ \A r \in Req : rq[r] \in {"new", "picked", "remote", "fwd", "onA", "agentA", "atB", "answered", "failed", "gone"}
    /\ \A s \in TSess : ts[s] \in {"new", "onA", "onB"}

\* The agent stays connected throughout, so no request fails; unless a
\* drain let it go before its new connection took the record (lg).
NoLoss == \A r \in Req : rq[r] # "failed" \/ lg

\* No request runs on the agent without its answer reaching the visitor
\* (a visitor retrying a failed one would run it twice); the same bound.
RunOnce == \A r \in exec : rq[r] # "failed" \/ lg

\* Once A stopped routing, the record never names it again.
OneOwner == rel \in {"stopped", "done"} => rec = "B" /\ ~routes

\* No TCP session is placed on A's old primary once it found the agent's
\* record moved (the agent closes that stream: finding MV-4).
NoTCPAfterMove == \A s \in TSess : ts[s] = "onA" => ~late[s]

\* Every request is answered, and A's old session ends once the agent
\* moved (liveness: MoveLive.cfg, without the drain's bound).
AllAnswered == <>(\A r \in Req : rq[r] \in {"answered", "gone"})
OldEnds == (ag = "both") ~> (rel = "ended")

\* Witnesses (make spec-mutants): each must be violated, so the states the
\* invariant named beside it guards are reached.
WitnessHeld == ~(rel = "stopped" /\ \E r \in Req : rq[r] \in {"picked", "onA", "agentA"}) \* NoLoss, RunOnce
WitnessLetGo == ~(lg /\ \E r \in Req : rq[r] = "failed")                                  \* NoLoss: the bounded loss
WitnessRemote == ~(rel \in {"stopped", "done"} /\ \E r \in Req : rq[r] = "remote")         \* NoLoss (MV-1)
WitnessOwner == ~(rel = "done")                                                            \* OneOwner
WitnessTCP == ~(\E s \in TSess : ts[s] = "onA" /\ rec = "B")                               \* NoTCPAfterMove
=============================================================================
