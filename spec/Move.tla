------------------------------- MODULE Move -------------------------------
(* A make-before-break move of one agent's ConnectHTTP session off replica  *)
(* A to replica B (core/session agent_move.go, agent_rebalance.go,          *)
(* dispatcher_session_watch.go; the agent's half: github.com/nfltr/relay    *)
(* client/agent move.go, connect.go, run.go). spec/README.md "Move.tla".    *)
(*                                                                          *)
(* A request reaches the agent's stream on A in two steps: the ingress      *)
(* picks A (frontend/http dispatcherURL: the agent is in A's Agents) and    *)
(* later publishes its first message to the agent's topic (proxy.go; the    *)
(* first chunk of the body is read in between). The topic's consumer        *)
(* (relayRequestsToAgent) sends it on the stream. Lease = TRUE is the       *)
(* design: the pick takes a route lease on the stream, released once the   *)
(* first message is on the consumer's channel; MOVE_DONE goes out in band,  *)
(* after the routes stopped, every lease ended and the channel drained.     *)
(* Lease = FALSE is the code before it: MOVE_DONE a settle timer after the  *)
(* routes stopped (any time, here).                                         *)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Req,     \* the visitors' requests
          Lease,   \* the route lease and the in-band MOVE_DONE (the design)
          Mutant   \* "none", or a bug put back (spec/README.md)

VARIABLES
    rel,     \* A's watch of the old session: routing, asked, gaveUp, stopped, done, ended
    routes,  \* A's Agents holds the old stream (IsAgentConnected)
    conn,    \* A received MOVE_CONNECTED
    rec,     \* the directory's record: "A" or "B"
    ag,      \* the agent: old, dialing, failed, both, closing, closed
    rq,      \* request -> new, picked, onA, agentA, atB, answered, failed
    exec,    \* requests the agent ran
    infl,    \* requests the agent received on the old stream, not answered
    cq,      \* the topic consumer's channel on A (published, not yet sent)
    chA,     \* the old stream, relay -> agent
    upA      \* the old stream, agent -> relay

vars == <<rel, routes, conn, rec, ag, rq, exec, infl, cq, chA, upA>>

Init ==
    /\ rel = "routing" /\ routes = TRUE /\ conn = FALSE /\ rec = "A"
    /\ ag = "old" /\ rq = [r \in Req |-> "new"] /\ exec = {} /\ infl = {}
    /\ cq = <<>> /\ chA = <<>> /\ upA = <<>>

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
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, cq, chA, upA>>

\* The first message published to the agent's topic on A.
Publish(r) ==
    /\ rq[r] = "picked"
    /\ IF rel # "ended"
       THEN /\ cq' = Append(cq, r) /\ rq' = [rq EXCEPT ![r] = "onA"]
       ELSE /\ rq' = [rq EXCEPT ![r] = "failed"] /\ UNCHANGED cq \* ErrNoConsumer
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, chA, upA>>

\* relayRequestsToAgent: the consumer's next message onto the stream.
Forward ==
    /\ cq # <<>> /\ rel # "ended"
    /\ chA' = Append(chA, Head(cq)) /\ cq' = Tail(cq)
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, infl, upA>>

\* B serves what reaches it (its own session; not this model's).
ServeB(r) ==
    /\ rq[r] = "atB" /\ rq' = [rq EXCEPT ![r] = "answered"] /\ exec' = exec \cup {r}
    /\ UNCHANGED <<rel, routes, conn, rec, ag, infl, cq, chA, upA>>

(* ---------------- replica A (WatchAgentSession) ------------------------- *)

\* startMove: MOVE_RECONNECT; A stops renewing the record.
Ask ==
    /\ rel = "routing" /\ rel' = "asked"
    /\ chA' = Append(chA, "RECONNECT")
    /\ UNCHANGED <<routes, conn, rec, ag, rq, exec, infl, cq, upA>>

\* moveBound passed without MOVE_CONNECTED: a rebalancing move gives up;
\* the agent stays, A renews its record again.
GiveUp ==
    /\ rel = "asked" /\ ~conn /\ rel' = "gaveUp"
    /\ UNCHANGED <<routes, conn, rec, ag, rq, exec, infl, cq, chA, upA>>

\* stopRouting: the record names the new session; A routes no more.
StopRouting ==
    /\ rel = "asked"
    /\ conn /\ rec = "B"
    /\ rel' = "stopped" /\ routes' = FALSE
    /\ UNCHANGED <<conn, rec, ag, rq, exec, infl, cq, chA, upA>>

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
    /\ UNCHANGED <<conn, rec, ag, exec, infl, upA>>

\* MOVE_DONE. The design: in band, by the consumer's goroutine, once no
\* lease is held and the channel is drained. The code: a timer after
\* stopRouting, sent past the channel.
Done ==
    /\ rel = "stopped"
    /\ Lease => (/\ \A r \in Req : rq[r] # "picked" \/ Mutant = "done-ignores-leases"
                 /\ cq = <<>> \/ Mutant = "done-overtakes")
    /\ rel' = "done" /\ chA' = Append(chA, "DONE")
    /\ UNCHANGED <<routes, conn, rec, ag, rq, exec, infl, cq, upA>>

\* A receives from the old stream.
RecvA ==
    /\ upA # <<>> /\ rel # "ended" /\ upA' = Tail(upA)
    /\ CASE Head(upA) = "CONNECTED" ->
                /\ conn' = TRUE /\ UNCHANGED <<rel, routes, rq, cq, chA>>
         [] Head(upA) = "EOF" -> EndA /\ UNCHANGED conn
         [] OTHER ->
                /\ rq' = [rq EXCEPT ![Head(upA)] = "answered"]
                /\ UNCHANGED <<rel, routes, conn, cq, chA>>
    /\ UNCHANGED <<rec, ag, exec, infl>>

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
    /\ UNCHANGED <<rel, routes, conn, rec, cq, upA>>

\* moveConnect: B accepts the new connection, its session takes the
\* record; MOVE_CONNECTED on the old stream.
ConnectB ==
    /\ ag = "dialing" /\ ag' = "both" /\ rec' = "B"
    /\ upA' = Append(upA, "CONNECTED")
    /\ UNCHANGED <<rel, routes, conn, rq, exec, infl, cq, chA>>

\* Every try refused: the agent stays on the old connection.
DialFails ==
    /\ ag = "dialing" /\ ag' = "failed"
    /\ UNCHANGED <<rel, routes, conn, rec, rq, exec, infl, cq, chA, upA>>

Answer(r) ==
    /\ r \in infl /\ infl' = infl \ {r} /\ upA' = Append(upA, r)
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, cq, chA>>

\* closeWhenAnswered: CloseSend once every request received is answered.
CloseOld ==
    /\ ag = "closing" /\ (infl = {} \/ Mutant = "close-early")
    /\ ag' = "closed" /\ upA' = Append(upA, "EOF")
    /\ UNCHANGED <<rel, routes, conn, rec, rq, exec, infl, cq, chA>>

Next ==
    \/ \E r \in Req : Pick(r) \/ Publish(r) \/ ServeB(r) \/ Answer(r)
    \/ Forward \/ Ask \/ GiveUp \/ StopRouting \/ Refresh \/ Done \/ RecvA
    \/ RecvAgent \/ ConnectB \/ DialFails \/ CloseOld

Spec == Init /\ [][Next]_vars

\* Every step that stays enabled is taken (MoveLive.cfg); GiveUp is a
\* timer, not a duty.
Fairness ==
    /\ \A r \in Req : WF_vars(Pick(r)) /\ WF_vars(Publish(r)) /\ WF_vars(ServeB(r)) /\ WF_vars(Answer(r))
    /\ WF_vars(Forward) /\ WF_vars(StopRouting) /\ WF_vars(Refresh) /\ WF_vars(Done)
    /\ WF_vars(RecvA) /\ WF_vars(RecvAgent) /\ WF_vars(ConnectB) /\ WF_vars(CloseOld)

LiveSpec == Spec /\ Fairness

(* ---------------- properties -------------------------------------------- *)

TypeOK ==
    /\ rel \in {"routing", "asked", "gaveUp", "stopped", "done", "ended"}
    /\ rec \in {"A", "B"}
    /\ ag \in {"old", "dialing", "failed", "both", "closing", "closed"}
    /\ \A r \in Req : rq[r] \in {"new", "picked", "onA", "agentA", "atB", "answered", "failed"}

\* The agent stays connected throughout, so no request fails.
NoLoss == \A r \in Req : rq[r] # "failed"

\* No request runs on the agent without its answer reaching the visitor
\* (a visitor retrying a failed one would run it twice).
RunOnce == \A r \in exec : rq[r] # "failed"

\* Once A stopped routing, the record never names it again.
OneOwner == rel \in {"stopped", "done"} => rec = "B" /\ ~routes

\* Every request is answered, and A's old session ends once the agent
\* moved (liveness: MoveLive.cfg).
AllAnswered == <>(\A r \in Req : rq[r] = "answered")
OldEnds == (ag = "both") ~> (rel = "ended")
=============================================================================
