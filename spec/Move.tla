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
          FailBOn, \* B's new session may end after it took the record (MV-5)
          Mutant   \* "none", or a bug put back (spec/README.md)

VARIABLES
    rel,     \* A's watch of the old session: routing, asked, gaveUp, stopped, done, ended
    routes,  \* A's Agents holds the old stream (IsAgentConnected)
    conn,    \* A received MOVE_CONNECTED
    rec,     \* the directory's record: "A", "B", or "Bgone" (it names B's
             \* session, which ended: MV-5; until its TTL)
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
    late,    \* TCP session -> placed on A's TCP primary after it found the move
    bR,      \* B routes the agent: its new session registered (Agents), after its claim
    ref      \* requests B refused while the record named B's live session

vars == <<rel, routes, conn, rec, ag, rq, exec, infl, cq, chA, upA, drn, lg, tin, tsup, ts, late, bR, ref>>
tcpVars == <<tin, tsup, ts, late>>
bVars == <<bR, ref>>

Init ==
    /\ rel = "routing" /\ routes = TRUE /\ conn = FALSE /\ rec = "A"
    /\ ag = "old" /\ rq = [r \in Req |-> "new"] /\ exec = {} /\ infl = {}
    /\ cq = <<>> /\ chA = <<>> /\ upA = <<>>
    /\ drn = FALSE /\ lg = FALSE
    /\ tin = TRUE /\ tsup = FALSE /\ ts = [s \in TSess |-> "new"] /\ late = [s \in TSess |-> FALSE]
    /\ bR = FALSE /\ ref = {}

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
       ELSE IF rec = "B" THEN rq' = [rq EXCEPT ![r] = "toB"] \* forwarded to B (ArriveB)
       ELSE IF rec = "Bgone" THEN UNCHANGED rq \* B answers 503 Retry-After: no agent ran it
       ELSE rq' = [rq EXCEPT ![r] = "failed"] \* the record names A, which does not route
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* A peer replica's dispatcherURL: the record names A; the request goes
\* over the replica link.
PickRemote(r) ==
    /\ rq[r] = "new" /\ rec = "A"
    /\ rq' = [rq EXCEPT ![r] = "remote"]
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

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
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* The first message published to the agent's topic on A.
Publish(r) ==
    /\ rq[r] \in {"picked", "fwd"}
    /\ IF rel # "ended"
       THEN /\ cq' = Append(cq, r) /\ rq' = [rq EXCEPT ![r] = "onA"]
       ELSE /\ rq' = [rq EXCEPT ![r] = "failed"] /\ UNCHANGED cq \* ErrNoConsumer
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* The lease ends without a message published: the visitor went away
\* before its first chunk, or the request went over the agent link
\* (dispatch.go). It reached no agent here and is no loss (the visitor's
\* own end); without this step a lease that ends so kept MOVE_DONE back
\* forever in the model, where the code sends it (trace validation).
Unlease(r) ==
    /\ rq[r] = "picked" /\ rq' = [rq EXCEPT ![r] = "gone"]
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* relayRequestsToAgent: the consumer's next message onto the stream.
Forward ==
    /\ cq # <<>> /\ rel # "ended"
    /\ chA' = Append(chA, Head(cq)) /\ cq' = Tail(cq)
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, infl, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* B serves what reaches it (its own session; not this model's).
ServeB(r) ==
    /\ rq[r] = "atB" /\ rq' = [rq EXCEPT ![r] = "answered"] /\ exec' = exec \cup {r}
    /\ UNCHANGED <<rel, routes, conn, rec, ag, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

(* ---------------- replica B: the new session's arrival ----------------- *)
(* B's session claims the record (ConnectB) before it registers in B's      *)
(* Agents (RegisterB: dispatcher_subscribe.go, the topic and the consumer   *)
(* first). A request at B in between finds no route but a record naming B  *)
(* itself (GetIP: the loopback address). The design (RouteArriving): it     *)
(* waits for the registration, which follows the claim within the wait's   *)
(* bound (agentStreamWait), or until B's session ended (FailB: 503, retry). *)
(* The bug (finding DF-1, "no-arrival-wait"): B's ingress forwarded to its  *)
(* own loopback address over the replica link and a peer's forward was      *)
(* refused, both while the record named B's live session.                  *)

\* B's ingress (its dispatcherURL): here when B routes the agent, else the
\* record: A (forwarded over the replica link) or B itself (ArriveB).
PickB(r) ==
    /\ rq[r] = "new" /\ (bR \/ rec \in {"A", "B"})
    /\ rq' = [rq EXCEPT ![r] = IF bR THEN "atB" ELSE IF rec = "A" THEN "remote" ELSE "toB"]
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* A request for B at B (its own ingress, or a peer's forward: Ingress.peer):
\* served once B routes the agent; while the record names B's live session
\* the design waits (not enabled), the bug refuses it; B's session ended
\* (Bgone): 503 Retry-After before any agent ran it.
ArriveB(r) ==
    /\ rq[r] = "toB"
    /\ \/ bR /\ rq' = [rq EXCEPT ![r] = "atB"] /\ UNCHANGED ref
       \/ ~bR /\ rec = "B" /\ Mutant = "no-arrival-wait"
          /\ rq' = [rq EXCEPT ![r] = "new"] /\ ref' = ref \cup {r}
       \/ ~bR /\ rec # "B" /\ rq' = [rq EXCEPT ![r] = "new"] /\ UNCHANGED ref
    /\ UNCHANGED <<rel, routes, conn, rec, ag, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bR

\* B's new session registers (d.Agents.Store): B routes the agent.
RegisterB ==
    /\ rec = "B" /\ ~bR /\ bR' = TRUE
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED ref

(* ---------------- replica A (WatchAgentSession) ------------------------- *)

\* startMove: MOVE_RECONNECT; A stops renewing the record.
\* After a rebalancing move gave up, a drain asks again (its stopNow:
\* startMove with the move given up; finding 32).
Ask ==
    /\ \/ rel = "routing"
       \/ rel = "gaveUp" /\ drn
    /\ rel' = "asked"
    /\ chA' = Append(chA, "RECONNECT")
    /\ UNCHANGED <<routes, conn, rec, ag, rq, exec, infl, cq, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* A rebalance's moveBound passed (the giveUp timer, also after
\* MOVE_CONNECTED while the record check has not yet seen B's record):
\* the agent stays, A renews its record again. A drain has no giveUp.
GiveUp ==
    /\ rel = "asked" /\ ~drn /\ rel' = "gaveUp"
    /\ UNCHANGED <<routes, conn, rec, ag, rq, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* stopRouting: the record names the new session; A routes no more.
StopRouting ==
    /\ rel = "asked"
    /\ conn /\ rec # "A"
    /\ rel' = "stopped" /\ routes' = FALSE
    /\ UNCHANGED <<conn, rec, ag, rq, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* A's record renewal (routing, or after giving up): superseded by B's
\* record. The code ended the session (errSessionReplaced: its requests
\* fail); in the design a session A asked to move takes it as the move
\* done.
Refresh ==
    /\ rel = "gaveUp" /\ rec # "A"
    /\ IF Mutant # "supersede-ends"
       THEN /\ rel' = "stopped" /\ routes' = FALSE
            /\ UNCHANGED <<rq, cq, chA>>
       ELSE EndA
    /\ UNCHANGED <<conn, rec, ag, exec, infl, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* MOVE_DONE. The design: in band, by the consumer's goroutine, once no
\* lease is held and the channel is drained. The code: a timer after
\* stopRouting, sent past the channel.
Done ==
    /\ rel = "stopped"
    /\ Lease => (/\ \A r \in Req : rq[r] # "picked" \/ Mutant = "done-ignores-leases"
                 /\ cq = <<>> \/ Mutant = "done-overtakes")
    /\ rel' = "done" /\ chA' = Append(chA, "DONE")
    /\ UNCHANGED <<routes, conn, rec, ag, rq, exec, infl, cq, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* The replica starts draining (stopping), at any point of a move.
StartDrain ==
    /\ Drain /\ ~drn /\ drn' = TRUE
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, infl, cq, chA, upA, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

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
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

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
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

(* ---------------- the agent's TCP stream on A --------------------------- *)

\* A TCP client session: streamFor takes A's primary while it is in the
\* rotation, else the replica the record names.
PlaceTCP(s) ==
    /\ ts[s] = "new"
    /\ IF tin THEN /\ ts' = [ts EXCEPT ![s] = "onA"]
                   /\ late' = [late EXCEPT ![s] = tsup]
              ELSE /\ ts' = [ts EXCEPT ![s] = "onB"] /\ UNCHANGED late
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, infl, cq, chA, upA, drn, lg, tin, tsup>>
    /\ UNCHANGED bVars

\* sendLoop's renewal of A's TCP primary finds B's record: the agent moved
\* (movingAway), so it closes this stream with its old connection. The
\* stream leaves the rotation (yield), its open sessions run on; the bug
\* (finding MV-4) left it in rotation, so new sessions kept landing on a
\* stream the agent is about to close.
TCPSupersede ==
    /\ rec # "A" /\ ~tsup
    /\ tsup' = TRUE
    /\ tin' = (Mutant = "tcp-stays-in-rotation" /\ tin)
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, infl, cq, chA, upA, drn, lg, ts, late>>
    /\ UNCHANGED bVars

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
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* moveConnect: B accepts the new connection, its session takes the
\* record; MOVE_CONNECTED on the old stream.
ConnectB ==
    /\ ag = "dialing" /\ ag' = "both" /\ rec' = "B"
    /\ upA' = Append(upA, "CONNECTED")
    /\ UNCHANGED <<rel, routes, conn, rq, exec, infl, cq, chA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* B's new session ends after it took the record (MV-5: B crashed, or
\* the new connection broke): the record names that ended session until
\* its TTL. A has stopped renewing and refuses the agent, B answers 503
\* Retry-After before any agent ran a request: unavailable until the
\* agent's next connection takes the record, never a loss. That
\* reconnection is a new session, outside this model.
FailB ==
    /\ FailBOn /\ rec = "B" /\ rec' = "Bgone" /\ bR' = FALSE
    /\ UNCHANGED <<rel, routes, conn, ag, rq, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED ref

\* Every try refused: the agent stays on the old connection.
DialFails ==
    /\ ag = "dialing" /\ ag' = "failed"
    /\ UNCHANGED <<rel, routes, conn, rec, rq, exec, infl, cq, chA, upA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

Answer(r) ==
    /\ r \in infl /\ infl' = infl \ {r} /\ upA' = Append(upA, r)
    /\ UNCHANGED <<rel, routes, conn, rec, ag, rq, exec, cq, chA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

\* closeWhenAnswered: CloseSend once every request received is answered.
CloseOld ==
    /\ ag = "closing" /\ (infl = {} \/ Mutant = "close-early")
    /\ ag' = "closed" /\ upA' = Append(upA, "EOF")
    /\ UNCHANGED <<rel, routes, conn, rec, rq, exec, infl, cq, chA, drn, lg>>
    /\ UNCHANGED tcpVars /\ UNCHANGED bVars

Next ==
    \/ \E r \in Req : Pick(r) \/ PickRemote(r) \/ ArriveA(r) \/ Publish(r) \/ Unlease(r) \/ ServeB(r) \/ Answer(r)
    \/ Forward \/ Ask \/ GiveUp \/ StopRouting \/ Refresh \/ Done \/ RecvA
    \/ StartDrain \/ DrainBound \/ TCPSupersede \/ \E s \in TSess : PlaceTCP(s)
    \/ RecvAgent \/ ConnectB \/ DialFails \/ CloseOld \/ FailB
    \/ RegisterB \/ \E r \in Req : PickB(r) \/ ArriveB(r)

Spec == Init /\ [][Next]_vars

\* Every step that stays enabled is taken (MoveLive.cfg); GiveUp,
\* StartDrain and DrainBound are timers and events, not duties.
Fairness ==
    /\ \A r \in Req : WF_vars(Pick(r)) /\ WF_vars(ArriveA(r)) /\ WF_vars(Publish(r)) /\ WF_vars(ServeB(r)) /\ WF_vars(Answer(r))
    /\ WF_vars(Forward) /\ WF_vars(StopRouting) /\ WF_vars(Refresh) /\ WF_vars(Done)
    /\ WF_vars(RecvA) /\ WF_vars(RecvAgent) /\ WF_vars(ConnectB) /\ WF_vars(CloseOld)
    /\ WF_vars(RegisterB) /\ \A r \in Req : WF_vars(ArriveB(r))

LiveSpec == Spec /\ Fairness

(* ---------------- properties -------------------------------------------- *)

TypeOK ==
    /\ rel \in {"routing", "asked", "gaveUp", "stopped", "done", "ended"}
    /\ rec \in {"A", "B", "Bgone"}
    /\ ag \in {"old", "dialing", "failed", "both", "closing", "closed"}
    /\ \A r \in Req : rq[r] \in {"new", "picked", "remote", "fwd", "onA", "agentA", "toB", "atB", "answered", "failed", "gone"}
    /\ \A s \in TSess : ts[s] \in {"new", "onA", "onB"}
    /\ bR \in BOOLEAN /\ ref \subseteq Req

\* The agent stays connected throughout, so no request fails; unless a
\* drain let it go before its new connection took the record (lg).
NoLoss == \A r \in Req : rq[r] # "failed" \/ lg

\* No request runs on the agent without its answer reaching the visitor
\* (a visitor retrying a failed one would run it twice); the same bound.
RunOnce == \A r \in exec : rq[r] # "failed" \/ lg

\* Once A stopped routing, the record never names it again.
OneOwner == rel \in {"stopped", "done"} => rec # "A" /\ ~routes

\* No TCP session is placed on A's old primary once it found the agent's
\* record moved (the agent closes that stream: finding MV-4).
NoTCPAfterMove == \A s \in TSess : ts[s] = "onA" => ~late[s]

\* The replica the record names never refuses a request while that
\* session lives: it serves it, or waits for the session's registration
\* (finding DF-1). A refusal is only ever a stale read's (ArriveA), or
\* an ended session's (Bgone).
OwnerServes == ref = {}

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
WitnessArriving == ~(\E r \in Req : rq[r] = "toB" /\ rec = "B" /\ ~bR)                    \* OwnerServes (DF-1)
WitnessBGone == ~(rel \in {"stopped", "done"} /\ rec = "Bgone")                            \* OneOwner, NoLoss (MV-5)
=============================================================================
