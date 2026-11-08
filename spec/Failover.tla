----------------------------- MODULE Failover -----------------------------
(* An agent and its clients with several relays (design, lane            *)
(* RELAYFAILOVER; nfltr/nfltr docs/decisions/relay-failover-2026-11-08.md, *)
(* spec/README.md "Failover.tla"). No code yet: the steps name the Go     *)
(* they will be.                                                          *)
(*                                                                          *)
(* The agent's relay list is three endpoints: "a", an independent relay    *)
(* (cluster A, nothing shared with B), and "b1", "b2", two endpoints of     *)
(* one relay (cluster B: replicas or regions sharing one directory). The   *)
(* agent does not know which endpoint is of which cluster until one        *)
(* answers (the Welcome's relay id). It holds at most one link per         *)
(* cluster: it dials one endpoint at a time and names in its connect the   *)
(* clusters it holds links to; an endpoint of a named cluster refuses the  *)
(* connect before it claims the directory record (ALREADY_ATTACHED). A     *)
(* claim names the newest link (Directory.tla's guarantee: the newest Seq  *)
(* wins, an older session's renewal yields), so a failover inside a        *)
(* cluster needs no handshake with the endpoint the agent left: that       *)
(* endpoint's stale registration ends at its next renewal or liveness      *)
(* timeout.                                                                *)
(*                                                                          *)
(* A client session walks the same list in order: an endpoint unreachable *)
(* before the open was sent, or an answer that proves the open reached no  *)
(* agent (NOT_FOUND), goes on to the next; an open sent and not answered   *)
(* is reported (unknown) and never sent again, since the agent may have    *)
(* run it. After a full walk the session's hold either walks again or      *)
(* ends not_found. One share, issued by cluster B, is served by B's        *)
(* endpoints only (the defined rule: an object belongs to the relay that   *)
(* issued it).                                                             *)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Sess,      \* client sessions
          MaxFaults, \* crashes and agent-side partitions in a behaviour
          Share,     \* the share's visits and revocation are modelled
          Mutant     \* "none", or a bug the design prevents (spec/README.md)

Endpoints == {"a", "b1", "b2"}
Clusters == {"A", "B"}
Cl(e) == IF e = "a" THEN "A" ELSE "B"
EpOf(c) == {e \in Endpoints : Cl(e) = c}
Order == <<"a", "b1", "b2">>  \* the list, for the agent and its clients
N == Len(Order)
ShareCl == "B"                \* the cluster that issued the share

VARIABLES
    link,   \* agent: endpoint -> none, dialing, up
    hdr,    \* agent: endpoint -> the clusters named in its dialing connect
    known,  \* agent: endpoint -> its cluster once learned, "?" before
    refd,   \* agent: endpoint -> it refused a connect as ALREADY_ATTACHED
    alive,  \* endpoint -> the relay process runs (clients reach it)
    cut,    \* endpoint -> the agent's path to it is broken (a partition)
    reg,    \* endpoint -> its local registration of the agent (Agents)
    rec,    \* cluster -> the endpoint its directory record names, or "none"
    faults, \* faults taken
    st,     \* session -> walk, sent, exec, lost, opened, reported
    idx,    \* session -> the list index it tries next (N + 1: walk done)
    tried,  \* session -> the indices refused or unreachable in this walk
    ent,    \* session -> the endpoint its open was sent to
    tgt,    \* session -> the endpoint whose agent link the open went down
    execs,  \* session -> times the agent ran its open
    rep,    \* session -> the reported code: "", not_found, unknown
    revoked,\* the share was revoked (at its cluster's store)
    served, \* endpoints that served a visit of the share
    sar     \* a visit was served after the revocation

agentVars == <<link, hdr, known, refd>>
relayVars == <<alive, cut, reg, rec, faults>>
clientVars == <<st, idx, tried, ent, tgt, execs, rep>>
shareVars == <<revoked, served, sar>>
vars == <<agentVars, relayVars, clientVars, shareVars>>

Init ==
    /\ link = [e \in Endpoints |-> "none"] /\ hdr = [e \in Endpoints |-> {}]
    /\ known = [e \in Endpoints |-> "?"] /\ refd = [e \in Endpoints |-> FALSE]
    /\ alive = [e \in Endpoints |-> TRUE] /\ cut = [e \in Endpoints |-> FALSE]
    /\ reg = [e \in Endpoints |-> FALSE] /\ rec = [c \in Clusters |-> "none"]
    /\ faults = 0
    /\ st = [s \in Sess |-> "walk"] /\ idx = [s \in Sess |-> 1]
    /\ tried = [s \in Sess |-> {}] /\ ent = [s \in Sess |-> "none"]
    /\ tgt = [s \in Sess |-> "none"] /\ execs = [s \in Sess |-> 0]
    /\ rep = [s \in Sess |-> ""]
    /\ revoked = FALSE /\ served = {} /\ sar = FALSE

\* The clusters the agent holds a link to (it knows an up link's cluster
\* from its Welcome).
Held == {known[e] : e \in {f \in Endpoints : link[f] = "up"}}

\* The agent's link to e carries what the relay sends down it (the relay
\* still holds the session).
Delivers(e) == link[e] = "up" /\ alive[e] /\ ~cut[e] /\ reg[e]

\* Where endpoint e sends an open for the agent: its own registration,
\* else the endpoint its cluster's record names, over the replica link.
Route(e) ==
    IF reg[e] THEN e
    ELSE IF rec[Cl(e)] # "none" /\ alive[rec[Cl(e)]] /\ reg[rec[Cl(e)]] THEN rec[Cl(e)]
    ELSE "none"

(* ---------------- the agent (relay client/agent, planned) -------------- *)

\* dialRelays: the next endpoint without a link. Skipped while it is known
\* to be of a cluster the agent holds; one connect at a time (the bug,
\* "parallel-connect": every endpoint dialled at once, each connect naming
\* only the clusters already up). "stale-alt" (a lost handover): an
\* endpoint that once refused as ALREADY_ATTACHED is never dialled again,
\* even after its cluster's link ended.
Dial(e) ==
    /\ link[e] = "none" /\ alive[e] /\ ~cut[e]
    /\ IF Mutant = "stale-alt" THEN ~refd[e] /\ known[e] \notin Held ELSE known[e] \notin Held
    /\ (\A f \in Endpoints : link[f] # "dialing") \/ Mutant = "parallel-connect"
    /\ link' = [link EXCEPT ![e] = "dialing"]
    /\ hdr' = [hdr EXCEPT ![e] = Held]
    /\ UNCHANGED <<known, refd>> /\ UNCHANGED relayVars /\ UNCHANGED clientVars /\ UNCHANGED shareVars

\* The connect never answered: the endpoint died or the path broke.
DialFails(e) ==
    /\ link[e] = "dialing" /\ (~alive[e] \/ cut[e])
    /\ link' = [link EXCEPT ![e] = "none"] /\ hdr' = [hdr EXCEPT ![e] = {}]
    /\ UNCHANGED <<known, refd>> /\ UNCHANGED relayVars /\ UNCHANGED clientVars /\ UNCHANGED shareVars

\* The agent's liveness (link Liveness presets) finds an up link dead, or
\* the relay's end of the session reaches it: it closes its side, and
\* what the relay sends down it later is never run.
Detect(e) ==
    /\ link[e] = "up" /\ (~alive[e] \/ cut[e] \/ ~reg[e])
    /\ link' = [link EXCEPT ![e] = "none"]
    /\ UNCHANGED <<hdr, known, refd>> /\ UNCHANGED relayVars /\ UNCHANGED clientVars /\ UNCHANGED shareVars

(* ---------------- the endpoint (relay-core core/session, planned) ------ *)

\* The endpoint answers the connect. Its cluster named in the connect: it
\* refuses ALREADY_ATTACHED before it claims anything (the bug,
\* "claim-before-dedupe": the claim first, superseding the agent's live
\* link's record). Otherwise it claims the record (newest wins) and
\* registers the agent; the Welcome tells the agent its relay id.
Accept(e) ==
    /\ link[e] = "dialing" /\ alive[e] /\ ~cut[e]
    /\ known' = [known EXCEPT ![e] = Cl(e)]
    /\ IF Cl(e) \in hdr[e]
       THEN /\ link' = [link EXCEPT ![e] = "none"]
            /\ refd' = [refd EXCEPT ![e] = TRUE]
            /\ rec' = IF Mutant = "claim-before-dedupe" THEN [rec EXCEPT ![Cl(e)] = e] ELSE rec
            /\ UNCHANGED reg
       ELSE /\ link' = [link EXCEPT ![e] = "up"]
            /\ rec' = [rec EXCEPT ![Cl(e)] = e]
            /\ reg' = [reg EXCEPT ![e] = TRUE]
            /\ UNCHANGED refd
    /\ hdr' = [hdr EXCEPT ![e] = {}]
    /\ UNCHANGED <<alive, cut, faults>> /\ UNCHANGED clientVars /\ UNCHANGED shareVars

\* A stale registration ends: the agent left the link, or the path to it
\* broke (either side's liveness timeout may pass first),
\* or the record names a newer link (the renewal yields: ErrSuperseded).
\* The record goes with it only while it names this endpoint. The bug
\* ("renew-overwrites", a lost handover): the superseded renewal writes
\* the record back to itself.
Notice(e) ==
    /\ reg[e] /\ (link[e] # "up" \/ cut[e] \/ rec[Cl(e)] # e)
    /\ IF Mutant = "renew-overwrites" /\ rec[Cl(e)] # e
       THEN rec' = [rec EXCEPT ![Cl(e)] = e] /\ UNCHANGED reg
       ELSE /\ reg' = [reg EXCEPT ![e] = FALSE]
            /\ rec' = IF rec[Cl(e)] = e THEN [rec EXCEPT ![Cl(e)] = "none"] ELSE rec
    /\ UNCHANGED <<alive, cut, faults>> /\ UNCHANGED agentVars /\ UNCHANGED clientVars /\ UNCHANGED shareVars

\* A crashed endpoint's record passes its TTL.
Lapse(c) ==
    /\ rec[c] # "none" /\ ~alive[rec[c]]
    /\ rec' = [rec EXCEPT ![c] = "none"]
    /\ UNCHANGED <<alive, cut, reg, faults>> /\ UNCHANGED agentVars /\ UNCHANGED clientVars /\ UNCHANGED shareVars

(* ---------------- faults ----------------------------------------------- *)

Crash(e) ==
    /\ alive[e] /\ faults < MaxFaults
    /\ alive' = [alive EXCEPT ![e] = FALSE] /\ reg' = [reg EXCEPT ![e] = FALSE]
    /\ faults' = faults + 1
    /\ UNCHANGED <<cut, rec>> /\ UNCHANGED agentVars /\ UNCHANGED clientVars /\ UNCHANGED shareVars

Restart(e) ==
    /\ ~alive[e] /\ alive' = [alive EXCEPT ![e] = TRUE]
    /\ UNCHANGED <<cut, reg, rec, faults>> /\ UNCHANGED agentVars /\ UNCHANGED clientVars /\ UNCHANGED shareVars

\* The agent's path to e breaks; clients still reach e.
Cut(e) ==
    /\ link[e] = "up" /\ ~cut[e] /\ faults < MaxFaults
    /\ cut' = [cut EXCEPT ![e] = TRUE] /\ faults' = faults + 1
    /\ UNCHANGED <<alive, reg, rec>> /\ UNCHANGED agentVars /\ UNCHANGED clientVars /\ UNCHANGED shareVars

Heal(e) ==
    /\ cut[e] /\ cut' = [cut EXCEPT ![e] = FALSE]
    /\ UNCHANGED <<alive, reg, rec, faults>> /\ UNCHANGED agentVars /\ UNCHANGED clientVars /\ UNCHANGED shareVars

(* ---------------- a client session (nfltr dial, connect:, planned) ----- *)

\* The endpoint at idx is down: nothing was sent; the next one.
Skip(s) ==
    /\ st[s] = "walk" /\ idx[s] <= N /\ ~alive[Order[idx[s]]]
    /\ IF Mutant = "first-only"
       THEN st' = [st EXCEPT ![s] = "reported"] /\ rep' = [rep EXCEPT ![s] = "not_found"] /\ UNCHANGED <<idx, tried>>
       ELSE idx' = [idx EXCEPT ![s] = @ + 1] /\ tried' = [tried EXCEPT ![s] = @ \cup {idx[s]}] /\ UNCHANGED <<st, rep>>
    /\ UNCHANGED <<ent, tgt, execs>> /\ UNCHANGED agentVars /\ UNCHANGED relayVars /\ UNCHANGED shareVars

Send(s) ==
    /\ st[s] = "walk" /\ idx[s] <= N /\ alive[Order[idx[s]]]
    /\ st' = [st EXCEPT ![s] = "sent"] /\ ent' = [ent EXCEPT ![s] = Order[idx[s]]]
    /\ UNCHANGED <<idx, tried, tgt, execs, rep>> /\ UNCHANGED agentVars /\ UNCHANGED relayVars /\ UNCHANGED shareVars

\* The endpoint handles the open: no route is a typed NOT_FOUND (it
\* reached no agent: the next endpoint); a link that delivers runs it;
\* a stale registration or a dead endpoint swallows it (no answer).
Arrive(s) ==
    /\ st[s] = "sent"
    /\ LET e == ent[s] IN
       IF ~alive[e]
       THEN st' = [st EXCEPT ![s] = "lost"] /\ UNCHANGED <<idx, tried, tgt, execs, rep>>
       ELSE LET t == Route(e) IN
            IF t = "none"
            THEN IF Mutant = "first-only"
                 THEN st' = [st EXCEPT ![s] = "reported"] /\ rep' = [rep EXCEPT ![s] = "not_found"]
                      /\ UNCHANGED <<idx, tried, tgt, execs>>
                 ELSE /\ st' = [st EXCEPT ![s] = "walk"] /\ idx' = [idx EXCEPT ![s] = @ + 1]
                      /\ tried' = [tried EXCEPT ![s] = @ \cup {idx[s]}]
                      /\ UNCHANGED <<tgt, execs, rep>>
            ELSE IF Delivers(t)
            THEN /\ st' = [st EXCEPT ![s] = "exec"] /\ tgt' = [tgt EXCEPT ![s] = t]
                 /\ execs' = [execs EXCEPT ![s] = @ + 1] /\ UNCHANGED <<idx, tried, rep>>
            ELSE st' = [st EXCEPT ![s] = "lost"] /\ tgt' = [tgt EXCEPT ![s] = t] /\ UNCHANGED <<idx, tried, execs, rep>>
    /\ UNCHANGED ent /\ UNCHANGED agentVars /\ UNCHANGED relayVars /\ UNCHANGED shareVars

PathUp(s) == Delivers(tgt[s]) /\ alive[ent[s]]

\* The agent's answer comes back: the session opened.
Answer(s) ==
    /\ st[s] = "exec" /\ PathUp(s)
    /\ st' = [st EXCEPT ![s] = "opened"]
    /\ UNCHANGED <<idx, tried, ent, tgt, execs, rep>> /\ UNCHANGED agentVars /\ UNCHANGED relayVars /\ UNCHANGED shareVars

\* No answer is coming (the open was swallowed, or the path broke after
\* the agent ran it): reported unknown, never sent again. The bug
\* ("retry-ambiguous"): the next endpoint is tried.
Timeout(s) ==
    /\ st[s] = "lost" \/ (st[s] = "exec" /\ ~PathUp(s))
    /\ IF Mutant = "retry-ambiguous"
       THEN /\ st' = [st EXCEPT ![s] = "walk"] /\ idx' = [idx EXCEPT ![s] = @ + 1]
            /\ tried' = [tried EXCEPT ![s] = @ \cup {idx[s]}] /\ UNCHANGED rep
       ELSE st' = [st EXCEPT ![s] = "reported"] /\ rep' = [rep EXCEPT ![s] = "unknown"] /\ UNCHANGED <<idx, tried>>
    /\ UNCHANGED <<ent, tgt, execs>> /\ UNCHANGED agentVars /\ UNCHANGED relayVars /\ UNCHANGED shareVars

\* A full walk refused or unreachable: the hold walks again from the top,
\* or its --open-timeout passed (ends not_found).
Rewalk(s) ==
    /\ st[s] = "walk" /\ idx[s] > N
    /\ idx' = [idx EXCEPT ![s] = 1] /\ tried' = [tried EXCEPT ![s] = {}]
    /\ UNCHANGED <<st, ent, tgt, execs, rep>> /\ UNCHANGED agentVars /\ UNCHANGED relayVars /\ UNCHANGED shareVars

HoldEnds(s) ==
    /\ st[s] = "walk" /\ idx[s] > N
    /\ st' = [st EXCEPT ![s] = "reported"] /\ rep' = [rep EXCEPT ![s] = "not_found"]
    /\ UNCHANGED <<idx, tried, ent, tgt, execs>> /\ UNCHANGED agentVars /\ UNCHANGED relayVars /\ UNCHANGED shareVars

(* ---------------- the share (relay-server shares, existing) ------------ *)

\* A visit of the share at e: served by the endpoints of its cluster, which
\* route it to the agent. The bug ("share-everywhere"): the agent's every
\* relay serves it, each from its own copy; a revocation reaches the
\* issuing cluster's store only.
Visit(e) ==
    /\ Share /\ alive[e]
    /\ Cl(e) = ShareCl \/ Mutant = "share-everywhere"
    /\ Cl(e) = ShareCl => ~revoked
    /\ Route(e) # "none" /\ Delivers(Route(e))
    /\ served' = served \cup {e} /\ sar' = (sar \/ revoked)
    /\ UNCHANGED revoked /\ UNCHANGED agentVars /\ UNCHANGED relayVars /\ UNCHANGED clientVars

Revoke ==
    /\ Share /\ ~revoked /\ revoked' = TRUE
    /\ UNCHANGED <<served, sar>> /\ UNCHANGED agentVars /\ UNCHANGED relayVars /\ UNCHANGED clientVars

Next ==
    \/ \E e \in Endpoints : Dial(e) \/ DialFails(e) \/ Detect(e) \/ Accept(e) \/ Notice(e)
                           \/ Crash(e) \/ Restart(e) \/ Cut(e) \/ Heal(e) \/ Visit(e)
    \/ \E c \in Clusters : Lapse(c)
    \/ \E s \in Sess : Skip(s) \/ Send(s) \/ Arrive(s) \/ Answer(s) \/ Timeout(s) \/ Rewalk(s) \/ HoldEnds(s)
    \/ Revoke

Spec == Init /\ [][Next]_vars

\* The agent's, the endpoints' and the clients' own steps are taken while
\* enabled; faults, restarts, heals, visits, revocation and a hold's end
\* are events, not duties.
Fairness ==
    /\ \A e \in Endpoints : WF_vars(Dial(e)) /\ WF_vars(DialFails(e)) /\ WF_vars(Detect(e))
                            /\ WF_vars(Accept(e)) /\ WF_vars(Notice(e))
    /\ \A c \in Clusters : WF_vars(Lapse(c))
    /\ \A s \in Sess : WF_vars(Skip(s)) /\ WF_vars(Send(s)) /\ WF_vars(Arrive(s))
                       /\ WF_vars(Answer(s)) /\ WF_vars(Timeout(s)) /\ WF_vars(Rewalk(s))

LiveSpec == Spec /\ Fairness

(* ---------------- properties ------------------------------------------- *)

TypeOK ==
    /\ link \in [Endpoints -> {"none", "dialing", "up"}]
    /\ hdr \in [Endpoints -> SUBSET Clusters]
    /\ known \in [Endpoints -> Clusters \cup {"?"}]
    /\ refd \in [Endpoints -> BOOLEAN] /\ alive \in [Endpoints -> BOOLEAN]
    /\ cut \in [Endpoints -> BOOLEAN] /\ reg \in [Endpoints -> BOOLEAN]
    /\ rec \in [Clusters -> Endpoints \cup {"none"}]
    /\ st \in [Sess -> {"walk", "sent", "exec", "lost", "opened", "reported"}]
    /\ idx \in [Sess -> 1..(N + 1)] /\ tried \in [Sess -> SUBSET (1..N)]
    /\ execs \in [Sess -> Nat] /\ rep \in [Sess -> {"", "not_found", "unknown"}]
    /\ served \subseteq Endpoints

\* No split brain: the agent holds at most one link (up or dialing past
\* its connect's check) per cluster.
OneLinkPerCluster ==
    \A c \in Clusters : Cardinality({e \in EpOf(c) : link[e] = "up"}) <= 1

\* The cluster's record names every link of the agent that delivers: no
\* claim moves it to a link the agent does not hold (no lost handover). A
\* dead or cut link's record may lapse before the agent notices.
RecordFollowsAgent ==
    \A e \in Endpoints : Delivers(e) => rec[Cl(e)] = e

\* No session is run twice by the agent.
RunOnce == \A s \in Sess : execs[s] <= 1

\* A definite refusal is true: a session reported not_found was never run,
\* and was refused (or unreachable) at every endpoint of its last walk.
NotFoundTrue == \A s \in Sess : rep[s] = "not_found" => execs[s] = 0 /\ tried[s] = 1..N

\* An opened session ran on the agent (with Settles: every session ends
\* opened or reported to its client, none silently).
OpenedRan == \A s \in Sess : st[s] = "opened" => execs[s] = 1

\* The share is served by its issuing cluster's endpoints only, never
\* after its revocation.
ShareOneCluster == \A e \in served : Cl(e) = ShareCl
RevokedNotServed == ~sar

\* Liveness (FailoverLive.cfg): once the faults are over the agent holds a
\* link to every cluster it can reach (a handover is never lost), no
\* stale registration outlives its link, and every session ends opened
\* or reported.
Reachable(c) == \E e \in EpOf(c) : alive[e] /\ ~cut[e]
Reattaches == <>[](\A c \in Clusters : Reachable(c) => \E e \in EpOf(c) : link[e] = "up")
StaleEnds == <>[](\A e \in Endpoints : reg[e] => link[e] = "up")
Settles == \A s \in Sess : <>(st[s] \in {"opened", "reported"})

\* Witnesses (make spec-mutants): each must be violated.
WitnessRefused == ~(\E e \in Endpoints : refd[e])                              \* OneLinkPerCluster
WitnessHandover == ~(link["b2"] = "up" /\ reg["b1"])                          \* RecordFollowsAgent
WitnessReturn == ~(link["b1"] = "up" /\ refd["b1"] /\ faults > 0)            \* Reattaches
WitnessWalk == ~(\E s \in Sess : st[s] = "opened" /\ 1 \in tried[s])         \* NotFoundTrue
WitnessAmbiguous == ~(\E s \in Sess : rep[s] = "unknown" /\ execs[s] = 1)    \* RunOnce
WitnessStale == ~(\E s \in Sess : st[s] = "lost" /\ alive[ent[s]])          \* Settles
WitnessNotFound == ~(\E s \in Sess : rep[s] = "not_found")                   \* NotFoundTrue
WitnessRevoked == ~(revoked /\ served # {})                                   \* RevokedNotServed
=============================================================================
