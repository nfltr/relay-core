------------------------------- MODULE Handoff -------------------------------
(* Which transport a session of one agent takes: the agent link (sessions  *)
(* on connections bound with a primary stream's token) or the primary's    *)
(* frame streams; what a primary's end, a resumed link and a link that     *)
(* drops do to the sessions (spec/README.md "Handoff.tla"). No bytes: a    *)
(* session's bytes are link's (Link.tla) or AgentLink's.                    *)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Sess,    \* client sessions
    MaxGen,  \* primary streams over the run (each reconnect is the next)
    Mutant   \* "none" or a bug put back

Gens == 1..MaxGen
NoTok == 0

VARIABLES
    prim,     \* [Gens -> {"none", "up", "ended"}]: the relay's primary streams
    aend,     \* [Gens -> BOOLEAN]: the agent saw primary g end (its stream context)
    tok,      \* [Gens -> Gens \cup {NoTok}]: g's bind token (the primary that minted it)
    resumed,  \* [Gens -> BOOLEAN]: linkUse.resumed (a newer primary took g's connections)
    bound,    \* [Gens -> Gens \cup {NoTok}]: TCPDispatcher.bound, token -> primary
    conn,     \* [Gens -> BOOLEAN]: agentLink.Conns[token] has a connection
    dial,     \* [Gens -> BOOLEAN]: linkUse.dial: a dial request of primary g is out
    lz,       \* the agent's lazyLink of its current primary: [st |-> "none"|"dialing"|"up", t |-> token]
    pk,       \* the token of the agent's parked link (linkPark), NoTok for none
    sst,      \* [Sess -> {"idle", "arrived", "wait", "open", "done", "lost", "cut", "refused"}]
    via,      \* [Sess -> {"", "link", "grpc"}]
    on        \* [Sess -> Gens \cup {NoTok}]: the primary (grpc, wait) or the token (link)

vars == <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, sst, via, on>>

Init ==
    /\ prim = [g \in Gens |-> "none"]
    /\ aend = [g \in Gens |-> FALSE]
    /\ tok = [g \in Gens |-> NoTok]
    /\ resumed = [g \in Gens |-> FALSE]
    /\ bound = [t \in Gens |-> NoTok]
    /\ conn = [t \in Gens |-> FALSE]
    /\ dial = [g \in Gens |-> FALSE]
    /\ lz = [st |-> "none", t |-> NoTok]
    /\ pk = NoTok
    /\ sst = [s \in Sess |-> "idle"]
    /\ via = [s \in Sess |-> ""]
    /\ on = [s \in Sess |-> NoTok]

\* The newest primary that is up: TCPDispatcher.Streams (leaveRotationLocked
\* falls back to the latest primary left).
Cur == IF \E g \in Gens : prim[g] = "up" THEN CHOOSE g \in Gens : prim[g] = "up" /\ \A h \in Gens : prim[h] = "up" => h <= g ELSE NoTok

\* RegisterStream with resumeToken: the agent names its parked link's
\* token; the relay resumes it when that primary is here, not ended, not
\* resumed, and its link has a connection; else a new token. The agent
\* adopts a resumed link (lazyLink.adopt). dialNow asks for a link when
\* none is up.
Register(g) ==
    /\ prim[g] = "none"
    /\ IF g = 1 THEN TRUE ELSE aend[g - 1]
    /\ LET r == pk
           old == IF r # NoTok THEN bound[r] ELSE NoTok
           ok == old # NoTok /\ prim[old] = "up" /\ ~resumed[old] /\ conn[r]
           t == IF ok THEN r ELSE g
       IN /\ prim' = [prim EXCEPT ![g] = "up"]
          /\ tok' = [tok EXCEPT ![g] = t]
          /\ resumed' = IF ok THEN [resumed EXCEPT ![old] = TRUE] ELSE resumed
          /\ bound' = [bound EXCEPT ![t] = g]
          /\ dial' = [dial EXCEPT ![g] = ~conn[t]]
          /\ IF ok THEN lz' = [st |-> "up", t |-> t] /\ pk' = NoTok
                   ELSE lz' = [st |-> "none", t |-> NoTok] /\ UNCHANGED pk
    /\ UNCHANGED <<aend, conn, sst, via, on>>

\* The agent dials on the request (lazyLink.open, run); the relay verifies
\* and binds the connection (agentLink.verify, bind), waking the waiters.
Dial(g) ==
    /\ prim[g] # "none" /\ ~aend[g] /\ dial[g] /\ lz.st = "none"
    /\ lz' = [st |-> "dialing", t |-> tok[g]]
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, pk, sst, via, on>>

Bind ==
    /\ lz.st = "dialing"
    /\ LET p == bound[lz.t] IN
         IF p # NoTok /\ prim[p] = "up"
            THEN /\ conn' = [conn EXCEPT ![lz.t] = TRUE]
                 /\ dial' = [dial EXCEPT ![p] = FALSE]
                 /\ lz' = [lz EXCEPT !.st = "up"]
            ELSE /\ lz' = [st |-> "none", t |-> NoTok]  \* refused: the agent gives up on this primary
                 /\ UNCHANGED <<conn, dial>>
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, pk, sst, via, on>>

\* The agent's primary stream ends (its context): an up link waits in the
\* park for the next primary (lazyLink.end); a link parked before closes.
AgentEnd(g) ==
    /\ prim[g] # "none" /\ ~aend[g]
    /\ \A h \in Gens : h > g => prim[h] = "none"
    /\ aend' = [aend EXCEPT ![g] = TRUE]
    /\ IF lz.st = "up"
          THEN /\ pk' = lz.t
               /\ conn' = IF pk # NoTok /\ pk # lz.t THEN [conn EXCEPT ![pk] = FALSE] ELSE conn
          ELSE UNCHANGED <<pk, conn>>
    /\ lz' = [st |-> "none", t |-> NoTok]
    /\ sst' = [s \in Sess |-> IF sst[s] = "open" /\ via[s] = "link" /\ pk # NoTok /\ pk # lz.t /\ on[s] = pk
                                 THEN "cut" ELSE sst[s]]
    /\ UNCHANGED <<prim, tok, resumed, bound, dial, via, on>>

\* The relay's primary ends (unregister -> primaryEnded, CloseAll): its
\* token's connections close unless a newer primary resumed them; its
\* frame-stream sessions are lost; its waiters wake.
PrimEnd(g) ==
    /\ prim[g] = "up"
    /\ prim' = [prim EXCEPT ![g] = "ended"]
    /\ bound' = IF bound[tok[g]] = g THEN [bound EXCEPT ![tok[g]] = NoTok] ELSE bound
    /\ dial' = [dial EXCEPT ![g] = FALSE]
    /\ LET close == ~resumed[g] /\ Mutant # "keep-conns" IN
         /\ conn' = IF close THEN [conn EXCEPT ![tok[g]] = FALSE] ELSE conn
         /\ sst' = [s \in Sess |->
                      IF sst[s] = "open" /\ via[s] = "grpc" /\ on[s] = g THEN "lost"
                      ELSE IF sst[s] = "open" /\ via[s] = "link" /\ on[s] = tok[g] /\ close THEN "cut"
                      ELSE sst[s]]
    /\ UNCHANGED <<aend, tok, resumed, lz, pk, via, on>>

\* A link connection closes: dead past its liveness timeout (agentLink.silent
\* then ends its primary: PrimEnd), the park's TTL, the network.
LinkDrop(t) ==
    /\ conn[t]
    /\ conn' = [conn EXCEPT ![t] = FALSE]
    /\ sst' = [s \in Sess |-> IF sst[s] = "open" /\ via[s] = "link" /\ on[s] = t THEN "cut" ELSE sst[s]]
    /\ lz' = IF lz.t = t THEN [st |-> "none", t |-> NoTok] ELSE lz
    /\ pk' = IF pk = t THEN NoTok ELSE pk
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, dial, via, on>>

Arrive(s) ==
    /\ sst[s] = "idle"
    /\ sst' = [sst EXCEPT ![s] = "arrived"]
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, via, on>>

\* attach: the session on primary p's frame streams, or, when p takes no
\* more (addSession refuses an ending stream), on the newest primary; none
\* left: refused offline.
Attach(s, p) ==
    LET q == IF prim[p] = "up" \/ Mutant = "attach-ended" THEN p ELSE Cur IN
    IF q = NoTok
       THEN sst' = [sst EXCEPT ![s] = "refused"] /\ UNCHANGED <<via, on>>
       ELSE /\ sst' = [sst EXCEPT ![s] = "open"]
            /\ via' = [via EXCEPT ![s] = "grpc"]
            /\ on' = [on EXCEPT ![s] = q]

OnLink(s, t) ==
    /\ sst' = [sst EXCEPT ![s] = "open"]
    /\ via' = [via EXCEPT ![s] = "link"]
    /\ on' = [on EXCEPT ![s] = t]

\* handleLocalConn -> agentLink.open on the current primary p: the link when
\* p's token has a connection; else ask the agent to dial (once) and wait.
Place(s) ==
    /\ sst[s] = "arrived"
    /\ LET p == Cur IN
         IF p = NoTok
            THEN /\ sst' = [sst EXCEPT ![s] = "refused"]
                 /\ UNCHANGED <<dial, via, on>>
         ELSE IF conn[tok[p]]
            THEN OnLink(s, tok[p]) /\ UNCHANGED dial
         ELSE /\ sst' = [sst EXCEPT ![s] = "wait"]
              /\ on' = [on EXCEPT ![s] = p]
              /\ dial' = [dial EXCEPT ![p] = TRUE]
              /\ UNCHANGED via
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, lz, pk>>

\* The wait ends: a connection bound or the primary ended (the request
\* closed), or agentLinkDialWait passed. Then the link of p's token if it
\* has a connection, else attach.
Wake(s) ==
    /\ sst[s] = "wait"
    /\ LET p == on[s] IN
         /\ ~dial[p] \/ Mutant # "wait-forever"
         /\ IF conn[tok[p]] THEN OnLink(s, tok[p]) ELSE Attach(s, p)
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk>>

End(s) ==
    /\ sst[s] = "open"
    /\ sst' = [sst EXCEPT ![s] = "done"]
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, via, on>>

Next ==
    \/ \E g \in Gens : Register(g) \/ Dial(g) \/ AgentEnd(g) \/ PrimEnd(g)
    \/ Bind
    \/ \E t \in Gens : LinkDrop(t)
    \/ \E s \in Sess : Arrive(s) \/ Place(s) \/ Wake(s) \/ End(s)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ \A s \in Sess : WF_vars(Place(s)) /\ WF_vars(Wake(s))

-----------------------------------------------------------------------------
\* A session runs on frame streams only of a primary that is up (one that
\* ended lost them), on the link only through a connection that is there.
PlacedOnLive ==
    \A s \in Sess : sst[s] = "open" =>
        IF via[s] = "grpc" THEN prim[on[s]] = "up" ELSE conn[on[s]]

\* A relay link connection belongs to a token an up primary holds.
NoOrphanLink == \A t \in Gens : conn[t] => \E g \in Gens : tok[g] = t /\ prim[g] = "up"

Placed == \A s \in Sess : sst[s] = "arrived" ~> sst[s] \notin {"arrived", "wait"}
=============================================================================
