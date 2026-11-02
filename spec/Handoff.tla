------------------------------- MODULE Handoff -------------------------------
(* Which transport a session of one agent takes: the agent link (sessions  *)
(* on connections bound with a primary stream's token) or the primary's    *)
(* frame streams; what a primary's end, a resumed link and a link that     *)
(* drops or goes silent do to the sessions; what the relay counts as the   *)
(* agent carrying sessions (spec/README.md "Handoff.tla"). No bytes: a     *)
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
    hdr,      \* the token the relay echoed as resumed to the agent's newest primary, not yet adopted; NoTok
    tcpUp,    \* the agent's TCP path works (frame streams, link/TCP); UDP (QUIC) is another path
    sil,      \* [Gens -> BOOLEAN]: a link silence ended g's path (agentLink.silent): g must end
    sst,      \* [Sess -> {"idle", "arrived", "wait", "open", "done", "lost", "cut", "refused"}]
    via,      \* [Sess -> {"", "link", "grpc"}]
    on,       \* [Sess -> Gens \cup {NoTok}]: the primary (grpc, wait) or the token (link)
    by        \* [Sess -> Gens \cup {NoTok}]: the primary a link session opened through (agentLink.open's p)

vars == <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, hdr, tcpUp, sil, sst, via, on, by>>

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
    /\ hdr = NoTok
    /\ tcpUp = TRUE
    /\ sil = [g \in Gens |-> FALSE]
    /\ sst = [s \in Sess |-> "idle"]
    /\ via = [s \in Sess |-> ""]
    /\ on = [s \in Sess |-> NoTok]
    /\ by = [s \in Sess |-> NoTok]

\* The newest primary that is up: TCPDispatcher.Streams (leaveRotationLocked
\* falls back to the latest primary left).
Cur == IF \E g \in Gens : prim[g] = "up" THEN CHOOSE g \in Gens : prim[g] = "up" /\ \A h \in Gens : prim[h] = "up" => h <= g ELSE NoTok

\* RegisterStream with resumeToken: the agent names its parked link's
\* token; the relay resumes it when that primary is here and the agent's
\* (one agent here: the agent check is TestResumeTokenRefusesAnotherAgent),
\* not ended, not resumed, and its link has a connection; else a new token.
\* The header echoes it (hdr); the agent adopts it later (Adopt) or drains
\* its park (AdoptRefused). dialNow asks for a link when none is up.
Register(g) ==
    /\ prim[g] = "none" /\ tcpUp
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
          /\ hdr' = IF ok THEN t ELSE NoTok
    /\ lz' = [st |-> "none", t |-> NoTok]
    /\ UNCHANGED <<aend, conn, pk, tcpUp, sil, sst, via, on, by>>

\* The agent reads its new primary's header (relay client tcp.go): it
\* adopts the parked link the relay resumed (lazyLink.adopt) while the park
\* still holds it and no dial of its own is out.
Adopt ==
    /\ hdr # NoTok /\ pk = hdr /\ lz.st = "none"
    /\ lz' = [st |-> "up", t |-> hdr] /\ pk' = NoTok /\ hdr' = NoTok
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, tcpUp, sil, sst, via, on, by>>

\* A link connection of token t closes; its sessions are cut. redial: the
\* agent's up link redials it (its peerlink Node, linkBackoff), so its
\* lazy link is dialling again (finding HO-6: a trace).
DropAs(t, redial) ==
    /\ conn' = [conn EXCEPT ![t] = FALSE]
    /\ sst' = [s \in Sess |-> IF sst[s] = "open" /\ via[s] = "link" /\ on[s] = t THEN "cut" ELSE sst[s]]
    /\ lz' = IF lz.t # t THEN lz
             ELSE IF redial /\ lz.st = "up" THEN [lz EXCEPT !.st = "dialing"]
             ELSE [st |-> "none", t |-> NoTok]
    /\ pk' = IF pk = t THEN NoTok ELSE pk

DropBody(t) == DropAs(t, FALSE)

\* adopt refused (a dial of the new primary is out: lazyLink.adopt's
\* dialing), or the park's TTL passed first: the parked link drains
\* (drainNow), its sessions cut; a later session asks for a dial.
AdoptRefused ==
    /\ hdr # NoTok /\ (pk # hdr \/ lz.st # "none")
    /\ hdr' = NoTok
    /\ IF pk # NoTok THEN DropBody(pk) ELSE UNCHANGED <<conn, sst, lz, pk>>
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, dial, tcpUp, sil, via, on, by>>

\* The agent dials on the request (lazyLink.open, run); the relay verifies
\* and binds the connection (agentLink.verify, bind), waking the waiters.
Dial(g) ==
    /\ prim[g] # "none" /\ ~aend[g] /\ dial[g] /\ lz.st = "none"
    /\ lz' = [st |-> "dialing", t |-> tok[g]]
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, pk, hdr, tcpUp, sil, sst, via, on, by>>

Bind ==
    /\ lz.st = "dialing"
    /\ LET p == bound[lz.t] IN
         IF p # NoTok /\ prim[p] = "up"
            THEN /\ conn' = [conn EXCEPT ![lz.t] = TRUE]
                 /\ dial' = [dial EXCEPT ![p] = FALSE]
                 /\ lz' = [lz EXCEPT !.st = "up"]
            ELSE /\ lz' = [st |-> "none", t |-> NoTok]  \* refused: the agent gives up on this primary
                 /\ UNCHANGED <<conn, dial>>
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, pk, hdr, tcpUp, sil, sst, via, on, by>>

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
    /\ hdr' = NoTok \* an ended primary adopts nothing
    /\ sst' = [s \in Sess |-> IF sst[s] = "open" /\ via[s] = "link" /\ pk # NoTok /\ pk # lz.t /\ on[s] = pk
                                 THEN "cut" ELSE sst[s]]
    /\ UNCHANGED <<prim, tok, resumed, bound, dial, tcpUp, sil, via, on, by>>

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
    /\ UNCHANGED <<aend, tok, resumed, lz, pk, hdr, tcpUp, sil, via, on, by>>

\* A link connection closes: the park's TTL, the agent's stop, the network.
LinkDrop(t) ==
    /\ conn[t] /\ \E redial \in BOOLEAN : DropAs(t, redial)
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, dial, hdr, tcpUp, sil, via, on, by>>

\* A link connection dead past its liveness timeout (link.ErrDeadPeer on
\* TCP, QUIC's idle timeout). A silent TCP link means the agent's TCP path
\* is dead, its frame streams with it: agentLink.silent ends the primary
\* bound to the token (sil; SilentEnds). A QUIC link rides UDP, another
\* path: its silence only unbinds it, and the frame streams' own liveness
\* (heartbeats again once no link vouches for them) judges TCP.
Silent(t, tr) ==
    /\ conn[t] /\ DropBody(t)
    /\ tr = "tcp" => ~tcpUp
    /\ sil' = IF (tr = "tcp" \/ Mutant = "quic-silent-ends") /\ bound[t] # NoTok
                 THEN [sil EXCEPT ![bound[t]] = TRUE] ELSE sil
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, dial, hdr, tcpUp, via, on, by>>

\* The agent's TCP path dies, for the rest of the run (no reconnect over it).
PathDown ==
    /\ tcpUp /\ tcpUp' = FALSE
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, hdr, sil, sst, via, on, by>>

Arrive(s) ==
    /\ sst[s] = "idle"
    /\ sst' = [sst EXCEPT ![s] = "arrived"]
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, hdr, tcpUp, sil, via, on, by>>

\* attach: the session on primary p's frame streams, or, when p takes no
\* more (addSession refuses an ending stream), on the newest primary; none
\* left: refused offline.
Attach(s, p) ==
    /\ LET q == IF prim[p] = "up" \/ Mutant = "attach-ended" THEN p ELSE Cur IN
       IF q = NoTok
          THEN sst' = [sst EXCEPT ![s] = "refused"] /\ UNCHANGED <<via, on>>
          ELSE /\ sst' = [sst EXCEPT ![s] = "open"]
               /\ via' = [via EXCEPT ![s] = "grpc"]
               /\ on' = [on EXCEPT ![s] = q]
    /\ UNCHANGED by

\* agentLink.open on p's token: the link, counted on p; or, when node.Open
\* fails (the connection draining after GOAWAY or closing, the dial wait),
\* the frame streams (handleLocalConn's attach).
OnLinkOr(s, p) ==
    \/ /\ sst' = [sst EXCEPT ![s] = "open"]
       /\ via' = [via EXCEPT ![s] = "link"]
       /\ on' = [on EXCEPT ![s] = tok[p]]
       /\ by' = [by EXCEPT ![s] = p]
    \/ Attach(s, p)

\* handleLocalConn -> agentLink.open on the current primary p: the link when
\* p's token has a connection; else ask the agent to dial (once) and wait.
Place(s) ==
    /\ sst[s] = "arrived"
    /\ LET p == Cur IN
         IF p = NoTok
            THEN /\ sst' = [sst EXCEPT ![s] = "refused"]
                 /\ UNCHANGED <<dial, via, on, by>>
         ELSE IF conn[tok[p]]
            THEN OnLinkOr(s, p) /\ UNCHANGED dial
         ELSE /\ sst' = [sst EXCEPT ![s] = "wait"]
              /\ on' = [on EXCEPT ![s] = p]
              /\ dial' = [dial EXCEPT ![p] = TRUE]
              /\ UNCHANGED <<via, by>>
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, lz, pk, hdr, tcpUp, sil>>

\* The wait ends: a connection bound or the primary ended (the request
\* closed), or agentLinkDialWait passed. Then the link of p's token if it
\* has a connection, else attach.
Wake(s) ==
    /\ sst[s] = "wait"
    /\ LET p == on[s] IN
         /\ ~dial[p] \/ Mutant # "wait-forever"
         /\ IF conn[tok[p]] THEN OnLinkOr(s, p) ELSE Attach(s, p)
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, hdr, tcpUp, sil>>

End(s) ==
    /\ sst[s] = "open"
    /\ sst' = [sst EXCEPT ![s] = "done"]
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, hdr, tcpUp, sil, via, on, by>>

Next ==
    \/ \E g \in Gens : Register(g) \/ Dial(g) \/ AgentEnd(g) \/ PrimEnd(g)
    \/ Bind \/ Adopt \/ AdoptRefused \/ PathDown
    \/ \E t \in Gens : LinkDrop(t) \/ \E tr \in {"tcp", "quic"} : Silent(t, tr)
    \/ \E s \in Sess : Arrive(s) \/ Place(s) \/ Wake(s) \/ End(s)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ \A s \in Sess : WF_vars(Place(s)) /\ WF_vars(Wake(s))
                 /\ \A g \in Gens : WF_vars(sil[g] /\ PrimEnd(g))

-----------------------------------------------------------------------------
\* A session runs on frame streams only of a primary that is up (one that
\* ended lost them), on the link only through a connection that is there.
PlacedOnLive ==
    \A s \in Sess : sst[s] = "open" =>
        IF via[s] = "grpc" THEN prim[on[s]] = "up" ELSE conn[on[s]]

\* A relay link connection belongs to a token an up primary holds.
NoOrphanLink == \A t \in Gens : conn[t] => \E g \in Gens : tok[g] = t /\ prim[g] = "up"

\* What the relay counts as the agent carrying sessions (carriesSessions:
\* a rebalance moves only an agent carrying none, agent_rebalance.go
\* moveAgent): every primary of the agent here, the sessions on its frame
\* streams, and the link sessions of the token it holds (bound), whichever
\* primary they opened through. The bugs (finding HO-3): link sessions
\* counted on the primary they opened through, which a resume leaves
\* behind (count-per-primary); the newest primary only (carries-newest).
CarriesOn(g) ==
    \E s \in Sess :
        /\ sst[s] = "open"
        /\ \/ via[s] = "grpc" /\ on[s] = g
           \/ /\ via[s] = "link"
              /\ IF Mutant = "count-per-primary" THEN by[s] = g
                 ELSE on[s] = tok[g] /\ bound[tok[g]] = g
Carries ==
    IF Mutant = "carries-newest" THEN Cur # NoTok /\ CarriesOn(Cur)
    ELSE \E g \in Gens : prim[g] = "up" /\ CarriesOn(g)
CarriesExact == Carries <=> \E s \in Sess : sst[s] = "open"

\* A primary ends for its link's silence only when its TCP path is dead:
\* a QUIC link's silence says nothing of TCP (finding HO-2).
SilenceOnDeadPath == \A g \in Gens : sil[g] => ~tcpUp

Placed == \A s \in Sess : sst[s] = "arrived" ~> sst[s] \notin {"arrived", "wait"}

\* A silent link's primary ends (sendLoop's errAgentLinkSilent).
SilentEnds == \A g \in Gens : sil[g] ~> prim[g] = "ended"

\* Witnesses (make spec-mutants): each must be violated, so the states the
\* invariant named beside it guards are reached.
WitnessLinkOpen == ~\E s \in Sess : sst[s] = "open" /\ via[s] = "link"                 \* PlacedOnLive
WitnessResumed == ~\E g \in Gens : resumed[g] /\ prim[g] = "ended" /\ conn[tok[g]]     \* NoOrphanLink
WitnessCarryOld == ~\E s \in Sess : sst[s] = "open" /\ via[s] = "link" /\ prim[by[s]] = "ended" \* CarriesExact
WitnessFallback == ~\E s \in Sess : sst[s] = "open" /\ via[s] = "grpc" /\ conn[tok[on[s]]] \* PlacedOnLive (HO-4)
WitnessSilent == ~\E g \in Gens : sil[g]                                                \* SilenceOnDeadPath
=============================================================================
