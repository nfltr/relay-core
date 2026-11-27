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
    pkg,      \* the primary whose end parked pk
    pend,     \* [t, g]: the up link of primary g, whose end the agent saw, not yet parked (lazyLink.end); t = NoTok: none
    asked,    \* [Gens -> Gens \cup {NoTok}]: the parked link's token primary g asked the relay to resume
    hdr,      \* the token the relay echoed as resumed to the agent's newest primary, not yet adopted; NoTok
    tcpUp,    \* the agent's TCP path works (frame streams, link/TCP); UDP (QUIC) is another path
    sil,      \* [Gens -> BOOLEAN]: a link silence ended g's path (agentLink.silent): g must end
    sst,      \* [Sess -> {"idle", "arrived", "wait", "open", "done", "lost", "cut", "refused"}]
    via,      \* [Sess -> {"", "link", "grpc"}]
    on,       \* [Sess -> Gens \cup {NoTok}]: the primary (grpc, wait) or the token (link)
    by,       \* [Sess -> Gens \cup {NoTok}]: the primary a link session opened through (agentLink.open's p)
    grace,    \* [Gens -> BOOLEAN]: token t's live connections, kept after its primary ended unresumed, for the next primary to resume (G7)
    endcut    \* a primary's end cut a session on a live link (finding 5; G7's ResumeNotCut)

vars == <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, pkg, pend, asked, hdr, tcpUp, sil, sst, via, on, by, grace, endcut>>

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
    /\ pkg = NoTok
    /\ pend = [t |-> NoTok, g |-> NoTok]
    /\ asked = [g \in Gens |-> NoTok]
    /\ hdr = NoTok
    /\ tcpUp = TRUE
    /\ sil = [g \in Gens |-> FALSE]
    /\ sst = [s \in Sess |-> "idle"]
    /\ via = [s \in Sess |-> ""]
    /\ on = [s \in Sess |-> NoTok]
    /\ by = [s \in Sess |-> NoTok]
    /\ grace = [t \in Gens |-> FALSE]
    /\ endcut = FALSE

\* The newest primary that is up: TCPDispatcher.Streams (leaveRotationLocked
\* falls back to the latest primary left).
Cur == IF \E g \in Gens : prim[g] = "up" THEN CHOOSE g \in Gens : prim[g] = "up" /\ \A h \in Gens : prim[h] = "up" => h <= g ELSE NoTok

\* RegisterStream with resumeToken: the agent names its parked link's
\* token; the relay resumes it when that primary is here and the agent's
\* (one agent here: the agent check is TestResumeTokenRefusesAnotherAgent),
\* not ended, not resumed, and its link has a connection, or when the
\* token's primary ended and its live connections are in their resume
\* grace (G7); else a new token.
\* The header echoes it (hdr); the agent adopts it later (Adopt) or drains
\* its park (AdoptRefused). dialNow asks for a link when none is up.
Register(g) ==
    /\ prim[g] = "none" /\ tcpUp
    /\ IF g = 1 THEN TRUE ELSE aend[g - 1]
    \* The next primary connects once the end has parked the link
    \* (runTCPTunnel waits for lazyLink.end); the bug (end-async): end ran
    \* in its own goroutine, so the next primary could ask before the park.
    /\ pend.t = NoTok \/ Mutant = "end-async"
    /\ asked' = [asked EXCEPT ![g] = pk]
    /\ LET r == pk
           old == IF r # NoTok THEN bound[r] ELSE NoTok
           live == old # NoTok /\ prim[old] = "up" /\ ~resumed[old] /\ conn[r]
           graced == r # NoTok /\ grace[r] /\ conn[r]
           ok == live \/ graced
           t == IF ok THEN r ELSE g
       IN /\ prim' = [prim EXCEPT ![g] = "up"]
          /\ tok' = [tok EXCEPT ![g] = t]
          /\ resumed' = IF live THEN [resumed EXCEPT ![old] = TRUE] ELSE resumed
          /\ grace' = IF graced THEN [grace EXCEPT ![r] = FALSE] ELSE grace
          /\ bound' = [bound EXCEPT ![t] = g]
          /\ dial' = [dial EXCEPT ![g] = ~conn[t]]
          /\ hdr' = IF ok THEN t ELSE NoTok
    /\ lz' = [st |-> "none", t |-> NoTok]
    /\ UNCHANGED <<aend, conn, pk, pkg, pend, tcpUp, sil, sst, via, on, by, endcut>>

\* The agent reads its new primary's header (relay client tcp.go): it
\* adopts the parked link the relay resumed (lazyLink.adopt) while the park
\* still holds it and no dial of its own is out.
Adopt ==
    /\ hdr # NoTok /\ pk = hdr /\ lz.st = "none"
    /\ lz' = [st |-> "up", t |-> hdr] /\ pk' = NoTok /\ hdr' = NoTok
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, pkg, pend, asked, tcpUp, sil, sst, via, on, by, grace, endcut>>

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
    /\ pend' = IF pend.t = t THEN [t |-> NoTok, g |-> NoTok] ELSE pend
    /\ grace' = [grace EXCEPT ![t] = FALSE]

DropBody(t) == DropAs(t, FALSE)

\* adopt refused (a dial of the new primary is out: lazyLink.adopt's
\* dialing), or the park's TTL passed first: the parked link drains
\* (drainNow), its sessions cut; a later session asks for a dial.
AdoptRefused ==
    /\ hdr # NoTok /\ (pk # hdr \/ lz.st # "none")
    /\ hdr' = NoTok
    /\ IF pk # NoTok THEN DropBody(pk) ELSE UNCHANGED <<conn, sst, lz, pk, pend, grace>>
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, dial, pkg, asked, tcpUp, sil, via, on, by, endcut>>

\* The agent dials on the request (lazyLink.open, run); the relay verifies
\* and binds the connection (agentLink.verify, bind), waking the waiters.
Dial(g) ==
    /\ prim[g] # "none" /\ ~aend[g] /\ dial[g] /\ lz.st = "none"
    /\ lz' = [st |-> "dialing", t |-> tok[g]]
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, pk, pkg, pend, asked, hdr, tcpUp, sil, sst, via, on, by, grace, endcut>>

Bind ==
    /\ lz.st = "dialing"
    /\ LET p == bound[lz.t] IN
         IF p # NoTok /\ prim[p] = "up"
            THEN /\ conn' = [conn EXCEPT ![lz.t] = TRUE]
                 /\ dial' = [dial EXCEPT ![p] = FALSE]
                 /\ lz' = [lz EXCEPT !.st = "up"]
            ELSE /\ lz' = [st |-> "none", t |-> NoTok]  \* refused: the agent gives up on this primary
                 /\ UNCHANGED <<conn, dial, grace, endcut>>
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, pk, pkg, pend, asked, hdr, tcpUp, sil, sst, via, on, by, grace, endcut>>

\* The agent's primary stream ends (its context): lazyLink.end takes an up
\* link off the primary (pend), then Park puts it in the park for the next
\* primary; a link parked before closes, its sessions cut.
AgentEnd(g) ==
    /\ prim[g] # "none" /\ ~aend[g]
    /\ \A h \in Gens : h > g => prim[h] = "none"
    /\ aend' = [aend EXCEPT ![g] = TRUE]
    /\ pend' = IF lz.st = "up" THEN [t |-> lz.t, g |-> g] ELSE pend
    /\ lz' = [st |-> "none", t |-> NoTok]
    /\ hdr' = NoTok \* an ended primary adopts nothing
    /\ UNCHANGED <<prim, tok, resumed, bound, conn, dial, pk, pkg, asked, tcpUp, sil, sst, via, on, by, grace, endcut>>

Park ==
    /\ pend.t # NoTok
    /\ pk' = pend.t /\ pkg' = pend.g
    /\ pend' = [t |-> NoTok, g |-> NoTok]
    /\ conn' = IF pk # NoTok /\ pk # pend.t THEN [conn EXCEPT ![pk] = FALSE] ELSE conn
    /\ sst' = [s \in Sess |-> IF sst[s] = "open" /\ via[s] = "link" /\ pk # NoTok /\ pk # pend.t /\ on[s] = pk
                                 THEN "cut" ELSE sst[s]]
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, dial, lz, asked, hdr, tcpUp, sil, via, on, by, grace, endcut>>

\* The relay's primary ends (unregister -> primaryEnded, CloseAll): its
\* frame-stream sessions are lost; its waiters wake. Its token's live
\* connections, unless a newer primary resumed them, are kept for a resume
\* grace (G7): the agent's next primary resumes them (Register), or the
\* grace's end closes them (GraceEnd). The code before (close-at-end,
\* finding 5) closed them here, cutting the sessions of a live link when
\* the agent's own end of the primary reached the relay first.
PrimEnd(g) ==
    /\ prim[g] = "up"
    /\ prim' = [prim EXCEPT ![g] = "ended"]
    /\ bound' = IF bound[tok[g]] = g THEN [bound EXCEPT ![tok[g]] = NoTok] ELSE bound
    /\ dial' = [dial EXCEPT ![g] = FALSE]
    /\ LET t == tok[g]
           mine == ~resumed[g] /\ Mutant # "keep-conns"
           keep == mine /\ conn[t] /\ Mutant # "close-at-end"
           close == mine /\ ~keep
           cut == {s \in Sess : sst[s] = "open" /\ via[s] = "link" /\ on[s] = t /\ close}
       IN
         /\ conn' = IF close THEN [conn EXCEPT ![t] = FALSE] ELSE conn
         /\ grace' = IF keep THEN [grace EXCEPT ![t] = TRUE] ELSE grace
         /\ endcut' = (endcut \/ cut # {})
         /\ sst' = [s \in Sess |->
                      IF sst[s] = "open" /\ via[s] = "grpc" /\ on[s] = g THEN "lost"
                      ELSE IF s \in cut THEN "cut"
                      ELSE sst[s]]
    /\ UNCHANGED <<aend, tok, resumed, lz, pk, pkg, pend, asked, hdr, tcpUp, sil, via, on, by>>

\* The resume grace of token t ends with no primary resuming it
\* (primaryEnded's timer): its connections close, their sessions cut.
GraceEnd(t) ==
    /\ grace[t]
    /\ DropBody(t)
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, dial, pkg, asked, hdr, tcpUp, sil, via, on, by, endcut>>

\* A link connection closes: the park's TTL, the agent's stop, the network.
LinkDrop(t) ==
    /\ conn[t] /\ \E redial \in BOOLEAN : DropAs(t, redial)
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, dial, pkg, asked, hdr, tcpUp, sil, via, on, by, endcut>>

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
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, dial, pkg, asked, hdr, tcpUp, via, on, by, endcut>>

\* The agent's TCP path dies, for the rest of the run (no reconnect over it).
PathDown ==
    /\ tcpUp /\ tcpUp' = FALSE
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, pkg, pend, asked, hdr, sil, sst, via, on, by, grace, endcut>>

Arrive(s) ==
    /\ sst[s] = "idle"
    /\ sst' = [sst EXCEPT ![s] = "arrived"]
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, pkg, pend, asked, hdr, tcpUp, sil, via, on, by, grace, endcut>>

\* attach: the session on primary p's frame streams, or, when p takes no
\* more (addSession refuses an ending stream), on the newest primary; none
\* left: refused offline.
Attach(s, p) ==
    /\ LET q == IF prim[p] = "up" \/ Mutant = "attach-ended" THEN p ELSE Cur IN
       IF q = NoTok
          THEN sst' = [sst EXCEPT ![s] = "refused"] /\ UNCHANGED <<via, on, grace, endcut>>
          ELSE /\ sst' = [sst EXCEPT ![s] = "open"]
               /\ via' = [via EXCEPT ![s] = "grpc"]
               /\ on' = [on EXCEPT ![s] = q]
    /\ UNCHANGED by

\* agentLink.open on p's token: the link, counted on p; or, when node.Open
\* fails (the connection draining after GOAWAY or closing, the dial wait),
\* the frame streams (handleLocalConn's attach). The agent answers a link
\* session in its own context, also on a link in the park (newLinkSessions:
\* park.ctx); the bug (park-primary-ctx, RECONNFLAKE): it dialled the
\* target in its ended primary's context and refused the open.
OnLinkOr(s, p) ==
    \/ /\ sst' = [sst EXCEPT ![s] = IF Mutant = "park-primary-ctx" /\ pk = tok[p] THEN "refused" ELSE "open"]
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
                 /\ UNCHANGED <<dial, via, on, by, grace, endcut>>
         ELSE IF conn[tok[p]]
            THEN OnLinkOr(s, p) /\ UNCHANGED dial
         ELSE /\ sst' = [sst EXCEPT ![s] = "wait"]
              /\ on' = [on EXCEPT ![s] = p]
              /\ dial' = [dial EXCEPT ![p] = TRUE]
              /\ UNCHANGED <<via, by, grace, endcut>>
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, lz, pk, pkg, pend, asked, hdr, tcpUp, sil, grace, endcut>>

\* The wait ends: a connection bound or the primary ended (the request
\* closed), or agentLinkDialWait passed. Then the link of p's token if it
\* has a connection, else attach.
Wake(s) ==
    /\ sst[s] = "wait"
    /\ LET p == on[s] IN
         /\ ~dial[p] \/ Mutant # "wait-forever"
         /\ IF conn[tok[p]] THEN OnLinkOr(s, p) ELSE Attach(s, p)
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, pkg, pend, asked, hdr, tcpUp, sil, grace, endcut>>

End(s) ==
    /\ sst[s] = "open"
    /\ sst' = [sst EXCEPT ![s] = "done"]
    /\ UNCHANGED <<prim, aend, tok, resumed, bound, conn, dial, lz, pk, pkg, pend, asked, hdr, tcpUp, sil, via, on, by, grace, endcut>>

Next ==
    \/ \E g \in Gens : Register(g) \/ Dial(g) \/ AgentEnd(g) \/ PrimEnd(g)
    \/ Bind \/ Park \/ Adopt \/ AdoptRefused \/ PathDown
    \/ \E t \in Gens : LinkDrop(t) \/ GraceEnd(t) \/ \E tr \in {"tcp", "quic"} : Silent(t, tr)
    \/ \E s \in Sess : Arrive(s) \/ Place(s) \/ Wake(s) \/ End(s)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ \A s \in Sess : WF_vars(Place(s)) /\ WF_vars(Wake(s))
                 /\ \A g \in Gens : WF_vars(sil[g] /\ PrimEnd(g))
                 /\ \A t \in Gens : WF_vars(GraceEnd(t))

-----------------------------------------------------------------------------
\* A session runs on frame streams only of a primary that is up (one that
\* ended lost them), on the link only through a connection that is there.
PlacedOnLive ==
    \A s \in Sess : sst[s] = "open" =>
        IF via[s] = "grpc" THEN prim[on[s]] = "up" ELSE conn[on[s]]

\* A relay link connection belongs to a token an up primary holds, or to
\* one in its resume grace (G7).
NoOrphanLink == \A t \in Gens : conn[t] => grace[t] \/ \E g \in Gens : tok[g] = t /\ prim[g] = "up"

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
\* A graced token's link sessions count too (G7; the bug carries-no-grace:
\* only tokens a primary holds).
CarriesGraced ==
    Mutant # "carries-no-grace" /\ \E s \in Sess : sst[s] = "open" /\ via[s] = "link" /\ grace[on[s]]
Carries ==
    IF Mutant = "carries-newest" THEN Cur # NoTok /\ CarriesOn(Cur)
    ELSE CarriesGraced \/ \E g \in Gens : prim[g] = "up" /\ CarriesOn(g)
CarriesExact == Carries <=> \E s \in Sess : sst[s] = "open"

\* An open the relay placed on the link is answered by the agent: only
\* one with no stream for it is refused (finding 42).
LinkOpensAnswered == \A s \in Sess : sst[s] = "refused" => via[s] # "link"

\* A primary ends for its link's silence only when its TCP path is dead:
\* a QUIC link's silence says nothing of TCP (finding HO-2).
SilenceOnDeadPath == \A g \in Gens : sil[g] => ~tcpUp

\* The agent offers its parked link to every primary after the one whose
\* end parked it: a newer primary that did not ask to resume it left it to
\* drain at the park's TTL, its sessions cut (end-async).
ParkOffered == pk # NoTok => \A h \in Gens : h > pkg /\ prim[h] # "none" => asked[h] = pk

\* A primary's end never cuts the sessions of a live link: the link waits
\* for the agent's next primary in its grace (G7; close-at-end: finding 5).
ResumeNotCut == ~endcut

\* A grace ends: resumed, dropped or timed out.
GraceEnds == \A t \in Gens : grace[t] ~> ~grace[t]

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
WitnessEndUnparked == pend.t = NoTok                                                   \* ParkOffered
WitnessGraceResumed == ~\E s \in Sess : sst[s] = "open" /\ via[s] = "link" /\ prim[by[s]] = "ended" /\ ~resumed[by[s]] /\ bound[on[s]] # NoTok \* ResumeNotCut
WitnessParkedOpen == ~\E s \in Sess : sst[s] = "open" /\ via[s] = "link" /\ on[s] = pk   \* LinkOpensAnswered
=============================================================================
