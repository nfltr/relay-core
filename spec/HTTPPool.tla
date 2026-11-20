------------------------------- MODULE HTTPPool -------------------------------
(* The relay's HTTP keep-alive pool to one agent (frontend/http             *)
(* agent_http_link.go, lane HTTPPOOL; spec/README.md "HTTPPool.tla"): which *)
(* agent-link connection a request's HTTP session rides, when the agent     *)
(* replaces its primary stream (a restart, a takeover) or its QUIC          *)
(* connection (a partition, a closed socket), and what the pool may reuse   *)
(* after that. No bytes and no frame streams: a session's bytes are link's, *)
(* and a primary's frame-stream sessions end with it (Handoff.tla).         *)
EXTENDS Naturals

CONSTANTS
    Gens,    \* the agent's primary streams over the run (a reconnect is the next)
    Conns,   \* agent-link connections over the run
    Reqs,    \* requests; a session is named by the request that dialled it
    Mutant   \* "none" or a bug put back

NoG == 0
NoC == 0

VARIABLES
    prim,    \* [Gens -> {"none", "up", "ended"}]: the relay's primary streams
    cp,      \* [Conns -> Gens \cup {NoG}]: the primary a connection is bound to (its token's)
    ctr,     \* [Conns -> {"", "tcp", "quic"}]
    cst,     \* [Conns -> {"none", "live", "dead", "closed"}]: dead: the agent's end gone, unseen; the relay's open
    repl,    \* [Conns -> BOOLEAN]: the agent replaced it: a later QUIC connection of its primary bound
    rq,      \* [Reqs -> {"new", "keyed", "sent", "done", "failed"}]
    rkey,    \* [Reqs -> Gens \cup {NoG}]: the pool key the request read (Rewrite)
    rchg,    \* [Reqs -> BOOLEAN]: a route changed since it read it
    rs,      \* [Reqs -> Reqs \cup {0}]: the session it was written on
    sc,      \* [Reqs -> Conns \cup {NoC}]: session s's connection
    sk,      \* [Reqs -> Gens \cup {NoG}]: the key s is pooled under (its dialling request's)
    sr,      \* [Reqs -> Gens \cup {NoG}]: the route s rides (the primary it was placed on)
    sst,     \* [Reqs -> {"none", "busy", "idle", "closed"}]
    stale,   \* a request was written on a session the directory no longer names, with no route change since its key
    livecut  \* a replacement closed a connection the agent still holds

vars == <<prim, cp, ctr, cst, repl, rq, rkey, rchg, rs, sc, sk, sr, sst, stale, livecut>>

Init ==
    /\ prim = [g \in Gens |-> "none"]
    /\ cp = [c \in Conns |-> NoG]
    /\ ctr = [c \in Conns |-> ""]
    /\ cst = [c \in Conns |-> "none"]
    /\ repl = [c \in Conns |-> FALSE]
    /\ rq = [r \in Reqs |-> "new"]
    /\ rkey = [r \in Reqs |-> NoG]
    /\ rchg = [r \in Reqs |-> FALSE]
    /\ rs = [r \in Reqs |-> 0]
    /\ sc = [s \in Reqs |-> NoC]
    /\ sk = [s \in Reqs |-> NoG]
    /\ sr = [s \in Reqs |-> NoG]
    /\ sst = [s \in Reqs |-> "none"]
    /\ stale = FALSE
    /\ livecut = FALSE

\* The newest primary that is up: TCPDispatcher.Streams.
Cur == IF \E g \in Gens : prim[g] = "up" THEN CHOOSE g \in Gens : prim[g] = "up" /\ \A h \in Gens : prim[h] = "up" => h <= g ELSE NoG

Open(c) == cst[c] \in {"live", "dead"}

\* What the directory names for the agent: a connection of its current
\* primary that the agent has not replaced.
Named(c) == Open(c) /\ cp[c] = Cur /\ ~repl[c]

\* Writing on session s: stale when its connection is open, not named,
\* and no route changed since the request read its key (a change after
\* that is the request's own race, as a request a moment before it).
Wrote(r, s) == stale' = (stale \/ (Open(sc[s]) /\ ~Named(sc[s]) /\ ~rchg[r]))

\* Sessions on the connections in cs close (the pool drops them on the
\* read error; a request on one fails).
CloseSess(cs) == sst' = [s \in Reqs |-> IF sc[s] \in cs /\ sst[s] # "none" THEN "closed" ELSE sst[s]]

KeepReq == UNCHANGED <<rq, rkey, rs, sc, sk, sr>>

\* A route changed: every request that read its key before sees it.
Changed == rchg' = [r \in Reqs |-> rchg[r] \/ rq[r] # "new"]

\* RegisterStream (enterRotation): a newer primary takes the agent's new
\* sessions; its route is its own.
Register(g) ==
    /\ prim[g] = "none" /\ IF g = 1 THEN TRUE ELSE prim[g - 1] # "none"
    /\ prim' = [prim EXCEPT ![g] = "up"]
    /\ Changed
    /\ UNCHANGED <<cp, ctr, cst, repl, sst, stale, livecut>> /\ KeepReq

\* unregister: the primary leaves the rotation (the latest left takes
\* its place); its link's connections stay for the resume grace and end
\* by Close.
PrimEnd(g) ==
    /\ prim[g] = "up"
    /\ prim' = [prim EXCEPT ![g] = "ended"]
    /\ Changed
    /\ UNCHANGED <<cp, ctr, cst, repl, sst, stale, livecut>> /\ KeepReq

\* agentLink.bind: a connection of primary g's token binds. The agent
\* (relay client's quiclink Node: one connection to its relay, dialled
\* again only once its own is unusable) binds a QUIC connection only when
\* it holds no other of g; TCP connections of a token grow side by side.
\* The relay retires g's other QUIC connections: closes them, their
\* sessions with them (no-retire: the code before; retire-any: a TCP bind
\* retires too).
Bind(c, g, t) ==
    /\ cst[c] = "none" /\ prim[g] = "up"
    /\ t = "quic" => \A o \in Conns : cp[o] = g /\ ctr[o] = "quic" => cst[o] # "live"
    /\ LET old == {o \in Conns : o # c /\ cp[o] = g /\ Open(o) /\ ((ctr[o] = "quic" /\ t = "quic") \/ Mutant = "retire-any")}
           replaced == {o \in old : ctr[o] = "quic" /\ t = "quic"}
           retired == IF Mutant = "no-retire" THEN {} ELSE old
       IN /\ cp' = [cp EXCEPT ![c] = g]
          /\ ctr' = [ctr EXCEPT ![c] = t]
          /\ cst' = [o \in Conns |-> IF o = c THEN "live" ELSE IF o \in retired THEN "closed" ELSE cst[o]]
          /\ repl' = [o \in Conns |-> repl[o] \/ o \in replaced]
          /\ CloseSess(retired)
          /\ IF replaced # {} THEN Changed ELSE UNCHANGED rchg
          /\ livecut' = (livecut \/ \E o \in retired : cst[o] = "live")
    /\ UNCHANGED <<prim, stale>> /\ KeepReq

\* The agent's end of c goes without a word reaching the relay (a closed
\* socket, a lost close, a crash): the relay's end stays open.
Lose(c) ==
    /\ cst[c] = "live"
    /\ cst' = [cst EXCEPT ![c] = "dead"]
    /\ UNCHANGED <<prim, cp, ctr, repl, sst, rchg, stale, livecut>> /\ KeepReq

\* The relay's end of c closes: its liveness (DeadAfter, 15 s), the
\* resume grace's end, the agent's close reaching it.
Close(c) ==
    /\ Open(c)
    /\ cst' = [cst EXCEPT ![c] = "closed"]
    /\ CloseSess({c})
    /\ UNCHANGED <<prim, cp, ctr, repl, rchg, stale, livecut>> /\ KeepReq

\* agentHTTP's Rewrite: the request reads its pool key, the agent's route
\* (HTTPRoute); pool-by-agent: the agent alone, every session back in its
\* pool (the code before).
Key(r) ==
    /\ rq[r] = "new" /\ Cur # NoG
    /\ rq' = [rq EXCEPT ![r] = "keyed"]
    /\ rkey' = [rkey EXCEPT ![r] = IF Mutant = "pool-by-agent" THEN NoG ELSE Cur]
    /\ rchg' = [rchg EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<prim, cp, ctr, cst, repl, rs, sc, sk, sr, sst, stale, livecut>>

\* http.Transport takes an idle session pooled under the request's key.
Take(r, s) ==
    /\ rq[r] = "keyed" /\ sst[s] = "idle" /\ sk[s] = rkey[r]
    /\ rq' = [rq EXCEPT ![r] = "sent"]
    /\ rs' = [rs EXCEPT ![r] = s]
    /\ sst' = [sst EXCEPT ![s] = "busy"]
    /\ Wrote(r, s)
    /\ UNCHANGED <<prim, cp, ctr, cst, repl, rkey, rchg, sc, sk, sr, livecut>>

\* Or dials one (DialContext -> DialHTTP -> agentLink.open -> the
\* quiclink's pickServed): on any open connection of the primary current
\* now, which may be newer than the key's.
Dial(r, c) ==
    /\ rq[r] = "keyed" /\ Cur # NoG /\ cp[c] = Cur /\ Open(c)
    /\ rq' = [rq EXCEPT ![r] = "sent"]
    /\ rs' = [rs EXCEPT ![r] = r]
    /\ sc' = [sc EXCEPT ![r] = c]
    /\ sk' = [sk EXCEPT ![r] = rkey[r]]
    /\ sr' = [sr EXCEPT ![r] = cp[c]]
    /\ sst' = [sst EXCEPT ![r] = "busy"]
    /\ stale' = (stale \/ (~Named(c) /\ ~rchg[r]))
    /\ UNCHANGED <<prim, cp, ctr, cst, repl, rkey, rchg, livecut>>

\* The agent answers; the session goes back to the pool under its key,
\* unless it rides another route than that key's: ServeHTTP closes it
\* (no-single-use: pooled anyway).
Respond(r) ==
    /\ rq[r] = "sent" /\ sst[rs[r]] = "busy" /\ cst[sc[rs[r]]] = "live"
    /\ rq' = [rq EXCEPT ![r] = "done"]
    /\ sst' = [sst EXCEPT ![rs[r]] = IF sr[rs[r]] = sk[rs[r]] \/ Mutant \in {"no-single-use", "pool-by-agent"} THEN "idle" ELSE "closed"]
    /\ UNCHANGED <<prim, cp, ctr, cst, repl, rkey, rchg, rs, sc, sk, sr, stale, livecut>>

\* A request on a closed session fails (the reverse proxy's 502, or the
\* transport's retry of a replayable request on a reused one).
Fail(r) ==
    /\ rq[r] = "sent" /\ sst[rs[r]] = "closed"
    /\ rq' = [rq EXCEPT ![r] = "failed"]
    /\ UNCHANGED <<prim, cp, ctr, cst, repl, rkey, rchg, rs, sc, sk, sr, sst, stale, livecut>>

\* IdleConnTimeout (90 s) closes an idle session.
IdleEnd(s) ==
    /\ sst[s] = "idle"
    /\ sst' = [sst EXCEPT ![s] = "closed"]
    /\ UNCHANGED <<prim, cp, ctr, cst, repl, rchg, stale, livecut>> /\ KeepReq

Next ==
    \/ \E g \in Gens : Register(g) \/ PrimEnd(g)
    \/ \E c \in Conns : Lose(c) \/ Close(c) \/ \E g \in Gens, t \in {"tcp", "quic"} : Bind(c, g, t)
    \/ \E r \in Reqs : Key(r) \/ Respond(r) \/ Fail(r) \/ IdleEnd(r)
                       \/ (\E s \in Reqs : Take(r, s)) \/ (\E c \in Conns : Dial(r, c))

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* A request is written only on a session of a connection the directory
\* names for the agent (its current primary's, not replaced), unless a
\* route changed after the request read its key.
NoStaleDispatch == ~stale

\* A replacement closes only connections the agent no longer holds.
NoLiveCut == ~livecut

\* Witnesses (make spec-mutants): each must be violated.
WitnessReuse == ~\E r \in Reqs : rq[r] \in {"sent", "done"} /\ rs[r] # r            \* NoStaleDispatch: a pooled session reused
WitnessReplaced == ~\E c \in Conns : repl[c]                                          \* NoStaleDispatch: a QUIC connection replaced
WitnessRace == ~\E r \in Reqs : rq[r] = "sent" /\ Open(sc[rs[r]]) /\ ~Named(sc[rs[r]]) \* NoStaleDispatch: the race it allows is reached
WitnessGrowth == ~\E c, o \in Conns : c # o /\ cp[c] = cp[o] /\ cp[c] # NoG /\ cst[c] = "live" /\ cst[o] = "live" \* NoLiveCut: two live TCP connections
=============================================================================
