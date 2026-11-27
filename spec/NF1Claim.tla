------------------------------ MODULE NF1Claim ------------------------------
(* Which agent an nf1 capability address's open reaches when several      *)
(* agents' records declare its rendezvous id (nfltr docs/decisions/        *)
(* nf1-risks-2026-11-14.md, spec/README.md "NF1Claim.tla"). Agents are    *)
(* agent ids: a reconnect or a replica takeover of one agent replaces its  *)
(* own record (Claim again), never adds a second one; Directory.tla checks *)
(* that record's own lifecycle. Holders hold the address's server key      *)
(* (the owner's servers: one cat serve, a copy of its key file, the same   *)
(* key under another agent name); Others hold at most the address (its    *)
(* clients). A declaration of the rid is accepted only with a              *)
(* registration proof (rid = H("nf1-rid", n, spki) and a signature by that *)
(* key over ("nf1-reg", R, rid)), which only Holders can make (relay       *)
(* spec/proverif/cap_address.pv queries 27, 28). The index set and the     *)
(* record are written in one store step and pruned in one (Directory.tla   *)
(* IndexComplete). One open at a time, repeated: the snapshot of the set,  *)
(* each member's record read in its own step, then the dial, newest claim  *)
(* first, falling back to the next on a failed dial.                       *)
(* The replacement window (lane NF1ROUTE): an agent id's connections come  *)
(* and go; a new connection writes its record (ConnectHTTP) before its     *)
(* ConnectTCP stream registers, and until then the agent's newest stream   *)
(* is the replaced connection's, which may hold another key (an earlier    *)
(* address). strm is what that stream's connection declared: "rid", the   *)
(* rid (accepted, so with its proof), "other" anything else.               *)
EXTENDS Naturals, FiniteSets

CONSTANTS Holders,   \* agents holding the server key
          Others,    \* agents holding at most the address
          MaxClaims, \* claims in a behaviour
          Mutant     \* "none", or a bug put back (spec/README.md)

Agents == Holders \cup Others
None == "none"
Lost == "lost"  \* a link refusal passed on: the agent's leg ended before it answered

VARIABLES
    rec,  \* agent -> the claim stamp of its live record declaring the rid, 0: none
    idx,  \* the index set inventory:nf1:<rid>
    n,    \* claims so far (the stamps)
    op,   \* the open in progress
    act,  \* the last step, for the witnesses
    strm, \* agent -> what its newest ConnectTCP stream's connection declared: "rid", "other"
    pend  \* agent -> what its newest connection, its stream not yet registered, declared; "none"

vars == <<rec, idx, n, op, act, strm, pend>>

IdleOp == [pc |-> "idle", left |-> {}, got |-> [a \in Agents |-> 0], tried |-> {}, res |-> None, steady |-> {}, lost |-> FALSE, via |-> None]

TypeOK ==
    /\ rec \in [Agents -> 0..MaxClaims]
    /\ idx \subseteq Agents
    /\ n \in 0..MaxClaims
    /\ op.pc \in {"idle", "read", "dial", "answer", "done"} /\ op.lost \in BOOLEAN
    /\ op.left \subseteq Agents /\ op.tried \subseteq Agents /\ op.steady \subseteq Holders
    /\ op.got \in [Agents -> 0..MaxClaims]
    /\ op.res \in Agents \cup {None, Lost}
    /\ op.via \in {None, "rid", "other"}
    /\ strm \in [Agents -> {"rid", "other"}]
    /\ pend \in [Agents -> {"none", "rid", "other"}]

Init ==
    /\ rec = [a \in Agents |-> 0]
    /\ idx = {}
    /\ n = 0
    /\ op = IdleOp
    /\ act = [s |-> "Init", a |-> None]
    /\ strm = [a \in Agents |-> "other"] \* an earlier connection's, or none
    /\ pend = [a \in Agents |-> "none"]

\* The relay's check of a declaration's registration proof. "no-proof" is
\* the code before this design: any declaration of the rid is taken.
\* "refuse-second" is the rejected rule: a declaration is refused while
\* another agent's live record declares the rid.
Accepts(a) ==
    /\ a \in Holders \/ Mutant = "no-proof"
    /\ Mutant = "refuse-second" => \A b \in Agents \ {a} : rec[b] = 0

\* An agent connects, reconnects or takes over on another replica:
\* ClaimSession writes its record and the index member in one step
\* (Cache.SetAdd); a refused declaration writes nothing about the rid.
Claim(a) ==
    /\ n < MaxClaims
    /\ IF Accepts(a)
       THEN /\ n' = n + 1
            /\ rec' = [rec EXCEPT ![a] = n + 1]
            /\ idx' = idx \cup {a}
            /\ act' = [s |-> "Claim", a |-> a]
            \* its ConnectTCP stream registers later (Register): the window
            /\ pend' = [pend EXCEPT ![a] = "rid"]
       ELSE /\ act' = [s |-> "Refused", a |-> a]
            /\ UNCHANGED <<rec, idx, n, pend>>
    /\ UNCHANGED <<op, strm>>

\* A holder whose record goes stops being steady for the open in progress.
Gone(a) == op' = [op EXCEPT !.steady = @ \ {a}]

\* A new connection of agent a that declares another address (the owner
\* restarted it with a fresh key under the same name): its record no
\* longer declares the rid (one store step, as Claim's; the member stays
\* until a read prunes it), its stream registers later.
Replace(a) ==
    /\ n < MaxClaims /\ rec[a] # 0
    /\ n' = n + 1
    /\ rec' = [rec EXCEPT ![a] = 0]
    /\ pend' = [pend EXCEPT ![a] = "other"]
    /\ Gone(a)
    /\ act' = [s |-> "Replace", a |-> a]
    /\ UNCHANGED <<idx, strm>>

\* The newest connection's ConnectTCP stream registers (enterRotation, in
\* session order): it becomes the agent's newest stream here.
Register(a) ==
    /\ pend[a] # "none"
    /\ strm' = [strm EXCEPT ![a] = pend[a]]
    /\ pend' = [pend EXCEPT ![a] = "none"]
    /\ act' = [s |-> "Register", a |-> a]
    /\ UNCHANGED <<rec, idx, n, op>>

\* The agent's session ends: DeleteSession, its record and member in one
\* step (Cache.DelIf on its own session).
End(a) ==
    /\ rec[a] # 0
    /\ rec' = [rec EXCEPT ![a] = 0]
    /\ idx' = idx \ {a}
    /\ Gone(a)
    /\ act' = [s |-> "End", a |-> a]
    /\ UNCHANGED <<n, strm, pend>>

\* Its record's TTL passed (renewals late, or the agent crashed): the
\* member stays until a read prunes it.
Expire(a) ==
    /\ rec[a] # 0
    /\ rec' = [rec EXCEPT ![a] = 0]
    /\ Gone(a)
    /\ act' = [s |-> "Expire", a |-> a]
    /\ UNCHANGED <<idx, n, strm, pend>>

\* A read's prune of a member whose record is gone, in one step.
Prune(a) ==
    /\ a \in idx /\ rec[a] = 0
    /\ idx' = idx \ {a}
    /\ act' = [s |-> "Prune", a |-> a]
    /\ UNCHANGED <<rec, n, op, strm, pend>>

\* NF1Agents: the snapshot of the set (SMEMBERS). steady: the holders
\* whose record is live now; one leaves it when its record goes.
Snap ==
    /\ op.pc = "idle"
    /\ op' = [IdleOp EXCEPT !.pc = "read", !.left = idx,
                            !.steady = {h \in Holders : rec[h] # 0}]
    /\ act' = [s |-> "Snap", a |-> None]
    /\ UNCHANGED <<rec, idx, n, strm, pend>>

\* NF1Agents: one member's record (GET), in its own step.
Read(a) ==
    /\ op.pc = "read" /\ a \in op.left
    /\ op' = [op EXCEPT !.left = @ \ {a}, !.got[a] = rec[a]]
    /\ act' = [s |-> "Read", a |-> a]
    /\ UNCHANGED <<rec, idx, n, strm, pend>>

Cands == {a \in Agents : op.got[a] # 0}

\* openNF1's rule over what was read: no live record, not served;
\* otherwise dial. "exactly-one" is the code before this design.
Decide ==
    /\ op.pc = "read" /\ op.left = {}
    /\ IF Cands = {} \/ (Mutant = "exactly-one" /\ Cardinality(Cands) # 1)
       THEN op' = [op EXCEPT !.pc = "done", !.res = None]
       ELSE op' = [op EXCEPT !.pc = "dial"]
    /\ act' = [s |-> "Decide", a |-> None]
    /\ UNCHANGED <<rec, idx, n, strm, pend>>

\* The newest claim not yet tried (the latest of the owner's servers to
\* connect is the likeliest live; no invariant depends on the order: TLC
\* finds none broken with the oldest first).
Next1 ==
    LET left == Cands \ op.tried
    IN CHOOSE a \in left : \A b \in left : op.got[a] >= op.got[b]

\* An nf1 open takes the agent's newest stream only when that stream's
\* connection declared the rid ("route-by-id": any stream of the agent
\* id, the code before NF1ROUTE).
Routes(a) == strm[a] = "rid" \/ Mutant = "route-by-id"

\* The open waits (bounded, nf1Stream) while the agent's newest stream is
\* not one that declared the rid and the connection that did has not
\* registered its stream yet. The bound is an assumption (spec/README.md
\* "NF1Claim.tla"): the stream registers within it. "no-wait": the open
\* takes the window as a failed dial.
Waits(a) == ~Routes(a) /\ pend[a] = "rid" /\ Mutant # "no-wait"

\* The dial (AgentServices, then OnwardWithRetry): it reaches the agent
\* while its record is live and its newest stream declared the rid; else
\* the next candidate ("no-fallback": the first failed dial answers not
\* served).
Dial ==
    /\ op.pc = "dial"
    /\ LET a == Next1
       IN /\ rec[a] # 0 => ~Waits(a)
          /\ IF rec[a] # 0 /\ Routes(a)
             THEN op' = [op EXCEPT !.pc = "answer", !.res = a, !.via = strm[a]]
             ELSE IF Mutant = "no-fallback" \/ Cands \ (op.tried \cup {a}) = {}
             THEN op' = [op EXCEPT !.pc = "done", !.res = None, !.tried = @ \cup {a}]
             ELSE op' = [op EXCEPT !.tried = @ \cup {a}]
    /\ act' = [s |-> "Dial", a |-> None]
    /\ UNCHANGED <<rec, idx, n, strm, pend>>

\* The agent's answer (FeatureChain: the agent answers the open itself).
\* An agent gone before it answered (its link ended, its record since or
\* soon): its leg's link refusal (connection lost, draining) is the one
\* answer not served, never passed on (lane PIPERESET2: relay-core
\* TestNF1CapsAndNotServed read "relay unreachable"; mutant lost-answer,
\* the code before). No other candidate is tried: the client's first
\* bytes went to this leg.
Answer ==
    /\ op.pc = "answer"
    /\ IF rec[op.res] # 0
       THEN op' = [op EXCEPT !.pc = "done"]
       ELSE op' = [op EXCEPT !.pc = "done", !.lost = TRUE,
                             !.res = IF Mutant = "lost-answer" THEN Lost ELSE None]
    /\ act' = [s |-> "Answer", a |-> None]
    /\ UNCHANGED <<rec, idx, n, strm, pend>>

\* The open's answer went back; the next open starts.
Reset ==
    /\ op.pc = "done"
    /\ op' = IdleOp
    /\ act' = [s |-> "Reset", a |-> None]
    /\ UNCHANGED <<rec, idx, n, strm, pend>>

Next ==
    \/ \E a \in Agents : Claim(a) \/ Replace(a) \/ Register(a) \/ End(a) \/ Expire(a) \/ Prune(a) \/ Read(a)
    \/ Snap \/ Decide \/ Dial \/ Answer \/ Reset

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants. *)

\* A live record of the rid has its agent in the index (Directory.tla's).
IndexComplete == \A a \in Agents : rec[a] # 0 => a \in idx

\* The relay routes an address only to an agent holding its server key
\* (with the pin, the rest refuse at the client anyway: this is the
\* duplicate that would make every open fail), on a stream of a connection
\* that declared the rid: never a replaced or sibling connection under the
\* agent's id that holds another key (lane NF1ROUTE; TestNF1RelayCap, 8 of
\* 20 runs under -race: "server key does not match the address's pin").
RouteSound == op.pc = "done" /\ op.res \in Agents => op.res \in Holders /\ op.via = "rid"

\* Unknown, offline and disabled are one answer: an open answers served or
\* not served, never a link refusal that tells an agent left (no
\* enumeration of recently served addresses).
OneAnswer == op.res # Lost

\* No duplicate-address denial: an open answers not served only if no
\* key holder's record was live from its snapshot to its answer.
NoFalseNotServed == op.pc = "done" /\ op.res = None /\ ~op.lost => op.steady = {}

\* A key holder's declaration is never refused (the owner's second server,
\* or its new machine while the old record waits for its TTL).
NoHolderRefused == act.s = "Refused" => act.a \notin Holders

\* Witnesses: each must be violated (its state reached).
WitnessTwoServing == Cardinality({h \in Holders : rec[h] # 0}) < 2
WitnessRoutedAmongTwo == ~(op.pc = "done" /\ op.res # None /\ Cardinality(Cands) >= 2)
WitnessFallback == ~(op.pc = "done" /\ op.res # None /\ op.tried # {})
WitnessRefused == act.s # "Refused"
WitnessNotServed == ~(op.pc = "done" /\ op.res = None /\ Cands # {})
WitnessLostAnswer == ~(op.pc = "done" /\ op.lost)
\* An open's candidate in the replacement window: its record live, its
\* newest stream the replaced connection's, the declaring one pending.
WitnessWindow == ~(op.pc = "dial" /\ rec[Next1] # 0 /\ strm[Next1] = "other" /\ pend[Next1] = "rid")
=============================================================================
