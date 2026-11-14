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
(* spec/proverif/cap_address.pv queries 25, 26). The index set and the     *)
(* record are written in one store step and pruned in one (Directory.tla   *)
(* IndexComplete). One open at a time, repeated: the snapshot of the set,  *)
(* each member's record read in its own step, then the dial, newest claim  *)
(* first, falling back to the next on a failed dial.                       *)
EXTENDS Naturals, FiniteSets

CONSTANTS Holders,   \* agents holding the server key
          Others,    \* agents holding at most the address
          MaxClaims, \* claims in a behaviour
          Mutant     \* "none", or a bug put back (spec/README.md)

Agents == Holders \cup Others
None == "none"

VARIABLES
    rec,  \* agent -> the claim stamp of its live record declaring the rid, 0: none
    idx,  \* the index set inventory:nf1:<rid>
    n,    \* claims so far (the stamps)
    op,   \* the open in progress
    act   \* the last step, for the witnesses

vars == <<rec, idx, n, op, act>>

IdleOp == [pc |-> "idle", left |-> {}, got |-> [a \in Agents |-> 0], tried |-> {}, res |-> None, steady |-> {}]

TypeOK ==
    /\ rec \in [Agents -> 0..MaxClaims]
    /\ idx \subseteq Agents
    /\ n \in 0..MaxClaims
    /\ op.pc \in {"idle", "read", "dial", "done"}
    /\ op.left \subseteq Agents /\ op.tried \subseteq Agents /\ op.steady \subseteq Holders
    /\ op.got \in [Agents -> 0..MaxClaims]
    /\ op.res \in Agents \cup {None}

Init ==
    /\ rec = [a \in Agents |-> 0]
    /\ idx = {}
    /\ n = 0
    /\ op = IdleOp
    /\ act = [s |-> "Init", a |-> None]

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
       ELSE /\ act' = [s |-> "Refused", a |-> a]
            /\ UNCHANGED <<rec, idx, n>>
    /\ UNCHANGED op

\* A holder whose record goes stops being steady for the open in progress.
Gone(a) == op' = [op EXCEPT !.steady = @ \ {a}]

\* The agent's session ends: DeleteSession, its record and member in one
\* step (Cache.DelIf on its own session).
End(a) ==
    /\ rec[a] # 0
    /\ rec' = [rec EXCEPT ![a] = 0]
    /\ idx' = idx \ {a}
    /\ Gone(a)
    /\ act' = [s |-> "End", a |-> a]
    /\ UNCHANGED n

\* Its record's TTL passed (renewals late, or the agent crashed): the
\* member stays until a read prunes it.
Expire(a) ==
    /\ rec[a] # 0
    /\ rec' = [rec EXCEPT ![a] = 0]
    /\ Gone(a)
    /\ act' = [s |-> "Expire", a |-> a]
    /\ UNCHANGED <<idx, n>>

\* A read's prune of a member whose record is gone, in one step.
Prune(a) ==
    /\ a \in idx /\ rec[a] = 0
    /\ idx' = idx \ {a}
    /\ act' = [s |-> "Prune", a |-> a]
    /\ UNCHANGED <<rec, n, op>>

\* NF1Agents: the snapshot of the set (SMEMBERS). steady: the holders
\* whose record is live now; one leaves it when its record goes.
Snap ==
    /\ op.pc = "idle"
    /\ op' = [IdleOp EXCEPT !.pc = "read", !.left = idx,
                            !.steady = {h \in Holders : rec[h] # 0}]
    /\ act' = [s |-> "Snap", a |-> None]
    /\ UNCHANGED <<rec, idx, n>>

\* NF1Agents: one member's record (GET), in its own step.
Read(a) ==
    /\ op.pc = "read" /\ a \in op.left
    /\ op' = [op EXCEPT !.left = @ \ {a}, !.got[a] = rec[a]]
    /\ act' = [s |-> "Read", a |-> a]
    /\ UNCHANGED <<rec, idx, n>>

Cands == {a \in Agents : op.got[a] # 0}

\* openNF1's rule over what was read: no live record, not served;
\* otherwise dial. "exactly-one" is the code before this design.
Decide ==
    /\ op.pc = "read" /\ op.left = {}
    /\ IF Cands = {} \/ (Mutant = "exactly-one" /\ Cardinality(Cands) # 1)
       THEN op' = [op EXCEPT !.pc = "done", !.res = None]
       ELSE op' = [op EXCEPT !.pc = "dial"]
    /\ act' = [s |-> "Decide", a |-> None]
    /\ UNCHANGED <<rec, idx, n>>

\* The newest claim not yet tried (the latest of the owner's servers to
\* connect is the likeliest live; no invariant depends on the order: TLC
\* finds none broken with the oldest first).
Next1 ==
    LET left == Cands \ op.tried
    IN CHOOSE a \in left : \A b \in left : op.got[a] >= op.got[b]

\* The dial (AgentServices, then OnwardWithRetry): it reaches the agent
\* while its record is live; else the next candidate ("no-fallback": the
\* first failed dial answers not served).
Dial ==
    /\ op.pc = "dial"
    /\ LET a == Next1
       IN IF rec[a] # 0
          THEN op' = [op EXCEPT !.pc = "done", !.res = a]
          ELSE IF Mutant = "no-fallback" \/ Cands \ (op.tried \cup {a}) = {}
          THEN op' = [op EXCEPT !.pc = "done", !.res = None, !.tried = @ \cup {a}]
          ELSE op' = [op EXCEPT !.tried = @ \cup {a}]
    /\ act' = [s |-> "Dial", a |-> None]
    /\ UNCHANGED <<rec, idx, n>>

\* The open's answer went back; the next open starts.
Reset ==
    /\ op.pc = "done"
    /\ op' = IdleOp
    /\ act' = [s |-> "Reset", a |-> None]
    /\ UNCHANGED <<rec, idx, n>>

Next ==
    \/ \E a \in Agents : Claim(a) \/ End(a) \/ Expire(a) \/ Prune(a) \/ Read(a)
    \/ Snap \/ Decide \/ Dial \/ Reset

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants. *)

\* A live record of the rid has its agent in the index (Directory.tla's).
IndexComplete == \A a \in Agents : rec[a] # 0 => a \in idx

\* The relay routes an address only to an agent holding its server key
\* (with the pin, the rest refuse at the client anyway: this is the
\* duplicate that would make every open fail).
RouteSound == op.pc = "done" /\ op.res # None => op.res \in Holders

\* No duplicate-address denial: an open answers not served only if no
\* key holder's record was live from its snapshot to its answer.
NoFalseNotServed == op.pc = "done" /\ op.res = None => op.steady = {}

\* A key holder's declaration is never refused (the owner's second server,
\* or its new machine while the old record waits for its TTL).
NoHolderRefused == act.s = "Refused" => act.a \notin Holders

\* Witnesses: each must be violated (its state reached).
WitnessTwoServing == Cardinality({h \in Holders : rec[h] # 0}) < 2
WitnessRoutedAmongTwo == ~(op.pc = "done" /\ op.res # None /\ Cardinality(Cands) >= 2)
WitnessFallback == ~(op.pc = "done" /\ op.res # None /\ op.tried # {})
WitnessRefused == act.s # "Refused"
WitnessNotServed == ~(op.pc = "done" /\ op.res = None /\ Cands # {})
=============================================================================
