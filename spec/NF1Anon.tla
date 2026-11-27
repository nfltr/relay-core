------------------------------ MODULE NF1Anon ------------------------------
(* An nf1 address served with no account on either side (nfltr            *)
(* docs/decisions/nf1-anon-2026-11-16.md, spec/README.md "NF1Anon.tla"):   *)
(* the relay carries such a session's signalling only (the rendezvous      *)
(* answer, TLS, the proof, the control exchange), never its bulk. An       *)
(* anonymous server registers an address with its registration proof and   *)
(* no credential (NF1Claim.tla checks who may); the registration lives at  *)
(* most Lifetime (the principal's NotAfter), then the relay ends it and    *)
(* the server registers again. The relay, not the peers, is what this      *)
(* spec checks: the client and the agent are arbitrary (a patched SDK      *)
(* sends whatever it likes on its relayed leg). The relay counts every     *)
(* byte of an anonymous session's relayed legs (it cannot tell signalling  *)
(* from bulk: both are ciphertext) against a leg budget and the address's  *)
(* budget, and cuts the leg past either, or past the leg's time limit,     *)
(* with the typed reason account_required, which both ends get (link's     *)
(* ABORT, both ways). A session with an account behind its server is       *)
(* never budgeted. Bytes are units; one unit is one relayed write.         *)
EXTENDS Naturals, FiniteSets

CONSTANTS Sess,      \* the sessions of a behaviour
          LegBudget, \* units one relayed leg of an anonymous session may carry
          AddrBudget,\* units every relayed leg of the anonymous address may carry together
          MaxUnits,  \* units one transfer moves, whatever its path
          Lifetime,  \* ticks an anonymous registration lives
          MaxRegs,   \* anonymous registrations in a behaviour
          Mutant     \* "none", or a bug put back (spec/README.md)

Servers == {"anon", "acct"}  \* the address's server: anonymous, or an account's agent
Routes == {"nf1", "keyed"}   \* an nf1 open, or any other route (<agent>/<svc>, HTTP, A2A)
Reasons == {"", "account_required", "expired", "reset"}
None == "none"

VARIABLES
    reg,    \* the anonymous server's registration: [st, age, n]
    srv,    \* s -> the server it opened, None before
    route,  \* s -> the route it came by, None before
    st,     \* s -> "idle", "relayed", "cutting", "direct", "detached", "done", "failed"
    leg,    \* s -> units its current relayed leg carried
    att,    \* s -> relayed legs after the first (re-attaches, A)
    used,   \* units the relay counted against the anonymous address
    arel,   \* s -> ghost: units the relay carried for s, every leg
    sent,   \* s -> units the transfer moved, any path
    why,    \* s -> [c |-> what the client saw, a |-> what the agent saw]
    act     \* the last step, for the witnesses

vars == <<reg, srv, route, st, leg, att, used, arel, sent, why, act>>

NoWhy == [c |-> "", a |-> ""]

TypeOK ==
    /\ reg.st \in {"none", "live"} /\ reg.age \in 0..Lifetime + 1 /\ reg.n \in 0..MaxRegs
    /\ srv \in [Sess -> Servers \cup {None}]
    /\ route \in [Sess -> Routes \cup {None}]
    /\ st \in [Sess -> {"idle", "relayed", "cutting", "direct", "detached", "done", "failed"}]
    /\ leg \in [Sess -> 0..MaxUnits]
    /\ att \in [Sess -> 0..1]
    /\ used \in 0..MaxUnits * Cardinality(Sess)
    /\ arel \in [Sess -> 0..MaxUnits]
    /\ sent \in [Sess -> 0..MaxUnits]
    /\ why \in [Sess -> [c : Reasons, a : Reasons]]

Init ==
    /\ reg = [st |-> "none", age |-> 0, n |-> 0]
    /\ srv = [s \in Sess |-> None]
    /\ route = [s \in Sess |-> None]
    /\ st = [s \in Sess |-> "idle"]
    /\ leg = [s \in Sess |-> 0]
    /\ att = [s \in Sess |-> 0]
    /\ used = 0
    /\ arel = [s \in Sess |-> 0]
    /\ sent = [s \in Sess |-> 0]
    /\ why = [s \in Sess |-> NoWhy]
    /\ act = [s |-> "Init", x |-> None]

\* The truth: s's server has no account.
Anon(s) == srv[s] = "anon"

\* The relay's rule for which sessions it budgets: an nf1 open whose
\* agent registered with no credential (the principal's provider), every
\* relayed leg of it. "reattach-unbudgeted" budgets only the first leg (a
\* relayed fallback after the direct path died is carried as an account's);
\* "cut-account" budgets every nf1 session, an account's too.
Budgeted(s) ==
    /\ route[s] = "nf1"
    /\ Anon(s) \/ Mutant = "cut-account"
    /\ att[s] = 0 \/ Mutant # "reattach-unbudgeted"

\* Its reason when the relay cuts a leg; "untyped-cut": a plain reset.
CutWhy == IF Mutant = "untyped-cut" THEN "reset" ELSE "account_required"

Others(s) == <<reg, srv, route>>

-----------------------------------------------------------------------------
(* The anonymous server's registration. *)

\* It connects with no credential and its registration proof: the relay
\* takes the declaration (NF1Claim.tla Accepts) and a principal with
\* NotAfter = now + Lifetime. used is the per-source bucket's: a new
\* registration from the same source does not refill it.
Register ==
    /\ reg.st = "none" /\ reg.n < MaxRegs
    /\ reg' = [st |-> "live", age |-> 0, n |-> reg.n + 1]
    /\ act' = [s |-> "Register", x |-> None]
    /\ UNCHANGED <<srv, route, st, leg, att, used, arel, sent, why>>

\* Time passes for the registration.
Tick ==
    /\ reg.st = "live" /\ reg.age < Lifetime
    /\ reg' = [reg EXCEPT !.age = @ + 1]
    /\ act' = [s |-> "Tick", x |-> None]
    /\ UNCHANGED <<srv, route, st, leg, att, used, arel, sent, why>>

\* Its NotAfter: the relay ends the agent's connection, and with it every
\* relayed leg it holds (a direct path is the peers' and lives on).
\* "no-expiry": the relay lets the connection run past it.
Expire ==
    /\ reg.st = "live" /\ reg.age = Lifetime
    /\ IF Mutant = "no-expiry"
       THEN /\ reg' = [reg EXCEPT !.age = Lifetime + 1]
            /\ UNCHANGED <<st, why>>
       ELSE /\ reg' = [reg EXCEPT !.st = "none", !.age = 0]
            /\ st' = [s \in Sess |-> IF Anon(s) /\ st[s] = "relayed" THEN "failed" ELSE st[s]]
            /\ why' = [s \in Sess |-> IF Anon(s) /\ st[s] = "relayed" THEN [c |-> "expired", a |-> "expired"] ELSE why[s]]
    /\ act' = [s |-> "Expire", x |-> None]
    /\ UNCHANGED <<srv, route, leg, att, used, arel, sent>>

-----------------------------------------------------------------------------
(* Sessions. *)

\* A client opens the server by route r. The anonymous server only while
\* registered, and only by an nf1 open: its agent has no account, so no
\* keyed, HTTP or A2A route names it ("keyed-route": one does, and it
\* carries no budget).
Open(s, v, r) ==
    /\ st[s] = "idle"
    /\ v = "anon" => reg.st = "live"
    /\ v = "anon" /\ r = "keyed" => Mutant = "keyed-route"
    /\ srv' = [srv EXCEPT ![s] = v]
    /\ route' = [route EXCEPT ![s] = r]
    /\ st' = [st EXCEPT ![s] = "relayed"]
    /\ act' = [s |-> "Open", x |-> s]
    /\ UNCHANGED <<reg, leg, att, used, arel, sent, why>>

\* Either peer writes one unit on the relayed leg: signalling or bulk, the
\* relay cannot tell. Over the leg's or the address's budget the relay
\* cuts the leg instead; "no-budget": it never does.
Relay(s) ==
    /\ st[s] = "relayed" /\ sent[s] < MaxUnits
    /\ IF /\ Budgeted(s)
          /\ leg[s] + 1 > LegBudget \/ used + 1 > AddrBudget
          /\ Mutant # "no-budget"
       THEN /\ st' = [st EXCEPT ![s] = "cutting"]
            /\ why' = [why EXCEPT ![s].a = CutWhy]
            /\ act' = [s |-> "Cut", x |-> s]
            /\ UNCHANGED <<leg, used, arel, sent>>
       ELSE /\ leg' = [leg EXCEPT ![s] = @ + 1]
            /\ used' = IF Budgeted(s) THEN used + 1 ELSE used
            /\ arel' = [arel EXCEPT ![s] = @ + 1]
            /\ sent' = [sent EXCEPT ![s] = @ + 1]
            /\ act' = [s |-> "Relay", x |-> s]
            /\ UNCHANGED <<st, why>>
    /\ UNCHANGED <<reg, srv, route, att>>

\* The relayed leg's time limit (an anonymous session's relayed leg lives
\* at most the limit, then account_required); "no-time-limit": none.
TimeCut(s) ==
    /\ st[s] = "relayed" /\ Budgeted(s) /\ Mutant # "no-time-limit"
    /\ st' = [st EXCEPT ![s] = "cutting"]
    /\ why' = [why EXCEPT ![s].a = CutWhy]
    /\ act' = [s |-> "TimeCut", x |-> s]
    /\ UNCHANGED <<reg, srv, route, leg, att, used, arel, sent>>

\* A cut (Cut, TimeCut) aborts the agent's leg, then the client's: two
\* steps, and the join between the legs runs in between. Deliver: the
\* cut's abort of the client's leg. JoinEnd: the join's copy toward the
\* client ended first, at the agent's leg the cut aborted, and passes on
\* what the budgeted leg's read returned: the cut's reason;
\* "join-clean-end": that leg's local end, a clean end the client reads as
\* a cut in transit (relay-core v0.16.5, TestNF1AnonLegTime).
Deliver(s) ==
    /\ st[s] = "cutting"
    /\ st' = [st EXCEPT ![s] = "failed"]
    /\ why' = [why EXCEPT ![s].c = why[s].a]
    /\ act' = [s |-> "Deliver", x |-> s]
    /\ UNCHANGED <<reg, srv, route, leg, att, used, arel, sent>>

JoinEnd(s) ==
    /\ st[s] = "cutting"
    /\ st' = [st EXCEPT ![s] = "failed"]
    /\ why' = [why EXCEPT ![s].c = IF Mutant = "join-clean-end" THEN "reset" ELSE why[s].a]
    /\ act' = [s |-> "JoinEnd", x |-> s]
    /\ UNCHANGED <<reg, srv, route, leg, att, used, arel, sent>>

\* The punch succeeded after the control exchange: the session moves to
\* its direct path and releases the relayed leg.
Punch(s) ==
    /\ st[s] = "relayed" /\ leg[s] >= 1
    /\ st' = [st EXCEPT ![s] = "direct"]
    /\ leg' = [leg EXCEPT ![s] = 0]
    /\ act' = [s |-> "Punch", x |-> s]
    /\ UNCHANGED <<reg, srv, route, att, used, arel, sent, why>>

\* A unit on the direct path: the relay never sees it.
Direct(s) ==
    /\ st[s] = "direct" /\ sent[s] < MaxUnits
    /\ sent' = [sent EXCEPT ![s] = @ + 1]
    /\ act' = [s |-> "Direct", x |-> s]
    /\ UNCHANGED <<reg, srv, route, st, leg, att, used, arel, why>>

\* The direct path died (liveness).
Lose(s) ==
    /\ st[s] = "direct" /\ sent[s] < MaxUnits
    /\ st' = [st EXCEPT ![s] = "detached"]
    /\ act' = [s |-> "Lose", x |-> s]
    /\ UNCHANGED <<reg, srv, route, leg, att, used, arel, sent, why>>

\* A relayed re-attach (A, a new nf1 open to the address): the honest SDK
\* does not re-attach an anonymous session, but a patched one may.
Reattach(s) ==
    /\ st[s] = "detached" /\ att[s] = 0
    /\ srv[s] = "anon" => reg.st = "live"
    /\ st' = [st EXCEPT ![s] = "relayed"]
    /\ att' = [att EXCEPT ![s] = 1]
    /\ act' = [s |-> "Reattach", x |-> s]
    /\ UNCHANGED <<reg, srv, route, leg, used, arel, sent, why>>

\* The client gives up waiting for a direct path (the SDK's direct-required
\* bound): account_required to both ends (an ABORT with that reason).
GiveUp(s) ==
    /\ st[s] \in {"relayed", "detached"} /\ Anon(s)
    /\ st' = [st EXCEPT ![s] = "failed"]
    /\ why' = [why EXCEPT ![s] = [c |-> "account_required", a |-> "account_required"]]
    /\ act' = [s |-> "GiveUp", x |-> s]
    /\ UNCHANGED <<reg, srv, route, leg, att, used, arel, sent>>

\* The whole transfer moved: the session ends cleanly.
Done(s) ==
    /\ st[s] \in {"relayed", "direct"} /\ sent[s] = MaxUnits
    /\ st' = [st EXCEPT ![s] = "done"]
    /\ act' = [s |-> "Done", x |-> s]
    /\ UNCHANGED <<reg, srv, route, leg, att, used, arel, sent, why>>

Next ==
    \/ Register \/ Tick \/ Expire
    \/ \E s \in Sess :
        \/ \E v \in Servers, r \in Routes : Open(s, v, r)
        \/ Relay(s) \/ TimeCut(s) \/ Deliver(s) \/ JoinEnd(s) \/ Punch(s) \/ Direct(s) \/ Lose(s)
        \/ Reattach(s) \/ GiveUp(s) \/ Done(s)

Spec == Init /\ [][Next]_vars

\* The relay's time limit on an anonymous relayed leg is fair (shorter
\* than the registration's lifetime, whose end is not needed here).
LiveSpec == Spec /\ \A s \in Sess : WF_vars(TimeCut(s))

-----------------------------------------------------------------------------
(* Invariants. *)

\* No relayed leg of an anonymous session carries more than LegBudget.
LegBounded == \A s \in Sess : Anon(s) => leg[s] <= LegBudget

\* Every relayed leg of the anonymous address together carries at most
\* AddrBudget: no bulk through the relay, by chunking into sessions or by
\* re-attaching.
AddrBounded ==
    LET RECURSIVE Sum(_)
        Sum(S) == IF S = {} THEN 0 ELSE LET s == CHOOSE x \in S : TRUE IN arel[s] + Sum(S \ {s})
    IN Sum({s \in Sess : Anon(s)}) <= AddrBudget

\* The cut is never forced on a session with an account behind it.
NoCutOnAccount == \A s \in Sess : why[s].c = "account_required" => Anon(s)

\* A failed session ends with a typed reason, the same at both ends.
TypedFailure ==
    \A s \in Sess : st[s] = "failed" => why[s].c = why[s].a /\ why[s].c \in {"account_required", "expired"}

\* A registration lives at most its lifetime.
RegBounded == reg.st = "live" => reg.age <= Lifetime

\* Liveness (NF1AnonLive.cfg): an anonymous session does not stay relayed.
LeavesRelay == \A s \in Sess : [](st[s] = "relayed" /\ Anon(s) => <>(st[s] # "relayed"))

\* Witnesses: each must be violated (its state reached).
WitnessDirectDone == ~\E s \in Sess : Anon(s) /\ st[s] = "done" /\ arel[s] < MaxUnits
WitnessCut == ~\E s \in Sess : Anon(s) /\ act.s = "Cut" /\ act.x = s
WitnessAccountRelayed == ~\E s \in Sess : ~Anon(s) /\ st[s] = "done" /\ arel[s] > LegBudget
WitnessReattachCut == ~\E s \in Sess : Anon(s) /\ att[s] = 1 /\ st[s] = "failed"
WitnessRenewed == reg.n < 2
WitnessDirectOutlivesReg == ~\E s \in Sess : Anon(s) /\ st[s] = "direct" /\ reg.st = "none"
=============================================================================
