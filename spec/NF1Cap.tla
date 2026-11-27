------------------------------ MODULE NF1Cap ------------------------------
(* The owner's daily cap on an nf1 address's relayed bytes (nfltr         *)
(* docs/decisions/nf1-relay-cap-2026-11-25.md, spec/README.md             *)
(* "NF1Cap.tla"). An address served with an account: its owner pays for   *)
(* every relayed byte of its sessions, whoever opens them (OpenMeter). A   *)
(* leaked address must cost the owner at most the cap the owner set: the  *)
(* relay counts every relayed byte of a session whose client is not the   *)
(* owner's own account against the address (its rid, not the agent's      *)
(* connection) for the current UTC day, carries what fits, and past the   *)
(* cap refuses new opens and cuts open relayed legs both ways with        *)
(* limit_exceeded. The owner sets the cap on its agent (a label); the      *)
(* relay learns a change (the agent's label update here, a re-read of its *)
(* record elsewhere) and applies it to every open session's next write.   *)
(* The peers are arbitrary: a client writes whatever it likes on its      *)
(* relayed legs and opens as many as it likes. A direct path's bytes      *)
(* never cross the relay. Bytes are units; one unit is one relayed write. *)
EXTENDS Naturals, FiniteSets

CONSTANTS Sess,      \* the sessions of a behaviour
          Caps,      \* the caps the owner may set (units a day)
          MaxUnits,  \* units one transfer moves, whatever its path
          MaxSets,   \* cap changes in a behaviour
          MaxDays,   \* day boundaries in a behaviour
          MaxConns,  \* agent reconnections in a behaviour
          Mutant     \* "none", or a bug put back (spec/README.md)

Clients == {"stranger", "owner"}  \* no account or another account; the owner's own
Reasons == {"", "limit_exceeded", "reset"}
None == "none"
Max(a, b) == IF a > b THEN a ELSE b

VARIABLES
    ocap,   \* the cap the owner set (the agent's label)
    rcap,   \* the cap the relay applies to the address
    used,   \* the relay's counter: units counted against the address today
    cli,    \* s -> who opened it, None before
    st,     \* s -> "idle", "relayed", "direct", "refused", "failed", "done"
    att,    \* s -> relayed legs after the first (re-attach or control session)
    scap,   \* s -> the cap the relay read at s's open ("stale-cap" uses it)
    rel,    \* s -> ghost: units the relay carried for s today, every leg
    sent,   \* s -> units the transfer moved, any path
    why,    \* s -> [c |-> what the client saw, a |-> what the agent saw]
    hi,     \* ghost: today's bound, the relay's cap or what was counted when it was lowered
    n,      \* ghost: [sets, days, conns] steps taken
    act     \* the last step, for the witnesses

vars == <<ocap, rcap, used, cli, st, att, scap, rel, sent, why, hi, n, act>>

CapMax == CHOOSE c \in Caps : \A d \in Caps : d <= c
CapMin == CHOOSE c \in Caps : \A d \in Caps : c <= d
NoWhy == [c |-> "", a |-> ""]

TypeOK ==
    /\ ocap \in Caps /\ rcap \in Caps
    /\ used \in 0..MaxUnits * Cardinality(Sess) * 2
    /\ cli \in [Sess -> Clients \cup {None}]
    /\ st \in [Sess -> {"idle", "relayed", "direct", "refused", "failed", "done"}]
    /\ att \in [Sess -> 0..1]
    /\ scap \in [Sess -> Caps]
    /\ rel \in [Sess -> 0..MaxUnits]
    /\ sent \in [Sess -> 0..MaxUnits]
    /\ why \in [Sess -> [c : Reasons, a : Reasons]]
    /\ hi \in Nat
    /\ n \in [sets : 0..MaxSets, days : 0..MaxDays, conns : 0..MaxConns]

Init ==
    /\ ocap = CapMax /\ rcap = CapMax
    /\ used = 0
    /\ cli = [s \in Sess |-> None]
    /\ st = [s \in Sess |-> "idle"]
    /\ att = [s \in Sess |-> 0]
    /\ scap = [s \in Sess |-> CapMax]
    /\ rel = [s \in Sess |-> 0]
    /\ sent = [s \in Sess |-> 0]
    /\ why = [s \in Sess |-> NoWhy]
    /\ hi = CapMax
    /\ n = [sets |-> 0, days |-> 0, conns |-> 0]
    /\ act = [s |-> "Init", x |-> None]

\* The relay's rule for which relayed legs count: every leg of a session
\* whose client is not the owner's own account. "count-owner": the owner's
\* too; "uncounted-reattach": only the first leg (a control session or a
\* relayed re-attach carried free).
Counted(s) ==
    /\ cli[s] = "stranger" \/ Mutant = "count-owner"
    /\ att[s] = 0 \/ Mutant # "uncounted-reattach"

\* The cap the relay checks a write of s against; "stale-cap": the one it
\* read at s's open.
CapOf(s) == IF Mutant = "stale-cap" THEN scap[s] ELSE rcap

\* Its reason when the relay cuts or refuses; "untyped-cut": a plain reset.
CutWhy == IF Mutant = "untyped-cut" THEN "reset" ELSE "limit_exceeded"

\* Units a counted relayed write adds to the counter; "double-count": both
\* the read and the write side of the join count it.
Unit == IF Mutant = "double-count" THEN 2 ELSE 1

-----------------------------------------------------------------------------
(* The owner and the relay. *)

\* The owner sets another cap on its agent (the label, --relay-cap).
SetCap(c) ==
    /\ n.sets < MaxSets /\ c # ocap
    /\ ocap' = c
    /\ n' = [n EXCEPT !.sets = @ + 1]
    /\ act' = [s |-> "SetCap", x |-> None]
    /\ UNCHANGED <<rcap, used, cli, st, att, scap, rel, sent, why, hi>>

\* The relay learns the owner's cap: the agent's label update on its
\* replica, a re-read of the agent's record on another (at most every
\* refresh interval). Lowered, today's bound is the new cap or what was
\* counted already. "no-learn": the relay reads the cap only at the
\* agent's connection (LearnsCap).
Learn ==
    /\ rcap # ocap /\ Mutant # "no-learn"
    /\ rcap' = ocap
    /\ hi' = Max(ocap, used)
    /\ act' = [s |-> "Learn", x |-> None]
    /\ UNCHANGED <<ocap, used, cli, st, att, scap, rel, sent, why, n>>

\* The UTC day ends: the counter starts again at zero.
NewDay ==
    /\ n.days < MaxDays
    /\ used' = 0
    /\ rel' = [s \in Sess |-> 0]
    /\ hi' = rcap
    /\ n' = [n EXCEPT !.days = @ + 1]
    /\ act' = [s |-> "NewDay", x |-> None]
    /\ UNCHANGED <<ocap, rcap, cli, st, att, scap, sent, why>>

\* The serving agent reconnects (a new connection, the same address): the
\* counter is the address's and lives on. "reset-on-reconnect": keyed by
\* the agent's connection, a new one starts at zero.
Reconnect ==
    /\ n.conns < MaxConns
    /\ used' = IF Mutant = "reset-on-reconnect" THEN 0 ELSE used
    /\ n' = [n EXCEPT !.conns = @ + 1]
    /\ act' = [s |-> "Reconnect", x |-> None]
    /\ UNCHANGED <<ocap, rcap, cli, st, att, scap, rel, sent, why, hi>>

-----------------------------------------------------------------------------
(* Sessions. *)

\* A client c opens the address: refused with the typed reason when it
\* counts and today's counter is at the cap ("open-past-cap": opened).
Open(s, c) ==
    /\ st[s] = "idle"
    /\ cli' = [cli EXCEPT ![s] = c]
    /\ scap' = [scap EXCEPT ![s] = rcap]
    /\ IF /\ c = "stranger" \/ Mutant = "count-owner"
          /\ used >= rcap
          /\ Mutant # "open-past-cap"
       THEN /\ st' = [st EXCEPT ![s] = "refused"]
            /\ why' = [why EXCEPT ![s].c = CutWhy]
       ELSE /\ st' = [st EXCEPT ![s] = "relayed"]
            /\ UNCHANGED why
    /\ act' = [s |-> "Open", x |-> s]
    /\ UNCHANGED <<ocap, rcap, used, att, rel, sent, hi, n>>

\* Either peer writes one unit on the relayed leg. A counted write past the
\* cap is not carried: the relay cuts both legs with the reason instead
\* ("no-cap": it never does).
Relay(s) ==
    /\ st[s] = "relayed" /\ sent[s] < MaxUnits
    /\ IF /\ Counted(s)
          /\ used + 1 > CapOf(s)
          /\ Mutant # "no-cap"
       THEN /\ st' = [st EXCEPT ![s] = "failed"]
            /\ why' = [why EXCEPT ![s] = [c |-> CutWhy, a |-> CutWhy]]
            /\ act' = [s |-> "Cut", x |-> s]
            /\ UNCHANGED <<used, rel, sent>>
       ELSE /\ used' = IF Counted(s) THEN used + Unit ELSE used
            /\ rel' = [rel EXCEPT ![s] = @ + 1]
            /\ sent' = [sent EXCEPT ![s] = @ + 1]
            /\ act' = [s |-> "Relay", x |-> s]
            /\ UNCHANGED <<st, why>>
    /\ UNCHANGED <<ocap, rcap, cli, att, scap, hi, n>>

\* The punch succeeded: the session moves to its direct path.
Punch(s) ==
    /\ st[s] = "relayed"
    /\ st' = [st EXCEPT ![s] = "direct"]
    /\ act' = [s |-> "Punch", x |-> s]
    /\ UNCHANGED <<ocap, rcap, used, cli, att, scap, rel, sent, why, hi, n>>

\* A unit on the direct path: the relay never sees it.
Direct(s) ==
    /\ st[s] = "direct" /\ sent[s] < MaxUnits
    /\ sent' = [sent EXCEPT ![s] = @ + 1]
    /\ act' = [s |-> "Direct", x |-> s]
    /\ UNCHANGED <<ocap, rcap, used, cli, st, att, scap, rel, why, hi, n>>

\* The direct path died and the session re-attaches relayed (A), or a
\* second relayed leg (the control session C) of the same session.
Reattach(s) ==
    /\ st[s] = "direct" /\ att[s] = 0 /\ sent[s] < MaxUnits
    /\ st' = [st EXCEPT ![s] = "relayed"]
    /\ att' = [att EXCEPT ![s] = 1]
    /\ act' = [s |-> "Reattach", x |-> s]
    /\ UNCHANGED <<ocap, rcap, used, cli, scap, rel, sent, why, hi, n>>

\* The whole transfer moved: the session ends cleanly.
Done(s) ==
    /\ st[s] \in {"relayed", "direct"} /\ sent[s] = MaxUnits
    /\ st' = [st EXCEPT ![s] = "done"]
    /\ act' = [s |-> "Done", x |-> s]
    /\ UNCHANGED <<ocap, rcap, used, cli, att, scap, rel, sent, why, hi, n>>

Next ==
    \/ \E c \in Caps : SetCap(c)
    \/ Learn \/ NewDay \/ Reconnect
    \/ \E s \in Sess :
        \/ \E c \in Clients : Open(s, c)
        \/ Relay(s) \/ Punch(s) \/ Direct(s) \/ Reattach(s) \/ Done(s)

Spec == Init /\ [][Next]_vars

\* The relay learns a changed cap (its refresh interval is a bound).
LiveSpec == Spec /\ WF_vars(Learn)

-----------------------------------------------------------------------------
(* Invariants. *)

RECURSIVE SumOver(_)
SumOver(S) == IF S = {} THEN 0 ELSE LET s == CHOOSE x \in S : TRUE IN rel[s] + SumOver(S \ {s})

\* Today's relayed units of strangers' sessions.
Strangers == SumOver({s \in Sess : cli[s] = "stranger"})

\* Every relayed unit of a stranger's session is counted once, against the
\* address: the counter is exactly their sum.
CountedOnce == used = Strangers

\* No relayed unit of a stranger's session beyond today's cap: the cap the
\* relay knows, or, after it was lowered, what was counted already.
CapBounded == Strangers <= hi

\* A lowered cap holds: today's bound never exceeds the larger of the
\* relay's cap and the units counted when it learned the lowering.
BoundSound == hi <= Max(rcap, used)

\* The owner's own sessions are never cut or refused for the cap.
NoCapOnOwner == \A s \in Sess : why[s].c = "limit_exceeded" => cli[s] = "stranger"

\* A failed or refused session ends with the typed reason, the same at
\* both ends of a cut.
TypedFailure ==
    /\ \A s \in Sess : st[s] = "failed" => why[s].c = why[s].a /\ why[s].c = "limit_exceeded"
    /\ \A s \in Sess : st[s] = "refused" => why[s].c = "limit_exceeded"

\* Liveness (NF1CapLive.cfg): the relay comes to apply the owner's cap.
LearnsCap == [](ocap # rcap => <>(ocap = rcap))

\* Witnesses: each must be violated (its state reached).
WitnessCut == ~\E s \in Sess : act.s = "Cut" /\ act.x = s
WitnessRefused == ~\E s \in Sess : st[s] = "refused"
WitnessDirectPastCap == ~\E s \in Sess : cli[s] = "stranger" /\ st[s] = "done" /\ used = rcap /\ rel[s] < sent[s]
WitnessOwnerPastCap == ~\E s \in Sess : cli[s] = "owner" /\ st[s] = "done" /\ rel[s] = MaxUnits /\ used >= rcap
WitnessLoweredCut == ~\E s \in Sess : rcap = CapMin /\ n.sets > 0 /\ act.s = "Cut" /\ act.x = s /\ scap[s] > rcap
WitnessNextDay == ~(n.days > 0 /\ \E s \in Sess : cli[s] = "stranger" /\ rel[s] > 0 /\ st[s] # "idle" /\ act.s = "Relay")
=============================================================================
