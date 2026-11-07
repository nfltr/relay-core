---------------------------- MODULE AccountLimit ----------------------------
(* The account's own connection limits (nfltr/nfltr                        *)
(* docs/decisions/relay-plan-conn-limits-2026-11-08.md), past the flood      *)
(* floor: one relay replica's limiter (platform/connlimit Accounts) counts   *)
(* each admitted visitor connection against its account in total and        *)
(* against (account, source address), open connections and a token bucket  *)
(* of new ones, under the limits of the plan revision the replica holds.     *)
(* The plan is one value in the accounts store (the global settings); a      *)
(* change is a new revision, which the replica applies when the journal      *)
(* says so (relay-server Journal.tla bounds when): Apply. Counts are per     *)
(* replica; a replica is modelled alone (two are two copies of this).        *)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Accounts,   \* accounts whose agents are visited
    Sources,    \* visitor source addresses (one stands for a NAT crowd)
    Limits,     \* the values a plan's limit may take (>= 1)
    MaxRev,     \* plan revisions the store writes
    MaxLive,    \* bound on live connections per (account, source)
    Mutant      \* "none", or a bug put back (spec/README.md "AccountLimit.tla")

Plans == [max: Limits, smax: Limits, burst: Limits]
Any == "any"  \* the source key of the mutant count-by-ip-only

\* The per-source key: the account and the address (count-by-ip-only: the
\* address alone, so accounts behind one address share one count).
SKey(a, s) == IF Mutant = "count-by-ip-only" THEN <<Any, s>> ELSE <<a, s>>
SKeys == (Accounts \cup {Any}) \X Sources

VARIABLES
    plan,    \* the store's plan and its revision
    rev,
    held,    \* the plan revision the replica applied, and its revision
    hrev,
    live,    \* ground truth: live connections per (account, source)
    open,    \* the limiter's open count per account
    sopen,   \* the limiter's open count per source key
    tok,     \* tokens per account bucket
    stok,    \* tokens per source-key bucket
    burst,   \* the burst each account bucket was last set to (x/time/rate SetBurst)
    sburst,  \* the same per source-key bucket
    bad,     \* history: an admission over the held plan's limits
    starved  \* history: a refusal not caused by the account's own counts

vars == <<plan, rev, held, hrev, live, open, sopen, tok, stok, burst, sburst, bad, starved>>

Sum(f, S) == LET F[T \in SUBSET S] ==
                 IF T = {} THEN 0 ELSE LET x == CHOOSE x \in T : TRUE IN f[x] + F[T \ {x}]
             IN F[S]
OwnOpen(a) == Sum([s \in Sources |-> live[<<a, s>>]], Sources)

Min(x, y) == IF x < y THEN x ELSE y

TypeOK ==
    /\ plan \in Plans /\ held \in Plans /\ rev \in 0..MaxRev /\ hrev \in 0..MaxRev
    /\ live \in [Accounts \X Sources -> 0..MaxLive]
    /\ open \in [Accounts -> Nat] /\ sopen \in [SKeys -> Nat]
    /\ tok \in [Accounts -> Nat] /\ stok \in [SKeys -> Nat]
    /\ burst \in [Accounts -> Limits] /\ sburst \in [SKeys -> Limits]
    /\ bad \in BOOLEAN /\ starved \in BOOLEAN

Init ==
    /\ plan \in Plans /\ rev = 0 /\ held = plan /\ hrev = 0
    /\ live = [k \in Accounts \X Sources |-> 0]
    /\ open = [a \in Accounts |-> 0] /\ sopen = [k \in SKeys |-> 0]
    /\ tok = [a \in Accounts |-> plan.burst] /\ stok = [k \in SKeys |-> plan.burst]
    /\ burst = [a \in Accounts |-> plan.burst] /\ sburst = [k \in SKeys |-> plan.burst]
    /\ bad = FALSE /\ starved = FALSE

\* The operator saves new limits (global settings): a new revision.
ChangePlan(p) ==
    /\ rev < MaxRev
    /\ plan' = p /\ rev' = rev + 1
    /\ UNCHANGED <<held, hrev, live, open, sopen, tok, stok, burst, sburst, bad, starved>>

\* The replica applies the store's newest revision (reloadGlobalSettings on
\* the journal's GlobalSettingsSubject entry): SetPlan.
Apply ==
    /\ hrev < rev
    /\ held' = plan /\ hrev' = rev
    /\ UNCHANGED <<plan, rev, live, open, sopen, tok, stok, burst, sburst, bad, starved>>

\* A token returns to a bucket (time passes), up to the burst it was set to.
Refill(a) ==
    /\ tok[a] < burst[a]
    /\ tok' = [tok EXCEPT ![a] = @ + 1]
    /\ UNCHANGED <<plan, rev, held, hrev, live, open, sopen, stok, burst, sburst, bad, starved>>
RefillSource(k) ==
    /\ stok[k] < sburst[k]
    /\ stok' = [stok EXCEPT ![k] = @ + 1]
    /\ UNCHANGED <<plan, rev, held, hrev, live, open, sopen, tok, burst, sburst, bad, starved>>

\* A bucket used under the held plan takes its burst first (SetBurst caps
\* the tokens: x/time/rate advance). cache-stale-plan: a bucket keeps the
\* burst it was made with.
Synced(b) == IF Mutant = "cache-stale-plan" THEN b ELSE held.burst
Capped(t, b) == Min(t, Synced(b))

\* A visitor from s connects to an agent of account a (Accounts.Admit).
Admit(a, s) ==
    LET k == SKey(a, s)
        t == Capped(tok[a], burst[a])
        st == Capped(stok[k], sburst[k])
    IN
    /\ live[<<a, s>>] < MaxLive
    /\ open[a] < held.max /\ sopen[k] < held.smax /\ t > 0 /\ st > 0
    /\ live' = [live EXCEPT ![<<a, s>>] = @ + 1]
    /\ open' = [open EXCEPT ![a] = @ + 1] /\ sopen' = [sopen EXCEPT ![k] = @ + 1]
    /\ tok' = [tok EXCEPT ![a] = t - 1] /\ stok' = [stok EXCEPT ![k] = st - 1]
    /\ burst' = [burst EXCEPT ![a] = Synced(@)] /\ sburst' = [sburst EXCEPT ![k] = Synced(@)]
    \* The truth: within the held plan's limits.
    /\ bad' = (bad \/ OwnOpen(a) >= held.max \/ live[<<a, s>>] >= held.smax
                   \/ t > held.burst \/ st > held.burst)
    /\ UNCHANGED <<plan, rev, held, hrev, starved>>

\* The limiter refuses it (a TLS alert, a dashboard count): one of its
\* checks failed. The truth: the account's own connections explain it
\* (its own open counts, or its buckets, which are its alone when keyed by
\* account and source).
Refuse(a, s) ==
    LET k == SKey(a, s)
        t == Capped(tok[a], burst[a])
        st == Capped(stok[k], sburst[k])
    IN
    /\ live[<<a, s>>] < MaxLive
    /\ ~(open[a] < held.max /\ sopen[k] < held.smax /\ t > 0 /\ st > 0)
    /\ starved' = (starved \/ ~(OwnOpen(a) >= held.max \/ live[<<a, s>>] >= held.smax \/ t = 0
                                  \/ (k = <<a, s>> /\ st = 0)))
    /\ UNCHANGED <<plan, rev, held, hrev, live, open, sopen, tok, stok, burst, sburst, bad>>

\* An admitted connection closes: its counts are released once
\* (sync.OnceFunc; release-twice: a second Close releases again).
Close(a, s) ==
    LET k == SKey(a, s)
        n == IF Mutant = "release-twice" THEN 2 ELSE 1
        dec(x) == IF x >= n THEN x - n ELSE 0
    IN
    /\ live[<<a, s>>] > 0
    /\ live' = [live EXCEPT ![<<a, s>>] = @ - 1]
    /\ open' = [open EXCEPT ![a] = dec(@)] /\ sopen' = [sopen EXCEPT ![k] = dec(@)]
    /\ UNCHANGED <<plan, rev, held, hrev, tok, stok, burst, sburst, bad, starved>>

Next ==
    \/ \E p \in Plans : ChangePlan(p)
    \/ Apply
    \/ \E a \in Accounts : Refill(a)
    \/ \E k \in SKeys : RefillSource(k)
    \/ \E a \in Accounts, s \in Sources : Admit(a, s) \/ Refuse(a, s) \/ Close(a, s)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ WF_vars(Apply)

\* Every admission is within the limits of the plan revision held.
LimitHolds == ~bad
\* A refusal is the refused account's own doing.
NoAccountStarvesAnother == ~starved
\* The limiter's counts are the live connections: each released once.
CountsMatch ==
    /\ \A a \in Accounts : open[a] = OwnOpen(a)
    /\ Mutant # "count-by-ip-only" => \A a \in Accounts, s \in Sources : sopen[<<a, s>>] = live[<<a, s>>]

\* Every revision the store writes reaches the replica.
PlanReaches == <>[](hrev = rev)

\* Witnesses (checked violated: the states the invariants guard are reached).
WitnessRefused == ~\E a \in Accounts, s \in Sources : ENABLED Refuse(a, s)
WitnessLowered == ~(hrev > 0 /\ \E a \in Accounts : OwnOpen(a) > held.max)
WitnessShared == ~\A a \in Accounts : live[<<a, CHOOSE s \in Sources : TRUE>>] > 0
=============================================================================
