----------------------------- MODULE KeyAccount -----------------------------
(* Accounts of a key-file relay (keyfile: no database; nfltr/nfltr        *)
(* docs/decisions/tenants-reach-policy-2026-11-07.md "KEYACCT"). Each line  *)
(* of the file is one machine: an id the file assigns (never reused), a     *)
(* name and an account. A line generated alone is its own account (the      *)
(* account id is that first machine's id); `keys generate --account X`      *)
(* joins the account of X, a machine the file holds. The provider's owner   *)
(* is the line's account, so the machines of one account reach each other  *)
(* (relay-core Reach: owner == owner) and only they do. A name is a label:  *)
(* unique among an account's live machines, resolved within the caller's    *)
(* account only, free again once its machine is revoked. A reload swaps     *)
(* keys and the (account, name) index in one step and is refused when it     *)
(* would change a live machine's account. Revoking the first machine        *)
(* leaves its account to the others.                                        *)
EXTENDS FiniteSets, Naturals

CONSTANTS
    Ids,      \* machine ids the file may assign
    Names,    \* machine names
    None,     \* no machine yet (a model value)
    Mutant    \* "none", or a bug put back (spec/README.md "KeyAccount.tla")

VARIABLES
    live,   \* the machines the file holds
    used,   \* every id the file ever assigned (never reused)
    acct,   \* each assigned machine's account
    nm,     \* each assigned machine's name
    fixed   \* history: the account each machine got at generate

vars == <<live, used, acct, nm, fixed>>

\* The name n is free in account a (live machines only: a revoked machine's
\* name is free at once).
NameFree(a, n) == \A m \in live : acct[m] = a => nm[m] # n

Init ==
    /\ live = {} /\ used = {}
    /\ acct = [m \in Ids |-> None] /\ nm = [m \in Ids |-> None] /\ fixed = [m \in Ids |-> None]

\* `keys generate --name n`: a fresh id, its own account.
Generate(m, n) ==
    /\ m \notin used
    /\ NameFree(m, n)
    /\ live' = live \cup {m} /\ used' = used \cup {m}
    /\ acct' = [acct EXCEPT ![m] = m] /\ nm' = [nm EXCEPT ![m] = n] /\ fixed' = [fixed EXCEPT ![m] = m]

\* `keys generate --name n --account j`: j must be a machine the file
\* holds; the new machine joins j's account (j's id only when j is that
\* account's first machine).
Join(m, n, j) ==
    LET a == IF j \in live THEN acct[j] ELSE j IN
    /\ m \notin used /\ m # j
    /\ j \in live \/ (Mutant = "join-outside" /\ j \notin used)
    /\ NameFree(a, n) \/ Mutant = "dup-name"
    /\ live' = live \cup {m} /\ used' = used \cup {m}
    /\ acct' = [acct EXCEPT ![m] = a] /\ nm' = [nm EXCEPT ![m] = n] /\ fixed' = [fixed EXCEPT ![m] = a]

\* `keys revoke --machine m`: its line leaves the file; the other machines
\* of its account keep the account (its id, when m was the first).
Revoke(m) ==
    /\ m \in live
    /\ live' = live \ {m}
    /\ IF Mutant = "revoke-reassigns"
         THEN acct' = [x \in Ids |-> IF x \in live /\ acct[x] = m /\ x # m THEN x ELSE acct[x]]
         ELSE UNCHANGED acct
    /\ UNCHANGED <<used, nm, fixed>>

\* An edit of line m reloaded (Watch): a new name n and account a. The
\* file loads only with every live machine's account as it was and the
\* names unique per account; otherwise nothing changes.
Reload(m, n, a) ==
    /\ m \in live
    /\ a = acct[m] \/ Mutant = "reload-account"
    /\ \A x \in live \ {m} : acct[x] = a => nm[x] # n
    /\ acct' = [acct EXCEPT ![m] = a] /\ nm' = [nm EXCEPT ![m] = n]
    /\ UNCHANGED <<live, used, fixed>>

Next ==
    \/ \E m \in Ids, n \in Names : Generate(m, n)
    \/ \E m, j \in Ids, n \in Names : Join(m, n, j)
    \/ \E m \in Ids : Revoke(m)
    \/ \E m, a \in Ids, n \in Names : Reload(m, n, a)

Spec == Init /\ [][Next]_vars

\* The provider's owner of machine m's principal (keyfile.Provider).
Owner(m) == IF Mutant = "owner-own-id" THEN m ELSE acct[m]

\* The edge's name resolution for caller c (keyfile.Policy.ResolveMachine).
Resolve(c, n) == {m \in live : nm[m] = n /\ (acct[m] = acct[c] \/ Mutant = "resolve-global")}

TypeOK ==
    /\ live \subseteq used /\ used \subseteq Ids
    /\ \A m \in used : acct[m] \in Ids /\ nm[m] \in Names

\* A machine's account never changes after generate.
AccountStable == \A m \in used : acct[m] = fixed[m]

\* Every account is the id of a machine generated as its own account:
\* `--account` never names one outside the file.
AccountRooted == \A m \in live : \E r \in used : fixed[r] = r /\ acct[m] = r

\* Names are unique among an account's live machines.
NamesUnique == \A x, y \in live : x # y /\ acct[x] = acct[y] => nm[x] # nm[y]

\* The machines of one account, and only they, share an owner (reach).
SameAccountReaches == \A x, y \in live : (Owner(x) = Owner(y)) <=> (acct[x] = acct[y])

\* A name resolves to at most one machine, of the caller's account.
ResolveOwnAccount == \A c \in live, n \in Names :
    /\ Cardinality(Resolve(c, n)) <= 1
    /\ \A t \in Resolve(c, n) : acct[t] = acct[c]

\* Witnesses (checked violated: the states the invariants guard are reached).
\* Two machines of one account whose first machine was revoked.
WitnessOutlivesFirst == ~\E x, y \in live : x # y /\ acct[x] = acct[y] /\ acct[x] \notin live
\* A revoked machine's name taken again in its account.
WitnessNameReused == ~\E x \in used \ live, y \in live : x # y /\ acct[x] = acct[y] /\ nm[x] = nm[y]
\* One name in two accounts.
WitnessNameInTwoAccounts == ~\E x, y \in live : acct[x] # acct[y] /\ nm[x] = nm[y]
=============================================================================
