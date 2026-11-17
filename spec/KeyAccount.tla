----------------------------- MODULE KeyAccount -----------------------------
(* Accounts of a key-file relay (keyfile: no database; nfltr/nfltr        *)
(* docs/decisions/tenants-reach-policy-2026-11-07.md "KEYACCT"). Each line  *)
(* of the file is one machine: an id the file assigns (never reused), a     *)
(* name, an account and the key its certificate certifies. A machine joins *)
(* only by a single-use code minted from the data dir (lane L5, nfltr/nfltr *)
(* docs/decisions/api-key-cut-2026-11-15.md "L5 SELFHOST"); a code minted   *)
(* without --account makes the machine its own account (the account id is  *)
(* that first machine's id), one with --account X joins the account of X, a *)
(* machine the file holds. The provider's owner is the line's account, so  *)
(* the machines of one account reach each other (relay-core Reach: owner == *)
(* owner) and only they do. A name is a label: unique among an account's    *)
(* live machines, resolved within the caller's account only, free again     *)
(* once its machine is revoked. A reload swaps the lines and the (account,  *)
(* name) index in one step and is refused when it would change a live       *)
(* machine's account. Revoking the first machine leaves its account to the *)
(* others. A certificate (signed by the data dir's machine CA) authenticates *)
(* only while its machine's line is live and binds that same key. A reissue *)
(* with a new key rebinds the line (Rotate); the old certificate is still   *)
(* unexpired but authenticates nothing. The API key cut (lane L4S): no line *)
(* holds a bearer key any more, so Generate and Join (`keys generate`) are  *)
(* gone and every machine is a certificate machine.                         *)
EXTENDS FiniteSets, Naturals

CONSTANTS
    Ids,      \* machine ids the file may assign
    Names,    \* machine names
    None,     \* no machine yet (a model value)
    Codes,    \* enrolment codes `nfltr-relay machines code` may mint
    Keys,     \* machine keys (made on the machines, never sent)
    Mutant    \* "none", or a bug put back (spec/README.md "KeyAccount.tla")

VARIABLES
    live,   \* the machines the file holds
    used,   \* every id the file ever assigned (never reused)
    acct,   \* each assigned machine's account
    nm,     \* each assigned machine's name
    fixed,  \* history: the account each machine got at enrolment
    code,   \* each code's terms: None (not minted) or [name, acct] (acct None: its own)
    spent,  \* each code's redemptions (history: the machines it enrolled)
    bound,  \* each machine's line: the key it binds (None: not enrolled)
    certs   \* the certificates the CA signed: <<machine, key>> (none expire here)

vars == <<live, used, acct, nm, fixed, code, spent, bound, certs>>
cvars == <<code, spent, bound, certs>>

\* The name n is free in account a (live machines only: a revoked machine's
\* name is free at once).
NameFree(a, n) == \A m \in live : acct[m] = a => nm[m] # n

\* The certificate <<m, k>> authenticates (keyfile.Provider: it chains to
\* the data dir's CA, its machine id has a line, and the line binds the
\* leaf's key).
Authenticates(m, k) ==
    /\ m \in live \/ Mutant = "cert-after-revoke"
    /\ bound[m] = k \/ Mutant = "cert-any-key"

Init ==
    /\ live = {} /\ used = {}
    /\ acct = [m \in Ids |-> None] /\ nm = [m \in Ids |-> None] /\ fixed = [m \in Ids |-> None]
    /\ code = [c \in Codes |-> None] /\ spent = [c \in Codes |-> {}]
    /\ bound = [m \in Ids |-> None] /\ certs = {}

\* `nfltr-relay machines code --name n [--account j]`: a single-use code
\* for a machine named n, in j's account (resolved now: j a machine the
\* file holds) or its own (j = None). The relay's first start mints one
\* for "admin" (admin-enrol-code).
MintCode(c, n, j) ==
    /\ code[c] = None
    /\ j = None \/ j \in live \/ (Mutant = "code-join-outside" /\ j \notin used)
    /\ code' = [code EXCEPT ![c] = [name |-> n, acct |-> IF j \in live THEN acct[j] ELSE j]]
    /\ UNCHANGED <<live, used, acct, nm, fixed, spent, bound, certs>>

\* A machine POSTs the enrol route {code, csr for k}: the code is spent, a
\* fresh id m gets a line binding k (its own account, or the code's) and
\* the CA signs <<m, k>>, in one step under the key manager's lock.
Enrol(c, m, k) ==
    LET a == IF code[c].acct = None THEN m ELSE code[c].acct IN
    /\ code[c] # None
    /\ spent[c] = {} \/ Mutant = "code-reuse"
    /\ m \notin used
    /\ NameFree(a, code[c].name) \/ Mutant = "code-dup-name"
    /\ live' = live \cup {m} /\ used' = used \cup {m}
    /\ acct' = [acct EXCEPT ![m] = a] /\ nm' = [nm EXCEPT ![m] = code[c].name] /\ fixed' = [fixed EXCEPT ![m] = a]
    /\ spent' = [spent EXCEPT ![c] = @ \cup {m}]
    /\ bound' = [bound EXCEPT ![m] = k]
    /\ certs' = certs \cup {<<m, k>>}
    /\ UNCHANGED code

\* The reissue route: a certificate that authenticates, a CSR for k and
\* the proof signed by the old key. The same key renews; a new key
\* rotates: the line binds k from now on.
Reissue(m, k) ==
    /\ \E o \in Keys : <<m, o>> \in certs /\ Authenticates(m, o)
    /\ bound' = [bound EXCEPT ![m] = k]
    /\ certs' = certs \cup {<<m, k>>}
    /\ UNCHANGED <<live, used, acct, nm, fixed, code, spent>>

\* `machines revoke --machine m`: its line leaves the file; the other machines
\* of its account keep the account (its id, when m was the first).
Revoke(m) ==
    /\ m \in live
    /\ live' = live \ {m}
    /\ IF Mutant = "revoke-reassigns"
         THEN acct' = [x \in Ids |-> IF x \in live /\ acct[x] = m /\ x # m THEN x ELSE acct[x]]
         ELSE UNCHANGED acct
    /\ UNCHANGED <<used, nm, fixed>>
    /\ UNCHANGED cvars

\* An edit of line m reloaded (Watch): a new name n and account a. The
\* file loads only with every live machine's account as it was and the
\* names unique per account; otherwise nothing changes.
Reload(m, n, a) ==
    /\ m \in live
    /\ a = acct[m] \/ Mutant = "reload-account"
    /\ \A x \in live \ {m} : acct[x] = a => nm[x] # n
    /\ acct' = [acct EXCEPT ![m] = a] /\ nm' = [nm EXCEPT ![m] = n]
    /\ UNCHANGED <<live, used, fixed>>
    /\ UNCHANGED cvars

Next ==
    \/ \E c \in Codes, n \in Names, j \in Ids \cup {None} : MintCode(c, n, j)
    \/ \E c \in Codes, m \in Ids, k \in Keys : Enrol(c, m, k)
    \/ \E m \in Ids, k \in Keys : Reissue(m, k)
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
    /\ \A m \in Ids : bound[m] \in Keys \cup {None}
    /\ certs \subseteq used \X Keys

\* A machine's account never changes after enrolment.
AccountStable == \A m \in used : acct[m] = fixed[m]

\* Every account is the id of a machine enrolled as its own account:
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

\* A code enrols at most one machine.
EnrolCodeSingleUse == \A c \in Codes : Cardinality(spent[c]) <= 1

\* A certificate authenticates only its live machine's current key: a
\* revoked line, or a key a reissue replaced, admits nothing.
CertBoundToLine == \A <<m, k>> \in certs :
    Authenticates(m, k) => m \in live /\ bound[m] = k

\* A machine enrolled by a code is in the code's account (or its own); its
\* name is the code's until a reload edits it.
EnrolledAsCoded == \A c \in Codes, m \in Ids : m \in spent[c] =>
    acct[m] = IF code[c].acct = None THEN m ELSE code[c].acct

\* Witnesses (checked violated: the states the invariants guard are reached).
\* Two machines of one account whose first machine was revoked.
WitnessOutlivesFirst == ~\E x, y \in live : x # y /\ acct[x] = acct[y] /\ acct[x] \notin live
\* A revoked machine's name taken again in its account.
WitnessNameReused == ~\E x \in used \ live, y \in live : x # y /\ acct[x] = acct[y] /\ nm[x] = nm[y]
\* One name in two accounts.
WitnessNameInTwoAccounts == ~\E x, y \in live : acct[x] # acct[y] /\ nm[x] = nm[y]
\* A machine enrolled by a code into another machine's account.
WitnessCodeJoins == ~\E c \in Codes, m \in Ids : m \in spent[c] /\ acct[m] # m
\* A signed certificate whose key a rotation replaced, its line live.
WitnessRotatedAway == ~\E m \in live, k \in Keys : <<m, k>> \in certs /\ bound[m] # k
\* A signed certificate of a revoked machine.
WitnessCertOfRevoked == ~\E m \in used \ live, k \in Keys : <<m, k>> \in certs
=============================================================================
