--------------------------- MODULE TraceKeyAccount ---------------------------
(* Trace validation of KeyAccount.tla (spec/README.md "KeyAccount traces"): *)
(* the steps one key file's store takes in the keyfile tests (-tags        *)
(* relaytrace), recorded by keyfile trace_on.go, must be a behaviour of    *)
(* KeyAccount. A Load line is one the file held at start (Load below); an *)
(* Enrol line names the account and name the machine got (EnrolledAsCoded); *)
(* a line of a step KeyAccount has no action for (Generate, a key line's,   *)
(* gone with the API key cut) is refused (OTHER); an Auth                  *)
(* line is the provider's answer for a certificate that chains to the CA   *)
(* (Authenticates), which changes nothing: true only for a certificate the *)
(* CA signed. The trace is accepted when TLC reaches its end: TraceNotDone *)
(* is then violated.                                                       *)
EXTENDS TraceKeyAccountLog, KeyAccount, TLC

VARIABLE l   \* the next line

E == Log[l]

\* A line the key file holds at start (a restart): machine m, named n, of
\* account a, its line binding k (every line binds a key: no key lines).
\* An account whose first machine is no longer in the file is that revoked
\* machine's: it goes to used, the id never assigned again.
Load(m, n, a, k) ==
    LET r == {a} \ (used \cup {m}) IN
    /\ m \notin used /\ NameFree(a, n) /\ k # ""
    /\ live' = live \cup {m} /\ used' = used \cup {m} \cup r
    /\ acct' = [x \in Ids |-> IF x = m \/ x \in r THEN a ELSE acct[x]]
    /\ nm' = [x \in Ids |-> IF x = m \/ x \in r THEN n ELSE nm[x]]
    /\ fixed' = [x \in Ids |-> IF x = m \/ x \in r THEN a ELSE fixed[x]]
    /\ bound' = [bound EXCEPT ![m] = k]
    /\ certs' = certs \cup {<<m, k>>}
    /\ revoked' = revoked \cup r
    /\ UNCHANGED <<code, spent, snap>>

Step ==
    CASE E.ev = "Load" -> Load(E.m, E.n, E.a, E.k)
      [] E.ev = "Revoke" -> Revoke(E.m)
      [] E.ev = "MintCode" -> IF E.a = "" THEN MintCode(E.c, E.n, None)
                              ELSE \E j \in live : acct[j] = E.a /\ MintCode(E.c, E.n, j)
      [] E.ev = "Enrol" -> Enrol(E.c, E.m, E.k) /\ acct'[E.m] = E.a /\ nm'[E.m] = E.n
      [] E.ev = "Reissue" -> Reissue(E.m, E.k)
      [] E.ev = "Auth" -> /\ IF E.ok THEN Authenticates(E.m, E.k) /\ <<E.m, E.k>> \in certs
                                     ELSE ~Authenticates(E.m, E.k)
                          /\ UNCHANGED vars
      [] OTHER -> Print(<<"unknown event", E>>, FALSE)

TraceInit == Init /\ l = 1

TraceNext == l <= Len(Log) /\ Step /\ l' = l + 1

\* Violated once every line was taken: the trace is a behaviour.
TraceNotDone == l <= Len(Log)
=============================================================================
