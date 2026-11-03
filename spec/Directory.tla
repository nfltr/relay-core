----------------------------- MODULE Directory -----------------------------
(* The agent records of the shared directory (core/directory session.go:   *)
(* ClaimSession, PutSession with renew, DeleteSession, GetSession) as one  *)
(* agent's sessions on several pods see them. spec/README.md               *)
(* "Directory.tla". Each record key (the dispatcher's and the streamer's)  *)
(* names at most one session; each claim takes the key's next Seq from its *)
(* own counter (seqKey, finding 34); a renewal yields (ErrSuperseded) to a *)
(* newer session's record on another pod, writes nothing under a newer    *)
(* one on its own pod, and yields when there is no record but the counter  *)
(* is past its Seq (finding 13); a session's end deletes the record only   *)
(* while it names that session. Every store call is atomic; the            *)
(* get-then-set and get-then-delete windows (ponytail comments) are        *)
(* relay-server Replicas.tla's (its finding 1b). A record may lapse (its   *)
(* TTL past a late renewal) and, with Loss, the store may restart empty    *)
(* once, losing the counters too (relay-server finding R7).                *)
(*                                                                          *)
(* The core/directory test TestDirectoryModelWalks replays walks of this   *)
(* module (scripts/spec.sh walks, spec/testdata/directory-walks.ndjson)    *)
(* against the real store and compares the records and counters after      *)
(* every step.                                                             *)
EXTENDS Naturals, Sequences, FiniteSets, TLC, TLCExt, Json

CONSTANTS Pods,    \* replicas, as strings ("p1")
          Keys,    \* the agent's record keys: "d" dispatcher, "s" streamer
          MaxSess, \* claims in a behaviour
          Loss,    \* the store may restart empty once
          Mutant   \* "none", or a bug put back (spec/README.md)

VARIABLES
    rec,   \* key -> the session its record names (an index of sess), 0: none
    ctr,   \* counter key -> the Seq counter (0: none)
    sess,  \* the sessions in claim order: key, pod, seq, st (live, sup, end)
    w,     \* key -> the session the record was last written for; 0 once it lapsed or was lost
    lost,  \* the store restarted empty
    act    \* the last step, for the walks and the witnesses

vars == <<rec, ctr, sess, w, lost, act>>

\* A session's start time is its claim order (Since; distinct, monotonic).
Since(i) == i

\* The counter of key k: its own (seqKey of the record key), or, as the
\* code was before finding 34's fix, one per agent.
CK(k) == IF Mutant = "shared-counter" THEN "d" ELSE k

\* Session.newer: by Seq, an equal Seq (a store that restarted empty gave
\* it twice) by start time (finding R7). "seq-tie" is the code before it.
Newer(a, b) ==
    IF sess[a].seq # sess[b].seq \/ Mutant = "seq-tie"
    THEN sess[a].seq > sess[b].seq
    ELSE Since(a) > Since(b)

\* The order the invariants judge by (Newer without the mutant).
After(a, b) ==
    IF sess[a].seq # sess[b].seq THEN sess[a].seq > sess[b].seq ELSE Since(a) > Since(b)

Sess == 1..Len(sess)

TypeOK ==
    /\ rec \in [Keys -> 0..MaxSess]
    /\ ctr \in [Keys -> 0..(MaxSess + 1)]
    /\ \A i \in Sess : sess[i].key \in Keys /\ sess[i].pod \in Pods /\ sess[i].st \in {"live", "sup", "end"}
    /\ w \in [Keys -> 0..MaxSess]
    /\ lost \in BOOLEAN

Init ==
    /\ rec = [k \in Keys |-> 0]
    /\ ctr = [k \in Keys |-> 0]
    /\ sess = <<>>
    /\ w = [k \in Keys |-> 0]
    /\ lost = FALSE
    /\ act = [n |-> "Init", p |-> "", k |-> "", i |-> 0]

\* ClaimSession: nextSeq (the counter's next, above the record's), then
\* the record written for the new session.
Claim(p, k) ==
    /\ Len(sess) < MaxSess
    /\ LET c == ctr[CK(k)] + 1
           s == IF rec[k] # 0 /\ sess[rec[k]].seq >= c THEN sess[rec[k]].seq + 1 ELSE c
           i == Len(sess) + 1
       IN /\ sess' = Append(sess, [key |-> k, pod |-> p, seq |-> s, st |-> "live"])
          /\ ctr' = [ctr EXCEPT ![CK(k)] = s]
          /\ rec' = [rec EXCEPT ![k] = i]
          /\ w' = [w EXCEPT ![k] = i]
          /\ act' = [n |-> "Claim", p |-> p, k |-> k, i |-> i]
    /\ UNCHANGED lost

\* PutSession with renew, by live session i.
Renew(i) ==
    /\ sess[i].st = "live"
    /\ LET k == sess[i].key
           r == rec[k]
       IN IF r # 0 /\ Newer(r, i)
          THEN IF sess[r].pod # sess[i].pod
               THEN /\ sess' = [sess EXCEPT ![i].st = "sup"]
                    /\ UNCHANGED <<rec, w>>
               ELSE UNCHANGED <<sess, rec, w>> \* a newer session here holds it
          ELSE IF r = 0 /\ ctr[CK(k)] > sess[i].seq /\ Mutant # "renew-resurrects"
          THEN /\ sess' = [sess EXCEPT ![i].st = "sup"]
               /\ UNCHANGED <<rec, w>>
          ELSE /\ rec' = [rec EXCEPT ![k] = i]
               /\ w' = [w EXCEPT ![k] = i]
               /\ UNCHANGED sess
    /\ act' = [n |-> "Renew", p |-> "", k |-> "", i |-> i]
    /\ UNCHANGED <<ctr, lost>>

\* The record r names session i as DeleteSession checks it: its pod and
\* its session (Seq and start time; "seq-only" compares the Seq alone).
OwnRecord(r, i) ==
    /\ sess[r].pod = sess[i].pod
    /\ sess[r].seq = sess[i].seq
    /\ (Mutant = "seq-only" \/ Since(r) = Since(i))

\* A session's end (it ended, or was superseded): DeleteSession, which
\* deletes the record when it names this pod and this session.
End(i) ==
    /\ sess[i].st # "end"
    /\ LET k == sess[i].key
           r == rec[k]
       IN IF r # 0 /\ (Mutant = "delete-any" \/ OwnRecord(r, i))
          THEN /\ rec' = [rec EXCEPT ![k] = 0]
          ELSE UNCHANGED rec
    /\ sess' = [sess EXCEPT ![i].st = "end"]
    /\ act' = [n |-> "End", p |-> "", k |-> "", i |-> i]
    /\ UNCHANGED <<ctr, w, lost>>

\* A record's TTL passed (its session's renewals were late).
Expire(k) ==
    /\ rec[k] # 0
    /\ rec' = [rec EXCEPT ![k] = 0]
    /\ w' = [w EXCEPT ![k] = 0]
    /\ act' = [n |-> "Expire", p |-> "", k |-> k, i |-> 0]
    /\ UNCHANGED <<ctr, sess, lost>>

\* The store restarted empty: records and counters gone (once).
StoreLoss ==
    /\ Loss /\ ~lost
    /\ rec' = [k \in Keys |-> 0]
    /\ ctr' = [k \in Keys |-> 0]
    /\ w' = [k \in Keys |-> 0]
    /\ lost' = TRUE
    /\ act' = [n |-> "Loss", p |-> "", k |-> "", i |-> 0]
    /\ UNCHANGED sess

RenewSome == \E i \in Sess : Renew(i)
EndSome == \E i \in Sess : End(i)

Next ==
    \/ \E p \in Pods, k \in Keys : Claim(p, k)
    \/ RenewSome
    \/ EndSome
    \/ \E k \in Keys : Expire(k)
    \/ StoreLoss

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants. *)

\* A session yields only to a newer session of its own record (finding 34).
SupersededByNewer ==
    \A i \in Sess : sess[i].st = "sup" =>
        \E j \in Sess : sess[j].key = sess[i].key /\ j # i /\ After(j, i)

\* Until the store loses its counters, a record never names a session
\* older than another claimed session of its key (finding 13: an older
\* session writing back a record a newer one left).
NoResurrect ==
    lost \/ \A k \in Keys : rec[k] # 0 =>
        ~\E j \in Sess : sess[j].key = k /\ sess[j].seq > sess[rec[k]].seq

\* A record goes only with its own session's end, its TTL or the store:
\* another session's end never deletes it.
DeleteOwn ==
    \A k \in Keys : rec[k] = 0 /\ w[k] # 0 => sess[w[k]].st = "end"

\* No two live sessions of a key both overwrite each other's record: one
\* yields (finding R7, equal Seqs after a store loss).
NoPingPong ==
    \A i, j \in Sess :
        (i # j /\ sess[i].key = sess[j].key /\ sess[i].st = "live" /\ sess[j].st = "live")
            => (Newer(i, j) \/ Newer(j, i))

\* Witnesses: each must be violated (the state its invariant guards is reached).
WitnessSuperseded == \A i \in Sess : sess[i].st # "sup"
WitnessResurrectSetup ==
    ~\E i, j \in Sess : sess[i].key = sess[j].key /\ sess[i].st = "live" /\ sess[j].st = "end"
        /\ sess[j].seq > sess[i].seq /\ rec[sess[i].key] = 0
WitnessEndKeeps == ~(act.n = "End" /\ rec[sess[act.i].key] \notin {0, act.i})
WitnessTie ==
    ~\E i, j \in Sess : i # j /\ sess[i].key = sess[j].key /\ sess[i].st = "live" /\ sess[j].st = "live"
        /\ sess[i].seq = sess[j].seq

-----------------------------------------------------------------------------
(* Walks for the Go replay (scripts/spec.sh walks, -simulate): at depth     *)
(* WalkDepth, or with no step left, the walk so far (TLCExt!Trace) is      *)
(* written to WalkDir/<n>.ndjson, one state a line. An invariant only of   *)
(* the walks' config; it always holds.                                     *)
CONSTANTS WalkDepth, WalkDir

WalkDump ==
    (TLCGet("level") >= WalkDepth \/ ~ENABLED Next) =>
        ndJsonSerialize(WalkDir \o "/" \o ToString(TLCGet("stats").traces) \o ".ndjson", Trace)
=============================================================================
