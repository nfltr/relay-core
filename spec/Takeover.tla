------------------------------ MODULE Takeover ------------------------------
(* Who holds an agent ID: processes that register it (nfltr up, nfltr cat  *)
(* serve) and the relay's rule for a second registration (core/session    *)
(* dispatcher_session_takeover.go takeOverSession, dispatcher_grpc.go's    *)
(* AgentTunnel replace, dispatcher_tcp_streams.go's replaced stream).      *)
(* spec/README.md "Takeover.tla".                                          *)
(*                                                                          *)
(* The relay's rule: another credential is refused (AlreadyExists); the    *)
(* same credential takes the agent over, newest wins, and the replaced     *)
(* stream is told (errReplacedSessionStatus, final: the client exits). The *)
(* relay cannot refuse the same credential: it cannot tell a live holder  *)
(* from one that died (a reboot, a lost network) before liveness notices,  *)
(* so a refusal there would refuse the restart. The machine can: a process *)
(* holds the machine-local lock of (relay, agent) while it serves, and the *)
(* kernel releases it when the process ends, however it ends. So a second *)
(* process on the machine is refused at start, naming the holder, unless  *)
(* it passes --replace; then it registers, the relay replaces the first    *)
(* and tells it, and the newcomer takes the lock once the first is gone.  *)
(* The client half is nfltr cmd/nfltr/internal/upcli (agent_hold.go).     *)
EXTENDS Naturals, FiniteSets

CONSTANTS Procs,   \* processes registering the agent ID
          Cred,    \* process -> its credential (one machine, or a copied config)
          Mach,    \* process -> its machine
          Mutant   \* "none", or a bug put back (spec/README.md)

VARIABLES
    ps,     \* process -> idle, started, serving, refused, replaced, stopped, dead
    rep,    \* the processes started with --replace
    lock,   \* machine -> the process holding its (relay, agent) lock, or "none"
    rel,    \* the process the relay's live session of the agent names, or "none"
    told,   \* processes whose replaced notice is in flight
    why     \* process -> its refusal: "", "holder" (a live local holder),
            \* "relay" (the relay: another credential's session) or "stale"
            \* (the relay, its own credential's session of a dead process)

vars == <<ps, rep, lock, rel, told, why>>

\* The configs' processes: p1 and p2 on m1 with one credential, p3 on m2
\* with another (Cred <- TwoAndOther, Mach <- TwoAndOtherMach).
TwoAndOther == [p \in Procs |-> IF p = "p3" THEN "o" ELSE "c"]
TwoAndOtherMach == [p \in Procs |-> IF p = "p3" THEN "m2" ELSE "m1"]

Machines == {Mach[p] : p \in Procs}
Alive(p) == ps[p] \in {"started", "serving"}

TypeOK ==
    /\ ps \in [Procs -> {"idle", "started", "serving", "refused", "replaced", "stopped", "dead"}]
    /\ rep \subseteq Procs
    /\ lock \in [Machines -> Procs \cup {"none"}]
    /\ rel \in Procs \cup {"none"}
    /\ told \subseteq Procs
    /\ why \in [Procs -> {"", "holder", "relay", "stale"}]

Init ==
    /\ ps = [p \in Procs |-> "idle"]
    /\ rep = {}
    /\ lock = [m \in Machines |-> "none"]
    /\ rel = "none"
    /\ told = {}
    /\ why = [p \in Procs |-> ""]

\* The process starts: it takes its machine's lock, or is refused naming
\* the holder (which is alive: the kernel released a dead one's), or, with
\* --replace, goes on without it. Mutant "no-lock": no local check.
Start(p, r) ==
    /\ ps[p] = "idle"
    /\ LET m == Mach[p] IN
       IF lock[m] = "none" \/ Mutant = "no-lock"
       THEN /\ ps' = [ps EXCEPT ![p] = "started"]
            /\ lock' = IF lock[m] = "none" THEN [lock EXCEPT ![m] = p] ELSE lock
            /\ UNCHANGED why
       ELSE IF r
       THEN /\ ps' = [ps EXCEPT ![p] = "started"]
            /\ UNCHANGED <<lock, why>>
       ELSE /\ ps' = [ps EXCEPT ![p] = "refused"]
            /\ why' = [why EXCEPT ![p] = "holder"]
            /\ UNCHANGED lock
    /\ rep' = IF r THEN rep \cup {p} ELSE rep
    /\ UNCHANGED <<rel, told>>

\* The relay registers it. Mutant "silent-replace": the replaced session
\* is not told. Mutant "relay-refuses": the relay refuses the same
\* credential while it holds a session (the alternative design).
Connect(p) ==
    /\ ps[p] = "started"
    /\ LET q == rel IN
       IF q # "none" /\ q # p /\ (Cred[q] # Cred[p] \/ Mutant = "relay-refuses")
       THEN /\ ps' = [ps EXCEPT ![p] = "refused"]
            /\ why' = [why EXCEPT ![p] = IF ps[q] = "dead" /\ Cred[q] = Cred[p] THEN "stale" ELSE "relay"]
            /\ lock' = IF lock[Mach[p]] = p THEN [lock EXCEPT ![Mach[p]] = "none"] ELSE lock
            /\ UNCHANGED <<rel, told>>
       ELSE /\ ps' = [ps EXCEPT ![p] = "serving"]
            /\ rel' = p
            /\ told' = IF q \notin {"none", p} /\ Mutant # "silent-replace" THEN told \cup {q} ELSE told
            /\ UNCHANGED <<lock, why>>
    /\ UNCHANGED rep

\* The replaced process reads the notice and exits (its typed code); its
\* lock goes with it. Mutant "notice-ignored": the client redials instead.
Notice(p) ==
    /\ p \in told
    /\ told' = told \ {p}
    /\ ps' = IF Alive(p) /\ Mutant # "notice-ignored" THEN [ps EXCEPT ![p] = "replaced"] ELSE ps
    /\ lock' = IF lock[Mach[p]] = p THEN [lock EXCEPT ![Mach[p]] = "none"] ELSE lock
    /\ UNCHANGED <<rep, rel, why>>

\* A --replace process takes the lock once the one it replaced is gone.
Acquire(p) ==
    /\ ps[p] = "serving" /\ lock[Mach[p]] = "none"
    /\ lock' = [lock EXCEPT ![Mach[p]] = p]
    /\ UNCHANGED <<ps, rep, rel, told, why>>

\* Ctrl+C: the session ends at the relay, the lock with the process.
Stop(p) ==
    /\ ps[p] = "serving"
    /\ ps' = [ps EXCEPT ![p] = "stopped"]
    /\ rel' = IF rel = p THEN "none" ELSE rel
    /\ lock' = IF lock[Mach[p]] = p THEN [lock EXCEPT ![Mach[p]] = "none"] ELSE lock
    /\ UNCHANGED <<rep, told, why>>

\* The process dies unseen (kill -9 on a lost network, a reboot): the kernel
\* releases its lock; the relay keeps its session until liveness notices.
Crash(p) ==
    /\ Alive(p)
    /\ ps' = [ps EXCEPT ![p] = "dead"]
    /\ lock' = IF lock[Mach[p]] = p THEN [lock EXCEPT ![Mach[p]] = "none"] ELSE lock
    /\ UNCHANGED <<rep, rel, told, why>>

\* The relay's liveness ends a dead holder's session.
Detect ==
    /\ rel # "none" /\ ps[rel] = "dead"
    /\ rel' = "none"
    /\ UNCHANGED <<ps, rep, lock, told, why>>

\* A dead process restarts (systemd): a new process with its identity.
Restart(p) ==
    /\ ps[p] = "dead"
    /\ ps' = [ps EXCEPT ![p] = "idle"]
    /\ rep' = rep \ {p}
    /\ told' = told \ {p}
    /\ UNCHANGED <<lock, rel, why>>

Next ==
    \/ \E p \in Procs, r \in BOOLEAN : Start(p, r)
    \/ \E p \in Procs : Connect(p) \/ Notice(p) \/ Acquire(p) \/ Stop(p) \/ Crash(p) \/ Restart(p)
    \/ Detect

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ \A p \in Procs : WF_vars(Notice(p))

-----------------------------------------------------------------------------
(* Invariants. *)

\* No silent replace: a process that believes it serves is the one dials
\* reach, or its replaced notice is on its way.
NoSilentReplace == \A p \in Procs : ps[p] = "serving" => rel = p \/ p \in told

\* A second process of the machine serves only by --replace: two live
\* processes of one machine without it never both register.
DuplicateRefused ==
    \A p, q \in Procs : (p # q /\ Mach[p] = Mach[q] /\ Alive(p) /\ Alive(q)) => (p \in rep \/ q \in rep)

\* A restart after an unseen death is never refused for the session its
\* dead predecessor left (the relay could not have known it was dead).
NoFalseRefusal == \A p \in Procs : why[p] # "stale"

\* The lock names a live process.
LockLive == \A m \in Machines : lock[m] # "none" => Alive(lock[m])

\* Liveness (LiveSpec): a replaced process stops serving.
ReplacedExits == \A p \in Procs : (p \in told) ~> (ps[p] # "serving")

\* Witnesses: each must be violated.
WitnessRefused == \A p \in Procs : why[p] # "holder"
WitnessReplaced == \A p \in Procs : ps[p] # "replaced"
WitnessOtherCred == \A p \in Procs : why[p] # "relay"
\* A restart takes over the session the relay still holds for its dead self.
WitnessStaleTakeover == ~\E p, q \in Procs : p # q /\ rel = p /\ ps[q] = "dead" /\ told = {q}
=============================================================================
