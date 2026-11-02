------------------------------ MODULE AgentLink ------------------------------
(* The TCP data path between the relay (core/session TCPDispatcher) and an *)
(* agent (relay client/agent tcpTunnel) over one frame stream with          *)
(* per-session credit (spec/README.md). Side "r" is the relay, "a" the      *)
(* agent; the relay opens every session.                                    *)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Sess,      \* session ids
    Win,       \* [P -> Nat]: each side's receive window per session
    Chunk,     \* most bytes in one data frame (dataplane.MaxChunk)
    MaxBytes,  \* bytes each side's app writes per session, at most
    Credit,    \* the stream keeps per-session credit (FALSE: an older agent)
    InProc,    \* sessions whose relay end is an inProcConn (Write and CloseWrite on two goroutines)
    CanBreak,  \* the stream may fail
    Fair,      \* the session whose apps are fair (liveness); the others' may stall
    Aborts,    \* the apps may close a session whole (a write or read error)
    EndAck,    \* a side ends cleanly only once the peer acknowledged its half close (finding 2's design; FALSE: the code as it is)
    Mutant     \* "none" or a bug put back (spec/README.md "Mutants")

P == {"r", "a"}
Peer(p) == IF p = "r" THEN "a" ELSE "r"
\* Credit.Consumed grants once a quarter window was consumed.
Thr(p) == IF Win[p] \div 4 < 1 THEN 1 ELSE Win[p] \div 4

VARIABLES
    ch,     \* [P -> Seq(frame)]: what p queued and the peer has not received
    alive,  \* the stream has not failed
    down,   \* [P -> BOOLEAN]: p's receive loop ended (CloseAll / closeAll ran)
    st,     \* [P -> [Sess -> {"none", "open", "gone"}]]
    cr,     \* [P -> [Sess -> credit]]: link.Credit's sessionCredit
    pend,   \* [P -> [Sess -> Nat]]: credit taken, data frame not yet queued
    gpend,  \* [P -> [Sess -> Nat]]: a window update decided, not yet queued
    q,      \* [P -> [Sess -> Nat]]: bytes queued for the app, unread
    rcv,    \* [P -> [Sess -> Nat]]: bytes received in order
    wr,     \* [P -> [Sess -> Nat]]: bytes the app wrote (queued, or dropped by a dead stream)
    wfin,   \* [P -> [Sess -> {"", "half", "whole"}]]: this side's close
    rfin,   \* [P -> [Sess -> {"", "half", "whole"}]]: the peer's close, received
    finAt,  \* [P -> [Sess -> Nat]]: wr at this side's close
    why,    \* [P -> [Sess -> {"", "eof", "lost", "reset"}]]: how the reader's side ended
    clean,  \* [P -> [Sess -> BOOLEAN]]: ended with both halves closed (FIN both ways)
    acked,  \* [P -> [Sess -> BOOLEAN]]: the peer acknowledged this side's half close (EndAck)
    err     \* receive steps that broke the protocol

vars == <<ch, alive, down, st, cr, pend, gpend, q, rcv, wr, wfin, rfin, finAt, why, clean, acked, err>>

NoCr == [on |-> FALSE, n |-> 0, held |-> 0, cons |-> 0]
Zero == [p \in P |-> [s \in Sess |-> 0]]
None == [p \in P |-> [s \in Sess |-> ""]]

Frame(t, s, n, off) == [t |-> t, s |-> s, n |-> n, off |-> off]

\* p queues f: lost once the stream failed (the relay's enqueue drops it at
\* <-a.Done, the agent's send fails at sendDone).
Put(p, f) == ch' = IF alive THEN [ch EXCEPT ![p] = Append(@, f)] ELSE ch

Init ==
    /\ ch = [p \in P |-> <<>>]
    /\ alive = TRUE
    /\ down = [p \in P |-> FALSE]
    /\ st = [p \in P |-> [s \in Sess |-> "none"]]
    /\ cr = [p \in P |-> [s \in Sess |-> NoCr]]
    /\ pend = Zero /\ gpend = Zero /\ q = Zero /\ rcv = Zero /\ wr = Zero
    /\ wfin = None /\ rfin = None /\ finAt = Zero /\ why = None
    /\ clean = [p \in P |-> [s \in Sess |-> FALSE]]
    /\ acked = [p \in P |-> [s \in Sess |-> FALSE]]
    /\ err = {}

\* handleLocalConn: attach (addSession, openCredit), then the open frame.
Open(s) ==
    /\ st["r"][s] = "none" /\ ~down["r"]
    /\ st' = [st EXCEPT !["r"][s] = "open"]
    /\ cr' = [cr EXCEPT !["r"][s] = [NoCr EXCEPT !.on = Credit, !.n = Win["a"]]]
    /\ Put("r", Frame("open", s, 0, 0))
    /\ UNCHANGED <<alive, down, pend, gpend, q, rcv, wr, wfin, rfin, finAt, why, clean, acked, err>>

\* send -> sendData: Credit.Acquire takes n (waiting for it), the frame is
\* queued by SendData. Without credit nothing is taken.
Write(p, s, n) ==
    /\ st[p][s] = "open" /\ wfin[p][s] = "" /\ pend[p][s] = 0
    /\ wr[p][s] + n <= MaxBytes
    /\ IF Credit
          THEN /\ cr[p][s].on /\ cr[p][s].n >= n
               /\ cr' = [cr EXCEPT ![p][s].n = @ - n]
          ELSE UNCHANGED cr
    /\ pend' = [pend EXCEPT ![p][s] = n]
    /\ UNCHANGED <<ch, alive, down, st, gpend, q, rcv, wr, wfin, rfin, finAt, why, clean, acked, err>>

\* An inProcConn's send looks again under the session's wmu: after a close
\* (CloseWrite or Close on another goroutine) it gives the credit back and
\* sends nothing (finding 1).
SendData(p, s) ==
    /\ pend[p][s] > 0
    /\ IF p = "r" /\ s \in InProc /\ wfin[p][s] # "" /\ Mutant # "inproc-closewrite"
          THEN /\ cr' = IF cr[p][s].on THEN [cr EXCEPT ![p][s].n = @ + pend[p][s]] ELSE cr
               /\ UNCHANGED <<ch, wr>>
          ELSE /\ Put(p, Frame("data", s, pend[p][s], wr[p][s]))
               /\ wr' = [wr EXCEPT ![p][s] = @ + pend[p][s]]
               /\ UNCHANGED cr
    /\ pend' = [pend EXCEPT ![p][s] = 0]
    /\ UNCHANGED <<alive, down, st, gpend, q, rcv, wfin, rfin, finAt, why, clean, acked, err>>

\* The app's EOF: relay closeWrite, agent pump's half close. Its data is
\* sent by the same goroutine first, except on an inProcConn, whose Write
\* may hold credit (pend) when CloseWrite runs on another goroutine.
CloseWrite(p, s) ==
    /\ st[p][s] = "open" /\ wfin[p][s] = ""
    /\ pend[p][s] = 0 \/ (p = "r" /\ s \in InProc)
    /\ wfin' = [wfin EXCEPT ![p][s] = "half"]
    /\ finAt' = [finAt EXCEPT ![p][s] = wr[p][s]]
    /\ IF Mutant = "fin-overtakes" /\ alive
          THEN ch' = [ch EXCEPT ![p] = <<Frame("half", s, 0, wr[p][s])>> \o @]
          ELSE Put(p, Frame("half", s, 0, wr[p][s]))
    /\ UNCHANGED <<alive, down, st, cr, pend, gpend, q, rcv, wr, rfin, why, clean, acked, err>>

\* A whole close: a write error, a read error, sessionConn.Close. Its
\* queue takes nothing more (markClosed); Finish forgets the session. A
\* send already holding credit still goes.
Abort(p, s) ==
    /\ st[p][s] = "open" /\ wfin[p][s] # "whole"
    /\ wfin' = [wfin EXCEPT ![p][s] = "whole"]
    /\ finAt' = [finAt EXCEPT ![p][s] = IF wfin[p][s] = "" THEN wr[p][s] ELSE @]  \* the first close's
    /\ Put(p, Frame("whole", s, 0, wr[p][s]))
    /\ UNCHANGED <<alive, down, st, cr, pend, gpend, q, rcv, wr, rfin, why, clean, acked, err>>

\* The app takes k bytes: Credit.Consumed, a window update once a quarter
\* window was consumed (queued by SendWin).
Read(p, s, k) ==
    /\ k <= q[p][s]
    /\ q' = [q EXCEPT ![p][s] = @ - k]
    /\ IF Credit /\ cr[p][s].on
          THEN LET c == cr[p][s].cons + k
                   g == IF Mutant = "grant-early" THEN c + q[p][s] - k ELSE c
               IN IF c >= Thr(p)
                     THEN /\ cr' = [cr EXCEPT ![p][s].cons = 0,
                                              ![p][s].held = IF @ > g THEN @ - g ELSE 0]
                          /\ gpend' = [gpend EXCEPT ![p][s] = @ + g]
                     ELSE /\ cr' = [cr EXCEPT ![p][s].cons = c]
                          /\ UNCHANGED gpend
          ELSE UNCHANGED <<cr, gpend>>
    /\ UNCHANGED <<ch, alive, down, st, pend, rcv, wr, wfin, rfin, finAt, why, clean, acked, err>>

SendWin(p, s) ==
    /\ gpend[p][s] > 0
    /\ Put(p, Frame("win", s, gpend[p][s], 0))
    /\ gpend' = [gpend EXCEPT ![p][s] = 0]
    /\ UNCHANGED <<alive, down, st, cr, pend, q, rcv, wr, wfin, rfin, finAt, why, clean, acked, err>>

\* The frame at the head of p's input waits for room in its session's queue:
\* the receive loop of every session on the stream waits with it.
HOL(p) ==
    /\ alive /\ ch[Peer(p)] # <<>>
    /\ LET f == Head(ch[Peer(p)]) IN
         /\ f.t = "data" /\ st[p][f.s] = "open" /\ rfin[p][f.s] = ""
         /\ q[p][f.s] + f.n > Win[p]

RecvData(p, f) ==
    LET s == f.s IN
    IF st[p][s] # "open" \/ (Credit /\ ~cr[p][s].on)
       THEN UNCHANGED <<cr, q, rcv, err>>  \* a session forgotten here: dropped, not charged
    ELSE IF Credit /\ f.n > Win[p] - cr[p][s].held
       THEN /\ err' = err \cup {"credit"}
            /\ UNCHANGED <<cr, q, rcv>>
    ELSE /\ IF Credit THEN cr' = [cr EXCEPT ![p][s].held = @ + f.n] ELSE UNCHANGED cr
         /\ IF rfin[p][s] = "whole" \/ wfin[p][s] = "whole"
               THEN UNCHANGED <<q, rcv, err>>  \* past a whole close (a closed queue): dropped
            ELSE IF rfin[p][s] = "half"
               THEN /\ err' = err \cup {"afterfin"}  \* pushed to a closed queue: dropped
                    /\ UNCHANGED <<q, rcv>>
            ELSE IF f.off # rcv[p][s]
               THEN /\ err' = err \cup {"order"}
                    /\ UNCHANGED <<q, rcv>>
            ELSE /\ q' = [q EXCEPT ![p][s] = @ + f.n]
                 /\ rcv' = [rcv EXCEPT ![p][s] = @ + f.n]
                 /\ UNCHANGED err

RecvWin(p, f) ==
    IF ~cr[p][f.s].on
       THEN UNCHANGED <<cr, err>>
    ELSE IF f.n > Win[Peer(p)] - cr[p][f.s].n
       THEN /\ err' = err \cup {"credit"}
            /\ UNCHANGED cr
    ELSE /\ cr' = [cr EXCEPT ![p][f.s].n = @ + f.n]
         /\ UNCHANGED err

\* relay Dispatch (peerClosedWrite / markClosed), agent closeFrame: the
\* reader gets EOF once the queued tail is read. After this side's whole
\* close the session takes nothing (its queue is closed, the agent forgot it).
RecvClose(p, f) ==
    IF st[p][f.s] = "open" /\ rfin[p][f.s] = "" /\ wfin[p][f.s] # "whole"
       THEN /\ rfin' = [rfin EXCEPT ![p][f.s] = f.t]
            /\ why' = [why EXCEPT ![p][f.s] = IF @ = "" THEN "eof" ELSE @]
            /\ UNCHANGED err
       ELSE UNCHANGED <<rfin, why, err>>

\* relay recvLoop (grpcCreditFrame, Dispatch), agent serveTCPStream. With
\* EndAck a half close taken (RecvClose) is acknowledged at once: every
\* byte before it was received (the stream is FIFO).
Recv(p) ==
    /\ alive /\ ch[Peer(p)] # <<>> /\ ~HOL(p)
    /\ LET f == Head(ch[Peer(p)])
           ack == EndAck /\ f.t = "half" /\ st[p][f.s] = "open" /\ rfin[p][f.s] = "" /\ wfin[p][f.s] # "whole"
       IN
         /\ ch' = [ch EXCEPT ![Peer(p)] = Tail(@),
                             ![p] = IF ack THEN Append(@, Frame("ack", f.s, 0, 0)) ELSE @]
         /\ acked' = IF f.t = "ack" /\ st[p][f.s] = "open" THEN [acked EXCEPT ![p][f.s] = TRUE] ELSE acked
         /\ CASE f.t = "open" ->
                   /\ p = "a"
                   /\ st' = [st EXCEPT !["a"][f.s] = "open"]
                   /\ cr' = [cr EXCEPT !["a"][f.s] = [NoCr EXCEPT !.on = Credit, !.n = Win["r"]]]
                   /\ UNCHANGED <<q, rcv, rfin, why, err>>
              [] f.t = "data" -> RecvData(p, f) /\ UNCHANGED <<st, rfin, why>>
              [] f.t = "win" -> RecvWin(p, f) /\ UNCHANGED <<st, q, rcv, rfin, why>>
              [] f.t = "ack" -> UNCHANGED <<st, cr, q, rcv, rfin, why, err>>
              [] OTHER -> RecvClose(p, f) /\ UNCHANGED <<st, cr, q, rcv>>
    /\ UNCHANGED <<alive, down, pend, gpend, wr, wfin, finAt, clean>>

\* The session ends here (relay removeSession, agent closeSession): both
\* halves closed (relay closeWrite/peerClosedWrite -> markClosed, agent
\* pump after <-written) or the peer's whole close, and the queue read; or
\* this side's whole close (what is queued is dropped). With EndAck both
\* halves end the session only once the peer acknowledged this side's.
Finish(p, s) ==
    /\ st[p][s] = "open"
    /\ \/ wfin[p][s] = "whole"
       \/ /\ q[p][s] = 0
          /\ \/ wfin[p][s] = "half" /\ rfin[p][s] = "half" /\ (acked[p][s] \/ ~EndAck)
             \/ rfin[p][s] = "whole"
    /\ st' = [st EXCEPT ![p][s] = "gone"]
    /\ cr' = [cr EXCEPT ![p][s] = NoCr]
    /\ q' = [q EXCEPT ![p][s] = 0]
    /\ clean' = [clean EXCEPT ![p][s] = wfin[p][s] = "half" /\ rfin[p][s] = "half"]
    /\ UNCHANGED <<ch, alive, down, pend, gpend, rcv, wr, wfin, rfin, finAt, why, acked, err>>

\* Without credit only: the head frame's session took nothing for the stall
\* bound (deliver, PushWithin): resetSession drops it and closes it.
Stall(p) ==
    /\ HOL(p) /\ ~Credit
    /\ LET s == Head(ch[Peer(p)]).s IN
         /\ st' = [st EXCEPT ![p][s] = "gone"]
         /\ why' = [why EXCEPT ![p][s] = "reset"]
         /\ q' = [q EXCEPT ![p][s] = 0]
         /\ wfin' = [wfin EXCEPT ![p][s] = "whole"]
         /\ finAt' = [finAt EXCEPT ![p][s] = wr[p][s]]
         /\ ch' = [ch EXCEPT ![Peer(p)] = Tail(@), ![p] = Append(@, Frame("whole", s, 0, wr[p][s]))]
    /\ UNCHANGED <<alive, down, cr, pend, gpend, rcv, wr, rfin, clean, acked, err>>

\* The stream fails: what was on the way is lost.
Break ==
    /\ CanBreak /\ alive
    /\ alive' = FALSE
    /\ ch' = [p \in P |-> <<>>]
    /\ UNCHANGED <<down, st, cr, pend, gpend, q, rcv, wr, wfin, rfin, finAt, why, clean, acked, err>>

\* relay unregister -> CloseAll (lose: an RST unless the agent had closed
\* its side), Credit.Close; agent closeAll (a target the relay had not
\* half-closed is reset).
Down(p) ==
    /\ ~alive /\ ~down[p]
    /\ down' = [down EXCEPT ![p] = TRUE]
    /\ st' = [st EXCEPT ![p] = [s \in Sess |-> IF @[s] = "open" THEN "gone" ELSE @[s]]]
    /\ why' = [why EXCEPT ![p] = [s \in Sess |->
                 IF st[p][s] = "open" /\ @[s] = ""
                    THEN IF Mutant = "lose-as-eof" THEN "eof" ELSE "lost"
                    ELSE @[s]]]
    /\ cr' = [cr EXCEPT ![p] = [s \in Sess |-> NoCr]]
    /\ pend' = [pend EXCEPT ![p] = [s \in Sess |-> 0]]
    /\ gpend' = [gpend EXCEPT ![p] = [s \in Sess |-> 0]]
    /\ UNCHANGED <<ch, alive, q, rcv, wr, wfin, rfin, finAt, clean, acked, err>>

App(p, s) ==
    \/ \E n \in 1..Chunk : Write(p, s, n)
    \/ CloseWrite(p, s)
    \/ Aborts /\ Abort(p, s)
    \/ \E k \in 1..q[p][s] : Read(p, s, k)

Next ==
    \/ \E s \in Sess : Open(s)
    \/ \E p \in P, s \in Sess : App(p, s) \/ SendData(p, s) \/ SendWin(p, s) \/ Finish(p, s)
    \/ \E p \in P : Recv(p) \/ Stall(p) \/ Down(p)
    \/ Break

\* Weak fairness on the steps that are not an app's, and on the apps of
\* session Fair (writes all, then closes; reads). Every step makes progress
\* (bytes and frames are bounded, nothing is undone), so a behaviour ends;
\* weak fairness on the union of the steps is then as strong as on each.
Protocol ==
    \/ \E p \in P : Recv(p) \/ Stall(p) \/ Down(p)
    \/ \E p \in P, s \in Sess : SendData(p, s) \/ SendWin(p, s) \/ Finish(p, s)
FairApps ==
    \/ Open(Fair)
    \/ \E p \in P :
         \/ \E n \in 1..Chunk : Write(p, Fair, n)
         \/ wr[p][Fair] = MaxBytes /\ CloseWrite(p, Fair)
         \/ \E k \in 1..q[p][Fair] : Read(p, Fair, k)
Fairness == WF_vars(Protocol) /\ WF_vars(FairApps)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Invariants (spec/README.md).                                             *)

NoProtocolError == err = {}

CreditBounds ==
    \A p \in P, s \in Sess : cr[p][s].on =>
        /\ 0 <= cr[p][s].n /\ cr[p][s].n <= Win[Peer(p)]
        /\ 0 <= cr[p][s].held /\ cr[p][s].held <= Win[p]

BufferBound == \A p \in P, s \in Sess : q[p][s] <= Win[p]

\* The bytes of frames of type t for s in a sequence of frames.
RECURSIVE SumOf(_, _, _)
SumOf(seq, t, s) ==
    IF seq = <<>> THEN 0
    ELSE (IF Head(seq).t = t /\ Head(seq).s = s THEN Head(seq).n ELSE 0) + SumOf(Tail(seq), t, s)
DataOnWay(p, s) == SumOf(ch[p], "data", s)
WinOnWay(p, s) == SumOf(ch[p], "win", s)

\* p's credit for s plus what is on its way or held at the peer is the
\* peer's window, while both sides know s.
Conservation ==
    Credit /\ alive =>
        \A p \in P, s \in Sess : cr[p][s].on /\ cr[Peer(p)][s].on =>
            cr[p][s].n + pend[p][s] + DataOnWay(p, s)
              + cr[Peer(p)][s].held + gpend[Peer(p)][s] + WinOnWay(Peer(p), s) = Win[Peer(p)]

NoHOL == \A p \in P : ~HOL(p)

TypedEnd == \A p \in P, s \in Sess : why[p][s] = "eof" => rcv[p][s] = finAt[Peer(p)][s]

DownEndsAll == \A p \in P : down[p] => \A s \in Sess : st[p][s] # "open"

\* A clean end at p only once the peer took p's tail and close: holds with
\* EndAck; without (the code as it is, finding 2) a clean end can come while
\* they are on the way when the stream fails.
NoSilentLoss ==
    \A p \in P, s \in Sess : clean[p][s] /\ ~alive =>
        rcv[Peer(p)][s] = finAt[p][s] /\ rfin[Peer(p)][s] # ""

(* Liveness. *)
EndsAfterBreak == (~alive) ~> (\A p \in P, s \in Sess : st[p][s] # "open")
Isolation == <>(\A p \in P : st[p][Fair] = "gone")
=============================================================================
