----------------------------- MODULE MCAgentLink -----------------------------
(* Small sizes for model checking (spec/README.md "What is abstracted").    *)
EXTENDS AgentLink
MCWin == [p \in {"r", "a"} |-> 2]
MCWin1 == [p \in {"r", "a"} |-> 1]
MCWinW == [p \in {"r", "a"} |-> IF p = "r" THEN 1 ELSE 2]  \* AgentLinkWrite.cfg: the relay's writes
=============================================================================
