----------------------------- MODULE MCAgentLink -----------------------------
(* Small sizes for model checking (spec/README.md "What is abstracted").    *)
EXTENDS AgentLink
MCWin == [p \in {"r", "a"} |-> 2]
MCWin1 == [p \in {"r", "a"} |-> 1]
=============================================================================
