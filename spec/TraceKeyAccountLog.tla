------------------------- MODULE TraceKeyAccountLog -------------------------
(* A recorded KeyAccount trace (keyfile trace_on.go), read once: the ids,  *)
(* names, codes and keys it names size the model's constants.              *)
EXTENDS Json, Sequences, Naturals

CONSTANT TraceFile

Log == ndJsonDeserialize(TraceFile)

Field(f) == {Log[i][f] : i \in 1..Len(Log)} \ {""}

TraceIds == Field("m") \cup Field("a")
TraceNames == Field("n")
TraceCodes == Field("c")
TraceKeys == Field("k")
=============================================================================
