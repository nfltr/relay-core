---- MODULE KeyAccount_TTrace_1794884982 ----
EXTENDS KeyAccount, Sequences, TLCExt, KeyAccount_TEConstants, Toolbox, Naturals, TLC

_expression ==
    LET KeyAccount_TEExpression == INSTANCE KeyAccount_TEExpression
    IN KeyAccount_TEExpression!expression
----

_trace ==
    LET KeyAccount_TETrace == INSTANCE KeyAccount_TETrace
    IN KeyAccount_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        code = ((c1 :> [acct |-> i1, name |-> n1] @@ c2 :> [acct |-> i1, name |-> n1]))
        /\
        spent = ((c1 :> {} @@ c2 :> {}))
        /\
        bound = ((i1 :> None @@ i2 :> None @@ i3 :> None))
        /\
        fixed = ((i1 :> i1 @@ i2 :> i2 @@ i3 :> i3))
        /\
        used = ({i1, i2, i3})
        /\
        certs = ({})
        /\
        live = ({})
        /\
        acct = ((i1 :> i1 @@ i2 :> i2 @@ i3 :> i3))
        /\
        nm = ((i1 :> n1 @@ i2 :> n1 @@ i3 :> n1))
    )
----

_init ==
    /\ certs = _TETrace[1].certs
    /\ live = _TETrace[1].live
    /\ fixed = _TETrace[1].fixed
    /\ acct = _TETrace[1].acct
    /\ spent = _TETrace[1].spent
    /\ nm = _TETrace[1].nm
    /\ used = _TETrace[1].used
    /\ bound = _TETrace[1].bound
    /\ code = _TETrace[1].code
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ certs  = _TETrace[i].certs
        /\ certs' = _TETrace[j].certs
        /\ live  = _TETrace[i].live
        /\ live' = _TETrace[j].live
        /\ fixed  = _TETrace[i].fixed
        /\ fixed' = _TETrace[j].fixed
        /\ acct  = _TETrace[i].acct
        /\ acct' = _TETrace[j].acct
        /\ spent  = _TETrace[i].spent
        /\ spent' = _TETrace[j].spent
        /\ nm  = _TETrace[i].nm
        /\ nm' = _TETrace[j].nm
        /\ used  = _TETrace[i].used
        /\ used' = _TETrace[j].used
        /\ bound  = _TETrace[i].bound
        /\ bound' = _TETrace[j].bound
        /\ code  = _TETrace[i].code
        /\ code' = _TETrace[j].code

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("KeyAccount_TTrace_1794884982.json", _TETrace)

=============================================================================

 Note that you can extract this module `KeyAccount_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `KeyAccount_TEExpression.tla` file takes precedence 
  over the module `KeyAccount_TEExpression` below).

---- MODULE KeyAccount_TEExpression ----
EXTENDS KeyAccount, Sequences, TLCExt, KeyAccount_TEConstants, Toolbox, Naturals, TLC

expression == 
    [
        \* To hide variables of the `KeyAccount` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        certs |-> certs
        ,live |-> live
        ,fixed |-> fixed
        ,acct |-> acct
        ,spent |-> spent
        ,nm |-> nm
        ,used |-> used
        ,bound |-> bound
        ,code |-> code
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_certsUnchanged |-> certs = certs'
        
        \* Format the `certs` variable as Json value.
        \* ,_certsJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(certs)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_certsModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].certs # _TETrace[s-1].certs
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE KeyAccount_TETrace ----
\*EXTENDS KeyAccount, IOUtils, KeyAccount_TEConstants, TLC
\*
\*trace == IODeserialize("KeyAccount_TTrace_1794884982.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE KeyAccount_TETrace ----
EXTENDS KeyAccount, KeyAccount_TEConstants, TLC

trace == 
    <<
    ([code |-> (c1 :> None @@ c2 :> None),spent |-> (c1 :> {} @@ c2 :> {}),bound |-> (i1 :> None @@ i2 :> None @@ i3 :> None),fixed |-> (i1 :> None @@ i2 :> None @@ i3 :> None),used |-> {},certs |-> {},live |-> {},acct |-> (i1 :> None @@ i2 :> None @@ i3 :> None),nm |-> (i1 :> None @@ i2 :> None @@ i3 :> None)]),
    ([code |-> (c1 :> None @@ c2 :> None),spent |-> (c1 :> {} @@ c2 :> {}),bound |-> (i1 :> None @@ i2 :> None @@ i3 :> None),fixed |-> (i1 :> i1 @@ i2 :> None @@ i3 :> None),used |-> {i1},certs |-> {},live |-> {i1},acct |-> (i1 :> i1 @@ i2 :> None @@ i3 :> None),nm |-> (i1 :> n1 @@ i2 :> None @@ i3 :> None)]),
    ([code |-> (c1 :> None @@ c2 :> None),spent |-> (c1 :> {} @@ c2 :> {}),bound |-> (i1 :> None @@ i2 :> None @@ i3 :> None),fixed |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> None),used |-> {i1, i2},certs |-> {},live |-> {i1, i2},acct |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> None),nm |-> (i1 :> n1 @@ i2 :> n1 @@ i3 :> None)]),
    ([code |-> (c1 :> None @@ c2 :> None),spent |-> (c1 :> {} @@ c2 :> {}),bound |-> (i1 :> None @@ i2 :> None @@ i3 :> None),fixed |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),used |-> {i1, i2, i3},certs |-> {},live |-> {i1, i2, i3},acct |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),nm |-> (i1 :> n1 @@ i2 :> n1 @@ i3 :> n1)]),
    ([code |-> (c1 :> [acct |-> i1, name |-> n1] @@ c2 :> None),spent |-> (c1 :> {} @@ c2 :> {}),bound |-> (i1 :> None @@ i2 :> None @@ i3 :> None),fixed |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),used |-> {i1, i2, i3},certs |-> {},live |-> {i1, i2, i3},acct |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),nm |-> (i1 :> n1 @@ i2 :> n1 @@ i3 :> n1)]),
    ([code |-> (c1 :> [acct |-> i1, name |-> n1] @@ c2 :> [acct |-> i1, name |-> n1]),spent |-> (c1 :> {} @@ c2 :> {}),bound |-> (i1 :> None @@ i2 :> None @@ i3 :> None),fixed |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),used |-> {i1, i2, i3},certs |-> {},live |-> {i1, i2, i3},acct |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),nm |-> (i1 :> n1 @@ i2 :> n1 @@ i3 :> n1)]),
    ([code |-> (c1 :> [acct |-> i1, name |-> n1] @@ c2 :> [acct |-> i1, name |-> n1]),spent |-> (c1 :> {} @@ c2 :> {}),bound |-> (i1 :> None @@ i2 :> None @@ i3 :> None),fixed |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),used |-> {i1, i2, i3},certs |-> {},live |-> {i2, i3},acct |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),nm |-> (i1 :> n1 @@ i2 :> n1 @@ i3 :> n1)]),
    ([code |-> (c1 :> [acct |-> i1, name |-> n1] @@ c2 :> [acct |-> i1, name |-> n1]),spent |-> (c1 :> {} @@ c2 :> {}),bound |-> (i1 :> None @@ i2 :> None @@ i3 :> None),fixed |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),used |-> {i1, i2, i3},certs |-> {},live |-> {i3},acct |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),nm |-> (i1 :> n1 @@ i2 :> n1 @@ i3 :> n1)]),
    ([code |-> (c1 :> [acct |-> i1, name |-> n1] @@ c2 :> [acct |-> i1, name |-> n1]),spent |-> (c1 :> {} @@ c2 :> {}),bound |-> (i1 :> None @@ i2 :> None @@ i3 :> None),fixed |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),used |-> {i1, i2, i3},certs |-> {},live |-> {},acct |-> (i1 :> i1 @@ i2 :> i2 @@ i3 :> i3),nm |-> (i1 :> n1 @@ i2 :> n1 @@ i3 :> n1)])
    >>
----


=============================================================================

---- MODULE KeyAccount_TEConstants ----
EXTENDS KeyAccount

CONSTANTS i1, i2, i3, n1, n2, c1, c2, k1, k2

=============================================================================

---- CONFIG KeyAccount_TTrace_1794884982 ----
CONSTANTS
    Ids = { i1 , i2 , i3 }
    Names = { n1 , n2 }
    None = None
    Codes = { c1 , c2 }
    Keys = { k1 , k2 }
    Mutant = "none"
    i2 = i2
    n2 = n2
    n1 = n1
    k2 = k2
    i3 = i3
    None = None
    c2 = c2
    i1 = i1
    c1 = c1
    k1 = k1

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Tue Nov 17 08:41:52 IST 2026