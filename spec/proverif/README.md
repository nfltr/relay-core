# Symbolic model of the E2EE share gate

ProVerif 2.05 (Dolev-Yao). `make spec-proverif` (in `make ci`, seconds):
`spec/proverif/run.sh` runs each line of `expect.txt`, "<model> <flags>
<results>": the model with the lines its flags select (`(* ONLY:f *)`,
`(* NOT:f *)`), its results in query order (T holds, F attack, ? not
proved). A flag is a planted mutant; its target query reads F.
ProVerif comes from `scripts/fetch-proverif.sh` (pinned source,
built once into `~/.cache/relay-proverif`, offline after). The same two
scripts are in github.com/nfltr/relay, relay-server and nfltr, which model
the other E2EE paths (nfltr's `docs/e2ee.md` "Formal proofs").

## e2ee_share.pv

Part 1, the gate (the relay honest; the attacker is every visitor and the
network). A share whose policy carries a credential reaches only an agent
stream that names it in `x-nfltr-share-policy`, checked on the replica that
holds the stream, also for a visitor who came in on another replica: the
replica link carries the share and is mutually authenticated (private in
the model). The agent checks the credential on every E2EE session. A
passthrough or older agent stream (names nothing, checks nothing) never
gets such a connection.

Part 2, the relay hostile: it owns the share domain, its DNS and the route
of every ACME challenge, so a WebPKI certificate (ACME at the agent) does
not keep it out; it also returned the policy, so it knows the credential.
Only the agent's own fingerprint, checked by the visitor, does (relay's
`spec/proverif/e2ee_tcp.pv`).

| # | query | result |
|---|---|---|
| 1 | the backend behind a credentialed share, against visitors without the credential, on either replica | holds |
| 2 | a browser on the share's ACME certificate, against the relay | attack (documented: the relay can be issued a certificate for its own domain) |

Mutants: the gate skipped on the local path, the gate skipped on a session
the link brought, the share not carried on the link, the agent skipping the
credential.

Two relays: Part 1 is about an honest relay, so it models two replicas
(visitor on X, agent on Y) and the link; against hostile relays the gate
is no defence at all (Part 2).

## Bindings

The model lists, as `BIND:<id>` lines in a comment, where query 1 relies on
a check that a visitor's connection could slip past, and a Go test has one
row per id that sends a protected share's connection there through the real
code and requires a refusal; the test fails when the model's ids and its
rows differ, so a binding added to the model needs its row (`make
spec-bindings`, in `make ci`; nfltr
`docs/decisions/e2ee-model-code-link-2026-11-09.md`).

| id | row | where |
|---|---|---|
| `gate.here` | a share to an agent whose primary names no share, on its gRPC and data-connection streams, and handed on after attach (`TestProtectedShareOnAuxStreams`, `TestShareHandedOnToUnenforcingStreamRefused`) | `core/session/e2ee_share_bindings_test.go` `TestE2EEShareModelBindings` |
| `gate.link` | a share the replica link brought, to a passthrough stream, on the frame streams and on an agent that uses the agent link: refused `SHARE_POLICY_UNENFORCED` before the agent is asked anything | same |
| `relayserver.link.share` | the open on the replica link carries the share, and a cross-replica share to an agent that does not enforce it is refused | relay-server `core/replicas/e2ee_share_bindings_test.go` |
| `relay.agent.cred` | the agent's Basic, Bearer and header checks | github.com/nfltr/relay `client/agent/e2ee_share_bindings_test.go` |

The relay-core test lists the other repositories' ids
(`shareForeignRows`). Planted Go mutants caught (lane E2EEVECTORS-2): every
local gate skipped, an auxiliary stream trusted for its own metadata, the
gate skipped on the link (`peer_link_tcp.go`), the share left out of the
link's open (relay-server), the agent skipping the credential (relay).
Query 2 is an attack by design and has no row.

## Environment assumptions

| assumption | source |
|---|---|
| the replica link authenticates both replicas | relay-server `core/replicas` (mTLS, `S2S_TOKEN`) |
| SHA-256 and the constant-time digest compare are ideal | the agent compares SHA-256 digests (`e2ee_share_policy.go`) |
| a CA issues to whoever answers the challenge for a name | RFC 8555 |

## Code map

| model | code |
|---|---|
| e2ee_share.pv | core/session/e2ee_share_gate.go |
| e2ee_share.pv | core/session/dispatcher_tcp.go |
| e2ee_share.pv | core/session/peer_link_tcp.go |
