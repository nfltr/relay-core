# Contributing

Issues are welcome. Pull requests are by arrangement: open an issue
first, and sign off every commit (Developer Certificate of Origin,
`git commit -s`). This policy is a placeholder and may change. Security
reports: see [SECURITY.md](SECURITY.md).

For the maintainers:

- `make ci` runs what the GitHub workflow runs (`.github/workflows/check.yml`,
  read by `scripts/ci.sh`); GitHub Actions is not a gate, `make ci` is.
  `make hooks` installs a pre-push hook that runs it for pushes to main
  and tags.
- Every `.go`, `.sh` and Makefile starts with
  `// Copyright 2026 nfltr` and `// SPDX-License-Identifier: Apache-2.0`
  (`#` comments outside Go).
- An exported API change is recorded in [CHANGELOG.md](CHANGELOG.md)
  and, after the first release, in `api/` (`UPDATE=1 make check-api`).

## Dependencies behind ports

Every external dependency (gRPC and its wire, quic-go, config and metrics
libraries, databases, Redis, cloud and provider APIs, docker, any library with
a wide surface) is reached through a narrow port this repository owns: only
its adapter and the escape hatches (migration, backup and restore, break-glass
tools) import or name it, and code above it, tests included, uses the port.
`make check-dep-ports` (`scripts/check-dep-ports.sh`, a copy of nfltr's, kept
identical; in `make ci`) checks it against `scripts/dep-ports.txt` (per
dependency, its adapter paths). Today's violations are listed in
`scripts/dep-ports-allow.txt`, which may only shrink: a new violation fails,
and so does an entry whose violation is gone. A new dependency gets its rule
and adapter in the change that adds it.

## Verify the design with TLA+ before writing code

A change to a protocol, the wire format, concurrency, storage or a
cross-process state machine, whose bugs would be silent (lost, duplicated or
reordered data; a session, lease or grant that never ends or is used twice; a
revocation that does not take effect), starts as a TLA+ spec of the design in
`spec/` (`spec/README.md` maps each action to the Go function it models). Check it with TLC before any code: the invariants and liveness
properties hold, `-coverage 1` shows every action taken, and a planted
mutant of each failure the design prevents is caught. Code starts only after
that check passes. The spec change lands before or with the code, and the
trace recorder then validates the implementation against the spec.

Each layer's spec states what the layer above may assume; the agent link and handoff specs assume only the link's session guarantees. Every bug found
becomes a mutant the checker must catch. The small checks run in `make ci`;
the larger configs, liveness and trace checks run nightly (`make spec`, `spec-mutants` in `make ci`; `spec-nightly`, `spec-traces`). Not for CLI,
docs, handlers, config or value transforms: tests and fuzzing cover those.
