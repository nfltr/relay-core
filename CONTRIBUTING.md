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
