#!/bin/sh
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# check-api.sh: the module's exported API against its latest release
# before HEAD (the newest tag v*), with apidiff. The report must equal
# (line for line, in any order) api/<that tag>.txt, the API changes this
# release makes on purpose (no file: none). UPDATE=1 writes the file from
# the report instead. No v* tag before HEAD (a clone without the tags, a
# shallow one) fails: passing would compare nothing (lane APITAGS).
set -eu
APIDIFF=golang.org/x/exp/cmd/apidiff@v0.0.0-20260908205506-85c1c2202aba
root=$(git rev-parse --show-toplevel)
cd "$root"
base=$(git describe --tags --abbrev=0 --match "v[0-9]*" HEAD^ 2>/dev/null || true)
if [ -z "$base" ]; then
	echo "check-api: no release tag v* before HEAD (a shallow clone? fetch the tags: git fetch --unshallow --tags)" >&2
	exit 1
fi
mod=$(go list -m)
tmp=$(mktemp -d)
trap 'git worktree remove --force "$tmp/old" >/dev/null 2>&1 || true; rm -rf "$tmp"' EXIT
git worktree add --quiet --detach "$tmp/old" "$base"
(cd "$tmp/old" && go run "$APIDIFF" -m -w "$tmp/old.exp" "$mod")
go run "$APIDIFF" -m "$tmp/old.exp" "$mod" >"$tmp/report"
want="api/$(basename "$base").txt"
if [ "${UPDATE:-}" = 1 ]; then
	if [ -s "$tmp/report" ]; then mkdir -p api && cp "$tmp/report" "$want"; else rm -f "$want"; fi
	echo "check-api: wrote $want"
	exit 0
fi
: >"$tmp/want"
[ ! -f "$want" ] || cp "$want" "$tmp/want"
# apidiff orders the packages of a module as it finds them, which differs
# between hosts: compare the lines sorted.
LC_ALL=C sort "$tmp/want" >"$tmp/want.sorted"
LC_ALL=C sort "$tmp/report" >"$tmp/report.sorted"
if ! diff -u "$tmp/want.sorted" "$tmp/report.sorted"; then
	echo "check-api: the API changed since $base other than $want lists (- listed, + found)." >&2
	echo "  On purpose? Record it in CHANGELOG.md with the migration, then: UPDATE=1 make check-api" >&2
	exit 1
fi
echo "check-api: API changes since $base as listed"
