#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# check-binary-size.sh: fails when the stripped linux/amd64 nfltr-relay-core
# (cmd/nfltr-relay-core, -trimpath -ldflags "-s -w") exceeds its budget,
# CORE_SIZE_BUDGET_BYTES (the Makefile's: measured + 5 % when it was set),
# and then prints the largest packages of an unstripped build (symbol
# sizes, go tool nm) to show what grew.
set -euo pipefail
budget="${CORE_SIZE_BUDGET_BYTES:?set CORE_SIZE_BUDGET_BYTES}"
cd "$(dirname "$0")/.."
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
build() { CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath "$@" ./cmd/nfltr-relay-core; }
build -ldflags "-s -w" -o "$tmp/core"
size="$(wc -c <"$tmp/core" | tr -d ' ')"
if ((size <= budget)); then
	echo "nfltr-relay-core linux/amd64: $size bytes (budget $budget)"
	exit 0
fi
echo "nfltr-relay-core linux/amd64: $size bytes exceeds the budget of $budget bytes by $((size - budget))." >&2
echo "Largest packages of nfltr-relay-core (symbol sizes of an unstripped build):" >&2
build -o "$tmp/core-full"
# A symbol's package: its name up to the first "." after the last "/".
go tool nm -size "$tmp/core-full" | awk '$2 ~ /^[0-9]+$/ && $3 ~ /^[TtRrDd]$/ && NF >= 4 {
	name = $4; i = match(name, /\/[^\/]*$/); rest = (i ? substr(name, i) : name)
	j = index(rest, "."); pkg = (j ? substr(name, 1, (i ? i - 1 : 0) + j - 1) : name)
	sum[pkg] += $2
} END { for (p in sum) printf "%12d %s\n", sum[p], p }' | sort -rn | awk 'NR <= 15' >&2
echo "Cut the import edge that pulled the growth in (cmd/nfltr-relay-core/links_test.go lists what the core may link), or raise CORE_SIZE_BUDGET_BYTES in the Makefile with the reason in CHANGELOG.md." >&2
exit 1
