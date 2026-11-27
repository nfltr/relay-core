#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0
#
# test-sharded.sh <go test flags...> -- <packages...>: go test of the
# packages, the long poles (below) each split by test name into shards that
# run at once beside the other packages, each shard its own process and log
# (all printed in order once all ended); fails if any part failed. A
# package's last shard skips what its other shards run, so every test runs
# exactly once. core/session's ~420 tests run mostly one at a time and wait
# on loopback connections and timers: it was the race pass's long pole
# (181 s of ~240 s on the runner). The pattern is relay-server's
# scripts/test-sharded.sh.
set -uo pipefail -f # -f: the shard regexps are not globs
cd "$(dirname "$0")/.."
flags=()
while [ $# -gt 0 ] && [ "$1" != -- ]; do flags+=("$1"); shift; done
shift
mod=$(go list -m)
# "<package> <run regexp>...": the last shard is the rest.
sharded='core/session ^Test[A-C] ^TestD ^Test[E-P]'
pkgs=$(go list "$@") || exit
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
names=()
part() { # name, go test args...
	local n=$1
	shift
	(
		go test -count=1 ${flags[@]+"${flags[@]}"} "$@" >"$dir/$n.log" 2>&1
		echo $? >"$dir/$n.rc"
	) &
	names+=("$n")
}
rest=$pkgs
while read -r p shards; do
	grep -qx "$mod/$p" <<<"$pkgs" || continue
	rest=$(grep -vx "$mod/$p" <<<"$rest")
	i=0
	for r in $shards; do
		part "${p//\//_}-$i" -run "$r" "$mod/$p"
		i=$((i + 1))
	done
	part "${p//\//_}-$i" -skip "$(tr ' ' '|' <<<"$shards")" "$mod/$p"
done <<<"$sharded"
# shellcheck disable=SC2086
[ -z "$rest" ] || part packages $rest
wait
failed=0
for n in "${names[@]}"; do
	echo "── $n"
	cat "$dir/$n.log"
	read -r rc <"$dir/$n.rc"
	[ "$rc" -eq 0 ] || { echo "test-sharded: $n exit $rc"; failed=1; }
done
exit "$failed"
