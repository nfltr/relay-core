#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0
#
# run-examples.sh: builds every program under examples/ and runs each one
# that needs no running relay (all but quickstart, which
# scripts/readme-quickstart.sh runs against nfltr-relay-core) under a
# timeout (EXAMPLE_TIMEOUT seconds, default 60); fails on a non-zero exit
# or a timeout.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
export GOWORK=off
bin="$(mktemp -d)"
trap 'rm -rf "$bin"' EXIT
CGO_ENABLED=0 go build -o "$bin/" ./examples/... || exit 1
fail=0
for d in examples/*/; do
	name="$(basename "$d")"
	[ "$name" = quickstart ] && continue
	"$bin/$name" >"$bin/$name.out" 2>&1 &
	pid=$!
	( sleep "${EXAMPLE_TIMEOUT:-60}"; kill "$pid" 2>/dev/null ) &
	watchdog=$!
	wait "$pid"
	status=$?
	kill "$watchdog" 2>/dev/null
	wait "$watchdog" 2>/dev/null
	if [ "$status" -eq 0 ]; then
		echo "examples: $name ok: $(tail -n 1 "$bin/$name.out")"
	else
		echo "examples: $name FAILED (exit $status)" >&2
		cat "$bin/$name.out" >&2
		fail=1
	fi
done
exit "$fail"
