#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0
#
# readme-quickstart.sh [source]: runs README.md's quickstart, the shell
# block after the <!-- quickstart --> marker, as a reader would, in an
# empty directory: its clone URL becomes source (a path or URL; default
# this checkout, so its committed HEAD), and the relay's ports 8080 and 9090
# become free ones (the quickstart's addresses and the relay's PROXY_PORT
# and GRPC_PORT; the internal and metrics ports are kernel-chosen), so a
# busy machine runs it too. It passes when the block exits 0 and printed
# the bytes it sent through the relay.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${1:-$ROOT}"
block="$(awk '/<!-- quickstart -->/ { f = 1; next } f && /^```sh/ { g = 1; next } g && /^```/ { exit } g' "$ROOT/README.md")"
[ -n "$block" ] || { echo "readme-quickstart: no quickstart block in README.md" >&2; exit 1; }
port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
proxy="$(port)" grpc="$(port)"
block="${block//https:\/\/github.com\/nfltr\/relay-core/$SRC relay-core}"
block="${block//127.0.0.1:8080/127.0.0.1:$proxy}"
block="${block//127.0.0.1:9090/127.0.0.1:$grpc}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
# The block runs under QUICKSTART_TIMEOUT seconds (default 600: a fresh
# clone builds from source), with none of the relay's or nfltr's settings
# from this environment.
(
	cd "$work" && exec env -u NFLTR_API_KEY -u SECURITY_APIKEY_FILE -u LISTEN_ADDRESS -u TLS_ENABLE GOWORK=off \
		PROXY_PORT="$proxy" GRPC_PORT="$grpc" INTERNAL_PORT=0 METRICS_PORT=0 bash -euo pipefail -c "$block"
) >"$work.out" 2>&1 &
pid=$!
( sleep "${QUICKSTART_TIMEOUT:-600}"; kill "$pid" 2>/dev/null ) &
watchdog=$!
status=0
wait "$pid" || status=$?
kill "$watchdog" 2>/dev/null || true
out="$(cat "$work.out")"
rm -f "$work.out"
[ "$status" -eq 0 ] || {
	echo "readme-quickstart: the quickstart failed:" >&2
	echo "$out" >&2
	exit 1
}
echo "$out" | grep -q '^echoed: hello through nfltr-relay-core$' || {
	echo "readme-quickstart: no echoed bytes in:" >&2
	echo "$out" >&2
	exit 1
}
echo "readme-quickstart: ok"
