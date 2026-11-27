#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0
#
# time-to-first-byte.sh [source]: the wall time of README.md's quickstart
# from `git clone` of source (a path or URL; default this checkout) to the
# first bytes echoed through the relay (scripts/readme-quickstart.sh). The
# Go module and build caches are the machine's own: a cold cache adds the
# module downloads and a full build.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
now() { perl -MTime::HiRes=time -e 'printf "%.2f\n", time'; }
start="$(now)"
"$ROOT/scripts/readme-quickstart.sh" "${1:-$ROOT}"
end="$(now)"
echo "time-to-first-byte: $(echo "$end - $start" | bc) s"
