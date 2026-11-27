#!/usr/bin/env bash
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# fuzz-short.sh: every Go fuzz target, FUZZTIME each (default 10s;
# make fuzz-long: 30m each). FUZZPARALLEL caps the workers per target
# (default: GOMAXPROCS). A crasher fails the run and go writes it to
# <package>/testdata/fuzz/<Target>/; commit it with the fix as a
# regression seed (it then runs in every plain `go test`). A new target is
# added here: the run fails when this list and
# `grep -rn 'func Fuzz' --include='*_test.go' .` differ.
# FUZZ_OUT=<file> also writes one "<package> <target> <exit status>" line per
# target.
set -uo pipefail
cd "$(dirname "$0")/.."
TARGETS=(
	"core/identity FuzzTemplate"
	"core/sharecode FuzzValidCode"
	"core/session FuzzExtractSNI"
	"core/session FuzzExtractSNIFromTLSClient"
	"core/session FuzzParseSNIExtension"
)
# The list must name every target in the module.
want=$(grep -rhoE '^func Fuzz[A-Za-z0-9_]+' --include='*_test.go' . | sed 's/^func //' | LC_ALL=C sort)
have=$(for t in "${TARGETS[@]}"; do echo "${t#* }"; done | LC_ALL=C sort)
if [ "$want" != "$have" ]; then
	echo "fuzz-short: TARGETS differs from the module's fuzz targets:" >&2
	diff <(echo "$have") <(echo "$want") >&2
	exit 1
fi
failed=0
for t in "${TARGETS[@]}"; do
	read -r dir name <<<"$t"
	echo "── ${dir} ${name} (${FUZZTIME:-10s})"
	start=$(date +%s)
	rc=0
	(cd "${dir}" && go test -run '^$' -fuzz "^${name}\$" -fuzztime "${FUZZTIME:-10s}" \
		-fuzzminimizetime 100x ${FUZZPARALLEL:+-parallel "${FUZZPARALLEL}"} .) || rc=$?
	[ -n "${FUZZ_OUT:-}" ] && echo "${dir} ${name} ${rc}" >>"${FUZZ_OUT}"
	[ "$rc" = 0 ] || failed=1
	echo "── ${dir} ${name}: $(($(date +%s) - start))s"
done
exit "${failed}"
