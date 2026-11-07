#!/bin/sh
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0
# run.sh checks the ProVerif models (README.md): each line of expect.txt is
# "<model> <flags> <results>"; the model is run with those flags (a line
# ending "(* ONLY:f *)" is kept only with flag f, one ending "(* NOT:f *)"
# only without it; "-" is no flag) and its RESULT lines, in order, must read
# <results> (T true, F false: an attack, ? not proved, X not judged). A mutant is a flag whose line
# lists F where the model holds T. Exit 1 on any difference.
set -eu
cd "$(dirname "$0")"
pv=$(../../scripts/fetch-proverif.sh)
out=${PROVERIF_OUT:-$(mktemp -d)}
mkdir -p "$out"
fail=0
filter=${1:-}
while read -r model flags want; do
	case $model in '' | '#'*) continue ;; esac
	if [ -n "$filter" ] && [ "$filter" != "$model" ]; then continue; fi
	tag=$(echo "$model.$flags" | tr ',' '_')
	awk -v flags=",$flags," '{
		keep = 1
		i = index($0, "(* ONLY:"); if (i) { f = substr($0, i + 8); sub(/ .*/, "", f); if (index(flags, "," f ",") == 0) keep = 0 }
		i = index($0, "(* NOT:"); if (i) { f = substr($0, i + 7); sub(/ .*/, "", f); if (index(flags, "," f ",") != 0) keep = 0 }
		if (keep) print }' "$model.pv" >"$out/$tag.pv"
	start=$(date +%s)
	if ! "$pv" "$out/$tag.pv" >"$out/$tag.log" 2>&1; then
		echo "FAIL $model [$flags]: proverif exited non-zero, see $out/$tag.log"
		fail=1
		continue
	fi
	got=$(grep '^RESULT [^(]' "$out/$tag.log" | sed -e 's/.* is true\.$/T/' -e 's/.* is false\.$/F/' -e 's/.*cannot be proved\.$/?/' | tr -d '\n')
	secs=$(($(date +%s) - start))
	# X in <results>: a query the line does not judge (a mutant's other queries).
	if awk -v g="$got" -v w="$want" 'BEGIN { if (length(g) != length(w)) exit 1
		for (i = 1; i <= length(w); i++) { c = substr(w, i, 1); if (c != "X" && c != substr(g, i, 1)) exit 1 } }'; then
		echo "ok   $model [$flags] $got (${secs}s)"
	else
		echo "FAIL $model [$flags]: got $got, want $want (${secs}s), see $out/$tag.log"
		fail=1
	fi
done <"${PROVERIF_EXPECT:-expect.txt}"
exit $fail
