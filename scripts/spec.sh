#!/bin/sh
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# spec.sh check|liveness|mutants|simulate|traces|trace F: the TLA+ models of
# spec/ (spec/README.md).
#   check     TLC on spec/AgentLink.cfg and spec/Handoff.cfg: invariants (CI).
#   mutants   each Mutant must be caught, and each documented race found
#             (spec/README.md "Findings") (CI).
#   liveness  TLC on spec/AgentLinkLive.cfg, AgentLinkBreak.cfg and
#             HandoffLive.cfg (weekly).
#   simulate  random walks on two sessions with larger sizes (weekly).
#   traces    the core/session tests in TRACE_TESTS run with -tags
#             relaytrace; each stream's trace checked against spec/Trace.tla
#             (weekly).
#   trace F   one recorded trace.
# TLC_WORKERS (default 4) caps TLC's threads; COVERAGE=1 adds -coverage 1.
# Java and tla2tools.jar come from scripts/fetch-tla.sh.
set -eu
cd "$(dirname "$0")/.."
tools=$(scripts/fetch-tla.sh)
java=$(echo "$tools" | sed -n 1p)
jar=$(echo "$tools" | sed -n 2p)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# tlc CFG MODULE [args]: TLC in spec/, its state files in $META (default $work/states).
tlc() {
	c=$1 mod=$2
	shift 2
	cov=
	[ -z "${COVERAGE:-}" ] || cov="-coverage 1"
	(cd spec && "$java" -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers "${TLC_WORKERS:-4}" $cov \
		-deadlock -noGenerateSpecTE -metadir "${META:-$work/states}" -config "$c" "$@" "$mod")
}

# run CFG MODULE [args]: TLC must pass.
run() {
	echo "== $1"
	st=0
	tlc "$@" >"$work/check.out" 2>&1 || st=$?
	grep -E '^[0-9]+ states generated|depth of|Finished in|Error|violated' "$work/check.out" || true
	[ -z "${COVERAGE:-}" ] || grep -E '^<[A-Za-z]+.*>: 0:0$' "$work/check.out" | sed 's/^/never taken: /' || true
	if [ $st != 0 ]; then
		tail -60 "$work/check.out"
		exit $st
	fi
}

# expect CFG MODULE NAME SED...: TLC on CFG edited by the sed expressions
# must report an invariant or property violation.
fail=0
expect() {
	c=$1 mod=$2 name=$3
	shift 3
	sed "$@" "spec/$c" >"$work/Mutant.cfg"
	if tlc "$work/Mutant.cfg" "$mod" >"$work/m.out" 2>&1; then
		echo "$name: not caught"
		fail=1
	elif grep -qE '^Error: (Invariant .* is violated|Temporal propert.* was violated)' "$work/m.out"; then
		echo "$name: caught: $(grep -m1 -E '^Error: (Invariant|Temporal)' "$work/m.out" | sed 's/^Error: //') ($(grep -c '^State [0-9]*:' "$work/m.out") states deep, $(grep -E '^[0-9]+ states generated' "$work/m.out" | cut -d' ' -f1) states)"
	else
		echo "$name: TLC failed:"
		tail -20 "$work/m.out"
		fail=1
	fi
}

# trace FILE: TLC on one recorded trace (spec/Trace.tla); 0 when accepted.
trace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Sess <- TraceSess
    Win <- TraceWin
    Chunk = 0
    MaxBytes = 2000000000
    Credit = TRUE
    InProc = {}
    CanBreak = TRUE
    Fair = ""
    Aborts = TRUE
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TraceNotDone
    NoProtocolError
    CreditBounds
    BufferBound
    Conservation
    NoHOL
    TypedEnd
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" Trace.tla >"$1.out" 2>&1 || true
	grep -q '^Error: Invariant TraceNotDone is violated' "$1.out"
}

case ${1:-} in
check)
	run AgentLink.cfg MCAgentLink.tla
	run Handoff.cfg Handoff.tla
	;;
liveness)
	run AgentLinkLive.cfg MCAgentLink.tla
	run AgentLinkBreak.cfg MCAgentLink.tla
	run HandoffLive.cfg Handoff.tla
	;;
mutants)
	m='s/Mutant = "none"/Mutant = "'
	# A bug put back per invariant.
	expect AgentLink.cfg MCAgentLink.tla fin-overtakes -e "$m"'fin-overtakes"/'
	expect AgentLink.cfg MCAgentLink.tla grant-early -e "$m"'grant-early"/'
	expect AgentLink.cfg MCAgentLink.tla no-credit -e 's/Credit = TRUE/Credit = FALSE/'
	expect AgentLink.cfg MCAgentLink.tla lose-as-eof -e "$m"'lose-as-eof"/'
	expect Handoff.cfg Handoff.tla attach-ended -e "$m"'attach-ended"/'
	expect Handoff.cfg Handoff.tla keep-conns -e "$m"'keep-conns"/'
	expect HandoffLive.cfg Handoff.tla wait-forever -e "$m"'wait-forever"/'
	# The code's races (spec/README.md "Findings").
	expect AgentLink.cfg MCAgentLink.tla race:inproc-closewrite -e 's/InProc = {}/InProc = {1}/'
	expect AgentLink.cfg MCAgentLink.tla race:clean-tail-lost -e 's/DownEndsAll/DownEndsAll NoSilentLoss/'
	exit $fail
	;;
simulate)
	# Random walks on two sessions, window 2, 3 bytes each way (weekly).
	sed -e 's/Sess = {1}/Sess = {1, 2}/' spec/AgentLink.cfg >"$work/Sim.cfg"
	echo "== AgentLink.cfg, two sessions, -simulate num=${SIM_NUM:-100000} -depth ${SIM_DEPTH:-80}"
	st=0
	tlc "$work/Sim.cfg" MCAgentLink.tla -simulate num="${SIM_NUM:-100000}" -depth "${SIM_DEPTH:-80}" >"$work/sim.out" 2>&1 || st=$?
	grep -E 'states generated|traces generated|Finished in|Error|violated' "$work/sim.out" || true
	[ $st = 0 ] || { tail -60 "$work/sim.out"; exit $st; }
	;;
traces)
	tests=${TRACE_TESTS:-'^(TestTCPTunnelConcurrentSessionsIntact|TestTCPTunnelHalfClose|TestTCPTunnelCloseAfterDataDeliversEverything|TestTCPTunnelStreamLossCutsSessions|TestTCPTunnelStuckTargetDoesNotStallOtherSessions)$'}
	mkdir -p "$work/traces"
	RELAYTRACE=$work/traces go test -tags relaytrace -count=1 -run "$tests" ./core/session/ >"$work/test.out" 2>&1 || {
		cat "$work/test.out"
		exit 1
	}
	# The checker must reject a trace that is no behaviour.
	cp spec/testdata/reject-data-after-fin.ndjson "$work/reject.ndjson"
	if trace "$work/reject.ndjson"; then
		echo "spec/testdata/reject-data-after-fin.ndjson: accepted, want rejected"
		exit 1
	fi
	n=0 bad=0
	for t in "$work"/traces/*.ndjson; do
		n=$((n + 1))
		if TLC_WORKERS=1 trace "$t"; then
			echo "trace $(basename "$t"): $(wc -l <"$t" | tr -d ' ') lines: accepted"
		else
			bad=$((bad + 1))
			echo "trace $(basename "$t"): $(wc -l <"$t" | tr -d ' ') lines: REJECTED"
			grep -E 'Error' "$t.out" | head -5
			[ -z "${TRACE_KEEP:-}" ] || cp "$t" "$TRACE_KEEP/"
		fi
	done
	echo "$n traces, $bad rejected"
	[ "$n" -gt 0 ] && [ "$bad" = 0 ]
	;;
trace)
	trace "$2" && echo accepted || { echo rejected; exit 1; }
	;;
*)
	echo "usage: spec.sh check|liveness|mutants|simulate|traces|trace FILE" >&2
	exit 2
	;;
esac
