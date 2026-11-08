#!/bin/sh
# Copyright 2026 nfltr
# SPDX-License-Identifier: Apache-2.0

# spec.sh actions|check|nightly|liveness|mutants|simulate|traces|walks|trace F: the TLA+ models of
# spec/ (spec/README.md).
#   actions   every model step in spec/README.md's action tables (and every
#             row a step), every event a trace module takes recorded by Go.
#   check     actions, then TLC on spec/AgentLink.cfg (Refines left out),
#             AgentLinkIface.cfg (Refines on fewer bytes), spec/Handoff.cfg,
#             spec/Move.cfg, spec/Presence.cfg, spec/Directory.cfg and
#             DirectoryHeal.cfg, spec/Failover.cfg and FailoverShare.cfg: invariants; spec/AgentLinkWrite.cfg:
#             WriteReturns (CI).
#   nightly   TLC on spec/AgentLink.cfg with Refines and AgentLinkLarge.cfg:
#             AgentLink's invariants at larger sizes; spec/PresenceLarge.cfg:
#             Presence's with a crash (nightly, weekly).
#   mutants   each Mutant must be caught, and each documented race found
#             (spec/README.md "Findings") (CI).
#   liveness  TLC on spec/AgentLinkLive.cfg, AgentLinkBreak.cfg,
#             HandoffLive.cfg, MoveLive.cfg, PresenceLive.cfg and
#             FailoverLive.cfg (weekly).
#   simulate  random walks on two sessions with larger sizes (weekly).
#   traces    the core/session tests in TRACE_TESTS run with -tags
#             relaytrace; each stream's trace checked against spec/Trace.tla,
#             each move's (move-*.ndjson) against spec/TraceMove.tla, each
#             agent's agent-link handoff (handoff-*.ndjson) against
#             spec/TraceHandoff.tla, the agent's side of it
#             (hagent-*.ndjson, relay client/agent's recorder, also from
#             relay client/agent's own tests in CLIENT_TESTS) against
#             spec/TraceHandoffAgent.tla, each deployment's presence
#             (presence-*.ndjson) against spec/TracePresence.tla, each
#             agent-to-agent call's credit (callcredit-*.ndjson, relay
#             client/rpcclient's recorder, TestInternalAgentRouting_*)
#             against relay's spec/TraceCallCredit.tla (relayspec), each
#             call's lifecycle (calllifecycle-*.ndjson, the same recorder)
#             against relay's spec/TraceCallLifecycle.tla; with
#             COVERAGE=1 the steps no trace took (weekly).
#   walks     random walks of spec/Directory.cfg into
#             spec/testdata/directory-walks.ndjson (core/directory
#             TestDirectoryModelWalks replays them against the store).
#   trace F   one recorded trace (its kind by its name).
# TLC_WORKERS (default 4) caps TLC's threads; COVERAGE=1 adds -coverage 1;
# MUTANTS=<regex> narrows mutants to those whose "CFG NAME" it matches.
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
	(cd spec && "$java" -XX:+UseParallelGC -DTLA-Library="$linkspec" -cp "$jar" tlc2.TLC -workers "${TLC_WORKERS:-4}" $cov \
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
	# MUTANTS=<regex> runs only the mutants whose "CFG NAME" it matches.
	[ -z "${MUTANTS:-}" ] || echo "$c $name" | grep -qE "$MUTANTS" || return 0
	# A mutant's own invariant must catch it: Refines only for interface:*.
	case $name in
	interface:*) sed "$@" "spec/$c" >"$work/Mutant.cfg" ;;
	*) sed "$@" "spec/$c" | sed '/^PROPERTY Refines/d' >"$work/Mutant.cfg" ;;
	esac
	if tlc "$work/Mutant.cfg" "$mod" >"$work/m.out" 2>&1; then
		echo "$name: not caught"
		fail=1
	elif grep -qE '^Error: (Invariant .* is violated|Temporal propert.* (was|were) violated|Action property .* is violated)' "$work/m.out"; then
		echo "$name: caught: $(grep -m1 -E '^Error: (Invariant|Temporal|Action)' "$work/m.out" | sed 's/^Error: //') ($(grep -c '^State [0-9]*:' "$work/m.out") states deep, $(grep -E '^[0-9]+ states generated' "$work/m.out" | cut -d' ' -f1) states)"
	else
		echo "$name: TLC failed:"
		tail -20 "$work/m.out"
		fail=1
	fi
}

# trace FILE: TLC on one recorded trace (spec/Trace.tla); 0 when accepted.
trace() {
	cfg=$work/$(basename "$1").cfg
	endack=FALSE # the stream acked half closes (Up's "a", dataplane.EndAckHeader)
	! head -1 "$1" | grep -q '"a":true' || endack=TRUE
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
    EndAck = $endack
    OutCap = 1000000
    RelayAbort = TRUE
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANTS
    NoProtocolError
    CreditBounds
    BufferBound
    Conservation
    NoHOL
    TypedEnd
    TraceNotDone
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" Trace.tla >"$1.out" 2>&1 || true
	grep -q '^Error: Invariant TraceNotDone is violated' "$1.out"
}

# LinkSession.tla is link's (spec/README.md "The link session interface"):
# TLC reads it from the required link module, so a link spec change never
# leaves a stale copy here. go list -m names no Dir for a module not yet
# downloaded (a fresh CI machine), so download it first.
go mod download github.com/nfltr/link github.com/nfltr/relay/client
linkspec=$(go list -m -f '{{.Dir}}' github.com/nfltr/link)/spec
[ -f "$linkspec/LinkSession.tla" ] || { echo "spec.sh: no LinkSession.tla in $linkspec (link >= v0.12.2 required)" >&2; exit 1; }

# relayspec: a relay tree holding what checks a CallCredit trace (relay
# scripts/spec.sh cctraces, its spec/CallCredit.tla and
# TraceCallCredit.tla), at the relay/client version go.mod requires: those
# files from github.com/nfltr/relay (public) at that commit or tag, so the
# check is the one that version's recorder was written for. RELAY_DIR=<a
# relay checkout> uses that instead (co-development under a go.work).
relayspec() {
	if [ -n "${RELAY_DIR:-}" ]; then
		echo "$RELAY_DIR"
		return
	fi
	v=$(go list -m -f '{{.Version}}' github.com/nfltr/relay/client)
	case $v in
	*-0.*-*) rev=${v##*-} ;; # a pseudo-version: its commit
	*) rev=client/$v ;;
	esac
	for f in scripts/spec.sh scripts/fetch-tla.sh spec/CallCredit.tla spec/TraceCallCredit.tla spec/CallLifecycle.tla spec/TraceCallLifecycle.tla; do
		mkdir -p "$work/relay/$(dirname "$f")"
		curl -fsSL --retry 3 -o "$work/relay/$f" "https://raw.githubusercontent.com/nfltr/relay/$rev/$f"
	done
	chmod +x "$work/relay/scripts/spec.sh" "$work/relay/scripts/fetch-tla.sh"
	echo "$work/relay"
}

# htrace FILE: TLC on one recorded Handoff trace (spec/TraceHandoff.tla); 0 when accepted.
htrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Sess <- TraceSess
    MaxGen <- TraceMaxGen
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANTS
    PlacedOnLive
    NoOrphanLink
    CarriesExact
    SilenceOnDeadPath
    TraceNotDone
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" TraceHandoff.tla >"$1.out" 2>&1 || true
	grep -q '^Error: Invariant TraceNotDone is violated' "$1.out"
}

# atrace FILE: TLC on one agent-side Handoff trace (spec/TraceHandoffAgent.tla); 0 when accepted.
atrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Sess <- TraceSess
    MaxGen <- TraceMaxGen
    Mutant = "${AMUTANT:-none}"
INIT TraceInit
NEXT TraceNext
INVARIANTS
    PlacedOnLive
    NoOrphanLink
    CarriesExact
    SilenceOnDeadPath
    LinkOpensAnswered
    ParkOffered
    TraceNotDone
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" TraceHandoffAgent.tla >"$1.out" 2>&1 || true
	grep -q '^Error: Invariant TraceNotDone is violated' "$1.out"
}

# trace_move FILE: TLC on one recorded move (spec/TraceMove.tla); 0 when accepted.
trace_move() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Req <- TraceReq
    TSess <- TraceTSess
    Lease = TRUE
    Drain = TRUE
    FailBOn = FALSE
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANT TraceNotDone
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" TraceMove.tla >"$1.out" 2>&1 || true
	grep -q '^Error: Invariant TraceNotDone is violated' "$1.out"
}

# ptrace FILE: TLC on one recorded presence trace (spec/TracePresence.tla); 0 when accepted.
ptrace() {
	cfg=$work/$(basename "$1").cfg
	cat >"$cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    R <- TraceR
    W <- TraceW
    MaxSeq <- TraceMaxSeq
    MaxUpd <- TraceMaxUpd
    MaxResync <- TraceMaxResync
    MaxFail <- TraceMaxFail
    Crash = FALSE
    InFlight = FALSE
    Mutant = "none"
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TypeOK
    NoSilentStale
    SupersededByNewer
    TraceNotDone
CFG
	META=$work/states-$(basename "$1") tlc "$cfg" TracePresence.tla >"$1.out" 2>&1 || true
	grep -q '^Error: Invariant TraceNotDone is violated' "$1.out"
}

# check_trace FILE: the checker for FILE's kind.
check_trace() {
	case $(basename "$1") in
	presence-* | reject-presence-*) ptrace "$1" ;;
	move-* | reject-move-*) trace_move "$1" ;;
	handoff-* | reject-handoff-*) htrace "$1" ;;
	hagent-* | reject-hagent-*) atrace "$1" ;;
	*) trace "$1" ;;
	esac
}

# cltrace FILE: each agent's call in one recorded CallLifecycle trace
# (calllifecycle-*.ndjson, relay client/rpcclient's recorder) checked
# against relay's spec/TraceCallLifecycle.tla ($rs, relayspec); 0 when all
# are accepted. The config is relay scripts/spec.sh's cltrace (which takes
# no trace files from elsewhere: relay's cltraces mode records its own).
cltrace() {
	: >"$1.out"
	for ac in $(sed -E 's/.*"a":"([^"]+)","c":"([^"]+)".*/\1:\2/' "$1" | sort -u); do
		ag=${ac%%:*} call=${ac#*:} m=none
		grep "\"a\":\"$ag\",\"c\":\"$call\"" "$1" | grep -q '"cont":true' || m=old-caller
		cat >"$1.$ag.$call.cfg" <<CFG
CONSTANTS
    TraceFile = "$1"
    Agent = "$ag"
    Call = "$call"
    Txns <- TraceTxns
    N <- TraceLen
    Retries <- TraceRetries
    Mutant = "$m"
INIT TraceInit
NEXT TraceNext
INVARIANTS
    TypeOK
    AtMostOneCall
    NoEndedEntry
    TraceNotDone
CFG
		META=$work/states-$(basename "$1").$ag.$call tlc "$1.$ag.$call.cfg" "$rs/spec/TraceCallLifecycle.tla" >>"$1.out" 2>&1 || true
		grep -q '^Error: Invariant TraceNotDone is violated' "$1.out" || return 1
		: >"$1.out"
	done
}

# checktraces DIR CHECKER: check each DIR/*.ndjson; fails on a rejected one or none.
checktraces() {
	n=0 bad=0
	for t in "$1"/*.ndjson; do
		[ -e "$t" ] || continue
		n=$((n + 1))
		if TLC_WORKERS=1 "$2" "$t"; then
			echo "trace $(basename "$t"): $(wc -l <"$t" | tr -d ' ') lines: accepted"
		else
			bad=$((bad + 1))
			echo "trace $(basename "$t"): $(wc -l <"$t" | tr -d ' ') lines: REJECTED"
			grep -E 'Error|unknown event' "$t.out" | head -5
			[ -z "${TRACE_KEEP:-}" ] || cp "$t" "$TRACE_KEEP/"
		fi
		[ -z "${COVERAGE:-}" ] || grep -E '^<[A-Za-z]+ line .*>: [0-9]+:[0-9]+$' "$t.out" >>"$1/coverage"
	done
	echo "$n traces, $bad rejected"
	# COVERAGE=1: the trace module's actions no trace takes (test gaps).
	[ ! -s "$1/coverage" ] || sed -E 's/^<([A-Za-z]+) line.*:([0-9]+)$/\1 \2/' "$1/coverage" |
		awk '{n[$1] += $2} END {for (a in n) if (n[a] == 0) print "  never taken: " a}' | sort
	[ "$n" -gt 0 ] && [ "$bad" = 0 ]
}

# actions_check: each model's steps against its README action table (both
# ways) and each trace module's event names against the recorder's Go.
actions_check() {
	ok=0
	for m in AgentLink Handoff Move Presence Directory KeyAccount AccountLimit Failover; do
		# A step: a definition whose body primes or keeps a variable (not a temporal formula).
		acts=$(awk '
			function flush() { if (name != "" && act && !tmp) print name; name = "" }
			/^[A-Z][A-Za-z0-9_]*(\(.*\))? ==/ { flush(); name = $1; sub(/\(.*/, "", name); act = 0; tmp = 0; next }
			/^(=+$|-+ MODULE)/ { flush() }
			name != "" && /UNCHANGED|'"'"' *(=|\\in)/ { act = 1 }
			name != "" && /\[\]\[|~>/ { tmp = 1 }
			END { flush() }' "spec/$m.tla" | sort -u)
		tab=$(awk -v m="## $m.tla" '
			index($0, m) == 1 { in_s = 1; next }
			/^## / { in_s = 0 }
			in_s && /^\| action \| Go \|/ { in_t = 1; next }
			in_s && in_t && !/^\|/ { in_t = 0 }
			in_s && in_t && /^\| `/ { split($0, c, "|"); s = c[2]
				while (match(s, /`[A-Z][A-Za-z0-9_]*/)) { print substr(s, RSTART + 1, RLENGTH - 1); s = substr(s, RSTART + RLENGTH) } }' spec/README.md | sort -u)
		for a in $acts; do
			echo "$tab" | grep -qx "$a" || { echo "$m.tla: step $a is not in spec/README.md's action table"; ok=1; }
		done
		for a in $tab; do
			grep -qE "^$a(\(.*\))? ==" "spec/$m.tla" || { echo "spec/README.md: $m's action $a is not defined in $m.tla"; ok=1; }
		done
	done
	go=$(cat core/session/*.go | grep -v '^//' | grep -ohE '"[A-Za-z0-9]+"' | sort -u)
	for t in Trace TraceHandoff TraceMove TraceMoveLog TracePresence; do
		for e in $(grep -E '\bev\b' "spec/$t.tla" | grep -ohE '"[A-Z][A-Za-z0-9]*"' | sort -u); do
			echo "$go" | grep -qx "$e" || { echo "spec/$t.tla: event $e is recorded by no Go in core/session"; ok=1; }
		done
	done
	# The agent's side (TraceHandoffAgent.tla) is recorded in relay client/agent.
	ago=$(grep -v '^//' "$(go list -m -f '{{.Dir}}' github.com/nfltr/relay/client)/agent/trace_handoff_on.go" | grep -ohE '"[A-Za-z0-9]+"' | sort -u)
	for e in $(grep -E '\bev\b' spec/TraceHandoffAgent.tla | grep -ohE '"[A-Z][A-Za-z0-9]*"' | sort -u); do
		echo "$ago" | grep -qx "$e" || { echo "spec/TraceHandoffAgent.tla: event $e is recorded by no Go in relay client/agent"; ok=1; }
	done
	return $ok
}

case ${1:-} in
actions)
	actions_check && echo "every step mapped, every traced event recorded"
	;;
check)
	actions_check || exit 1
	# AgentLink.cfg's Refines is the nightly's (it triples the run); in CI
	# AgentLinkIface.cfg checks it on fewer bytes.
	sed '/^PROPERTY Refines/d' spec/AgentLink.cfg >"$work/AgentLink.cfg"
	run "$work/AgentLink.cfg" MCAgentLink.tla
	run AgentLinkIface.cfg MCAgentLink.tla
	run AgentLinkAbort.cfg MCAgentLink.tla
	run AgentLinkWrite.cfg MCAgentLink.tla
	run Handoff.cfg Handoff.tla
	run Move.cfg Move.tla
	run Presence.cfg Presence.tla
	run Directory.cfg Directory.tla
	run KeyAccount.cfg KeyAccount.tla
	run AccountLimit.cfg AccountLimit.tla
	run Failover.cfg Failover.tla
	run FailoverShare.cfg Failover.tla
	# That race heals at the latest session's next renewal (finding 38).
	run DirectoryHeal.cfg Directory.tla
	;;
nightly)
	run AgentLink.cfg MCAgentLink.tla
	run AgentLinkLarge.cfg MCAgentLink.tla
	run PresenceLarge.cfg Presence.tla
	;;
liveness)
	run AgentLinkLive.cfg MCAgentLink.tla
	run AgentLinkBreak.cfg MCAgentLink.tla
	run HandoffLive.cfg Handoff.tla
	run MoveLive.cfg Move.tla
	run PresenceLive.cfg Presence.tla
	run AccountLimitLive.cfg AccountLimit.tla
	run FailoverLive.cfg Failover.tla
	;;
mutants)
	m='s/Mutant = "none"/Mutant = "'
	# A bug put back per invariant.
	# KeyAccount.tla: the key file's accounts (KEYACCT).
	for b in join-outside dup-name revoke-reassigns reload-account owner-own-id resolve-global; do
		expect KeyAccount.cfg KeyAccount.tla "$b" -e "$m$b\"/"
	done
	for w in OutlivesFirst NameReused NameInTwoAccounts; do
		expect KeyAccount.cfg KeyAccount.tla "witness:$w" -e "s/^    ResolveOwnAccount$/    Witness$w/"
	done
	# AccountLimit.tla: an account's own connection limits (CONNLIMIT).
	for b in count-by-ip-only cache-stale-plan release-twice; do
		expect AccountLimit.cfg AccountLimit.tla "$b" -e "$m$b\"/"
	done
	for w in Refused Lowered Shared; do
		expect AccountLimit.cfg AccountLimit.tla "witness:$w" -e "s/^    CountsMatch$/    Witness$w/"
	done
	# Failover.tla: several relays (RELAYFAILOVER, design): each failure the
	# design prevents put back; witnesses.
	for b in parallel-connect claim-before-dedupe renew-overwrites share-everywhere; do
		expect FailoverShare.cfg Failover.tla "failover:$b" -e "$m$b\"/"
	done
	for b in retry-ambiguous first-only; do
		expect Failover.cfg Failover.tla "failover:$b" -e "$m$b\"/"
	done
	expect FailoverLive.cfg Failover.tla failover:stale-alt -e "$m"'stale-alt"/' -e 's/Sess = {"s1"}/Sess = {}/' -e '/^    Settles$/d'
	expect FailoverShare.cfg Failover.tla witness:failover-refused -e 's/^    OneLinkPerCluster$/    WitnessRefused/'
	expect FailoverShare.cfg Failover.tla witness:failover-handover -e 's/^    RecordFollowsAgent$/    WitnessHandover/'
	expect FailoverShare.cfg Failover.tla witness:failover-return -e 's/^    RecordFollowsAgent$/    WitnessReturn/'
	expect FailoverShare.cfg Failover.tla witness:failover-revoked -e 's/^    RevokedNotServed$/    WitnessRevoked/'
	for w in Walk Ambiguous Stale NotFound; do
		expect Failover.cfg Failover.tla "witness:failover-$w" -e "s/^    RunOnce$/    Witness$w/"
	done
	expect AgentLink.cfg MCAgentLink.tla fin-overtakes -e "$m"'fin-overtakes"/'
	expect AgentLink.cfg MCAgentLink.tla grant-early -e "$m"'grant-early"/'
	expect AgentLink.cfg MCAgentLink.tla no-credit -e 's/Credit = TRUE/Credit = FALSE/' -e 's/MaxBytes = 2/MaxBytes = 3/' # a byte past the window
	expect AgentLink.cfg MCAgentLink.tla lose-as-eof -e "$m"'lose-as-eof"/'
	expect AgentLink.cfg MCAgentLink.tla lose-unacked-clean -e "$m"'lose-unacked-clean"/'
	expect AgentLink.cfg MCAgentLink.tla no-hold -e "$m"'no-hold"/'
	expect AgentLink.cfg MCAgentLink.tla abort-as-eof -e "$m"'abort-as-eof"/'
	expect AgentLinkAbort.cfg MCAgentLink.tla relay-abort-as-eof -e "$m"'relay-abort-as-eof"/'
	expect AgentLinkAbort.cfg MCAgentLink.tla interface:relay-abort-as-eof -e "$m"'relay-abort-as-eof"/' -e '/^INVARIANTS/,$d'
	expect AgentLinkAbort.cfg MCAgentLink.tla witness:relay-abort -e 's/    AbortTyped/    WitnessRelayAbort/'
	# Witnesses: the states the invariants above guard are reached.
	# The same bugs break link's session interface alone (Refines, every
	# invariant of AgentLink left out).
	for b in fin-overtakes grant-early lose-as-eof abort-as-eof inproc-closewrite; do
		expect AgentLink.cfg MCAgentLink.tla "interface:$b" -e "$m$b\"/" -e '/^INVARIANTS/,$d'
	done
	expect AgentLink.cfg MCAgentLink.tla witness:abort -e 's/    AbortTyped/    WitnessAbort/'
	expect AgentLink.cfg MCAgentLink.tla witness:held-eof -e 's/    NoSilentLoss/    WitnessHeldEof/'
	expect Handoff.cfg Handoff.tla attach-ended -e "$m"'attach-ended"/'
	expect Handoff.cfg Handoff.tla keep-conns -e "$m"'keep-conns"/'
	expect HandoffLive.cfg Handoff.tla wait-forever -e "$m"'wait-forever"/'
	# Handoff.tla: carriesSessions (HO-3), QUIC silence (HO-2), a parked link's
	# answers (finding 42); witnesses.
	for b in count-per-primary carries-newest quic-silent-ends park-primary-ctx end-async; do
		expect Handoff.cfg Handoff.tla "$b" -e "$m$b\"/"
	done
	expect Handoff.cfg Handoff.tla witness:link-open -e 's/^    PlacedOnLive$/    WitnessLinkOpen/'
	expect Handoff.cfg Handoff.tla witness:fallback -e 's/^    PlacedOnLive$/    WitnessFallback/'
	expect Handoff.cfg Handoff.tla witness:resumed -e 's/^    NoOrphanLink$/    WitnessResumed/'
	expect Handoff.cfg Handoff.tla witness:carry-old -e 's/^    CarriesExact$/    WitnessCarryOld/'
	expect Handoff.cfg Handoff.tla witness:silent -e 's/^    SilenceOnDeadPath$/    WitnessSilent/'
	expect Handoff.cfg Handoff.tla witness:parked-open -e 's/^    LinkOpensAnswered$/    WitnessParkedOpen/'
	expect Handoff.cfg Handoff.tla witness:end-unparked -e 's/^    ParkOffered$/    WitnessEndUnparked/'
	# A trace whose last step breaks an invariant is rejected: every trace
	# cfg lists TraceNotDone last, since TLC reports the first violated
	# invariant in listed order (spec/README.md "Trace validation"). The
	# fixture ends at the open park-primary-ctx refuses on the parked link.
	t=$work/hagent-mutant-last.ndjson
	cp spec/testdata/mutant-hagent-park-primary-ctx-last.ndjson "$t"
	if [ -z "${MUTANTS:-}" ] || echo "trace park-primary-ctx-last" | grep -qE "$MUTANTS"; then
		if AMUTANT=park-primary-ctx atrace "$t"; then
			echo "trace:park-primary-ctx-last: accepted, want rejected"
			fail=1
		else
			echo "trace:park-primary-ctx-last: caught: $(grep -m1 '^Error: Invariant' "$t.out" | sed 's/^Error: //')"
		fi
	fi
	# The code's races (spec/README.md "Findings").
	expect AgentLink.cfg MCAgentLink.tla inproc-closewrite -e "$m"'inproc-closewrite"/'
	expect AgentLinkWrite.cfg MCAgentLink.tla inproc-enqueue-blocks -e "$m"'inproc-enqueue-blocks"/'
	# A stream without the end acknowledgement (finding 2: an older agent or relay).
	expect AgentLink.cfg MCAgentLink.tla race:clean-tail-lost -e 's/EndAck = TRUE/EndAck = FALSE/'
	# Move.tla: the move's bugs, and the code's races before the design (findings 6, 7).
	expect Move.cfg Move.tla done-ignores-leases -e "$m"'done-ignores-leases"/'
	expect Move.cfg Move.tla done-overtakes -e "$m"'done-overtakes"/'
	expect Move.cfg Move.tla close-early -e "$m"'close-early"/'
	expect Move.cfg Move.tla race:settle-timer -e 's/Lease = TRUE/Lease = FALSE/'
	expect Move.cfg Move.tla race:supersede-ends -e "$m"'supersede-ends"/'
	# Findings MV-1 (peer forwards), MV-2 (the drain's bound), MV-4 (TCP rotation), 45 (arrival), 46 (onward); witnesses.
	for b in peer-no-lease drain-bound-after-stop tcp-stays-in-rotation no-arrival-wait peer-refuses-moved; do
		expect Move.cfg Move.tla "$b" -e "$m$b\"/"
	done
	expect Move.cfg Move.tla witness:held -e 's/^    NoLoss$/    WitnessHeld/'
	expect Move.cfg Move.tla witness:let-go -e 's/^    NoLoss$/    WitnessLetGo/'
	expect Move.cfg Move.tla witness:remote -e 's/^    RunOnce$/    WitnessRemote/'
	expect Move.cfg Move.tla witness:owner -e 's/^    OneOwner$/    WitnessOwner/'
	expect Move.cfg Move.tla witness:tcp -e 's/^    NoTCPAfterMove$/    WitnessTCP/'
	expect Move.cfg Move.tla witness:b-gone -e 's/^    OneOwner$/    WitnessBGone/'
	expect Move.cfg Move.tla witness:arriving -e 's/^    OwnerServes$/    WitnessArriving/'
	# Presence.tla: the bugs the design prevents (findings 8-13, 24-29, 34; release-by-agent: 2a1cf33).
	for b in unfenced-label-write partial-list partial-my-agents renew-race drop-no-resync renew-resurrects publish-before-write resync-early resync-ignored publish-drop silent-rewrite no-late-down shared-counter release-by-agent; do
		expect Presence.cfg Presence.tla "$b" -e "$m$b\"/"
	done
	expect Presence.cfg Presence.tla witness:stale -e 's/^    NoSilentStale$/    WitnessStale/'
	expect Presence.cfg Presence.tla witness:remote-update -e 's/^    NoSilentStale$/    WitnessRemoteUpdate/'
	expect Presence.cfg Presence.tla witness:superseded -e 's/^    SupersededByNewer$/    WitnessSuperseded/'
	# Finding 14: an update received on a replaced connection.
	expect Presence.cfg Presence.tla update-by-agent -e "$m"'update-by-agent"/'
	expect Presence.cfg Presence.tla witness:update-replaced -e 's/^    OwnUpdate$/    WitnessUpdateReplaced/'
	expect Presence.cfg Presence.tla witness:replaced -e 's/^SPECIFICATION Spec$/SPECIFICATION Spec PROPERTY WitnessReplaced/'
	# The wake is paid only if the feed's steps go on (finding 26); no
	# failed writes, to keep these quick.
	for b in subscriber-stops outbox-stops publish-drop; do
		expect PresenceLive.cfg Presence.tla "live:$b" -e "$m$b\"/" -e 's/MaxFail = 1/MaxFail = 0/' -e 's/Crash = TRUE/Crash = FALSE/'
	done
	# Directory.tla: the session records' bugs (findings 13, 34, 35, 37, R7); witnesses.
	for b in renew-resurrects shared-counter seq-only seq-tie delete-any seqless-by-pod seqless-masks-counter; do
		expect Directory.cfg Directory.tla "dir:$b" -e "$m$b\"/"
	done
	expect Directory.cfg Directory.tla witness:dir-superseded -e 's/^    SupersededByNewer$/    WitnessSuperseded/'
	expect Directory.cfg Directory.tla witness:dir-resurrect -e 's/^    NoResurrect$/    WitnessResurrectSetup/'
	expect Directory.cfg Directory.tla witness:dir-end-keeps -e 's/^    DeleteOwn$/    WitnessEndKeeps/'
	expect Directory.cfg Directory.tla witness:dir-tie -e 's/^    NoPingPong$/    WitnessTie/'
	# PutSession's renew and DeleteSession as a GET, then a SET or DEL: the
	# race the design closes with an atomic step (finding 38).
	expect DirectoryHeal.cfg Directory.tla race:renew-get-set -e 's/^PROPERTY Heals$/INVARIANT DeleteOwn NoResurrect/'
	expect DirectoryHeal.cfg Directory.tla heal:renew-yields -e "$m"'renew-yields"/'
	expect Presence.cfg Presence.tla no-crash-resync -e "$m"'no-crash-resync"/' -e 's/Crash = FALSE/Crash = TRUE/'
	exit $fail
	;;
simulate)
	# Random walks on two sessions, window 2, 3 bytes each way (weekly).
	sed -e 's/Sess = {1}/Sess = {1, 2}/' spec/AgentLinkLarge.cfg >"$work/Sim.cfg"
	echo "== AgentLinkLarge.cfg, two sessions, -simulate num=${SIM_NUM:-100000} -depth ${SIM_DEPTH:-80}"
	st=0
	tlc "$work/Sim.cfg" MCAgentLink.tla -simulate num="${SIM_NUM:-100000}" -depth "${SIM_DEPTH:-80}" >"$work/sim.out" 2>&1 || st=$?
	grep -E 'states generated|traces generated|Finished in|Error|violated' "$work/sim.out" || true
	[ $st = 0 ] || { tail -60 "$work/sim.out"; exit $st; }
	;;
traces)
	tests=${TRACE_TESTS:-'^(TestTCPTunnelConcurrentSessionsIntact|TestTCPTunnelHalfClose|TestTCPTunnelCloseAfterDataDeliversEverything|TestTCPTunnelStreamLossCutsSessions|TestTCPTunnelStuckTargetDoesNotStallOtherSessions|TestAgentMoves.*|TestAgentThatDoesNotMoveIsLetGo|TestDrainBoundEndsOnceRoutingStopped|TestMoveAskedAtDrainStart|TestRebalance.*|TestConnectWhileTheMovedSessionStillEndsIsUnavailable|TestConnectTCPOfAMovingAgentStaysWhileDraining|TestMovedAgentsTCPStreamTakesNoNewSessions|TestTCPTunnelClientResetResetsTargetEndToEnd|TestTraceWriteAfterFinishIsLeftOut)$'}
	htests=${HANDOFF_TESTS:-'^(TestAgentLinkTCPSessions|TestAgentLinkCutResetsVisitor|TestAgentLinkSilentEndsPrimary|TestAgentLinkUpForThePrimarysLife|TestAgentLinkDialUnanswered|TestAgentLinkEndsWithItsPort|TestHeartbeatThresholdRestartsWhenTheLinkGoesDown|TestAgentLinkResumedByTheNextPrimary|TestAgentLinkCarriesSessions)$'}
	ptests=${PRESENCE_TESTS:-'^(TestPresenceAcrossReplicas|TestPresenceAcrossReplicasFailedWrites)$'}
	cctests=${CALLCREDIT_TESTS:-'^TestInternalAgentRouting_.*$'}
	# The agent's own Handoff tests (relay client/agent, fake relays): the
	# steps the real relay pair leaves to chance (an open on the parked link,
	# a refused adoption, a dial that gives up).
	ctests=${CLIENT_TESTS:-'^(TestLazyAgentLink|TestLazyAgentLinkGivesUp|TestAgentLinkUpOnlyWhileBound|TestTCPTunnelReturnsAfterTheLinkIsParked|TestTCPTunnelAdoptRefusedWhileDialling)$'}
	mkdir -p "$work/all" "$work/traces" "$work/mtraces" "$work/htraces" "$work/atraces" "$work/ptraces" "$work/cctraces" "$work/cltraces" "$work/ftraces"
	RELAYTRACE=$work/all go test -tags relaytrace -count=1 -run "$tests|$htests|$ptests|$cctests" ./core/session/ >"$work/test.out" 2>&1 || {
		cat "$work/test.out"
		exit 1
	}
	mkdir -p "$work/client"
	RELAYTRACE=$work/client go test -tags relaytrace -count=1 -run "$ctests" github.com/nfltr/relay/client/agent >>"$work/test.out" 2>&1 || {
		cat "$work/test.out"
		exit 1
	}
	for t in "$work"/client/*.ndjson; do
		[ ! -e "$t" ] || mv "$t" "$work/atraces/hagent-client-$(basename "$t" | sed 's/^hagent-//')"
	done
	for t in "$work"/all/*.ndjson; do
		case $(basename "$t") in
		move-*) mv "$t" "$work/mtraces/" ;;
		handoff-*) mv "$t" "$work/htraces/" ;;
		hagent-*) mv "$t" "$work/atraces/" ;;
		presence-*) mv "$t" "$work/ptraces/" ;;
		callcredit-*) mv "$t" "$work/cctraces/" ;;
		calllifecycle-*) mv "$t" "$work/cltraces/" ;;
		failover-*) mv "$t" "$work/ftraces/" ;;
		*) mv "$t" "$work/traces/" ;;
		esac
	done
	# The checker must reject a trace that is no behaviour.
	for r in spec/testdata/reject-*.ndjson; do
		cp "$r" "$work/$(basename "$r")"
		if check_trace "$work/$(basename "$r")"; then
			echo "$r: accepted, want rejected"
			exit 1
		fi
		echo "$r: rejected"
	done
	st=0
	echo "== AgentLink (spec/Trace.tla)"
	checktraces "$work/traces" trace || st=1
	echo "== Move (spec/TraceMove.tla)"
	checktraces "$work/mtraces" trace_move || st=1
	if [ -n "${COVERAGE:-}" ]; then
		# The replica-side steps of Move.tla no move trace took: test gaps.
		cat "$work"/mtraces/move-*.ndjson 2>/dev/null |
			sed -E 's/^\{"ev":"([A-Za-z]+)","r":"[^"]*","m":"([^"]*)","ok":(true|false)\}$/\1 \2 \3/' |
			awk '{ if (NF == 3 && $2 != "CONNECTED" && $2 != "EOF") $2 = "TXN"; if (NF == 2) { $3 = $2; $2 = "" } print $1 " " $2 " " $3 }' |
			sort -u >"$work/taken"
		for a in 'Pick  true' 'Pick  false' 'Publish  true' 'Publish  false' 'Release  false' 'Forward  false' 'Ask  false' \
			'GiveUp  false' 'StopRouting  false' 'Refresh  false' 'Done  false' 'StartDrain  false' 'DrainBound  false' \
			'RecvA CONNECTED false' 'RecvA TXN false' 'RecvA EOF false' 'TCPSupersede  true' 'PlaceTCP  true' 'ArriveA  true' 'ArriveA  false'; do
			grep -qxF "$a" "$work/taken" || echo "move step never taken: $a"
		done
		echo "(PlaceTCP false is not logged: a session placed elsewhere is not A's step)"
	fi
	echo "== Handoff (spec/TraceHandoff.tla)"
	checktraces "$work/htraces" htrace || st=1
	echo "== Handoff, the agent's side (spec/TraceHandoffAgent.tla)"
	checktraces "$work/atraces" atrace || st=1
	echo "== Presence (spec/TracePresence.tla)"
	checktraces "$work/ptraces" ptrace || st=1
	echo "== CallCredit, agent-to-agent calls (relay spec/TraceCallCredit.tla)"
	rs=$(relayspec)
	ls "$work"/cctraces/*.ndjson >/dev/null 2>&1 || { echo "no CallCredit trace"; st=1; }
	"$rs/scripts/spec.sh" cctraces "$work"/cctraces/*.ndjson || st=1
	echo "== CallLifecycle, agent-to-agent calls (relay spec/TraceCallLifecycle.tla)"
	checktraces "$work/cltraces" cltrace || st=1
	# The accept/refuse recorder (trace_failover_on.go) has no checker yet:
	# spec/TraceFailover.tla is lane RFTRACE's. Its traces are not AgentLink's.
	echo "== Failover: $(ls "$work"/ftraces | wc -l | tr -d ' ') traces recorded, not checked (no spec/TraceFailover.tla yet)"
	if [ -n "${COVERAGE:-}" ]; then
		# Presence.tla's steps no presence trace took: test gaps.
		cat "$work"/ptraces/presence-*.ndjson 2>/dev/null |
			sed -E 's/^\{"ev":"([A-Za-z]+)","r":"[^"]*","ok":(true|false).*$/\1 \2/' | sort -u >"$work/ptaken"
		for a in Connect Publish Deliver Renew Superseded 'Update true' 'Update false' 'End true' 'End false' LateDown Resync Wake Cursor Snap; do
			grep -q "^$a" "$work/ptaken" || echo "presence step never taken: $a"
		done
	fi
	exit $st
	;;
walks)
	# Random walks of spec/Directory.cfg (-simulate) into
	# spec/testdata/directory-walks.ndjson, one state a line, each walk from
	# its Init line; core/directory TestDirectoryModelWalks replays them
	# against the real store. Fixed seed: the same walks until the spec changes.
	mkdir -p "$work/walks"
	sed -e "s/WalkDepth = 0/WalkDepth = ${WALK_DEPTH:-12}/" -e "s#WalkDir = \"\"#WalkDir = \"$work/walks\"#" \
		-e 's/^    NoPingPong$/    NoPingPong\n    WalkDump/' spec/Directory.cfg >"$work/Walk.cfg"
	TLC_WORKERS=1 tlc "$work/Walk.cfg" Directory.tla -simulate num="${WALK_NUM:-60}" -depth "${WALK_DEPTH:-12}" -seed "${WALK_SEED:-1}" >"$work/walk.out" 2>&1 || {
		tail -40 "$work/walk.out"
		exit 1
	}
	# Drop TLC's _action and the model's w, lost and pr (empty: TwoStep is
	# off): the replay reads act, rec, ctr and sess.
	for n in $(ls "$work/walks" | sort -n); do cat "$work/walks/$n"; done |
		sed -E -e 's/^\{"_action":\{"name":"[A-Za-z]+","location":\{[^}]*\}(,"context":\{[^}]*\},"parameters":\[[^]]*\])?\},/{/' \
			-e 's/,"w":\{[^}]*\}//' -e 's/,"lost":(true|false)//' -e 's/,"pr":\[\]//' -e 's/"pr":\[\],//' >spec/testdata/directory-walks.ndjson
	echo "$(ls "$work/walks" | wc -l | tr -d ' ') walks, $(wc -l <spec/testdata/directory-walks.ndjson | tr -d ' ') states: spec/testdata/directory-walks.ndjson"
	;;
trace)
	check_trace "$2" && echo accepted || { grep -E 'Error|unknown event' "$2.out" | head -5; echo rejected; exit 1; }
	;;
*)
	echo "usage: spec.sh actions|check|nightly|liveness|mutants|simulate|traces|walks|trace FILE" >&2
	exit 2
	;;
esac
