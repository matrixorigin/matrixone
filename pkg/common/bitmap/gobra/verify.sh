#!/usr/bin/env bash
# Formally verify the bitmap core (bitmap.go) with Gobra.
# See gobra/README.md for what is proven and what is trusted.
# Requires: GOBRA_JAR (path to gobra.jar) and Z3_EXE (path to z3 4.16).
#
# Usage: verify.sh [--one] [extra Gobra arguments]
#
# Viper verifies every member on its own, against the others' contracts, so
# verifying members in separate runs proves the same thing as one run. By
# default each member of bitmap.go gets a Gobra run, and so a Z3 process, of
# its own; VERIFY_JOBS runs go at once. One run peaks at about 2.5 GB (JVM
# heap capped at 2 GB by the default GOBRA_JAVA_OPTS, plus Z3), so the default
# is one job per 3 GB of available memory, at most 4. Without the heap cap a
# JVM grows to a quarter of RAM, and many parallel runs exhaust memory. In a single run Silicon
# reuses each Z3 process across many members, and some of these proofs then
# succeed or fail depending on what that process verified before them.
# "--one" does the single run anyway.
set -euo pipefail
dir=$(cd "$(dirname "$0")/.." && pwd)
: "${GOBRA_JAR:?set GOBRA_JAR to the path of gobra.jar}"
: "${Z3_EXE:?set Z3_EXE to the path of z3}"
: "${GOBRA_JAVA_OPTS:=-Xss128m -Xmx2g}"
if [ -z "${VERIFY_JOBS:-}" ]; then
	avail_gb=$(awk '/^MemAvailable:/ { print int($2 / 1048576) }' /proc/meminfo 2>/dev/null || echo 0)
	VERIFY_JOBS=$((avail_gb / 3))
	[ "$VERIFY_JOBS" -gt 4 ] && VERIFY_JOBS=4
	[ "$VERIFY_JOBS" -lt 1 ] && VERIFY_JOBS=1
fi
export Z3_EXE GOBRA_JAR GOBRA_JAVA_OPTS
cd "$dir"

if [ "${1:-}" = "--one" ]; then
	shift
	exec java $GOBRA_JAVA_OPTS -jar "$GOBRA_JAR" -I gobra/stubs -i types.go bitmap.go bitmap_select.go bitmap_mem.gobra bitmap_lemmas.gobra "$@"
fi

run() { # run <label> <files...>; prints one result line
	label=$1
	shift
	if out=$(java $GOBRA_JAVA_OPTS -jar "$GOBRA_JAR" -I gobra/stubs --assertTimeout 120000 -i "$@" 2>&1) &&
		grep -q "Gobra found 0 errors" <<<"$out"; then
		echo "ok    $label"
	else
		echo "FAIL  $label"
		grep -E -A2 "Error at" <<<"$out" | sed 's/^/      /'
		return 1
	fi
}
export -f run

# Members start at a top-level "func" line, ghost or not, or at a predicate.
# Pure functions and predicates are verified together: a pure function
# verified alone fails to check the predicate bodies it unfolds (this is
# true of the original proof as well).
pure=$(awk '
	/^\/\/ @ pure$/ { p = NR }
	/^(func |\/\/ @ (pure )?func |\/\/ @ pred )/ {
		if ($0 ~ /pure func|^\/\/ @ pred/ || (p && NR - p <= 3)) print NR
	}' bitmap.go)
# PeekNext calls the pure HasNext and hits the same problem on its own.
pure="$pure $(grep -n -E '^func \(itr \*BitmapIterator\) PeekNext\(' bitmap.go | cut -d: -f1)"
members=$(grep -n -E '^(func |// @ (pure )?func |// @ pred )' bitmap.go |
	sed -E 's/^([0-9]+):(\/\/ @ )?(pure )?(func|pred) (\([^)]*\) )?([A-Za-z0-9_]+).*/\1 \6/' |
	awk -v pure="$(echo $pure)" 'BEGIN { n = split(pure, a, " "); for (i = 1; i <= n; i++) skip[a[i]] = 1 } !($1 in skip)')

results=$(mktemp)
trap 'rm -f "$results"' EXIT
{
	# Line 1 of bitmap.go starts no member: this run verifies only the lemmas.
	echo "bitmap_lemmas.gobra types.go bitmap.go@1 bitmap_mem.gobra bitmap_lemmas.gobra"
	echo "pure-functions-and-predicates types.go bitmap.go@$(echo $pure | tr ' ' ',') bitmap_mem.gobra bitmap_lemmas.gobra@1"
	while read -r line name; do
		echo "$name types.go bitmap.go@$line bitmap_mem.gobra bitmap_lemmas.gobra@1"
	done <<<"$members"
	# bitmap_select.go (row-selection primitives) is verified with bitmap.go
	# loaded (bitmap.go@1 verifies none of its members). Runs for bitmap.go
	# members leave this file out; see the note at the top of the file.
	grep -n -E '^func ' bitmap_select.go |
		sed -E 's/^([0-9]+):func (\([^)]*\) )?([A-Za-z0-9_]+).*/\1 \3/' |
		while read -r line name; do
			echo "select:$name types.go bitmap.go@1 bitmap_select.go@$line bitmap_mem.gobra bitmap_lemmas.gobra@1"
		done
} | xargs -P "$VERIFY_JOBS" -L 1 bash -c 'run "$@"' _ | tee "$results" || true
if grep -q '^FAIL' "$results"; then
	echo "verification failed"
	exit 1
fi
echo "all $(grep -c '^ok' "$results") runs verified"
