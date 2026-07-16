#!/usr/bin/env bash
set -euo pipefail

here=$(cd -- "$(dirname -- "$0")" && pwd)
harness=$(cd -- "$here/.." && pwd)
extract="$harness/extract-phyplan.sh"
classify="$harness/classifier.sh"
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT HUP INT TERM

pass=0
fail=0
ok() {
    pass=$((pass + 1))
    printf 'ok - %s\n' "$1"
}
not_ok() {
    fail=$((fail + 1))
    printf 'not ok - %s\n' "$1" >&2
}
assert_file() {
    local name=$1 expected=$2 actual=$3
    if cmp -s "$expected" "$actual"; then ok "$name"; else not_ok "$name"; diff -u "$expected" "$actual" >&2 || true; fi
}
assert_class() {
    local name=$1 mode=$2 evidence=$3 expected=$4 allowed=${5-cn1,cn2}
    local output
    output=$("$classify" "$mode" "$evidence" "$tmp/flags" 0 "$allowed")
    if grep -q "^classification=$expected$" <<<"$output"; then ok "$name"; else not_ok "$name ($output)"; fi
}

printf '1..22\n'

# Real-output-shaped fixture: idx is intentionally ignored in the stable ID.
printf 'attempt-1-127.0.0.1:18100-scope-1-hashbuild-1\t127.0.0.1:18100\t132096\tfalse\t0\t0\n' >"$tmp/expected-broadcast.tsv"
"$extract" "$here/fixtures/broadcast.out" false "$tmp/broadcast.tsv"
assert_file 'extracts sanitized broadcast output without idx' "$tmp/expected-broadcast.tsv" "$tmp/broadcast.tsv"

# Repeated printed Scope numbers across CNs and multiple HashBuilds in one
# scope use independent encounter/operator ordinals.
cat >"$tmp/expected-scopes.tsv" <<'EOF'
attempt-1-cn-a-scope-1-hashbuild-1	cn-a	1000	false	0	0
attempt-1-cn-a-scope-1-hashbuild-2	cn-a	2000	false	4	64
attempt-1-cn-b-scope-1-hashbuild-1	cn-b	3000	false	0	0
EOF
"$extract" "$here/fixtures/scopes-and-builds.out" false "$tmp/scopes.tsv"
assert_file 'tracks per-CN scope and HashBuild ordinals' "$tmp/expected-scopes.tsv" "$tmp/scopes.tsv"

{
    printf '=== MO_25782_ATTEMPT 1 ===\n'
    cat "$here/fixtures/broadcast.out"
    printf '=== MO_25782_ATTEMPT 2 ===\n'
    sed 's/127\.0\.0\.1:18100/127.0.0.1:18200/g' "$here/fixtures/broadcast.out"
} >"$tmp/two-attempts.out"
cat >"$tmp/expected-two-attempts.tsv" <<'EOF'
attempt-1-127.0.0.1:18100-scope-1-hashbuild-1	127.0.0.1:18100	132096	false	0	0
attempt-2-127.0.0.1:18200-scope-1-hashbuild-1	127.0.0.1:18200	132096	false	0	0
EOF
"$extract" "$tmp/two-attempts.out" false "$tmp/two-attempts.tsv"
assert_file 'preserves two execution-attempt identities' "$tmp/expected-two-attempts.tsv" "$tmp/two-attempts.tsv"

# A later malformed instance invalidates the complete extraction and leaves no
# partial destination behind.
printf 'stale\n' >"$tmp/incomplete.tsv"
if "$extract" "$here/fixtures/incomplete.out" false "$tmp/incomplete.tsv"; then
    not_ok 'rejects incomplete HashBuild'
elif [[ ! -e "$tmp/incomplete.tsv" ]]; then
    ok 'rejects incomplete HashBuild'
else
    not_ok 'rejects incomplete HashBuild (partial destination)'
fi

cat >"$tmp/two-zero.tsv" <<'EOF'
a	cn1	1000	false	0	0
b	cn2	1000	false	0	0
EOF
cat >"$tmp/two-spill.tsv" <<'EOF'
a	cn1	1000	false	1	0
b	cn2	1000	false	0	0
EOF
cat >"$tmp/one.tsv" <<'EOF'
a	cn1	1000	false	0	0
EOF
cat >"$tmp/duplicate.tsv" <<'EOF'
a	cn1	1000	false	0	0
a	cn2	1000	false	0	0
EOF
cat >"$tmp/unknown.tsv" <<'EOF'
a	cn3	1000	false	0	0
b	cn2	1000	false	0	0
EOF
cat >"$tmp/low-input.tsv" <<'EOF'
a	cn1	999	false	0	0
b	cn2	1000	false	0	0
EOF
cat >"$tmp/wrong-flag.tsv" <<'EOF'
a	cn1	1000	true	0	0
b	cn2	1000	true	0	0
EOF

assert_class 'broadcast requires two CNs' broadcast "$tmp/one.tsv" INCONCLUSIVE
assert_class 'broadcast two CNs zero spill reproduces' broadcast "$tmp/two-zero.tsv" REPRODUCED
assert_class 'broadcast any spill is not reproduced' broadcast "$tmp/two-spill.tsv" NOT_REPRODUCED

if grep -q '^classification=INCONCLUSIVE$' <("$classify" broadcast "$tmp/duplicate.tsv" "$tmp/flags" 0 'cn1,cn2'); then ok 'rejects duplicate instance'; else not_ok 'rejects duplicate instance'; fi
if grep -q '^classification=INCONCLUSIVE$' <("$classify" broadcast "$tmp/unknown.tsv" "$tmp/flags" 0 'cn1,cn2'); then ok 'rejects unknown CN'; else not_ok 'rejects unknown CN'; fi
if grep -q '^classification=INCONCLUSIVE$' <("$classify" broadcast "$tmp/low-input.tsv" "$tmp/flags" 0 'cn1,cn2'); then ok 'rejects input below threshold'; else not_ok 'rejects input below threshold'; fi
if grep -q '^classification=INCONCLUSIVE$' <("$classify" broadcast "$tmp/wrong-flag.tsv" "$tmp/flags" 0 'cn1,cn2'); then ok 'rejects unexpected shuffle flag'; else not_ok 'rejects unexpected shuffle flag'; fi

cat >"$tmp/shuffle-spill.tsv" <<'EOF'
a	cn1	1000	true	0	2
EOF
cat >"$tmp/shuffle-none.tsv" <<'EOF'
a	cn1	1000	true	0	0
EOF
cat >"$tmp/shuffle-mixed.tsv" <<'EOF'
a	cn1	1000	true	0	0
b	cn2	1000	true	1	0
EOF
cat >"$tmp/shuffle-empty-partition.tsv" <<'EOF'
a	cn1	0	true	0	0
b	cn1	1000	true	1	8
EOF
assert_class 'shuffle spill reproduces' shuffle "$tmp/shuffle-spill.tsv" REPRODUCED
assert_class 'shuffle no spill is not reproduced' shuffle "$tmp/shuffle-none.tsv" NOT_REPRODUCED
assert_class 'shuffle mixed counters reproduces on any spill' shuffle "$tmp/shuffle-mixed.tsv" REPRODUCED
assert_class 'shuffle permits empty non-spilling partitions' shuffle "$tmp/shuffle-empty-partition.tsv" REPRODUCED

printf 'timeout=1\n' >"$tmp/flags"
if grep -q '^classification=INCONCLUSIVE$' <("$classify" shuffle "$tmp/shuffle-spill.tsv" "$tmp/flags" 0 'cn1,cn2'); then ok 'honors timeout flag'; else not_ok 'honors timeout flag'; fi
: >"$tmp/flags"
if grep -q '^classification=INCONCLUSIVE$' <("$classify" shuffle "$tmp/shuffle-spill.tsv" "$tmp/flags" 7 'cn1,cn2'); then ok 'honors non-zero query rc'; else not_ok 'honors non-zero query rc'; fi
if grep -q '^classification=REPRODUCED$' <("$classify" broadcast "$tmp/two-zero.tsv" "$tmp/flags" 0); then ok 'optional CN allow-list preserves two-CN check'; else not_ok 'optional CN allow-list preserves two-CN check'; fi

cat >"$tmp/provenance-evidence.tsv" <<'EOF'
attempt-1-a	cn1	1000	false	0	0
attempt-2-b	cn2	1000	false	0	0
EOF
printf 'attempt source\n' >"$tmp/provenance-source.out"
printf 'complete\n' >"$tmp/complete"
snapshot_path="$(realpath -e -- "$tmp/provenance-source.out")"
evidence_path="$(realpath -e -- "$tmp/provenance-evidence.tsv")"
snapshot_sha="$(sha256sum -- "$snapshot_path" | awk '{print $1}')"
evidence_sha="$(sha256sum -- "$evidence_path" | awk '{print $1}')"
cat >"$tmp/provenance.tsv" <<EOF
schema_version	run_id	runtime	phase	snapshot_path	snapshot_sha256	evidence_path	evidence_sha256	query_rc	extracted_at
1	run-1	/private/runtime	broadcast	${snapshot_path}	${snapshot_sha}	${evidence_path}	${evidence_sha}	0	2026-07-16T00:00:00+08:00
EOF
if grep -q '^classification=REPRODUCED$' <("$classify" broadcast "$evidence_path" "$tmp/flags" 0 'cn1,cn2' "$tmp/provenance.tsv" "$tmp/complete" run-1 /private/runtime "$snapshot_path"); then ok 'accepts complete current-run provenance'; else not_ok 'accepts complete current-run provenance'; fi

printf '# tamper\n' >>"$evidence_path"
if grep -q '^reason=evidence_provenance_invalid$' <("$classify" broadcast "$evidence_path" "$tmp/flags" 0 'cn1,cn2' "$tmp/provenance.tsv" "$tmp/complete" run-1 /private/runtime "$snapshot_path"); then ok 'rejects evidence changed after provenance'; else not_ok 'rejects evidence changed after provenance'; fi

cat >"$tmp/one-attempt-evidence.tsv" <<'EOF'
attempt-1-a	cn1	1000	false	0	0
attempt-1-b	cn1	1000	false	0	0
EOF
evidence_path="$(realpath -e -- "$tmp/one-attempt-evidence.tsv")"
evidence_sha="$(sha256sum -- "$evidence_path" | awk '{print $1}')"
cat >"$tmp/one-attempt-provenance.tsv" <<EOF
schema_version	run_id	runtime	phase	snapshot_path	snapshot_sha256	evidence_path	evidence_sha256	query_rc	extracted_at
1	run-1	/private/runtime	broadcast	${snapshot_path}	${snapshot_sha}	${evidence_path}	${evidence_sha}	0	2026-07-16T00:00:00+08:00
EOF
if grep -q '^reason=two_attempts_required$' <("$classify" broadcast "$evidence_path" "$tmp/flags" 0 'cn1,cn2' "$tmp/one-attempt-provenance.tsv" "$tmp/complete" run-1 /private/runtime "$snapshot_path"); then ok 'requires both attempt identities in production evidence'; else not_ok 'requires both attempt identities in production evidence'; fi

cat >"$tmp/swapped-attempt-evidence.tsv" <<'EOF'
attempt-1-a	cn2	1000	false	0	0
attempt-2-b	cn1	1000	false	0	0
EOF
evidence_path="$(realpath -e -- "$tmp/swapped-attempt-evidence.tsv")"
evidence_sha="$(sha256sum -- "$evidence_path" | awk '{print $1}')"
cat >"$tmp/swapped-attempt-provenance.tsv" <<EOF
schema_version	run_id	runtime	phase	snapshot_path	snapshot_sha256	evidence_path	evidence_sha256	query_rc	extracted_at
1	run-1	/private/runtime	broadcast	${snapshot_path}	${snapshot_sha}	${evidence_path}	${evidence_sha}	0	2026-07-16T00:00:00+08:00
EOF
if grep -q '^reason=attempt_cn_route_mismatch$' <("$classify" broadcast "$evidence_path" "$tmp/flags" 0 'cn1,cn2' "$tmp/swapped-attempt-provenance.tsv" "$tmp/complete" run-1 /private/runtime "$snapshot_path"); then ok 'binds each attempt to its requested CN'; else not_ok 'binds each attempt to its requested CN'; fi

if (( fail != 0 )); then
    printf '%d passed, %d failed\n' "$pass" "$fail" >&2
    exit 1
fi
printf '%d passed\n' "$pass"
