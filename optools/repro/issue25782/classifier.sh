#!/usr/bin/env bash
# Conservative per-operator classifier for issue #25782.
set -euo pipefail

usage() {
    printf 'usage: %s {broadcast|shuffle} EVIDENCE_TSV FLAGS_FILE QUERY_RC [ALLOWED_CNS [PROVENANCE COMPLETE RUN_ID RUNTIME SNAPSHOT]]\n' "$0" >&2
    exit 2
}

(( $# == 4 || $# == 5 || $# == 10 )) || usage
mode="$1"
evidence="$2"
flags="$3"
query_rc="$4"
allowed_cns="${5-}"
provenance="${6-}"
complete_marker="${7-}"
expected_run_id="${8-}"
expected_runtime="${9-}"
expected_snapshot="${10-}"
case "$mode" in
    broadcast|shuffle) ;;
    *) usage ;;
esac

if (( $# >= 5 )) && [[ -z "$allowed_cns" ]]; then
    printf '%s: ALLOWED_CNS must be a non-empty comma-separated list\n' "$0" >&2
    exit 2
fi

result=INCONCLUSIVE
reason=missing_operator_evidence
provenance_ok=1
require_attempts=0
if (( $# == 10 )); then
    require_attempts=1
    provenance_ok=0
    expected_header=$'schema_version\trun_id\truntime\tphase\tsnapshot_path\tsnapshot_sha256\tevidence_path\tevidence_sha256\tquery_rc\textracted_at'
    header="$(sed -n '1p' "$provenance" 2>/dev/null || true)"
    record="$(sed -n '2p' "$provenance" 2>/dev/null || true)"
    extra_record="$(sed -n '3p' "$provenance" 2>/dev/null || true)"
    IFS=$'\t' read -r p_schema p_run p_runtime p_phase p_snapshot p_snapshot_sha p_evidence p_evidence_sha p_rc p_time p_extra <<<"$record"
    if [[ -f "$complete_marker" && "$(cat "$complete_marker" 2>/dev/null || true)" == complete &&
          "$header" == "$expected_header" && -n "$record" && -z "$extra_record" && -z "${p_extra:-}" &&
          "$p_schema" == 1 && "$p_run" == "$expected_run_id" && "$p_runtime" == "$expected_runtime" &&
          "$p_phase" == "$mode" && "$p_rc" == "$query_rc" && -n "$p_time" &&
          -f "$expected_snapshot" && -f "$evidence" &&
          "$p_snapshot" == "$(realpath -e -- "$expected_snapshot" 2>/dev/null || true)" &&
          "$p_evidence" == "$(realpath -e -- "$evidence" 2>/dev/null || true)" &&
          "$p_snapshot_sha" == "$(sha256sum -- "$expected_snapshot" 2>/dev/null | awk '{print $1}')" &&
          "$p_evidence_sha" == "$(sha256sum -- "$evidence" 2>/dev/null | awk '{print $1}')" ]]; then
        provenance_ok=1
    fi
fi
if (( provenance_ok == 0 )); then
    reason=evidence_provenance_invalid
elif [[ "$query_rc" =~ ^[0-9]+$ ]] && (( 10#$query_rc != 0 )); then
    reason=query_process_exit
elif [[ -f "$flags" ]] && grep -Eiq '(^|[[:space:]])(timeout|cancelled|safety_cancel|watchdog_failure|coverage_gap|result_mismatch|cgroup_oom|oom_kill)=(1|true|yes)' "$flags"; then
    reason=timeout_or_safety_cancel_or_oom
elif [[ ! -s "$evidence" ]]; then
    reason=missing_operator_evidence
else
    # TSV columns are instance_id, cn_id, input_rows, is_shuffle, SpillRows,
    # SpillSize.  Input rows are operator counters, never table totals.
    evidence_result="$(awk -F '\t' -v mode="$mode" -v allowed_arg="$allowed_cns" -v require_attempts="$require_attempts" '
        function fail(why) {
          if (!bad) { bad=1; bad_reason=why }
        }
        BEGIN {
          n=0; bad=0; saw_header=0; logical=0; any_spill=0; distinct=0
          allow_supplied=(allowed_arg != "")
          if (allow_supplied) {
            allowed_count=split(allowed_arg, allowed_parts, ",")
            for (i=1; i<=allowed_count; i++) {
              if (allowed_parts[i] == "" || allowed_parts[i] ~ /[[:space:]]/) {
                fail("invalid_allowed_cn_list")
              } else {
                allowed[allowed_parts[i]]=1
              }
            }
          }
        }
        /^[[:space:]]*#/ { next }
        {
          logical++
          # A single exact header is accepted, but a malformed or repeated
          # header is an invalid evidence row.
          if (logical == 1 && $1 == "instance_id") {
            if (NF != 6 || $1 != "instance_id" || $2 != "cn_id" ||
                $3 != "input_rows" || $4 != "is_shuffle" ||
                $5 != "SpillRows" || $6 != "SpillSize") {
              fail("invalid_header")
            } else {
              saw_header=1
            }
            next
          }
          if ($1 == "instance_id") {
            fail("invalid_header")
            next
          }
          # Every non-comment data line must have exactly the six documented
          # tab-separated fields.  Extra fields are not silently ignored.
          if (NF != 6) { fail("invalid_operator_row"); next }
          if ($1 !~ /^[^[:space:]]+$/ || $2 !~ /^[^[:space:]]+$/ ||
              $1 == "-" || $2 == "-") { fail("invalid_identity"); next }
          if (require_attempts) {
            if ($1 !~ /^attempt-[12]-/) { fail("missing_attempt_identity"); next }
            attempt=$1
            sub(/^attempt-/, "", attempt)
            sub(/-.*/, "", attempt)
            attempt_seen[attempt]=1
            if (allow_supplied && allowed_count >= 2 && $2 != allowed_parts[attempt]) {
              fail("attempt_cn_route_mismatch"); next
            }
          }
          if ($3 !~ /^[0-9]+$/) { fail("invalid_input_rows"); next }
          if ($4 !~ /^(true|false|0|1)$/) {
            fail("invalid_shuffle_flag"); next
          }
          if ($5 !~ /^[0-9]+$/ || $6 !~ /^[0-9]+$/) {
            fail("invalid_spill_counter"); next
          }
          if ($1 in seen) { fail("duplicate_instance"); next }
          seen[$1]=1
          isshuffle=($4 == "true" || $4 == "1")
          expected=(mode == "shuffle")
          if (isshuffle != expected) { fail("unexpected_shuffle_flag"); next }
          if (allow_supplied && !($2 in allowed)) {
            fail("unknown_cn"); next
          }
          inrows=($3 + 0)
          spilled=(($5 + 0) > 0 || ($6 + 0) > 0)
          if (mode == "broadcast" && inrows < 1000) {
            fail("input_rows_below_threshold"); next
          }
          if (mode == "shuffle" && inrows < 1000 && spilled) {
            fail("spill_below_threshold"); next
          }
          n++
          if (!cn_seen[$2]) { cn_seen[$2]=1; distinct++ }
          if (mode == "shuffle" && inrows >= 1000) {
            eligible++
            if (require_attempts) eligible_attempt[attempt]=1
            if (spilled) {
              any_spill=1
              if (require_attempts) spill_attempt[attempt]=1
            }
          } else if (mode == "broadcast" && spilled) {
            any_spill=1
          }
        }
        END {
          if (bad) print "INCONCLUSIVE\t" bad_reason
          else if (n == 0) print "INCONCLUSIVE\tinvalid_or_empty_operator_rows"
          else if (require_attempts && !(1 in attempt_seen && 2 in attempt_seen))
            print "INCONCLUSIVE\ttwo_attempts_required"
          else if (mode == "shuffle" && eligible == 0)
            print "INCONCLUSIVE\tno_threshold_eligible_hashbuild"
          else if (mode == "shuffle" && require_attempts &&
                   !(1 in eligible_attempt && 2 in eligible_attempt))
            print "INCONCLUSIVE\tthreshold_eligible_hashbuild_required_per_attempt"
          else if (mode == "shuffle" && require_attempts &&
                   !(1 in spill_attempt && 2 in spill_attempt))
            print "NOT_REPRODUCED\tshuffle_positive_control_did_not_spill_per_attempt"
          else if (mode == "broadcast" && distinct < 2)
            print "INCONCLUSIVE\tbroadcast_requires_two_cns"
          else if (mode == "broadcast" && any_spill)
            print "NOT_REPRODUCED\tbroadcast_hashbuild_spilled"
          else if (mode == "broadcast")
            print "REPRODUCED\tbroadcast_hashbuild_spill_bypassed"
          else if (mode == "shuffle" && any_spill)
            print "REPRODUCED\tshuffle_positive_control_spilled"
          else if (mode == "shuffle")
            print "NOT_REPRODUCED\tshuffle_positive_control_did_not_spill"
          else
            print "INCONCLUSIVE\tmixed_or_unexpected_operator_flags"
        }
    ' "$evidence")"
    result="${evidence_result%%$'\t'*}"
    reason="${evidence_result#*$'\t'}"
fi

printf 'classification=%s\nreason=%s\n' "$result" "$reason"
