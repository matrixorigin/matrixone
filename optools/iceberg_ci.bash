#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-core}"
REPORT_DIR="${MO_ICEBERG_REPORT_DIR:-${ROOT_DIR}/test/iceberg/reports/ci_$(date -u +%Y%m%dT%H%M%SZ)}"
COVER_DIR="${MO_ICEBERG_COVER_DIR:-${ROOT_DIR}/test/iceberg/reports/coverage}"
THIRDPARTIES_INSTALL_DIR="${ROOT_DIR}/thirdparties/install"

export CGO_CFLAGS="${CGO_CFLAGS:-"-I${ROOT_DIR}/cgo -I${THIRDPARTIES_INSTALL_DIR}/include"}"
export CGO_LDFLAGS="${CGO_LDFLAGS:-"-Wl,-rpath,${THIRDPARTIES_INSTALL_DIR}/lib -Wl,-rpath,${ROOT_DIR}/cgo -L${THIRDPARTIES_INSTALL_DIR}/lib -L${ROOT_DIR}/cgo -lmo -lusearch_c -lm"}"
export LD_LIBRARY_PATH="${THIRDPARTIES_INSTALL_DIR}/lib:${ROOT_DIR}/cgo${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
export DYLD_LIBRARY_PATH="${THIRDPARTIES_INSTALL_DIR}/lib:${ROOT_DIR}/cgo${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"

log() {
  printf '[iceberg-ci] %s\n' "$*"
}

die() {
  printf '[iceberg-ci] ERROR: %s\n' "$*" >&2
  exit 1
}

run() {
  log "$*"
  "$@"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

require_env() {
  local key="$1"
  if [[ -z "${!key:-}" ]]; then
    die "$key is required for MO_ICEBERG_CI_PROFILE=${MO_ICEBERG_CI_PROFILE:-local}"
  fi
}

require_file() {
  local key="$1"
  require_env "$key"
  [[ -f "${!key}" ]] || die "$key must point to an existing file: ${!key}"
}

require_sql_template() {
  local key="$1"
  require_env "$key"
  case "${!key}" in
    *"{sql}"*) ;;
    *) die "$key must contain the {sql} placeholder" ;;
  esac
}

contains_profile() {
  local needle="$1"
  local profile="${MO_ICEBERG_CI_PROFILE:-local}"
  case ",${profile}," in
    *",${needle},"*|*",nightly,"*) return 0 ;;
    *) return 1 ;;
  esac
}

go_test_core() {
  run go test -tags matrixone_test -short -count=1 \
    ./pkg/iceberg/api \
    ./pkg/iceberg/catalog \
    ./pkg/iceberg/dml \
    ./pkg/iceberg/io \
    ./pkg/iceberg/maintenance \
    ./pkg/iceberg/metadata \
    ./pkg/iceberg/model \
    ./pkg/iceberg/ref \
    ./pkg/iceberg/write \
    ./pkg/sql/iceberg \
    ./pkg/sql/compile \
    ./pkg/sql/colexec/icebergdelete \
    ./pkg/sql/colexec/external \
    ./pkg/pb/pipeline \
    ./pkg/sql/plan \
    ./pkg/sql/plan/explain
}

go_test_embedded() {
  run go test -tags matrixone_test -short -count=1 ./pkg/embed -run '^TestIcebergSQLEngineEmbedded'
}

go_test_adapter() {
  (cd "${ROOT_DIR}/pkg/iceberg/adapter/iceberggo" && run go test -tags iceberggo -count=1 ./...)
}

go_test_golden() {
  run python3 "${ROOT_DIR}/pkg/iceberg/metadata/testdata/generate_golden_vectors.py" --check
  run go test ./pkg/iceberg/metadata -run '^TestGoldenVectorProvenanceArtifacts$' -count=1
}

sanitize_artifact_file() {
  local src="$1"
  local dst="$2"
  if [[ ! -f "$src" ]]; then
    printf 'log file was not captured: %s\n' "$src" >"$dst"
    return
  fi
  python3 - "$src" "$dst" <<'PY'
import hashlib
import re
import sys

src, dst = sys.argv[1], sys.argv[2]
with open(src, "r", encoding="utf-8", errors="replace") as f:
    text = f.read()

def redact_path(match):
    value = match.group(0)
    return "<redacted:path:%s>" % hashlib.sha256(value.encode()).hexdigest()[:8]

text = re.sub(r"\b(?:s3|gs|abfs|abfss)://[^\s,;)`]+", redact_path, text, flags=re.I)
text = re.sub(r"(AKIA[0-9A-Z]{16}|X-Amz-Signature=[^&\s]+|AWSAccessKeyId=[^&\s]+|raw-token|raw-secret-key)", "<redacted>", text)
with open(dst, "w", encoding="utf-8") as f:
    f.write(text)
PY
}

ICEBERG_E2E_TMP_DIR=""
ICEBERG_E2E_MO_PID=""

iceberg_e2e_collect_logs() {
  [[ -n "$ICEBERG_E2E_TMP_DIR" ]] || return
  mkdir -p "$REPORT_DIR"
  sanitize_artifact_file "${ICEBERG_E2E_TMP_DIR}/mo-service.log" "${REPORT_DIR}/mo-service.log"
  if command -v docker >/dev/null 2>&1; then
    (cd "${ROOT_DIR}/etc/launch-minio-local" && docker compose logs --no-color nessie >"${ICEBERG_E2E_TMP_DIR}/nessie.log" 2>&1) || true
    (cd "${ROOT_DIR}/etc/launch-minio-local" && docker compose logs --no-color minio >"${ICEBERG_E2E_TMP_DIR}/minio.log" 2>&1) || true
    sanitize_artifact_file "${ICEBERG_E2E_TMP_DIR}/nessie.log" "${REPORT_DIR}/nessie.log"
    sanitize_artifact_file "${ICEBERG_E2E_TMP_DIR}/minio.log" "${REPORT_DIR}/minio.log"
  fi
}

iceberg_e2e_cleanup() {
  local status=$?
  if [[ -n "$ICEBERG_E2E_MO_PID" ]] && kill -0 "$ICEBERG_E2E_MO_PID" >/dev/null 2>&1; then
    kill "$ICEBERG_E2E_MO_PID" >/dev/null 2>&1 || true
    wait "$ICEBERG_E2E_MO_PID" >/dev/null 2>&1 || true
  fi
  iceberg_e2e_collect_logs
  (cd "$ROOT_DIR" && make dev-down-iceberg-tier-a >/dev/null 2>&1) || true
  return "$status"
}

iceberg_e2e_local() {
  require_cmd docker
  require_cmd curl
  require_cmd python3

  mkdir -p "$REPORT_DIR"
  ICEBERG_E2E_TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mo-iceberg-e2e-local.XXXXXX")"
  trap iceberg_e2e_cleanup EXIT

  run make build
  go_test_adapter
  go_test_golden

  run make dev-up-iceberg-tier-a
  log "starting mo-service for Iceberg E2E local"
  MO_ICEBERG_ALLOW_PLAIN_HTTP=1 "${ROOT_DIR}/mo-service" -launch "${ROOT_DIR}/etc/launch-minio-local/launch.toml" \
    >"${ICEBERG_E2E_TMP_DIR}/mo-service.log" 2>&1 &
  ICEBERG_E2E_MO_PID="$!"

  run go run ./optools/iceberg_e2e_local.go \
    --catalog-uri "${MO_ICEBERG_E2E_CATALOG_URI:-http://127.0.0.1:19120/iceberg}" \
    --warehouse "${MO_ICEBERG_E2E_WAREHOUSE:-s3://mo-iceberg/warehouse}" \
    --dsn "${MO_ICEBERG_E2E_DSN:-root:111@tcp(127.0.0.1:6001)/?timeout=5s&readTimeout=30s&writeTimeout=30s&multiStatements=false}" \
    --report-dir "$REPORT_DIR"

  iceberg_e2e_collect_logs
  verify_report_artifacts "$REPORT_DIR"
}

coverage_threshold_for() {
  case "$1" in
    ./pkg/iceberg/api) printf '38.0' ;;
    ./pkg/iceberg/catalog) printf '76.0' ;;
    ./pkg/iceberg/dml) printf '68.0' ;;
    ./pkg/iceberg/io) printf '71.8' ;;
    ./pkg/iceberg/maintenance) printf '75.4' ;;
    ./pkg/iceberg/metadata) printf '69.7' ;;
    ./pkg/iceberg/ref) printf '74.7' ;;
    ./pkg/iceberg/write) printf '74.8' ;;
    ./pkg/sql/iceberg) printf '64.7' ;;
    *) die "no coverage threshold registered for $1" ;;
  esac
}

extract_go_test_coverage() {
  awk '
    /coverage:/ {
      for (i = 1; i <= NF; i++) {
        if ($i == "coverage:") {
          gsub("%", "", $(i + 1));
          print $(i + 1);
          found = 1;
          exit;
        }
      }
    }
    END {
      if (!found) {
        exit 1;
      }
    }
  '
}

assert_percent_ge() {
  local actual="$1"
  local threshold="$2"
  local label="$3"
  awk -v actual="$actual" -v threshold="$threshold" -v label="$label" '
    BEGIN {
      if ((actual + 0) + 0.0001 < (threshold + 0)) {
        printf("%s coverage %.1f%% is below threshold %.1f%%\n", label, actual, threshold) > "/dev/stderr";
        exit 1;
      }
    }
  '
}

go_test_coverage() {
  mkdir -p "$COVER_DIR"
  local packages=(
    ./pkg/iceberg/api
    ./pkg/iceberg/catalog
    ./pkg/iceberg/dml
    ./pkg/iceberg/io
    ./pkg/iceberg/maintenance
    ./pkg/iceberg/metadata
    ./pkg/iceberg/ref
    ./pkg/iceberg/write
    ./pkg/sql/iceberg
  )

  local coverprofile="${COVER_DIR}/iceberg_core_cover.out"
  run go test -cover -coverprofile="$coverprofile" -count=1 "${packages[@]}"
  local total
  total="$(go tool cover -func="$coverprofile" | awk '/^total:/ { gsub("%", "", $3); print $3 }')"
  [[ -n "$total" ]] || die "failed to read total coverage from $coverprofile"
  assert_percent_ge "$total" "70.0" "iceberg core total"
  log "iceberg core total coverage ${total}% >= 70.0%"

  local pkg out pct threshold
  for pkg in "${packages[@]}"; do
    log "checking package coverage threshold for $pkg"
    out="$(go test -cover -count=1 "$pkg" 2>&1)"
    printf '%s\n' "$out"
    pct="$(printf '%s\n' "$out" | extract_go_test_coverage)" || die "failed to parse coverage for $pkg"
    threshold="$(coverage_threshold_for "$pkg")"
    assert_percent_ge "$pct" "$threshold" "$pkg"
    log "$pkg coverage ${pct}% >= ${threshold}%"
  done
}

preflight_http_get() {
  local url="$1"
  local label="$2"
  curl -fsS --max-time 10 "$url" >/dev/null || die "$label preflight failed: $url"
}

catalog_config_url() {
  local uri="${1%/}"
  case "$uri" in
    */v1) printf '%s/config' "$uri" ;;
    *) printf '%s/v1/config' "$uri" ;;
  esac
}

minio_health_url() {
  local endpoint="$1"
  case "$endpoint" in
    http://*|https://*) ;;
    *) endpoint="http://${endpoint}" ;;
  esac
  printf '%s/minio/health/live' "${endpoint%/}"
}

preflight() {
  local profile="${MO_ICEBERG_CI_PROFILE:-local}"
  if [[ "$profile" == "local" || "$profile" == "none" ]]; then
    log "MO_ICEBERG_CI_PROFILE=${profile}; external preflight is intentionally skipped"
    return
  fi

  require_cmd curl
  require_cmd go

  if contains_profile tier-a; then
    require_env MO_ICEBERG_IT
    [[ "$MO_ICEBERG_IT" == "1" ]] || die "MO_ICEBERG_IT must be 1 for tier-a profile"
    require_env MO_ICEBERG_CATALOG_URI
    require_env MO_ICEBERG_S3_ENDPOINT
    require_sql_template MO_ICEBERG_MO_SQL_CMD
    require_sql_template MO_ICEBERG_SPARK_SQL_CMD
    preflight_http_get "$(catalog_config_url "$MO_ICEBERG_CATALOG_URI")" "Nessie/Iceberg REST catalog"
    preflight_http_get "$(minio_health_url "$MO_ICEBERG_S3_ENDPOINT")" "MinIO"
  fi

  if contains_profile credential-vending; then
    require_env MO_ICEBERG_CREDENTIAL_VENDING
    [[ "$MO_ICEBERG_CREDENTIAL_VENDING" == "1" ]] || die "MO_ICEBERG_CREDENTIAL_VENDING must be 1 for credential-vending profile"
    require_env MO_ICEBERG_CATALOG_URI
    require_file MO_ICEBERG_CREDENTIAL_VENDING_SCENARIOS
    require_sql_template MO_ICEBERG_MO_SQL_CMD
    require_sql_template MO_ICEBERG_SPARK_SQL_CMD
    require_env MO_ICEBERG_CREDENTIAL_VENDING_EXPIRED_ERROR
    preflight_http_get "$(catalog_config_url "$MO_ICEBERG_CATALOG_URI")" "credential-vending catalog"
  fi

  if contains_profile cross-engine; then
    require_env MO_ICEBERG_CROSS_ENGINE
    [[ "$MO_ICEBERG_CROSS_ENGINE" == "1" ]] || die "MO_ICEBERG_CROSS_ENGINE must be 1 for cross-engine profile"
    require_sql_template MO_ICEBERG_MO_SQL_CMD
    require_sql_template MO_ICEBERG_SPARK_SQL_CMD
    require_sql_template MO_ICEBERG_TRINO_SQL_CMD
  fi

  if contains_profile golden-real; then
    require_env MO_ICEBERG_CROSS_ENGINE
    [[ "$MO_ICEBERG_CROSS_ENGINE" == "1" ]] || die "MO_ICEBERG_CROSS_ENGINE must be 1 for golden-real profile"
    require_file MO_ICEBERG_GOLDEN_REAL_SCENARIOS
    require_sql_template MO_ICEBERG_MO_SQL_CMD
    require_sql_template MO_ICEBERG_SPARK_SQL_CMD
    require_sql_template MO_ICEBERG_TRINO_SQL_CMD
  fi

  if contains_profile tier-b; then
    require_env MO_ICEBERG_PUBLIC_DATASET
    [[ "$MO_ICEBERG_PUBLIC_DATASET" == "1" ]] || die "MO_ICEBERG_PUBLIC_DATASET must be 1 for tier-b profile"
    require_env MO_ICEBERG_PUBLIC_DATASET_CATALOG_URI
    require_env MO_ICEBERG_PUBLIC_DATASET_WAREHOUSE
    require_file MO_ICEBERG_PUBLIC_DATASET_SCENARIOS
    require_sql_template MO_ICEBERG_PUBLIC_DATASET_MO_SQL_CMD
    require_sql_template MO_ICEBERG_PUBLIC_DATASET_OFFICIAL_SQL_CMD
    preflight_http_get "$(catalog_config_url "$MO_ICEBERG_PUBLIC_DATASET_CATALOG_URI")" "Tier B public dataset catalog"
  fi

  if contains_profile tier-c; then
    require_env MO_ICEBERG_SANDBOX
    require_env MO_ICEBERG_TIER_C_CATALOG_URI
    require_env MO_ICEBERG_TIER_C_WAREHOUSE
    require_file MO_ICEBERG_TIER_C_SCENARIOS
    require_sql_template MO_ICEBERG_TIER_C_MO_SQL_CMD
    require_sql_template MO_ICEBERG_TIER_C_OFFICIAL_SQL_CMD
    preflight_http_get "$(catalog_config_url "$MO_ICEBERG_TIER_C_CATALOG_URI")" "Tier C catalog"
  fi

  if contains_profile remote-signing; then
    require_env MO_ICEBERG_REMOTE_SIGNING
    [[ "$MO_ICEBERG_REMOTE_SIGNING" == "1" ]] || die "MO_ICEBERG_REMOTE_SIGNING must be 1 for remote-signing profile"
    require_env MO_ICEBERG_TIER_C_SIGNER_URI
  fi

  if contains_profile server-planning; then
    require_env MO_ICEBERG_SERVER_PLANNING
    [[ "$MO_ICEBERG_SERVER_PLANNING" == "1" ]] || die "MO_ICEBERG_SERVER_PLANNING must be 1 for server-planning profile"
  fi

  if contains_profile tier-d; then
    require_env MO_ICEBERG_NESR
    [[ "$MO_ICEBERG_NESR" == "1" ]] || die "MO_ICEBERG_NESR must be 1 for tier-d profile"
    require_file MO_ICEBERG_NESR_SCENARIOS
    require_sql_template MO_ICEBERG_NESR_MO_SQL_CMD
    require_sql_template MO_ICEBERG_NESR_EXTERNAL_SQL_CMD
    require_file MO_ICEBERG_NESR_EXPECTED_KPI
    require_env MO_ICEBERG_NESR_RESIDENCY_ERROR
  fi

  log "preflight passed for MO_ICEBERG_CI_PROFILE=${profile}"
}

run_tier_a_if_enabled() {
  if contains_profile tier-a; then
    run go test ./pkg/sql/iceberg -run '^TestIcebergTierA' -count=1 -v
  else
    log "tier-a profile not enabled; skipping Tier A test execution"
  fi
}

run_cross_engine_if_enabled() {
  if contains_profile cross-engine; then
    run go test ./pkg/sql/iceberg -run '^TestIcebergDMLCrossEngineDiff$' -count=1 -v
  else
    log "cross-engine profile not enabled; skipping cross-engine diff execution"
  fi
}

run_golden_real_if_enabled() {
  if contains_profile golden-real; then
    export MO_ICEBERG_CROSS_ENGINE=1
    export MO_ICEBERG_CROSS_ENGINE_SCENARIOS="${MO_ICEBERG_GOLDEN_REAL_SCENARIOS}"
    run go test ./pkg/sql/iceberg -run '^TestIcebergDMLCrossEngineDiff$' -count=1 -v
  else
    log "golden-real profile not enabled; skipping real-file golden cross-engine diff"
  fi
}

run_external_profile() {
  local profile="$1"
  local scenario_file="$2"
  run python3 "${ROOT_DIR}/optools/iceberg_external_runner.py" \
    --profile "$profile" \
    --scenario-file "$scenario_file" \
    --report-dir "$REPORT_DIR"
}

run_remaining_external_if_enabled() {
  if contains_profile credential-vending; then
    run_external_profile credential-vending "$MO_ICEBERG_CREDENTIAL_VENDING_SCENARIOS"
  else
    log "credential-vending profile not enabled; skipping real credential vending scenarios"
  fi

  if contains_profile tier-b; then
    run_external_profile tier-b "$MO_ICEBERG_PUBLIC_DATASET_SCENARIOS"
  else
    log "tier-b profile not enabled; skipping public dataset scenarios"
  fi

  if contains_profile tier-c; then
    run_external_profile tier-c "$MO_ICEBERG_TIER_C_SCENARIOS"
  else
    log "tier-c profile not enabled; skipping sandbox scenarios"
  fi

  if contains_profile tier-d; then
    run_external_profile tier-d "$MO_ICEBERG_NESR_SCENARIOS"
  else
    log "tier-d profile not enabled; skipping NESR scenarios"
  fi
}

validate_external_templates() {
  local template
  for template in \
    "${ROOT_DIR}/test/iceberg/credential_vending_scenarios.example.json" \
    "${ROOT_DIR}/test/iceberg/tier_b_public_dataset_scenarios.example.json" \
    "${ROOT_DIR}/test/iceberg/tier_c_sandbox_scenarios.example.json" \
    "${ROOT_DIR}/test/iceberg/tier_d_nesr_scenarios.example.json"; do
    run python3 "${ROOT_DIR}/optools/iceberg_external_runner.py" \
      --profile template \
      --scenario-file "$template" \
      --report-dir "${REPORT_DIR}/template-validation" \
      --validate-only
  done
  run python3 -m json.tool "${ROOT_DIR}/test/iceberg/golden_real_scenarios.example.json" >/dev/null
  run go test ./pkg/sql/iceberg -run '^TestGoldenRealScenarioExampleValidates$' -count=1
  selftest_external_runner
}

verify_report_artifacts() {
  local dir="${1:-$REPORT_DIR}"
  [[ -d "$dir" ]] || die "report directory does not exist: $dir"
  local found=0
  local case_dir
  while IFS= read -r case_dir; do
    found=1
    for name in metadata.json diff.json summary.md; do
      [[ -s "${case_dir}/${name}" ]] || die "missing or empty ${case_dir}/${name}"
    done
    local engine
    while IFS= read -r engine; do
      [[ -n "$engine" ]] || continue
      [[ -s "${case_dir}/${engine}.out" ]] || die "missing or empty ${case_dir}/${engine}.out"
    done < <(python3 - "${case_dir}/diff.json" <<'PY'
import json
import re
import sys

with open(sys.argv[1], encoding="utf-8") as f:
    diff = json.load(f)
seen = set()
for item in diff.get("engines", []):
    name = str(item.get("engine", "")).strip()
    if not name:
        continue
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)
    if safe and safe not in seen:
        seen.add(safe)
        print(safe)
PY
    )
  done < <(find "$dir" -mindepth 1 -maxdepth 1 -type d | sort)
  [[ "$found" == "1" ]] || die "no report case directories found under $dir"
  if grep -R -E '(AKIA[0-9A-Z]{16}|X-Amz-Signature=|AWSAccessKeyId=|raw-token|raw-secret-key)' "$dir" >/dev/null 2>&1; then
    die "report directory contains unredacted sensitive material: $dir"
  fi
  if grep -R -E '(s3|gs|abfs|abfss)://[^[:space:],;)`]+' "$dir" >/dev/null 2>&1; then
    die "report directory contains an unredacted object path: $dir"
  fi
  log "report artifacts verified under $dir"
}

selftest_external_runner() {
  local tmp_dir="${REPORT_DIR}/external-runner-selftest"
  local scenario_file="${tmp_dir}/scenarios.json"
  rm -rf "$tmp_dir"
  mkdir -p "$tmp_dir"
  cat >"$scenario_file" <<'JSON'
[
  {
    "id": "ICE-TEST-124",
    "case_id": "ICE-TEST-124_external_runner_success_selftest",
    "name": "external-runner-success-selftest",
    "comparison_mode": "ordered",
    "engines": [
      {
        "name": "mo",
        "command_template": "printf 'warning: noisy line\\n1 ksa\\ns3://bucket/raw-token/path\\n' # {sql}",
        "sql": "select 1"
      },
      {
        "name": "spark",
        "command_template": "printf '1 ksa\\ns3://bucket/raw-token/path\\n' # {sql}",
        "sql": "select 1"
      },
      {
        "name": "trino",
        "command_template": "printf '1 ksa\\ns3://bucket/raw-token/path\\n' # {sql}",
        "sql": "select 1"
      },
      {
        "name": "official",
        "command_template": "printf '1 ksa\\ns3://bucket/raw-token/path\\n' # {sql}",
        "sql": "select 1"
      }
    ]
  },
  {
    "id": "ICE-TEST-135",
    "case_id": "ICE-TEST-135_external_runner_expected_error_selftest",
    "name": "external-runner-expected-error-selftest",
    "comparison_mode": "ordered",
    "engines": [
      {
        "name": "mo",
        "command_template": "printf 'ICEBERG_RESIDENCY_DENIED: denied secret=raw-secret-key s3://bucket/private/path\\n'; exit 7 # {sql}",
        "sql": "select 1",
        "expect_error_contains": "ICEBERG_RESIDENCY_DENIED"
      }
    ]
  }
]
JSON
  run python3 "${ROOT_DIR}/optools/iceberg_external_runner.py" \
    --profile selftest \
    --scenario-file "$scenario_file" \
    --report-dir "$tmp_dir"
  rm -f "$scenario_file"
  local old_expected="${MO_ICEBERG_EXTERNAL_EXPECTED_TESTS:-}"
  export MO_ICEBERG_EXTERNAL_EXPECTED_TESTS="ICE-TEST-124 ICE-TEST-135"
  verify_report_artifacts "$tmp_dir"
  verify_external_coverage "$tmp_dir"
  if [[ -n "$old_expected" ]]; then
    export MO_ICEBERG_EXTERNAL_EXPECTED_TESTS="$old_expected"
  else
    unset MO_ICEBERG_EXTERNAL_EXPECTED_TESTS
  fi
}

external_expected_tests_for_profile() {
  if [[ -n "${MO_ICEBERG_EXTERNAL_EXPECTED_TESTS:-}" ]]; then
    printf '%s\n' "$MO_ICEBERG_EXTERNAL_EXPECTED_TESTS"
    return
  fi

  local profile="${MO_ICEBERG_CI_PROFILE:-}"
  local expected=()
  case ",${profile}," in
    *",credential-vending,"*) expected+=(ICE-TEST-124) ;;
  esac
  case ",${profile}," in
    *",tier-b,"*) expected+=(ICE-TEST-130) ;;
  esac
  case ",${profile}," in
    *",tier-c,"*) expected+=(ICE-TEST-131 ICE-TEST-132 ICE-TEST-133 ICE-TEST-134) ;;
  esac
  case ",${profile}," in
    *",tier-d,"*) expected+=(ICE-TEST-135) ;;
  esac
  case ",${profile}," in
    *",golden-real,"*) expected+=(ICE-TEST-156 ICE-TEST-157 ICE-TEST-158 ICE-TEST-159) ;;
  esac
  case ",${profile}," in
    *",nightly,"*)
      expected=(
        ICE-TEST-124
        ICE-TEST-130
        ICE-TEST-131
        ICE-TEST-132
        ICE-TEST-133
        ICE-TEST-134
        ICE-TEST-135
        ICE-TEST-156
        ICE-TEST-157
        ICE-TEST-158
        ICE-TEST-159
      )
      ;;
  esac

  if [[ "${#expected[@]}" -eq 0 ]]; then
    expected=(
      ICE-TEST-124
      ICE-TEST-130
      ICE-TEST-131
      ICE-TEST-132
      ICE-TEST-133
      ICE-TEST-134
      ICE-TEST-135
      ICE-TEST-156
      ICE-TEST-157
      ICE-TEST-158
      ICE-TEST-159
    )
  fi
  printf '%s\n' "${expected[*]}"
}

verify_external_coverage() {
  local dir="${1:-$REPORT_DIR}"
  [[ -d "$dir" ]] || die "report directory does not exist: $dir"
  local expected
  expected="$(external_expected_tests_for_profile)"
  run python3 - "$dir" "$expected" <<'PY'
import datetime as dt
import json
import pathlib
import re
import sys

report_dir = pathlib.Path(sys.argv[1])
expected = [item for item in re.split(r"[\s,]+", sys.argv[2].strip()) if item]
expected_set = set(expected)
found = {}
artifacts = []

for metadata_path in sorted(report_dir.rglob("metadata.json")):
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"read/parse {metadata_path}: {exc}")
    diff_path = metadata_path.with_name("diff.json")
    diff = {}
    if diff_path.exists():
        try:
            diff = json.loads(diff_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise SystemExit(f"read/parse {diff_path}: {exc}")
    case_id = str(metadata.get("case_id") or diff.get("case_id") or "").strip()
    match = re.search(r"ICE-TEST-\d+", case_id)
    if not match:
        continue
    test_id = match.group(0)
    status = str(metadata.get("status") or diff.get("status") or "").strip().lower()
    artifact = {
        "test_id": test_id,
        "case_id": case_id,
        "status": status,
        "path": str(metadata_path.parent),
    }
    artifacts.append(artifact)
    if status == "success":
        found.setdefault(test_id, []).append(artifact)

missing = [test_id for test_id in expected if test_id not in found]
failed = [item for item in artifacts if item["test_id"] in expected_set and item["status"] != "success"]
out_json = report_dir / "external_artifact_coverage.json"
out_md = report_dir / "external_artifact_coverage.md"
now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
payload = {
    "generated_at_utc": now,
    "expected_tests": expected,
    "missing_tests": missing,
    "failed_artifacts": failed,
    "covered_tests": {test_id: found.get(test_id, []) for test_id in expected},
}
out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")

with out_md.open("w", encoding="utf-8") as out:
    out.write("# Iceberg External Artifact Coverage\n\n")
    out.write(f"generated_at_utc: `{now}`\n\n")
    out.write("| Test | Status | Artifact count | Example artifact |\n")
    out.write("| --- | --- | ---: | --- |\n")
    for test_id in expected:
        entries = found.get(test_id, [])
        status = "covered" if entries else "missing"
        example = entries[0]["path"] if entries else ""
        out.write(f"| `{test_id}` | {status} | {len(entries)} | `{example}` |\n")
    if failed:
        out.write("\n## Failed Artifacts\n\n")
        out.write("| Test | Status | Artifact |\n")
        out.write("| --- | --- | --- |\n")
        for item in failed:
            out.write(f"| `{item['test_id']}` | `{item['status']}` | `{item['path']}` |\n")

if missing or failed:
    details = []
    if missing:
        details.append("missing: " + ", ".join(missing))
    if failed:
        details.append("failed artifacts: " + ", ".join(f"{item['test_id']}({item['status']})" for item in failed))
    raise SystemExit("external artifact coverage incomplete; " + "; ".join(details))

print(f"[iceberg-ci] external artifact coverage verified for {len(expected)} test(s)")
PY
  log "external artifact coverage written to ${dir}/external_artifact_coverage.md"
}

external_coverage_profile_enabled() {
  local profile="${MO_ICEBERG_CI_PROFILE:-local}"
  case ",${profile}," in
    *",credential-vending,"*|*",tier-b,"*|*",tier-c,"*|*",tier-d,"*|*",golden-real,"*|*",nightly,"*) return 0 ;;
    *) return 1 ;;
  esac
}

generate_cap_dashboard() {
  local out_dir="${1:-$REPORT_DIR}"
  local plan_path="$ROOT_DIR/docs/iceberg/matrixone_iceberg_connector_test_plan.md"
  mkdir -p "$out_dir"
  if [[ ! -f "$plan_path" ]]; then
    cat >"$out_dir/cap_coverage.md" <<EOF
# Iceberg CAP Coverage Dashboard

generated_at_utc: \`$(date -u +%Y-%m-%dT%H:%M:%SZ)\`

The Iceberg test plan document is not present in this checkout, so CAP coverage generation was skipped.
EOF
    log "CAP coverage dashboard skipped because ${plan_path} is not present"
    return
  fi
  run python3 - "$plan_path" "$out_dir/cap_coverage.md" <<'PY'
import collections
import datetime as dt
import re
import sys

plan_path, out_path = sys.argv[1], sys.argv[2]
line_re = re.compile(r"^- \[( |x)\] (ICE-TEST-\d+) \[([^\]]+)\](.*)$")
cap_re = re.compile(r"CAP-\d+")

by_cap = collections.defaultdict(lambda: collections.Counter())
by_layer = collections.defaultdict(lambda: collections.Counter())
rows = []

with open(plan_path, encoding="utf-8") as f:
    for line in f:
        m = line_re.match(line.rstrip())
        if not m:
            continue
        status = "done" if m.group(1) == "x" else "open"
        case_id = m.group(2)
        layer = m.group(3)
        caps = cap_re.findall(line)
        if not caps:
            caps = ["CAP-UNSPECIFIED"]
        by_layer[layer][status] += 1
        for cap in caps:
            by_cap[cap][status] += 1
            rows.append((cap, layer, case_id, status))

def pct(done, total):
    return "0.0%" if total == 0 else f"{done * 100.0 / total:.1f}%"

now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
with open(out_path, "w", encoding="utf-8") as out:
    out.write("# Iceberg CAP Coverage Dashboard\n\n")
    out.write(f"generated_at_utc: `{now}`\n\n")
    out.write("## By CAP\n\n")
    out.write("| CAP | done | open | total | completion |\n")
    out.write("| --- | ---: | ---: | ---: | ---: |\n")
    for cap in sorted(by_cap):
        done = by_cap[cap]["done"]
        open_ = by_cap[cap]["open"]
        total = done + open_
        out.write(f"| `{cap}` | {done} | {open_} | {total} | {pct(done, total)} |\n")
    out.write("\n## By Layer\n\n")
    out.write("| Layer | done | open | total | completion |\n")
    out.write("| --- | ---: | ---: | ---: | ---: |\n")
    for layer in sorted(by_layer):
        done = by_layer[layer]["done"]
        open_ = by_layer[layer]["open"]
        total = done + open_
        out.write(f"| `{layer}` | {done} | {open_} | {total} | {pct(done, total)} |\n")
    out.write("\n## Open Items\n\n")
    out.write("| CAP | Layer | Test |\n")
    out.write("| --- | --- | --- |\n")
    for cap, layer, case_id, status in rows:
        if status == "open":
            out.write(f"| `{cap}` | `{layer}` | `{case_id}` |\n")
PY
  log "CAP coverage dashboard written to ${out_dir}/cap_coverage.md"
}

generate_external_readiness() {
  local out_dir="${1:-$REPORT_DIR}"
  mkdir -p "$out_dir"
  run python3 - "$out_dir/external_readiness.md" "$out_dir/external_readiness.json" <<'PY'
import datetime as dt
import json
import os
import shutil
import sys

md_path, json_path = sys.argv[1], sys.argv[2]

commands = ["docker", "spark-sql", "pyspark", "trino", "minio", "nessie", "mc", "go", "python3"]
spark_home = os.environ.get("SPARK_HOME", "").strip()
command_candidates = {
    "spark-sql": [
        os.environ.get("SPARK_SQL_BIN", "").strip(),
        os.path.join(spark_home, "bin", "spark-sql") if spark_home else "",
        "/tmp/mo-iceberg-pyspark/bin/spark-sql",
    ],
    "pyspark": [
        os.environ.get("PYSPARK_BIN", "").strip(),
        os.path.join(spark_home, "bin", "pyspark") if spark_home else "",
        "/tmp/mo-iceberg-pyspark/bin/pyspark",
    ],
}
profiles = [
    {
        "name": "credential-vending",
        "tests": ["ICE-TEST-124"],
        "required_env": [
            "MO_ICEBERG_CREDENTIAL_VENDING",
            "MO_ICEBERG_CATALOG_URI",
            "MO_ICEBERG_CREDENTIAL_VENDING_SCENARIOS",
            "MO_ICEBERG_MO_SQL_CMD",
            "MO_ICEBERG_SPARK_SQL_CMD",
            "MO_ICEBERG_CREDENTIAL_VENDING_EXPIRED_ERROR",
        ],
        "help": "Real catalog must vend short-lived object credentials and expose a readable table.",
    },
    {
        "name": "tier-b",
        "tests": ["ICE-TEST-130"],
        "required_env": [
            "MO_ICEBERG_PUBLIC_DATASET",
            "MO_ICEBERG_PUBLIC_DATASET_CATALOG_URI",
            "MO_ICEBERG_PUBLIC_DATASET_WAREHOUSE",
            "MO_ICEBERG_PUBLIC_DATASET_SCENARIOS",
            "MO_ICEBERG_PUBLIC_DATASET_MO_SQL_CMD",
            "MO_ICEBERG_PUBLIC_DATASET_OFFICIAL_SQL_CMD",
        ],
        "help": "Public dataset catalog and official client command are required.",
    },
    {
        "name": "tier-c",
        "tests": ["ICE-TEST-131", "ICE-TEST-132", "ICE-TEST-133", "ICE-TEST-134"],
        "required_env": [
            "MO_ICEBERG_SANDBOX",
            "MO_ICEBERG_TIER_C_CATALOG_URI",
            "MO_ICEBERG_TIER_C_WAREHOUSE",
            "MO_ICEBERG_TIER_C_SCENARIOS",
            "MO_ICEBERG_TIER_C_MO_SQL_CMD",
            "MO_ICEBERG_TIER_C_OFFICIAL_SQL_CMD",
        ],
        "optional_env": [
            "MO_ICEBERG_REMOTE_SIGNING",
            "MO_ICEBERG_TIER_C_SIGNER_URI",
            "MO_ICEBERG_SERVER_PLANNING",
        ],
        "help": "Polaris/Open Catalog/Gravitino sandbox plus official client command are required.",
    },
    {
        "name": "tier-d",
        "tests": ["ICE-TEST-135"],
        "required_env": [
            "MO_ICEBERG_NESR",
            "MO_ICEBERG_NESR_SCENARIOS",
            "MO_ICEBERG_NESR_MO_SQL_CMD",
            "MO_ICEBERG_NESR_EXTERNAL_SQL_CMD",
            "MO_ICEBERG_NESR_EXPECTED_KPI",
            "MO_ICEBERG_NESR_RESIDENCY_ERROR",
        ],
        "help": "NESR de-identified demo data and expected KPI file are required.",
    },
    {
        "name": "golden-real",
        "tests": ["ICE-TEST-156", "ICE-TEST-157", "ICE-TEST-158", "ICE-TEST-159"],
        "required_env": [
            "MO_ICEBERG_CROSS_ENGINE",
            "MO_ICEBERG_GOLDEN_REAL_SCENARIOS",
            "MO_ICEBERG_MO_SQL_CMD",
            "MO_ICEBERG_SPARK_SQL_CMD",
            "MO_ICEBERG_TRINO_SQL_CMD",
        ],
        "help": "Spark/Iceberg-generated real files and MO/Spark/Trino commands are required.",
    },
]


def env_status(key):
    value = os.environ.get(key, "").strip()
    if not value:
        return {"name": key, "present": False, "value": ""}
    redacted = "<set>"
    if key.endswith("_SCENARIOS") or key.endswith("_EXPECTED_KPI"):
        redacted = value
    elif key.endswith("_CMD"):
        redacted = value.split()[0] + " ..." if value.split() else "<set>"
    return {"name": key, "present": True, "value": redacted}


def file_ok(key):
    value = os.environ.get(key, "").strip()
    if not value:
        return False
    if key.endswith("_SCENARIOS") or key.endswith("_EXPECTED_KPI"):
        return os.path.isfile(value)
    return True


def resolve_command(cmd):
    path = shutil.which(cmd)
    if path:
        return path
    for candidate in command_candidates.get(cmd, []):
        if candidate and os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return ""


now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
command_status = [{"name": cmd, "path": resolve_command(cmd)} for cmd in commands]
profile_status = []
for profile in profiles:
    required = [env_status(key) for key in profile.get("required_env", [])]
    optional = [env_status(key) for key in profile.get("optional_env", [])]
    missing = [item["name"] for item in required if not item["present"]]
    bad_files = [item["name"] for item in required if item["present"] and not file_ok(item["name"])]
    ready = not missing and not bad_files
    profile_status.append({
        "name": profile["name"],
        "tests": profile["tests"],
        "ready": ready,
        "missing_env": missing,
        "missing_files": bad_files,
        "required_env": required,
        "optional_env": optional,
        "help": profile["help"],
    })

payload = {
    "generated_at_utc": now,
    "commands": command_status,
    "profiles": profile_status,
}
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, ensure_ascii=False)
    f.write("\n")

with open(md_path, "w", encoding="utf-8") as out:
    out.write("# Iceberg External Test Readiness\n\n")
    out.write(f"generated_at_utc: `{now}`\n\n")
    out.write("Runbook: `test/iceberg/README.md`\n\n")
    out.write("Environment template: `test/iceberg/external_profiles.env.example`\n\n")
    out.write("## Commands\n\n")
    out.write("| command | status | path |\n")
    out.write("| --- | --- | --- |\n")
    for item in command_status:
        status = "ready" if item["path"] else "missing"
        out.write(f"| `{item['name']}` | {status} | `{item['path']}` |\n")
    out.write("\n## Profiles\n\n")
    out.write("| profile | tests | readiness | missing env | missing files | note |\n")
    out.write("| --- | --- | --- | --- | --- | --- |\n")
    for profile in profile_status:
        ready = "ready" if profile["ready"] else "not ready"
        tests = ", ".join(f"`{test}`" for test in profile["tests"])
        missing = ", ".join(f"`{key}`" for key in profile["missing_env"]) or "-"
        bad_files = ", ".join(f"`{key}`" for key in profile["missing_files"]) or "-"
        out.write(f"| `{profile['name']}` | {tests} | {ready} | {missing} | {bad_files} | {profile['help']} |\n")
    out.write("\nProfiles marked `not ready` are intentionally left open in the test plan until a real environment produces report artifacts.\n")
PY
  log "external readiness report written to ${out_dir}/external_readiness.md"
}

usage() {
  cat <<'USAGE'
Usage: optools/iceberg_ci.bash <profile>

Profiles:
  core        Run L0/L1 Iceberg package tests with matrixone_test tag.
  embedded   Run L2 in-process embedded Iceberg SQL tests.
  adapter    Run the nested iceberg-go adapter module with -tags iceberggo.
  golden     Regenerate/check golden vectors and provenance.
  external-templates Validate external profile scenario templates for credential-vending/Tier B/C/D.
  coverage   Enforce Iceberg coverage thresholds.
  preflight  Fail-closed external environment checks when MO_ICEBERG_CI_PROFILE is set.
  artifact   Verify report artifacts under MO_ICEBERG_REPORT_DIR.
  external-coverage Verify external reports cover the expected open ICE-TEST ids.
  dashboard  Generate CAP coverage dashboard under MO_ICEBERG_REPORT_DIR.
  readiness  Generate external profile readiness report under MO_ICEBERG_REPORT_DIR.
  e2e-local  Run local Nessie/MinIO/MO Iceberg E2E smoke without Spark.
  golden-real Run real-file golden vector cross-engine scenarios from MO_ICEBERG_GOLDEN_REAL_SCENARIOS.
  nightly    Run preflight, enabled external tests, artifact check, golden, dashboard.
  local      Run core, embedded, adapter, golden, coverage, dashboard.
USAGE
}

cd "$ROOT_DIR"
require_cmd go

case "$PROFILE" in
  core) go_test_core ;;
  embedded) go_test_embedded ;;
  adapter) go_test_adapter ;;
  golden) require_cmd python3; go_test_golden ;;
  external-templates) require_cmd python3; validate_external_templates ;;
  coverage) go_test_coverage ;;
  preflight) preflight ;;
  artifact) verify_report_artifacts "$REPORT_DIR" ;;
  external-coverage) require_cmd python3; verify_external_coverage "$REPORT_DIR" ;;
  dashboard) generate_cap_dashboard "$REPORT_DIR" ;;
  readiness) generate_external_readiness "$REPORT_DIR" ;;
  e2e-local) iceberg_e2e_local ;;
  golden-real)
    export MO_ICEBERG_CI_PROFILE="${MO_ICEBERG_CI_PROFILE:-golden-real}"
    preflight
    run_golden_real_if_enabled
    verify_report_artifacts "$REPORT_DIR"
    verify_external_coverage "$REPORT_DIR"
    ;;
  nightly)
    preflight
    run_tier_a_if_enabled
    run_cross_engine_if_enabled
    run_golden_real_if_enabled
    run_remaining_external_if_enabled
    go_test_golden
    generate_cap_dashboard "$REPORT_DIR"
    generate_external_readiness "$REPORT_DIR"
    if [[ -d "$REPORT_DIR" ]] && find "$REPORT_DIR" -mindepth 1 -maxdepth 1 -type d | grep -q .; then
      verify_report_artifacts "$REPORT_DIR"
      if external_coverage_profile_enabled; then
        verify_external_coverage "$REPORT_DIR"
      fi
    else
      log "no external report cases found under $REPORT_DIR; artifact verification skipped"
    fi
    ;;
  local)
    go_test_core
    go_test_embedded
    go_test_adapter
    go_test_golden
    validate_external_templates
    go_test_coverage
    generate_cap_dashboard "$REPORT_DIR"
    generate_external_readiness "$REPORT_DIR"
    ;;
  -h|--help|help) usage ;;
  *) usage; die "unknown profile: $PROFILE" ;;
esac
