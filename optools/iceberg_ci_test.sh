#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_REL="optools/iceberg_ci.bash"

fail() {
  printf 'iceberg_ci_test: %s\n' "$*" >&2
  exit 1
}

make_fixture() {
  local dir="$1"
  mkdir -p "$dir"/{bin,tmp,optools,etc/launch-minio-local,pkg/iceberg/adapter/iceberggo}
  cp "$ROOT_DIR/$SCRIPT_REL" "$dir/$SCRIPT_REL"
  for config in log tn cn; do
    printf 'data-dir = "./etc/launch-minio-local/mo-data"\n' >"$dir/etc/launch-minio-local/$config.toml"
  done
  cat >"$dir/bin/go" <<'EOF'
#!/usr/bin/env bash
if [[ "${1:-}" == run ]]; then
  : >"$GO_RUN_MARKER"
  for ((i = 1; i <= $#; i++)); do
    if [[ "${!i}" == "--report-dir" ]]; then
      report_index=$((i + 1))
      report_dir="${!report_index}"
      mkdir -p "$report_dir/success"
      printf '{}\n' >"$report_dir/success/metadata.json"
      printf '{}\n' >"$report_dir/success/diff.json"
      printf 'success\n' >"$report_dir/success/summary.md"
      break
    fi
  done
fi
if [[ "${ICEBERG_CI_TEST_MODE}" == adapter-failure && "$*" == *"-tags iceberggo"* ]]; then
  exit 23
fi
EOF
  cat >"$dir/bin/python3" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  cat >"$dir/bin/docker" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  cat >"$dir/bin/curl" <<'EOF'
#!/usr/bin/env bash
if [[ "${ICEBERG_CI_TEST_MODE:-}" == child-failure ]]; then
  exit 1
fi
exit 0
EOF
  cat >"$dir/bin/rm" <<'EOF'
#!/usr/bin/env bash
for arg in "$@"; do
  if [[ "${ICEBERG_CI_TEST_MODE:-}" == cleanup-failure && "$arg" == *"/mo-iceberg-e2e-local."* ]]; then
    printf 'injected temporary directory removal failure: %s\n' "$arg" >&2
    exit 31
  fi
done
exec /bin/rm "$@"
EOF
  cat >"$dir/bin/make" <<'EOF'
#!/usr/bin/env bash
for arg in "$@"; do
  case "$arg" in
    BIN_NAME=*) bin="${arg#BIN_NAME=}" ;;
  esac
done
if [[ "${ICEBERG_CI_TEST_MODE}" == build-failure && "${1:-}" == build ]]; then
  exit 22
fi
if [[ -n "${bin:-}" ]]; then
  if [[ "${ICEBERG_CI_TEST_MODE}" == cleanup-failure ]]; then
    printf '#!/usr/bin/env bash\nwhile :; do sleep 1; done\n' >"$bin"
  else
    printf '#!/usr/bin/env bash\nexit 24\n' >"$bin"
  fi
  chmod +x "$bin"
fi
EOF
  chmod +x "$dir/bin"/{go,python3,docker,curl,rm,make} "$dir/$SCRIPT_REL"
}

run_failure_case() {
  local mode="$1"
  local dir
  dir="$(mktemp -d "${TMPDIR:-/tmp}/iceberg-ci-test.XXXXXX")"
  trap 'rm -rf -- "$dir"' RETURN
  make_fixture "$dir"
  local output="$dir/output.log"
  if (
    cd "$dir"
    PATH="$dir/bin:$PATH" \
      TMPDIR="$dir/tmp" \
      MO_ICEBERG_REPORT_DIR="$dir/reports" \
      ICEBERG_CI_TEST_MODE="$mode" \
      GO_RUN_MARKER="$dir/go-run" \
      "$dir/$SCRIPT_REL" e2e-local
  ) >"$output" 2>&1; then
    cat "$output" >&2
    fail "$mode unexpectedly succeeded"
  fi
  if find "$dir/tmp" -maxdepth 1 -type d -name 'mo-iceberg-e2e-local.*' | grep -q .; then
    cat "$output" >&2
    fail "$mode left an owned E2E temporary directory"
  fi
  if [[ "$mode" == child-failure && -e "$dir/go-run" ]]; then
    cat "$output" >&2
    fail "child failure continued to the E2E client"
  fi
}

run_failure_case build-failure
run_failure_case adapter-failure
run_failure_case child-failure

run_cleanup_failure_case() {
  local dir
  dir="$(mktemp -d "${TMPDIR:-/tmp}/iceberg-ci-test.XXXXXX")"
  trap 'rm -rf -- "$dir"' RETURN
  make_fixture "$dir"
  local output="$dir/output.log"
  if (
    cd "$dir"
    PATH="$dir/bin:$PATH" \
      TMPDIR="$dir/tmp" \
      MO_ICEBERG_REPORT_DIR="$dir/reports" \
      ICEBERG_CI_TEST_MODE=cleanup-failure \
      GO_RUN_MARKER="$dir/go-run" \
      "$dir/$SCRIPT_REL" e2e-local
  ) >"$output" 2>&1; then
    cat "$output" >&2
    fail "cleanup failure unexpectedly preserved a successful exit"
  fi
  [[ -e "$dir/go-run" ]] || {
    cat "$output" >&2
    fail "cleanup failure did not run a successful E2E body"
  }
  grep -F 'Iceberg E2E cleanup failed (status 31); promoting successful body exit' "$output" >/dev/null || {
    cat "$output" >&2
    fail "cleanup failure did not emit its promotion diagnostic"
  }
}

run_cleanup_failure_case
