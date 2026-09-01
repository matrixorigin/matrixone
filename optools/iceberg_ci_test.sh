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
  mkdir -p "$dir"/{bin,tmp,optools,etc/launch-minio-local}
  cp "$ROOT_DIR/$SCRIPT_REL" "$dir/$SCRIPT_REL"
  for config in log tn cn; do
    printf 'data-dir = "./etc/launch-minio-local/mo-data"\n' >"$dir/etc/launch-minio-local/$config.toml"
  done
  cat >"$dir/bin/go" <<'EOF'
#!/usr/bin/env bash
if [[ "${1:-}" == run ]]; then
  : >"$GO_RUN_MARKER"
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
  printf '#!/usr/bin/env bash\nexit 24\n' >"$bin"
  chmod +x "$bin"
fi
EOF
  chmod +x "$dir/bin"/{go,python3,docker,make} "$dir/$SCRIPT_REL"
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
