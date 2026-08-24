#!/bin/bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
analyzer="${script_dir}/active_ut_cases.awk"
test_dir=$(mktemp -d "${TMPDIR:-/tmp}/active-ut-cases.XXXXXX")
trap 'rm -rf "${test_dir}"' EXIT

function assert_report() {
    local name=$1
    local input=$2
    local expected=$3
    local actual="${test_dir}/${name}.actual"

    awk -f "${analyzer}" "${input}" | LC_ALL=C sort > "${actual}"
    if ! diff -u "${expected}" "${actual}"; then
        echo "active UT case analyzer test failed: ${name}" >&2
        exit 1
    fi
}

cat > "${test_dir}/mixed.json" <<'EOF'
{"Time":"2026-08-10T01:00:00Z","Action":"start","Package":"example/active"}
{"Time":"2026-08-10T01:00:01Z","Action":"run","Package":"example/active","Test":"TestDone"}
{"Time":"2026-08-10T01:00:02Z","Action":"pass","Package":"example/active","Test":"TestDone"}
{"Time":"2026-08-10T01:00:03Z","Action":"run","Package":"example/active","Test":"TestBlocked/subcase"}
{"Time":"2026-08-10T01:00:04Z","Action":"pause","Package":"example/active","Test":"TestBlocked/subcase"}
{"Time":"2026-08-10T01:00:05Z","Action":"output","Package":"example/active","Test":"TestBlocked/subcase","Output":"contains \\"Action\\":\\"pass\\" but is not an event"}
{"Time":"2026-08-10T01:00:06Z","Action":"start","Package":"example/setup"}
{"Time":"2026-08-10T01:00:07Z","Action":"start","Package":"example/failed"}
{"Time":"2026-08-10T01:00:08Z","Action":"run","Package":"example/failed","Test":"TestTimedOut"}
{"Time":"2026-08-10T01:00:09Z","Action":"fail","Package":"example/failed"}
{"Time":"2026-08-10T01:00:10Z","Action":"start","Package":"example/reused"}
{"Time":"2026-08-10T01:00:11Z","Action":"run","Package":"example/reused","Test":"TestFirst"}
{"Time":"2026-08-10T01:00:12Z","Action":"pass","Package":"example/reused","Test":"TestFirst"}
{"Time":"2026-08-10T01:00:13Z","Action":"pass","Package":"example/reused"}
{"Time":"2026-08-10T01:00:14Z","Action":"start","Package":"example/reused"}
{"Time":"2026-08-10T01:00:15Z","Action":"run","Package":"example/reused","Test":"TestSecond"}
EOF

cat > "${test_dir}/mixed.expected" <<'EOF'
active UT case: package=example/active test=TestBlocked/subcase state=pause started=2026-08-10T01:00:03Z
active UT case: package=example/failed test=TestTimedOut state=run started=2026-08-10T01:00:08Z
active UT case: package=example/reused test=TestSecond state=run started=2026-08-10T01:00:15Z
active UT package (no active case event): package=example/setup started=2026-08-10T01:00:06Z
EOF
assert_report "mixed" "${test_dir}/mixed.json" "${test_dir}/mixed.expected"

cat > "${test_dir}/complete.json" <<'EOF'
{"Time":"2026-08-10T02:00:00Z","Action":"start","Package":"example/complete"}
{"Time":"2026-08-10T02:00:01Z","Action":"run","Package":"example/complete","Test":"TestComplete"}
{"Time":"2026-08-10T02:00:02Z","Action":"pass","Package":"example/complete","Test":"TestComplete"}
{"Time":"2026-08-10T02:00:03Z","Action":"pass","Package":"example/complete"}
EOF

cat > "${test_dir}/complete.expected" <<'EOF'
no active or incomplete UT package/test case found
EOF
assert_report "complete" "${test_dir}/complete.json" "${test_dir}/complete.expected"

cat > "${test_dir}/escaped.json" <<'EOF'
{"Time":"2026-08-10T03:00:00Z","Action":"start","Package":"example/escaped"}
{"Time":"2026-08-10T03:00:01Z","Action":"run","Package":"example/escaped","Test":"TestName/quote_\"_slash_\\"}
EOF

cat > "${test_dir}/escaped.expected" <<'EOF'
active UT case: package=example/escaped test=TestName/quote_\"_slash_\\ state=run started=2026-08-10T03:00:01Z
EOF
assert_report "escaped" "${test_dir}/escaped.json" "${test_dir}/escaped.expected"

mkdir "${test_dir}/timeout-package"
cat > "${test_dir}/timeout-package/go.mod" <<'EOF'
module example/timeout

go 1.22
EOF
cat > "${test_dir}/timeout-package/timeout_test.go" <<'EOF'
package timeout

import (
	"testing"
	"time"
)

func TestBlocked(t *testing.T) {
	time.Sleep(time.Hour)
}

func TestPass(t *testing.T) {}
EOF

(
    cd "${test_dir}/timeout-package"
    go test -json -count=1 -run '^TestPass$' ./...
) > "${test_dir}/pass.json"
awk -f "${analyzer}" "${test_dir}/pass.json" | LC_ALL=C sort > "${test_dir}/pass.actual"
if ! grep -Fxq 'no active or incomplete UT package/test case found' "${test_dir}/pass.actual"; then
    echo "real successful go test left a false active case" >&2
    cat "${test_dir}/pass.actual" >&2
    exit 1
fi

set +e
(
    cd "${test_dir}/timeout-package"
    go test -json -count=1 -timeout=100ms -run '^TestBlocked$' ./...
) > "${test_dir}/timeout.json"
timeout_status=$?
set -e
if (( timeout_status == 0 )); then
    echo "real go test timeout unexpectedly succeeded" >&2
    exit 1
fi

awk -f "${analyzer}" "${test_dir}/timeout.json" | LC_ALL=C sort > "${test_dir}/timeout.actual"
if ! grep -Fq 'active UT case: package=example/timeout test=TestBlocked ' "${test_dir}/timeout.actual"; then
    echo "real go test timeout did not identify TestBlocked" >&2
    cat "${test_dir}/timeout.actual" >&2
    exit 1
fi

if command -v timeout > /dev/null; then
    cat > "${test_dir}/termination.bash" <<'EOF'
#!/bin/bash

function on_termination() {
    trap - TERM
    echo "termination handler ran"
    awk -f "${ACTIVE_UT_ANALYZER}" "${ACTIVE_UT_REPORT}"
    exit 143
}

trap on_termination TERM
sleep 10
EOF
    cat > "${test_dir}/termination.json" <<'EOF'
{"Time":"2026-08-10T04:00:00Z","Action":"start","Package":"example/signal"}
{"Time":"2026-08-10T04:00:01Z","Action":"run","Package":"example/signal","Test":"TestSignalBlocked"}
EOF

    set +e
    termination_output=$(
        ACTIVE_UT_ANALYZER="${analyzer}" \
        ACTIVE_UT_REPORT="${test_dir}/termination.json" \
            timeout 1s bash "${test_dir}/termination.bash" 2>&1
    )
    termination_status=$?
    set -e
    if (( termination_status != 124 )); then
        echo "GNU timeout status changed by termination diagnostics: ${termination_status}" >&2
        echo "${termination_output}" >&2
        exit 1
    fi
    if ! grep -Fq 'active UT case: package=example/signal test=TestSignalBlocked ' <<< "${termination_output}"; then
        echo "termination diagnostics did not identify TestSignalBlocked" >&2
        echo "${termination_output}" >&2
        exit 1
    fi
fi

echo "active UT case analyzer tests passed"
