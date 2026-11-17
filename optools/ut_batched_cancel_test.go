// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package optools

import (
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

const batchedOwnerSetup = `source ./run_ut.sh UT
function logger() { :; }
function checkpoint_ut_event() { :; }
UT_TIMEOUT=1
LD_LIBRARY_PATH=''; CGO_CFLAGS=''; CGO_LDFLAGS=''
UT_REPORT="$CASE_DIR/authoritative.out"
UT_STDERR="$CASE_DIR/stderr.log"
PREBUILT_RACE_REPORT="$CASE_DIR/issues-report"
PREBUILT_RACE_TEST_BINARY="$CASE_DIR/issues.test"
: > "$UT_REPORT"; : > "$UT_STDERR"
: > "$CASE_DIR/fixture_test.go"
completion="$G_WKSP/$G_TS-owner-complete.CURRENT_UT_PID"
`

func injectBatchedHook(t *testing.T, text, anchor, replacement string) string {
	t.Helper()
	if count := strings.Count(text, anchor); count != 1 {
		t.Fatalf("batched test hook anchor count = %d, want 1", count)
	}
	return strings.Replace(text, anchor, replacement, 1)
}

// These synthetic children exercise the same parent drain/KILL path with a
// shorter grace. Existing ownership tests retain the production grace value.
func shortenBatchedOwnerGrace(t *testing.T, text string) string {
	t.Helper()
	const anchor = "        wait_for_ut_process_group \"${!pid_name}\" 20 || join_status=125\n"
	return injectBatchedHook(t, text, anchor, "        wait_for_ut_process_group \"${!pid_name}\" 4 || join_status=125\n")
}

// The real owner cancels while the inventory partitioner's sort is running.
// A failed partition is normally allowed to fall back to the complete go test,
// but that fallback must not start after the owner requested cancellation.
func TestBatchedPreparationCancellationDoesNotStartFallback(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version|env) exit 0 ;;
 list) printf '%s\texample/package\nfixture_test.go\n' "$CASE_DIR"; exit 0 ;;
 test)
  shift; compile=0; out=''
  while (( $# )); do
   [[ "$1" == -c ]] && compile=1
   if [[ "$1" == -o ]]; then out=$2; shift; fi
   shift
  done
  if (( compile )); then
   printf '#!/bin/bash\nprintf "TestOne\\nTestTwo\\n"\n' > "$out"
   chmod +x "$out"
   exit 0
  fi
  : > "$CASE_DIR/fallback-ran"
  printf '%s\n' '{"Action":"pass","Package":"example/package"}'
  exit 0 ;;
esac
exit 81
`
	script := `source ./run_ut.sh UT
function logger() { :; }
function checkpoint_ut_event() { :; }
function stop_ut_heartbeat() { :; }
function snapshot_ut_diagnostics() { :; }
function report_slow_ut_cases() { :; }
function report_active_ut_cases() { :; }
cat > "$CASE_DIR/sort" <<'EOF'
#!/bin/bash
: > "$CASE_DIR/sort-ready"
exec sleep 30
EOF
chmod +x "$CASE_DIR/sort"
export PATH="$CASE_DIR:$PATH"
UT_TIMEOUT=1
LD_LIBRARY_PATH=''; CGO_CFLAGS=''; CGO_LDFLAGS=''
UT_REPORT="$CASE_DIR/authoritative.out"
UT_STDERR="$CASE_DIR/stderr.log"
UT_CHECKPOINT="$CASE_DIR/checkpoint.log"
LOG="$CASE_DIR/runner.log"
PREBUILT_RACE_REPORT="$CASE_DIR/issues-report"
PREBUILT_RACE_TEST_BINARY="$CASE_DIR/issues.test"
mkdir -p "$UT_DIAGNOSTIC_DIR"
: > "$UT_REPORT"; : > "$UT_STDERR"; : > "$UT_CHECKPOINT"
: > "$CASE_DIR/fixture_test.go"
cleanup() {
 status=$?
 [[ "$status" == 143 || "$status" == 125 ]] || status=90
 [[ ! -e "$CASE_DIR/fallback-ran" ]] || status=91
 exit "$status"
}
trap cleanup EXIT
trap handle_ut_termination TERM
start_ut_command embedded 'batched mock' run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || exit 92
for ((i=0; i<200; i++)); do
 [[ -e "$CASE_DIR/sort-ready" ]] && break
 kill -0 "$CURRENT_UT_PID" 2>/dev/null || break
 sleep .025
done
[[ -e "$CASE_DIR/sort-ready" ]] || exit 93
kill -TERM "$$"
exit 94
`
	out, err := scheduleHarnessWithMock(t, script, mock)
	exit, ok := err.(*exec.ExitError)
	if !ok || (exit.ExitCode() != 143 && exit.ExitCode() != 125) {
		t.Fatalf("canceled preparation must not run the ordinary fallback: %v\n%s", err, out)
	}
}

// A persistent request must stop discovery even when TERM did not reach its
// shell. The inventory child has its own process group and must be drained.
func TestBatchedMarkerOnlyCancelsInventory(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version|env) exit 0 ;;
 list) printf '%s\texample/package\nfixture_test.go\n' "$CASE_DIR"; exit 0 ;;
 test)
  if [[ " $* " != *' -c '* ]]; then : > "$CASE_DIR/fallback"; exit 0; fi
  while (( $# )); do
   if [[ "$1" == -o ]]; then out=$2; break; fi
   shift
  done
  cat > "$out" <<'EOF'
#!/bin/bash
printf '%s\n' "$$" > "$CASE_DIR/inventory-pid"
: > "$CASE_DIR/inventory-ready"
exec sleep 30
EOF
  chmod +x "$out"; exit 0 ;;
esac
exit 81
`
	script := batchedOwnerSetup + `
start_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || exit 90
for ((i=0; i<300; i++)); do
 [[ -e "$CASE_DIR/inventory-ready" ]] && break
 kill -0 "$CURRENT_UT_PID" 2>/dev/null || exit 91
 sleep .02
done
[[ -e "$CASE_DIR/inventory-ready" ]] || exit 92
read -r inventory_pid < "$CASE_DIR/inventory-pid"
: > "$completion.cancel"
status=0; finish_ut_command || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 ]] || exit 93
[[ -e "$completion.cancel" && ! -s "$completion" && ! -e "$CASE_DIR/fallback" ]] || exit 94
[[ -e "$PREBUILT_RACE_TEST_BINARY" && -e "$PREBUILT_RACE_REPORT" ]] || exit 95
if ut_process_group_alive "$inventory_pid"; then exit 96; fi
`
	out, err := scheduleHarnessWithMock(t, script, mock)
	if err != nil {
		t.Fatalf("marker-only inventory cancellation: %v\n%s", err, out)
	}
}

// Each preparation step can finish successfully after cancellation arrived.
// No later list, build, inventory, or fallback may then be admitted.
func TestBatchedCancellationStopsPreparationAdmission(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
stage=${1:-}
case "$stage" in
 version) exit 0 ;;
 env|list) ;;
 test)
  if [[ " $* " != *' -c '* ]]; then : > "$CASE_DIR/fallback"; exit 0; fi
  stage=compile
  while (( $# )); do
   if [[ "$1" == -o ]]; then out=$2; break; fi
   shift
  done
  cat > "$out" <<'EOF'
#!/bin/bash
: > "$CASE_DIR/inventory-ran"
printf 'TestOne\n'
EOF
  chmod +x "$out" ;;
 *) exit 81 ;;
esac
printf '%s\n' "$stage" >> "$CASE_DIR/stages"
if [[ "$stage" == "$CANCEL_STAGE" ]]; then
 : > "$CASE_DIR/stage-ready"
 while [[ ! -e "$CASE_DIR/stage-release" ]]; do sleep .02; done
fi
if [[ "$stage" == list ]]; then
 printf '%s\texample/package\nfixture_test.go\n' "$CASE_DIR"
fi
exit 0
`
	for _, stage := range []string{"env", "list", "compile"} {
		for _, signal := range []string{"marker", "term"} {
			t.Run(stage+"_"+signal, func(t *testing.T) {
				script := batchedOwnerSetup + `
start_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || exit 90
for ((i=0; i<300; i++)); do
 [[ -e "$CASE_DIR/stage-ready" ]] && break
 kill -0 "$CURRENT_UT_PID" 2>/dev/null || exit 91
 sleep .02
done
[[ -e "$CASE_DIR/stage-ready" ]] || exit 92
if [[ "$CANCEL_SIGNAL" == marker ]]; then
 : > "$completion.cancel"
else
 kill -TERM "$CURRENT_UT_PID"
fi
: > "$CASE_DIR/stage-release"
status=0; finish_ut_command || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 && ! -s "$completion" ]] || exit 93
[[ ! -e "$CASE_DIR/inventory-ran" && ! -e "$CASE_DIR/fallback" ]] || exit 94
case "$CANCEL_STAGE" in
 env) ! grep -Eq '^(list|compile)$' "$CASE_DIR/stages" || exit 95 ;;
 list) ! grep -q '^compile$' "$CASE_DIR/stages" || exit 96 ;;
esac
`
				out, err := scheduleHarnessWithMock(t, script, mock, "CANCEL_STAGE="+stage, "CANCEL_SIGNAL="+signal)
				if err != nil {
					t.Fatalf("canceled %s must not admit its successor: %v\n%s", stage, err, out)
				}
			})
		}
	}
}

func TestBatchedMarkerStopsTestMainScan(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version|env) exit 0 ;;
 list) printf '%s\texample/package\nfirst_test.go\nsecond_test.go\n' "$CASE_DIR"; exit 0 ;;
 test) : > "$CASE_DIR/unexpected-compile"; exit 81 ;;
esac
exit 82
`
	script := batchedOwnerSetup + `
: > "$CASE_DIR/first_test.go"; : > "$CASE_DIR/second_test.go"
cat > "$CASE_DIR/grep" <<'EOF'
#!/bin/bash
if [[ -e "$CASE_DIR/grep-ready" ]]; then : > "$CASE_DIR/second-grep"; exit 1; fi
: > "$CASE_DIR/grep-ready"
while [[ ! -e "$CASE_DIR/grep-release" ]]; do sleep .02; done
exit 1
EOF
chmod +x "$CASE_DIR/grep"
export PATH="$CASE_DIR:$PATH"
start_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || exit 90
for ((i=0; i<300; i++)); do
 [[ -e "$CASE_DIR/grep-ready" ]] && break
 kill -0 "$CURRENT_UT_PID" 2>/dev/null || exit 91
 sleep .02
done
[[ -e "$CASE_DIR/grep-ready" ]] || exit 92
: > "$completion.cancel"; : > "$CASE_DIR/grep-release"
status=0; finish_ut_command || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 && ! -s "$completion" ]] || exit 93
[[ ! -e "$CASE_DIR/second-grep" && ! -e "$CASE_DIR/unexpected-compile" ]] || exit 94
`
	out, err := scheduleHarnessWithMock(t, script, mock)
	if err != nil {
		t.Fatalf("canceled TestMain scan admitted further work: %v\n%s", err, out)
	}
}

func TestBatchedTERMBeforeInventoryFork(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version|env) exit 0 ;;
 list) printf '%s\texample/package\nfixture_test.go\n' "$CASE_DIR"; exit 0 ;;
 test)
  if [[ " $* " != *' -c '* ]]; then : > "$CASE_DIR/fallback"; exit 0; fi
  while (( $# )); do
   if [[ "$1" == -o ]]; then out=$2; break; fi
   shift
  done
  printf '#!/bin/bash\n: > "$CASE_DIR/inventory-ran"\nprintf "TestOne\\n"\n' > "$out"
  chmod +x "$out"; exit 0 ;;
esac
exit 81
`
	transform := func(text string) string {
		const anchor = "    trap 'term_pending=1' TERM\n    if (( term_pending != 0 )) || batched_race_cancel_requested; then\n"
		return injectBatchedHook(t, text, anchor, "    trap 'term_pending=1' TERM\n    fixture_self_pid fixture_owner_pid\n    kill -TERM \"$fixture_owner_pid\"\n    if (( term_pending != 0 )) || batched_race_cancel_requested; then\n")
	}
	script := batchedOwnerSetup + `
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 && ! -s "$completion" ]] || exit 90
[[ ! -e "$CASE_DIR/inventory-ran" && ! -e "$CASE_DIR/fallback" ]] || exit 91
`
	out, err := scheduleHarnessWithMockTransform(t, script, mock, transform)
	if err != nil {
		t.Fatalf("TERM before inventory fork: %v\n%s", err, out)
	}
}

// A killed helper cannot leave its fallback outside the group still owned by
// the actual parent. The child deliberately ignores TERM to exercise KILL.
func TestBatchedKilledHelperDoesNotOrphanFallback(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 env) [[ "$MODE" != early ]]; exit $? ;;
 list) printf '%s\texample/package\nfixture_test.go\n' "$CASE_DIR"; exit 0 ;;
 test)
  if [[ " $* " == *' -c '* ]]; then
   while (( $# )); do
    if [[ "$1" == -o ]]; then out=$2; break; fi
    shift
   done
   printf '#!/bin/bash\nprintf "TestOne\\nTestOne\\n"\n' > "$out"
   chmod +x "$out"; exit 0
  fi
  trap '' TERM
  fixture_self_pid owner
  printf '%s\n' "$owner" > "$CASE_DIR/fallback-pid"
  : > "$CASE_DIR/fallback-ready"
  exec sleep 30 ;;
esac
exit 81
`
	for _, mode := range []string{"early", "partition"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			script := batchedOwnerSetup + `
start_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 || exit 90
owner_pid=$CURRENT_UT_PID
for ((i=0; i<300; i++)); do
 [[ -e "$CASE_DIR/fallback-ready" ]] && break
 kill -0 "$owner_pid" 2>/dev/null || exit 91
 sleep .02
done
[[ -e "$CASE_DIR/fallback-ready" ]] || exit 92
read -r fallback_pid < "$CASE_DIR/fallback-pid"
fallback_group=$(ps -o pgid= -p "$fallback_pid" | tr -d '[:space:]')
[[ "$fallback_group" == "$owner_pid" ]] || exit 93
kill -KILL "$owner_pid"
status=0; finish_ut_command || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 && ! -s "$completion" ]] || exit 94
if ut_process_group_alive "$owner_pid"; then exit 95; fi
`
			transform := func(text string) string { return shortenBatchedOwnerGrace(t, text) }
			out, err := scheduleHarnessWithMockTransform(t, script, mock, transform, "MODE="+mode)
			if err != nil {
				t.Fatalf("killed helper left a fallback behind (%s): %v\n%s", mode, err, out)
			}
		})
	}
}

func TestBatchedFallbackPreservesStdin(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 env) exit 1 ;;
 test)
  IFS= read -r line || exit 91
  printf '%s\n' "$line" > "$CASE_DIR/fallback-stdin"
  printf '%s\n' '{"Action":"pass","Package":"example/package"}'
  exit 0 ;;
esac
exit 81
`
	transform := func(text string) string {
		const anchor = "    ) 0<&0 &\n    pid=$!\n"
		return injectBatchedHook(t, text, anchor, anchor+"    : > \"$CASE_DIR/async-fallback\"\n")
	}
	for _, mode := range []string{"native", "other_platform"} {
		t.Run(mode, func(t *testing.T) {
			script := batchedOwnerSetup + `
if [[ "$MODE" == other_platform ]]; then OSTYPE=darwin-test; fi
printf 'test-input\n' > "$CASE_DIR/input"
exec 3< "$CASE_DIR/input"
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 <&3 || status=$?
[[ "$status" == 0 && "$(<"$CASE_DIR/fallback-stdin")" == test-input ]] || exit 90
if (( BASH_VERSINFO[0] >= 5 )) && [[ "$OSTYPE" == linux* ]]; then
 [[ -e "$CASE_DIR/async-fallback" ]] || exit 91
else
 [[ ! -e "$CASE_DIR/async-fallback" ]] || exit 92
fi
`
			out, err := scheduleHarnessWithMockTransform(t, script, mock, transform, "MODE="+mode)
			if err != nil {
				t.Fatalf("fallback changed inherited stdin (%s): %v\n%s", mode, err, out)
			}
		})
	}
}

func TestBatchedFallbackWithoutVerifiedOwner(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 env) exit 1 ;;
 test)
  : > "$CASE_DIR/fallback-ready"
  while [[ ! -e "$CASE_DIR/fallback-release" ]]; do sleep .02; done
  printf '%s\n' '{"Action":"pass","Package":"example/package"}'; exit 0 ;;
esac
exit 81
`
	for _, mode := range []string{"normal", "marker", "unknown_trap"} {
		t.Run(mode, func(t *testing.T) {
			script := batchedOwnerSetup + `
cat > "$CASE_DIR/ps" <<'EOF'
#!/bin/bash
if [[ "${1:-}" == -o && "${2:-}" == pgid= ]]; then
 : > "$CASE_DIR/owner-probe"
 if [[ "$MODE" != unknown_trap ]]; then exit 1; fi
fi
exec /bin/ps "$@"
EOF
chmod +x "$CASE_DIR/ps"
export PATH="$CASE_DIR:$PATH"
if [[ "$MODE" == unknown_trap ]]; then
 function trap() {
  if [[ "${1:-}" == -p && "${2:-}" == INT ]]; then printf 'unknown trap format\n'; else builtin trap "$@"; fi
 }
fi
function terminate_ut_process_group() { : > "$CASE_DIR/unexpected-broadcast"; }
start_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 || exit 90
for ((i=0; i<300; i++)); do
 [[ -e "$CASE_DIR/fallback-ready" ]] && break
 kill -0 "$CURRENT_UT_PID" 2>/dev/null || exit 91
 sleep .02
done
[[ -e "$CASE_DIR/fallback-ready" && -e "$CASE_DIR/owner-probe" ]] || exit 92
if [[ "$MODE" == marker ]]; then : > "$completion.cancel"; fi
: > "$CASE_DIR/fallback-release"
status=0; finish_ut_command || status=$?
if [[ "$MODE" == marker ]]; then
 [[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 ]] || exit 93
else
 [[ "$status" == 0 && "$CURRENT_UT_DRAIN_FAILED" == 0 ]] || exit 94
fi
[[ ! -e "$CASE_DIR/unexpected-broadcast" ]] || exit 95
[[ "$(grep -c '"Action":"pass"' "$UT_REPORT")" == 1 ]] || exit 96
`
			out, err := scheduleHarnessWithMock(t, script, mock, "MODE="+mode)
			if err != nil {
				t.Fatalf("unverified owner (%s): %v\n%s", mode, err, out)
			}
		})
	}
}

// The dispatcher's trap replaces the preparation trap. An intervening TERM
// must remain visible before any test process is admitted.
func TestBatchedCancellationAtDispatcherHandoff(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version|env) exit 0 ;;
 list) printf '%s\texample/package\nfixture_test.go\n' "$CASE_DIR"; exit 0 ;;
 test)
  while (( $# )); do
   if [[ "$1" == -o ]]; then out=$2; break; fi
   shift
  done
  printf '#!/bin/bash\nprintf "TestOne\\nTestTwo\\n"\n' > "$out"
  chmod +x "$out"; exit 0 ;;
 tool) : > "$CASE_DIR/test-started"; exit 0 ;;
esac
exit 81
`
	transform := func(text string) string {
		const anchor = "    previous_term_trap=$(trap -p TERM)\n    function stop_prebuilt_race_commands(){"
		return injectBatchedHook(t, text, anchor, "    fixture_self_pid fixture_owner_pid\n    kill -TERM \"$fixture_owner_pid\"\n"+anchor)
	}
	script := batchedOwnerSetup + `
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || status=$?
[[ "$status" == 143 && "$CURRENT_UT_DRAIN_FAILED" == 0 ]] || exit 90
[[ "$(<"$completion")" == 143 && ! -e "$CASE_DIR/test-started" ]] || exit 91
`
	out, err := scheduleHarnessWithMockTransform(t, script, mock, transform)
	if err != nil {
		t.Fatalf("dispatcher cancellation handoff: %v\n%s", err, out)
	}
}

func TestBatchedCancellationAtDispatcherReturn(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version|env) exit 0 ;;
 list) printf '%s\texample/package\nfixture_test.go\n' "$CASE_DIR"; exit 0 ;;
 test)
  while (( $# )); do
   if [[ "$1" == -o ]]; then out=$2; break; fi
   shift
  done
  printf '#!/bin/bash\nprintf "TestOne\\nTestTwo\\n"\n' > "$out"
  chmod +x "$out"; exit 0 ;;
 tool) printf 'started\n' >> "$CASE_DIR/test-started"; exit 0 ;;
esac
exit 81
`
	for _, when := range []string{"before_restore", "after_restore"} {
		t.Run(when, func(t *testing.T) {
			transform := func(text string) string {
				const anchor = "    restore_ut_term_trap \"${previous_term_trap}\"\n    # TERM between the final poll"
				const signal = "    fixture_self_pid fixture_owner_pid\n    kill -TERM \"$fixture_owner_pid\"\n"
				replacement := signal + anchor
				if when == "after_restore" {
					replacement = "    restore_ut_term_trap \"${previous_term_trap}\"\n" + signal + "    # TERM between the final poll"
				}
				return injectBatchedHook(t, text, anchor, replacement)
			}
			script := batchedOwnerSetup + `
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || status=$?
[[ "$status" == 143 && "$CURRENT_UT_DRAIN_FAILED" == 0 ]] || exit 90
[[ "$(<"$completion")" == 143 ]] || exit 91
[[ "$(wc -l < "$CASE_DIR/test-started" | tr -d ' ')" == 2 ]] || exit 92
`
			out, err := scheduleHarnessWithMockTransform(t, script, mock, transform)
			if err != nil {
				t.Fatalf("dispatcher return cancellation at %s: %v\n%s", when, err, out)
			}
		})
	}
}

// A cancellation can arrive after the helper checked the marker but before
// it publishes success. The parent still owns the final acceptance decision.
func TestBatchedJoinRejectsRacingSuccess(t *testing.T) {
	t.Parallel()
	transform := func(text string) string {
		const anchor = "    publish_ut_helper_completion \"${helper_status}\" || return 125\n"
		return injectBatchedHook(t, text, anchor, "    if [[ \"${owner_name}\" == CURRENT_UT_PID && \"${helper_name}\" == run_race_test_batches ]]; then : > \"${UT_HELPER_COMPLETION}.cancel\"; fi\n"+anchor)
	}
	script := batchedOwnerSetup + `
function run_race_test_batches() { : > "$PREBUILT_RACE_REPORT"; }
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 ]] || exit 90
[[ "$(<"$completion")" == 0 && -e "$completion.cancel" ]] || exit 91
`
	out, err := scheduleHarnessWithMockTransform(t, script, scheduleHarnessMock(), transform)
	if err != nil {
		t.Fatalf("racing success must not be accepted: %v\n%s", err, out)
	}
}

// Direct TERM has no parent marker. Exercise each trap handoff from helper
// entry through completion publication, without letting later work start.
func TestBatchedDirectTERMAcrossHelperHandoffs(t *testing.T) {
	t.Parallel()
	const signal = "    fixture_self_pid fixture_owner_pid\n    kill -TERM \"$fixture_owner_pid\"\n"
	for _, tc := range []struct {
		name, anchor, replacement string
		stub                      bool
	}{
		{"before_wrapper", "        trap 'BATCHED_RACE_INVOKE_TERM_REQUESTED=1' TERM\n", "        trap 'BATCHED_RACE_INVOKE_TERM_REQUESTED=1' TERM\n" + signal, false},
		{"before_restore", "    restore_ut_term_trap \"${saved_batched_term_trap}\"\n", signal + "    restore_ut_term_trap \"${saved_batched_term_trap}\"\n", true},
		{"after_restore", "    restore_ut_term_trap \"${saved_batched_term_trap}\"\n", "    restore_ut_term_trap \"${saved_batched_term_trap}\"\n" + signal, true},
		{"after_helper", "    \"$@\" || helper_status=$?\n", "    \"$@\" || helper_status=$?\n" + signal, true},
		{"before_ack", "    publish_ut_helper_completion \"${helper_status}\" || return 125\n", signal + "    publish_ut_helper_completion \"${helper_status}\" || return 125\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transform := func(text string) string {
				return injectBatchedHook(t, text, tc.anchor, tc.replacement)
			}
			mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 *) : > "$CASE_DIR/unexpected-go"; exit 0 ;;
esac
`
			script := batchedOwnerSetup + `
if [[ "$STUB" == true ]]; then
 function run_race_test_batches_impl() { : > "$PREBUILT_RACE_REPORT"; }
fi
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 && ! -s "$completion" ]] || exit 90
[[ ! -e "$CASE_DIR/unexpected-go" ]] || exit 91
`
			out, err := scheduleHarnessWithMockTransform(t, script, mock, transform, "STUB="+strconv.FormatBool(tc.stub))
			if err != nil {
				t.Fatalf("direct TERM at %s: %v\n%s", tc.name, err, out)
			}
		})
	}
}

func TestBatchedCancellationRetainsSelectedFailure(t *testing.T) {
	t.Parallel()
	transform := func(text string) string {
		const anchor = "    publish_ut_helper_completion \"${helper_status}\" || return 125\n"
		return injectBatchedHook(t, text, anchor, "    fixture_self_pid fixture_owner_pid\n    kill -TERM \"$fixture_owner_pid\"\n"+anchor)
	}
	script := batchedOwnerSetup + `
function logger() { printf '%s %s\n' "$1" "$2" >&2; }
function run_race_test_batches_impl() { : > "$PREBUILT_RACE_REPORT"; return 7; }
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 && ! -s "$completion" ]] || exit 90
grep -q 'batched UT owner selected status 7' "$UT_STDERR" || exit 91
`
	out, err := scheduleHarnessWithMockTransform(t, script, scheduleHarnessMock(), transform)
	if err != nil {
		t.Fatalf("late cancellation lost a selected failure: %v\n%s", err, out)
	}
}

// An unacknowledged owner keeps its reports. Expose hard links for the bounded
// artifact collector, but never merge live or partial bytes into UT_REPORT.
func TestBatchedUnjoinedDiagnosticsStaySeparate(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"current", "prebuild", "link_failure", "archive_failure", "nested_archive", "unknown_archive"} {
		t.Run(mode, func(t *testing.T) {
			script := batchedOwnerSetup + `
UT_DIAGNOSTIC_DIR="$CASE_DIR/diagnostics"
UT_CHECKPOINT="$CASE_DIR/checkpoint"
function logger() { printf '%s %s\n' "$1" "$2" >> "$CASE_DIR/messages"; }
if [[ "$MODE" == prebuild ]]; then CLUSTER_PREBUILD_DRAIN_FAILED=1; else CURRENT_UT_DRAIN_FAILED=1; fi
printf 'authoritative\n' > "$UT_REPORT"
printf 'checkpoint\n' > "$UT_CHECKPOINT"
printf 'base\n' > "$PREBUILT_RACE_REPORT"
printf 'shard-before\n' > "$PREBUILT_RACE_REPORT.00"
printf 'third-digit\n' > "$PREBUILT_RACE_REPORT.100"
ln -s "$PREBUILT_RACE_REPORT.00" "$PREBUILT_RACE_REPORT.01"
if [[ "$MODE" == link_failure ]]; then function ln() { return 1; }; fi
snapshot_ut_diagnostics; snapshot_ut_diagnostics
[[ -e "$PREBUILT_RACE_REPORT" && -e "$PREBUILT_RACE_REPORT.00" ]] || exit 90
[[ "$(<"$UT_DIAGNOSTIC_DIR/ut-report.json")" == authoritative ]] || exit 91
links=("$UT_DIAGNOSTIC_DIR"/live-prebuilt-*/live.00.jsonl)
if [[ "$MODE" == link_failure ]]; then
 [[ ! -e "${links[0]}" ]] || exit 92
 grep -q 'original remains at' "$CASE_DIR/messages" || exit 93
else
 [[ ${#links[@]} == 1 && "${links[0]}" -ef "$PREBUILT_RACE_REPORT.00" ]] || exit 94
 [[ "${links[0]%/*}/live.100.jsonl" -ef "$PREBUILT_RACE_REPORT.100" ]] || exit 97
 [[ ! -e "${links[0]%/*}/live.01.jsonl" ]] || exit 98
 grep -q 'non-authoritative' "${links[0]%/*}/origin.txt" || exit 99
 printf 'shard-after\n' >> "$PREBUILT_RACE_REPORT.00"
 grep -q 'shard-after' "${links[0]}" || exit 95
 ! grep -q 'shard-' "$UT_DIAGNOSTIC_DIR/ut-report.json" || exit 96
 old_directory=${links[0]%/*}
 if [[ "$MODE" == archive_failure ]]; then function python3() { return 1; }; fi
 if [[ "$MODE" == nested_archive ]]; then
  mkdir -p "$CASE_DIR/nested-archive"
  ln -s "$UT_DIAGNOSTIC_DIR" "$CASE_DIR/nested-archive/scratch"
  BUILD_WKSP="$CASE_DIR/nested-archive"
 fi
 if [[ "$MODE" == unknown_archive ]]; then printf 'keep\n' > "$old_directory/unknown-UT.log"; fi
 status=0; archive_previous_live_prebuilt_diagnostics || status=$?
 if [[ "$MODE" == archive_failure || "$MODE" == nested_archive || "$MODE" == unknown_archive ]]; then
  [[ "$status" != 0 && -e "$old_directory/live.00.jsonl" ]] || exit 100
 else
  [[ "$status" == 0 && ! -e "$old_directory" ]] || exit 101
  archived=("$BUILD_WKSP"/scratch/archived-live-prebuilt.*/live-prebuilt-*/live.00.jsonl)
  [[ ${#archived[@]} == 1 && "${archived[0]}" -ef "$PREBUILT_RACE_REPORT.00" ]] || exit 102
  G_TS=20000101000000
  snapshot_ut_diagnostics
  [[ -e "$UT_DIAGNOSTIC_DIR/live-prebuilt-$G_TS/live.00.jsonl" ]] || exit 103
 fi
fi
`
			out, err := scheduleHarnessWithMock(t, script, scheduleHarnessMock(), "MODE="+mode)
			if err != nil {
				t.Fatalf("unjoined diagnostics (%s): %v\n%s", mode, err, out)
			}
		})
	}
}

func TestBatchedSCAIgnoresPreviousUTDiagnostics(t *testing.T) {
	t.Parallel()
	script := `mkdir -p ../ut-report/live-prebuilt-20261115010203
printf 'previous\n' > ../ut-report/live-prebuilt-20261115010203/live.00.jsonl
source ./run_ut.sh SCA
[[ "$(<"$BUILD_WKSP/ut-report/live-prebuilt-20261115010203/live.00.jsonl")" == previous ]] || exit 90
`
	out, err := scheduleHarnessWithMock(t, script, scheduleHarnessMock())
	if err != nil {
		t.Fatalf("SCA must not archive UT diagnostics: %v\n%s", err, out)
	}
}

// A genuine partition error is not a cancellation: the complete test runs
// once with the remaining timeout and its output and status stay authoritative.
func TestBatchedPartitionFailureFallsBackOnce(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version|env) exit 0 ;;
 list) printf '%s\texample/package\nfixture_test.go\n' "$CASE_DIR"; exit 0 ;;
 test)
  if [[ " $* " == *' -c '* ]]; then
   while (( $# )); do
    if [[ "$1" == -o ]]; then out=$2; break; fi
    shift
   done
   printf '#!/bin/bash\nprintf "TestOne\\nTestOne\\n"\n' > "$out"
   chmod +x "$out"; exit 0
  fi
  [[ ! -e "$CASE_DIR/issues.test" && ! -e "$CASE_DIR/issues.test-metadata" && ! -e "$CASE_DIR/issues.test-inventory" ]] || exit 98
  printf '%s\n' "$*" > "$CASE_DIR/fallback-args"
  printf 'run\n' >> "$CASE_DIR/fallback-runs"
  printf '%s\n' '{"Action":"output","Package":"example/package","Output":"fallback event"}'
  printf 'fallback diagnostic\n' >&2
  exit "$FALLBACK_STATUS" ;;
esac
exit 81
`
	for _, fallbackStatus := range []string{"0", "7"} {
		t.Run("exit_"+fallbackStatus, func(t *testing.T) {
			script := batchedOwnerSetup + `
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || status=$?
[[ "$status" == "$FALLBACK_STATUS" && "$CURRENT_UT_DRAIN_FAILED" == 0 ]] || exit 90
[[ "$(<"$CASE_DIR/fallback-runs")" == run ]] || exit 91
args=$(<"$CASE_DIR/fallback-args")
[[ "$args" == 'test -mod=readonly -vet=off -short -v -json -tags matrixone_test -p 1 -timeout '* ]] || exit 92
[[ "$args" == *'s -race example/package' ]] || exit 93
[[ "$(grep -c 'fallback event' "$UT_REPORT")" == 1 ]] || exit 94
grep -q 'fallback diagnostic' "$UT_STDERR" || exit 95
consume_race_report PREBUILT_RACE_REPORT PREBUILT_RACE_TEST_BINARY || exit 96
[[ "$(grep -c 'fallback event' "$UT_REPORT")" == 1 ]] || exit 97
`
			out, err := scheduleHarnessWithMock(t, script, mock, "FALLBACK_STATUS="+fallbackStatus)
			if err != nil {
				t.Fatalf("uncanceled partition fallback: %v\n%s", err, out)
			}
		})
	}
}

func TestBatchedCancellationAroundFallbackCleanup(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version|env) exit 0 ;;
 list) printf '%s\texample/package\nfixture_test.go\n' "$CASE_DIR"; exit 0 ;;
 test)
  if [[ " $* " != *' -c '* ]]; then : > "$CASE_DIR/unexpected-fallback"; exit 0; fi
  while (( $# )); do
   if [[ "$1" == -o ]]; then out=$2; break; fi
   shift
  done
  printf '#!/bin/bash\nprintf "TestOne\\nTestOne\\n"\n' > "$out"
  chmod +x "$out"; exit 0 ;;
esac
exit 81
`
	for _, point := range []string{"before", "after"} {
		for _, signal := range []string{"marker", "term"} {
			t.Run(point+"_"+signal, func(t *testing.T) {
				transform := func(text string) string {
					hook := "        : > \"${UT_HELPER_COMPLETION}.cancel\"\n"
					if signal == "term" {
						hook = "        fixture_self_pid fixture_owner_pid\n        kill -TERM \"$fixture_owner_pid\"\n"
					}
					anchor := "        if batched_race_cancel_requested; then return 125; fi\n        # No batch has executed"
					if point == "after" {
						anchor = "        rm -f \"${metadata}\" \"${inventory}\" \"${binary}\"\n        if batched_race_cancel_requested; then return 125; fi\n"
						return injectBatchedHook(t, text, anchor, "        rm -f \"${metadata}\" \"${inventory}\" \"${binary}\"\n"+hook+"        if batched_race_cancel_requested; then return 125; fi\n")
					}
					return injectBatchedHook(t, text, anchor, hook+anchor)
				}
				script := batchedOwnerSetup + `
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 && ! -s "$completion" ]] || exit 90
[[ ! -e "$CASE_DIR/unexpected-fallback" ]] || exit 91
if [[ "$POINT" == before ]]; then
 [[ -e "$PREBUILT_RACE_TEST_BINARY" ]] || exit 92
else
 [[ ! -e "$PREBUILT_RACE_TEST_BINARY" ]] || exit 93
fi
`
				out, err := scheduleHarnessWithMockTransform(t, script, mock, transform, "POINT="+point)
				if err != nil {
					t.Fatalf("cancellation %s cleanup via %s: %v\n%s", point, signal, err, out)
				}
			})
		}
	}
}

// Exercise the actual outer termination handler, including repeated TERM,
// while a fallback owns a TERM-resistant descendant in the helper group.
func TestBatchedOuterCancellationDrainsFallback(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 env) exit 1 ;;
 test)
  trap '' TERM
  (trap '' TERM; fixture_self_pid child; printf '%s\n' "$child" > "$CASE_DIR/fallback-child"; exec sleep 30) &
  fixture_self_pid owner
  printf '%s\n' "$owner" > "$CASE_DIR/fallback-pid"
  for ((i=0; i<300; i++)); do
   [[ -s "$CASE_DIR/fallback-child" ]] && break
   sleep .01
  done
  [[ -s "$CASE_DIR/fallback-child" ]] || exit 82
  printf '%s\n' '{"Action":"output","Package":"example/package","Output":"partial event"}'
  : > "$CASE_DIR/fallback-ready"
  wait; exit 7 ;;
esac
exit 81
`
	script := batchedOwnerSetup + `
UT_HELPER_TERM_GRACE_TICKS=41
# Observe the production grace selected by the outer handler, while shortening
# only this fixture's real wait. Other owner and join waits remain unchanged.
eval "$(declare -f wait_for_ut_process_group | sed '1s/^wait_for_ut_process_group /fixture_original_wait_for_ut_process_group /')"
eval "$(declare -f terminate_ut_process_group | sed '1s/^terminate_ut_process_group /fixture_original_terminate_ut_process_group /')"
function wait_for_ut_process_group() {
 local expected observed
 if (( UT_TERMINATING == 1 )) && [[ "$1" == "$CURRENT_UT_PID" && "$2" == "$UT_HELPER_TERM_GRACE_TICKS" ]]; then
  printf '%s %s\n' "$1" "$2" >> "$CASE_DIR/outer-wait"
  # The parent is already handling its first TERM. Ask the background sender
  # for two more, and wait for its acknowledgements before shortening grace.
  printf 'ready\n' >&8 || return 1
  for expected in first second; do
   IFS= read -r -t 5 observed <&9 || return 1
   [[ "$observed" == "$expected" ]] || return 1
   printf '%s\n' "$observed" >> "$CASE_DIR/repeated-terms"
  done
  wait "$repeater_pid" || return 1
  fixture_original_wait_for_ut_process_group "$1" 4
 else
  fixture_original_wait_for_ut_process_group "$@"
 fi
}
function terminate_ut_process_group() {
 if (( UT_TERMINATING == 1 )) && [[ "$1" == "$CURRENT_UT_PID" && "${2:-TERM}" == KILL ]]; then
  printf '%s\n' "$1" >> "$CASE_DIR/outer-kill"
 fi
 fixture_original_terminate_ut_process_group "$@"
}
function stop_ut_heartbeat() { :; }
function snapshot_ut_diagnostics() { :; }
function report_slow_ut_cases() { :; }
function report_active_ut_cases() { :; }
owner_pid=''
cleanup() {
 status=$?
 [[ "$status" == 125 || "$status" == 143 ]] || status=90
 [[ -e "$completion.cancel" && ! -s "$completion" ]] || status=91
 [[ -e "$PREBUILT_RACE_REPORT" ]] || status=92
 [[ "$(grep -c 'partial event' "$UT_REPORT")" == 1 ]] || status=93
 observed_wait=$(cat "$CASE_DIR/outer-wait" 2>/dev/null)
 observed_kill=$(cat "$CASE_DIR/outer-kill" 2>/dev/null)
 [[ "$observed_wait" == "$owner_pid 41" ]] || { printf 'outer wait %q expected %q\n' "$observed_wait" "$owner_pid 41" >&2; status=94; }
 [[ "$observed_kill" == "$owner_pid" ]] || { printf 'outer kill %q expected %q\n' "$observed_kill" "$owner_pid" >&2; status=95; }
 [[ "$(cat "$CASE_DIR/repeated-terms" 2>/dev/null)" == $'first\nsecond' ]] || { printf 'repeated TERM missing\n' >&2; status=96; }
 if [[ -e "$CASE_DIR/fallback-pid" && -e "$CASE_DIR/fallback-child" && -n "$owner_pid" ]]; then
  if ut_process_group_alive "$owner_pid"; then printf 'group %s still alive\n' "$owner_pid" >&2; status=94; fi
 else status=95; fi
 exit "$status"
}
trap cleanup EXIT
trap handle_ut_termination TERM
start_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || exit 96
owner_pid=$CURRENT_UT_PID
for ((i=0; i<300; i++)); do
 [[ -e "$CASE_DIR/fallback-ready" ]] && break
 kill -0 "$CURRENT_UT_PID" 2>/dev/null || break
 sleep .02
done
[[ -e "$CASE_DIR/fallback-ready" ]] || exit 97
for pid in "$(<"$CASE_DIR/fallback-pid")" "$(<"$CASE_DIR/fallback-child")"; do
 pgid=$(ps -o pgid= -p "$pid")
 [[ "${pgid//[[:space:]]/}" == "$owner_pid" ]] || exit 99
done
parent_pid=$$
mkfifo "$CASE_DIR/handler-ready" "$CASE_DIR/signal-ack"
exec 8<>"$CASE_DIR/handler-ready" 9<>"$CASE_DIR/signal-ack"
(
 IFS= read -r -t 5 ready <&8 || exit 90
 [[ "$ready" == ready ]] || exit 91
 kill -TERM "$parent_pid" || exit 92
 printf 'first\n' >&9 || exit 93
 kill -TERM "$parent_pid" || exit 94
 printf 'second\n' >&9 || exit 95
) &
repeater_pid=$!
kill -TERM "$parent_pid"
exit 98
`
	out, err := scheduleHarnessWithMockTimeout(t, 30*time.Second, script, mock, nil)
	exit, ok := err.(*exec.ExitError)
	if !ok || (exit.ExitCode() != 143 && exit.ExitCode() != 125) {
		t.Fatalf("outer cancellation must drain fallback descendants: %v\n%s", err, out)
	}
}
