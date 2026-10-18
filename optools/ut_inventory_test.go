// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package optools

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestRaceInventoryPartition(t *testing.T) {
	t.Parallel()
	names := []string{"TestFirst", "Example_named", "FuzzSeeds", "Test字面", "TestLiteral.[a]+(b)?"}
	for _, layout := range []string{"roundrobin", "contiguous"} {
		for _, groups := range []int{1, 2, 8} {
			t.Run(fmt.Sprintf("%s/%d", layout, groups), func(t *testing.T) {
				inventory := filepath.Join(t.TempDir(), "inventory")
				// Include diagnostic output and a final root without a newline.
				if err := os.WriteFile(inventory, []byte("setup diagnostic\n"+strings.Join(names, "\n")), 0600); err != nil {
					t.Fatal(err)
				}
				out, err := exec.Command("bash", "-c", `source ./ut_tools.bash
partition_race_test_inventory "$1" "$2" "$3" || exit $?
for ((i=0;i<${#shard_patterns[@]};i++)); do printf '%s\t%s\n' "${shard_patterns[i]}" "${shard_counts[i]}"; done`,
					"bash", inventory, strconv.Itoa(groups), layout).CombinedOutput()
				if err != nil {
					t.Fatalf("partition: %v\n%s", err, out)
				}
				lines := strings.Split(strings.TrimSpace(string(out)), "\n")
				if len(lines) != groups {
					t.Fatalf("got %d groups, want %d", len(lines), groups)
				}
				seen := make(map[string]int)
				for group, line := range lines {
					fields := strings.Split(line, "\t")
					if len(fields) != 2 {
						t.Fatalf("bad pattern/count record: %q", line)
					}
					re := regexp.MustCompile(fields[0])
					matched := 0
					for index, name := range names {
						if !re.MatchString(name) {
							continue
						}
						seen[name]++
						matched++
						expected := index % groups
						if layout == "contiguous" {
							expected = index / ((len(names) + groups - 1) / groups)
						}
						if group != expected || re.MatchString(name+"Suffix") || re.MatchString("Prefix"+name) {
							t.Fatalf("root %q selected by incorrect group/pattern %d %q", name, group, fields[0])
						}
					}
					if fields[1] != strconv.Itoa(matched) {
						t.Fatalf("pattern/count disagree: %q, matched %d", line, matched)
					}
					if re.MatchString("TestLiteralXaab") {
						t.Fatalf("pattern treated a literal name as an expression: %q", fields[0])
					}
				}
				for _, name := range names {
					if seen[name] != 1 {
						t.Fatalf("root %q executed in %d groups", name, seen[name])
					}
				}
			})
		}
	}
}

func TestRaceInventoryRejectsIncompleteDiscovery(t *testing.T) {
	t.Parallel()
	for _, inventory := range []string{"diagnostics only\n", "TestOnce\nTestOnce\n", "TestValid\nTest invalid\n"} {
		t.Run(inventory, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "inventory")
			if err := os.WriteFile(path, []byte(inventory), 0600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command("bash", "-c", `source ./ut_tools.bash
shard_patterns=(stale); shard_counts=(99)
if partition_race_test_inventory "$1" 4 contiguous; then exit 9; fi
(( ${#shard_patterns[@]} == 0 && ${#shard_counts[@]} == 0 ))`, "bash", path).CombinedOutput()
			if err != nil {
				t.Fatalf("invalid discovery must not publish a partial selection: %v\n%s", err, out)
			}
		})
	}
}

func TestRaceInventoryCancellationAtCompletion(t *testing.T) {
	t.Parallel()
	transform := func(text string) string {
		const anchor = "    done\n    wait \"${pid}\" || status=$?\n"
		if strings.Count(text, anchor) != 1 {
			t.Fatalf("inventory completion anchor count = %d, want 1", strings.Count(text, anchor))
		}
		return strings.Replace(text, anchor,
			"    done\n    ut_test_inventory_before_wait\n    wait \"${pid}\" || status=$?\n", 1)
	}
	script := `source ./run_ut.sh UT
	function logger() { :; }
function ut_test_inventory_before_wait() { kill -TERM "$$"; }
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
cat > "$CASE_DIR/list.sh" <<'EOF'
#!/bin/bash
printf 'TestOne\n'
EOF
chmod +x "$CASE_DIR/list.sh"
status=0
run_race_inventory_with_deadline "$CASE_DIR" "$CASE_DIR/list.sh" "$CASE_DIR/inventory" "$(( $(date +%s) + 10 ))" || status=$?
[[ "$status" == 125 ]] || exit 90
[[ -s "$CASE_DIR/inventory" ]] || exit 91
`
	out, err := scheduleHarnessWithMockTransform(t, script, scheduleHarnessMock(), transform)
	if err != nil {
		t.Fatalf("completion cancellation must return status 125: %v\n%s", err, out)
	}
}

func TestPrebuiltRaceDeadlineFailureIsReported(t *testing.T) {
	t.Parallel()
	script := `source ./run_ut.sh UT
function logger() { :; }
UT_TIMEOUT=1
race_packages=(example/a example/b)
race_dirs=("$CASE_DIR" "$CASE_DIR")
race_binaries=("$CASE_DIR/a.test" "$CASE_DIR/b.test")
race_patterns=('.*' '.*')
race_deadlines=(1 1)
status=0
run_prebuilt_race_commands serial "$CASE_DIR/report" 1 10 || status=$?
[[ "$status" == 1 ]] || exit 90
grep -q '"Action":"fail"' "$CASE_DIR/report.00" || exit 91
grep -q 'shared test timeout exhausted before batch execution' "$CASE_DIR/report.00" || exit 92
`
	out, err := scheduleHarnessWithMock(t, script, scheduleHarnessMock())
	if err != nil {
		t.Fatalf("deadline exhaustion must publish a failure event: %v\n%s", err, out)
	}
}

func TestOuterCancellationKeepsUnpublishedDiscoveryArtifacts(t *testing.T) {
	t.Parallel()
	script := `source ./run_ut.sh UT
function logger() { :; }
function checkpoint_ut_event() { :; }
function stop_ut_heartbeat() { :; }
function snapshot_ut_diagnostics() { :; }
function report_slow_ut_cases() { :; }
function report_active_ut_cases() { :; }
UT_REPORT="$CASE_DIR/authoritative-report"
UT_CHECKPOINT="$CASE_DIR/checkpoint"
UT_STDERR="$CASE_DIR/stderr"
LOG="$CASE_DIR/runner.log"
PREBUILT_RACE_REPORT="$CASE_DIR/issues-report"
PREBUILT_RACE_TEST_BINARY="$CASE_DIR/issues.test"
mkdir -p "$UT_DIAGNOSTIC_DIR"
: > "$UT_CHECKPOINT"
: > "$UT_STDERR"
: > "$PREBUILT_RACE_TEST_BINARY"
: > "$PREBUILT_RACE_TEST_BINARY-metadata"
: > "$PREBUILT_RACE_TEST_BINARY-inventory"
cleanup() {
 status=$?
 [[ "$status" == 143 ]] || status=90
 [[ -f "$PREBUILT_RACE_TEST_BINARY" ]] || status=91
 [[ -f "$PREBUILT_RACE_TEST_BINARY-metadata" ]] || status=92
 [[ -f "$PREBUILT_RACE_TEST_BINARY-inventory" ]] || status=93
 exit "$status"
}
	trap cleanup EXIT
	handle_ut_termination
`
	out, err := scheduleHarnessWithMock(t, script, scheduleHarnessMock())
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 143 {
		t.Fatalf("outer cancellation deleted unpublished discovery artifacts: %v\n%s", err, out)
	}
}

func TestOuterCancellationKeepsReportWhenChildDrainFails(t *testing.T) {
	t.Parallel()
	script := `source ./run_ut.sh UT
function logger() { :; }
function checkpoint_ut_event() { :; }
function stop_ut_heartbeat() { :; }
function snapshot_ut_diagnostics() { :; }
function report_slow_ut_cases() { :; }
function report_active_ut_cases() { :; }
function terminate_ut_process_group() { :; }
function wait_for_ut_process_group() { return 1; }
UT_REPORT="$CASE_DIR/authoritative-report"
UT_CHECKPOINT="$CASE_DIR/checkpoint"
UT_STDERR="$CASE_DIR/stderr"
LOG="$CASE_DIR/runner.log"
PREBUILT_RACE_REPORT="$CASE_DIR/issues-report"
PREBUILT_RACE_TEST_BINARY="$CASE_DIR/issues.test"
: > "$UT_CHECKPOINT"
: > "$UT_STDERR"
: > "$PREBUILT_RACE_REPORT.00"
: > "$PREBUILT_RACE_TEST_BINARY"
: > "$PREBUILT_RACE_TEST_BINARY-metadata"
: > "$PREBUILT_RACE_TEST_BINARY-inventory"
cleanup() {
 status=$?
 [[ "$status" == 125 ]] || status=90
 [[ -f "$PREBUILT_RACE_REPORT.00" ]] || status=91
 [[ ! -e "$UT_REPORT" ]] || status=92
 [[ -f "$PREBUILT_RACE_TEST_BINARY" ]] || status=93
 [[ -f "$PREBUILT_RACE_TEST_BINARY-metadata" ]] || status=94
 [[ -f "$PREBUILT_RACE_TEST_BINARY-inventory" ]] || status=95
 exit "$status"
}
trap cleanup EXIT
CURRENT_UT_PID=999999
CURRENT_UT_LABEL='batched issues'
handle_ut_termination
`
	out, err := scheduleHarnessWithMock(t, script, scheduleHarnessMock())
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 125 {
		t.Fatalf("undrained child must retain report ownership: %v\n%s", err, out)
	}
}

func TestOuterCancellationKeepsReportForFailedDrainHelper(t *testing.T) {
	t.Parallel()
	for _, helperStatus := range []string{"125", "137"} {
		for _, phase := range []string{"active", "captured", "flagged", "released", "joined", "raw-status"} {
			t.Run("status="+helperStatus+"/"+phase, func(t *testing.T) {
				script := `source ./run_ut.sh UT
function logger() { :; }
function checkpoint_ut_event() { :; }
function stop_ut_heartbeat() { :; }
function snapshot_ut_diagnostics() { :; }
function report_slow_ut_cases() { :; }
function report_active_ut_cases() { :; }
function terminate_ut_process_group() { :; }
function wait_for_ut_process_group() { return 0; }
UT_REPORT="$CASE_DIR/authoritative-report"
UT_CHECKPOINT="$CASE_DIR/checkpoint"
UT_STDERR="$CASE_DIR/stderr"
LOG="$CASE_DIR/runner.log"
PREBUILT_RACE_REPORT="$CASE_DIR/issues-report"
PREBUILT_RACE_TEST_BINARY="$CASE_DIR/issues.test"
: > "$UT_CHECKPOINT"
: > "$UT_STDERR"
: > "$PREBUILT_RACE_REPORT.00"
: > "$PREBUILT_RACE_TEST_BINARY"
cleanup() {
 status=$?
 [[ "$status" == 125 ]] || status=90
 [[ -f "$PREBUILT_RACE_REPORT.00" ]] || status=91
 [[ ! -e "$UT_REPORT" ]] || status=92
 [[ -f "$PREBUILT_RACE_TEST_BINARY" ]] || status=93
 exit "$status"
}
trap cleanup EXIT
(if [[ "$HELPER_STATUS" == 137 ]]; then fixture_self_pid fixture_pid; kill -KILL "$fixture_pid"; else exit 125; fi) &
CURRENT_UT_PID=$!
CURRENT_UT_LABEL='batched issues'
if [[ "$PHASE" == active ]]; then
 handle_ut_termination
elif [[ "$PHASE" == raw-status ]]; then
 builtin wait "$CURRENT_UT_PID" || true
 handle_ut_join_term "$HELPER_STATUS" 0 CURRENT_UT_DRAIN_FAILED
else
 finish_ut_command; [[ "$?" == 125 && -z "$CURRENT_UT_PID" ]] || exit 96
 start_ut_command serial retry true; [[ "$?" == 125 ]] || exit 97
 trap handle_ut_termination TERM
 kill -TERM "$$"
fi
`
				transform := func(text string) string {
					anchor := "        wait \"${!pid_name}\" || join_status=$?\n"
					if phase == "flagged" {
						anchor = "        if (( join_status == 125 )); then printf -v \"${failed_flag_name}\" '%s' 1; fi\n"
					}
					if phase == "released" {
						anchor = "        printf -v \"${pid_name}\" '%s' ''\n"
					}
					if phase == "captured" || phase == "flagged" || phase == "released" {
						if strings.Count(text, anchor) != 1 {
							t.Fatal("missing unique owner join boundary")
						}
						return strings.Replace(text, anchor, anchor+"        kill -TERM \"$$\"\n", 1)
					}
					return text
				}
				out, err := scheduleHarnessWithMockTransform(t, script, scheduleHarnessMock(), transform, "PHASE="+phase, "HELPER_STATUS="+helperStatus)
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.ExitCode() != 125 {
					t.Fatalf("failed-drain helper must retain report ownership: %v\n%s", err, out)
				}
			})
		}
	}
}

func TestKilledOrdinaryUTOwnerPreservesExitStatus(t *testing.T) {
	t.Parallel()
	for _, pidMode := range []string{"native", "fallback"} {
		for _, owner := range []string{"current", "light"} {
			t.Run(pidMode+"/"+owner, func(t *testing.T) {
				script := `if [[ "$PID_MODE" == fallback ]]; then unset BASHPID; fi
source ./run_ut.sh UT
function logger() { :; }
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" CGO_CFLAGS="${CGO_CFLAGS:-}" CGO_LDFLAGS="${CGO_LDFLAGS:-}"
status=0
if [[ "$OWNER" == current ]]; then
 start_ut_command serial ordinary go test example/ordinary
 owner_pid=$CURRENT_UT_PID
 finish_ut_command || status=$?
 [[ -z "$CURRENT_UT_PID" && "$CURRENT_UT_DRAIN_FAILED" == 0 ]] || exit 90
else
 start_light_race example/light 1
 owner_pid=$LIGHT_RACE_JOB_PID
 finish_light_race || status=$?
 [[ -z "$LIGHT_RACE_JOB_PID" && "$LIGHT_RACE_DRAIN_FAILED" == 0 ]] || exit 91
fi
[[ "$(<"$CASE_DIR/killed-helper.pid")" == "$owner_pid" ]] || exit 99
[[ "$status" == 137 ]] || exit 92
! ut_drain_failed || exit 93
start_ut_command serial later true || exit 94
finish_ut_command || exit 95
`
				mock := `#!/bin/bash
if [[ "$PID_MODE" == fallback ]]; then unset BASHPID; fi
if [[ "$1" == version ]]; then exit 0; fi
[[ "$1" == test ]] || exit 96
fixture_self_pid fixture_pid
printf '%s\n' "$fixture_pid" > "$CASE_DIR/killed-helper.pid"
kill -KILL "$fixture_pid"
`
				out, err := scheduleHarnessWithMock(t, script, mock, "OWNER="+owner, "PID_MODE="+pidMode)
				if err != nil {
					t.Fatalf("ordinary killed owner changed its status contract: %v\n%s", err, out)
				}
			})
		}
	}
}

func TestIssuesDiscoveryFallbackUsesRemainingDeadline(t *testing.T) {
	t.Parallel()
	script := `source ./run_ut.sh UT
function logger() { :; }
UT_TIMEOUT=1
PREBUILT_RACE_REPORT="$CASE_DIR/discovery-report"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
mkdir -p "$CASE_DIR/pkg"
printf 'package fixture\n' > "$CASE_DIR/pkg/TestFile.go"
fallback_args="$CASE_DIR/fallback-args"
date_state="$CASE_DIR/date-calls"
date_mode="$CASE_DIR/date-mode"
printf '0\n' > "$date_state"
printf 'remaining\n' > "$date_mode"
function date() {
 if [[ "$1" == +%s ]]; then
  date_calls=$(<"$date_state")
  date_calls=$((date_calls + 1))
  printf '%s\n' "$date_calls" > "$date_state"
  if (( date_calls == 1 )); then
   printf '1000\n'
  elif [[ "$(<"$date_mode")" == exhausted ]]; then
   printf '1060\n'
  else
   printf '1050\n'
  fi
 else
  command date "$@"
 fi
}
`
	mock := `#!/bin/bash
case "$1" in
version) exit 0 ;;
env) printf '\n'; exit 0 ;;
list) printf '%s\t%s\nTestFile.go\n' "$CASE_DIR/pkg" example/issues; exit 0 ;;
test)
 if [[ " $* " == *' -c '* ]]; then
  output=''
  while (( $# > 0 )); do
   if [[ "$1" == -o ]]; then output=$2; break; fi
   shift
  done
  [[ -n "$output" ]] || exit 4
  cat > "$output" <<'EOF'
#!/bin/bash
sleep 1
exit 7
EOF
  chmod +x "$output"
  exit 0
 fi
 printf '%s\n' "$*" > "$CASE_DIR/fallback-args"
 exit 0
 ;;
esac
exit 99
`
	// The listing binary fails after the shared deadline has started. The
	// fallback command must receive only the remaining seconds, not a fresh
	// UT_TIMEOUT-sized budget.
	script += `
status=0
run_race_test_batches example/issues "$CASE_DIR/issues.test" 4 || status=$?
[[ "$status" == 0 ]] || exit 90
grep -q -- '-timeout 10s' "$fallback_args" || { cat "$fallback_args"; exit 91; }
consume_race_report PREBUILT_RACE_REPORT || exit 94
PREBUILT_RACE_REPORT="$CASE_DIR/discovery-exhausted-report"
printf 'exhausted\n' > "$date_mode"
printf '0\n' > "$date_state"
rm -f "$fallback_args"
status=0
run_race_test_batches example/issues "$CASE_DIR/issues-exhausted.test" 4 || status=$?
[[ "$status" == 124 ]] || exit 92
[[ ! -e "$fallback_args" ]] || exit 93
`
	out, err := scheduleHarnessWithMock(t, script, mock)
	if err != nil {
		t.Fatalf("discovery fallback did not carry the remaining deadline: %v\n%s", err, out)
	}
}

func TestIssuesBatchesUseBoundedProcessPool(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, contract, main, processExit, wantStatus string }{
		{"independent", "independent", "", "0", "0"},
		{"cleanup-only", "cleanup-only", "func TestMain(m *testing.M) {}", "0", "0"},
		{"cleanup-failure", "cleanup-only", "func TestMain(m *testing.M) {}", "7", "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			script := `source ./run_ut.sh UT
PREBUILT_RACE_REPORT="$CASE_DIR/issues-report"
function logger() { printf '%s\n' "$*"; }
UT_ISSUES_BATCH_PARALLEL=2
export GOMAXPROCS=8
mkfifo "$CASE_DIR/ready-One" "$CASE_DIR/ready-Two"
printf 'package fixture\n%s\n' "$TEST_MAIN" > "$CASE_DIR/TestFile.go"
mock_binary="$CASE_DIR/issues.test"
status=0
run_ut_command serial issues run_race_test_batches example/issues "$mock_binary" 8 2 2 "$CONTRACT" || status=$?
consume_race_report PREBUILT_RACE_REPORT
cat "$UT_REPORT"
[[ "$status" == "$WANT_STATUS" ]] || exit 90
[[ "$GOMAXPROCS" == 8 ]] || exit 94
for name in One Two Three Four Five Six Seven Eight; do
 [[ "$(<"$CASE_DIR/pool-$name")" == 2 ]] || exit 91
 [[ -e "$CASE_DIR/start-$name" ]] || exit 92
 [[ "$(<"$CASE_DIR/cpu-$name")" == 2 ]] || exit 93
done
`
			mock := `#!/bin/bash
case "$1" in
env)
 printf '\n'
 ;;
list)
 printf '%s\t%s\nTestFile.go\n' "$CASE_DIR" example/issues
 ;;
test)
 if [[ " $* " == *' -c '* ]]; then
  [[ "$GOMAXPROCS" == 8 ]] || exit 94
  output=''
  while (( $# > 0 )); do
   if [[ "$1" == -o ]]; then output=$2; break; fi
   shift
  done
  [[ -n "$output" ]] || exit 4
  cat > "$output" <<'EOF'
#!/bin/bash
if [[ "$*" == *-test.list=* ]]; then
 [[ "$GOMAXPROCS" == 8 ]] || exit 95
 printf 'TestOne\nTestTwo\nTestThree\nTestFour\nTestFive\nTestSix\nTestSeven\nTestEight\n'
 exit 0
fi
name=''
for candidate in One Two Three Four Five Six Seven Eight; do
 if [[ "$*" == *"Test$candidate"* ]]; then name=$candidate; break; fi
done
[[ -n "$name" ]] || exit 96
printf '%s\n' "${MO_TEST_CLUSTER_ADMISSION_POOL_SIZE:-unset}" > "$CASE_DIR/pool-$name"
printf '%s\n' "$GOMAXPROCS" > "$CASE_DIR/cpu-$name"
touch "$CASE_DIR/start-$name"
if [[ "$name" == One || "$name" == Two ]]; then
 exec 7<> "$CASE_DIR/ready-One" 8<> "$CASE_DIR/ready-Two"
 if [[ "$name" == One ]]; then
  printf 'ready\n' >&7
  read -r -t 5 ready <&8 || exit 7
 else
  printf 'ready\n' >&8
  read -r -t 5 ready <&7 || exit 7
 fi
fi
printf '{"Action":"pass","Package":"example/issues"}\n'
exit "$PROCESS_EXIT"
EOF
  chmod +x "$output"
 fi
 ;;
tool)
 [[ "$2" == test2json ]] || exit 5
 exec "$6" "${@:7}"
 ;;
*)
 exit 6
 ;;
esac
`
			out, err := scheduleHarnessWithMock(t, script, mock, "CONTRACT="+tc.contract, "TEST_MAIN="+tc.main, "PROCESS_EXIT="+tc.processExit, "WANT_STATUS="+tc.wantStatus)
			if err != nil {
				t.Fatalf("issues batches did not run two admitted processes: %v\n%s", err, out)
			}
			lines := strings.Split(strings.TrimSpace(string(out)), "\n")
			if len(lines) != 8 {
				t.Fatalf("want eight batch events: %s", out)
			}
			for _, line := range lines {
				assertScheduleJSONReport(t, []byte(line), map[string]string{"example/issues": "pass"})
			}
		})
	}
}

// A denied/unknown lifetime contract must run the ordinary package exactly
// once and still release its batch-report ownership before another package.
func TestRaceBatchPreparationFallbackReleasesOwnership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, contract, fallbackExit string
		badReport                    bool
	}{
		{"denied-main", "independent", "0", false},
		{"unknown-contract", "unknown", "0", false},
		{"fallback-failure", "independent", "7", false},
		{"report-publication-failure", "independent", "0", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			script := `source ./run_ut.sh UT
function logger() { :; }
printf 'func TestMain(m *testing.M) {}\n' > "$CASE_DIR/TestFile.go"
if [[ "$BAD_REPORT" == true ]]; then
 PREBUILT_RACE_REPORT="$CASE_DIR/missing-parent/report"
 status=0
 run_ut_command embedded fallback run_race_test_batches example/first "$CASE_DIR/batch.test" 4 2 2 "$CONTRACT" || status=$?
 [[ "$status" == 125 && ! -e "$CASE_DIR/executed" ]] || exit 95
 printf 'publication-failed-before-execution\n'
 exit 0
fi
for package in first second; do
 PREBUILT_RACE_REPORT="$CASE_DIR/batch-report"
 PREBUILT_RACE_TEST_BINARY="$CASE_DIR/batch.test"
 status=0
 run_ut_command embedded fallback run_race_test_batches "example/$package" "$PREBUILT_RACE_TEST_BINARY" 4 2 2 "$CONTRACT" || status=$?
 [[ "$status" == "$FALLBACK_EXIT" ]] || exit 90
 consume_race_report PREBUILT_RACE_REPORT PREBUILT_RACE_TEST_BINARY || exit 91
 [[ -z "$PREBUILT_RACE_REPORT" && -z "$PREBUILT_RACE_TEST_BINARY" ]] || exit 92
 [[ ! -e "$CASE_DIR/batch-report" && ! -e "$CASE_DIR/batch.test" ]] || exit 93
done
[[ "$(cat "$CASE_DIR/executed")" == $'example/first\nexample/second' ]] || exit 94
cat "$UT_REPORT"
`
			mock := `#!/bin/bash
case "$1" in
 version|env) exit 0 ;;
 list) printf '%s\t%s\nTestFile.go\n' "$CASE_DIR" "${!#}" ;;
 test)
  [[ " $* " != *' -c '* ]] || exit 95
  printf '%s\n' "${!#}" >> "$CASE_DIR/executed"
  action=pass; [[ "$FALLBACK_EXIT" == 0 ]] || action=fail
  printf '{"Action":"%s","Package":"%s"}\n' "$action" "${!#}"
  exit "$FALLBACK_EXIT"
  ;;
 *) exit 96 ;;
esac
`
			out, err := scheduleHarnessWithMock(t, script, mock, "CONTRACT="+tc.contract, "FALLBACK_EXIT="+tc.fallbackExit, fmt.Sprintf("BAD_REPORT=%t", tc.badReport))
			if err != nil {
				t.Fatalf("fallback ownership: %v\n%s", err, out)
			}
			if tc.badReport {
				if !strings.Contains(string(out), "publication-failed-before-execution") {
					t.Fatalf("wrong publication result: %s", out)
				}
				return
			}
			action := "pass"
			if tc.fallbackExit != "0" {
				action = "fail"
			}
			assertScheduleJSONReport(t, out, map[string]string{"example/first": action, "example/second": action})
		})
	}
}

func TestRaceInventoryPreservesRunnableRootsAndStress(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeScopeFixture(t, root, "go.mod", "module inventoryfixture\n\ngo 1.27.0\n")
	writeScopeFixture(t, root, "inventory_test.go", `package inventoryfixture
import ("context"; "fmt"; "os"; "sync/atomic"; "testing"; "time")
func record(name string) {
 f, err := os.OpenFile(os.Getenv("INVENTORY_RECORD"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
 if err != nil { panic(err) }; defer f.Close()
 if _,err = fmt.Fprintln(f,name); err != nil { panic(err) }
}
func TestOne(t *testing.T) { record("one") }
func TestTwentyRounds(t *testing.T) {
 for i:=0;i<20;i++ { t.Run(fmt.Sprint(i),func(t *testing.T) { record("round") }) }
}
func TestParallelResolvers(t *testing.T) {
 var started atomic.Int32
 ready := make(chan struct{})
 ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
 t.Cleanup(cancel)
 for worker:=0;worker<4;worker++ {
  t.Run(fmt.Sprint(worker),func(t *testing.T) {
   t.Parallel()
   if started.Add(1)==4 { close(ready) }
   select { case <-ready: record("parallel"); case <-ctx.Done(): t.Fatal("four resolvers were serialized") }
  })
 }
}
func FuzzSeeds(f *testing.F) {
 for _,s:=range []string{"a","b","c"} { f.Add(s) }
 f.Fuzz(func(t *testing.T,s string) { record("seed:"+s) })
}
func Example_result() { record("example"); fmt.Println("result")
 // Output: result
}
`)
	binary := filepath.Join(root, "inventory.test")
	// The parent optools package is already race-tested. This child only
	// exercises the test binary's list/run protocol; compiling it with race
	// instrumentation adds minutes of linker work without testing another
	// ownership or scheduling path.
	build := exec.CommandContext(t.Context(), "go", "test", "-c", "-o", binary)
	build.Dir = root
	build.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build real runtime inventory: %v\n%s", err, out)
	}
	list, err := exec.CommandContext(t.Context(), binary, "-test.list=^(Test|Fuzz|Example)").CombinedOutput()
	if err != nil {
		t.Fatalf("list real binary: %v\n%s", err, list)
	}
	inventory := filepath.Join(root, "inventory")
	if err := os.WriteFile(inventory, list, 0600); err != nil {
		t.Fatal(err)
	}
	patterns, err := exec.Command("bash", "-c", `source ./ut_tools.bash
partition_race_test_inventory "$1" 4 contiguous || exit $?
printf '%s\n' "${shard_patterns[@]}"`, "bash", inventory).CombinedOutput()
	if err != nil {
		t.Fatalf("partition real inventory: %v\n%s", err, patterns)
	}
	run := func(record string, selectors []string) map[string]int {
		t.Helper()
		for _, selector := range selectors {
			cmd := exec.CommandContext(t.Context(), binary, "-test.short=true", "-test.count=1", "-test.parallel=8", "-test.run="+selector)
			cmd.Env = append(os.Environ(), "INVENTORY_RECORD="+record)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("run %q: %v\n%s", selector, err, out)
			}
		}
		data, err := os.ReadFile(record)
		if err != nil {
			t.Fatal(err)
		}
		counts := make(map[string]int)
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			counts[line]++
		}
		return counts
	}
	control := run(filepath.Join(root, "control"), []string{".*"})
	batched := run(filepath.Join(root, "batched"), strings.Split(strings.TrimSpace(string(patterns)), "\n"))
	want := map[string]int{"one": 1, "round": 20, "seed:a": 1, "seed:b": 1, "seed:c": 1, "example": 1, "parallel": 4}
	if !reflect.DeepEqual(control, want) || !reflect.DeepEqual(batched, control) {
		t.Fatalf("runtime coverage/stress changed: control=%v batched=%v want=%v", control, batched, want)
	}
}

func TestKilledIndependentHelperRetainsEvidence(t *testing.T) {
	t.Parallel()
	for _, pidMode := range []string{"native", "fallback"} {
		for _, owner := range []string{"engine", "plan", "prebuild"} {
			t.Run(pidMode+"/"+owner, func(t *testing.T) {
				script := `if [[ "$PID_MODE" == fallback ]]; then unset BASHPID; fi
source ./run_ut.sh UT
function logger() { :; }
function killed_helper() {
 printf 'diagnostic\n' > "$CASE_DIR/evidence"
 fixture_self_pid fixture_pid
 printf '%s\n' "$fixture_pid" > "$CASE_DIR/killed-helper.pid"
 kill -KILL "$fixture_pid"
}
function run_engine_race_shards() { killed_helper; }
function run_plan_race_shards() { killed_helper; }
function run_embedded_prebuild() { killed_helper; }
status=0
case "$OWNER" in
 engine)
  start_engine_race example/engine 1
  owner_pid=$ENGINE_RACE_JOB_PID
  join_ut_owner ENGINE_RACE_JOB_PID ENGINE_RACE_DRAIN_FAILED || status=$?
  [[ -z "$ENGINE_RACE_JOB_PID" && "$ENGINE_RACE_DRAIN_FAILED" == 1 ]] || exit 90
  consume_engine_race_report; [[ "$?" == 125 ]] || exit 91
  ;;
 plan)
  start_plan_race example/plan
  owner_pid=$PLAN_RACE_JOB_PID
  join_ut_owner PLAN_RACE_JOB_PID PLAN_RACE_DRAIN_FAILED || status=$?
  [[ -z "$PLAN_RACE_JOB_PID" && "$PLAN_RACE_DRAIN_FAILED" == 1 ]] || exit 92
  consume_plan_race_report; [[ "$?" == 125 ]] || exit 93
  ;;
 prebuild)
  UT_PREBUILD_MIN_FREE_KB=1
  start_embedded_prebuild example/embedded 1
  owner_pid=$CLUSTER_PREBUILD_JOB_PID
  finish_embedded_prebuild || status=$?
  [[ -z "$CLUSTER_PREBUILD_JOB_PID" && "$CLUSTER_PREBUILD_DRAIN_FAILED" == 1 ]] || exit 94
  cleanup_embedded_prebuild; [[ "$?" == 125 && -d "$CLUSTER_PREBUILD_DIR" ]] || exit 95
  ;;
esac
[[ "$(<"$CASE_DIR/killed-helper.pid")" == "$owner_pid" ]] || exit 99
[[ "$status" == 125 && -f "$CASE_DIR/evidence" ]] || exit 96
start_ut_command serial forbidden touch "$CASE_DIR/admitted"; [[ "$?" == 125 ]] || exit 97
[[ ! -e "$CASE_DIR/admitted" ]] || exit 98
`
				out, err := scheduleHarnessWithMock(t, script, scheduleHarnessMock(), "OWNER="+owner, "PID_MODE="+pidMode)
				if err != nil {
					t.Fatalf("killed helper lost ownership evidence: %v\n%s", err, out)
				}
			})
		}
	}
}

// Exercise the common owner gate through real helper admission, including
// normal stage failure, stale evidence and failed completion publication.
func TestHelperCompletionAdmissionAndJoin(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"failure", "missing", "mismatch", "malformed", "stale", "init-failure"} {
		t.Run(mode, func(t *testing.T) {
			script := `source ./run_ut.sh UT
function logger() { :; }
completion="$G_WKSP/$G_TS-owner-complete.ENGINE_RACE_JOB_PID"
ENGINE_RACE_REPORT="$G_WKSP/$G_TS-engine-race-report.out"
ENGINE_RACE_TEST_BINARY="$G_WKSP/$G_TS-engine-race.test"
function run_engine_race_shards() {
 touch "$CASE_DIR/admitted"
 printf 'engine\n' > "$ENGINE_RACE_REPORT"
 : > "$ENGINE_RACE_TEST_BINARY"
 if [[ "$MODE" == stale ]]; then exit 1; fi
 return 1
}
if [[ "$MODE" == stale ]]; then printf '1\n' > "$completion"; fi
if [[ "$MODE" == init-failure ]]; then mkdir "$completion"; fi
if [[ "$MODE" != failure && "$MODE" != stale && "$MODE" != init-failure ]]; then
 function publish_ut_helper_completion() {
  case "$MODE" in
   missing) rm -f "$UT_HELPER_COMPLETION" ;;
   mismatch) printf '0\n' > "$UT_HELPER_COMPLETION" ;;
   malformed) printf '1\nextra\n' > "$UT_HELPER_COMPLETION" ;;
  esac
 }
fi
status=0
start_engine_race example/engine 1 || status=$?
if [[ "$status" == 0 ]]; then join_ut_owner ENGINE_RACE_JOB_PID ENGINE_RACE_DRAIN_FAILED || status=$?; fi
if [[ "$MODE" == failure ]]; then
 [[ "$status" == 1 && "$ENGINE_RACE_DRAIN_FAILED" == 0 ]] || exit 90
 consume_engine_race_report || exit 91
 [[ ! -e "$G_WKSP/$G_TS-engine-race.test" && ! -e "$G_WKSP/$G_TS-engine-race-report.out" ]] || exit 91
 start_ut_command serial after-failure true && finish_ut_command || exit 91
else
 [[ "$status" == 125 && "$ENGINE_RACE_DRAIN_FAILED" == 1 ]] || exit 92
 start_ut_command serial forbidden touch "$CASE_DIR/forbidden"; [[ "$?" == 125 && ! -e "$CASE_DIR/forbidden" ]] || exit 93
fi
if [[ "$MODE" == init-failure ]]; then
 [[ ! -e "$CASE_DIR/admitted" ]] || exit 94
else
 if [[ "$MODE" != failure ]]; then
  consume_engine_race_report; [[ "$?" == 125 && -f "$ENGINE_RACE_REPORT" && -f "$ENGINE_RACE_TEST_BINARY" ]] || exit 94
 fi
fi
[[ -z "$ENGINE_RACE_JOB_PID" ]] || exit 95
`
			out, err := scheduleHarnessWithMock(t, script, scheduleHarnessMock(), "MODE="+mode)
			if err != nil {
				t.Fatalf("helper completion contract: %v\n%s", err, out)
			}
		})
	}
}

func TestIssuesFallbackCompletionDrainsOwnedGroup(t *testing.T) {
	t.Parallel()
	script := `source ./run_ut.sh UT
function logger() { :; }
function fixture_cleanup() {
 if [[ -f "$CASE_DIR/fallback-owner" ]]; then
  read -r child_pid group_pid < "$CASE_DIR/fallback-owner"
  kill -TERM "$child_pid" 2>/dev/null || true
  wait_for_ut_process_group "$group_pid" 4 || true
 fi
}
trap fixture_cleanup EXIT
PREBUILT_RACE_REPORT="$CASE_DIR/issues-report"
PREBUILT_RACE_TEST_BINARY="$CASE_DIR/issues.test"
UT_TIMEOUT=17
: > "$PREBUILT_RACE_TEST_BINARY-metadata"
status=0
run_ut_command serial issues run_race_test_batches example/issues "$PREBUILT_RACE_TEST_BINARY" 2 || status=$?
read -r child_pid group_pid < "$CASE_DIR/fallback-owner"
live=no; if ut_process_group_alive "$group_pid"; then live=yes; fi
printf 'ISSUES_FALLBACK status=%s live=%s flag=%s\n' "$status" "$live" "$CURRENT_UT_DRAIN_FAILED"
[[ "$live" == no && "$status" == 0 && "$CURRENT_UT_DRAIN_FAILED" == 0 ]] || exit 90
`
	mock := `#!/bin/bash
case "$1" in
 version) exit 0 ;;
 env) exit 1 ;;
 test)
  sleep 30 &
  child_pid=$!
  group_pid=$(ps -o pgid= -p "$child_pid" | tr -d ' ')
  printf '%s %s\n' "$child_pid" "$group_pid" > "$CASE_DIR/fallback-owner"
  exit 0 ;;
 *) exit 91 ;;
esac
`
	out, err := scheduleHarnessWithMock(t, script, mock)
	if err != nil {
		t.Fatalf("completion accepted live fallback group: %v\n%s", err, out)
	}
}

func TestOuterCancellationPrebuildReportTransfer(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"transferred", "transfer-failed", "unpublished", "not-started"} {
		t.Run(mode, func(t *testing.T) {
			script := `source ./run_ut.sh UT
function logger() { :; }
function checkpoint_ut_event() { :; }
function stop_ut_heartbeat() { :; }
function snapshot_ut_diagnostics() { :; }
function report_slow_ut_cases() { :; }
function report_active_ut_cases() { :; }
CLUSTER_PREBUILD_DIR="$CASE_DIR/prebuild"
CLUSTER_PREBUILD_REPORT="$CLUSTER_PREBUILD_DIR/build"
mkdir "$CLUSTER_PREBUILD_DIR"
: > "$CLUSTER_PREBUILD_REPORT"
: > "$CLUSTER_PREBUILD_REPORT.package.0.test"
: > "$CLUSTER_PREBUILD_REPORT.package.0.meta"
PREBUILT_RACE_REPORT="$CASE_DIR/execution"
UT_REPORT="$CASE_DIR/authoritative"
case "$MODE" in
 transferred|transfer-failed)
  printf '{"Action":"pass","Package":"example/a"}\n' > "$PREBUILT_RACE_REPORT"
  ;;
 not-started) PREBUILT_RACE_REPORT="" ;;
esac
if [[ "$MODE" == transfer-failed ]]; then UT_REPORT="$CASE_DIR/missing/authoritative"; fi
check_transfer() {
 status=$?
 [[ "$status" == 143 ]] || status=90
 case "$MODE" in
  transferred|not-started)
   [[ ! -d "$CASE_DIR/prebuild" ]] || status=91
   [[ -z "$PREBUILT_RACE_REPORT$CLUSTER_PREBUILD_REPORT" ]] || status=92
   if [[ "$MODE" == transferred ]]; then
    grep -q '"Action":"pass"' "$UT_REPORT" || status=93
   fi
   ;;
  *)
   [[ -f "$CASE_DIR/prebuild/build.package.0.test" && -f "$CASE_DIR/prebuild/build.package.0.meta" ]] || status=94
   [[ -n "$PREBUILT_RACE_REPORT" && -n "$CLUSTER_PREBUILD_REPORT" ]] || status=95
   if [[ "$MODE" == transfer-failed ]]; then [[ -s "$PREBUILT_RACE_REPORT" ]] || status=96; fi
   ;;
 esac
 exit "$status"
}
trap check_transfer EXIT
handle_ut_termination
`
			out, err := scheduleHarnessWithMock(t, script, scheduleHarnessMock(), "MODE="+mode)
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 143 {
				t.Fatalf("cancellation must preserve untransferred diagnostics: %v\n%s", err, out)
			}
		})
	}
}
