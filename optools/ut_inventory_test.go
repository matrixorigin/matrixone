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

func TestRaceInventoryCancellationRetainsFailedDrainDiagnostics(t *testing.T) {
	transform := func(text string) string {
		const anchor = "    pid=$!\n    set +m\n"
		if strings.Count(text, anchor) != 1 {
			t.Fatalf("inventory pid handoff anchor count = %d, want 1", strings.Count(text, anchor))
		}
		return strings.Replace(text, anchor,
			"    pid=$!\n    ut_test_inventory_after_pid\n    set +m\n", 1)
	}
	script := `source ./run_ut.sh UT
function logger() { :; }
original_drain_helper=$(declare -f wait_for_ut_process_group)
owned_pid=""
function ut_test_inventory_after_pid() {
 owned_pid=$pid
 IFS= read -r -t 5 ready <&9 || exit 94
 [[ "$ready" == ready ]] || exit 94
 kill -TERM "$$"
}
function wait_for_ut_process_group() { touch "$CASE_DIR/drain-checked"; return 1; }
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
mkfifo "$CASE_DIR/list-ready" "$CASE_DIR/list-hold"
exec 8<>"$CASE_DIR/list-hold" 9<>"$CASE_DIR/list-ready"
cleanup() {
 local result=$?
 if [[ -n "$owned_pid" ]]; then
  printf 'release\n' >&8
  terminate_ut_process_group "$owned_pid" KILL
  eval "$original_drain_helper"
  if wait_for_ut_process_group "$owned_pid" 1; then
   wait "$owned_pid" 2>/dev/null || true
  else
   result=96
  fi
 fi
 exec 8>&- 9>&-
 exit "$result"
}
trap cleanup EXIT
trap 'touch "$CASE_DIR/term-restored"' TERM
cat > "$CASE_DIR/list.sh" <<'EOF'
#!/bin/bash
trap '' TERM
printf 'discovery diagnostic\n'
printf 'ready\n' >&9
IFS= read -r _ <&8
EOF
chmod +x "$CASE_DIR/list.sh"
status=0
run_race_inventory_with_deadline "$CASE_DIR" "$CASE_DIR/list.sh" "$CASE_DIR/inventory" "$(( $(date +%s) + 10 ))" || status=$?
[[ "$status" == 125 ]] || exit 90
[[ -e "$CASE_DIR/drain-checked" ]] || exit 91
grep -qx 'discovery diagnostic' "$CASE_DIR/inventory" || exit 92
# Failed drainage returns without joining or deleting the live writer's output.
ut_process_group_alive "$owned_pid" || exit 93
kill -TERM "$$"
[[ -e "$CASE_DIR/term-restored" ]] || exit 95
`
	out, err := scheduleHarnessWithMockTransform(t, script, scheduleHarnessMock(), transform)
	if err != nil {
		t.Fatalf("failed discovery drain must return promptly with diagnostics: %v\n%s", err, out)
	}
}

func TestPrebuiltRaceDeadlineFailureIsReported(t *testing.T) {
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
(exit 125) &
CURRENT_UT_PID=$!
CURRENT_UT_LABEL='batched issues'
handle_ut_termination
`
	out, err := scheduleHarnessWithMock(t, script, scheduleHarnessMock())
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 125 {
		t.Fatalf("failed-drain helper must retain report ownership: %v\n%s", err, out)
	}
}

func TestIssuesDiscoveryFallbackUsesRemainingDeadline(t *testing.T) {
	script := `source ./run_ut.sh UT
function logger() { :; }
UT_TIMEOUT=1
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
run_issues_race_batches example/issues "$CASE_DIR/issues.test" 4 || status=$?
[[ "$status" == 0 ]] || exit 90
grep -q -- '-timeout 10s' "$fallback_args" || { cat "$fallback_args"; exit 91; }
printf 'exhausted\n' > "$date_mode"
printf '0\n' > "$date_state"
rm -f "$fallback_args"
status=0
run_issues_race_batches example/issues "$CASE_DIR/issues-exhausted.test" 4 || status=$?
[[ "$status" == 124 ]] || exit 92
[[ ! -e "$fallback_args" ]] || exit 93
`
	out, err := scheduleHarnessWithMock(t, script, mock)
	if err != nil {
		t.Fatalf("discovery fallback did not carry the remaining deadline: %v\n%s", err, out)
	}
}

func TestIssuesBatchesUseBoundedProcessPool(t *testing.T) {
	script := `source ./run_ut.sh UT
function logger() { :; }
UT_ISSUES_BATCH_PARALLEL=2
printf 'package fixture\n' > "$CASE_DIR/TestFile.go"
mock_binary="$CASE_DIR/issues.test"
status=0
run_issues_race_batches example/issues "$mock_binary" 2 || status=$?
[[ "$status" == 0 ]] || exit 90
for name in one two; do
 [[ "$(<"$CASE_DIR/pool-$name")" == 2 ]] || exit 91
 [[ -e "$CASE_DIR/start-$name" ]] || exit 92
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
  output=''
  while (( $# > 0 )); do
   if [[ "$1" == -o ]]; then output=$2; break; fi
   shift
  done
  [[ -n "$output" ]] || exit 4
  cat > "$output" <<'EOF'
#!/bin/bash
if [[ "$*" == *-test.list=* ]]; then
 printf 'TestOne\nTestTwo\n'
 exit 0
fi
if [[ "$*" == *TestOne* ]]; then
 name=one
else
 name=two
fi
printf '%s\n' "${MO_TEST_CLUSTER_ADMISSION_POOL_SIZE:-unset}" > "$CASE_DIR/pool-$name"
touch "$CASE_DIR/start-$name"
for _ in $(seq 1 100); do
 [[ -e "$CASE_DIR/start-one" && -e "$CASE_DIR/start-two" ]] && exit 0
 sleep 0.01
done
exit 7
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
	out, err := scheduleHarnessWithMock(t, script, mock)
	if err != nil {
		t.Fatalf("issues batches did not run two admitted processes: %v\n%s", err, out)
	}
}

func TestRaceInventoryPreservesRunnableRootsAndStress(t *testing.T) {
	root := t.TempDir()
	writeScopeFixture(t, root, "go.mod", "module inventoryfixture\n\ngo 1.27.0\n")
	writeScopeFixture(t, root, "inventory_test.go", `package inventoryfixture
import ("fmt"; "os"; "testing")
func record(name string) {
 f, err := os.OpenFile(os.Getenv("INVENTORY_RECORD"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
 if err != nil { panic(err) }; defer f.Close()
 if _,err = fmt.Fprintln(f,name); err != nil { panic(err) }
}
func TestOne(t *testing.T) { record("one") }
func TestTwentyRounds(t *testing.T) {
 for i:=0;i<20;i++ { t.Run(fmt.Sprint(i),func(t *testing.T) { record("round") }) }
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
	build := exec.CommandContext(t.Context(), "go", "test", "-race", "-c", "-o", binary)
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
	want := map[string]int{"one": 1, "round": 20, "seed:a": 1, "seed:b": 1, "seed:c": 1, "example": 1}
	if !reflect.DeepEqual(control, want) || !reflect.DeepEqual(batched, control) {
		t.Fatalf("runtime coverage/stress changed: control=%v batched=%v want=%v", control, batched, want)
	}
}
