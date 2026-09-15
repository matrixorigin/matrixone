// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package optools

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Source the real runner in a private repository layout. Its startup clears
// diagnostics, so sourcing it in the actual checkout would corrupt another UT.
func scheduleHarness(t *testing.T, script string, variables ...string) ([]byte, error) {
	return scheduleHarnessWithMock(t, script, `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
# The real env/go command must preserve both events around the report merge.
printf 'heavy-start\n'
printf started > "$CASE_DIR/heavy-started"
if [[ "$EXPECT_OVERLAP" == 1 ]]; then
 while [[ ! -e "$CASE_DIR/plan-started" ]]; do sleep 0.01; done
fi
printf 'heavy-end\n'
exit "$HEAVY_STATUS"
`, variables...)
}

func scheduleHarnessWithMock(t *testing.T, script, mock string, variables ...string) ([]byte, error) {
	return scheduleHarnessWithMockTransform(t, script, mock, nil, variables...)
}

func scheduleHarnessWithMockTransform(t *testing.T, script, mock string, transform func(string) string, variables ...string) ([]byte, error) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "optools")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"run_ut.sh", "utilities.sh", "ut_tools.bash", "ut_process.bash", "active_ut_cases.awk", "summarize_ut_slow_cases.py"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if name == "run_ut.sh" {
			text := string(data)
			index := strings.Index(text, "if [[ 'SCA' == $TEST_TYPE ]]; then")
			if index < 0 {
				t.Fatal("missing runner dispatch")
			}
			text = text[:index]
			if transform != nil {
				text = transform(text)
			}
			data = []byte(text)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(mock), 0755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-c", script)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 3 * time.Second
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "UT_WORKDIR="+root, "CASE_DIR="+root)
	cmd.Env = append(cmd.Env, variables...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("runner harness timed out: %s", out)
	}
	return out, err
}

func TestResolveCgroupMemoryBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, limitFile, rootLimit, parentLimit, leafLimit, wantScope, wantLimit, wantComplete string
	}{
		{"tight-parent", "memory.max", "max", "17179869184", "max", "parent", "17179869184", "1"},
		{"tight-leaf", "memory.max", "max", "17179869184", "4294967296", "leaf", "4294967296", "1"},
		{"equal-prefers-parent", "memory.max", "max", "8589934592", "8589934592", "parent", "8589934592", "1"},
		{"parent-pressure-within-looser-limit", "memory.max", "max", "17179869184", "8589934592", "leaf", "8589934592", "1"},
		{"no-visible-finite-limit", "memory.max", "max", "max", "max", "leaf", "unknown", "1"},
		{"missing-visible-ancestor", "memory.max", "max", "", "8589934592", "leaf", "unknown", "0"},
		{"local-events-option", "memory.max", "max", "max", "8589934592", "leaf", "unknown", "0"},
		{"v1-unlimited-sentinel", "memory.limit_in_bytes", "9223372036854771712", "8589934592", "9223372036854771712", "parent", "8589934592", "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "cgroup")
			parent := filepath.Join(root, "parent")
			leaf := filepath.Join(parent, "leaf")
			for _, dir := range []string{root, parent, leaf} {
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatal(err)
				}
			}
			mountInfoPath := filepath.Join(root, "mountinfo")
			if tc.limitFile == "memory.max" {
				mountOptions := "rw"
				if tc.name == "local-events-option" {
					mountOptions += ",memory_localevents"
				}
				mountInfo := "36 25 0:32 / " + root + " rw - cgroup2 cgroup " + mountOptions + "\n"
				if err := os.WriteFile(mountInfoPath, []byte(mountInfo), 0644); err != nil {
					t.Fatal(err)
				}
			}
			for dir, limit := range map[string]string{
				root:   tc.rootLimit,
				parent: tc.parentLimit,
				leaf:   tc.leafLimit,
			} {
				if limit == "" {
					continue
				}
				if err := os.WriteFile(filepath.Join(dir, tc.limitFile), []byte(limit+"\n"), 0644); err != nil {
					t.Fatal(err)
				}
				if tc.limitFile == "memory.max" && limit != "max" {
					peak := "2048"
					if tc.name == "parent-pressure-within-looser-limit" {
						if dir == parent {
							peak = "17000000000"
						} else if dir == leaf {
							peak = "6442450944"
						}
					}
					for name, value := range map[string]string{
						"memory.current": "1024",
						"memory.peak":    peak,
						"memory.events":  "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
					} {
						if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0644); err != nil {
							t.Fatal(err)
						}
					}
				} else if tc.limitFile == "memory.limit_in_bytes" && limit == "8589934592" {
					for name, value := range map[string]string{
						"memory.usage_in_bytes":     "1024",
						"memory.max_usage_in_bytes": "2048",
						"memory.failcnt":            "0",
					} {
						if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0644); err != nil {
							t.Fatal(err)
						}
					}
				}
			}

			script := `source ./run_ut.sh UT
resolve_cgroup_memory_boundary "$CGROUP_TEST_LEAF" "$CGROUP_TEST_ROOT" "$CGROUP_LIMIT_FILE" "$CGROUP_TEST_MOUNTINFO"
printf 'scope=%s limit=%s complete=%s events_hierarchical=%s boundaries=%s\n' "$CGROUP_MEMORY_PATH" "$CGROUP_MEMORY_LIMIT" "$CGROUP_MEMORY_HIERARCHY_COMPLETE" "$CGROUP_MEMORY_EVENTS_HIERARCHICAL" "$CGROUP_MEMORY_HIERARCHY"
`
			out, err := scheduleHarness(t, script,
				"CGROUP_TEST_ROOT="+root,
				"CGROUP_TEST_LEAF="+leaf,
				"CGROUP_LIMIT_FILE="+tc.limitFile,
				"CGROUP_TEST_MOUNTINFO="+mountInfoPath,
			)
			if err != nil {
				t.Fatalf("resolve cgroup memory boundary: %v\n%s", err, out)
			}
			wantPath := parent
			if tc.wantScope == "leaf" {
				wantPath = leaf
			}
			wantEvents := "1"
			if tc.limitFile == "memory.limit_in_bytes" {
				wantEvents = "not_applicable"
			} else if tc.name == "local-events-option" {
				wantEvents = "0"
			}
			wantPrefix := "scope=" + wantPath + " limit=" + tc.wantLimit + " complete=" + tc.wantComplete + " events_hierarchical=" + wantEvents + " boundaries="
			if !strings.HasPrefix(string(out), wantPrefix) {
				t.Fatalf("unexpected boundary:\n got: %q\nwant prefix: %q", out, wantPrefix)
			}
			if tc.name == "parent-pressure-within-looser-limit" {
				parentBoundary := parent + "|17179869184|1024|17000000000|oom=0,oom_kill=0"
				leafBoundary := leaf + "|8589934592|1024|6442450944|oom=0,oom_kill=0"
				if !strings.Contains(string(out), parentBoundary) || !strings.Contains(string(out), leafBoundary) {
					t.Fatalf("did not retain metrics for both constraining ancestors:\n%s", out)
				}
			}
		})
	}
}

func TestHeavyPlanReusesReleasedEngineCapacity(t *testing.T) {
	for _, tc := range []struct{ name, budget, overlap, engine, heavy, plan, expected string }{
		{"default", "3", "", "0", "0", "0", "0"},
		{"overlap", "3", "1", "0", "0", "0", "0"},
		{"engine-failure", "3", "1", "7", "0", "0", "1"},
		{"heavy-failure", "3", "1", "0", "8", "0", "1"},
		{"plan-failure", "3", "1", "0", "0", "9", "1"},
		{"sequential-baseline", "3", "0", "0", "0", "0", "0"},
		{"one-slot", "1", "1", "0", "0", "0", "0"},
		{"two-slots", "2", "1", "0", "0", "0", "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectedOverlap := "0"
			if tc.budget == "3" && tc.overlap != "0" {
				expectedOverlap = "1"
			}
			script := `source ./run_ut.sh UT
trap handle_ut_termination TERM
function logger() { :; }
function report_cgroup_memory_usage() {
 if [[ "$1" == "Final race UT" ]]; then
  [[ -z "$CURRENT_UT_PID$LIGHT_RACE_JOB_PID$ENGINE_RACE_JOB_PID$PLAN_RACE_JOB_PID$CLUSTER_PREBUILD_JOB_PID" ]] || exit 97
  [[ -z "$ENGINE_RACE_REPORT$PLAN_RACE_REPORT" ]] || exit 98
  touch "$CASE_DIR/final-memory"
 fi
}
function make() { :; }
function egrep() { echo fake.pb.go; }
# Skip the unrelated native smoke, while retaining the real race scheduler.
MO_CL_CUDA=1
UT_SHARD=heavy-plan
function list_embedded_cluster_test_packages() { echo example/embed; }
function go() {
 if [[ "$1" == clean ]]; then return 0; fi
 shift 2
 if [[ "$1" == ./... ]]; then
  printf '%s\n' example/embed github.com/matrixorigin/matrixone/pkg/{sql/plan,vm/engine/test,vectorindex/hnsw,tests/issues,backup,fileservice,sql/plan/function,vm/engine/tae/db/test}
 else
  for package in "$@"; do echo "github.com/matrixorigin/matrixone/${package#./}"; done
 fi
}
function run_engine_race_shards() {
 mkdir "$CASE_DIR/engine-once" || return 90
 while [[ ! -e "$CASE_DIR/heavy-started" ]]; do sleep 0.01; done
 printf 'engine\n' > "$ENGINE_RACE_REPORT"
 touch "$CASE_DIR/engine-finished"
 return "$ENGINE_STATUS"
}
function run_plan_race_shards() {
 mkdir "$CASE_DIR/plan-once" || return 91
 [[ -e "$CASE_DIR/engine-finished" ]] || return 92
 printf 'plan\n' > "$PLAN_RACE_REPORT"
 touch "$CASE_DIR/plan-started"
 return "$PLAN_STATUS"
}
run_tests
[[ "$UT_TEST_STATUS" == "$EXPECTED_STATUS" ]] || exit 93
[[ -d "$CASE_DIR/engine-once" && -d "$CASE_DIR/plan-once" ]] || exit 94
[[ -z "$CURRENT_UT_PID$ENGINE_RACE_JOB_PID$PLAN_RACE_JOB_PID" ]] || exit 95
[[ -e "$CASE_DIR/final-memory" ]] || exit 96
printf '\nREPORT\n'
cat "$UT_REPORT"
`
			out, err := scheduleHarness(t, script, "HEAVY_RACE_PARALLEL="+tc.budget, "UT_OVERLAP_PLAN="+tc.overlap, "ENGINE_STATUS="+tc.engine, "HEAVY_STATUS="+tc.heavy, "PLAN_STATUS="+tc.plan, "EXPECTED_STATUS="+tc.expected, "EXPECT_OVERLAP="+expectedOverlap)
			if err != nil {
				t.Fatalf("schedule: %v\n%s", err, out)
			}
			if !strings.HasSuffix(string(out), "REPORT\nheavy-start\nheavy-end\nengine\nplan\n") {
				t.Fatalf("lost or duplicated report events:\n%s", out)
			}
		})
	}
}

func TestHeavyPlanCancellationStopsWritersBeforeMerge(t *testing.T) {
	script := `source ./run_ut.sh UT
function logger() { :; }
UT_HELPER_TERM_GRACE_TICKS=4
trap handle_ut_termination TERM
trap 'printf "\nCANCEL_REPORT\n"; cat "$UT_REPORT"' EXIT
ENGINE_RACE_REPORT="$CASE_DIR/engine-report"
printf 'engine\n' > "$ENGINE_RACE_REPORT"
start_ut_command heavy 'heavy writer' bash -c '
 trap '\''printf "heavy-stopped\n"; exit 143'\'' TERM
 printf "heavy-start\n"
 touch "$CASE_DIR/heavy-ready"
 while :; do sleep 0.01; done
'
function run_plan_race_shards() {
 trap 'printf "plan-stopped\n" >> "$PLAN_RACE_REPORT"; exit 143' TERM
 printf 'plan-start\n' > "$PLAN_RACE_REPORT"
 touch "$CASE_DIR/plan-ready"
 while :; do sleep 0.01; done
}
start_plan_race example/plan
while [[ ! -e "$CASE_DIR/heavy-ready" || ! -e "$CASE_DIR/plan-ready" ]]; do sleep 0.01; done
kill -TERM "$$"
`
	out, err := scheduleHarness(t, script)
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 143 {
		t.Fatalf("expected TERM exit 143: %v\n%s", err, out)
	}
	if !strings.HasSuffix(string(out), "CANCEL_REPORT\nheavy-start\nheavy-stopped\nplan-start\nplan-stopped\nengine\n") {
		t.Fatalf("writer was not stopped before merging: %s", out)
	}
}

func TestCancellationReportsCompletedSlowCases(t *testing.T) {
	script := `source ./run_ut.sh UT
function logger() { printf "%s\n" "$2"; }
UT_REPORT="$CASE_DIR/report.json"
UT_DIAGNOSTIC_DIR="$CASE_DIR/diagnostics"
mkdir -p "$UT_DIAGNOSTIC_DIR"
cat > "$UT_REPORT" <<'EOF'
{"Time":"2026-09-10T01:00:01Z","Action":"run","Package":"example/slow","Test":"TestSlow"}
{"Time":"2026-09-10T01:00:04Z","Action":"pass","Package":"example/slow","Test":"TestSlow","Elapsed":3.5}
{"Time":"2026-09-10T01:00:05Z","Action":"run","Package":"example/slow","Test":"TestBlocked"}
EOF
trap handle_ut_termination TERM
set +e
( handle_ut_termination )
status=$?
set -e
[[ "$status" == 143 ]] || exit 90
[[ -s "$UT_DIAGNOSTIC_DIR/top.txt" ]] || exit 91
[[ -s "$UT_DIAGNOSTIC_DIR/ut-report.json" ]] || exit 92
[[ -s "$UT_DIAGNOSTIC_DIR/ut-checkpoint.log" ]] || exit 93
grep -q 'TestSlow' "$UT_DIAGNOSTIC_DIR/top.txt"
`
	out, err := scheduleHarnessWithMock(t, script, `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
exit 0
`)
	if err != nil {
		t.Fatalf("cancellation diagnostics: %v\n%s", err, out)
	}
}

func TestUTHeartbeatStopsCleanly(t *testing.T) {
	script := `source ./run_ut.sh UT
mkfifo "$CASE_DIR/heartbeat-ready"
exec 9<> "$CASE_DIR/heartbeat-ready"
trap 'stop_ut_heartbeat; exec 9>&-' EXIT
function logger() {
    printf "%s\n" "$2" >> "$CASE_DIR/ut.log"
    if [[ "$2" == *active_cases=2* && "$2" == *TestPrivate* && "$2" == *TestShard* ]]; then
        printf 'ready\n' >&9
    fi
}
UT_HEARTBEAT_INTERVAL=60
start_ut_heartbeat
heartbeat_pid="$UT_HEARTBEAT_PID"
stop_ut_heartbeat
# The enclosing harness deadline is shorter than this 60-second interval.
# Completion proves cancellation without making host scheduling speed an oracle.
[[ -z "$UT_HEARTBEAT_PID" ]] || exit 90
! kill -0 "$heartbeat_pid" 2>/dev/null || exit 90

UT_HEARTBEAT_INTERVAL=1
start_ut_heartbeat
LIGHT_RACE_REPORT="$G_WKSP/${G_TS}-light-race-report.out"
cat > "$LIGHT_RACE_REPORT" <<'EOF'
{"Time":"2026-09-10T01:00:01Z","Action":"run","Package":"example/light","Test":"TestPrivate"}
EOF
cat > "$G_WKSP/${G_TS}-engine-race-report.out.1" <<'EOF'
{"Time":"2026-09-10T01:00:01Z","Action":"run","Package":"example/engine","Test":"TestShard"}
EOF
read -r -t 10 heartbeat_ready <&9 || exit 92
stop_ut_heartbeat
exec 9>&-
[[ -z "$UT_HEARTBEAT_PID" ]] || exit 91
grep -q 'event=heartbeat' "$UT_CHECKPOINT"
grep -q 'active_cases=2' "$CASE_DIR/ut.log"
grep -q 'TestPrivate' "$CASE_DIR/ut.log"
grep -q 'TestShard' "$CASE_DIR/ut.log"
`
	mock := `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
exit 0
	`
	out, err := scheduleHarnessWithMock(t, script, mock)
	if err != nil {
		t.Fatalf("heartbeat lifecycle: %v\n%s", err, out)
	}
}

func TestUTHeartbeatStopDuringSleepRegistration(t *testing.T) {
	script := `source ./run_ut.sh UT
mkfifo "$CASE_DIR/heartbeat-child"
mkfifo "$CASE_DIR/heartbeat-release"
mkfifo "$CASE_DIR/timer-input"
exec 7<> "$CASE_DIR/heartbeat-child"
exec 8<> "$CASE_DIR/heartbeat-release"
exec 9<> "$CASE_DIR/timer-input"
timer_pid=""
heartbeat_pid=""
cleanup() {
    if [[ -z "$timer_pid" && -f "$CASE_DIR/heartbeat-child.pid" ]]; then
        timer_pid=$(cat "$CASE_DIR/heartbeat-child.pid")
    fi
    # Unblock the injected launch-window hook before terminating its owner.
    printf 'cleanup\n' >&8 2>/dev/null || true
    if [[ -n "$timer_pid" ]]; then
        kill -KILL "$timer_pid" 2>/dev/null || true
    fi
    if [[ -n "$heartbeat_pid" ]]; then
        kill -KILL "$heartbeat_pid" 2>/dev/null || true
        wait "$heartbeat_pid" 2>/dev/null || true
    fi
    timer_pid=""
    heartbeat_pid=""
    exec 7>&-
    exec 8>&-
    exec 9>&-
}
trap cleanup EXIT

# The long timer is deliberately TERM-ignoring and has no cleanup contract.
# The production stop path must use exact-PID KILL and then reap it. The short
# polling sleeps used by stop_ut_heartbeat remain the system sleep command.
sleep() {
    if [[ "${1:-}" == 60 ]]; then
        trap '' TERM INT
        while :; do
            IFS= read -r _ <&9 || true
        done
    fi
    command sleep "$@"
}

# scheduleHarnessWithMockTransform injects this function call between the
# timer spawn and production PID registration. It publishes the child PID,
# then waits for the parent to deliver TERM and release the launch window.
ut_test_before_heartbeat_sleep_registration() {
    printf '%s\n' "$1" > "$CASE_DIR/heartbeat-child.pid"
    printf '%s\n' "$1" >&7
    while ! read -r _ <&8; do
        :
    done
}

UT_HEARTBEAT_INTERVAL=60
start_ut_heartbeat
heartbeat_pid="$UT_HEARTBEAT_PID"
read -r -t 10 timer_pid <&7 || exit 90
[[ "$timer_pid" =~ ^[0-9]+$ ]] || exit 91
kill "-${HEARTBEAT_STOP_SIGNAL}" "$heartbeat_pid" || exit 92
printf 'release\n' >&8
stop_ut_heartbeat
[[ -z "$UT_HEARTBEAT_PID" ]] || exit 93
if kill -0 "$heartbeat_pid" 2>/dev/null; then
    exit 94
fi
heartbeat_pid=""
if kill -0 "$timer_pid" 2>/dev/null; then
    exit 95
fi
rm -f "$CASE_DIR/heartbeat-child.pid"
timer_pid=""
`
	mock := `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
exit 0
	`
	transform := func(text string) string {
		const anchor = "            heartbeat_sleep_pid=$!\n"
		if got := strings.Count(text, anchor); got != 1 {
			t.Fatalf("heartbeat registration anchor count = %d, want 1", got)
		}
		return strings.Replace(text, anchor,
			"            ut_test_before_heartbeat_sleep_registration \"$!\"\n"+anchor, 1)
	}
	for _, signal := range []string{"TERM", "INT"} {
		t.Run(strings.ToLower(signal), func(t *testing.T) {
			out, err := scheduleHarnessWithMockTransform(t, script, mock, transform,
				"HEARTBEAT_STOP_SIGNAL="+signal)
			if err != nil {
				t.Fatalf("heartbeat registration lifecycle: %v\n%s", err, out)
			}
		})
	}
}

func TestUTHeartbeatStopDuringHelperRegistration(t *testing.T) {
	script := `source ./run_ut.sh UT
mkfifo "$CASE_DIR/heartbeat-ready"
exec 7<> "$CASE_DIR/heartbeat-ready"
helper_pid=""
timer_pid=""
restart_pid=""
caller_term_count=0
caller_seen_pid=""
publication_count=0
cleanup() {
    if [[ -z "$timer_pid" && -f "$CASE_DIR/heartbeat-timer.pid" ]]; then
        timer_pid=$(cat "$CASE_DIR/heartbeat-timer.pid")
    fi
    if [[ -z "$helper_pid" && -f "$CASE_DIR/heartbeat-helper.pid" ]]; then
        helper_pid=$(cat "$CASE_DIR/heartbeat-helper.pid")
    fi
    if [[ -n "$timer_pid" ]]; then
        kill -KILL "$timer_pid" 2>/dev/null || true
    fi
    if [[ -n "$helper_pid" ]]; then
        kill -KILL "$helper_pid" 2>/dev/null || true
        wait "$helper_pid" 2>/dev/null || true
    fi
    if [[ -n "$restart_pid" ]]; then
        kill -KILL "$restart_pid" 2>/dev/null || true
        wait "$restart_pid" 2>/dev/null || true
    fi
    timer_pid=""
    helper_pid=""
    restart_pid=""
    exec 7>&-
}
trap cleanup EXIT

function logger() { :; }
caller_term() {
    caller_term_count=$((caller_term_count + 1))
    caller_seen_pid="$UT_HEARTBEAT_PID"
    stop_ut_heartbeat
}
trap caller_term TERM

# The transformed helper reports its registered timer before the parent
# launch hook sends TERM. This keeps the outer publication race deterministic
# while also proving the published helper owns and reaps its timer.
ut_test_heartbeat_ready() {
    printf '%s\n' "$heartbeat_sleep_pid" > "$CASE_DIR/heartbeat-timer.pid"
    printf '%s\n' "$heartbeat_sleep_pid" >&7
}
ut_test_before_heartbeat_registration() {
    publication_count=$((publication_count + 1))
    helper_pid="$1"
    printf '%s\n' "$1" > "$CASE_DIR/heartbeat-helper.pid"
    read -r -t 10 timer_pid <&7 || exit 90
    [[ "$timer_pid" =~ ^[0-9]+$ ]] || exit 91
    if [[ "$publication_count" == 1 ]]; then
        kill -TERM "$$"
    fi
}

UT_HEARTBEAT_INTERVAL=60
start_ut_heartbeat
[[ "$caller_term_count" == 1 ]] || exit 92
[[ "$caller_seen_pid" == "$helper_pid" && -n "$caller_seen_pid" ]] || exit 93
[[ -z "$UT_HEARTBEAT_PID" ]] || exit 94
if kill -0 "$helper_pid" 2>/dev/null; then
    exit 95
fi
rm -f "$CASE_DIR/heartbeat-helper.pid"
helper_pid=""
if kill -0 "$timer_pid" 2>/dev/null; then
    exit 96
fi
rm -f "$CASE_DIR/heartbeat-timer.pid"
timer_pid=""
[[ "$(trap -p TERM)" == *caller_term* ]] || exit 97

# The publication fence must not poison a later generation after cancellation.
start_ut_heartbeat
restart_pid="$UT_HEARTBEAT_PID"
[[ -n "$restart_pid" ]] || exit 98
[[ "$helper_pid" == "$restart_pid" ]] || exit 99
# Transfer the second-generation owner token to restart_pid before any
# operation can fail, so EXIT cleanup never owns the same PID twice.
rm -f "$CASE_DIR/heartbeat-helper.pid"
helper_pid=""
stop_ut_heartbeat
[[ -z "$UT_HEARTBEAT_PID" ]] || exit 100
if kill -0 "$restart_pid" 2>/dev/null; then
    exit 101
fi
rm -f "$CASE_DIR/heartbeat-helper.pid"
restart_pid=""
if kill -0 "$timer_pid" 2>/dev/null; then
    exit 102
fi
rm -f "$CASE_DIR/heartbeat-timer.pid"
timer_pid=""
`
	mock := `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
exit 0
	`
	transform := func(text string) string {
		const readyAnchor = "            heartbeat_sleep_pid=$!\n"
		if got := strings.Count(text, readyAnchor); got != 1 {
			t.Fatalf("heartbeat timer registration anchor count = %d, want 1", got)
		}
		text = strings.Replace(text, readyAnchor,
			readyAnchor+"            ut_test_heartbeat_ready\n", 1)
		const publicationAnchor = "    UT_HEARTBEAT_PID=$!\n"
		if got := strings.Count(text, publicationAnchor); got != 1 {
			t.Fatalf("heartbeat publication anchor count = %d, want 1", got)
		}
		return strings.Replace(text, publicationAnchor,
			"    ut_test_before_heartbeat_registration \"$!\"\n"+publicationAnchor, 1)
	}
	out, err := scheduleHarnessWithMockTransform(t, script, mock, transform)
	if err != nil {
		t.Fatalf("heartbeat helper registration lifecycle: %v\n%s", err, out)
	}
}

func TestLightIssuesOverlapPreservesReportsAndFailures(t *testing.T) {
	mock := `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
if [[ "$*" == *example/light-package* ]]; then
 printf 'light-start\n'
 touch "$CASE_DIR/light-started"
 while [[ ! -e "$CASE_DIR/serial-started" ]]; do sleep 0.01; done
 printf 'light-end\n'
 exit "$LIGHT_STATUS"
fi
exit 99
`
	for _, tc := range []struct {
		name, lightStatus, serialStatus string
	}{
		{name: "success", lightStatus: "0", serialStatus: "0"},
		{name: "light-failure", lightStatus: "7", serialStatus: "0"},
		{name: "serial-failure", lightStatus: "0", serialStatus: "9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			script := `source ./run_ut.sh UT
function logger() { :; }
trap handle_ut_termination TERM
start_light_race example/light-package 2
start_ut_command serial 'exclusive issues package' bash -c '
 printf "serial-start\n"
 touch "$CASE_DIR/serial-started"
 while [[ ! -e "$CASE_DIR/light-started" ]]; do sleep 0.01; done
 printf "serial-end\n"
 exit "$SERIAL_STATUS"
'
serial_status=0
finish_ut_command || serial_status=$?
light_status=0
finish_light_race || light_status=$?
[[ "$serial_status" == "$SERIAL_STATUS" ]] || exit 90
[[ "$light_status" == "$LIGHT_STATUS" ]] || exit 91
[[ -z "$CURRENT_UT_PID$LIGHT_RACE_JOB_PID$LIGHT_RACE_REPORT" ]] || exit 92
report=$(cat "$UT_REPORT")
[[ "$report" == $'serial-start\nserial-end\nlight-start\nlight-end' ]] || { printf 'REPORT=%q\n' "$report"; exit 93; }
`
			out, err := scheduleHarnessWithMock(t, script, mock,
				"LIGHT_STATUS="+tc.lightStatus, "SERIAL_STATUS="+tc.serialStatus)
			if err != nil {
				t.Fatalf("overlap: %v\n%s", err, out)
			}
		})
	}
}

func TestLightIssuesCancellationStopsWritersBeforeMerge(t *testing.T) {
	mock := `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
if [[ "$*" == *example/light-package* ]]; then
 trap 'printf "light-stopped\\n"; exit 143' TERM
 printf 'light-start\n'
 touch "$CASE_DIR/light-started"
 while :; do sleep 0.01; done
fi
exit 99
`
	script := `source ./run_ut.sh UT
function logger() { :; }
UT_HELPER_TERM_GRACE_TICKS=4
trap handle_ut_termination TERM
trap 'printf "\nCANCEL_REPORT\n"; cat "$UT_REPORT"' EXIT
start_light_race example/light-package 2
start_ut_command serial 'exclusive issues package' bash -c '
 trap '\''printf "serial-stopped\n"; exit 143'\'' TERM
 printf "serial-start\n"
 touch "$CASE_DIR/serial-started"
 while :; do sleep 0.01; done
'
while [[ ! -e "$CASE_DIR/light-started" || ! -e "$CASE_DIR/serial-started" ]]; do sleep 0.01; done
kill -TERM "$$"
`
	out, err := scheduleHarnessWithMock(t, script, mock)
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 143 {
		t.Fatalf("expected TERM exit 143: %v\n%s", err, out)
	}
	if !strings.HasSuffix(string(out), "CANCEL_REPORT\nserial-start\nserial-stopped\nlight-start\nlight-stopped\n") {
		t.Fatalf("writer was not stopped before merging: %s", out)
	}
}

func TestRunTestsPlacesHNSWBeforeLightIssuesOverlap(t *testing.T) {
	mock := `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
if [[ "$*" == *pkg/vectorindex/hnsw* ]]; then
 printf 'hnsw\n'
 touch "$CASE_DIR/hnsw-done"
 exit 0
fi
if [[ "$*" == *pkg/light-package* ]]; then
 if [[ "$EXPECT_OVERLAP" == 1 ]]; then
  [[ -e "$CASE_DIR/hnsw-done" ]] || exit 81
 fi
 printf 'light\n'
 touch "$CASE_DIR/light-started"
 if [[ "$EXPECT_OVERLAP" == 1 ]]; then
  while [[ ! -e "$CASE_DIR/serial-started" ]]; do sleep 0.01; done
 fi
 printf 'light-end\n'
 exit 0
fi
if [[ "$*" == *pkg/tests/issues* ]]; then
 printf 'serial\n'
 touch "$CASE_DIR/serial-started"
 if [[ "$EXPECT_OVERLAP" == 1 ]]; then
  while [[ ! -e "$CASE_DIR/light-started" ]]; do sleep 0.01; done
 fi
 printf 'serial-end\n'
 exit 0
fi
if [[ "$*" == *pkg/tests/embedded* ]]; then printf 'embedded\n'; exit 0; fi
if [[ "$*" == *pkg/backup* ]]; then printf 'heavy\n'; exit 0; fi
exit 0
`
	for _, tc := range []struct {
		name, parallel, overlap, expectOverlap, expected string
	}{
		{name: "default-off", parallel: "6", overlap: "__default__", expectOverlap: "0", expected: "light\nlight-end\nhnsw\nserial\nserial-end"},
		{name: "overlap", parallel: "6", overlap: "1", expectOverlap: "1", expected: "hnsw\nserial\nserial-end\nlight\nlight-end"},
		{name: "sequential-explicit-off", parallel: "6", overlap: "0", expectOverlap: "0", expected: "light\nlight-end\nhnsw\nserial\nserial-end"},
		{name: "sequential-single-slot", parallel: "1", overlap: "0", expectOverlap: "0", expected: "light\nlight-end\nhnsw\nserial\nserial-end"},
		{name: "single-slot-guard", parallel: "1", overlap: "1", expectOverlap: "0", expected: "light\nlight-end\nhnsw\nserial\nserial-end"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			script := `if [[ "$UT_OVERLAP_VALUE" == "__default__" ]]; then unset UT_OVERLAP_LIGHT; fi
source ./run_ut.sh UT
function logger() { :; }
function make() { :; }
function egrep() { echo fake.pb.go; }
	MO_CL_CUDA=1
	UT_SHARD=all
UT_PARALLEL=${UT_PARALLEL_VALUE}
if [[ "$UT_OVERLAP_VALUE" != "__default__" ]]; then UT_OVERLAP_LIGHT=${UT_OVERLAP_VALUE}; fi
UT_OVERLAP_LIGHT_PARALLEL=2
UT_OVERLAP_PLAN=0
UT_PREBUILD_EMBEDDED=0
function go() {
 if [[ "$1" == clean ]]; then return 0; fi
 if [[ "$1" != list ]]; then return 0; fi
 if [[ "$*" == *"./pkg/sql/plan"* ]]; then echo github.com/matrixorigin/matrixone/pkg/sql/plan; return 0; fi
 if [[ "$*" == *"./pkg/vm/engine/test"* ]]; then echo github.com/matrixorigin/matrixone/pkg/vm/engine/test; return 0; fi
 if [[ "$*" == *"./pkg/vectorindex/hnsw"* ]]; then echo github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw; return 0; fi
 if [[ "$*" == *"./pkg/tests/issues"* ]]; then echo github.com/matrixorigin/matrixone/pkg/tests/issues; return 0; fi
 if [[ "$*" == *"./pkg/backup"* ]]; then echo github.com/matrixorigin/matrixone/pkg/backup; return 0; fi
 if [[ "$*" == *"./..."* ]]; then
  printf '%s\n' github.com/matrixorigin/matrixone/pkg/{sql/plan,vm/engine/test,vectorindex/hnsw,tests/issues,tests/embedded,light-package,backup}
 fi
}
function list_embedded_cluster_test_packages() { echo github.com/matrixorigin/matrixone/pkg/tests/embedded; }
function run_engine_race_shards() { printf 'engine\n' > "$ENGINE_RACE_REPORT"; return 0; }
function run_plan_race_shards() { return 0; }
trap handle_ut_termination TERM
run_tests
[[ "$UT_TEST_STATUS" == 0 ]] || exit 90
[[ -z "$CURRENT_UT_PID$LIGHT_RACE_JOB_PID$ENGINE_RACE_JOB_PID$PLAN_RACE_JOB_PID" ]] || exit 91
report=$(cat "$UT_REPORT")
[[ "$report" == *"$EXPECTED_REPORT"* ]] || { printf 'REPORT=%q\n' "$report"; exit 92; }
`
			out, err := scheduleHarnessWithMock(t, script, mock,
				"UT_PARALLEL_VALUE="+tc.parallel,
				"UT_OVERLAP_VALUE="+tc.overlap,
				"EXPECT_OVERLAP="+tc.expectOverlap,
				"EXPECTED_REPORT="+tc.expected)
			if err != nil {
				t.Fatalf("scheduler: %v\n%s", err, out)
			}
		})
	}
}
