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
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "optools")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"run_ut.sh", "utilities.sh", "ut_tools.bash", "ut_process.bash", "active_ut_cases.awk"} {
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
			data = []byte(text[:index])
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0755); err != nil {
			t.Fatal(err)
		}
	}
	mock := `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
# The real env/go command must preserve both events around the report merge.
printf 'heavy-start\n'
printf started > "$CASE_DIR/heavy-started"
if [[ "$EXPECT_OVERLAP" == 1 ]]; then
 while [[ ! -e "$CASE_DIR/plan-started" ]]; do sleep 0.01; done
fi
printf 'heavy-end\n'
exit "$HEAVY_STATUS"
`
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
function report_cgroup_memory_usage() { :; }
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
