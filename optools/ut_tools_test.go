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
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const goUTAnalysisModule = "github.com/matrixorigin/go-ut-analysis@v0.0.0-20250711025253-f31acb12d3b1"

func writeRunUTPrelude(t *testing.T) string {
	t.Helper()

	path, err := filepath.Abs("run_ut.sh")
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	marker := []byte("if [[ 'SCA' == $TEST_TYPE ]]; then")
	index := bytes.Index(contents, marker)
	if index < 0 {
		t.Fatal("run_ut.sh main dispatch marker not found")
	}
	prelude := filepath.Join(t.TempDir(), "run_ut-prelude.sh")
	if err := os.WriteFile(prelude, contents[:index], 0o755); err != nil {
		t.Fatal(err)
	}
	return prelude
}

func writeMockGo(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "go")
	script := `#!/bin/bash
count=0
if [[ -f "${MOCK_GO_COUNTER}" ]]; then count=$(<"${MOCK_GO_COUNTER}"); fi
count=$((count + 1))
echo "${count}" > "${MOCK_GO_COUNTER}"
echo "$*" >> "${MOCK_GO_ARGS}"
if (( count >= MOCK_GO_SUCCEED_AFTER )); then exit 0; fi
exit "${MOCK_GO_FAILURE_STATUS}"
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func runInstall(t *testing.T, attempts, succeedAfter, failureStatus int) ([]byte, int, string, string) {
	t.Helper()

	toolsPath, err := filepath.Abs("ut_tools.bash")
	if err != nil {
		t.Fatal(err)
	}
	counter := filepath.Join(t.TempDir(), "attempts")
	arguments := filepath.Join(t.TempDir(), "arguments")
	mockGoDir := writeMockGo(t)
	cmd := exec.Command("bash", "-c", `source "$1"; install_go_ut_analysis "$2" 0`,
		"bash", toolsPath, strconv.Itoa(attempts))
	cmd.Env = append(os.Environ(),
		"PATH="+mockGoDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"MOCK_GO_COUNTER="+counter,
		"MOCK_GO_ARGS="+arguments,
		"MOCK_GO_SUCCEED_AFTER="+strconv.Itoa(succeedAfter),
		"MOCK_GO_FAILURE_STATUS="+strconv.Itoa(failureStatus),
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return output, 0, counter, arguments
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		return output, exitError.ExitCode(), counter, arguments
	}
	t.Fatalf("install go-ut-analysis: %v", err)
	return nil, 0, "", ""
}

func assertAttempts(t *testing.T, counter, arguments string, expected int) {
	t.Helper()

	attempts, err := os.ReadFile(counter)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(attempts)) != strconv.Itoa(expected) {
		t.Fatalf("expected %d attempts, got %q", expected, attempts)
	}
	invocations, err := os.ReadFile(arguments)
	if err != nil {
		t.Fatal(err)
	}
	for lineNumber, invocation := range strings.Split(strings.TrimSpace(string(invocations)), "\n") {
		if invocation != "install "+goUTAnalysisModule {
			t.Fatalf("attempt %d used unexpected arguments %q", lineNumber+1, invocation)
		}
	}
}

func TestInstallGoUTAnalysisRetriesTransientFailures(t *testing.T) {
	output, status, counter, arguments := runInstall(t, 3, 3, 42)
	if status != 0 {
		t.Fatalf("install failed with status %d: %s", status, output)
	}
	assertAttempts(t, counter, arguments, 3)
}

func TestInstallGoUTAnalysisDoesNotRetrySuccess(t *testing.T) {
	output, status, counter, arguments := runInstall(t, 3, 1, 42)
	if status != 0 {
		t.Fatalf("install failed with status %d: %s", status, output)
	}
	assertAttempts(t, counter, arguments, 1)
}

func TestInstallGoUTAnalysisPreservesFinalFailure(t *testing.T) {
	output, status, counter, arguments := runInstall(t, 3, 4, 42)
	if status != 42 {
		t.Fatalf("expected status 42, got %d: %s", status, output)
	}
	assertAttempts(t, counter, arguments, 3)
}

func TestUTProcessGroupsEscalateAfterTerm(t *testing.T) {
	processPath, err := filepath.Abs("ut_process.bash")
	if err != nil {
		t.Fatal(err)
	}

	// Exercise the same shared cleanup primitive used by engine, plan, and
	// embedded prebuild helpers. Both groups deliberately ignore TERM; the
	// bounded KILL escalation must clear their descendants without waiting for
	// the process leader to cooperate.
	script := `
set -o nounset
test_dir=$(mktemp -d)
ready_one="$test_dir/ready-one"
ready_two="$test_dir/ready-two"
pid_one="$test_dir/pid-one"
pid_two="$test_dir/pid-two"
first=0
second=0
cleanup() {
    for pid in "$first" "$second"; do
        if [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
            kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
        fi
    done
    for pid_file in "$pid_one" "$pid_two"; do
        [[ -f "$pid_file" ]] || continue
        read -r leader child < "$pid_file" || true
        for pid in "$leader" "$child"; do
            if [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then kill -KILL "$pid" 2>/dev/null || true; fi
        done
    done
    wait "$first" 2>/dev/null || true
    wait "$second" 2>/dev/null || true
    rm -rf "$test_dir"
}
trap cleanup EXIT
force_count=0
function logger() {
    if [[ "$2" == *"force stopping process group"* ]]; then force_count=$((force_count + 1)); fi
}
source "$1"
(
    kill_calls=0
    function kill() { kill_calls=$((kill_calls + 1)); return 0; }
    terminate_ut_process_group 0 TERM
    ut_process_group_alive 0
    if (( kill_calls != 0 )); then
        echo "zero pid attempted process-group signaling" >&2
        exit 1
    fi
) || exit 1
set -m
bash -c 'trap "" TERM; sleep 30 & child=$!; printf "%s %s\\n" "$BASHPID" "$child" > "$2"; printf ready > "$1"; wait "$child"' bash "$ready_one" "$pid_one" &
first=$!
bash -c 'trap "" TERM; sleep 30 & child=$!; printf "%s %s\\n" "$BASHPID" "$child" > "$2"; printf ready > "$1"; wait "$child"' bash "$ready_two" "$pid_two" &
second=$!
set +m
wait_ready() {
    for attempt in {1..200}; do
        [[ -s "$1" ]] && return 0
        sleep 0.05
    done
    return 1
}
wait_ready "$ready_one" || { echo "first process did not become ready" >&2; exit 1; }
wait_ready "$ready_two" || { echo "second process did not become ready" >&2; exit 1; }
terminate_ut_process_groups 2 "$first" "$second"
wait "$first" 2>/dev/null || true
wait "$second" 2>/dev/null || true
read -r first_leader first_child < "$pid_one"
read -r second_leader second_child < "$pid_two"
if (( force_count != 2 )); then
    echo "expected KILL escalation for two groups, got $force_count" >&2
    exit 1
fi
for pid in "$first_leader" "$first_child" "$second_leader" "$second_child"; do
    gone=0
    for attempt in {1..20}; do
        if ! kill -0 "$pid" 2>/dev/null; then
            gone=1
            break
        fi
        state=$(ps -o stat= -p "$pid" 2>/dev/null | tr -d ' ')
        if [[ -z "$state" || "$state" == Z* ]]; then
            gone=1
            break
        fi
        sleep 0.05
    done
    if (( gone == 0 )); then
        echo "TERM-ignoring process descendant survived cancellation: $pid" >&2
        exit 1
    fi
done
`
	cmd := exec.Command("bash", "-c", script, "bash", processPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("process-group cancellation harness failed: %v\n%s", err, output)
	}
}

func TestAppendUTReportUsesOneAuthoritativeRepresentation(t *testing.T) {
	processPath, err := filepath.Abs("ut_process.bash")
	if err != nil {
		t.Fatal(err)
	}

	script := `
set -o nounset
source "$1"
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT
report="$test_dir/engine.out"
destination="$test_dir/all.out"
printf 'complete-1\ncomplete-2\n' > "$report"
printf 'stale-shard\n' > "$report.1"
: > "$report.ready"
append_ut_report "$report" "$destination"
if [[ "$(cat "$destination")" != $'complete-1\ncomplete-2' ]]; then
    echo "ready report was not selected" >&2
    exit 1
fi
: > "$destination"
rm -f "$report.ready"
printf 'partial-one\n' > "$report.1"
printf 'partial-two\n' > "$report.2"
append_ut_report "$report" "$destination"
if [[ "$(cat "$destination")" != $'partial-one\npartial-two' ]]; then
    echo "partial reports were not selected" >&2
    exit 1
fi
`
	cmd := exec.Command("bash", "-c", script, "bash", processPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("report ownership harness failed: %v\n%s", err, output)
	}
}

func TestAppendUTReportPreservesSourcesWhenCopyIsInterrupted(t *testing.T) {
	processPath, err := filepath.Abs("ut_process.bash")
	if err != nil {
		t.Fatal(err)
	}

	// The injected cat emits one line and delivers TERM to the shell that owns
	// append_ut_report.  The helper must leave the old destination and complete
	// source untouched, then a retry must append exactly once.  Run both source
	// representations used by the consumers: the engine's ready-marked base
	// report and the plan helper's unmarked base report.
	script := `
set -o nounset
source "$1"
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT

interrupt_once() {
    local report=$1
    local destination=$2
    local ready=$3
    printf 'existing\n' > "$destination"
    printf 'first\nsecond\n' > "$report"
    if [[ "$ready" == 1 ]]; then : > "$report.ready"; fi

    term_pending=0
    trap 'term_pending=1' TERM
    interrupt_source="$report"
    function cat() {
        if [[ "$1" == "$interrupt_source" ]]; then
            command head -n 1 "$1"
            kill -TERM "$$"
            return 143
        fi
        command cat "$@"
    }
    append_ut_report "$report" "$destination"
    status=$?
    if (( status == 0 || term_pending == 0 )); then
        echo "interrupted transfer was not detected" >&2
        exit 1
    fi
    if [[ "$(command cat "$destination")" != $'existing' ]]; then
        echo "destination changed after interrupted transfer" >&2
        exit 1
    fi
    if [[ "$(command cat "$report")" != $'first\nsecond' ]]; then
        echo "source changed after interrupted transfer" >&2
        exit 1
    fi
    if [[ "$ready" == 1 && ! -f "$report.ready" ]]; then
        echo "ready marker was lost after interrupted transfer" >&2
        exit 1
    fi
    if compgen -G "$destination.tmp.*" > /dev/null; then
        echo "temporary destination survived interrupted transfer" >&2
        exit 1
    fi

    trap - TERM
    unset -f cat
    append_ut_report "$report" "$destination"
    if [[ "$(command cat "$destination")" != $'existing\nfirst\nsecond' ]]; then
        echo "retry did not append the source exactly once" >&2
        exit 1
    fi
    rm -f "$report" "$report.ready"
}

interrupt_once "$test_dir/engine.out" "$test_dir/engine-all.out" 1
interrupt_once "$test_dir/plan.out" "$test_dir/plan-all.out" 0

publish_after_rename() {
    local report=$1
    local destination=$2
    local ready=$3
    printf 'existing\n' > "$destination"
    printf 'first\nsecond\n' > "$report"
    if [[ "$ready" == 1 ]]; then : > "$report.ready"; fi

    term_pending=0
    trap 'term_pending=1' TERM
    function mv() {
        command mv "$@"
        local status=$?
        if (( status == 0 )); then
            # Model GNU timeout delivering group TERM after rename(2) has
            # committed but before the external mv reports its status.
            kill -TERM "$$"
            return 143
        fi
        return "$status"
    }
    append_ut_report "$report" "$destination"
    status=$?
    if (( status != 0 || term_pending == 0 )); then
        echo "post-rename interruption was not treated as committed" >&2
        exit 1
    fi
    if [[ "$(command cat "$destination")" != $'existing\nfirst\nsecond' ]]; then
        echo "post-rename destination was not published exactly once" >&2
        exit 1
    fi
    if compgen -G "$destination.tmp.*" > /dev/null; then
        echo "post-rename temporary state survived" >&2
        exit 1
    fi

    # append_ut_report does not own source cleanup; the consumer removes the
    # source only after this committed return.
    trap - TERM
    unset -f mv
    rm -f "$report" "$report.ready"
}

publish_after_rename "$test_dir/engine-after-rename.out" "$test_dir/engine-after-rename-all.out" 1
publish_after_rename "$test_dir/plan-after-rename.out" "$test_dir/plan-after-rename-all.out" 0

publish_with_cleanup_interruption() {
    local report=$1
    local destination=$2
    local ready=$3
    local before_delete=${4:-0}
    printf 'existing\n' > "$destination"
    printf 'first\nsecond\n' > "$report"
    if [[ "$ready" == 1 ]]; then : > "$report.ready"; fi

    term_pending=0
    rm_interrupted=0
    trap 'term_pending=1' TERM
    function rm() {
        if [[ "$*" == *".expected"* ]] &&
            (( before_delete == 1 && rm_interrupted == 0 )); then
            rm_interrupted=1
            # Model TERM before unlink.  The retry must perform the cleanup
            # after the pending trap has been recorded.
            kill -TERM "$$"
            return 143
        fi
        command rm "$@"
        local status=$?
        if (( status == 0 && before_delete == 0 && rm_interrupted == 0 )) &&
            [[ "$*" == *".expected"* ]]; then
            # The publication has already committed.  Model TERM after the
            # expected hard link is removed but before rm reports status.
            rm_interrupted=1
            kill -TERM "$$"
            return 143
        fi
        return "$status"
    }
    append_ut_report "$report" "$destination"
    status=$?
    if (( status != 0 || term_pending == 0 )); then
        echo "cleanup interruption changed the committed status" >&2
        exit 1
    fi
    if [[ "$(command cat "$destination")" != $'existing\nfirst\nsecond' ]]; then
        echo "cleanup interruption duplicated or lost the report" >&2
        exit 1
    fi
    if compgen -G "$destination.tmp.*" > /dev/null; then
        echo "cleanup interruption left transaction state" >&2
        exit 1
    fi

    trap - TERM
    unset -f rm
    rm -f "$report" "$report.ready"
}

publish_with_cleanup_interruption "$test_dir/engine-after-cleanup.out" "$test_dir/engine-after-cleanup-all.out" 1
publish_with_cleanup_interruption "$test_dir/plan-after-cleanup.out" "$test_dir/plan-after-cleanup-all.out" 0
publish_with_cleanup_interruption "$test_dir/engine-before-cleanup.out" "$test_dir/engine-before-cleanup-all.out" 1 1
publish_with_cleanup_interruption "$test_dir/plan-before-cleanup.out" "$test_dir/plan-before-cleanup-all.out" 0 1

link_after_creation_interruption() {
    local report=$1
    local destination=$2
    printf 'existing\n' > "$destination"
    printf 'first\nsecond\n' > "$report"

    term_pending=0
    trap 'term_pending=1' TERM
    function ln() {
        command ln "$@"
        local status=$?
        if (( status == 0 )); then
            # The hard link exists, but publication has not started.  The
            # failed transfer must remove both temporary names.
            kill -TERM "$$"
            return 143
        fi
        return "$status"
    }
    append_ut_report "$report" "$destination"
    status=$?
    if (( status == 0 || term_pending == 0 )); then
        echo "link interruption was not reported as a failed transfer" >&2
        exit 1
    fi
    if [[ "$(command cat "$destination")" != $'existing' ]]; then
        echo "link interruption changed the destination" >&2
        exit 1
    fi
    if compgen -G "$destination.tmp.*" > /dev/null; then
        echo "link interruption left transaction state" >&2
        exit 1
    fi

    trap - TERM
    unset -f ln
    rm -f "$report"
}

link_after_creation_interruption "$test_dir/engine-after-link.out" "$test_dir/engine-after-link-all.out"
`
	cmd := exec.Command("bash", "-c", script, "bash", processPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("interrupted report transfer harness failed: %v\n%s", err, output)
	}
}

func TestLogserviceCompanionTracksPIDBeforeCancellation(t *testing.T) {
	prelude := writeRunUTPrelude(t)
	runUTDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	mockGoDir := t.TempDir()
	mockGo := filepath.Join(mockGoDir, "go")
	if err := os.WriteFile(mockGo, []byte(`#!/bin/bash
if [[ "${1:-}" == version ]]; then exit 0; fi
if [[ "${1:-}" == test ]]; then
    trap 'printf terminated > "${MOCK_GO_TERM}"; exit 143' TERM
    printf started > "${MOCK_GO_STARTED}"
    kill -TERM "${PPID}"
	( sleep 4; kill -TERM "$$" 2>/dev/null || true ) &
    while :; do sleep 1; done
fi
exit 0
`), 0o755); err != nil {
		t.Fatal(err)
	}
	startedPath := filepath.Join(t.TempDir(), "companion.started")
	termPath := filepath.Join(t.TempDir(), "companion.term")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-c", `source "$1" UT race; trap handle_ut_termination TERM; trap 'if [[ "${LOGSERVICE_RACE_JOB_PID:-}" =~ ^[1-9][0-9]*$ ]]; then terminate_ut_process_group "$LOGSERVICE_RACE_JOB_PID" TERM; fi' EXIT; start_logservice_race example.com/logservice; while [[ ! -s "$MOCK_GO_TERM" ]]; do sleep 0.01; done`, "bash", prelude)
	cmd.Dir = runUTDir
	env := os.Environ()
	for index, value := range env {
		if strings.HasPrefix(value, "PATH=") {
			env[index] = "PATH=" + mockGoDir + string(os.PathListSeparator) + os.Getenv("PATH")
		}
	}
	cmd.Env = append(env,
		"MOCK_GO_STARTED="+startedPath,
		"MOCK_GO_TERM="+termPath,
		"UT_WORKDIR="+t.TempDir(),
	)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("companion cancellation harness timed out: %v\n%s", ctx.Err(), output)
	}
	if err == nil {
		t.Fatalf("expected cancellation exit, got success: %s", output)
	}
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 143 {
		t.Fatalf("expected cancellation status 143, got %v\n%s", err, output)
	}
	if _, err := os.Stat(startedPath); err != nil {
		t.Fatalf("companion did not start before cancellation: %v\n%s", err, output)
	}
	if _, err := os.Stat(termPath); err != nil {
		t.Fatalf("companion did not receive cancellation after pid registration: %v\n%s", err, output)
	}
}

func TestLogserviceCompanionStderrConsumeIsIdempotent(t *testing.T) {
	prelude := writeRunUTPrelude(t)
	runUTDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	testDir := t.TempDir()
	script := `
set -o nounset
source "$1" UT race
function logger() { :; }
function handle_ut_termination() { :; }
test_dir="$2"
UT_REPORT="$test_dir/all.out"
UT_STDERR="$test_dir/stderr.out"
LOGSERVICE_RACE_REPORT="$test_dir/companion.json"
LOGSERVICE_RACE_STDERR="$test_dir/companion.err"
printf 'existing\n' > "$UT_REPORT"
printf 'companion-error\n' > "$LOGSERVICE_RACE_STDERR"
printf '{"Action":"pass"}\n' > "$LOGSERVICE_RACE_REPORT"
function remove_ut_report_file() { return 1; }
consume_logservice_race_report
if [[ "$(cat "$UT_STDERR")" != 'companion-error' ]]; then
    echo "stderr was not atomically consumed" >&2
    exit 1
fi
if [[ "$(cat "$UT_REPORT")" != $'existing\n{"Action":"pass"}' ]]; then
    echo "JSON report was not consumed" >&2
    exit 1
fi
if [[ -n "$LOGSERVICE_RACE_STDERR" || -n "$LOGSERVICE_RACE_REPORT" ]]; then
    echo "consumed report ownership was not cleared" >&2
    exit 1
fi
# A second consume must be a no-op even though the injected cleanup failure
# left the private stderr source on disk.
consume_logservice_race_report
if [[ "$(cat "$UT_STDERR")" != 'companion-error' ]]; then
    echo "stderr was duplicated after cleanup retry" >&2
    exit 1
fi
`
	cmd := exec.Command("bash", "-c", script, "bash", prelude, testDir)
	cmd.Dir = runUTDir
	cmd.Env = append(os.Environ(), "UT_WORKDIR="+t.TempDir())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("companion stderr ownership harness failed: %v\n%s", err, output)
	}
}

func TestSummarizeUTSetupReportsCumulativePhases(t *testing.T) {
	scriptPath, err := filepath.Abs("summarize_ut_setup.py")
	if err != nil {
		t.Fatal(err)
	}
	reportPath := filepath.Join(t.TempDir(), "ut.json")
	report := strings.Join([]string{
		`{"Action":"output","Package":"example/cluster","Test":"TestCluster","Output":"    MO_UT_SETUP fixture=shared-cluster phase=cluster-start duration=2s status=ready\n"}`,
		`{"Action":"output","Package":"example/cluster","Test":"TestCluster2","Output":"    MO_UT_SETUP fixture=shared-cluster phase=cluster-start duration=500ms status=ready\n"}`,
		`{"Action":"output","Package":"example/cluster","Test":"TestCluster3","Output":"    MO_UT_SETUP fixture=shared-cluster phase=slow-start duration=1m2.5s status=ready\n"}`,
		`{"Action":"output","Package":"example/cluster","Test":"TestCluster4","Output":"    MO_UT_SETUP fixture=shared-cluster phase=hour-start duration=1h2m3s status=ready\n"}`,
		`null`,
		`[]`,
		`{"Action":"output","Package":"example/issues","Test":"TestIssue","Output":"    MO_UT_SETUP fixture=issue26875 phase=database-create duration=100ms status=error\n"}`,
		`{"Action":"output","Package":"example/cluster","Test":"TestCluster5","Output":"    MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=10 phase=cluster-construct duration=1ms status=ready\n    MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=10 phase=admission-acquire duration=2s status=ready wait=2s hold=2s admission_released=false\n    MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=10 phase=service-start duration=3s status=ready wait=2s hold=5s admission_released=false\n    MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=10 phase=admission-release duration=1ms status=ready wait=2s hold=5s admission_released=true\n"}`,
		`{"Action":"output","Package":"example/cluster","Test":"TestCluster6","Output":"    MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=11 phase=cluster-construct duration=1ms status=ready\n    MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=11 phase=admission-acquire duration=100ms status=ready wait=100ms hold=100ms\n    MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=11 phase=service-start duration=4s status=ready wait=100ms hold=4.1s\n"}`,
		"not json",
	}, "\n")
	runSummary := func(reportPath, report string) string {
		t.Helper()
		if err := os.WriteFile(reportPath, []byte(report), 0o644); err != nil {
			t.Fatal(err)
		}
		output, err := exec.Command("python3", scriptPath, reportPath).CombinedOutput()
		if err != nil {
			t.Fatalf("summarize setup timing: %v\n%s", err, output)
		}
		return string(output)
	}

	text := runSummary(reportPath, report)
	if !strings.Contains(text, "ignored malformed JSON lines=1 (report may be truncated by cancellation)") {
		t.Fatalf("truncated report warning missing: %s", text)
	}
	if !strings.Contains(text, "fixture=shared-cluster phase=cluster-start count=2 total=2.50s max=2.00s") {
		t.Fatalf("missing cumulative setup summary: %s", text)
	}
	if !strings.Contains(text, "fixture=shared-cluster phase=slow-start count=1 total=1.04m max=1.04m") {
		t.Fatalf("missing compound duration summary: %s", text)
	}
	if !strings.Contains(text, "fixture=shared-cluster phase=hour-start count=1 total=62.05m max=62.05m") {
		t.Fatalf("missing hour duration summary: %s", text)
	}
	if !strings.Contains(text, "fixture=issue26875 phase=database-create count=1 total=100.00ms max=100.00ms errors=1") {
		t.Fatalf("missing setup error summary: %s", text)
	}
	if !strings.Contains(text, "embedded-cluster diagnosis: clusters=2 admission_wait(total=2.10s max=2.00s) service_start(total=7.00s max=4.00s) admission_hold_observed_max=5.00s admission_unreleased_observed=1 admission_release_evidence=partial slowest_completed_admission_waits=2.00s:example/cluster:TestCluster5 pid=10 cluster=1,100.00ms:example/cluster:TestCluster6 pid=11 cluster=1") {
		t.Fatalf("missing embedded cluster diagnosis: %s", text)
	}

	// Reusing a cluster object after a successful Close creates a new
	// admission lease with the same (pid, cluster_id).  The old release must
	// not make the second, still-active lease look released.
	reacquired := strings.Join([]string{
		`{"Action":"output","Package":"example/cluster","Test":"TestCluster","Output":"    MO_UT_SETUP fixture=embedded-cluster cluster_id=7 pid=20 phase=admission-acquire duration=2s status=ready wait=2s hold=2s admission_released=false\n    MO_UT_SETUP fixture=embedded-cluster cluster_id=7 pid=20 phase=admission-release duration=1ms status=ready wait=2s hold=2s admission_released=true\n    MO_UT_SETUP fixture=embedded-cluster cluster_id=7 pid=20 phase=admission-acquire duration=3s status=ready wait=3s hold=3s admission_released=false\n"}`,
	}, "\n")
	text = runSummary(filepath.Join(t.TempDir(), "reacquired.json"), reacquired)
	if !strings.Contains(text, "embedded-cluster diagnosis: clusters=1 admission_wait(total=5.00s max=3.00s) admission_hold_observed_max=3.00s admission_unreleased_observed=1 admission_release_evidence=partial") {
		t.Fatalf("reacquired lease was not reported as active: %s", text)
	}

	// Once that second lease is released, the current state must converge back
	// to zero rather than retaining a stale unreleased generation.
	released := strings.Join([]string{
		`{"Action":"output","Package":"example/cluster","Test":"TestCluster","Output":"    MO_UT_SETUP fixture=embedded-cluster cluster_id=7 pid=20 phase=admission-acquire duration=2s status=ready wait=2s hold=2s admission_released=false\n    MO_UT_SETUP fixture=embedded-cluster cluster_id=7 pid=20 phase=admission-release duration=1ms status=ready wait=2s hold=2s admission_released=true\n    MO_UT_SETUP fixture=embedded-cluster cluster_id=7 pid=20 phase=admission-acquire duration=3s status=ready wait=3s hold=3s admission_released=false\n    MO_UT_SETUP fixture=embedded-cluster cluster_id=7 pid=20 phase=admission-release duration=1ms status=ready wait=3s hold=3s admission_released=true\n"}`,
	}, "\n")
	text = runSummary(filepath.Join(t.TempDir(), "released.json"), released)
	if !strings.Contains(text, "embedded-cluster diagnosis: clusters=1 admission_wait(total=5.00s max=3.00s) admission_hold_observed_max=3.00s admission_unreleased_observed=0 admission_release_evidence=complete") {
		t.Fatalf("reacquired lease was not accounted for: %s", text)
	}

	// A release record can survive while its acquire record is truncated.  The
	// real release event carries hold metadata, so this must remain unknown/
	// partial rather than being promoted to a completed lease.
	releaseOnly := strings.Join([]string{
		`{"Action":"output","Package":"example/cluster","Test":"TestReleaseOnly","Output":"    MO_UT_SETUP fixture=embedded-cluster cluster_id=8 pid=21 phase=admission-release duration=1ms status=ready wait=2s hold=2s admission_released=true\n"}`,
	}, "\n")
	text = runSummary(filepath.Join(t.TempDir(), "release-only.json"), releaseOnly)
	if !strings.Contains(text, "embedded-cluster diagnosis: clusters=1 admission_hold_observed_max=2.00s admission_unreleased_observed=1 admission_release_evidence=partial") {
		t.Fatalf("release-only record was promoted to complete: %s", text)
	}

	// A hold followed by a release is equally insufficient when the matching
	// acquire event is absent from the captured prefix/suffix.
	holdThenRelease := strings.Join([]string{
		`{"Action":"output","Package":"example/cluster","Test":"TestHoldThenRelease","Output":"    MO_UT_SETUP fixture=embedded-cluster cluster_id=9 pid=22 phase=service-start duration=3s status=ready hold=3s admission_released=false\n    MO_UT_SETUP fixture=embedded-cluster cluster_id=9 pid=22 phase=admission-release duration=1ms status=ready hold=3s admission_released=true\n"}`,
	}, "\n")
	text = runSummary(filepath.Join(t.TempDir(), "hold-then-release.json"), holdThenRelease)
	if !strings.Contains(text, "embedded-cluster diagnosis: clusters=1 service_start(total=3.00s max=3.00s) admission_hold_observed_max=3.00s admission_unreleased_observed=1 admission_release_evidence=partial") {
		t.Fatalf("hold-then-release record was promoted to complete: %s", text)
	}
}

func writeScopeFixture(t *testing.T, root, name, contents string) {
	t.Helper()

	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestListEmbeddedClusterTestPackagesFollowsTransitiveTestDeps(t *testing.T) {
	toolsPath, err := filepath.Abs("ut_tools.bash")
	if err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	fixtures := map[string]string{
		"go.mod": `module example.com/cluster-scope

go 1.24
`,
		"pkg/embed/embed.go": `package embed

const Enabled = true
`,
		"pkg/embed/embed_test.go": `package embed

import "testing"

func TestEmbed(t *testing.T) {}
`,
		"pkg/helper/helper.go": `package helper

import "example.com/cluster-scope/pkg/embed"

var Enabled = embed.Enabled
`,
		"pkg/owner/owner.go": `package owner
`,
		"pkg/owner/owner_test.go": `package owner

import (
	_ "example.com/cluster-scope/pkg/helper"
	"testing"
)

func TestOwner(t *testing.T) {}
`,
		"pkg/light/light.go": `package light
`,
		"pkg/light/light_test.go": `package light

import "testing"

func TestLight(t *testing.T) {}
`,
	}
	for name, contents := range fixtures {
		writeScopeFixture(t, root, name, contents)
	}

	cmd := exec.Command("bash", "-c",
		`source "$1"; list_embedded_cluster_test_packages ./...`,
		"bash", toolsPath)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"GOWORK=off",
		"GOFLAGS=-mod=readonly",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("discover embedded cluster packages: %v\n%s", err, output)
	}

	expected := strings.Join([]string{
		"example.com/cluster-scope/pkg/embed",
		"example.com/cluster-scope/pkg/owner",
	}, "\n")
	if actual := strings.TrimSpace(string(output)); actual != expected {
		t.Fatalf("unexpected embedded cluster packages:\n%s\nexpected:\n%s", actual, expected)
	}

	cmd = exec.Command("bash", "-c",
		`source "$1"; list_embedded_cluster_test_packages ./pkg/owner ./pkg/light`,
		"bash", toolsPath)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"GOWORK=off",
		"GOFLAGS=-mod=readonly",
	)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("discover partial embedded cluster scope: %v\n%s", err, output)
	}
	if actual := strings.TrimSpace(string(output)); actual != "example.com/cluster-scope/pkg/owner" {
		t.Fatalf("partial scope included an unrequested package: %s", actual)
	}
}

func TestListEmbeddedClusterTestPackagesPreservesGoListFailure(t *testing.T) {
	toolsPath, err := filepath.Abs("ut_tools.bash")
	if err != nil {
		t.Fatal(err)
	}
	counter := filepath.Join(t.TempDir(), "attempts")
	arguments := filepath.Join(t.TempDir(), "arguments")
	mockGoDir := writeMockGo(t)
	cmd := exec.Command("bash", "-c",
		`source "$1"; list_embedded_cluster_test_packages ./...`,
		"bash", toolsPath)
	cmd.Env = append(os.Environ(),
		"PATH="+mockGoDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"MOCK_GO_COUNTER="+counter,
		"MOCK_GO_ARGS="+arguments,
		"MOCK_GO_SUCCEED_AFTER=2",
		"MOCK_GO_FAILURE_STATUS=42",
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected go list failure, got success: %s", output)
	}
	exitError, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("unexpected command error: %v", err)
	}
	if exitError.ExitCode() != 42 {
		t.Fatalf("expected status 42, got %d: %s", exitError.ExitCode(), output)
	}
	attempts, err := os.ReadFile(counter)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(attempts)) != "1" {
		t.Fatalf("expected one go list attempt, got %q", attempts)
	}
}

func runUTToolsBash(t *testing.T, script string, env ...string) ([]byte, int) {
	t.Helper()

	toolsPath, err := filepath.Abs("ut_tools.bash")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "-c", script, "bash", toolsPath)
	cmd.Env = append(os.Environ(), env...)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return output, 0
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		return output, exitError.ExitCode()
	}
	t.Fatalf("run UT tools bash: %v", err)
	return nil, 0
}

func TestUTShardStagesFormCompleteDisjointMap(t *testing.T) {
	script := `source "$1"
for shard in all light issues embedded heavy-plan; do
    stages=$(list_ut_shard_stages "${shard}") || exit $?
    printf '%s=' "${shard}"
    printf '%s\n' "${stages}" | paste -sd, -
done
for shard in light issues embedded heavy-plan; do
    UT_SHARD=${shard}
    for stage in light hnsw serial embedded heavy plan; do
        if should_run_ut_stage "${stage}"; then
            printf '%s:%s\n' "${shard}" "${stage}"
        else
            status=$?
            if (( status != 1 )); then exit "${status}"; fi
        fi
    done
done`
	output, status := runUTToolsBash(t, script)
	if status != 0 {
		t.Fatalf("shard map failed with status %d: %s", status, output)
	}

	expected := `all=light,hnsw,serial,embedded,heavy,plan
light=light,hnsw
issues=serial
embedded=embedded
heavy-plan=heavy,plan
light:light
light:hnsw
issues:serial
embedded:embedded
heavy-plan:heavy
heavy-plan:plan`
	if actual := strings.TrimSpace(string(output)); actual != expected {
		t.Fatalf("unexpected shard map:\n%s\nexpected:\n%s", actual, expected)
	}
}

func TestUTShardStagesRejectUnknownValues(t *testing.T) {
	tests := []struct {
		name   string
		script string
	}{
		{name: "shard", script: `source "$1"; list_ut_shard_stages unknown`},
		{name: "stage", script: `source "$1"; UT_SHARD=all; UT_SHARD_ROUTING_ERROR=0; should_run_ut_stage unknown; status=$?; (( UT_SHARD_ROUTING_ERROR == 1 )) || exit 3; exit "${status}"`},
		{name: "selected shard", script: `source "$1"; UT_SHARD=unknown; UT_SHARD_ROUTING_ERROR=0; should_run_ut_stage light; status=$?; (( UT_SHARD_ROUTING_ERROR == 1 )) || exit 3; exit "${status}"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output, status := runUTToolsBash(t, test.script)
			if status != 2 {
				t.Fatalf("expected status 2, got %d: %s", status, output)
			}
		})
	}
}

func TestValidateCompletePartition(t *testing.T) {
	tests := []struct {
		name       string
		expected   string
		groups     []string
		wantStatus int
		wantError  string
	}{
		{name: "complete", groups: []string{"a", "b\nc"}},
		{name: "duplicate expected", expected: "a\nb\nb\nc", groups: []string{"a", "b\nc"}, wantStatus: 1, wantError: "UT package occurs 2 times in expected scope: b"},
		{name: "missing", groups: []string{"a", "b"}, wantStatus: 1, wantError: "Missing UT package from partition: c"},
		{name: "duplicate", groups: []string{"a\nb", "b\nc"}, wantStatus: 1, wantError: "UT package occurs 2 times in partition: b"},
		{name: "unexpected", groups: []string{"a\nb", "c\nd"}, wantStatus: 1, wantError: "Unexpected UT package in partition: d"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := test.expected
			if expected == "" {
				expected = "a\nb\nc"
			}
			script := `source "$1"; validate_complete_partition "UT package" "${EXPECTED}" "${GROUP_ONE}" "${GROUP_TWO}"`
			output, status := runUTToolsBash(t, script,
				"EXPECTED="+expected,
				"GROUP_ONE="+test.groups[0],
				"GROUP_TWO="+test.groups[1],
			)
			if status != test.wantStatus {
				t.Fatalf("expected status %d, got %d: %s", test.wantStatus, status, output)
			}
			if test.wantError != "" && !strings.Contains(string(output), test.wantError) {
				t.Fatalf("missing error %q in output: %s", test.wantError, output)
			}
		})
	}
}
