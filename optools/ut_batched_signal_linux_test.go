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
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Compare an actual Go test process (race-instrumented when enabled) against
// the original foreground invocation, before and after cmd/go-style Notify.
func TestBatchedFallbackSignalDisposition(t *testing.T) {
	if os.Getenv("MO_BATCHED_SIGNAL_PROBE") == "1" {
		readBits := func() string {
			data, err := os.ReadFile("/proc/self/status")
			if err != nil {
				t.Fatal(err)
			}
			bits := make(map[string]uint64)
			for _, line := range strings.Split(string(data), "\n") {
				key, value, ok := strings.Cut(line, ":")
				if !ok || (key != "SigIgn" && key != "SigCgt") {
					continue
				}
				bits[key], err = strconv.ParseUint(strings.TrimSpace(value), 16, 64)
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(bits) != 2 {
				t.Fatal("missing signal disposition")
			}
			return fmt.Sprintf("%d/%d", (bits["SigIgn"]>>1)&3, (bits["SigCgt"]>>1)&3)
		}
		before := readBits()
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGQUIT)
		after := readBits()
		signal.Stop(signals)
		if err := os.WriteFile(os.Getenv("MO_BATCHED_SIGNAL_OUTPUT"), []byte(before+" "+after+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		return
	}

	t.Parallel()
	program, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 env) exit 1 ;;
 test) exec "$TEST_BINARY" -test.run '^TestBatchedFallbackSignalDisposition$' ;;
esac
exit 81
`
	for _, mode := range []string{"default", "ignore", "custom"} {
		t.Run(mode, func(t *testing.T) {
			transform := func(text string) string {
				const foreground = "    if (( use_owned_group == 0 )); then\n"
				text = injectBatchedHook(t, text, foreground, "    MO_BATCHED_SIGNAL_OUTPUT=\"$CASE_DIR/foreground-signals\" \"$TEST_BINARY\" -test.run '^TestBatchedFallbackSignalDisposition$' > /dev/null || return 125\n"+foreground)
				const anchor = "    ) 0<&0 &\n    pid=$!\n"
				return injectBatchedHook(t, text, anchor, anchor+"    : > \"$CASE_DIR/async-fallback\"\n")
			}
			script := batchedOwnerSetup + `
trap - INT QUIT
case "$SIGNAL_MODE" in
 ignore) trap '' INT QUIT ;;
 custom) trap ':' INT QUIT ;;
esac
export MO_BATCHED_SIGNAL_PROBE=1 GORACE=atexit_sleep_ms=0
export MO_BATCHED_SIGNAL_OUTPUT="$CASE_DIR/fallback-signals"
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 || status=$?
[[ "$status" == 0 ]] || exit 91
if (( BASH_VERSINFO[0] >= 5 )); then [[ -e "$CASE_DIR/async-fallback" ]] || exit 93; fi
cmp "$CASE_DIR/foreground-signals" "$CASE_DIR/fallback-signals" || exit 92
`
			out, err := scheduleHarnessWithMockTransform(t, script, mock, transform, "SIGNAL_MODE="+mode, "TEST_BINARY="+program)
			if err != nil {
				t.Fatalf("Go signal disposition (%s): %v\n%s", mode, err, out)
			}
		})
	}
}

// Exercise the actual go test driver as well as its test process. The probe
// records INT/QUIT dispositions in both processes without sending a signal.
func TestBatchedFallbackRealGoDriverSignals(t *testing.T) {
	t.Parallel()
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("missing test binary build information")
	}
	raceFlag := ""
	for _, setting := range info.Settings {
		if setting.Key == "-race" && setting.Value == "true" {
			raceFlag = "-race"
		}
	}
	t.Logf("nested go test race enabled: %t", raceFlag != "")
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/signalprobe\n\ngo 1.27.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	const source = `package signalprobe
import (
 "fmt"
 "os"
 "strconv"
 "strings"
 "testing"
)
func bits(t *testing.T, pid string) string {
 data, err := os.ReadFile("/proc/"+pid+"/status"); if err != nil { t.Fatal(err) }
 found := map[string]uint64{}
 for _, line := range strings.Split(string(data), "\n") {
  key, value, ok := strings.Cut(line, ":")
  if !ok || (key != "SigIgn" && key != "SigCgt") { continue }
  n, err := strconv.ParseUint(strings.TrimSpace(value), 16, 64); if err != nil { t.Fatal(err) }
  found[key] = n
 }
 if len(found) != 2 { t.Fatal("missing signal status") }
 return fmt.Sprintf("%d/%d", (found["SigIgn"]>>1)&3, (found["SigCgt"]>>1)&3)
}
func TestProbe(t *testing.T) {
 output := bits(t, "self")+" "+bits(t, strconv.Itoa(os.Getppid()))+"\n"
 if err := os.WriteFile(os.Getenv("MO_SIGNAL_OUTPUT"), []byte(output), 0600); err != nil { t.Fatal(err) }
}
`
	if err := os.WriteFile(filepath.Join(root, "signal_test.go"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 env) exit 1 ;;
 test)
  cd "$SIGNAL_PACKAGE" || exit 90
  exec "$REAL_GO" test -mod=readonly -vet=off -short -v -json -tags matrixone_test -p 1 -timeout 60s $RACE_FLAG -count=1 . ;;
esac
exit 81
`
	transform := func(text string) string {
		const foreground = "    if (( use_owned_group == 0 )); then\n"
		const probe = "    (cd \"$SIGNAL_PACKAGE\" && MO_SIGNAL_OUTPUT=\"$CASE_DIR/foreground-signals\" \"$REAL_GO\" test -mod=readonly -vet=off -short -v -json -tags matrixone_test -p 1 -timeout 60s $RACE_FLAG -count=1 . > /dev/null) || return 125\n"
		text = injectBatchedHook(t, text, foreground, probe+foreground)
		const anchor = "    ) 0<&0 &\n    pid=$!\n"
		return injectBatchedHook(t, text, anchor, anchor+"    : > \"$CASE_DIR/async-fallback\"\n")
	}
	// Test one ignored and one default signal in the real driver; the smaller
	// test above separately checks default, ignore, and custom trap modes.
	script := batchedOwnerSetup + `
trap - INT QUIT
trap '' INT
export GORACE=atexit_sleep_ms=0
export MO_SIGNAL_OUTPUT="$CASE_DIR/fallback-signals"
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 || status=$?
[[ "$status" == 0 ]] || exit 91
if (( BASH_VERSINFO[0] >= 5 )); then [[ -e "$CASE_DIR/async-fallback" ]] || exit 93; fi
cmp "$CASE_DIR/foreground-signals" "$CASE_DIR/fallback-signals" || exit 92
`
	out, err := scheduleHarnessWithMockTimeout(t, 40*time.Second, script, mock, transform, "SIGNAL_PACKAGE="+root, "REAL_GO="+goTool, "RACE_FLAG="+raceFlag)
	if err != nil {
		t.Fatalf("go test driver signal disposition: %v\n%s", err, out)
	}
}

// The fallback is already running and has emitted partial JSON. A request
// without TERM must stop even a TERM-resistant group and retain that output.
func TestBatchedMarkerOnlyCancelsRunningFallback(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 env) exit 1 ;;
 test)
  trap '' TERM
  (trap '' TERM; exec sleep 30) &
  child=$!
  fixture_self_pid owner
  printf '%s %s\n' "$owner" "$child" > "$CASE_DIR/fallback-pids"
  printf '%s\n' '{"Action":"output","Package":"example/package","Output":"partial failure"}'
  printf 'fallback diagnostic\n' >&2
  : > "$CASE_DIR/fallback-ready"
  wait "$child"; exit 7 ;;
esac
exit 81
`
	script := batchedOwnerSetup + `
start_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || exit 90
owner_pid=$CURRENT_UT_PID
for ((i=0; i<300; i++)); do
 [[ -e "$CASE_DIR/fallback-ready" ]] && break
 kill -0 "$CURRENT_UT_PID" 2>/dev/null || exit 91
 sleep .02
done
[[ -e "$CASE_DIR/fallback-ready" ]] || exit 92
read -r fallback_pid descendant_pid < "$CASE_DIR/fallback-pids"
: > "$completion.cancel"
status=0; finish_ut_command || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 ]] || exit 93
[[ -e "$completion.cancel" && ! -s "$completion" && -e "$PREBUILT_RACE_REPORT" ]] || exit 94
[[ "$(grep -c 'partial failure' "$UT_REPORT")" == 1 ]] || exit 95
grep -q 'fallback diagnostic' "$UT_STDERR" || exit 96
if ut_process_group_alive "$owner_pid"; then exit 97; fi
`
	transform := func(text string) string { return shortenBatchedOwnerGrace(t, text) }
	out, err := scheduleHarnessWithMockTransform(t, script, mock, transform)
	if err != nil {
		t.Fatalf("marker-only fallback cancellation: %v\n%s", err, out)
	}
}

// TERM can arrive after the asynchronous fork but before $! is assigned. The
// owner must record that PID before acting on the sticky request.
func TestBatchedFallbackCancellationBetweenForkAndPID(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 env) exit 1 ;;
 test) exec sleep 30 ;;
esac
exit 81
`
	transform := func(text string) string {
		const anchor = "    ) 0<&0 &\n    pid=$!\n"
		return injectBatchedHook(t, text, anchor, "    ) 0<&0 &\n    fixture_self_pid fixture_owner_pid\n    kill -TERM \"$fixture_owner_pid\"\n    pid=$!\n    printf '%s\\n' \"$pid\" > \"$CASE_DIR/fallback-pid\"\n")
	}
	script := batchedOwnerSetup + `
start_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 4 2 independent || exit 89
owner_pid=$CURRENT_UT_PID
status=0; finish_ut_command || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 && ! -s "$completion" ]] || exit 90
[[ -s "$CASE_DIR/fallback-pid" ]] || exit 91
if ut_process_group_alive "$owner_pid"; then exit 92; fi
`
	out, err := scheduleHarnessWithMockTransform(t, script, mock, transform)
	if err != nil {
		t.Fatalf("fallback fork/PID cancellation: %v\n%s", err, out)
	}
}

func TestBatchedKilledBeforeFallbackPIDRegistration(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 env) exit 1 ;;
 test) trap '' TERM; : > "$CASE_DIR/fallback-ready"; exec sleep 30 ;;
esac
exit 81
`
	transform := func(text string) string {
		const anchor = "    ) 0<&0 &\n    pid=$!\n"
		const replacement = `    ) 0<&0 &
    child_pid=$!
    for ((i=0; i<300; i++)); do
        [[ -e "$CASE_DIR/fallback-ready" ]] && break
        sleep .02
    done
    [[ -e "$CASE_DIR/fallback-ready" ]] || exit 89
    fixture_self_pid fixture_owner_pid
    child_group=$(ps -o pgid= -p "$child_pid" | tr -d '[:space:]')
    [[ "$child_group" == "$fixture_owner_pid" ]] || exit 88
    : > "$CASE_DIR/pre-registration-kill"
    kill -KILL "$fixture_owner_pid"
    pid=$!
`
		return shortenBatchedOwnerGrace(t, injectBatchedHook(t, text, anchor, replacement))
	}
	script := batchedOwnerSetup + `
start_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 || exit 90
owner_pid=$CURRENT_UT_PID
status=0; finish_ut_command || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 && ! -s "$completion" ]] || exit 91
[[ -e "$CASE_DIR/pre-registration-kill" ]] || exit 92
if ut_process_group_alive "$owner_pid"; then exit 93; fi
`
	out, err := scheduleHarnessWithMockTransform(t, script, mock, transform)
	if err != nil {
		t.Fatalf("helper killed before fallback PID publication: %v\n%s", err, out)
	}
}

func TestBatchedCancellationBeforeFallbackFork(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 env) exit 1 ;;
 test) : > "$CASE_DIR/unexpected-fallback"; exit 0 ;;
esac
exit 81
`
	for _, signal := range []string{"marker", "term"} {
		t.Run(signal, func(t *testing.T) {
			transform := func(text string) string {
				const anchor = "    set +m\n    if batched_race_cancel_requested; then return 125; fi\n    (\n"
				hook := "    : > \"${UT_HELPER_COMPLETION}.cancel\"\n"
				if signal == "term" {
					hook = "    fixture_self_pid fixture_owner_pid\n    kill -TERM \"$fixture_owner_pid\"\n"
				}
				return injectBatchedHook(t, text, anchor, "    set +m\n"+hook+"    if batched_race_cancel_requested; then return 125; fi\n    (\n")
			}
			script := batchedOwnerSetup + `
status=0
run_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 && ! -s "$completion" ]] || exit 90
[[ ! -e "$CASE_DIR/unexpected-fallback" ]] || exit 91
`
			out, err := scheduleHarnessWithMockTransform(t, script, mock, transform)
			if err != nil {
				t.Fatalf("cancellation before fallback fork (%s): %v\n%s", signal, err, out)
			}
		})
	}
}

func TestBatchedCancellationRetainsFallbackExit(t *testing.T) {
	t.Parallel()
	mock := `#!/bin/bash
case "${1:-}" in
 version) exit 0 ;;
 env) exit 1 ;;
 test)
  : > "$CASE_DIR/fallback-ready"
  while [[ ! -e "$CASE_DIR/fallback-release" ]]; do sleep .02; done
  exit 7 ;;
esac
exit 81
`
	transform := func(text string) string {
		const anchor = "    while kill -0 \"${pid}\" 2>/dev/null; do\n        if batched_race_cancel_requested; then\n"
		return injectBatchedHook(t, text, anchor, "    while kill -0 \"${pid}\" 2>/dev/null; do\n        ut_test_before_fallback_cancel \"$pid\"\n        if batched_race_cancel_requested; then\n")
	}
	script := batchedOwnerSetup + `
function logger() { printf '%s %s\n' "$1" "$2" >&2; }
function ut_test_before_fallback_cancel() {
 : > "$CASE_DIR/hook-ready"
 while [[ ! -e "$CASE_DIR/fallback-release" ]]; do sleep .02; done
 while kill -0 "$1" 2>/dev/null; do sleep .02; done
 : > "$CASE_DIR/leader-gone"
 while [[ ! -e "$CASE_DIR/hook-release" ]]; do sleep .02; done
}
start_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 || exit 90
for ((i=0; i<300; i++)); do
 [[ -e "$CASE_DIR/fallback-ready" && -e "$CASE_DIR/hook-ready" ]] && break
 kill -0 "$CURRENT_UT_PID" 2>/dev/null || exit 91
 sleep .02
done
[[ -e "$CASE_DIR/fallback-ready" && -e "$CASE_DIR/hook-ready" ]] || exit 92
: > "$CASE_DIR/fallback-release"
for ((i=0; i<300; i++)); do
 [[ -e "$CASE_DIR/leader-gone" ]] && break
 kill -0 "$CURRENT_UT_PID" 2>/dev/null || exit 93
 sleep .02
done
[[ -e "$CASE_DIR/leader-gone" ]] || exit 94
: > "$completion.cancel"; : > "$CASE_DIR/hook-release"
status=0; finish_ut_command || status=$?
[[ "$status" == 125 && "$CURRENT_UT_DRAIN_FAILED" == 1 && ! -s "$completion" ]] || exit 95
grep -q 'fallback exited with status 7 during cancellation' "$UT_STDERR" || exit 96
`
	out, err := scheduleHarnessWithMockTransform(t, script, mock, transform)
	if err != nil {
		t.Fatalf("cancellation lost the fallback exit status: %v\n%s", err, out)
	}
}

// The cancellation drain grace is not a timeout for a normally running test.
func TestBatchedFallbackDoesNotUseCancellationGraceAsTimeout(t *testing.T) {
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
	transform := func(text string) string {
		const anchor = "    pid=$!\n    while kill -0 \"${pid}\" 2>/dev/null; do\n"
		return injectBatchedHook(t, text, anchor, "    pid=$!\n    printf '%s\\n' \"$pid\" > \"$CASE_DIR/fallback-pid\"\n    while kill -0 \"${pid}\" 2>/dev/null; do\n")
	}
	script := batchedOwnerSetup + `
eval "$(declare -f wait_for_ut_process_group | sed '1s/^wait_for_ut_process_group /fixture_original_wait_for_ut_process_group /')"
function wait_for_ut_process_group() {
 if [[ -e "$CASE_DIR/fallback-pid" && ! -e "$CASE_DIR/fallback-release" ]] &&
    [[ "$1" == "$(<"$CASE_DIR/fallback-pid")" || "$1" == "${BATCHED_RACE_OWNER_PID:-none}" || "$1" == "${owner_pid:-none}" ]]; then
  : > "$CASE_DIR/premature-drain"; return 1
 fi
 fixture_original_wait_for_ut_process_group "$@"
}
start_ut_command embedded batched run_race_test_batches example/package "$PREBUILT_RACE_TEST_BINARY" 4 || exit 90
owner_pid=$CURRENT_UT_PID
for ((i=0; i<300; i++)); do
 [[ -e "$CASE_DIR/fallback-ready" ]] && break
 kill -0 "$CURRENT_UT_PID" 2>/dev/null || exit 91
 sleep .02
done
[[ -e "$CASE_DIR/fallback-ready" && ! -e "$CASE_DIR/premature-drain" ]] || exit 92
: > "$CASE_DIR/fallback-release"
status=0; finish_ut_command || status=$?
[[ "$status" == 0 && "$CURRENT_UT_DRAIN_FAILED" == 0 ]] || exit 90
[[ "$(grep -c '"Action":"pass"' "$UT_REPORT")" == 1 ]] || exit 91
[[ ! -e "$CASE_DIR/premature-drain" ]] || exit 93
`
	out, err := scheduleHarnessWithMockTransform(t, script, mock, transform)
	if err != nil {
		t.Fatalf("normal fallback exceeded cleanup grace: %v\n%s", err, out)
	}
}
