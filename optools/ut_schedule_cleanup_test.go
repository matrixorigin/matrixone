// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The runner owns ordinary cancellation. This is the fixture's emergency
// cleanup after command failure, including descendants reparented by KILL.
func runScheduleHarnessCommand(ctx context.Context, cmd *exec.Cmd, root string) ([]byte, error, string) {
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	if cmd.WaitDelay == 0 {
		cmd.WaitDelay = 3 * time.Second
	}
	out, err := cmd.CombinedOutput()
	if err == nil && ctx.Err() == nil {
		return out, nil, ""
	}
	diagnostic, cleanupErr, survivors := cleanupScheduleFixture(root)
	if ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
	}
	if survivors {
		err = errors.Join(err, errScheduleFixtureSurvivors)
	}
	if cleanupErr != nil {
		err = errors.Join(err, cleanupErr)
	}
	return out, err, diagnostic
}

var errScheduleFixtureSurvivors = errors.New("runner left live fixture-owned processes after exit")

type scheduleProcess struct {
	pid   int
	start string
	state string
}

func scheduleOwnedProcess(pid int, root string) (*scheduleProcess, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if os.IsNotExist(err) || errors.Is(err, syscall.ESRCH) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// comm is parenthesized and may itself contain spaces or closing parentheses.
	end := bytes.LastIndexByte(data, ')')
	if end < 0 {
		return nil, fmt.Errorf("invalid process stat for %d", pid)
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return nil, fmt.Errorf("incomplete process stat for %d", pid)
	}
	if fields[0] == "Z" {
		return nil, nil
	}
	environment, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
	if os.IsNotExist(err) || errors.Is(err, syscall.ESRCH) || os.IsPermission(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	identity := []byte("CASE_DIR=" + root)
	for _, entry := range bytes.Split(environment, []byte{0}) {
		if bytes.Equal(entry, identity) {
			return &scheduleProcess{pid: pid, start: fields[19], state: fields[0]}, nil
		}
	}
	return nil, nil
}

func killScheduleProcess(root string, process *scheduleProcess) (bool, error) {
	current, err := scheduleOwnedProcess(process.pid, root)
	if err != nil {
		return false, err
	}
	if current == nil || current.start != process.start {
		return false, nil
	}
	// Individual PIDs only: the runner creates separate groups, and the parent
	// Go test can share a group with the harness shell.
	err = syscall.Kill(process.pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	return err == nil, err
}

func cleanupScheduleFixture(root string) (result string, cleanupErr error, survivors bool) {
	var diagnostic strings.Builder
	defer func() {
		reports, err := scheduleFixtureDiagnostics(root)
		result += reports
		cleanupErr = errors.Join(cleanupErr, err)
	}()
	// Linux CI exposes orphan identity through procfs. Other Unix platforms
	// retain ordinary runner cleanup; do not pretend procfs discovery ran there.
	if runtime.GOOS != "linux" {
		return "fixture orphan discovery unavailable on " + runtime.GOOS, nil, false
	}
	deadline := time.Now().Add(time.Second)
	for {
		entries, err := os.ReadDir("/proc")
		if err != nil {
			return diagnostic.String(), err, survivors
		}
		live := 0
		for _, entry := range entries {
			pid, err := strconv.Atoi(entry.Name())
			if err != nil {
				continue
			}
			process, err := scheduleOwnedProcess(pid, root)
			if os.IsPermission(err) || os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return diagnostic.String(), err, survivors
			}
			if process == nil {
				continue
			}
			live++
			survivors = true
			killed, err := killScheduleProcess(root, process)
			if err != nil {
				return diagnostic.String(), err, survivors
			}
			if killed {
				fmt.Fprintf(&diagnostic, "fixture cleanup pid=%d state=%s start=%s\n", pid, process.state, process.start)
			}
		}
		if live == 0 {
			break
		}
		if time.Now().After(deadline) {
			return diagnostic.String(), errors.New("fixture-owned processes did not drain"), survivors
		}
		time.Sleep(10 * time.Millisecond)
	}
	return diagnostic.String(), nil, survivors
}

func scheduleFixtureDiagnostics(root string) (string, error) {
	var diagnostic strings.Builder
	// Existing files are the diagnostic authority. Bound reads even if a test
	// produced a large report; never dump process environments.
	files := []string{"ut-report/ut-checkpoint.log", "ut-report/ut-stderr.log", "ut-report/ut-report.json"}
	reports, _ := filepath.Glob(filepath.Join(root, "scratch/*/*-UT-Report.out"))
	for _, report := range reports {
		relative, err := filepath.Rel(root, report)
		if err == nil {
			files = append(files, relative)
		}
	}
	for _, name := range files {
		f, err := os.Open(filepath.Join(root, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return diagnostic.String(), err
		}
		stat, err := f.Stat()
		if err == nil && stat.Size() > 4096 {
			_, err = f.Seek(-4096, io.SeekEnd)
		}
		if err != nil {
			_ = f.Close()
			return diagnostic.String(), err
		}
		data, err := io.ReadAll(io.LimitReader(f, 4096))
		_ = f.Close()
		if err != nil {
			return diagnostic.String(), err
		}
		fmt.Fprintf(&diagnostic, "%s:\n%s\n", name, data)
	}
	return diagnostic.String(), nil
}

func TestScheduleHarnessForcedCleanup(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("orphan discovery requires Linux procfs")
	}
	for _, phase := range []string{"cancel", "pipe-holder", "exit-143"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			neighbor := exec.Command("sleep", "30")
			neighbor.Env = append(os.Environ(), "CASE_DIR="+root+"/neighbor")
			if err := neighbor.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = neighbor.Process.Kill(); _ = neighbor.Wait() })
			t.Cleanup(func() { _, _, _ = cleanupScheduleFixture(root) })
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			defer writer.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			script := `if [[ "$PHASE" == cancel ]]; then trap '' TERM; fi
setsid bash -c 'trap "" TERM; printf "%s\n" "$BASHPID" >&3; exec sleep 30' &
if [[ "$PHASE" == cancel ]]; then wait; fi
if [[ "$PHASE" == exit-143 ]]; then exit 143; fi`
			cmd := exec.CommandContext(ctx, "bash", "-c", script)
			cmd.Env = append(os.Environ(), "CASE_DIR="+root, "PHASE="+phase)
			cmd.ExtraFiles = []*os.File{writer}
			cmd.WaitDelay = 100 * time.Millisecond
			type result struct {
				err        error
				diagnostic string
			}
			done := make(chan result, 1)
			go func() {
				_, err, diagnostic := runScheduleHarnessCommand(ctx, cmd, root)
				done <- result{err, diagnostic}
			}()
			if err := reader.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			var pid int
			if _, err := fmt.Fscanf(reader, "%d\n", &pid); err != nil {
				t.Fatal(err)
			}
			if phase == "cancel" {
				cancel()
			}
			var got result
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("forced cleanup did not finish")
			}
			expected := error(exec.ErrWaitDelay)
			if phase == "cancel" {
				expected = context.Canceled
			}
			if phase == "exit-143" {
				var exit *exec.ExitError
				if !errors.As(got.err, &exit) || exit.ExitCode() != 143 {
					t.Fatalf("lost exit143: %v", got.err)
				}
				if _, bare := got.err.(*exec.ExitError); bare {
					t.Fatal("orphan cleanup hid a broken cancellation")
				}
			} else if !errors.Is(got.err, expected) {
				t.Fatalf("error = %v, want %v; %s", got.err, expected, got.diagnostic)
			}
			if !errors.Is(got.err, errScheduleFixtureSurvivors) {
				t.Fatalf("orphan failure was hidden: %v", got.err)
			}
			if !strings.Contains(got.diagnostic, fmt.Sprintf("pid=%d ", pid)) {
				t.Fatalf("missing owned child cleanup: %s", got.diagnostic)
			}
			process, err := scheduleOwnedProcess(pid, root)
			if err != nil || process != nil {
				t.Fatalf("owned child survived: %v %v", process, err)
			}
			if err := neighbor.Process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("neighbor was touched: %v", err)
			}
			if process, err := scheduleOwnedProcess(neighbor.Process.Pid, root+"/neighbor"); err != nil || process == nil {
				t.Fatalf("neighbor is not live: %v %v", process, err)
			}
		})
	}
}

func TestScheduleCleanupRejectsStaleIdentity(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process identity requires Linux procfs")
	}
	root := t.TempDir()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	cmd := exec.Command("bash", "-c", `printf 'ready\n' >&3; while :; do sleep 30; done`)
	cmd.Env = append(os.Environ(), "CASE_DIR="+root)
	cmd.ExtraFiles = []*os.File{writer}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	t.Cleanup(func() { _, _, _ = cleanupScheduleFixture(root) })
	if err := reader.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var ready string
	if _, err := fmt.Fscanln(reader, &ready); err != nil {
		t.Fatal(err)
	}
	process, err := scheduleOwnedProcess(cmd.Process.Pid, root)
	if err != nil || process == nil {
		t.Fatalf("missing live fixture identity: %v %v", process, err)
	}
	process.start += "-stale"
	killed, err := killScheduleProcess(root, process)
	if err != nil || killed {
		t.Fatalf("stale identity signalled: %v %v", killed, err)
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("live process was touched: %v", err)
	}
	if _, err, _ := cleanupScheduleFixture(root); err != nil {
		t.Fatal(err)
	}
	if process, err := scheduleOwnedProcess(cmd.Process.Pid, root); err != nil || process != nil {
		t.Fatalf("fixture cleanup failed: %v %v", process, err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("fixture process was not killed")
	}
	if process, err := scheduleOwnedProcess(cmd.Process.Pid, root); err != nil || process != nil {
		t.Fatalf("reaped process still owned: %v %v", process, err)
	}
}

const cancellationGoMock = `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
if [[ "$1" == list ]]; then
 printf '%s\t%s\n' "$CASE_DIR" 'example/plan'
 if [[ "$PHASE" == metadata ]]; then printf 'ready\n' >&8; read -r _ <&9; fi
 exit 0
fi
if [[ "$1" == test ]]; then
 previous=""
 for arg in "$@"; do
  if [[ "$previous" == -o ]]; then output=$arg; fi
  previous=$arg
 done
 cat > "$output" <<'BINARY'
#!/bin/bash
if [[ "$*" == *-test.list=* ]]; then
 printf 'TestOne\n'
 if [[ "$PHASE" == list ]]; then printf 'ready\n' >&8; read -r _ <&9; fi
 exit 0
fi
exec sleep 30
BINARY
 chmod +x "$output"
 if [[ "$PHASE" == build ]]; then printf 'ready\n' >&8; read -r _ <&9; fi
 exit 0
fi
if [[ "$1" == tool ]]; then printf 'ready\n' >&8; read -r _ <&9; fi
exit 99
`

func TestHelperCancellationOwnership(t *testing.T) {
	for _, tc := range []struct{ helper, phase, mode string }{
		{"plan", "metadata", "cancel"}, {"plan", "build", "cancel"},
		{"plan", "list", "cancel"}, {"plan", "shard", "cancel"},
		{"engine", "metadata", "duplicate"}, {"plan", "metadata", "duplicate"},
		{"engine", "metadata", "failed-drain"}, {"plan", "metadata", "failed-drain"},
		{"prebuild", "build", "failed-drain"},
	} {
		t.Run(tc.helper+"/"+tc.phase+"/"+tc.mode, func(t *testing.T) {
			transform := func(text string) string {
				var anchor string
				occurrence := 0
				switch tc.helper {
				case "engine":
					anchor = "        child_pid=$!\n"
				case "prebuild":
					anchor = "            child_pids[package_index]=$!\n"
				case "plan":
					anchor = "    plan_child_pid=$!\n"
					switch tc.phase {
					case "build":
						occurrence = 1
					case "list":
						occurrence = 2
					case "shard":
						anchor = "            shard_pids[shard]=$!\n"
					}
				}
				expected := 1
				if tc.helper == "plan" && tc.phase != "shard" {
					expected = 3
				}
				if strings.Count(text, anchor) != expected {
					t.Fatalf("publication anchor %q: count %d", anchor, strings.Count(text, anchor))
				}
				offset := 0
				for i := 0; i <= occurrence; i++ {
					offset += strings.Index(text[offset:], anchor)
					if i < occurrence {
						offset += len(anchor)
					}
				}
				return text[:offset] + "    ut_test_publication \"$!\"\n" + text[offset:]
			}
			script := `source ./run_ut.sh UT
function logger() { :; }
ENGINE_RACE_REPORT="$CASE_DIR/engine-report"
ENGINE_RACE_TEST_BINARY="$CASE_DIR/engine.test"
PLAN_RACE_SHARDS=1
PLAN_RACE_PARALLEL=1
mkfifo "$CASE_DIR/ready" "$CASE_DIR/hold"
exec 8<>"$CASE_DIR/ready" 9<>"$CASE_DIR/hold"
function cleanup_check() {
 status=$?
 [[ "$status" == "$EXPECTED_STATUS" ]] || status=90
 if [[ -s "$CASE_DIR/child.pid" ]]; then
  pid=$(<"$CASE_DIR/child.pid")
  if ut_process_group_alive "$pid"; then kill -KILL -- -"$pid" 2>/dev/null || true; status=91; fi
 fi
 if [[ "$MODE" == failed-drain ]]; then
  [[ -e "$(<"$CASE_DIR/artifact")" && ! -e "$CASE_DIR/joined" ]] || status=92
 fi
 (( $(wc -l < "$CASE_DIR/drains") == 1 )) || status=93
 printf 'OWNERSHIP %s\n' "$status"
 exit "$status"
}
trap cleanup_check EXIT
eval "$(declare -f terminate_ut_process_groups | sed '1s/terminate_ut_process_groups/original_terminate_ut_process_groups/')"
function terminate_ut_process_groups() {
 printf 'drain\n' >> "$CASE_DIR/drains"
 if [[ "$MODE" == duplicate ]]; then kill -TERM $$; fi
 original_terminate_ut_process_groups "$@" || return $?
 [[ "$MODE" != failed-drain ]]
}
function ut_test_publication() {
 read -r _ <&8
 printf '%s\n' "$1" > "$CASE_DIR/child.pid"
 if [[ "$MODE" == failed-drain ]]; then
  # Record an owned artifact and make a join after failed drain observable.
  if [[ "$HELPER" == plan ]]; then touch "$plan_test_binary"; printf '%s\n' "$plan_test_binary" > "$CASE_DIR/artifact";
  elif [[ "$HELPER" == engine ]]; then printf '%s\n' "$metadata_file" > "$CASE_DIR/artifact";
  else printf '%s\n' "$package_report" > "$CASE_DIR/artifact"; fi
  function wait() { touch "$CASE_DIR/joined"; builtin wait "$@"; }
 fi
 kill -TERM $$
}
case "$HELPER" in
 engine) run_engine_race_shards example/engine 1 ;;
 plan) run_plan_race_shards example/plan ;;
 prebuild) run_embedded_prebuild example/plan 1 "$CASE_DIR/prebuild" ;;
esac
`
			expected := 143
			if tc.mode == "failed-drain" {
				expected = 125
			}
			out, err := scheduleHarnessWithMockTransform(t, script, cancellationGoMock, transform,
				"HELPER="+tc.helper, "PHASE="+tc.phase, "MODE="+tc.mode, fmt.Sprintf("EXPECTED_STATUS=%d", expected))
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != expected || !strings.Contains(string(out), fmt.Sprintf("OWNERSHIP %d", expected)) {
				t.Fatalf("cancellation ownership: %v\n%s", err, out)
			}
		})
	}
}

func TestPrebuildReportOpenFailureRetainsUndrainedArtifacts(t *testing.T) {
	transform := func(text string) string {
		const anchor = "            child_pids[package_index]=$!\n"
		if strings.Count(text, anchor) != 1 {
			t.Fatal("missing prebuild publication")
		}
		return strings.Replace(text, anchor, anchor+"            ut_test_prebuild_registered\n", 1)
	}
	script := `source ./run_ut.sh UT
function logger() { :; }
mkfifo "$CASE_DIR/ready" "$CASE_DIR/hold"
exec 8<>"$CASE_DIR/ready" 9<>"$CASE_DIR/hold"
function ut_test_prebuild_registered() {
 read -r _ <&8
 printf '%s\n' "${child_pids[package_index]}" > "$CASE_DIR/child.pid"
 # Fail the second report open after the first child is admitted.
 mkdir "$CASE_DIR/prebuild.build.1"
 function wait() { touch "$CASE_DIR/joined"; builtin wait "$@"; }
}
eval "$(declare -f terminate_ut_process_groups | sed '1s/terminate_ut_process_groups/original_terminate_ut_process_groups/')"
function terminate_ut_process_groups() { original_terminate_ut_process_groups "$@" || return $?; return 1; }
status=0
run_embedded_prebuild $'example/a\nexample/b' 2 "$CASE_DIR/prebuild" || status=$?
[[ "$status" == 125 && ! -e "$CASE_DIR/joined" ]] || exit 90
[[ -f "$CASE_DIR/prebuild.build.0" && -f "$CASE_DIR/prebuild.package.0.test" ]] || exit 91
! ut_process_group_alive "$(<"$CASE_DIR/child.pid")" || exit 92
`
	out, err := scheduleHarnessWithMockTransform(t, script, cancellationGoMock, transform, "PHASE=build")
	if err != nil {
		t.Fatalf("partial prebuild admission cleanup: %v\n%s", err, out)
	}
}

func TestPrebuildFailedDrainBlocksConsumers(t *testing.T) {
	for _, consumer := range []string{"embedded", "companions"} {
		t.Run(consumer, func(t *testing.T) {
			script := `source ./run_ut.sh UT
function logger() { :; }
CLUSTER_PREBUILD_DIR="$CASE_DIR/artifacts"
CLUSTER_PREBUILD_REPORT="$CLUSTER_PREBUILD_DIR/report"
mkdir "$CLUSTER_PREBUILD_DIR"
printf 'retained\n' > "$CLUSTER_PREBUILD_REPORT.build.0"
# The helper has reaped its shell but reports an unproven descendant drain.
(exit 125) &
CLUSTER_PREBUILD_JOB_PID=$!
function run_ut_command() { touch "$CASE_DIR/fallback"; return 0; }
status=0
if [[ "$CONSUMER" == embedded ]]; then
 run_embedded_tests example/a 1 || status=$?
else
 stop_race_companions || status=$?
fi
[[ "$status" == 125 ]] || exit 90
[[ -f "$CLUSTER_PREBUILD_REPORT.build.0" && -d "$CLUSTER_PREBUILD_DIR" ]] || exit 91
[[ ! -e "$CASE_DIR/fallback" && -z "$CLUSTER_PREBUILD_JOB_PID" ]] || exit 92
run_embedded_tests example/a 1; [[ "$?" == 125 ]] || exit 93
start_embedded_prebuild example/a 1; [[ "$?" == 125 ]] || exit 94
trap 'status=$?; [[ -f "$CLUSTER_PREBUILD_REPORT.build.0" && -d "$CLUSTER_PREBUILD_DIR" && ! -e "$CASE_DIR/fallback" ]] || status=95; exit "$status"' EXIT
trap handle_ut_termination TERM
kill -TERM "$$"
exit 96
`
			out, err := scheduleHarness(t, script, "CONSUMER="+consumer)
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 125 {
				t.Fatalf("failed-drain consumer %s: %v\n%s", consumer, err, out)
			}
		})
	}
}
