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
	var cancelDiagnostic string
	cmd.Cancel = func() error {
		err := cmd.Process.Signal(syscall.SIGTERM)
		cancelDiagnostic = scheduleFixtureProcessDiagnostics(root, "deadline")
		return err
	}
	if cmd.WaitDelay == 0 {
		cmd.WaitDelay = 3 * time.Second
	}
	out, err := cmd.CombinedOutput()
	if err == nil && ctx.Err() == nil {
		return out, nil, ""
	}
	processDiagnostic := scheduleFixtureProcessDiagnostics(root, "before-emergency-cleanup")
	diagnostic, cleanupErr, survivors := cleanupScheduleFixture(root)
	diagnostic = cancelDiagnostic + processDiagnostic + diagnostic
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

// Only the explicitly traced cancellation fixture collects these observations.
// Never expose environment contents, and recheck process start identity.
func scheduleFixtureProcessDiagnostics(root, phase string) string {
	if runtime.GOOS != "linux" {
		return ""
	}
	if _, err := os.Stat(filepath.Join(root, "ut-signal-trace.log")); err != nil {
		return ""
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return fmt.Sprintf("process snapshot %s: %v\n", phase, err)
	}
	var out strings.Builder
	fmt.Fprintf(&out, "process snapshot %s\n", phase)
	deadline := time.Now().Add(100 * time.Millisecond)
	records := 0
	for _, entry := range entries {
		if records == 32 || time.Now().After(deadline) {
			out.WriteString("process snapshot truncated\n")
			break
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		process, err := scheduleOwnedProcess(pid, root)
		if err != nil || process == nil {
			continue
		}
		var details strings.Builder
		for _, name := range []string{"stat", "status", "wchan", "task/" + entry.Name() + "/children"} {
			file, err := os.Open(fmt.Sprintf("/proc/%d/%s", pid, name))
			if err != nil {
				continue
			}
			data, _ := io.ReadAll(io.LimitReader(file, 4096))
			_ = file.Close()
			if name == "status" {
				for _, line := range strings.Split(string(data), "\n") {
					if strings.HasPrefix(line, "Sig") || strings.HasPrefix(line, "ShdPnd:") {
						fmt.Fprintln(&details, line)
					}
				}
			} else {
				fmt.Fprintf(&details, "%s: %s\n", name, data)
			}
		}
		current, err := scheduleOwnedProcess(pid, root)
		if err != nil || current == nil || current.start != process.start {
			continue
		}
		fmt.Fprintf(&out, "pid=%d start=%s\n%s", pid, process.start, details.String())
		records++
	}
	return out.String()
}

func scheduleFixtureDiagnostics(root string) (string, error) {
	var diagnostic strings.Builder
	// Existing files are the diagnostic authority. Bound reads even if a test
	// produced a large report; never dump process environments.
	files := []string{"ut-signal-trace.log", "ut-report/ut-checkpoint.log", "ut-report/ut-stderr.log", "ut-report/ut-report.json"}
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
			if err := os.WriteFile(filepath.Join(root, "ut-signal-trace.log"), nil, 0600); err != nil {
				t.Fatal(err)
			}
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
			if !strings.Contains(got.diagnostic, "process snapshot before-emergency-cleanup") || strings.Contains(got.diagnostic, fmt.Sprintf("pid=%d ", neighbor.Process.Pid)) {
				t.Fatalf("missing scoped pre-cleanup evidence: %s", got.diagnostic)
			}
			if !strings.Contains(got.diagnostic, "ShdPnd:") {
				t.Fatalf("missing process-directed pending signal evidence: %s", got.diagnostic)
			}
			if phase == "cancel" && !strings.Contains(got.diagnostic, "process snapshot deadline") {
				t.Fatal("missing deadline snapshot")
			}
			if len(got.diagnostic) > 400000 {
				t.Fatal("unbounded process diagnostics")
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
 printf 'OWNERSHIP %s\n' "$status" >&7
 exit "$status"
}
exec 7>&1
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

func TestHelperFailedDrainKeepsParentOwnership(t *testing.T) {
	for _, helper := range []string{"engine", "prebuild", "inventory"} {
		modes := []string{"failed-drain", "failed-drain-term"}
		if helper == "prebuild" {
			modes = append(modes, "drained")
		}
		for _, mode := range modes {
			t.Run(helper+"/"+mode, func(t *testing.T) {
				transform := func(text string) string {
					anchor, hook := "        child_pid=$!\n", "        ut_test_publication \"$!\" \"$*\"\n"
					if helper == "prebuild" {
						anchor = "            child_pids[package_index]=$!\n"
						hook = "            ut_test_publication \"$!\" ''\n"
					}
					if helper == "inventory" {
						anchor = "    pid=$!\n    set +m\n"
						hook = "    pid=$!\n    ut_test_publication \"$pid\" \"\"\n    set +m\n"
					}
					if strings.Count(text, anchor) != 1 {
						t.Fatal("missing unique helper publication")
					}
					if helper == "inventory" {
						return strings.Replace(text, anchor, hook, 1)
					}
					return strings.Replace(text, anchor, hook+anchor, 1)
				}
				script := `source ./run_ut.sh UT
function logger() { :; }
mkfifo "$CASE_DIR/ready" "$CASE_DIR/hold"
exec 8<>"$CASE_DIR/ready" 9<>"$CASE_DIR/hold"
cat > "$CASE_DIR/helper.sh" <<'CHILD'
cd "$CASE_DIR/optools"
source ./run_ut.sh UT
function logger() { :; }
ENGINE_RACE_REPORT="$PROBE_REPORT"
ENGINE_RACE_TEST_BINARY="$PROBE_BINARY"
# Both the old restored-trap return and the new terminal exit reach this
# injection. Require the marker so removing the old window cannot empty it.
function exit() {
 if [[ "$1" == 125 && "$MODE" == failed-drain-term ]]; then
  touch "$CASE_DIR/term-sent"
  kill -TERM "$$"
 fi
 builtin exit "$@"
}
function wait_for_ut_process_group() {
 touch "$CASE_DIR/drain-checked"
 function wait() { touch "$CASE_DIR/forbidden-join"; builtin wait "$@"; }
 return 1
}
eval "$(declare -f terminate_ut_process_groups | sed '1s/terminate_ut_process_groups/original_terminate_ut_process_groups/')"
function terminate_ut_process_groups() {
 if [[ "$MODE" == drained ]]; then original_terminate_ut_process_groups "$@"; return $?; fi
 touch "$CASE_DIR/drain-checked"
 function wait() { touch "$CASE_DIR/forbidden-join"; builtin wait "$@"; }
 return 1
}
function ut_test_publication() {
 if [[ "$HELPER" == engine && "$2" != *'go test'* ]]; then return; fi
 read -r -t 5 _ <&8 || exit 90
 printf '%s\n' "$1" > "$CASE_DIR/child.pid"
 if [[ "$HELPER" == engine || "$HELPER" == inventory ]]; then
  printf 'retained\n' > "$ENGINE_RACE_REPORT"
  kill -TERM "$$"
 else
  # Fail report2 after admitting the first compiler, whose binary is live.
  mkdir "$report_base.build.1"
 fi
}
if [[ "$HELPER" == engine ]]; then
 run_engine_race_shards example/engine 1
elif [[ "$HELPER" == prebuild ]]; then
 run_embedded_prebuild $'example/a\nexample/b' 2 "$PROBE_REPORT"
else
 cat > "$CASE_DIR/list.sh" <<'LIST'
#!/bin/bash
trap '' TERM
printf 'discovery diagnostic\n'
printf 'ready\n' >&8
read -r _ <&9
LIST
 chmod +x "$CASE_DIR/list.sh"
 run_race_inventory_with_deadline "$CASE_DIR" "$CASE_DIR/list.sh" "$CASE_DIR/inventory" "$(( $(date +%s) + 10 ))"
fi
status=$?
touch "$CASE_DIR/helper-returned"
exit "$status"
CHILD
function run_engine_race_shards() {
 export PROBE_REPORT="$ENGINE_RACE_REPORT" PROBE_BINARY="$ENGINE_RACE_TEST_BINARY"
 exec bash "$CASE_DIR/helper.sh"
}
function run_embedded_prebuild() {
 export PROBE_REPORT="$CLUSTER_PREBUILD_REPORT" PROBE_BINARY=""
 exec bash "$CASE_DIR/helper.sh"
}
function cleanup_check() {
 local status=$? pid
 if [[ -f "$CASE_DIR/child.pid" ]]; then
  pid=$(<"$CASE_DIR/child.pid")
  # Retention is observed while the actual independently owned writer lives.
  if [[ "$MODE" != drained ]]; then ut_process_group_alive "$pid" || status=91; fi
  terminate_ut_process_group "$pid" KILL
  wait_for_ut_process_group "$pid" 1 || status=92
 fi
 exit "$status"
}
trap cleanup_check EXIT
if [[ "$HELPER" == engine ]]; then
 ENGINE_RACE_REPORT="$CASE_DIR/engine-report"
 ENGINE_RACE_TEST_BINARY="$CASE_DIR/engine.test"
 start_engine_race example/engine 1
 status=0
 join_ut_owner ENGINE_RACE_JOB_PID ENGINE_RACE_DRAIN_FAILED || status=$?
 [[ "$status" == 125 && -z "$ENGINE_RACE_JOB_PID" ]] || exit 93
 retained_binary=$ENGINE_RACE_TEST_BINARY
 consume_engine_race_report; [[ "$?" == 125 ]] || exit 94
elif [[ "$HELPER" == inventory ]]; then
 export PROBE_REPORT="$CASE_DIR/report" PROBE_BINARY="$CASE_DIR/list.sh"
 retained_binary="$PROBE_BINARY"
 start_ut_command serial inventory bash "$CASE_DIR/helper.sh"
 status=0
 finish_ut_command || status=$?
 [[ "$status" == 125 && -z "$CURRENT_UT_PID" ]] || exit 93
 grep -qx 'discovery diagnostic' "$CASE_DIR/inventory" || exit 94
else
 UT_PREBUILD_MIN_FREE_KB=1
 start_embedded_prebuild $'example/a\nexample/b' 2
 retained_binary="$CLUSTER_PREBUILD_REPORT.package.0.test"
 function run_ut_command() { touch "$CASE_DIR/fallback"; return 0; }
 status=0
 run_embedded_tests $'example/a\nexample/b' || status=$?
 if [[ "$MODE" == drained ]]; then
  [[ "$status" == 0 && -z "$CLUSTER_PREBUILD_JOB_PID" && ! -e "$retained_binary" && -e "$CASE_DIR/fallback" && -e "$CASE_DIR/helper-returned" ]] || exit 95
  ! ut_process_group_alive "$(<"$CASE_DIR/child.pid")" || exit 96
  exit 0
 fi
 [[ "$status" == 125 && -z "$CLUSTER_PREBUILD_JOB_PID" ]] || exit 95
fi
[[ -f "$CASE_DIR/drain-checked" && -f "$retained_binary" && ! -e "$CASE_DIR/fallback" && ! -e "$CASE_DIR/helper-returned" && ! -e "$CASE_DIR/forbidden-join" ]] || exit 96
if [[ "$MODE" == failed-drain-term ]]; then [[ -f "$CASE_DIR/term-sent" ]] || exit 97; fi
start_ut_command heavy after-failure touch "$CASE_DIR/admitted"; [[ "$?" == 125 ]] || exit 98
[[ ! -e "$CASE_DIR/admitted" ]] || exit 99
trap handle_ut_termination TERM
kill -TERM "$$"
exit 100
`
				out, err := scheduleHarnessWithMockTransform(t, script, cancellationGoMock, transform,
					"HELPER="+helper, "MODE="+mode, "PHASE=build")
				if mode == "drained" {
					if err != nil {
						t.Fatalf("drained prebuild fallback: %v\n%s", err, out)
					}
					return
				}
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.ExitCode() != 125 {
					t.Fatalf("failed-drain parent ownership: %v\n%s", err, out)
				}
			})
		}
	}
}

func TestPrebuildFailedDrainBlocksConsumers(t *testing.T) {
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
run_embedded_tests example/a 1 || status=$?
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
	out, err := scheduleHarness(t, script)
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 125 {
		t.Fatalf("failed-drain embedded consumer: %v\n%s", err, out)
	}
}

func TestIssuesRootDispatchDrainsHeartbeat(t *testing.T) {
	data, err := os.ReadFile("run_ut.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	index := strings.Index(text, "if [[ 'SCA' == $TEST_TYPE ]]; then")
	if index < 0 {
		t.Fatal("missing runner dispatch")
	}
	for _, tc := range []struct {
		name, companion, status string
		want                    int
	}{
		{"failed-drain", "none", "125", 125},
		{"failed-drain-light", "light", "125", 125},
		{"failed-drain-prebuild", "prebuild", "125", 125},
		{"ordinary-failure", "none", "7", 1},
		{"success", "none", "0", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The outer pipeline cannot exit until the independent reader gets
			// natural EOF. exec.WaitDelay closing its own pipes cannot satisfy it.
			script := `cat > "$CASE_DIR/root.sh" <<'ROOT'
source ./run_ut.sh UT
function logger() { :; }
function make() { :; }
function egrep() { echo fixture.pb.go; }
function report_cgroup_memory_usage() { :; }
MO_CL_CUDA=1
UT_SHARD=issues
UT_PREBUILD_EMBEDDED=0
UT_ISSUES_BATCHES=4
UT_HEARTBEAT_INTERVAL=60
UT_PREBUILD_MIN_FREE_KB=1
fixture_companion_pid=""
mkfifo "$CASE_DIR/timer-ready" "$CASE_DIR/companion-ready" "$CASE_DIR/companion-hold"
exec 7<> "$CASE_DIR/timer-ready" 8<> "$CASE_DIR/companion-ready" 9<> "$CASE_DIR/companion-hold"
function run_embedded_prebuild() {
 trap 'touch "$CASE_DIR/companion-stopped"; exit 0' TERM
 printf 'ready\n' >&8
 read -r _ <&9
}
function prepare_ut_native() {
 case "$COMPANION" in
 light) start_light_race example/light 1; fixture_companion_pid="$LIGHT_RACE_JOB_PID" ;;
 prebuild) start_embedded_prebuild example/embedded 1; fixture_companion_pid="$CLUSTER_PREBUILD_JOB_PID" ;;
 esac
}
function go() {
 if [[ "$1" == clean ]]; then return 0; fi
 if [[ "$1" != list ]]; then return 0; fi
 shift 2
 if [[ "$1" == ./... ]]; then
  printf '%s\n' github.com/matrixorigin/matrixone/pkg/{sql/plan,vm/engine/test,vectorindex/hnsw,tests/issues,backup,fileservice,sql/plan/function,vm/engine/tae/db/test}
 else
  for package in "$@"; do echo "github.com/matrixorigin/matrixone/${package#./}"; done
 fi
}
function list_embedded_cluster_test_packages() { :; }
# Only expensive child work is replaced: scheduling, joining, root dispatch,
# heartbeat and companion launch/shutdown remain the production functions.
function run_issues_race_batches() {
 read -r -t 5 timer_pid <&7 || return 90
 printf '%s\n' "$timer_pid" > "$CASE_DIR/timer.pid"
 if [[ "$COMPANION" != none ]]; then read -r -t 5 _ <&8 || return 91; fi
 printf 'retained\n' > "$PREBUILT_RACE_REPORT.build.0"
 touch "$PREBUILT_RACE_TEST_BINARY"
 return "$ISSUES_STATUS"
}
function post_test() { touch "$CASE_DIR/post-test"; }
function ut_summary() { touch "$CASE_DIR/summary"; exit "$UT_TEST_STATUS"; }
function check_root_exit() {
 local status=$? heartbeat_pid timer_pid
 heartbeat_pid=$(sed -n 's/.*event=heartbeat-start .*detail=pid=\([0-9]*\).*/\1/p' "$UT_CHECKPOINT")
 timer_pid=$(cat "$CASE_DIR/timer.pid")
 [[ -n "$heartbeat_pid" && -n "$timer_pid" ]] || exit 92
 if kill -0 "$heartbeat_pid" 2>/dev/null || kill -0 "$timer_pid" 2>/dev/null; then exit 93; fi
 if [[ "$COMPANION" != none ]]; then
  [[ -n "$fixture_companion_pid" && -e "$CASE_DIR/companion-stopped" ]] || exit 94
  if kill -0 "$fixture_companion_pid" 2>/dev/null; then exit 95; fi
 fi
 if [[ "$ISSUES_STATUS" == 125 ]]; then
  [[ -f "$PREBUILT_RACE_REPORT.build.0" && -f "$PREBUILT_RACE_TEST_BINARY" ]] || exit 96
  [[ ! -e "$CASE_DIR/post-test" && ! -e "$CASE_DIR/summary" ]] || exit 97
  if [[ "$COMPANION" == prebuild ]]; then
   [[ -d "$CLUSTER_PREBUILD_DIR" && -f "$CLUSTER_PREBUILD_REPORT" ]] || exit 98
  fi
 else
  [[ -e "$CASE_DIR/post-test" && -e "$CASE_DIR/summary" ]] || exit 99
  [[ ! -e "$G_WKSP/$G_TS-issues-race-report.out.build.0" && ! -e "$G_WKSP/$G_TS-issues-race.test" ]] || exit 100
 fi
 exit "$status"
}
trap check_root_exit EXIT
` + text[index:] + `
ROOT
set -o pipefail
bash "$CASE_DIR/root.sh" 2>&1 | cat
`
			mock := `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
trap 'touch "$CASE_DIR/companion-stopped"; exit 0' TERM
printf 'ready\n' >&8
read -r _ <&9
`
			transform := func(source string) string {
				const anchor = "            heartbeat_sleep_pid=$!\n"
				if strings.Count(source, anchor) != 1 {
					t.Fatal("missing unique heartbeat timer publication")
				}
				return strings.Replace(source, anchor, anchor+"            printf '%s\\n' \"$heartbeat_sleep_pid\" >&7\n", 1)
			}
			out, err := scheduleHarnessWithMockTransform(t, script, mock, transform,
				"COMPANION="+tc.companion, "ISSUES_STATUS="+tc.status)
			if tc.want == 0 {
				if err != nil {
					t.Fatalf("root success: %v\n%s", err, out)
				}
				return
			}
			// Emergency fixture reclamation joins a distinct error; accepting
			// only the bare exit ensures it cannot conceal a leaked process.
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != tc.want {
				t.Fatalf("root status: want %d, got %v\n%s", tc.want, err, out)
			}
		})
	}
}
