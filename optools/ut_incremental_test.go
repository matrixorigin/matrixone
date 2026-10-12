// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0

package optools

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestUTIncrementalActiveCases(t *testing.T) {
	out, err := exec.Command("python3", "testdata/active_ut_incremental_test.py", "-v").CombinedOutput()
	if err != nil {
		t.Fatalf("incremental reader: %v\n%s", err, out)
	}
}

func TestUTHeartbeatCancelsReader(t *testing.T) {
	script := `source ./run_ut.sh UT
mkfifo "$CASE_DIR/reader-ready" "$CASE_DIR/reader-block"
exec 9<> "$CASE_DIR/reader-ready"
exec 8<> "$CASE_DIR/reader-block"
trap 'stop_ut_heartbeat; exec 8>&-; exec 9>&-' EXIT
function python3() {
 sh -c 'echo "$PPID"' > "$CASE_DIR/reader-pid"
 printf 'ready\n' >&9
 read -r _ <&8
}
UT_HEARTBEAT_INTERVAL=1
start_ut_heartbeat
read -r -t 10 _ <&9 || exit 91
stop_ut_heartbeat
! kill -0 "$(<"$CASE_DIR/reader-pid")" 2>/dev/null || exit 92
[[ ! -e "$UT_HEARTBEAT_STATE" && ! -e "$UT_HEARTBEAT_STATE.out" ]] || exit 93
`
	if out, err := scheduleHarness(t, script); err != nil {
		t.Fatalf("reader cancellation: %v\n%s", err, out)
	}
}

func TestLightLinkGatePassThroughAndErrors(t *testing.T) {
	dir := t.TempDir()
	gate, err := filepath.Abs("ut_link_gate.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"compile", "link", "flock"} {
		body := "#!/bin/bash\nprintf '<%s>' \"$@\"\nexit 7\n"
		if name == "flock" {
			body = "#!/bin/bash\nexit 66\n"
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0755); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name, tool, slots, want string
		args                    []string
		status                  int
	}{
		{"compiler", "compile", "bad", "<arg with spaces><-x>", []string{"arg with spaces", "-x"}, 7},
		{"version", "link", "bad", "<-V=full>", []string{"-V=full"}, 7},
		{"invalid budget", "link", "0", "invalid slot count", nil, 2},
		{"locking error", "link", "1", "", nil, 66},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{gate, filepath.Join(dir, tc.tool)}, tc.args...)
			cmd := exec.Command("bash", args...)
			cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
				"UT_LINK_DIR="+dir, "UT_LINK_PARALLEL="+tc.slots)
			out, err := cmd.CombinedOutput()
			if err == nil || cmd.ProcessState.ExitCode() != tc.status || !strings.Contains(string(out), tc.want) {
				t.Fatalf("want status=%d containing %q: %v %s", tc.status, tc.want, err, out)
			}
		})
	}
}

func TestLightLinkGateConfiguration(t *testing.T) {
	for _, mode := range []string{"enabled", "disabled", "fallback", "external", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			script := `source ./run_ut.sh UT
function uname() { echo Linux; }
function flock() { :; }
function go() { [[ "$MODE" != external ]] || echo '-toolexec=custom'; }
light_stage_parallel=6
light_parallel=2
UT_LINK_PARALLEL=3
case "$MODE" in
 disabled) UT_LINK_PARALLEL=0 ;;
 fallback) function uname() { echo Darwin; } ;;
 invalid) UT_LINK_PARALLEL=65 ;;
esac
if [[ "$MODE" == invalid ]]; then
 ! prepare_light_link_gate || exit 90
 exit 0
fi
prepare_light_link_gate || exit 91
if [[ "$MODE" == enabled ]]; then
 [[ -d "$UT_LINK_DIR" && "${LIGHT_TOOL_FLAGS[0]}" == -toolexec ]] || exit 92
 [[ "$light_stage_parallel" == 6 ]] || exit 93
 for slot in 0 1 2; do [[ -f "$UT_LINK_DIR/slot-$slot" ]] || exit 94; done
 saved_dir=$UT_LINK_DIR
 # Exercise the actual background light dispatch and its normal cleanup path.
 start_light_race 'example/light' 6
 finish_light_race || exit 99
 [[ ! -e "$saved_dir" && -z "$UT_LINK_DIR" ]] || exit 95
else
 [[ -z "$UT_LINK_DIR" ]] || exit 96
 if [[ "$MODE" == disabled ]]; then
  [[ "$light_stage_parallel" == 6 ]] || exit 97
 else
  [[ "$light_stage_parallel" == 3 && "$light_parallel" == 2 ]] || exit 98
 fi
fi
`
			mock := `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
[[ "$1" == test && "$*" == *' -toolexec '* && "$*" == *'ut_link_gate.sh'* ]] || exit 81
[[ "$*" == *' -race '* && "$*" == *' -p 6 '* && -d "$UT_LINK_DIR" ]] || exit 82
printf 'gated light\n'
`
			if out, err := scheduleHarnessWithMock(t, script, mock, "MODE="+mode); err != nil {
				t.Fatalf("link gate configuration: %v\n%s", err, out)
			}
		})
	}
}
