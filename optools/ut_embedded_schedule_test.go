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
	"os/exec"
	"strings"
	"testing"
)

const embeddedSetup = `source ./run_ut.sh UT
function logger() { :; }
trap handle_ut_termination TERM
[[ "$UT_PREBUILD_EMBEDDED" == 0 && "$UT_HARD_TIMEOUT" == 120m ]] || exit 80
scope=$'example/a\nexample/b\nexample/c'
mkfifo "$CASE_DIR/release-a" "$CASE_DIR/ready" "$CASE_DIR/hold"
exec 7<>"$CASE_DIR/release-a"
exec 8<>"$CASE_DIR/ready"
exec 9<>"$CASE_DIR/hold"
export UT_TIMEOUT=17 TAGS=matrixone_test
`

const embeddedGoMock = `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
[[ "$1" == test ]] || exit 81
[[ " $* " == *' -race '* && " $* " == *' -short '* && " $* " == *' -tags matrixone_test '* && " $* " == *' -timeout 17m '* && " $* " == *' -mod=readonly '* && " $* " == *' -vet=off '* ]] || exit 82
if [[ " $* " == *' -c '* ]]; then
 [[ " $* " == *' -ldflags=-w '* ]] || exit 89
 package=${!#}; leaf=${package##*/}
 [[ " $* " == *' -p 1 '* ]] || exit 83
 mkdir "$CASE_DIR/compiled-$leaf" || exit 84
 while [[ "$1" != -o ]]; do shift; done
 output=$2
 printf 'build-%s\n' "$leaf"
 if [[ "$MODE" == build-cancel || "$MODE" == report-open ]]; then
  trap 'touch "$CASE_DIR/stopped-$leaf"; exit 143' TERM
  printf '%s\n' "$$" > "$CASE_DIR/pid-$leaf"
  printf 'ready\n' >&8
  if [[ "$MODE" == build-cancel && -n "${RUNNER_PID:-}" ]]; then
   while :; do
    if [[ -e "$CASE_DIR/join-waiting" ]]; then
     if mkdir "$CASE_DIR/term-sent" 2>/dev/null; then kill -TERM "$RUNNER_PID"; fi
    fi
    read -r -t 0.01 _ <&9 || true
   done
  else
   read -r _ <&9
  fi
 fi
 if [[ "$MODE" == reclaim ]]; then
  if [[ "$leaf" == a ]]; then read -r _ <&7; fi
  if [[ "$leaf" == c ]]; then printf 'release\n' >&7; fi
 fi
 [[ "$MODE" != build-failure || "$leaf" != b ]] || exit 7
 [[ "$MODE" != no-binary || "$leaf" != b ]] || exit 0
 # These outputs must NEVER execute: go test owns all runtime work.
 printf '#!/bin/bash\nexit 99\n' > "$output"
 chmod +x "$output"
 exit 0
fi
[[ " $* " != *' -ldflags=-w '* ]] || exit 88
[[ " $* " == *' -p 2 '* && " $* " == *' -json '* && " $* " == *' -v '* && " $* " == *' example/a example/b example/c '* ]] || exit 85
[[ "$PWD" -ef "$CASE_DIR" ]] || exit 86
mkdir "$CASE_DIR/authoritative" || exit 87
printf 'authoritative\n'
[[ "$MODE" != test-failure ]]
`

func TestEmbeddedPrebuildAuthoritativeExecution(t *testing.T) {
	defaultScript := `unset UT_PREBUILD_EMBEDDED
source ./run_ut.sh UT
[[ "$UT_PREBUILD_EMBEDDED" == 1 ]] || exit 80
`
	if out, err := scheduleHarness(t, defaultScript); err != nil {
		t.Fatalf("embedded prebuild default: %v\n%s", err, out)
	}

	for _, mode := range []string{"success", "reclaim", "build-failure", "no-binary", "off", "test-failure", "report-open"} {
		t.Run(mode, func(t *testing.T) {
			script := embeddedSetup + `
function ut_test_open_embedded_report() {
 if [[ "$package_index" == 1 ]]; then read -r _ <&8; return 1; fi
 return 0
}
if [[ "$MODE" != off ]]; then start_embedded_prebuild "$scope" 2; fi
artifact_dir=$CLUSTER_PREBUILD_DIR
status=0
run_embedded_tests "$scope" 2 || status=$?
if (( status != 0 )); then printf 'authoritative status=%s\n' "$status"; cat "$UT_REPORT" "$UT_STDERR"; fi
if [[ "$MODE" == test-failure ]]; then
 [[ "$status" != 0 ]] || exit 90
else
 [[ "$status" == 0 ]] || exit 91
fi
[[ -d "$CASE_DIR/authoritative" && "$(grep -c '^authoritative$' "$UT_REPORT")" == 1 ]] || exit 92
if [[ "$MODE" != off && "$MODE" != report-open ]]; then
 for p in a b c; do
  [[ -d "$CASE_DIR/compiled-$p" ]] || exit 93
  [[ "$(grep -c "^build-$p$" "$UT_STDERR")" == 1 ]] || exit 94
 done
fi
if [[ "$MODE" == success || "$MODE" == build-failure ]]; then
 for p in a b c; do
  grep -q "event=start stage=embedded-prebuild label=example/$p status= detail=package_index=" "$UT_CHECKPOINT" || exit 97
  grep -q "event=pid-start stage=embedded-prebuild label=example/$p status= detail=package_index=" "$UT_CHECKPOINT" || exit 98
  grep -q "event=finish stage=embedded-prebuild label=example/$p status=" "$UT_CHECKPOINT" || exit 99
 done
 if [[ "$MODE" == success ]]; then
  grep -q 'event=finish stage=embedded-prebuild label=example/a status=0 detail=package_index=' "$UT_CHECKPOINT" || exit 100
  grep -q 'event=finish stage=embedded-prebuild label=example/b status=0 detail=package_index=' "$UT_CHECKPOINT" || exit 101
  grep -q 'event=finish stage=embedded-prebuild label=example/c status=0 detail=package_index=' "$UT_CHECKPOINT" || exit 102
 else
  grep -q 'event=finish stage=embedded-prebuild label=example/b status=7 detail=package_index=' "$UT_CHECKPOINT" || exit 103
  grep -q 'event=join-finish stage=embedded-prebuild label=compile embedded-cluster packages status=1 detail=compile_only=true' "$UT_CHECKPOINT" || exit 104
 fi
 join_start=$(grep -n 'event=join-start stage=embedded-prebuild label=compile embedded-cluster packages' "$UT_CHECKPOINT" | cut -d: -f1)
 join_finish=$(grep -n 'event=join-finish stage=embedded-prebuild label=compile embedded-cluster packages' "$UT_CHECKPOINT" | cut -d: -f1)
 [[ -n "$join_start" && -n "$join_finish" && "$join_start" -lt "$join_finish" ]] || exit 105
fi
[[ -z "$CLUSTER_PREBUILD_JOB_PID$CURRENT_UT_PID$CLUSTER_PREBUILD_REPORT$CLUSTER_PREBUILD_DIR" ]] || exit 95
[[ -z "$artifact_dir" || ! -d "$artifact_dir" ]] || exit 96
if [[ "$MODE" == report-open ]]; then
 [[ -f "$CASE_DIR/stopped-a" ]] || exit 97
 ! kill -0 "$(<"$CASE_DIR/pid-a")" 2>/dev/null || exit 98
fi
`
			var transform func(string) string
			if mode == "report-open" {
				transform = func(text string) string {
					const anchor = "if ! : > \"${package_report}\"; then"
					if strings.Count(text, anchor) != 1 {
						t.Fatal("missing embedded output admission")
					}
					return strings.Replace(text, anchor, "if ! ut_test_open_embedded_report > \"${package_report}\"; then", 1)
				}
			}
			out, err := scheduleHarnessWithMockTransform(t, script, embeddedGoMock, transform, "MODE="+mode, "UT_PREBUILD_EMBEDDED=0", "UT_HARD_TIMEOUT=")
			if err != nil {
				t.Fatalf("embedded %s: %v\n%s", mode, err, out)
			}
		})
	}
}

func TestEmbeddedPrebuildCancellation(t *testing.T) {
	for _, phase := range []string{"build", "launch"} {
		t.Run(phase, func(t *testing.T) {
			script := embeddedSetup + `
cleanup_check() {
 status=$?
 for p in a b; do
  [[ -f "$CASE_DIR/stopped-$p" ]] || status=90
  if kill -0 "$(<"$CASE_DIR/pid-$p")" 2>/dev/null; then status=91; fi
  [[ "$(grep -c "^build-$p$" "$UT_STDERR")" == 1 ]] || status=92
 done
 [[ ! -d "$artifact_dir" ]] || status=93
 [[ -z "$CLUSTER_PREBUILD_JOB_PID$CURRENT_UT_PID" ]] || status=94
 [[ ! -d "$CASE_DIR/compiled-c" && ! -d "$CASE_DIR/authoritative" ]] || status=95
 printf 'CANCELLED %s\n' "$status"
 exit "$status"
}
trap cleanup_check EXIT
function ut_test_prebuild_spawned() {
 artifact_dir=$CLUSTER_PREBUILD_DIR
 read -r _ <&8
 read -r _ <&8
 kill -TERM "$$"
}
start_embedded_prebuild "$scope" 2
artifact_dir=$CLUSTER_PREBUILD_DIR
read -r _ <&8
read -r _ <&8
kill -TERM "$$"
`
			var transform func(string) string
			if phase == "launch" {
				transform = func(text string) string {
					const anchor = "    CLUSTER_PREBUILD_JOB_PID=$!\n"
					if strings.Count(text, anchor) != 1 {
						t.Fatal("missing unique prebuild publication")
					}
					return strings.Replace(text, anchor, "    ut_test_prebuild_spawned\n"+anchor, 1)
				}
			}
			out, err := scheduleHarnessWithMockTransform(t, script, embeddedGoMock, transform, "MODE=build-cancel", "UT_PREBUILD_EMBEDDED=0", "UT_HARD_TIMEOUT=")
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 143 || !strings.Contains(string(out), "CANCELLED 143") {
				t.Fatalf("embedded cancellation %s: %v\n%s", phase, err, out)
			}
		})
	}

	// Exercise the parent join path with the default helper ownership still
	// active. The injected hook delivers TERM after join-start but before the
	// blocking wait, so cancellation must stop both the issues owner and every
	// compiler child without falling through to authoritative execution.
	joinCancelScript := embeddedSetup + `
cleanup_check() {
 status=$?
 for p in a b; do
  [[ -f "$CASE_DIR/stopped-$p" ]] || status=90
  if kill -0 "$(<"$CASE_DIR/pid-$p")" 2>/dev/null; then status=91; fi
  [[ "$(grep -c "^build-$p$" "$UT_STDERR")" == 1 ]] || status=92
 done
 [[ -f "$CASE_DIR/issues-stopped" ]] || status=96
 if kill -0 "$issues_pid" 2>/dev/null; then status=97; fi
 [[ -z "$CLUSTER_PREBUILD_JOB_PID$CURRENT_UT_PID" ]] || status=93
 [[ ! -d "$artifact_dir" ]] || status=94
 [[ ! -d "$CASE_DIR/authoritative" ]] || status=95
 printf 'JOIN_CANCELLED %s\n' "$status"
 exit "$status"
}
trap cleanup_check EXIT
UT_HELPER_TERM_GRACE_TICKS=4
export RUNNER_PID=$$
start_embedded_prebuild "$scope" 2
artifact_dir=$CLUSTER_PREBUILD_DIR
read -r _ <&8
read -r _ <&8
start_ut_command serial issues bash -c '
 trap '\''touch "$CASE_DIR/issues-stopped"; exit 143'\'' TERM
 touch "$CASE_DIR/issues-active"
 while :; do sleep 0.01; done
'
issues_pid=$CURRENT_UT_PID
while [[ ! -e "$CASE_DIR/issues-active" ]]; do sleep 0.01; done
run_embedded_tests "$scope" 2
`
	joinCancelTransform := func(text string) string {
		const anchor = `    wait "${CLUSTER_PREBUILD_JOB_PID}" || prebuild_status=$?
`
		if strings.Count(text, anchor) != 1 {
			t.Fatalf("missing unique embedded prebuild join wait")
		}
		return strings.Replace(text, anchor, "    : > \"$CASE_DIR/join-waiting\"\n"+anchor, 1)
	}
	out, err := scheduleHarnessWithMockTransform(t, joinCancelScript, embeddedGoMock, joinCancelTransform, "MODE=build-cancel", "UT_PREBUILD_EMBEDDED=0", "UT_HARD_TIMEOUT=")
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 143 || !strings.Contains(string(out), "JOIN_CANCELLED 143") {
		t.Fatalf("embedded prebuild join cancellation: %v\n%s", err, out)
	}
}
