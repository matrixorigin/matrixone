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
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

const embeddedSetup = `source ./run_ut.sh UT
function logger() { :; }
trap handle_ut_termination TERM
[[ "$UT_PREBUILD_EMBEDDED" == 1 && "$UT_HARD_TIMEOUT" == 120m ]] || exit 80
# These fixtures require compilation; exercise the low-disk fallback separately.
export UT_PREBUILD_MIN_FREE_KB=1
scope=$'example/a\nexample/b\nexample/c'
mkfifo "$CASE_DIR/release-a" "$CASE_DIR/ready" "$CASE_DIR/hold"
exec 7<>"$CASE_DIR/release-a"
exec 8<>"$CASE_DIR/ready"
exec 9<>"$CASE_DIR/hold"
export UT_TIMEOUT=17 TAGS=matrixone_test
`

const embeddedGoMock = `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
if [[ "$1" == list ]]; then
 package=${!#}; leaf=${package##*/}
 [[ "$leaf" != embed ]] || leaf=a
 [[ "$MODE" != metadata-failure || "$leaf" != b ]] || exit 7
 mkdir -p "$CASE_DIR/package-$leaf"
 printf '%s\t%s\n' "$CASE_DIR/package-$leaf" "$package"
 exit 0
fi
if [[ "$1" == tool && "$2" == test2json ]]; then
 package=""
 while (( $# > 0 )); do
  if [[ "$1" == -p ]]; then package=$2; break; fi
  shift
 done
 [[ -n "$package" ]] || exit 106
 leaf=${package##*/}
 [[ "$leaf" != embed ]] || leaf=a
 [[ "$PWD" -ef "$CASE_DIR/package-$leaf" ]] || exit 107
 mkdir "$CASE_DIR/executed-$leaf" || exit 108
 if [[ "$MODE" == pool ]]; then
  [[ "$MO_TEST_CLUSTER_ADMISSION_POOL_SIZE" == 2 ]] || exit 109
  touch "$CASE_DIR/active-$leaf"
  printf 'pool=%s package=%s\n' "$MO_TEST_CLUSTER_ADMISSION_POOL_SIZE" "$package" >> "$CASE_DIR/pool-events"
  printf '%s %s\n' "$leaf" "$$" >&8
  case "$leaf" in
   a) read -r _ <&10 ;;
   b) read -r _ <&11 ;;
   c) read -r _ <&12 ;;
  esac
  rm -f "$CASE_DIR/active-$leaf"
 fi
 if [[ "$MODE" == execute-cancel && "$leaf" == a ]]; then
  trap 'touch "$CASE_DIR/stopped-execute-a"; exit 143' TERM
  printf '%s\n' "$$" > "$CASE_DIR/pid-execute-a"
  printf 'ready\n' >&8
  while :; do read -r -t 0.01 _ <&9 || true; done
 fi
 if [[ "$MODE" == execute-timeout && "$leaf" == a ]]; then
  ps -o pgid= -p $$ | tr -d ' ' > "$CASE_DIR/pgid-execute-timeout-a"
  trap '' TERM
  while :; do read -r -t 0.01 _ <&9 || true; done
 fi
 if [[ "$MODE" == execute-timeout-zero && "$leaf" == a ]]; then
  ps -o pgid= -p $$ | tr -d ' ' > "$CASE_DIR/pgid-execute-timeout-zero-a"
  trap 'printf "{\"Action\":\"pass\",\"Package\":\"%s\"}\\n" "$package"; exit 0' TERM
  while :; do read -r -t 0.01 _ <&9 || true; done
 fi
 if [[ "$MODE" == execute-cancel-resistant && "$leaf" == a ]]; then
  ps -o pgid= -p $$ | tr -d ' ' > "$CASE_DIR/pgid-execute-resistant-a"
  printf 'ready\n' >&8
  trap '' TERM
  while :; do read -r -t 0.01 _ <&9 || true; done
 fi
 if [[ "$MODE" == test-failure && "$leaf" == b ]]; then
  printf '{"Action":"fail","Package":"%s"}\n' "$package"
  exit 7
 fi
 printf '{"Action":"pass","Package":"%s"}\n' "$package"
 exit 0
fi
[[ "$1" == test ]] || exit 81
[[ " $* " == *' -race '* && " $* " == *' -short '* && " $* " == *' -tags matrixone_test '* && " $* " == *' -timeout 17m '* && " $* " == *' -mod=readonly '* && " $* " == *' -vet=off '* ]] || exit 82
if [[ " $* " == *' -c '* ]]; then
 [[ " $* " == *' -ldflags=-w '* ]] || exit 89
 package=${!#}; leaf=${package##*/}
 [[ "$leaf" != embed ]] || leaf=a
 [[ " $* " == *' -p 1 '* ]] || exit 83
 mkdir "$CASE_DIR/compiled-$leaf" || exit 84
 while [[ "$1" != -o ]]; do shift; done
 output=$2
 printf 'build-%s\n' "$leaf"
 if [[ "$MODE" == build-cancel ]]; then
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
 printf '#!/bin/bash\nexit 0\n' > "$output"
 chmod +x "$output"
 exit 0
fi
[[ " $* " != *' -ldflags=-w '* ]] || exit 88
[[ " $* " == *' -p 1 '* && " $* " == *' -json '* && " $* " == *' -v '* && " $* " == *' example/a example/b example/c '* ]] || exit 85
[[ "$PWD" -ef "$CASE_DIR" ]] || exit 86
mkdir "$CASE_DIR/authoritative" || exit 87
printf 'authoritative\n'
[[ "$MODE" != test-failure ]] || exit 7
`

func TestEmbeddedPrebuildAuthoritativeExecution(t *testing.T) {
	defaultScript := `unset UT_PREBUILD_EMBEDDED
source ./run_ut.sh UT
	[[ "$UT_PREBUILD_EMBEDDED" == 1 ]] || exit 80
`
	if out, err := scheduleHarness(t, defaultScript); err != nil {
		t.Fatalf("embedded prebuild default-on control: %v\n%s", err, out)
	}

	for _, mode := range []string{"success", "reclaim", "build-failure", "no-binary", "metadata-failure", "low-disk", "off", "test-failure", "execution-undrained"} {
		t.Run(mode, func(t *testing.T) {
			script := embeddedSetup + `
if [[ "$MODE" == low-disk ]]; then
 # Force the production disk guard independently of the host filesystem.
 function df() { printf 'Filesystem 1024-blocks Used Available Capacity Mounted\nmock 1 1 0 100%% /\n'; }
fi
if [[ "$MODE" == execution-undrained ]]; then
 # Replace only the child boundary: exercise the real command join and consumer.
 function run_prebuilt_embedded_tests() {
  printf 'retained\n' > "$PREBUILT_RACE_REPORT.00"
  return 125
 }
fi
if [[ "$MODE" != off ]]; then start_embedded_prebuild "$scope" 2; fi
artifact_dir=$CLUSTER_PREBUILD_DIR
status=0
run_embedded_tests "$scope" || status=$?
if [[ "$MODE" == execution-undrained ]]; then
 [[ "$status" == 125 && -z "$CURRENT_UT_PID$CLUSTER_PREBUILD_JOB_PID" ]] || exit 110
 [[ -d "$artifact_dir" && -f "$PREBUILT_RACE_REPORT.00" && ! -d "$CASE_DIR/authoritative" ]] || exit 111
 run_embedded_tests "$scope"; [[ "$?" == 125 ]] || exit 112
 trap 'status=$?; [[ -d "$artifact_dir" && -f "$PREBUILT_RACE_REPORT.00" && ! -d "$CASE_DIR/authoritative" ]] || status=113; exit "$status"' EXIT
 kill -TERM "$$"
 exit 114
fi
if (( status != 0 )); then printf 'authoritative status=%s\n' "$status"; cat "$UT_REPORT" "$UT_STDERR"; fi
if [[ "$MODE" == test-failure ]]; then
 [[ "$status" != 0 ]] || exit 90
else
 [[ "$status" == 0 ]] || exit 91
fi
if [[ "$MODE" == off || "$MODE" == low-disk || "$MODE" == build-failure || "$MODE" == no-binary || "$MODE" == metadata-failure ]]; then
 [[ -d "$CASE_DIR/authoritative" && "$(grep -c '^authoritative$' "$UT_REPORT")" == 1 ]] || exit 92
 for p in a b c; do [[ ! -d "$CASE_DIR/executed-$p" ]] || exit 106; done
else
 [[ ! -d "$CASE_DIR/authoritative" ]] || exit 107
 for p in a b c; do [[ -d "$CASE_DIR/executed-$p" ]] || exit 108; done
fi
if [[ "$MODE" == low-disk ]]; then
 for p in a b c; do [[ ! -d "$CASE_DIR/compiled-$p" ]] || exit 109; done
elif [[ "$MODE" != off ]]; then
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
`
			out, err := scheduleHarnessWithMockTransform(t, script, embeddedGoMock, nil, "MODE="+mode, "UT_PREBUILD_EMBEDDED=1", "UT_HARD_TIMEOUT=")
			if mode == "execution-undrained" {
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.ExitCode() != 125 {
					t.Fatalf("undrained embedded execution: %v\n%s", err, out)
				}
			} else if err != nil {
				t.Fatalf("embedded %s: %v\n%s", mode, err, out)
			}
		})
	}
}

func TestEmbeddedPrebuiltExecutionUsesBoundedProcessPool(t *testing.T) {
	script := embeddedSetup + `
start_embedded_prebuild "$scope" 1
artifact_dir=$CLUSTER_PREBUILD_DIR
mkfifo "$CASE_DIR/pool-release-a" "$CASE_DIR/pool-release-b" "$CASE_DIR/pool-release-c"
exec 10<>"$CASE_DIR/pool-release-a"
exec 11<>"$CASE_DIR/pool-release-b"
exec 12<>"$CASE_DIR/pool-release-c"
release_pool_children() {
 printf 'release\n' >&10
 printf 'release\n' >&11
 printf 'release\n' >&12
}
pool_driver_pid=""
cleanup_pool_driver() {
 release_pool_children
 if [[ -n "$pool_driver_pid" ]]; then
  kill -TERM "$pool_driver_pid" 2>/dev/null || true
  wait "$pool_driver_pid" 2>/dev/null || true
 fi
}
trap cleanup_pool_driver EXIT
(
 trap release_pool_children EXIT
 trap 'exit 143' TERM INT
 read -r -t 3 first first_pid <&8 || exit 100
 read -r -t 3 second second_pid <&8 || exit 101
 [[ "$first $second" == 'a b' || "$first $second" == 'b a' ]] || exit 102
 [[ "$first_pid" != "$second_pid" ]] || exit 103
 kill -0 "$first_pid" && kill -0 "$second_pid" || exit 104
 [[ -e "$CASE_DIR/active-a" && -e "$CASE_DIR/active-b" && ! -e "$CASE_DIR/executed-c" ]] || exit 105
 printf 'release\n' >&10
 read -r -t 3 third third_pid <&8 || exit 106
 [[ "$third" == c && "$third_pid" != "$first_pid" && "$third_pid" != "$second_pid" ]] || exit 107
 kill -0 "$third_pid" || exit 108
 [[ ! -e "$CASE_DIR/active-a" && -e "$CASE_DIR/active-b" && -e "$CASE_DIR/active-c" ]] || exit 109
) &
pool_driver_pid=$!
status=0
UT_EMBEDDED_PACKAGE_PARALLEL=2 run_embedded_tests "$scope" || status=$?
driver_status=0
wait "$pool_driver_pid" || driver_status=$?
pool_driver_pid=""
[[ "$status" == 0 && "$driver_status" == 0 ]] || exit 90
[[ "$(grep -c '^pool=2 ' "$CASE_DIR/pool-events")" == 3 ]] || exit 91
# Indexed execution checkpoints exclude the outer command and compile events.
awk '
$5 == "stage=embedded" && $6 ~ /^label=example\/[abc]$/ && /detail=package_index=[0-2] .*prebuilt=true/ {
 package=substr($6,7)
 if ($4 == "event=start") {
  if (started[package]++) bad=1
  starts++; active++
  if (active > peak) peak=active
  start_at[package]=NR
 } else if ($4 == "event=finish") {
  if (!started[package] || finished[package]++ || $7 != "status=0") bad=1
  if (!finishes && starts != 2) bad=1
  finishes++; active--
  finish_at[package]=NR
 }
 if (active < 0 || active > 2) bad=1
}
END {
 if (bad || starts != 3 || finishes != 3 || active != 0 || peak != 2 ||
     !(finish_at["example/a"] < start_at["example/c"] && start_at["example/c"] < finish_at["example/b"])) exit 1
}' "$UT_CHECKPOINT" || { cat "$UT_CHECKPOINT" >&2; exit 92; }
[[ -z "$CLUSTER_PREBUILD_JOB_PID$CURRENT_UT_PID" ]] || exit 93
[[ ! -d "$artifact_dir" ]] || exit 94
for leaf in a b c; do [[ ! -e "$CASE_DIR/active-$leaf" ]] || exit 95; done
cat "$UT_REPORT"
`
	out, err := scheduleHarnessWithMockTransform(t, script, embeddedGoMock, nil,
		"MODE=pool", "UT_PREBUILD_EMBEDDED=1", "UT_HARD_TIMEOUT=")
	if err != nil {
		t.Fatalf("embedded bounded process pool: %v\n%s", err, out)
	}
	assertScheduleJSONReport(t, out, map[string]string{"example/a": "pass", "example/b": "pass", "example/c": "pass"})
}

// Exercise preparation and consumption as well as admission. Compilation
// indices intentionally differ from execution indices after pkg/embed moves.
func TestEmbeddedPrebuiltAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, parallel, mode string
	}{
		{"serial", "1", "success"},
		{"pool", "2", "success"},
		{"heavy-failure", "2", "heavy-failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			script := embeddedSetup + `
scope=$'github.com/matrixorigin/matrixone/pkg/bootstrap\ngithub.com/matrixorigin/matrixone/pkg/embed\ngithub.com/matrixorigin/matrixone/pkg/tests/arrowload\ngithub.com/matrixorigin/matrixone/pkg/tests/sqlintegration\ngithub.com/matrixorigin/matrixone/pkg/tests/dml'
mkfifo "$CASE_DIR/release-heavy" "$CASE_DIR/release-light"
exec 10<>"$CASE_DIR/release-heavy" 11<>"$CASE_DIR/release-light"
release_children() { printf 'release\nrelease\nrelease\n' >&10; printf 'release\nrelease\n' >&11; }
driver_pid=""
cleanup_driver() {
 release_children
 if [[ -n "$driver_pid" ]]; then kill -TERM "$driver_pid" 2>/dev/null || true; wait "$driver_pid" 2>/dev/null || true; fi
}
trap cleanup_driver EXIT
start_embedded_prebuild "$scope" 1
artifact_dir=$CLUSTER_PREBUILD_DIR
(
 trap release_children EXIT
 trap 'exit 143' TERM INT
 for heavy in embed bootstrap; do
  read -r -t 5 leaf pid <&8 || exit 100
  [[ "$leaf" == "$heavy" ]] || exit 101
  kill -0 "$pid" || exit 102
  printf 'release\n' >&10
 done
 if [[ "$UT_EMBEDDED_PACKAGE_PARALLEL" == 2 ]]; then
  read -r -t 5 first first_pid <&8 || exit 103
  read -r -t 5 second second_pid <&8 || exit 104
  [[ "$first $second" == 'arrowload dml' || "$first $second" == 'dml arrowload' ]] || exit 105
  [[ "$first_pid" != "$second_pid" ]] || exit 106
  kill -0 "$first_pid" && kill -0 "$second_pid" || exit 107
  printf 'release\nrelease\n' >&11
  read -r -t 5 leaf pid <&8 || exit 108
  [[ "$leaf" == sqlintegration ]] || exit 109
  printf 'release\n' >&10
 else
  for next in arrowload sqlintegration dml; do
   read -r -t 5 leaf pid <&8 || exit 110
   [[ "$leaf" == "$next" ]] || exit 111
   if [[ "$leaf" == sqlintegration ]]; then printf 'release\n' >&10; else printf 'release\n' >&11; fi
  done
 fi
) &
driver_pid=$!
status=0
run_embedded_tests "$scope" || status=$?
driver_status=0
wait "$driver_pid" || driver_status=$?
driver_pid=""
[[ "$driver_status" == 0 ]] || exit "$driver_status"
if [[ "$MODE" == heavy-failure ]]; then [[ "$status" != 0 ]] || exit 112; else [[ "$status" == 0 ]] || exit 113; fi
[[ -z "$CLUSTER_PREBUILD_JOB_PID$CURRENT_UT_PID" && ! -d "$artifact_dir" ]] || exit 114
# Reconstruction proves every exclusion interval, not a sampled absence.
awk -v limit="$UT_EMBEDDED_PACKAGE_PARALLEL" '
$5 == "stage=embedded" && /detail=package_index=[0-4] .*prebuilt=true/ {
 package=substr($6,7); heavy=(package !~ /\/(arrowload|dml)$/)
 if ($4 == "event=start") {
  if (started[package]++ || (heavy && active) || heavy_active) bad=1
  active++; if (active>peak) peak=active
  if (heavy) heavy_active++
 } else if ($4 == "event=finish") {
  if (!started[package] || finished[package]++) bad=1
  active--; if (heavy) heavy_active--
 }
 if (active<0 || active>limit) bad=1
}
END { if (bad || length(started)!=5 || length(finished)!=5 || active || heavy_active || peak!=limit) exit 1 }
' "$UT_CHECKPOINT" || { cat "$UT_CHECKPOINT" >&2; exit 115; }
cat "$UT_REPORT"
`
			mock := `#!/bin/bash
if [[ "$1" == version ]]; then exit 0; fi
if [[ "$1" == list ]]; then
 package=${!#}; leaf=${package##*/}
 mkdir -p "$CASE_DIR/package-$leaf"
 printf '%s\t%s\n' "$CASE_DIR/package-$leaf" "$package"
 exit 0
fi
if [[ "$1" == test ]]; then
 package=${!#}; leaf=${package##*/}
 [[ " $* " == *' -c '* ]] || exit 120
 while [[ "$1" != -o ]]; do shift; done
 output=$2
 printf '%s\n' "$output" > "$CASE_DIR/binary-$leaf"
 printf '#!/bin/sh\nprintf "%%s\\n" "%s"\n' "$package" > "$output"
 chmod +x "$output"
 exit 0
fi
if [[ "$1" == tool && "$2" == test2json ]]; then
 package=$5; binary=$6; leaf=${package##*/}
 [[ "$PWD" -ef "$CASE_DIR/package-$leaf" ]] || exit 121
 [[ "$binary" == "$(<"$CASE_DIR/binary-$leaf")" && "$("$binary")" == "$package" ]] || exit 122
 [[ " $* " == *' -test.run=.* '* || "${!#}" == '-test.run=.*' ]] || exit 123
 mkdir "$CASE_DIR/executed-$leaf" || exit 124
 printf '%s %s\n' "$leaf" "$$" >&8
 case "$leaf" in arrowload|dml) read -r _ <&11 ;; *) read -r _ <&10 ;; esac
 if [[ "$MODE" == heavy-failure && "$leaf" == embed ]]; then
  printf '{"Action":"fail","Package":"%s"}\n' "$package"
  exit 7
 fi
 printf '{"Action":"pass","Package":"%s"}\n' "$package"
 exit 0
fi
exit 125
`
			out, err := scheduleHarnessWithMock(t, script, mock,
				"MODE="+tc.mode, "UT_EMBEDDED_PACKAGE_PARALLEL="+tc.parallel)
			if err != nil {
				t.Fatalf("prebuilt entry admission %s: %v\n%s", tc.name, err, out)
			}
			expected := map[string]string{
				"github.com/matrixorigin/matrixone/pkg/bootstrap":            "pass",
				"github.com/matrixorigin/matrixone/pkg/embed":                "pass",
				"github.com/matrixorigin/matrixone/pkg/tests/arrowload":      "pass",
				"github.com/matrixorigin/matrixone/pkg/tests/sqlintegration": "pass",
				"github.com/matrixorigin/matrixone/pkg/tests/dml":            "pass",
			}
			if tc.mode == "heavy-failure" {
				expected["github.com/matrixorigin/matrixone/pkg/embed"] = "fail"
			}
			assertScheduleJSONReport(t, out, expected)
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
[[ -n "$CLUSTER_PREBUILD_JOB_PID" && -d "$artifact_dir" ]] || exit 96
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
			out, err := scheduleHarnessWithMockTransform(t, script, embeddedGoMock, transform, "MODE=build-cancel", "UT_PREBUILD_EMBEDDED=1", "UT_HARD_TIMEOUT=")
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
[[ -n "$CLUSTER_PREBUILD_JOB_PID" && -d "$artifact_dir" ]] || exit 96
read -r _ <&8
read -r _ <&8
start_ut_command serial issues bash -c '
 trap '\''touch "$CASE_DIR/issues-stopped"; exit 143'\'' TERM
 touch "$CASE_DIR/issues-active"
 while :; do sleep 0.01; done
'
issues_pid=$CURRENT_UT_PID
while [[ ! -e "$CASE_DIR/issues-active" ]]; do sleep 0.01; done
run_embedded_tests "$scope"
`
	joinCancelTransform := func(text string) string {
		const anchor = `    join_ut_owner CLUSTER_PREBUILD_JOB_PID CLUSTER_PREBUILD_DRAIN_FAILED || prebuild_status=$?
`
		if strings.Count(text, anchor) != 1 {
			t.Fatalf("missing unique embedded prebuild join wait")
		}
		return strings.Replace(text, anchor, "    : > \"$CASE_DIR/join-waiting\"\n"+anchor, 1)
	}
	out, err := scheduleHarnessWithMockTransform(t, joinCancelScript, embeddedGoMock, joinCancelTransform, "MODE=build-cancel", "UT_PREBUILD_EMBEDDED=1", "UT_HARD_TIMEOUT=")
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 143 || !strings.Contains(string(out), "JOIN_CANCELLED 143") {
		t.Fatalf("embedded prebuild join cancellation: %v\n%s", err, out)
	}
}

func TestEmbeddedPrebuiltExecutionCancellation(t *testing.T) {
	for _, phase := range []string{"running", "active-publication", "watchdog-publication", "cleanup"} {
		t.Run(phase, func(t *testing.T) {
			script := embeddedSetup + `
scope=$'example/b\ngithub.com/matrixorigin/matrixone/pkg/embed\nexample/c'
cleanup_check() {
 status=$?
 [[ -f "$CASE_DIR/stopped-execute-a" ]] || status=90
 if kill -0 "$(<"$CASE_DIR/pid-execute-a")" 2>/dev/null; then status=91; fi
 [[ ! -d "$artifact_dir" ]] || status=92
 [[ -z "$CLUSTER_PREBUILD_JOB_PID$CURRENT_UT_PID" ]] || status=93
 [[ ! -d "$CASE_DIR/executed-b" && ! -d "$CASE_DIR/executed-c" ]] || status=94
 printf 'EXECUTE_CANCELLED %s\n' "$status"
 exit "$status"
}
function ut_test_execution_spawned() {
 read -r _ <&8
 kill -TERM $$
}
trap cleanup_check EXIT
if [[ "$PHASE" == cleanup ]]; then
 mkfifo "$CASE_DIR/cleanup-ready" "$CASE_DIR/cleanup-release"
 exec 11<>"$CASE_DIR/cleanup-ready" 12<>"$CASE_DIR/cleanup-release"
 eval "$(declare -f checkpoint_ut_event | sed '1s/checkpoint_ut_event/original_checkpoint_ut_event/')"
 function checkpoint_ut_event() {
  if [[ "$1" == cancel ]]; then printf 'ready\n' >&11; read -r _ <&12; fi
  original_checkpoint_ut_event "$@"
 }
 (read -r _ <&8; kill -TERM $$; read -r _ <&11; kill -TERM $$; printf 'release\n' >&12) &
fi
start_embedded_prebuild "$scope" 1
artifact_dir=$CLUSTER_PREBUILD_DIR
if [[ "$PHASE" == running ]]; then (read -r _ <&8; kill -TERM $$) & fi
run_embedded_tests "$scope" 2
`
			var transform func(string) string
			switch phase {
			case "active-publication":
				transform = func(text string) string {
					const anchor = "            test_pids[index]=$!\n"
					if strings.Count(text, anchor) != 1 {
						t.Fatal("missing active pid publication")
					}
					return strings.Replace(text, anchor, "        ut_test_execution_spawned\n"+anchor, 1)
				}
			case "watchdog-publication":
				transform = func(text string) string {
					const anchor = "            watchdog_pids[index]=$!\n"
					if strings.Count(text, anchor) != 1 {
						t.Fatal("missing watchdog pid publication")
					}
					return strings.Replace(text, anchor, "        ut_test_execution_spawned\n"+anchor, 1)
				}
			}
			out, err := scheduleHarnessWithMockTransform(t, script, embeddedGoMock, transform,
				"MODE=execute-cancel", "PHASE="+phase, "UT_PREBUILD_EMBEDDED=1", "UT_HARD_TIMEOUT=")
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 143 || !strings.Contains(string(out), "EXECUTE_CANCELLED 143") {
				t.Fatalf("embedded execution cancellation %s: %v\n%s", phase, err, out)
			}
		})
	}
}

func TestEmbeddedPrebuiltExecutionHardTimeout(t *testing.T) {
	script := embeddedSetup + `
function logger() { printf '%s\n' "$*"; }
start_embedded_prebuild "$scope" 1
artifact_dir=$CLUSTER_PREBUILD_DIR
status=0
run_embedded_tests "$scope" 2 || status=$?
[[ "$status" != 0 ]] || exit 90
for p in a b c; do [[ -d "$CASE_DIR/executed-$p" ]] || exit 91; done
! kill -0 -- "-$(<"$CASE_DIR/pgid-execute-timeout-a")" 2>/dev/null || exit 95
[[ -z "$CLUSTER_PREBUILD_JOB_PID$CURRENT_UT_PID$CLUSTER_PREBUILD_REPORT$CLUSTER_PREBUILD_DIR" ]] || exit 92
[[ ! -d "$artifact_dir" ]] || exit 93
grep -q 'prebuilt embedded package example/a failed' "$UT_STDERR" || exit 94
`
	out, err := scheduleHarnessWithMockTransform(t, script, embeddedGoMock, nil,
		"MODE=execute-timeout", "UT_PREBUILD_EMBEDDED=1", "UT_EMBEDDED_HARD_TIMEOUT_SECONDS=1", "UT_HARD_TIMEOUT=")
	if err != nil {
		t.Fatalf("embedded execution hard timeout: %v\n%s", err, out)
	}
}

func TestEmbeddedPrebuiltTimeoutAddsFailureEvent(t *testing.T) {
	script := embeddedSetup + `
start_embedded_prebuild "$scope" 1
status=0
run_embedded_tests "$scope" 2 || status=$?
[[ "$status" != 0 ]] || exit 90
grep -q '"Action":"fail"' "$UT_REPORT" || exit 91
grep -q 'UT runner hard timeout' "$UT_REPORT" || exit 92
`
	out, err := scheduleHarnessWithMockTransform(t, script, embeddedGoMock, nil,
		"MODE=execute-timeout-zero", "UT_PREBUILD_EMBEDDED=1", "UT_EMBEDDED_HARD_TIMEOUT_SECONDS=1", "UT_HARD_TIMEOUT=")
	if err != nil {
		t.Fatalf("embedded timeout JSON failure event: %v\n%s", err, out)
	}
}

func TestEmbeddedPrebuiltOuterCancellationKillsResistantGroup(t *testing.T) {
	for _, parallel := range []string{"1", "2"} {
		for _, drain := range []string{"complete", "failed"} {
			t.Run("parallel="+parallel+"/drain="+drain, func(t *testing.T) {
				script := embeddedSetup + `
if [[ "$DRAIN" == failed ]]; then
 original_stop=$(declare -f terminate_ut_process_groups)
 eval "${original_stop/terminate_ut_process_groups/real_terminate_ut_process_groups}"
 function terminate_ut_process_groups() {
  real_terminate_ut_process_groups "$@" || return $?
  # Stop the fixture safely, then model a descendant whose drainage cannot be proven.
  return 125
 }
fi
cleanup_check() {
 status=$?
 if [[ "$DRAIN" == failed ]]; then
  ! kill -0 "$(<"$CASE_DIR/pid-execute-a")" 2>/dev/null || status=90
  [[ "$status" == 125 ]] || status=93
  [[ -d "$artifact_dir" && -f "$PREBUILT_RACE_REPORT.00" ]] || status=94
 else
  ! kill -0 -- "-$(<"$CASE_DIR/pgid-execute-resistant-a")" 2>/dev/null || status=90
  [[ ! -d "$artifact_dir" ]] || status=91
 fi
 [[ -z "$CLUSTER_PREBUILD_JOB_PID$CURRENT_UT_PID" ]] || status=92
 printf 'RESISTANT_CANCELLED %s\n' "$status"
 exit "$status"
}
trap cleanup_check EXIT
start_embedded_prebuild "$scope" 1
artifact_dir=$CLUSTER_PREBUILD_DIR
(read -r _ <&8; kill -TERM $$) &
run_embedded_tests "$scope" 2
`
				mode := "execute-cancel-resistant"
				if drain == "failed" {
					mode = "execute-cancel"
				}
				out, err := scheduleHarnessWithMockTransform(t, script, embeddedGoMock, nil,
					"MODE="+mode, "UT_PREBUILD_EMBEDDED=1", "UT_HARD_TIMEOUT=",
					"UT_EMBEDDED_PACKAGE_PARALLEL="+parallel, "DRAIN="+drain)
				want := 143
				if drain == "failed" {
					want = 125
				}
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.ExitCode() != want || !strings.Contains(string(out), fmt.Sprintf("RESISTANT_CANCELLED %d", want)) {
					t.Fatalf("embedded resistant outer cancellation: %v\n%s", err, out)
				}
			})
		}
	}
}

func TestEmbeddedPrebuiltFailureKeepsReportJSONOnly(t *testing.T) {
	for _, parallel := range []string{"1", "2"} {
		t.Run("parallel="+parallel, func(t *testing.T) {
			script := embeddedSetup + `
function logger() { printf '%s\n' "$*"; }
start_embedded_prebuild "$scope" 1
status=0
run_embedded_tests "$scope" 2 > "$CASE_DIR/outer-log" || status=$?
[[ "$status" != 0 ]] || exit 90
cat "$UT_REPORT"

grep -q 'prebuilt embedded package example/b failed' "$UT_STDERR" || exit 92
`
			out, err := scheduleHarnessWithMockTransform(t, script, embeddedGoMock, nil,
				"MODE=test-failure", "UT_PREBUILD_EMBEDDED=1", "UT_HARD_TIMEOUT=", "UT_EMBEDDED_PACKAGE_PARALLEL="+parallel)
			if err != nil {
				t.Fatalf("embedded failure JSON report: %v\n%s", err, out)
			}
			assertScheduleJSONReport(t, out, map[string]string{"example/a": "pass", "example/b": "fail", "example/c": "pass"})
		})
	}
}

// Parse the actual runner output, rather than accepting a line that merely
// starts with a brace. Every fixture package must retain its terminal event.
func assertScheduleJSONReport(t *testing.T, report []byte, expected map[string]string) {
	t.Helper()
	if err := checkScheduleJSONReport(report, expected); err != nil {
		t.Fatalf("%v: %s", err, report)
	}
}

func checkScheduleJSONReport(report []byte, expected map[string]string) error {
	seen := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(string(report)), "\n") {
		var event struct {
			Action  string
			Package string
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return fmt.Errorf("invalid report JSON: %w", err)
		}
		want, ok := expected[event.Package]
		if event.Package == "" || event.Action == "" || !ok || want != event.Action || seen[event.Package] {
			return fmt.Errorf("unexpected terminal event: %s", line)
		}
		seen[event.Package] = true
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("want %d report events, got %d", len(expected), len(seen))
	}
	return nil
}

func TestScheduleJSONReportValidation(t *testing.T) {
	expected := map[string]string{"example/a": "pass", "example/b": "fail"}
	const pass = `{"Package":"example/a","Action":"pass"}`
	const fail = `{"Package":"example/b","Action":"fail"}`
	for _, tc := range []struct {
		name, report string
		valid        bool
	}{
		{"complete", pass + "\n" + fail, true},
		{"reordered-with-metadata", `{"Package":"example/b","Action":"fail","Elapsed":1}` + "\n" + pass, true},
		{"empty-object", "{}\n" + fail, false},
		{"foreign-without-action", `{"Package":"foreign/package"}` + "\n" + fail, false},
		{"missing-package", `{"Action":"pass"}` + "\n" + fail, false},
		{"empty-package", `{"Package":"","Action":"pass"}` + "\n" + fail, false},
		{"null-package", `{"Package":null,"Action":"pass"}` + "\n" + fail, false},
		{"missing-action", `{"Package":"example/a"}` + "\n" + fail, false},
		{"empty-action", `{"Package":"example/a","Action":""}` + "\n" + fail, false},
		{"null-action", `{"Package":"example/a","Action":null}` + "\n" + fail, false},
		{"unknown-package", `{"Package":"foreign/package","Action":"pass"}` + "\n" + fail, false},
		{"duplicate", pass + "\n" + pass, false},
		{"wrong-action", `{"Package":"example/a","Action":"fail"}` + "\n" + fail, false},
		{"malformed", "{\n" + fail, false},
		{"trailing-json", pass + "{}\n" + fail, false},
		{"wrong-package-type", `{"Package":42,"Action":"pass"}` + "\n" + fail, false},
		{"wrong-action-type", `{"Package":"example/a","Action":true}` + "\n" + fail, false},
		{"missing-event", pass, false},
		{"empty-report", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkScheduleJSONReport([]byte(tc.report), expected)
			if tc.valid && err != nil {
				t.Fatalf("valid terminal report rejected: %v", err)
			}
			if !tc.valid && err == nil {
				t.Fatal("invalid terminal report accepted")
			}
		})
	}
}
