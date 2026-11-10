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
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 真实入口使用受控外部命令，验证阶段失败不会被后续报告步骤吞掉。
func TestRaceUTEntryPreservesExit(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			root := t.TempDir()
			tools := filepath.Join(root, "optools")
			bin := filepath.Join(root, "bin")
			for _, dir := range []string{tools, bin, filepath.Join(root, "scratch"), filepath.Join(root, "pkg")} {
				require.NoError(t, os.MkdirAll(dir, 0700))
			}
			for _, name := range []string{"run_ut.sh", "run_race_ut.sh"} {
				data, err := os.ReadFile(name)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(tools, name), data, 0600))
			}
			require.NoError(t, os.WriteFile(filepath.Join(tools, "utilities.sh"), []byte(`G_TS=fixture
G_WKSP="$FIXTURE_ROOT/scratch"
logger_base(){ :; }
horiz_rule(){ :; }
`), 0600))
			commands := map[string]string{
				"make":           "#!/bin/bash\nexit 0\n",
				"go-ut-analysis": "#!/bin/bash\nmkdir -p ut-report\n",
				"go": `#!/bin/bash
case "$1" in
    list) printf '%s\n' github.com/matrixorigin/matrixone/pkg/sql/plan github.com/matrixorigin/matrixone/pkg/tests/issues ;;
    test)
        printf '{"Action":"output"}\n'
        if [[ "$FAIL_TEST" = 1 ]]; then exit 1; fi ;;
esac
`,
			}
			for name, body := range commands {
				require.NoError(t, os.WriteFile(filepath.Join(bin, name), []byte(body), 0700))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "run_ut.sh", "UT")
			cmd.Dir = tools
			flag := "0"
			if fail {
				flag = "1"
			}
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FIXTURE_ROOT="+root,
				"UT_PARALLEL=8", "UT_TIMEOUT=40", "FAIL_TEST="+flag)
			output, err := cmd.CombinedOutput()
			if fail {
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, "%s", output)
				require.Equal(t, 1, exit.ExitCode(), "%s", output)
			} else {
				require.NoError(t, err, "%s", output)
			}
			data, err := os.ReadFile(filepath.Join(root, "scratch/fixture-UT-Report.out"))
			require.NoError(t, err)
			require.Equal(t, 2, strings.Count(string(data), "{\"Action\":\"output\"}"))
		})
	}
}

func TestRaceScopePreservesInventoryAndFailure(t *testing.T) {
	const root = "github.com/matrixorigin/matrixone/"
	regular := root + "pkg/sql/plan"
	embed := root + "pkg/embed"
	issues := root + "pkg/tests/issues"
	for _, tc := range []struct {
		name     string
		packages []string
		fail     string
		tags     string
		status   string
		want     int
		calls    int
		parallel string
		timeout  string
	}{
		{name: "mixed_reordered", packages: []string{issues, regular, embed}, calls: 2},
		{name: "regular_only", packages: []string{regular}, calls: 1},
		{name: "caller_tags", packages: []string{regular, embed}, tags: "matrixone_test,gpu", calls: 2},
		{name: "embedded_only", packages: []string{embed, issues, root + "pkg/util/test"}, calls: 1},
		{name: "similar_name_is_regular", packages: []string{root + "pkg/embedded", issues}, calls: 2},
		{name: "regular_failure_retained", packages: []string{regular, issues}, fail: regular, status: "1", want: 1, calls: 2},
		{name: "embedded_failure_retained", packages: []string{regular, issues}, fail: issues, status: "1", want: 1, calls: 2},
		{name: "cancel_no_later_admission", packages: []string{regular, issues}, fail: regular, status: "143", want: 143, calls: 1},
		{name: "empty_rejected", want: 2},
		{name: "zero_parallel_rejected", packages: []string{regular}, parallel: "0", want: 2},
		{name: "invalid_timeout_rejected", packages: []string{regular}, timeout: "bad", want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			capture := filepath.Join(dir, "calls")
			report := filepath.Join(dir, "report")
			fake := `#!/bin/bash
printf 'CALL\n' >> "$CAPTURE"
printf '%s\n' "$@" >> "$CAPTURE"
printf '{"Action":"output"}\n'
for arg in "$@"; do
    if [[ -n "$FAIL_PACKAGE" && "$arg" = "$FAIL_PACKAGE" ]]; then exit "$FAIL_STATUS"; fi
done
`
			require.NoError(t, os.WriteFile(filepath.Join(dir, "go"), []byte(fake), 0700))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tags := tc.tags
			if tags == "" {
				tags = "matrixone_test"
			}
			parallel, timeout := tc.parallel, tc.timeout
			if parallel == "" {
				parallel = "8"
			}
			if timeout == "" {
				timeout = "40"
			}
			args := append([]string{"run_race_ut.sh", report, parallel, timeout, tags}, tc.packages...)
			cmd := exec.CommandContext(ctx, "bash", args...)
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "CAPTURE="+capture,
				"FAIL_PACKAGE="+tc.fail, "FAIL_STATUS="+tc.status)
			output, err := cmd.CombinedOutput()
			if tc.want == 0 {
				require.NoError(t, err, "%s", output)
			} else {
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, "%s", output)
				require.Equal(t, tc.want, exit.ExitCode(), "%s", output)
			}
			if tc.calls == 0 {
				_, err := os.Stat(capture)
				require.True(t, os.IsNotExist(err))
				return
			}
			data, err := os.ReadFile(capture)
			require.NoError(t, err)
			calls := strings.Split(strings.TrimPrefix(string(data), "CALL\n"), "CALL\n")
			require.Len(t, calls, tc.calls)
			for i, call := range calls {
				require.Contains(t, call, "test\n-short\n-v\n-json\n-tags\n"+tags+"\n")
				require.Contains(t, call, "-timeout\n40m\n-race\n")
				if strings.Contains(call, issues+"\n") || strings.Contains(call, embed+"\n") || strings.Contains(call, root+"pkg/util/test\n") {
					require.Contains(t, call, "-p\n1\n")
					require.NotContains(t, call, regular+"\n")
				} else {
					require.Zero(t, i, "普通阶段必须先完成")
					require.Contains(t, call, "-p\n8\n")
				}
			}
			for _, pkg := range tc.packages {
				want := 1
				if tc.want == 143 && pkg == issues {
					want = 0
				}
				require.Equal(t, want, strings.Count(string(data), pkg+"\n"), "包不能丢失或重试")
			}
			data, err = os.ReadFile(report)
			require.NoError(t, err)
			require.Equal(t, tc.calls, strings.Count(string(data), "{\"Action\":\"output\"}\n"), "两阶段 JSON 必须完整保留")
		})
	}
}
