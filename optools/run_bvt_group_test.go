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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Exercise the CI entry point without starting Docker or executing SQL.
func TestBVTGroupOwnsZoneMapOnce(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	write := func(name, body string) {
		p := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(body), 0o755))
	}
	selector, err := os.ReadFile("run_bvt_group.sh")
	require.NoError(t, err)
	write("run_bvt_group.sh", string(selector))
	write("cases/optimizer/a.sql", "select 1;")
	write("cases/dtype/b.test", "select 1;")
	write("isolated/non_primary_zonemap.sql", "select 1;")
	write("run_zonemap_bvt.sh", `set -eu
[[ "$1" -ef "$ROOT/tester" && "$2" -ef "$ROOT/isolated/non_primary_zonemap.sql" ]]
echo private >> "$ROOT/calls"
exit "$PRIVATE_RC"`)
	write("tester/run.sh", `#!/usr/bin/env bash
set -eu
while (($#)); do
    if [[ "$1" == -i ]]; then echo "shared $2" >> "$ROOT/calls"; exit 0; fi
    shift
done
exit 2`)
	for _, tc := range []struct{ group, rc, want string }{
		{"0", "0", "private\nshared " + root + "/cases/optimizer/a.sql"},
		{"1", "0", "shared " + root + "/cases/dtype/b.test"},
		{"0", "42", "private"},
	} {
		write("calls", "")
		cmd := exec.Command("bash", "run_bvt_group.sh", "tester", "cases", tc.group)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "ROOT="+root, "PRIVATE_RC="+tc.rc)
		out, err := cmd.CombinedOutput()
		if tc.rc == "0" {
			require.NoError(t, err, "%s", out)
		} else {
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, "%s", out)
			require.Equal(t, 42, exit.ExitCode())
		}
		calls, err := os.ReadFile(filepath.Join(root, "calls"))
		require.NoError(t, err)
		require.Equal(t, tc.want, strings.TrimSpace(string(calls)))
	}
}
