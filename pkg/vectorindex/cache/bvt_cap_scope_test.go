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

package cache

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	// A statement that can actually CHANGE a cap. Session-scope spellings are refused by the
	// variable definition itself (both are ScopeGlobal), so they cannot leak across cases and
	// a case is free to assert that refusal from anywhere.
	capSetRe = regexp.MustCompile(`(?i)^\s*set\s+(global\s+|@@global\.)\s*max_(gpu_)?index_cache_size\b`)
	// mo-tester session blocks: "-- @session:id=1&user=acct:admin&password=..." ... "-- @session}"
	sessionOpenRe  = regexp.MustCompile(`(?i)^\s*--\s*@session:id=\d+&user=([^:&\s]+)`)
	sessionCloseRe = regexp.MustCompile(`(?i)^\s*--\s*@session}`)
)

// The index cache is ONE object per CN, shared by every case the BVT runs concurrently. A cap
// set on the SYS account governs that whole cache, so a case that lowered it would evict a
// neighbouring case's warm indexes and fail it -- a failure that appears in the wrong file, only
// under concurrency, and not on a re-run.
//
// A cap set on a dedicated account governs that account's entries alone, which is why every
// case that binds a cap creates its own account and sets the value inside that session. This
// test is what keeps the next case from taking the easy route.
func TestBVTCapChangesStayInsideATenantAccount(t *testing.T) {
	root := repoRoot(t)
	suite := filepath.Join(root, "test", "distributed")
	if _, err := os.Stat(suite); err != nil {
		t.Skipf("BVT suite not present at %s", suite)
	}

	var offenders []string
	var checked, statements int

	require.NoError(t, filepath.WalkDir(suite, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".sql") {
			return err
		}
		f, oerr := os.Open(path)
		if oerr != nil {
			return oerr
		}
		defer f.Close()

		checked++
		account := "" // empty means the default connection, i.e. SYS
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		for line := 1; scanner.Scan(); line++ {
			text := scanner.Text()
			if m := sessionOpenRe.FindStringSubmatch(text); m != nil {
				account = m[1]
				continue
			}
			if sessionCloseRe.MatchString(text) {
				account = ""
				continue
			}
			if !capSetRe.MatchString(text) {
				continue
			}
			statements++
			if account == "" || strings.EqualFold(account, "sys") || strings.EqualFold(account, "dump") {
				rel, _ := filepath.Rel(root, path)
				where := account
				if where == "" {
					where = "the default (SYS) connection"
				}
				offenders = append(offenders, fmt.Sprintf("%s:%d sets a CN-wide cap on %s: %s",
					rel, line, where, strings.TrimSpace(text)))
			}
		}
		return scanner.Err()
	}))

	require.NotZero(t, checked, "found no .sql cases to scan -- the guard would pass vacuously")
	require.NotZero(t, statements, "found no cap statements -- the pattern has drifted from the cases")
	require.Empty(t, offenders, "a cap set outside a dedicated account governs the whole CN cache "+
		"and will evict other cases' indexes:\n  %s", strings.Join(offenders, "\n  "))
}

// repoRoot walks up from the test's working directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "no go.mod above %s", dir)
		dir = parent
	}
}
