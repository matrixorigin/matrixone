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
	"slices"
	"strings"
	"testing"
)

// TestRunBVTGroupPreservesCoverageAndBalanceAssignment executes the real group
// selector with a fake mo-tester. Rebalancing must move directories, never add,
// drop, duplicate, or reorder cases within their selected lexical stream.
func TestRunBVTGroupPreservesCoverageAndBalanceAssignment(t *testing.T) {
	root := t.TempDir()
	tester := filepath.Join(root, "tester")
	cases := filepath.Join(root, "cases")
	scripts := []string{
		"array/a.sql",
		"array/optimistic_only.sql", // Deliberately excluded by the existing contract.
		"snapshot/a.sql",
		"git4data/branch/a.sql",
		"udf_python/a.sql",
		"pessimistic_transaction/a.sql",
		"log/a.sql",
		"result_count/a.sql",
		"sql_source_type/a.sql",
		"statement_query_type/a.sql",
		"zz_statement_query_type/a.sql",
		"new_suite/a.test", // Unknown suites must still run exactly once.
	}
	for _, name := range scripts {
		path := filepath.Join(cases, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("select 1;\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(tester, 0o755); err != nil {
		t.Fatal(err)
	}
	runner := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "$CAPTURE.args"
while (($#)); do
  if [[ "$1" == "-i" ]]; then
    printf '%s' "$2" > "$CAPTURE"
    exit 0
  fi
  shift
done
exit 2
`
	runnerPath := filepath.Join(tester, "run.sh")
	if err := os.WriteFile(runnerPath, []byte(runner), 0o755); err != nil {
		t.Fatal(err)
	}
	script, err := filepath.Abs("run_bvt_group.sh")
	if err != nil {
		t.Fatal(err)
	}

	selected := make(map[string][]string)
	seen := make(map[string]int)
	resources := filepath.Join(root, "test resources")
	if err := os.MkdirAll(resources, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, group := range []string{"0", "1"} {
		capture := filepath.Join(root, "group-"+group)
		args := []string{script, tester, cases, group}
		if group == "1" {
			args = append(args, resources)
		}
		cmd := exec.Command("bash", args...)
		cmd.Env = append(os.Environ(), "CAPTURE="+capture)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("group %s: %v\n%s", group, err, output)
		}
		argv, err := os.ReadFile(capture + ".args")
		if err != nil {
			t.Fatal(err)
		}
		gotArgs := strings.Split(strings.TrimSuffix(string(argv), "\n"), "\n")
		if group == "0" {
			if len(gotArgs) != 7 {
				t.Fatalf("default resources must not add arguments: %q", gotArgs)
			}
		} else if len(gotArgs) != 9 || gotArgs[7] != "-s" || gotArgs[8] != resources {
			t.Fatalf("explicit resource directory must remain one argument: %q", gotArgs)
		}
		data, err := os.ReadFile(capture)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range strings.Split(string(data), ",") {
			rel, err := filepath.Rel(cases, path)
			if err != nil {
				t.Fatal(err)
			}
			rel = filepath.ToSlash(rel)
			selected[group] = append(selected[group], rel)
			seen[rel]++
		}
	}

	for _, name := range scripts {
		want := 1
		if strings.Contains(name, "optimistic") {
			want = 0
		}
		if seen[name] != want {
			t.Errorf("%s selected %d times, want %d (groups: %#v)", name, seen[name], want, selected)
		}
	}
	if !slices.Contains(selected["1"], "git4data/branch/a.sql") || slices.Contains(selected["0"], "git4data/branch/a.sql") {
		t.Fatalf("git4data must be assigned to group 1: %#v", selected)
	}

	statementStream := []string{
		"log/a.sql",
		"result_count/a.sql",
		"sql_source_type/a.sql",
		"statement_query_type/a.sql",
		"zz_statement_query_type/a.sql",
	}
	previous := -1
	for _, name := range statementStream {
		index := slices.Index(selected["1"], name)
		if index < 0 || index <= previous {
			t.Fatalf("statement-info producer/verifier order changed at %s: %#v", name, selected["1"])
		}
		previous = index
	}
}
