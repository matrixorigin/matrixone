// Copyright 2024 Matrix Origin
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

package dedup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testinfra/types"
)

func TestExtractStatements(t *testing.T) {
	content := `-- this is a comment
-- @bvt:issue#123
drop table if exists t1;
create table t1 (a int, b varchar(100));
insert into t1 values (1, 'hello');
SELECT a, b FROM t1 WHERE a > 0;
-- @bvt:issue
drop table t1;
`
	stmts := extractStatements(content)

	// Should skip: comments, drop table if exists (boilerplate), drop table (boilerplate)
	// Should keep: create table, insert, select
	if len(stmts) != 3 {
		t.Fatalf("got %d statements, want 3: %v", len(stmts), stmts)
	}
	if stmts[0] != "create table t1 (a int, b varchar(100));" {
		t.Errorf("stmts[0] = %q", stmts[0])
	}
	if stmts[1] != "insert into t1 values (1, 'hello');" {
		t.Errorf("stmts[1] = %q", stmts[1])
	}
	if stmts[2] != "select a, b from t1 where a > 0;" {
		t.Errorf("stmts[2] = %q", stmts[2])
	}
}

func TestExtractStatements_MultiLine(t *testing.T) {
	content := `CREATE TABLE t1 (
    a INT,
    b VARCHAR(100)
);
SELECT * FROM t1;`
	stmts := extractStatements(content)
	if len(stmts) != 2 {
		t.Fatalf("got %d statements, want 2: %v", len(stmts), stmts)
	}
}

func TestNormalizeSQL(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"  SELECT  *  FROM  t1 ;  ", "select * from t1 ;"},
		{"INSERT INTO t1  VALUES (1);", "insert into t1 values (1);"},
	}
	for _, tc := range cases {
		got := normalizeSQL(tc.in)
		if got != tc.want {
			t.Errorf("normalizeSQL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestIsBoilerplate(t *testing.T) {
	boilerplate := []string{
		"drop table if exists t1;",
		"drop database if exists db1;",
		"use db1;",
		"set global var = 1;",
		"begin;",
		"commit;",
	}
	for _, s := range boilerplate {
		if !isBoilerplate(s) {
			t.Errorf("%q should be boilerplate", s)
		}
	}

	notBoilerplate := []string{
		"create table t1 (a int);",
		"select * from t1;",
		"insert into t1 values (1);",
	}
	for _, s := range notBoilerplate {
		if isBoilerplate(s) {
			t.Errorf("%q should NOT be boilerplate", s)
		}
	}
}

func TestFilter_NoDuplicate(t *testing.T) {
	tmp := t.TempDir()
	catDir := filepath.Join(tmp, "test", "distributed", "cases", "function")
	if err := os.MkdirAll(catDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catDir, "existing.sql"), []byte("SELECT 1;\nSELECT 2;\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	d := New(tmp)
	cases := []types.SuggestedCase{
		{
			Type:     types.TestBVT,
			Category: "function",
			Filename: "new_case.sql",
			Content:  "CREATE TABLE t1 (a int);\nSELECT a FROM t1;\ndrop table t1;\n",
		},
	}

	kept, skipped := d.Filter(cases)
	if len(kept) != 1 {
		t.Errorf("kept = %d, want 1", len(kept))
	}
	if len(skipped) != 0 {
		t.Errorf("skipped = %v", skipped)
	}
}

func TestFilter_Duplicate(t *testing.T) {
	tmp := t.TempDir()
	catDir := filepath.Join(tmp, "test", "distributed", "cases", "function")
	if err := os.MkdirAll(catDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Existing file has these SQL statements
	existing := "CREATE TABLE t1 (a INT);\nINSERT INTO t1 VALUES (1);\nSELECT * FROM t1;\ndrop table t1;\n"
	if err := os.WriteFile(filepath.Join(catDir, "existing.sql"), []byte(existing), 0o644); err != nil {
		t.Fatal(err)
	}

	d := New(tmp)
	cases := []types.SuggestedCase{
		{
			Type:     types.TestBVT,
			Category: "function",
			Filename: "duplicate.sql",
			// Same SQL, slightly different formatting
			Content: "create table t1 (a int);\ninsert into t1 values (1);\nselect * from t1;\ndrop table t1;\n",
		},
	}

	kept, skipped := d.Filter(cases)
	if len(kept) != 0 {
		t.Errorf("kept = %d, want 0 (should be filtered)", len(kept))
	}
	if len(skipped) != 1 {
		t.Errorf("skipped = %d, want 1", len(skipped))
	}
}

func TestFilter_PartialOverlap(t *testing.T) {
	tmp := t.TempDir()
	catDir := filepath.Join(tmp, "test", "distributed", "cases", "function")
	if err := os.MkdirAll(catDir, 0o755); err != nil {
		t.Fatal(err)
	}
	existing := "SELECT 1;\nSELECT 2;\n"
	if err := os.WriteFile(filepath.Join(catDir, "existing.sql"), []byte(existing), 0o644); err != nil {
		t.Fatal(err)
	}

	d := New(tmp)
	cases := []types.SuggestedCase{
		{
			Type:     types.TestBVT,
			Category: "function",
			Filename: "partial.sql",
			// 4 statements, only 1 overlaps (SELECT 1) = 25% < 50%, so keep
			Content: "SELECT 1;\nSELECT 3;\nSELECT 4;\nSELECT 5;\n",
		},
	}

	kept, skipped := d.Filter(cases)
	if len(kept) != 1 {
		t.Errorf("kept = %d, want 1 (25%% overlap should pass)", len(kept))
	}
	if len(skipped) != 0 {
		t.Errorf("skipped = %v", skipped)
	}
}

func TestFilter_NightlySkipsDedup(t *testing.T) {
	tmp := t.TempDir()
	d := New(tmp)

	cases := []types.SuggestedCase{
		{
			Type:     types.TestStability,
			Category: "sysbench/new_scenario",
			Filename: "run.yml",
			Content:  "duration: 10\n",
		},
		{
			Type:     types.TestChaos,
			Category: "mo-chaos-config",
			Filename: "new_chaos.yaml",
			Content:  "chaos: test\n",
		},
	}

	kept, skipped := d.Filter(cases)
	if len(kept) != 2 {
		t.Errorf("kept = %d, want 2 (nightly cases skip dedup)", len(kept))
	}
	if len(skipped) != 0 {
		t.Errorf("skipped = %v", skipped)
	}
}

func TestFilter_EmptyNewCase(t *testing.T) {
	tmp := t.TempDir()
	d := New(tmp)

	cases := []types.SuggestedCase{
		{
			Type:     types.TestBVT,
			Category: "function",
			Filename: "empty.sql",
			Content:  "-- just comments\n-- nothing here\n",
		},
	}

	kept, _ := d.Filter(cases)
	// Empty SQL → 0 statements → not duplicate → kept
	if len(kept) != 1 {
		t.Errorf("kept = %d, want 1", len(kept))
	}
}
