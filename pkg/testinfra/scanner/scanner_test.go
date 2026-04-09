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

package scanner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScanBVTCases(t *testing.T) {
	// Create a temporary directory structure
	tmp := t.TempDir()
	casesDir := filepath.Join(tmp, "test", "distributed", "cases")
	funcDir := filepath.Join(casesDir, "function")
	ddlDir := filepath.Join(casesDir, "ddl")

	if err := os.MkdirAll(funcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(ddlDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create test files
	for _, f := range []string{"test1.test", "test2.test", "other.txt"} {
		if err := os.WriteFile(filepath.Join(funcDir, f), []byte("SELECT 1;"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"create.sql", "drop.test"} {
		if err := os.WriteFile(filepath.Join(ddlDir, f), []byte("CREATE TABLE t(a INT);"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	cases, err := ScanBVTCases(tmp)
	if err != nil {
		t.Fatalf("ScanBVTCases: %v", err)
	}

	if len(cases) != 2 {
		t.Fatalf("got %d categories, want 2", len(cases))
	}

	// Find each category
	caseMap := make(map[string]CaseInfo)
	for _, c := range cases {
		caseMap[c.Category] = c
	}

	funcInfo, ok := caseMap["function"]
	if !ok {
		t.Fatal("missing 'function' category")
	}
	// Should have 2 .test files, not the .txt
	if len(funcInfo.Files) != 2 {
		t.Errorf("function files = %d, want 2: %v", len(funcInfo.Files), funcInfo.Files)
	}

	ddlInfo, ok := caseMap["ddl"]
	if !ok {
		t.Fatal("missing 'ddl' category")
	}
	// Should have 1 .sql + 1 .test = 2
	if len(ddlInfo.Files) != 2 {
		t.Errorf("ddl files = %d, want 2: %v", len(ddlInfo.Files), ddlInfo.Files)
	}
}

func TestScanBVTCases_MissingDir(t *testing.T) {
	_, err := ScanBVTCases("/nonexistent/path")
	if err == nil {
		t.Fatal("expected error for missing dir")
	}
}

func TestFormatCaseSummary(t *testing.T) {
	cases := []CaseInfo{
		{Category: "function", Files: []string{"a.test", "b.test", "c.test"}},
		{Category: "ddl", Files: []string{"x.sql"}},
	}

	summary := FormatCaseSummary(cases)

	if !strings.Contains(summary, "function/3") {
		t.Errorf("summary should contain 'function/3', got: %q", summary)
	}
	if !strings.Contains(summary, "ddl/1") {
		t.Errorf("summary should contain 'ddl/1', got: %q", summary)
	}
}

func TestFormatCaseSummary_Empty(t *testing.T) {
	summary := FormatCaseSummary(nil)
	if summary != "" {
		t.Errorf("expected empty summary, got: %q", summary)
	}
}

func TestReadSampleCases(t *testing.T) {
	tmp := t.TempDir()
	funcDir := filepath.Join(tmp, "test", "distributed", "cases", "function")
	if err := os.MkdirAll(funcDir, 0o755); err != nil {
		t.Fatal(err)
	}

	content := "-- test sample\nSELECT 1;\nSELECT 2;\ndrop table if exists t1;\n"
	if err := os.WriteFile(filepath.Join(funcDir, "sample.test"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	// Changed path in pkg/sql/plan should map to "function" category
	result := ReadSampleCases(tmp, []string{"pkg/sql/plan/some_file.go"}, 3, 40)
	if !strings.Contains(result, "function/sample.test") {
		t.Errorf("should include function/sample.test, got: %q", result)
	}
	if !strings.Contains(result, "SELECT 1;") {
		t.Errorf("should include file content, got: %q", result)
	}
}

func TestReadSampleCases_NoMatch(t *testing.T) {
	tmp := t.TempDir()
	result := ReadSampleCases(tmp, []string{"unknown/path.go"}, 3, 40)
	if result != "" {
		t.Errorf("expected empty result for unknown path, got: %q", result)
	}
}

func TestInferCategories(t *testing.T) {
	cases := []struct {
		path string
		want []string
	}{
		{"pkg/sql/plan/foo.go", []string{"cte", "function", "join", "optimizer", "subquery", "window"}},
		{"pkg/txn/foo.go", []string{"optimistic", "pessimistic_transaction"}},
		{"pkg/frontend/foo.go", []string{"pitr", "snapshot", "tenant"}},
		{"pkg/fulltext/foo.go", []string{"fulltext"}},
		{"pkg/cdc/foo.go", []string{"dml"}},
		{"unknown/foo.go", nil},
	}

	for _, tc := range cases {
		got := inferCategories([]string{tc.path})
		if len(got) != len(tc.want) {
			t.Errorf("inferCategories(%q) = %v, want %v", tc.path, got, tc.want)
			continue
		}
		for i, g := range got {
			if g != tc.want[i] {
				t.Errorf("inferCategories(%q)[%d] = %q, want %q", tc.path, i, g, tc.want[i])
			}
		}
	}
}
