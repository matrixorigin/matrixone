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

package analyzer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testinfra/types"
)

func TestExtractChangedPaths(t *testing.T) {
	diff := `diff --git a/pkg/sql/plan/foo.go b/pkg/sql/plan/foo.go
--- a/pkg/sql/plan/foo.go
+++ b/pkg/sql/plan/foo.go
@@ -1,3 +1,4 @@
+import "fmt"
diff --git a/pkg/txn/bar.go b/pkg/txn/bar.go
--- a/pkg/txn/bar.go
+++ b/pkg/txn/bar.go
`
	paths := extractChangedPaths(diff)
	if len(paths) != 2 {
		t.Fatalf("paths = %v, want 2", paths)
	}
	if paths[0] != "pkg/sql/plan/foo.go" {
		t.Errorf("paths[0] = %q", paths[0])
	}
	if paths[1] != "pkg/txn/bar.go" {
		t.Errorf("paths[1] = %q", paths[1])
	}
}

func TestExtractJSON_Plain(t *testing.T) {
	input := `{"summary":"test","affected_modules":[],"coverage":[],"suggested_cases":[]}`
	result := extractJSON(input)
	if result != input {
		t.Errorf("extractJSON plain = %q", result)
	}
}

func TestExtractJSON_Markdown(t *testing.T) {
	input := "Some text\n```json\n{\"summary\":\"test\"}\n```\nmore text"
	result := extractJSON(input)
	if result != `{"summary":"test"}` {
		t.Errorf("extractJSON markdown = %q", result)
	}
}

func TestExtractJSON_MarkdownNoLang(t *testing.T) {
	input := "```\n{\"summary\":\"test\"}\n```"
	result := extractJSON(input)
	if result != `{"summary":"test"}` {
		t.Errorf("extractJSON no lang = %q", result)
	}
}

func TestExtractJSON_ConcatStrings(t *testing.T) {
	input := `{
  "content": "line1\n" +
             "line2\n" +
             "line3"
}`
	result := extractJSON(input)
	var m map[string]string
	if err := json.Unmarshal([]byte(result), &m); err != nil {
		t.Fatalf("should produce valid JSON: %v\nGot: %s", err, result)
	}
	if m["content"] != "line1\nline2\nline3" {
		t.Errorf("content = %q", m["content"])
	}
}

func TestSanitizeJSONConcat(t *testing.T) {
	input := `"hello\n" +
                "world"`
	got := sanitizeJSONConcat(input)
	want := `"hello\nworld"`
	if got != want {
		t.Errorf("sanitizeJSONConcat = %q, want %q", got, want)
	}
}

func TestBuildSystemPrompt(t *testing.T) {
	result := buildSystemPrompt("skill content here")
	if !strings.Contains(result, "MatrixOne") {
		t.Error("system prompt should mention MatrixOne")
	}
	if !strings.Contains(result, "skill content here") {
		t.Error("system prompt should include skills")
	}
	if !strings.Contains(result, "BVT") {
		t.Error("system prompt should mention BVT")
	}
}

func TestBuildUserPrompt(t *testing.T) {
	result := buildUserPrompt(123, "diff content", "function/10 ddl/5", "### sample\n```sql\nSELECT 1;\n```")
	if !strings.Contains(result, "PR #123") {
		t.Error("user prompt should mention PR number")
	}
	if !strings.Contains(result, "diff content") {
		t.Error("user prompt should include diff")
	}
	if !strings.Contains(result, "function/10") {
		t.Error("user prompt should include case summary")
	}
	if !strings.Contains(result, "sample") {
		t.Error("user prompt should include sample cases")
	}
	if !strings.Contains(result, "stability") {
		t.Error("user prompt should mention stability test type")
	}
	if !strings.Contains(result, "mo-chaos-config") {
		t.Error("user prompt should mention chaos config")
	}
}

func TestLoadSkills(t *testing.T) {
	tmp := t.TempDir()
	skillsDir := filepath.Join(tmp, "docs", "ai-skills")
	if err := os.MkdirAll(skillsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create priority skill doc
	if err := os.WriteFile(filepath.Join(skillsDir, "module-test-mapping.md"), []byte("mapping content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillsDir, "testing-guide.md"), []byte("testing content"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Create a non-priority doc
	if err := os.WriteFile(filepath.Join(skillsDir, "architecture.md"), []byte("arch content"), 0o644); err != nil {
		t.Fatal(err)
	}

	a := &Analyzer{config: Config{RepoRoot: tmp}}
	skills, err := a.loadSkills()
	if err != nil {
		t.Fatalf("loadSkills: %v", err)
	}

	if !strings.Contains(skills, "mapping content") {
		t.Error("should include module-test-mapping")
	}
	if !strings.Contains(skills, "testing content") {
		t.Error("should include testing-guide")
	}
	if !strings.Contains(skills, "arch content") {
		t.Error("should include architecture (under budget)")
	}
}

func TestLoadSkills_BudgetCap(t *testing.T) {
	tmp := t.TempDir()
	skillsDir := filepath.Join(tmp, "docs", "ai-skills")
	if err := os.MkdirAll(skillsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create a priority doc that's already 5KB
	bigContent := strings.Repeat("x", 5000)
	if err := os.WriteFile(filepath.Join(skillsDir, "module-test-mapping.md"), []byte(bigContent), 0o644); err != nil {
		t.Fatal(err)
	}
	// testing-guide pushes us over 6KB
	if err := os.WriteFile(filepath.Join(skillsDir, "testing-guide.md"), []byte(strings.Repeat("y", 2000)), 0o644); err != nil {
		t.Fatal(err)
	}
	// This extra doc should be skipped due to budget
	if err := os.WriteFile(filepath.Join(skillsDir, "extra.md"), []byte("extra"), 0o644); err != nil {
		t.Fatal(err)
	}

	a := &Analyzer{config: Config{RepoRoot: tmp}}
	skills, err := a.loadSkills()
	if err != nil {
		t.Fatalf("loadSkills: %v", err)
	}

	// Priority docs always loaded
	if !strings.Contains(skills, bigContent) {
		t.Error("priority doc should always be loaded")
	}
	// Extra should be skipped (budget >6000)
	if strings.Contains(skills, "extra") {
		t.Error("extra doc should be skipped (over budget)")
	}
}

func TestLoadSkills_NoDir(t *testing.T) {
	tmp := t.TempDir()
	a := &Analyzer{config: Config{RepoRoot: tmp}}
	skills, err := a.loadSkills()
	if err != nil {
		t.Fatalf("loadSkills: %v", err)
	}
	if skills != "" {
		t.Errorf("expected empty skills, got %q", skills)
	}
}

func TestParseReport(t *testing.T) {
	jsonStr := `{
		"summary": "fix decimal compare",
		"affected_modules": ["pkg/sql"],
		"coverage": [
			{"type": "bvt", "status": "covered", "description": "ok"},
			{"type": "stability", "status": "not_related", "description": "n/a"},
			{"type": "chaos", "status": "not_related", "description": "n/a"},
			{"type": "bigdata", "status": "not_related", "description": "n/a"},
			{"type": "pitr", "status": "not_related", "description": "n/a"},
			{"type": "snapshot", "status": "not_related", "description": "n/a"}
		],
		"suggested_cases": []
	}`

	report, err := parseReport(jsonStr, 100)
	if err != nil {
		t.Fatalf("parseReport: %v", err)
	}
	if report.PRNumber != 100 {
		t.Errorf("PRNumber = %d, want 100", report.PRNumber)
	}
	if report.Summary != "fix decimal compare" {
		t.Errorf("Summary = %q", report.Summary)
	}
	if len(report.Coverage) != 6 {
		t.Errorf("Coverage len = %d, want 6", len(report.Coverage))
	}
}

func TestParseReport_WithMarkdown(t *testing.T) {
	input := "Here is the analysis:\n```json\n" +
		`{"summary":"test","affected_modules":[],"coverage":[],"suggested_cases":[]}` +
		"\n```\n"
	report, err := parseReport(input, 1)
	if err != nil {
		t.Fatalf("parseReport with markdown: %v", err)
	}
	if report.Summary != "test" {
		t.Errorf("Summary = %q", report.Summary)
	}
}

func TestParseReport_InvalidJSON(t *testing.T) {
	_, err := parseReport("not json", 1)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestFormatReport(t *testing.T) {
	report := &types.CoverageReport{
		PRNumber:        42,
		Summary:         "test PR",
		AffectedModules: []string{"pkg/sql", "pkg/vm"},
		Coverage: []types.CoverageItem{
			{Type: types.TestBVT, Status: types.StatusCovered, Description: "ok"},
			{Type: types.TestStability, Status: types.StatusNeedsAttention, Description: "needs work"},
			{Type: types.TestChaos, Status: types.StatusNotRelated, Description: "n/a"},
		},
		SuggestedCases: []types.SuggestedCase{
			{
				Type:     types.TestBVT,
				Category: "function",
				Filename: "decimal_compare.test",
				Content:  "SELECT 1.0 = 1;",
				Reason:   "missing decimal test",
			},
		},
	}

	output := FormatReport(report)

	checks := []string{
		"PR #42",
		"test PR",
		"pkg/sql",
		"pkg/vm",
		"BVT",
		"✅",
		"⚠️",
		"➖",
		"needs work",
		"decimal_compare.test",
		"SELECT 1.0 = 1;",
		"missing decimal test",
	}
	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Errorf("output should contain %q", check)
		}
	}
}

func TestAnalyze_EndToEnd(t *testing.T) {
	// Mock LLM server
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"choices": []map[string]interface{}{
				{
					"message": map[string]interface{}{
						"content": `{"summary":"mock","affected_modules":["pkg/test"],"coverage":[{"type":"bvt","status":"covered","description":"ok"},{"type":"stability","status":"not_related","description":"n/a"},{"type":"chaos","status":"not_related","description":"n/a"},{"type":"bigdata","status":"not_related","description":"n/a"},{"type":"pitr","status":"not_related","description":"n/a"},{"type":"snapshot","status":"not_related","description":"n/a"}],"suggested_cases":[]}`,
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	// Create temp repo structure
	tmp := t.TempDir()
	// Skills
	skillsDir := filepath.Join(tmp, "docs", "ai-skills")
	if err := os.MkdirAll(skillsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillsDir, "testing-guide.md"), []byte("guide"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Cases
	casesDir := filepath.Join(tmp, "test", "distributed", "cases", "function")
	if err := os.MkdirAll(casesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(casesDir, "test.test"), []byte("SELECT 1;"), 0o644); err != nil {
		t.Fatal(err)
	}

	// This test requires `gh` CLI to get the diff - skip if not available
	if _, err := os.Stat("/opt/homebrew/bin/gh"); err != nil {
		t.Skip("gh CLI not available, skipping end-to-end test")
	}

	cfg := Config{
		Repo:     "matrixorigin/matrixone",
		RepoRoot: tmp,
		APIURL:   srv.URL,
		APIKey:   "test-key",
		Model:    "test-model",
	}

	a := New(cfg)
	report, err := a.Analyze(context.Background(), 24088)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if report.PRNumber != 24088 {
		t.Errorf("PRNumber = %d", report.PRNumber)
	}
	if report.Summary != "mock" {
		t.Errorf("Summary = %q", report.Summary)
	}
	if len(report.Coverage) != 6 {
		t.Errorf("Coverage len = %d", len(report.Coverage))
	}
}
