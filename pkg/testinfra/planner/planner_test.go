// Copyright 2024 Matrix Origin
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

package planner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/testinfra/types"
)

// --- Diff parsing tests ---

const sampleDiff = `diff --git a/pkg/sql/plan/build.go b/pkg/sql/plan/build.go
index abc1234..def5678 100644
--- a/pkg/sql/plan/build.go
+++ b/pkg/sql/plan/build.go
@@ -10,5 +10,6 @@ func (p *Planner) Build(ctx context.Context) error {
+	// new line
@@ -50,3 +51,4 @@ func (p *Planner) Optimize() {
+	// another line
diff --git a/pkg/vm/engine/disttae/txn.go b/pkg/vm/engine/disttae/txn.go
new file mode 100644
--- /dev/null
+++ b/pkg/vm/engine/disttae/txn.go
@@ -0,0 +1,20 @@
+package disttae
+
+func NewTxn() {}
diff --git a/README.md b/README.md
index aaa..bbb 100644
--- a/README.md
+++ b/README.md
@@ -1,2 +1,3 @@
+Updated readme
`

func TestParseUnifiedDiff(t *testing.T) {
	summary := ParseUnifiedDiff(sampleDiff)
	require.Len(t, summary.Files, 3)

	// File 1: pkg/sql/plan/build.go
	f1 := summary.Files[0]
	assert.Equal(t, "pkg/sql/plan/build.go", f1.Path)
	assert.Equal(t, "modified", f1.ChangeKind)
	assert.Equal(t, "pkg/sql/plan", f1.Package)
	assert.Contains(t, f1.Functions, "Build")
	assert.Contains(t, f1.Functions, "Optimize")

	// File 2: new file
	f2 := summary.Files[1]
	assert.Equal(t, "pkg/vm/engine/disttae/txn.go", f2.Path)
	assert.Equal(t, "added", f2.ChangeKind)
	assert.Equal(t, "pkg/vm/engine/disttae", f2.Package)

	// File 3: non-Go file
	f3 := summary.Files[2]
	assert.Equal(t, "README.md", f3.Path)
	assert.Equal(t, "modified", f3.ChangeKind)
	assert.Equal(t, "", f3.Package)

	// Counts
	assert.True(t, summary.TotalAdded > 0)
}

func TestParseUnifiedDiffRenamed(t *testing.T) {
	diff := `diff --git a/pkg/old/file.go b/pkg/new/file.go
rename from pkg/old/file.go
rename to pkg/new/file.go
`
	summary := ParseUnifiedDiff(diff)
	require.Len(t, summary.Files, 1)
	assert.Equal(t, "renamed", summary.Files[0].ChangeKind)
}

func TestParseUnifiedDiffDeleted(t *testing.T) {
	diff := `diff --git a/pkg/sql/plan/old.go b/pkg/sql/plan/old.go
deleted file mode 100644
--- a/pkg/sql/plan/old.go
+++ /dev/null
@@ -1,10 +0,0 @@
-package plan
`
	summary := ParseUnifiedDiff(diff)
	require.Len(t, summary.Files, 1)
	assert.Equal(t, "deleted", summary.Files[0].ChangeKind)
	assert.Equal(t, 1, summary.TotalDeleted)
}

func TestParseUnifiedDiffEmpty(t *testing.T) {
	summary := ParseUnifiedDiff("")
	assert.Empty(t, summary.Files)
}

// --- Matcher tests ---

func TestMatcherSQLPlan(t *testing.T) {
	m := NewDefaultMatcher()
	r := m.Match("pkg/sql/plan/build.go")

	assert.Contains(t, r.UTPackages, "pkg/sql/plan/...")
	assert.Contains(t, r.BVTCategories, types.CategoryOptimizer)
	assert.Contains(t, r.BVTCategories, types.CategoryPlanCache)
	assert.Equal(t, types.PriorityHigh, r.Priority)
}

func TestMatcherDisttae(t *testing.T) {
	m := NewDefaultMatcher()
	r := m.Match("pkg/vm/engine/disttae/logtail.go")

	assert.Contains(t, r.UTPackages, "pkg/vm/engine/disttae/...")
	assert.Contains(t, r.BVTCategories, types.CategoryDisttae)
	assert.Contains(t, r.BVTCategories, types.CategoryPessimisticTransaction)
	assert.Equal(t, types.PriorityCritical, r.Priority)
}

func TestMatcherBVTTestFile(t *testing.T) {
	m := NewDefaultMatcher()
	r := m.Match("test/distributed/cases/optimizer/basic.test")

	assert.Contains(t, r.BVTCategories, types.TestCategory("optimizer"))
}

func TestMatcherNoMatch(t *testing.T) {
	m := NewDefaultMatcher()
	r := m.Match("docs/something.md")

	assert.Empty(t, r.UTPackages)
	assert.Empty(t, r.BVTCategories)
	assert.Equal(t, types.PriorityLow, r.Priority)
}

func TestMatcherMatchAll(t *testing.T) {
	m := NewDefaultMatcher()
	files := []types.FileChange{
		{Path: "pkg/sql/plan/build.go"},
		{Path: "pkg/vm/engine/disttae/txn.go"},
	}
	r := m.MatchAll(files)

	assert.Contains(t, r.UTPackages, "pkg/sql/plan/...")
	assert.Contains(t, r.UTPackages, "pkg/vm/engine/disttae/...")
	assert.Contains(t, r.BVTCategories, types.CategoryOptimizer)
	assert.Contains(t, r.BVTCategories, types.CategoryDisttae)
	// critical wins over high
	assert.Equal(t, types.PriorityCritical, r.Priority)
}

func TestInferBVTCategory(t *testing.T) {
	tests := []struct {
		path string
		want types.TestCategory
	}{
		{"test/distributed/cases/optimizer/basic.test", "optimizer"},
		{"test/distributed/cases/ddl/create_table.test", "ddl"},
		{"test/distributed/cases/README.md", ""},
		{"pkg/sql/plan/build.go", ""},
		{"test/distributed/cases/", ""},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, inferBVTCategory(tt.path), "path=%s", tt.path)
	}
}

// --- Planner tests ---

func TestPlannerGeneratePlan(t *testing.T) {
	p := NewPlanner()
	diff := &types.DiffSummary{
		PRNumber:   42,
		BaseBranch: "main",
		HeadBranch: "feature/optimizer",
		Files: []types.FileChange{
			{Path: "pkg/sql/plan/build.go", Package: "pkg/sql/plan"},
			{Path: "pkg/sql/plan/optimize.go", Package: "pkg/sql/plan"},
		},
		TotalAdded:   10,
		TotalDeleted: 5,
	}

	plan := p.GeneratePlan(diff)

	assert.Equal(t, 42, plan.PRNumber)
	assert.Equal(t, "main", plan.BaseBranch)
	assert.True(t, len(plan.Tasks) > 0)

	// Should have SCA task (Go files changed)
	hasSCA := false
	for _, task := range plan.Tasks {
		if task.Type == types.TestTypeSCA {
			hasSCA = true
		}
	}
	assert.True(t, hasSCA, "should have SCA task for Go changes")

	// Should have UT task for pkg/sql/plan
	hasUT := false
	for _, task := range plan.Tasks {
		if task.Type == types.TestTypeUT && task.Package == "pkg/sql/plan/..." {
			hasUT = true
		}
	}
	assert.True(t, hasUT, "should have UT task for pkg/sql/plan")

	// Should have BVT optimizer task
	hasBVT := false
	for _, task := range plan.Tasks {
		if task.Type == types.TestTypeBVT && task.Category == types.CategoryOptimizer {
			hasBVT = true
		}
	}
	assert.True(t, hasBVT, "should have BVT optimizer task")

	// Summary should be populated
	assert.Contains(t, plan.Summary, "PR #42")
}

func TestPlannerGeneratePlanFromDiff(t *testing.T) {
	p := NewPlanner()
	plan := p.GeneratePlanFromDiff(sampleDiff, 100, "main", "fix/bug")

	assert.Equal(t, 100, plan.PRNumber)
	assert.Equal(t, "main", plan.BaseBranch)
	assert.Equal(t, "fix/bug", plan.HeadBranch)
	assert.True(t, len(plan.Tasks) > 0)

	// Check tasks include both pkg/sql/plan and pkg/vm/engine/disttae
	pkgs := make(map[string]bool)
	for _, task := range plan.Tasks {
		if task.Type == types.TestTypeUT {
			pkgs[task.Package] = true
		}
	}
	assert.True(t, pkgs["pkg/sql/plan/..."])
	assert.True(t, pkgs["pkg/vm/engine/disttae/..."])
}

func TestPlannerNoGoFiles(t *testing.T) {
	p := NewPlanner()
	diff := &types.DiffSummary{
		PRNumber:   99,
		BaseBranch: "main",
		HeadBranch: "docs/update",
		Files: []types.FileChange{
			{Path: "docs/readme.md"},
			{Path: "README.md"},
		},
	}

	plan := p.GeneratePlan(diff)

	// No SCA task for non-Go changes
	for _, task := range plan.Tasks {
		assert.NotEqual(t, types.TestTypeSCA, task.Type)
	}
}

func TestPlannerCustomMatcher(t *testing.T) {
	custom := []PathMapping{
		{
			PathPrefix:    "custom/",
			UTPackages:    []string{"custom/..."},
			BVTCategories: []types.TestCategory{"custom_cat"},
			Priority:      types.PriorityCritical,
		},
	}
	m := NewMatcher(custom)
	p := NewPlannerWithMatcher(m)

	diff := &types.DiffSummary{
		Files: []types.FileChange{
			{Path: "custom/foo.go"},
		},
	}

	plan := p.GeneratePlan(diff)
	found := false
	for _, task := range plan.Tasks {
		if task.Category == "custom_cat" {
			found = true
		}
	}
	assert.True(t, found)
}

func TestExtractFuncFromHunk(t *testing.T) {
	tests := []struct {
		line string
		want string
	}{
		{"@@ -10,5 +10,6 @@ func (p *Planner) Build(ctx context.Context) error {", "Build"},
		{"@@ -10,5 +10,6 @@ func Optimize() {", "Optimize"},
		{"@@ -10,5 +10,6 @@ type Foo struct {", ""},
		{"@@ -10,5 +10,6 @@", ""},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, extractFuncFromHunk(tt.line), "line=%s", tt.line)
	}
}
