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

package types

import (
	"encoding/json"
	"time"
)

// Priority represents the execution priority of a test task.
type Priority int

const (
	PriorityCritical Priority = iota
	PriorityHigh
	PriorityMedium
	PriorityLow
)

func (p Priority) String() string {
	switch p {
	case PriorityCritical:
		return "critical"
	case PriorityHigh:
		return "high"
	case PriorityMedium:
		return "medium"
	case PriorityLow:
		return "low"
	default:
		return "unknown"
	}
}

// TestType represents the kind of test to run.
type TestType string

const (
	TestTypeUT  TestType = "unit_test"
	TestTypeBVT TestType = "bvt"
	TestTypeSCA TestType = "sca"
)

// TaskStatus represents the execution status of a test task.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusPassed    TaskStatus = "passed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusSkipped   TaskStatus = "skipped"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// TestCategory represents a category of BVT test cases mapped from
// test/distributed/cases/ subdirectories.
type TestCategory string

// Well-known BVT test categories corresponding to directories under
// test/distributed/cases/.
const (
	CategoryDDL                    TestCategory = "ddl"
	CategoryDML                    TestCategory = "dml"
	CategoryFunction               TestCategory = "function"
	CategoryExpression             TestCategory = "expression"
	CategoryJoin                   TestCategory = "join"
	CategorySubquery               TestCategory = "subquery"
	CategoryOptimizer              TestCategory = "optimizer"
	CategoryPlanCache              TestCategory = "plan_cache"
	CategoryDisttae                TestCategory = "disttae"
	CategoryPessimisticTransaction TestCategory = "pessimistic_transaction"
	CategoryOptimistic             TestCategory = "optimistic"
	CategoryLoadData               TestCategory = "load_data"
	CategoryDtype                  TestCategory = "dtype"
	CategoryView                   TestCategory = "view"
	CategoryCTE                    TestCategory = "cte"
	CategoryRecursiveCTE           TestCategory = "recursive_cte"
	CategoryWindow                 TestCategory = "window"
	CategoryUnion                  TestCategory = "union"
	CategoryTable                  TestCategory = "table"
	CategoryDatabase               TestCategory = "database"
	CategoryForeignKey             TestCategory = "foreign_key"
	CategorySnapshot               TestCategory = "snapshot"
	CategoryPITR                   TestCategory = "pitr"
	CategorySequence               TestCategory = "sequence"
	CategoryProcedure              TestCategory = "procedure"
	CategoryPrepare                TestCategory = "prepare"
	CategorySecurity               TestCategory = "security"
	CategorySystem                 TestCategory = "system"
	CategoryFulltext               TestCategory = "fulltext"
	CategoryUDF                    TestCategory = "udf"
	CategoryVector                 TestCategory = "vector"
	CategoryArray                  TestCategory = "array"
	CategoryStage                  TestCategory = "stage"
	CategoryHint                   TestCategory = "hint"
	CategoryAutoIncrement          TestCategory = "auto_increment"
	CategoryCharsetCollation       TestCategory = "charset_collation"
	CategoryTenant                 TestCategory = "tenant"
	CategoryPlugin                 TestCategory = "plugin"
	CategoryAccessControl          TestCategory = "zz_accesscontrol"
	CategoryCDC                    TestCategory = "cdc"
	CategorySet                    TestCategory = "set"
	CategorySystemVariable         TestCategory = "system_variable"
)

// FileChange represents a single file changed in a PR diff.
type FileChange struct {
	Path      string   `json:"path"`
	ChangeKind string  `json:"change_kind"` // added, modified, deleted, renamed
	Package   string   `json:"package"`     // Go package path, e.g. "pkg/sql/plan"
	Functions []string `json:"functions"`   // changed function names (best effort)
}

// DiffSummary contains the parsed result of a PR's code changes.
type DiffSummary struct {
	PRNumber     int          `json:"pr_number"`
	BaseBranch   string       `json:"base_branch"`
	HeadBranch   string       `json:"head_branch"`
	Files        []FileChange `json:"files"`
	TotalAdded   int          `json:"total_added"`
	TotalDeleted int          `json:"total_deleted"`
}

// AffectedPackages returns the deduplicated set of Go packages affected.
func (d *DiffSummary) AffectedPackages() []string {
	seen := make(map[string]struct{})
	var pkgs []string
	for _, f := range d.Files {
		if f.Package != "" {
			if _, ok := seen[f.Package]; !ok {
				seen[f.Package] = struct{}{}
				pkgs = append(pkgs, f.Package)
			}
		}
	}
	return pkgs
}

// TestTask represents a single executable test task within a TestPlan.
type TestTask struct {
	ID          string       `json:"id"`
	Type        TestType     `json:"type"`
	Category    TestCategory `json:"category,omitempty"`
	Package     string       `json:"package,omitempty"`     // Go package for UT
	TestFile    string       `json:"test_file,omitempty"`   // BVT .test file path
	Priority    Priority     `json:"priority"`
	EstDuration string       `json:"est_duration,omitempty"`
	Status      TaskStatus   `json:"status"`
	Reason      string       `json:"reason"` // why this task is included
}

// TestPlan is the structured output produced by the planner. It describes
// which tests should be run for a given PR.
type TestPlan struct {
	ID          string      `json:"id"`
	PRNumber    int         `json:"pr_number"`
	BaseBranch  string      `json:"base_branch"`
	HeadBranch  string      `json:"head_branch"`
	CreatedAt   time.Time   `json:"created_at"`
	Summary     string      `json:"summary"`
	DiffSummary DiffSummary `json:"diff_summary"`
	Tasks       []TestTask  `json:"tasks"`
}

// ToJSON serializes the TestPlan to indented JSON.
func (tp *TestPlan) ToJSON() ([]byte, error) {
	return json.MarshalIndent(tp, "", "  ")
}

// FromJSON deserializes a TestPlan from JSON bytes.
func FromJSON(data []byte) (*TestPlan, error) {
	var tp TestPlan
	if err := json.Unmarshal(data, &tp); err != nil {
		return nil, err
	}
	return &tp, nil
}

// TaskCountByStatus returns a map of status → count for quick summaries.
func (tp *TestPlan) TaskCountByStatus() map[TaskStatus]int {
	m := make(map[TaskStatus]int)
	for _, t := range tp.Tasks {
		m[t.Status]++
	}
	return m
}

// TaskCountByType returns a map of type → count.
func (tp *TestPlan) TaskCountByType() map[TestType]int {
	m := make(map[TestType]int)
	for _, t := range tp.Tasks {
		m[t.Type]++
	}
	return m
}
