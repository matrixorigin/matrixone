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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPriorityString(t *testing.T) {
	tests := []struct {
		p    Priority
		want string
	}{
		{PriorityCritical, "critical"},
		{PriorityHigh, "high"},
		{PriorityMedium, "medium"},
		{PriorityLow, "low"},
		{Priority(99), "unknown"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, tt.p.String())
	}
}

func TestDiffSummaryAffectedPackages(t *testing.T) {
	ds := DiffSummary{
		Files: []FileChange{
			{Path: "pkg/sql/plan/build.go", Package: "pkg/sql/plan"},
			{Path: "pkg/sql/plan/optimize.go", Package: "pkg/sql/plan"},
			{Path: "pkg/vm/engine/disttae/txn.go", Package: "pkg/vm/engine/disttae"},
			{Path: "README.md", Package: ""},
		},
	}
	pkgs := ds.AffectedPackages()
	assert.Equal(t, 2, len(pkgs))
	assert.Contains(t, pkgs, "pkg/sql/plan")
	assert.Contains(t, pkgs, "pkg/vm/engine/disttae")
}

func TestTestPlanJSON(t *testing.T) {
	plan := &TestPlan{
		ID:         "tp-001",
		PRNumber:   12345,
		BaseBranch: "main",
		HeadBranch: "feature/test",
		CreatedAt:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Summary:    "Test plan for PR #12345",
		Tasks: []TestTask{
			{
				ID:       "task-1",
				Type:     TestTypeUT,
				Package:  "pkg/sql/plan",
				Priority: PriorityHigh,
				Status:   TaskStatusPending,
				Reason:   "pkg/sql/plan modified",
			},
			{
				ID:       "task-2",
				Type:     TestTypeBVT,
				Category: CategoryOptimizer,
				Priority: PriorityMedium,
				Status:   TaskStatusPending,
				Reason:   "optimizer category mapped from pkg/sql/plan",
			},
		},
	}

	data, err := plan.ToJSON()
	require.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Equal(t, "tp-001", parsed["id"])
	assert.Equal(t, float64(12345), parsed["pr_number"])

	restored, err := FromJSON(data)
	require.NoError(t, err)
	assert.Equal(t, plan.ID, restored.ID)
	assert.Equal(t, plan.PRNumber, restored.PRNumber)
	assert.Equal(t, len(plan.Tasks), len(restored.Tasks))
	assert.Equal(t, plan.Tasks[0].Package, restored.Tasks[0].Package)
}

func TestTestPlanTaskCounts(t *testing.T) {
	plan := &TestPlan{
		Tasks: []TestTask{
			{Status: TaskStatusPending, Type: TestTypeUT},
			{Status: TaskStatusPending, Type: TestTypeBVT},
			{Status: TaskStatusRunning, Type: TestTypeUT},
			{Status: TaskStatusPassed, Type: TestTypeBVT},
			{Status: TaskStatusFailed, Type: TestTypeSCA},
		},
	}

	byStatus := plan.TaskCountByStatus()
	assert.Equal(t, 2, byStatus[TaskStatusPending])
	assert.Equal(t, 1, byStatus[TaskStatusRunning])
	assert.Equal(t, 1, byStatus[TaskStatusPassed])
	assert.Equal(t, 1, byStatus[TaskStatusFailed])

	byType := plan.TaskCountByType()
	assert.Equal(t, 2, byType[TestTypeUT])
	assert.Equal(t, 2, byType[TestTypeBVT])
	assert.Equal(t, 1, byType[TestTypeSCA])
}

func TestFromJSONInvalid(t *testing.T) {
	_, err := FromJSON([]byte("not json"))
	assert.Error(t, err)
}
