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

package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/testinfra/types"
)

// mockExecutor is a simple Executor for testing that always succeeds.
type mockExecutor struct{}

func (m *mockExecutor) Execute(_ context.Context, task types.TestTask) (*Result, error) {
	return &Result{
		TaskID: task.ID,
		Status: types.TaskStatusPassed,
		Output: "mock: ok",
	}, nil
}

func TestExecutePlan(t *testing.T) {
	plan := &types.TestPlan{
		Tasks: []types.TestTask{
			{ID: "t1", Type: types.TestTypeUT, Package: "pkg/sql/plan/..."},
			{ID: "t2", Type: types.TestTypeBVT, Category: types.CategoryOptimizer},
		},
	}
	results := ExecutePlan(context.Background(), &mockExecutor{}, plan)

	assert.Len(t, results, 2)
	for _, r := range results {
		assert.Equal(t, types.TaskStatusPassed, r.Status)
	}
	// Plan tasks should be updated
	assert.Equal(t, types.TaskStatusPassed, plan.Tasks[0].Status)
	assert.Equal(t, types.TaskStatusPassed, plan.Tasks[1].Status)
}

func TestFormatResults(t *testing.T) {
	results := []*Result{
		{TaskID: "t1", Status: types.TaskStatusPassed},
		{TaskID: "t2", Status: types.TaskStatusFailed, Error: "test failed"},
		{TaskID: "t3", Status: types.TaskStatusSkipped},
	}
	output := FormatResults(results)
	assert.Contains(t, output, "1 passed")
	assert.Contains(t, output, "1 failed")
	assert.Contains(t, output, "1 skipped")
	assert.Contains(t, output, "t2")
	assert.Contains(t, output, "test failed")
}

func TestLocalExecutorBVT(t *testing.T) {
	e := NewLocalExecutor("/tmp/test-repo")
	task := types.TestTask{
		ID:       "bvt-1",
		Type:     types.TestTypeBVT,
		Category: types.CategoryOptimizer,
	}
	result, err := e.Execute(context.Background(), task)
	assert.NoError(t, err)
	assert.Equal(t, types.TaskStatusPassed, result.Status)
	assert.Contains(t, result.Output, "optimizer")
}

func TestLocalExecutorBVTMissingCategory(t *testing.T) {
	e := NewLocalExecutor("/tmp/test-repo")
	task := types.TestTask{
		ID:   "bvt-bad",
		Type: types.TestTypeBVT,
	}
	result, err := e.Execute(context.Background(), task)
	assert.NoError(t, err)
	assert.Equal(t, types.TaskStatusFailed, result.Status)
	assert.Contains(t, result.Error, "requires a category")
}

func TestLocalExecutorUTMissingPackage(t *testing.T) {
	e := NewLocalExecutor("/tmp/test-repo")
	task := types.TestTask{
		ID:   "ut-bad",
		Type: types.TestTypeUT,
	}
	result, err := e.Execute(context.Background(), task)
	assert.NoError(t, err)
	assert.Equal(t, types.TaskStatusFailed, result.Status)
	assert.Contains(t, result.Error, "requires a package")
}

func TestLocalExecutorUnknownType(t *testing.T) {
	e := NewLocalExecutor("/tmp/test-repo")
	task := types.TestTask{
		ID:   "unknown",
		Type: "foobar",
	}
	result, err := e.Execute(context.Background(), task)
	assert.NoError(t, err)
	assert.Equal(t, types.TaskStatusFailed, result.Status)
	assert.Contains(t, result.Error, "unknown task type")
}
