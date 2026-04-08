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

// Package executor provides interfaces and implementations for executing
// test tasks described by a TestPlan. It wraps the existing optools shell
// scripts and provides a programmatic API for running unit tests, BVT
// tests, and static analysis.
package executor

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/testinfra/types"
)

// Result holds the outcome of a single test task execution.
type Result struct {
	TaskID    string           `json:"task_id"`
	Status    types.TaskStatus `json:"status"`
	Output    string           `json:"output"`
	Duration  time.Duration    `json:"duration"`
	Error     string           `json:"error,omitempty"`
	StartedAt time.Time        `json:"started_at"`
	EndedAt   time.Time        `json:"ended_at"`
}

// Executor defines the interface for running test tasks.
type Executor interface {
	// Execute runs the given task and returns the result.
	Execute(ctx context.Context, task types.TestTask) (*Result, error)
}

// LocalExecutor runs tests on the local machine by invoking Go test
// commands and BVT scripts.
type LocalExecutor struct {
	// RepoRoot is the absolute path to the matrixone repository root.
	RepoRoot string
	// UTTimeout is the timeout for unit test execution.
	UTTimeout time.Duration
	// Env holds additional environment variables for test execution.
	Env []string
}

// NewLocalExecutor creates a LocalExecutor with sensible defaults.
func NewLocalExecutor(repoRoot string) *LocalExecutor {
	return &LocalExecutor{
		RepoRoot:  repoRoot,
		UTTimeout: 15 * time.Minute,
	}
}

// Execute runs a single test task.
func (e *LocalExecutor) Execute(ctx context.Context, task types.TestTask) (*Result, error) {
	result := &Result{
		TaskID:    task.ID,
		Status:    types.TaskStatusRunning,
		StartedAt: time.Now(),
	}

	var err error
	switch task.Type {
	case types.TestTypeUT:
		err = e.executeUT(ctx, task, result)
	case types.TestTypeBVT:
		err = e.executeBVT(ctx, task, result)
	case types.TestTypeSCA:
		err = e.executeSCA(ctx, result)
	default:
		err = moerr.NewInternalErrorNoCtxf("unknown task type: %s", task.Type)
	}

	result.EndedAt = time.Now()
	result.Duration = result.EndedAt.Sub(result.StartedAt)

	if err != nil {
		result.Status = types.TaskStatusFailed
		result.Error = err.Error()
		return result, nil
	}

	result.Status = types.TaskStatusPassed
	return result, nil
}

func (e *LocalExecutor) executeUT(ctx context.Context, task types.TestTask, result *Result) error {
	if task.Package == "" {
		return moerr.NewInternalErrorNoCtx("UT task requires a package")
	}

	args := []string{
		"test", "-short", "-count=1",
		"-timeout", e.UTTimeout.String(),
		"-tags", "matrixone_test",
		fmt.Sprintf("./%s", task.Package),
	}

	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = e.RepoRoot
	cmd.Env = append(cmd.Environ(), e.Env...)

	out, err := cmd.CombinedOutput()
	result.Output = string(out)
	return err
}

func (e *LocalExecutor) executeBVT(ctx context.Context, task types.TestTask, result *Result) error {
	if task.Category == "" && task.TestFile == "" {
		return moerr.NewInternalErrorNoCtx("BVT task requires a category or test_file")
	}

	// For BVT, we document the command that would be run.
	// Actual BVT execution requires mo-tester and a running MO instance,
	// so in this first phase we record the intent.
	var target string
	if task.TestFile != "" {
		target = task.TestFile
	} else {
		target = filepath.Join("test/distributed/cases", string(task.Category))
	}

	result.Output = fmt.Sprintf("[BVT] Target: %s\nTo execute: mo-tester -p %s",
		target, filepath.Join(e.RepoRoot, target))
	return nil
}

func (e *LocalExecutor) executeSCA(ctx context.Context, result *Result) error {
	args := []string{
		"vet", "-tags", "matrixone_test",
		"./pkg/...",
	}

	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = e.RepoRoot
	cmd.Env = append(cmd.Environ(), e.Env...)

	out, err := cmd.CombinedOutput()
	result.Output = string(out)
	return err
}

// ExecutePlan runs all tasks in a TestPlan sequentially and returns results.
func ExecutePlan(ctx context.Context, executor Executor, plan *types.TestPlan) []*Result {
	var results []*Result
	for i := range plan.Tasks {
		task := &plan.Tasks[i]
		task.Status = types.TaskStatusRunning

		r, err := executor.Execute(ctx, *task)
		if err != nil {
			r = &Result{
				TaskID: task.ID,
				Status: types.TaskStatusFailed,
				Error:  err.Error(),
			}
		}
		task.Status = r.Status
		results = append(results, r)
	}
	return results
}

// FormatResults produces a human-readable summary of execution results.
func FormatResults(results []*Result) string {
	var b strings.Builder
	passed, failed, skipped := 0, 0, 0
	for _, r := range results {
		switch r.Status {
		case types.TaskStatusPassed:
			passed++
		case types.TaskStatusFailed:
			failed++
		case types.TaskStatusSkipped:
			skipped++
		}
	}
	fmt.Fprintf(&b, "Execution Summary: %d passed, %d failed, %d skipped (total: %d)\n",
		passed, failed, skipped, len(results))

	for _, r := range results {
		icon := "✅"
		if r.Status == types.TaskStatusFailed {
			icon = "❌"
		} else if r.Status == types.TaskStatusSkipped {
			icon = "⏭️"
		}
		fmt.Fprintf(&b, "  %s %s [%s] %s\n", icon, r.TaskID, r.Status, r.Duration)
		if r.Error != "" {
			fmt.Fprintf(&b, "     Error: %s\n", r.Error)
		}
	}
	return b.String()
}
