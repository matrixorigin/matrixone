// Copyright 2026 Matrix Origin
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

package vm

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type testContinuation struct {
	child                        *testContinuation
	result                       int
	closed                       int
	entered                      bool
	stepErr, closeErr, acceptErr error
	onStep                       func()
}

func (c *testContinuation) Step(context.Context) (ContinuationStep, error) {
	if c.onStep != nil {
		c.onStep()
	}
	if !c.entered {
		c.entered = true
		if c.child != nil {
			return ContinuationStep{Child: c.child}, c.stepErr
		}
	}
	return ContinuationStep{Done: true}, c.stepErr
}
func (c *testContinuation) Accept(child Continuation) error {
	c.result = child.(*testContinuation).result + 1
	return c.acceptErr
}
func (c *testContinuation) Close(error) error { c.closed++; return c.closeErr }

func TestContinuationDepthAndCleanup(t *testing.T) {
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer budget.Close()
	defer generation.Close()
	tasks := make([]testContinuation, 10000)
	for i := 0; i < len(tasks)-1; i++ {
		tasks[i].child = &tasks[i+1]
	}
	got := -1
	require.NoError(t, RunContinuation(context.Background(), generation, &tasks[0], func(c Continuation) error { got = c.(*testContinuation).result; return nil }))
	require.Equal(t, len(tasks)-1, got)
	require.Zero(t, generation.Used())
	for i := range tasks {
		require.Equal(t, 1, tasks[i].closed)
	}
}

func TestContinuationFailureUnwindsEveryOwner(t *testing.T) {
	failure := errors.New("read failed")
	for _, where := range []string{"step", "child_admission", "root_admission", "accept", "cleanup", "cancel", "step_with_child"} {
		t.Run(where, func(t *testing.T) {
			cap := uint64(4096)
			if where == "root_admission" {
				cap = 1
			}
			if where == "child_admission" {
				cap = 128
			}
			budget := process.MustNewExecutionResourceBudget(cap, cap)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			defer budget.Close()
			defer generation.Close()
			tasks := make([]testContinuation, 9)
			for i := 0; i < len(tasks)-1; i++ {
				tasks[i].child = &tasks[i+1]
			}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			switch where {
			case "step":
				tasks[8].stepErr = failure
			case "accept":
				tasks[7].acceptErr = failure
			case "cleanup":
				tasks[8].closeErr = failure
			case "cancel":
				tasks[7].onStep = func() { cancel(context.Canceled) }
				tasks[8].closeErr = failure
			case "step_with_child":
				tasks[0].stepErr = failure
			}
			err = RunContinuation(ctx, generation, &tasks[0], nil)
			require.Error(t, err)
			if where != "root_admission" && where != "child_admission" {
				require.ErrorIs(t, err, failure)
			}
			require.Zero(t, generation.Used())
			require.Equal(t, 1, tasks[0].closed)
			for i := range tasks {
				require.LessOrEqual(t, tasks[i].closed, 1)
			}
			if where == "step_with_child" {
				require.Equal(t, 1, tasks[1].closed)
				require.False(t, tasks[1].entered)
			}
			if where == "child_admission" {
				require.Equal(t, 1, tasks[8].closed)
				require.False(t, tasks[8].entered)
			}
		})
	}
}

func TestContinuationCancellationDoesNotHideFrozenFailure(t *testing.T) {
	frozen := process.MarkPipelineFailure(context.Canceled)
	require.Same(t, frozen, continuationError(frozen, errors.New("cleanup")))
	require.Same(t, frozen, continuationError(context.Canceled, frozen))
	task := &testContinuation{}
	require.Error(t, RunContinuation(context.Background(), nil, task, nil))
	require.Equal(t, 1, task.closed)
}
