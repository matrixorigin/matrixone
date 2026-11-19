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

package taskservice

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

type sqlTaskStopCycle struct{}

func (*sqlTaskStopCycle) Error() string   { return "cyclic precheck error" }
func (e *sqlTaskStopCycle) Unwrap() error { return e }

type sqlTaskCustomStopIdentity struct{}

func (*sqlTaskCustomStopIdentity) Error() string { return "custom precheck error" }
func (*sqlTaskCustomStopIdentity) Is(target error) bool {
	return target == context.Canceled //nolint:errorlint // this hook intentionally adds an identity
}
func (*sqlTaskCustomStopIdentity) Unwrap() error { return context.Canceled }

type sqlTaskCustomAsIdentity struct{}

func (*sqlTaskCustomAsIdentity) Error() string { return "custom precheck error" }
func (*sqlTaskCustomAsIdentity) As(any) bool   { return false }
func (*sqlTaskCustomAsIdentity) Unwrap() error { return context.Canceled }

func TestSQLTaskRunnerStopOnly(t *testing.T) {
	stops := make([]error, 64)
	for i := range stops {
		stops[i] = context.Canceled
	}
	cases := []struct {
		name string
		err  error
		stop error
		want bool
	}{
		{"nil_result", nil, context.Canceled, false},
		{"live_context", context.Canceled, nil, false},
		{"wrapped", fmt.Errorf("stop: %w", context.Canceled), context.Canceled, true},
		{"joined", errors.Join(context.Canceled, fmt.Errorf("stop: %w", context.Canceled)), context.Canceled, true},
		{"converted", moerr.ConvertGoError(context.Background(), context.Canceled), context.Canceled, true},
		{"converted_deadline", moerr.ConvertGoError(context.Background(), context.DeadlineExceeded), context.DeadlineExceeded, true},
		{"different_stop", context.DeadlineExceeded, context.Canceled, false},
		{"joined_failure", errors.Join(context.Canceled, errors.New("precheck failed")), context.Canceled, false},
		{"custom_is", &sqlTaskCustomStopIdentity{}, context.Canceled, false},
		{"custom_as", &sqlTaskCustomAsIdentity{}, context.Canceled, false},
		{"cyclic_error", &sqlTaskStopCycle{}, context.Canceled, false},
		{"over_budget", errors.Join(stops...), context.Canceled, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sqlTaskRunnerStopOnly(tc.err, tc.stop))
		})
	}
}

func TestSQLTaskStoppedWrapperReleasesSlotWithoutParentBackoff(t *testing.T) {
	store := NewMemTaskStorage()
	service := NewTaskService(runtime.DefaultRuntime(), store)
	runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(1), WithRunnerFetchInterval(time.Hour)).(*taskRunner)
	t.Cleanup(func() {
		require.NoError(t, runner.Stop())
		require.NoError(t, service.Close())
	})
	runner.RegisterExecutor(task.TaskCode_SQLTask, func(ctx context.Context, _ task.Task) error { return ctx.Err() })
	require.NoError(t, runner.Start())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runner.parallelismC <- struct{}{}
	runner.run(runningTask{ctx: ctx, task: task.AsyncTask{ID: 1, ParentTaskID: "sql-task:1",
		Metadata: task.TaskMetadata{ID: "stopped", Executor: task.TaskCode_SQLTask}}})
	require.Eventually(t, func() bool { return len(runner.parallelismC) == 0 }, time.Second, time.Millisecond)
	require.Nil(t, runner.deferredSQLTaskParent(), "a stopped invocation must not back off its parent")
	require.Equal(t, uint64(1), runner.sqlDeferred.Load())
}
