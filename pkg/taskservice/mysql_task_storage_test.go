// Copyright 2022 Matrix Origin
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

package taskservice

import (
	"slices"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
)

func TestBuildWhereClause(t *testing.T) {
	cases := []struct {
		condition conditions

		expected string
	}{
		{
			condition: conditions(map[condCode]condition{CondTaskID: &taskIDCond{op: EQ, taskID: 1}}),
			expected:  " AND task_id=1",
		},
		{
			condition: conditions(
				map[condCode]condition{
					CondTaskID:           &taskIDCond{op: EQ, taskID: 1},
					CondTaskRunner:       &taskRunnerCond{op: EQ, taskRunner: "abc"},
					CondTaskStatus:       &taskStatusCond{op: IN, taskStatus: []task.TaskStatus{task.TaskStatus_Created}},
					CondTaskEpoch:        &taskEpochCond{op: LE, taskEpoch: 100},
					CondTaskParentTaskID: &taskParentTaskIDCond{op: GE, taskParentTaskID: "ab"},
					CondTaskExecutor:     &taskExecutorCond{op: GE, taskExecutor: 1},
				},
			),
			expected: " AND task_id=1 AND task_runner='abc' AND task_status IN (0) AND task_epoch<=100 AND task_parent_id>='ab' AND task_metadata_executor>=1",
		},
		{
			condition: conditions(map[condCode]condition{
				CondTaskRunner:       &taskRunnerCond{op: EQ, taskRunner: "abc"},
				CondTaskStatus:       &taskStatusCond{op: IN, taskStatus: []task.TaskStatus{task.TaskStatus_Created}},
				CondTaskParentTaskID: &taskParentTaskIDCond{op: GE, taskParentTaskID: "ab"},
				CondTaskExecutor:     &taskExecutorCond{op: LE, taskExecutor: 1},
			},
			),
			expected: " AND task_runner='abc' AND task_status IN (0) AND task_parent_id>='ab' AND task_metadata_executor<=1",
		},
	}

	for _, c := range cases {
		result := buildWhereClause(&c.condition)
		actual := strings.Split(result, " AND ")
		expected := strings.Split(c.expected, " AND ")
		slices.Sort(actual)
		slices.Sort(expected)
		assert.Equal(t, expected, actual)
	}
}
