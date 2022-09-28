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
			condition: conditions{
				hasTaskIDCond: true,
				taskIDOp:      EQ,
				taskID:        1,
			},
			expected: "task_id=1",
		},
		{
			condition: conditions{
				hasTaskIDCond:       true,
				taskIDOp:            EQ,
				taskID:              1,
				hasTaskRunnerCond:   true,
				taskRunnerOp:        EQ,
				taskRunner:          "abc",
				hasTaskStatusCond:   true,
				taskStatusOp:        EQ,
				taskStatus:          task.TaskStatus_Created,
				hasTaskEpochCond:    true,
				taskEpochOp:         LE,
				taskEpoch:           100,
				hasTaskParentIDCond: true,
				taskParentTaskIDOp:  GE,
				taskParentTaskID:    "ab",
			},
			expected: "task_id=1 AND task_runner='abc' AND task_status=0 AND task_epoch<=100 AND task_parent_id>='ab'",
		},
		{
			condition: conditions{
				hasTaskRunnerCond:   true,
				taskRunnerOp:        EQ,
				taskRunner:          "abc",
				hasTaskStatusCond:   true,
				taskStatusOp:        EQ,
				taskStatus:          task.TaskStatus_Created,
				hasTaskParentIDCond: true,
				taskParentTaskIDOp:  GE,
				taskParentTaskID:    "ab",
			},
			expected: "task_runner='abc' AND task_status=0 AND task_parent_id>='ab'",
		},
	}

	for _, c := range cases {
		result := buildWhereClause(c.condition)
		assert.Equal(t, c.expected, result)
	}
}
