// Copyright 2026 Matrix Origin
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

package databranchutils

import "github.com/matrixorigin/matrixone/pkg/pb/task"

const (
	LineageGCTaskID       = "data_branch_lineage_gc"
	LineageGCTaskCronExpr = "0 */5 * * * *"
)

func LineageGCTaskMetadata() task.TaskMetadata {
	return task.TaskMetadata{
		ID:       LineageGCTaskID,
		Executor: task.TaskCode_DataBranchLineageGC,
		Options:  task.TaskOptions{Concurrency: 1},
	}
}
