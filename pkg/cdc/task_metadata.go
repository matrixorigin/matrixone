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

package cdc

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

const (
	defaultTaskMaxRetryTimes = 10
	defaultTaskRetryInterval = int64(10 * time.Second)
)

// BuildTaskMetadata constructs the CDC daemon task using the selected compatibility protocol.
func BuildTaskMetadata(taskID, extraOpts string, noFull bool) task.TaskMetadata {
	executor := task.TaskCode_InitCdc
	switch {
	case UsesGenerationAwareProtocol(extraOpts):
		executor = task.TaskCode_InitCdcSourcePatternV1
	case UsesSourcePatternProtocol(extraOpts):
		executor = task.TaskCode_InitCdcSourcePatternV1
	case noFull && UsesLosslessNoFullStart(extraOpts):
		executor = task.TaskCode_InitCdcLosslessStart
	case !noFull && UsesStableEpochInitialSnapshot(extraOpts):
		executor = task.TaskCode_InitCdcStableEpoch
	}
	return task.TaskMetadata{
		ID:       taskID,
		Executor: executor,
		Options: task.TaskOptions{
			MaxRetryTimes: defaultTaskMaxRetryTimes,
			RetryInterval: defaultTaskRetryInterval,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
}
