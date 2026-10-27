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

// BuildTaskDetails constructs the persisted details for a CDC daemon task.
func BuildTaskDetails(accountID uint32, accountName, userName, taskName, taskID string) *task.Details {
	return &task.Details{
		AccountID: accountID,
		Account:   accountName,
		Username:  userName,
		Details: &task.Details_CreateCdc{
			CreateCdc: &task.CreateCdcDetails{
				TaskName: taskName,
				TaskId:   taskID,
				Accounts: []*task.Account{
					{
						Id:   uint64(accountID),
						Name: accountName,
					},
				},
			},
		},
	}
}

// CDCStrToTime parses a CDC task timestamp in its configured timezone or RFC3339.
func CDCStrToTime(tsStr string, tz *time.Location) (ts time.Time, err error) {
	if tsStr == "" {
		return
	}

	if tz != nil {
		if ts, err = time.ParseInLocation(
			time.DateTime, tsStr, tz,
		); err == nil {
			return
		}
	}

	ts, err = time.Parse(time.RFC3339, tsStr)
	return
}
