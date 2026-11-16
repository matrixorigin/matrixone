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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

const (
	SQLTaskStatusRunning = "RUNNING"
	SQLTaskStatusSuccess = "SUCCESS"
	SQLTaskStatusFailed  = "FAILED"
	SQLTaskStatusSkipped = "SKIPPED"
	SQLTaskStatusTimeout = "TIMEOUT"
	SQLTaskStatusUnknown = "UNKNOWN"

	SQLTaskTriggerScheduled = "SCHEDULED"
	SQLTaskTriggerManual    = "MANUAL"
)

type SQLTask struct {
	TaskID             uint64
	TaskName           string
	AccountID          uint32
	DatabaseName       string
	CronExpr           string
	Timezone           string
	SQLBody            string
	GateCondition      string
	RetryLimit         int
	TimeoutSeconds     int
	Enabled            bool
	NextFireTime       int64
	TriggerCount       uint64
	ScheduleGeneration uint64
	Creator            string
	CreatorUserID      uint32
	CreatorRoleID      uint32
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type SQLTaskAlterAction uint8

const (
	SQLTaskSuspend SQLTaskAlterAction = iota
	SQLTaskResume
	SQLTaskSetSchedule
	SQLTaskSetGate
	SQLTaskSetRetry
	SQLTaskSetTimeout
)

type SQLTaskRun struct {
	RunID           uint64
	TaskID          uint64
	TaskName        string
	AccountID       uint32
	ScheduledAt     time.Time
	StartedAt       time.Time
	FinishedAt      time.Time
	DurationSeconds float64
	Status          string
	TriggerType     string
	AttemptNumber   int
	RowsAffected    int64
	ErrorCode       int
	ErrorMessage    string
	GateResult      bool
	RunnerCN        string
}

// SQLTaskRunAdmission carries invocation-local ownership and capacity, not
// persisted run data. Reserve is called once while the definition is locked.
type SQLTaskRunAdmission struct {
	AsyncTaskID     uint64
	RunnerCN        string
	Epoch           uint32
	ParentTaskID    string
	MetadataID      string
	TriggerSequence uint64
	Reserve         func(uint32, string) bool
}

func validateSQLTaskAdmission(definition SQLTask, run SQLTaskRun, admission *SQLTaskRunAdmission) error {
	if admission == nil || admission.Reserve == nil {
		return moerr.NewInvalidInputNoCtx("SQL task admission requires a recovery reservation")
	}
	if run.TaskID != definition.TaskID || run.AccountID != definition.AccountID || run.TaskName != definition.TaskName {
		return moerr.NewInvalidInputNoCtx("SQL task run identity differs from its definition")
	}
	if run.TriggerType != SQLTaskTriggerManual && run.TriggerType != SQLTaskTriggerScheduled {
		return moerr.NewInvalidInputNoCtx("unknown SQL task trigger type")
	}
	if run.TriggerType == SQLTaskTriggerScheduled && admission.AsyncTaskID == 0 ||
		run.TriggerType == SQLTaskTriggerManual && admission.AsyncTaskID != 0 {
		return ErrSQLTaskStaleFiring
	}
	return nil
}

func validateSQLTaskClaim(definition SQLTask, run SQLTaskRun, async task.AsyncTask, pointer uint64, admission *SQLTaskRunAdmission) error {
	if pointer != 0 {
		return ErrSQLTaskClaimed
	}
	if async.ID != admission.AsyncTaskID || async.Status != task.TaskStatus_Running ||
		async.TaskRunner != admission.RunnerCN || async.Epoch != admission.Epoch ||
		async.ParentTaskID != admission.ParentTaskID || async.Metadata.ID != admission.MetadataID ||
		async.Metadata.Executor != task.TaskCode_SQLTask || run.RunnerCN != admission.RunnerCN ||
		admission.ParentTaskID != fmt.Sprintf("sql-task:%d", definition.TaskID) ||
		validateSQLTaskTriggerSequence(admission.TriggerSequence) != nil ||
		admission.MetadataID != fmt.Sprintf("sqltask2:%d:%020d", definition.TaskID, admission.TriggerSequence) ||
		!definition.Enabled || admission.TriggerSequence < definition.ScheduleGeneration || admission.TriggerSequence > definition.TriggerCount {
		return ErrSQLTaskStaleFiring
	}
	return nil
}
