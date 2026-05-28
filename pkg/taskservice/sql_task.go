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

import "time"

const (
	SQLTaskStatusRunning = "RUNNING"
	SQLTaskStatusSuccess = "SUCCESS"
	SQLTaskStatusFailed  = "FAILED"
	SQLTaskStatusSkipped = "SKIPPED"
	SQLTaskStatusTimeout = "TIMEOUT"

	SQLTaskTriggerScheduled = "SCHEDULED"
	SQLTaskTriggerManual    = "MANUAL"
)

const (
	sqlTaskRunDefaultStaleTimeout = 24 * time.Hour
	sqlTaskRunStaleGrace          = time.Minute
)

type SQLTask struct {
	TaskID         uint64
	TaskName       string
	AccountID      uint32
	DatabaseName   string
	CronExpr       string
	Timezone       string
	SQLBody        string
	GateCondition  string
	RetryLimit     int
	TimeoutSeconds int
	Enabled        bool
	NextFireTime   int64
	TriggerCount   uint64
	Creator        string
	CreatorUserID  uint32
	CreatorRoleID  uint32
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

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

func isStaleSQLTaskRun(sqlTask SQLTask, run SQLTaskRun, now time.Time) bool {
	if run.Status != SQLTaskStatusRunning {
		return false
	}
	if run.StartedAt.IsZero() {
		return true
	}

	timeout := sqlTaskRunDefaultStaleTimeout
	if sqlTask.TimeoutSeconds > 0 {
		timeout = time.Duration(sqlTask.TimeoutSeconds)*time.Second + sqlTaskRunStaleGrace
	}
	return !run.StartedAt.Add(timeout).After(now)
}

func markStaleSQLTaskRun(sqlTask SQLTask, run *SQLTaskRun, now time.Time) {
	run.FinishedAt = now
	if !run.StartedAt.IsZero() {
		run.DurationSeconds = now.Sub(run.StartedAt).Seconds()
	}
	if sqlTask.TimeoutSeconds > 0 {
		run.Status = SQLTaskStatusTimeout
		run.ErrorMessage = "sql task run recovered after exceeding timeout"
		return
	}
	run.Status = SQLTaskStatusFailed
	run.ErrorMessage = "sql task run recovered from stale RUNNING state"
}
