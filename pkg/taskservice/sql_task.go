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
