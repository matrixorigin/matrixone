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
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

var (
	ErrSQLTaskOverlap  = moerr.NewInternalErrorNoCtx("sql task is already running")
	ErrSQLTaskNotFound = moerr.NewInternalErrorNoCtx("sql task not found")
)

func BuildSQLTaskCronSpec(cronExpr, timezone string) string {
	if cronExpr == "" {
		return ""
	}
	if timezone == "" {
		return cronExpr
	}
	return fmt.Sprintf("CRON_TZ=%s %s", timezone, cronExpr)
}

func ParseSQLTaskSchedule(parser cron.Parser, cronExpr, timezone string) (cron.Schedule, error) {
	return parser.Parse(BuildSQLTaskCronSpec(cronExpr, timezone))
}

func NextSQLTaskFireTime(parser cron.Parser, cronExpr, timezone string, now time.Time) (int64, error) {
	if cronExpr == "" {
		return 0, nil
	}
	schedule, err := ParseSQLTaskSchedule(parser, cronExpr, timezone)
	if err != nil {
		return 0, err
	}
	return schedule.Next(now).UnixMilli(), nil
}

func ParseSQLTaskTimeout(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, moerr.NewInvalidArgNoCtx("timeout", value)
	}
	return int(d / time.Second), nil
}

func DefinerOpts(sqlTask SQLTask) ie.SessionOverrideOptions {
	builder := ie.NewOptsBuilder().
		AccountId(sqlTask.AccountID).
		Internal(true)

	if sqlTask.DatabaseName != "" {
		builder.Database(sqlTask.DatabaseName)
	}
	if sqlTask.Creator != "" {
		builder.Username(sqlTask.Creator)
	}
	if sqlTask.CreatorUserID != 0 {
		builder.UserId(sqlTask.CreatorUserID)
	}
	if sqlTask.CreatorRoleID != 0 {
		builder.DefaultRoleId(sqlTask.CreatorRoleID)
	}
	return builder.Finish()
}

func BuildSQLTaskMetadata(ctx *task.SQLTaskContext) task.TaskMetadata {
	data, _ := ctx.Marshal()
	return task.TaskMetadata{
		ID:       fmt.Sprintf("sql-task:%d:%d", ctx.TaskId, ctx.TriggerCount),
		Executor: task.TaskCode_SQLTask,
		Context:  data,
		Options: task.TaskOptions{
			MaxRetryTimes: 0,
			RetryInterval: 0,
			Concurrency:   1,
		},
	}
}
