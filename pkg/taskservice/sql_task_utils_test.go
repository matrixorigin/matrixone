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
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

func TestSQLTaskScheduleAndTimeoutUtils(t *testing.T) {
	parser := cron.NewParser(
		cron.Second |
			cron.Minute |
			cron.Hour |
			cron.Dom |
			cron.Month |
			cron.Dow |
			cron.Descriptor,
	)
	now := time.Date(2026, 4, 11, 8, 0, 0, 0, time.UTC)

	require.Equal(t, "", BuildSQLTaskCronSpec("", "UTC"))
	require.Equal(t, "0 * * * * *", BuildSQLTaskCronSpec("0 * * * * *", ""))
	require.Equal(t, "CRON_TZ=UTC 0 * * * * *", BuildSQLTaskCronSpec("0 * * * * *", "UTC"))

	next, err := NextSQLTaskFireTime(parser, "", "UTC", now)
	require.NoError(t, err)
	require.Equal(t, int64(0), next)

	next, err = NextSQLTaskFireTime(parser, "0 * * * * *", "UTC", now)
	require.NoError(t, err)
	require.Equal(t, now.Add(time.Minute).UnixMilli(), next)

	_, err = ParseSQLTaskSchedule(parser, "bad cron", "")
	require.Error(t, err)

	timeout, err := ParseSQLTaskTimeout(" ")
	require.NoError(t, err)
	require.Equal(t, 0, timeout)
	timeout, err = ParseSQLTaskTimeout("1500ms")
	require.NoError(t, err)
	require.Equal(t, 1, timeout)
	_, err = ParseSQLTaskTimeout("bad")
	require.Error(t, err)
	_, err = ParseSQLTaskTimeout("-1s")
	require.Error(t, err)
}

func TestSQLTaskDefinerOptsAndMetadata(t *testing.T) {
	opts := DefinerOpts(SQLTask{AccountID: 7})
	require.NotNil(t, opts.AccountId)
	require.Equal(t, uint32(7), *opts.AccountId)
	require.NotNil(t, opts.IsInternal)
	require.True(t, *opts.IsInternal)
	require.Nil(t, opts.Database)
	require.Nil(t, opts.Username)
	require.Nil(t, opts.UserId)
	require.Nil(t, opts.DefaultRoleId)

	opts = DefinerOpts(SQLTask{
		AccountID:     7,
		DatabaseName:  "db1",
		Creator:       "u1",
		CreatorUserID: 11,
		CreatorRoleID: 22,
	})
	require.Equal(t, "db1", *opts.Database)
	require.Equal(t, "u1", *opts.Username)
	require.Equal(t, uint32(11), *opts.UserId)
	require.Equal(t, uint32(22), *opts.DefaultRoleId)

	ctx := &task.SQLTaskContext{
		TaskId:       42,
		TaskName:     "task_a",
		AccountId:    7,
		TriggerCount: 3,
		SQLBody:      "select 1",
	}
	metadata := BuildSQLTaskMetadata(ctx)
	require.Equal(t, "sql-task:42:3", metadata.ID)
	require.Equal(t, task.TaskCode_SQLTask, metadata.Executor)
	require.Equal(t, uint32(1), metadata.Options.Concurrency)

	var decoded task.SQLTaskContext
	require.NoError(t, decoded.Unmarshal(metadata.Context))
	require.Equal(t, ctx.TaskId, decoded.TaskId)
	require.Equal(t, ctx.TaskName, decoded.TaskName)
	require.Equal(t, ctx.SQLBody, decoded.SQLBody)
}
