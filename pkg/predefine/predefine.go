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

package predefine

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	idxcron "github.com/matrixorigin/matrixone/pkg/vectorindex/cron"
	"github.com/robfig/cron/v3"
)

const (
	sysAccountID   = 0
	sysAccountName = "sys"
)

// genInitCronTaskSQL Generate `insert` statement for creating system cron tasks, which works on the `mo_task`.`sys_cron_task` table.
func GenInitCronTaskSQL(codes ...int32) (string, error) {
	cronParser := cron.NewParser(
		cron.Second |
			cron.Minute |
			cron.Hour |
			cron.Dom |
			cron.Month |
			cron.Dow |
			cron.Descriptor)

	createCronTask := func(value task.TaskMetadata, cronExpr string) (*task.CronTask, error) {
		sche, err := cronParser.Parse(cronExpr)
		if err != nil {
			return nil, err
		}

		now := time.Now().UnixMilli()
		next := sche.Next(time.UnixMilli(now))

		return &task.CronTask{
			Metadata:     value,
			CronExpr:     cronExpr,
			NextTime:     next.UnixMilli(),
			TriggerTimes: 0,
			CreateAt:     now,
			UpdateAt:     now,
		}, nil
	}

	cronTasks := make([]*task.CronTask, 0, 4)
	task1, err := createCronTask(export.MergeTaskMetadata(task.TaskCode_MetricLogMerge), export.MergeTaskCronExprEvery05Min)
	if err != nil {
		return "", err
	}
	cronTasks = append(cronTasks, task1)

	task2, err := createCronTask(mometric.TaskMetadata(mometric.StorageUsageCronTask, task.TaskCode_MetricStorageUsage), mometric.StorageUsageTaskCronExpr)
	if err != nil {
		return "", err
	}
	cronTasks = append(cronTasks, task2)

	// task3 is deleted already

	task4, err := createCronTask(
		task.TaskMetadata{
			ID:       "mo_table_stats",
			Executor: task.TaskCode_MOTableStats,
			Options:  task.TaskOptions{Concurrency: 1},
		}, export.MergeTaskCronExprEveryMin)
	if err != nil {
		return "", err
	}
	cronTasks = append(cronTasks, task4)

	task5, err := createCronTask(idxcron.IndexUpdateTaskMetadata(task.TaskCode_IndexUpdateTaskExecutor), idxcron.IndexUpdateTaskCronExpr)
	if err != nil {
		return "", err
	}
	cronTasks = append(cronTasks, task5)

	sql := fmt.Sprintf(`insert into %s.sys_cron_task (
                           task_metadata_id,
						   task_metadata_executor,
                           task_metadata_context,
                           task_metadata_option,
                           cron_expr,
                           next_time,
                           trigger_times,
                           create_at,
                           update_at
                    ) values `, catalog.MOTaskDB)

	first := true
	for _, t := range cronTasks {
		if len(codes) != 0 && slices.Index(codes, int32(t.Metadata.Executor)) == -1 {
			// if the task codes specified, only process them.
			continue
		}

		if len(codes) == 0 && t.Metadata.Executor == task.TaskCode_MOTableStats {
			// test code to test if the init mo_table_stats task meta code works.
			continue
		}

		j, err := json.Marshal(t.Metadata.Options)
		if err != nil {
			return "", err
		}
		if first {
			first = false
			sql += fmt.Sprintf("('%s' ,%d ,'%s' ,'%s' ,'%s' ,%d ,%d ,%d ,%d)",
				t.Metadata.ID,
				t.Metadata.Executor,
				t.Metadata.Context,
				string(j),
				t.CronExpr,
				t.NextTime,
				t.TriggerTimes,
				t.CreateAt,
				t.UpdateAt)
		} else {
			sql += fmt.Sprintf(",('%s' ,%d ,'%s' ,'%s' ,'%s' ,%d ,%d ,%d ,%d)",
				t.Metadata.ID,
				t.Metadata.Executor,
				t.Metadata.Context,
				string(j),
				t.CronExpr,
				t.NextTime,
				t.TriggerTimes,
				t.CreateAt,
				t.UpdateAt)
		}
	}
	return sql, nil
}
func GenISCPTaskCheckSQL() string {
	return fmt.Sprintf("select * from %s.sys_daemon_task where task_metadata_executor = %d", catalog.MOTaskDB, task.TaskCode_ISCPExecutor)
}

func GenInitISCPTaskSQL() string {
	option := task.TaskOptions{
		MaxRetryTimes: 10,
		RetryInterval: int64(time.Second * 10),
		DelayDuration: 0,
		Concurrency:   0,
	}
	j, err := json.Marshal(option)
	if err != nil {
		panic(err)
	}
	taskID := uuid.Must(uuid.NewV7())
	details := &task.Details{
		AccountID: sysAccountID,
		Account:   sysAccountName,
		Details: &task.Details_ISCP{
			ISCP: &task.ISCPDetails{
				TaskName: "iscp",
				TaskId:   taskID.String(),
			},
		},
	}
	detailStr, err := details.Marshal()
	if err != nil {
		panic(err)
	}
	sql := fmt.Sprintf(
		`INSERT INTO %s.sys_daemon_task (
			task_metadata_id,
			task_metadata_executor,
			task_metadata_context,
			task_metadata_option,
			account_id,
			account,
			task_type,
			task_status,
			create_at,
			update_at,
			details
		)
		SELECT 
			'%s' AS task_metadata_id,
			%d AS task_metadata_executor,
			NULL AS task_metadata_context,
			'%s' AS task_metadata_option,
			%d AS account_id,
			'%s' AS account,
			'%s' AS task_type,
			0 AS task_status,
			NOW() AS create_at,
			NOW() AS update_at,
			'%s' AS details
		FROM DUAL
		WHERE NOT EXISTS (
			SELECT 1 FROM mo_task.sys_daemon_task WHERE task_metadata_executor = %d
		);`,
		catalog.MOTaskDB,
		taskID.String(),
		task.TaskCode_ISCPExecutor,
		string(j),
		sysAccountID,
		sysAccountName,
		task.TaskType_ISCP.String(),
		detailStr,
		task.TaskCode_ISCPExecutor,
	)
	return sql
}
