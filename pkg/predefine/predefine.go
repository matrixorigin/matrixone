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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"github.com/robfig/cron/v3"
)

// genInitCronTaskSQL Generate `insert` statement for creating system cron tasks, which works on the `mo_task`.`sys_cron_task` table.
func GenInitCronTaskSQL() (string, error) {
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

	cronTasks := make([]*task.CronTask, 0, 2)
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

	for i, t := range cronTasks {
		j, err := json.Marshal(t.Metadata.Options)
		if err != nil {
			return "", err
		}
		if i == 0 {
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
