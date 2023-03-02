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

package metric

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"go.uber.org/zap"
)

const (
	LoggerName              = "MetricTask"
	LoggerNameMetricStorage = "MetricStorage"

	StorageUsageCronTask     = "StorageUsage"
	StorageUsageTaskCronExpr = ExprEvery05Min

	ExprEvery05Min = "0 */1 * * * *"
	ParamSeparator = " "
)

// TaskMetadata handle args like: "{db_tbl_name} [date, default: today]"
func TaskMetadata(jobName string, id task.TaskCode, args ...string) task.TaskMetadata {
	return task.TaskMetadata{
		ID:       path.Join(jobName, path.Join(args...)),
		Executor: id,
		Context:  []byte(strings.Join(args, ParamSeparator)),
		Options:  task.TaskOptions{Concurrency: 1},
	}
}

// CreateCronTask should init once in/with schema-init.
func CreateCronTask(ctx context.Context, executorID task.TaskCode, taskService taskservice.TaskService) error {
	var err error
	ctx, span := trace.Start(ctx, "MetricCreateCronTask")
	defer span.End()
	logger := runtime.ProcessLevelRuntime().Logger().WithContext(ctx).Named(LoggerName)
	logger.Info(fmt.Sprintf("init metric task with CronExpr: %s", StorageUsageTaskCronExpr))
	if err = taskService.CreateCronTask(ctx, TaskMetadata(StorageUsageCronTask, executorID), StorageUsageTaskCronExpr); err != nil {
		return err
	}
	return nil
}

// GetMetricStorageUsageExecutor collect metric server_storage_usage
func GetMetricStorageUsageExecutor(sqlExecutor func() ie.InternalExecutor) func(ctx context.Context, task task.Task) error {
	f := func(ctx context.Context, task task.Task) error {
		return CalculateStorageUsage(ctx, sqlExecutor)
	}
	return f
}

const (
	ShowAccountSQL    = "SHOW ACCOUNTS;"
	ColumnAccountName = "account_name"
	ColumnSize        = "size"
)

var gUpdateStorageUsageInterval = defaultUpdateInterval()

func defaultUpdateInterval() *atomic.Int64 {
	v := new(atomic.Int64)
	v.Store(int64(time.Minute))
	return v
}

func SetUpdateStorageUsageInterval(interval time.Duration) {
	gUpdateStorageUsageInterval.Store(int64(interval))
}

func GetUpdateStorageUsageInterval() time.Duration {
	return time.Duration(gUpdateStorageUsageInterval.Load())
}

var QuitableWait = func(ctx context.Context) (*time.Ticker, error) {
	next := time.NewTicker(GetUpdateStorageUsageInterval())
	return next, nil
}

func CalculateStorageUsage(ctx context.Context, sqlExecutor func() ie.InternalExecutor) (err error) {
	ctx, span := trace.Start(ctx, "MetricStorageUsage")
	defer span.End()
	logger := runtime.ProcessLevelRuntime().Logger().WithContext(ctx).Named(LoggerNameMetricStorage)
	defer func() {
		logger.Info("finished", zap.Error(err))
	}()

	next := time.NewTicker(time.Second)

	for {
		select {
		case <-ctx.Done():
			logger.Info("receive context signal", zap.Error(ctx.Err()))
			StorageUsageFactory.Reset() // clean CN data for next cron task.
			return ctx.Err()
		case <-next.C:
			logger.Info("start next round")
		}

		// main
		// +-----------------+------------+---------------------+--------+----------------+----------+-------------+-----------+-------+----------------+
		// | account_name    | admin_name | created             | status | suspended_time | db_count | table_count | row_count | size  | comment        |
		// +-----------------+------------+---------------------+--------+----------------+----------+-------------+-----------+-------+----------------+
		// | sys             | root       | 2023-01-17 09:56:10 | open   | NULL           |        6 |          56 |      2082 | 0.341 | system account |
		// | query_tae_table | admin      | 2023-01-17 09:56:26 | open   | NULL           |        6 |          34 |       792 | 0.036 |                |
		// +-----------------+------------+---------------------+--------+----------------+----------+-------------+-----------+-------+----------------+
		executor := sqlExecutor()
		logger.Info("query storage size")
		result := executor.Query(ctx, ShowAccountSQL, ie.NewOptsBuilder().Finish())
		err = result.Error()
		if err != nil {
			return err
		}

		cnt := result.RowCount()
		if cnt == 0 {
			next = time.NewTicker(time.Minute)
			logger.Warn("got empty account info, wait shortly")
			continue
		}
		logger.Info("collect storage_usage cnt", zap.Uint64("cnt", cnt))
		StorageUsageFactory.Reset()
		for rowIdx := uint64(0); rowIdx < result.RowCount(); rowIdx++ {

			account, err := result.StringValueByName(ctx, rowIdx, ColumnAccountName)
			if err != nil {
				return err
			}

			sizeMB, err := result.Float64ValueByName(ctx, rowIdx, ColumnSize)
			if err != nil {
				return err
			}

			logger.Info("storage_usage", zap.String("account", account), zap.Float64("sizeMB", sizeMB))
			StorageUsage(account).Set(sizeMB)
		}

		// next round
		next, err = QuitableWait(ctx)
		if err != nil {
			return err
		}
		logger.Info("wait next round")
	}
}
