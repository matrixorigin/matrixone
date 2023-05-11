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
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
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
	ShowAllAccountSQL = "SHOW ACCOUNTS;"
	ShowAccountSQL    = "SHOW ACCOUNTS like %q;"
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

	go CheckNewAccountSize(ctx, logger, sqlExecutor)

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
		result := executor.Query(ctx, ShowAllAccountSQL, ie.NewOptsBuilder().Finish())
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

func CheckNewAccountSize(ctx context.Context, logger *log.MOLogger, sqlExecutor func() ie.InternalExecutor) {
	var err error
	ctx, span := trace.Start(ctx, "CheckNewAccountSize")
	defer span.End()
	defer func() {
		logger.Info("CheckNewAccountSize exit", zap.Error(err))
	}()

	const ColumnAccountName = "account_name"
	const ColumnCreatedTime = "created_time"
	const ColumnStatus = "status"

	opts := ie.NewOptsBuilder().Finish()

	var now time.Time
	var interval = time.Minute
	var next = time.NewTicker(interval)
	var lastCheckTime = time.Now().Add(-time.Second)
	for {
		select {
		case <-ctx.Done():
			logger.Info("receive context signal", zap.Error(ctx.Err()))
			return
		case now = <-next.C:
			logger.Info("start check account")
		}

		// mysql> select * from mo_catalog.mo_account;
		// +------------+--------------+--------+---------------------+----------------+---------+----------------+
		// | account_id | account_name | status | created_time        | comments       | version | suspended_time |
		// +------------+--------------+--------+---------------------+----------------+---------+----------------+
		// |          0 | sys          | open   | 2023-05-09 04:34:57 | system account |       1 | NULL           |
		// +------------+--------------+--------+---------------------+----------------+---------+----------------+
		executor := sqlExecutor()
		logger.Info("query new account")
		now := time.Now()
		sql := fmt.Sprintf("select account_name, created_time, status from mo_account "+
			"where create_time >= %q;",
			table.Time2DatetimeString(lastCheckTime))
		lastCheckTime = now
		result := executor.Query(ctx, sql, opts)
		err = result.Error()
		if err != nil {
			logger.Error("failed to fetch new created account", zap.Error(err))
			continue
		}

		cnt := result.RowCount()
		if cnt == 0 {
			logger.Warn("got empty new account info, wait next round")
			continue
		}
		logger.Info("collect new account cnt", zap.Uint64("cnt", cnt))
		for rowIdx := uint64(0); rowIdx < result.RowCount(); rowIdx++ {

			account, err := result.StringValueByName(ctx, rowIdx, ColumnAccountName)
			if err != nil {
				continue
			}

			createdTime, err := result.StringValueByName(ctx, rowIdx, ColumnCreatedTime)
			if err != nil {
				continue
			}

			showSql := fmt.Sprintf(ShowAccountSQL, account)
			showRet := executor.Query(ctx, showSql, opts)
			err = showRet.Error()
			if err != nil {
				logger.Error("failed to fetch account size", zap.Error(err), zap.String("account", account))
				continue
			}

			sizeMB, err := result.Float64ValueByName(ctx, rowIdx, ColumnSize)
			if err != nil {
				continue
			}

			logger.Info("storage_usage", zap.String("account", account), zap.Float64("sizeMB", sizeMB),
				zap.String("create_time", createdTime))
			StorageUsage(account).Set(sizeMB)
		}

		// calculate next Round time.
		next.Reset(interval)
	}

}
