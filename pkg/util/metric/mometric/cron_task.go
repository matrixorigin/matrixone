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

package mometric

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/metric"

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
	ctx = defines.AttachAccount(ctx, catalog.System_Account, catalog.System_User, catalog.System_Role)
	logger := runtime.ProcessLevelRuntime().Logger().WithContext(ctx).Named(LoggerName)
	logger.Debug(fmt.Sprintf("init metric task with CronExpr: %s", StorageUsageTaskCronExpr))
	if err = taskService.CreateCronTask(ctx, TaskMetadata(StorageUsageCronTask, executorID), StorageUsageTaskCronExpr); err != nil {
		return err
	}
	return nil
}

// GetMetricStorageUsageExecutor collect metric server_storage_usage
func GetMetricStorageUsageExecutor(sqlExecutor func() ie.InternalExecutor) func(ctx context.Context, task task.Task) error {
	return func(ctx context.Context, task task.Task) error {
		return CalculateStorageUsage(ctx, sqlExecutor)
	}
}

const (
	ShowAllAccountSQL = "SHOW ACCOUNTS;"
	ShowAccountSQL    = "SHOW ACCOUNTS like %q;"
	ColumnAccountName = "account_name" // result column in `show accounts`, or column in table mo_catalog.mo_account
	ColumnSize        = "size"         // result column in `show accounts`, or column in table mo_catalog.mo_account
	ColumnCreatedTime = "created_time" // column in table mo_catalog.mo_account
	ColumnStatus      = "status"       // column in table mo_catalog.mo_account
)

var (
	gUpdateStorageUsageInterval atomic.Int64
	gCheckNewInterval           atomic.Int64
)

func init() {
	gUpdateStorageUsageInterval.Store(int64(time.Minute))
	gCheckNewInterval.Store(int64(time.Minute))
}

func SetUpdateStorageUsageInterval(interval time.Duration) {
	gUpdateStorageUsageInterval.Store(int64(interval))
}

func GetUpdateStorageUsageInterval() time.Duration {
	return time.Duration(gUpdateStorageUsageInterval.Load())
}

func cleanStorageUsageMetric(logger *log.MOLogger, actor string) {
	// clean metric data for next cron task.
	metric.StorageUsageFactory.Reset()
	logger.Info("clean storage usage metric", zap.String("actor", actor))
}

func CalculateStorageUsage(ctx context.Context, sqlExecutor func() ie.InternalExecutor) (err error) {
	ctx, span := trace.Start(ctx, "MetricStorageUsage")
	defer span.End()
	ctx = defines.AttachAccount(ctx, catalog.System_Account, catalog.System_User, catalog.System_Role)
	ctx, cancel := context.WithCancel(ctx)
	logger := runtime.ProcessLevelRuntime().Logger().WithContext(ctx).Named(LoggerNameMetricStorage)
	logger.Info("started")
	defer func() {
		logger.Info("finished", zap.Error(err))
		cleanStorageUsageMetric(logger, "CalculateStorageUsage")
		// quit CheckNewAccountSize goroutine
		cancel()
	}()

	// start background task to check new account
	go checkNewAccountSize(ctx, logger, sqlExecutor)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	queryOpts := ie.NewOptsBuilder().Database(MetricDBConst).Internal(true).Finish()
	for {
		select {
		case <-ctx.Done():
			logger.Info("receive context signal", zap.Error(ctx.Err()))
			return ctx.Err()
		case <-ticker.C:
			logger.Info("start next round")
		}

		if !IsEnable() {
			logger.Debug("mometric is disable.")
			continue
		}

		// mysql> show accounts;
		// +-----------------+------------+---------------------+--------+----------------+----------+-------------+-----------+-------+----------------+
		// | account_name    | admin_name | created             | status | suspended_time | db_count | table_count | row_count | size  | comment        |
		// +-----------------+------------+---------------------+--------+----------------+----------+-------------+-----------+-------+----------------+
		// | sys             | root       | 2023-01-17 09:56:10 | open   | NULL           |        6 |          56 |      2082 | 0.341 | system account |
		// | query_tae_table | admin      | 2023-01-17 09:56:26 | open   | NULL           |        6 |          34 |       792 | 0.036 |                |
		// +-----------------+------------+---------------------+--------+----------------+----------+-------------+-----------+-------+----------------+
		logger.Debug("query storage size")
		showAccounts := func(ctx context.Context) ie.InternalExecResult {
			ctx, spanQ := trace.Start(ctx, "QueryStorageStorage", trace.WithHungThreshold(time.Minute))
			defer spanQ.End()
			return sqlExecutor().Query(ctx, ShowAllAccountSQL, queryOpts)
		}
		result := showAccounts(ctx)
		err = result.Error()
		if err != nil {
			return err
		}

		cnt := result.RowCount()
		if cnt == 0 {
			ticker.Reset(time.Minute)
			logger.Warn("got empty account info, wait shortly")
			continue
		}
		logger.Debug("collect storage_usage cnt", zap.Uint64("cnt", cnt))
		metric.StorageUsageFactory.Reset()
		for rowIdx := uint64(0); rowIdx < cnt; rowIdx++ {
			account, err := result.StringValueByName(ctx, rowIdx, ColumnAccountName)
			if err != nil {
				return err
			}
			sizeMB, err := result.Float64ValueByName(ctx, rowIdx, ColumnSize)
			if err != nil {
				return err
			}
			logger.Debug("storage_usage", zap.String("account", account), zap.Float64("sizeMB", sizeMB))

			metric.StorageUsage(account).Set(sizeMB)
		}

		// next round
		ticker.Reset(GetUpdateStorageUsageInterval())
		logger.Info("wait next round")
	}
}

func SetStorageUsageCheckNewInterval(interval time.Duration) {
	gCheckNewInterval.Store(int64(interval))
}

func GetStorageUsageCheckNewInterval() time.Duration {
	return time.Duration(gCheckNewInterval.Load())
}

func checkNewAccountSize(ctx context.Context, logger *log.MOLogger, sqlExecutor func() ie.InternalExecutor) {
	var err error
	ctx, span := trace.Start(ctx, "checkNewAccountSize")
	defer span.End()
	logger = logger.WithContext(ctx)
	logger.Info("checkNewAccountSize started")
	defer func() {
		logger.Info("checkNewAccountSize exit", zap.Error(err))
	}()

	if !IsEnable() {
		logger.Info("mometric is disable.")
		return
	}
	opts := ie.NewOptsBuilder().Database(MetricDBConst).Internal(true).Finish()

	var now time.Time
	var interval = GetStorageUsageCheckNewInterval()
	var next = time.NewTicker(interval)
	var lastCheckTime = time.Now().Add(-time.Second)
	var newAccountCnt uint64
	for {
		select {
		case <-ctx.Done():
			logger.Info("receive context signal", zap.Error(ctx.Err()))
			cleanStorageUsageMetric(logger, "checkNewAccountSize")
			return
		case now = <-next.C:
			logger.Debug("start check new account")
		}

		// mysql> select * from mo_catalog.mo_account;
		// +------------+--------------+--------+---------------------+----------------+---------+----------------+
		// | account_id | account_name | status | created_time        | comments       | version | suspended_time |
		// +------------+--------------+--------+---------------------+----------------+---------+----------------+
		// |          0 | sys          | open   | 2023-05-09 04:34:57 | system account |       1 | NULL           |
		// +------------+--------------+--------+---------------------+----------------+---------+----------------+
		executor := sqlExecutor()
		// tips: created_time column, in table mo_catalog.mo_account, always use UTC timestamp.
		// more details in pkg/frontend/authenticate.go, function frontend.createTablesInMoCatalog
		sql := fmt.Sprintf("select account_name, created_time from mo_catalog.mo_account where created_time >= %q;",
			table.Time2DatetimeString(lastCheckTime.UTC()))
		getNewAccounts := func(ctx context.Context, sql string, lastCheck, now time.Time) ie.InternalExecResult {
			ctx, spanQ := trace.Start(ctx, "QueryStorageStorage.getNewAccounts")
			defer spanQ.End()
			spanQ.AddExtraFields(zap.Time("last_check_time", lastCheck))
			spanQ.AddExtraFields(zap.Time("now", now))
			logger.Debug("query new account", zap.String("sql", sql))
			return executor.Query(ctx, sql, opts)
		}
		result := getNewAccounts(ctx, sql, lastCheckTime, now)
		lastCheckTime = now
		err = result.Error()
		if err != nil {
			logger.Error("failed to fetch new created account", zap.Error(err), zap.String("sql", sql))
			goto nextL
		}

		newAccountCnt = result.RowCount()
		if newAccountCnt == 0 {
			logger.Debug("got empty new account info, wait next round")
			goto nextL
		}
		logger.Debug("collect new account cnt", zap.Uint64("cnt", newAccountCnt))
		for rowIdx := uint64(0); rowIdx < result.RowCount(); rowIdx++ {

			account, err := result.StringValueByName(ctx, rowIdx, ColumnAccountName)
			if err != nil {
				continue
			}

			createdTime, err := result.StringValueByName(ctx, rowIdx, ColumnCreatedTime)
			if err != nil {
				continue
			}

			// query single account's storage
			showSql := fmt.Sprintf(ShowAccountSQL, account)
			getOneAccount := func(ctx context.Context, sql string) ie.InternalExecResult {
				ctx, spanQ := trace.Start(ctx, "QueryStorageStorage.getOneAccount")
				defer spanQ.End()
				spanQ.AddExtraFields(zap.String("account", account))
				logger.Debug("query one account", zap.String("sql", sql))
				return executor.Query(ctx, sql, opts)
			}
			showRet := getOneAccount(ctx, showSql)
			err = showRet.Error()
			if err != nil {
				logger.Error("failed to exec query sql",
					zap.Error(err), zap.String("account", account), zap.String("sql", showSql))
				continue
			}

			if result.RowCount() == 0 {
				logger.Warn("failed to fetch new account size, not exist.")
				continue
			}
			sizeMB, err := showRet.Float64ValueByName(ctx, 0, ColumnSize)
			if err != nil {
				logger.Error("failed to fetch new account size", zap.Error(err), zap.String("account", account))
				continue
			}
			// done query.

			// update new accounts metric
			logger.Info("new account storage_usage", zap.String("account", account), zap.Float64("sizeMB", sizeMB),
				zap.String("created_time", createdTime))

			metric.StorageUsage(account).Set(sizeMB)
		}

	nextL:
		// reset next Round
		next.Reset(GetStorageUsageCheckNewInterval())
		logger.Debug("wait next round, check new account")
	}
}
