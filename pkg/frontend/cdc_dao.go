// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
)

const (
	ExecutorType_BGExecutor  = 1
	ExecutorType_SQLExecutor = 2
)

type CDCDaoOption func(*CDCDao)

func WithBGExecutor(bgExecutor BackgroundExec) CDCDaoOption {
	return func(t *CDCDao) {
		t.bgExecutor = bgExecutor
	}
}

func WithSQLExecutor(sqlExecutor taskservice.SqlExecutor) CDCDaoOption {
	return func(t *CDCDao) {
		t.sqlExecutor = sqlExecutor
	}
}

type CDCDao struct {
	ses         *Session
	ts          taskservice.TaskService
	bgExecutor  BackgroundExec
	sqlExecutor taskservice.SqlExecutor
}

type DeleteCDCArtifactsResult struct {
	TaskRows       int64
	WatermarkRows  int64
	LocalTxnOpened bool
}

func NewCDCDao(
	ses *Session,
	opts ...CDCDaoOption,
) (t CDCDao) {
	t.ses = ses
	for _, opt := range opts {
		opt(&t)
	}
	return
}

func (t *CDCDao) MustGetTaskService() taskservice.TaskService {
	if t.ts == nil && t.ses != nil {
		t.ts = getPu(t.ses.GetService()).TaskService
	}
	if t.ts == nil {
		panic("taskService is nil")
	}
	return t.ts
}

func (t *CDCDao) MustGetBGExecutor(ctx context.Context) BackgroundExec {
	if t.bgExecutor == nil && t.ses != nil {
		t.bgExecutor = t.ses.GetBackgroundExec(ctx)
	}
	if t.bgExecutor == nil {
		panic("bgExecutor is nil")
	}
	return t.bgExecutor
}

func (t *CDCDao) MustGetSQLExecutor(ctx context.Context) taskservice.SqlExecutor {
	if t.sqlExecutor == nil {
		panic("sqlExecutor is nil")
	}
	return t.sqlExecutor
}
func (t *CDCDao) BuildCreateOpts(
	ctx context.Context, req *CDCCreateTaskRequest,
) (opts CDCCreateTaskOptions, err error) {
	err = opts.ValidateAndFill(ctx, t.ses, req)
	return
}

func (t *CDCDao) CreateTask(
	ctx context.Context,
	req *CDCCreateTaskRequest,
) (err error) {
	var opts CDCCreateTaskOptions
	if opts, err = t.BuildCreateOpts(ctx, req); err != nil {
		return
	}

	var (
		details *task.Details
	)
	if details, err = opts.BuildTaskDetails(); err != nil {
		return
	}

	creatTaskJob := func(
		ctx context.Context,
		tx taskservice.SqlExecutor,
	) (ret int, err error) {
		var (
			insertSql    string
			rowsAffected int64
		)
		if insertSql, err = opts.ToInsertTaskSQL(ctx, tx, t.ses.GetService()); err != nil {
			return
		}
		if rowsAffected, err = ExecuteAndGetRowsAffected(ctx, tx, insertSql); err != nil {
			return
		}
		return int(rowsAffected), nil
	}

	_, err = t.MustGetTaskService().AddCDCTask(
		ctx, opts.BuildTaskMetadata(), details, creatTaskJob,
	)
	return
}

func (t *CDCDao) ShowTasks(
	ctx context.Context,
	req *CDCShowTaskRequest,
) (err error) {

	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	var (
		taskId       string
		taskName     string
		sourceUri    string
		sinkUri      string
		state        string
		errMsg       string
		watermarkStr string
		executor     = t.MustGetBGExecutor(ctx)
		pu           = getPu(t.ses.GetService())
	)
	defer executor.Close()

	resultSet := GetCDCShowOutputResultSet()
	t.ses.SetMysqlResultSet(resultSet)

	var (
		snapTS        string
		txnOp         client.TxnOperator
		execResultSet []ExecResult
		queryAttrs    = cdc.CDCSQLTemplates[cdc.CDCShowTaskSqlTemplate_Idx].OutputAttrs
	)
	if txnOp, err = cdc.GetTxnOp(
		ctx,
		pu.StorageEngine,
		pu.TxnClient,
		"cdc-handleShowCdc",
	); err != nil {
		return
	}
	defer func() {
		cdc.FinishTxnOp(ctx, err, txnOp, pu.StorageEngine)
	}()

	snapTS = txnOp.SnapshotTS().ToStdTime().In(time.Local).String()
	startTS := types.TimestampToTS(txnOp.SnapshotTS())
	logutil.Debug(
		"cdc.frontend.dao.show_tasks_snapshot",
		zap.String("snapshot-ts", startTS.ToString()),
	)
	sql := cdc.CDCSQLBuilder.ShowTaskSQL(
		uint64(t.ses.GetTenantInfo().GetTenantID()),
		req.Option.All,
		string(req.Option.TaskName),
	)

	executor.ClearExecResultSet()
	if err = executor.Exec(ctx, sql); err != nil {
		return
	}

	if execResultSet, err = getResultSet(ctx, executor); err != nil {
		return
	}

	logutil.Debug(
		"cdc.frontend.dao.show_tasks_result_length",
		zap.Int("result-set-length", len(execResultSet)),
	)
	for _, result := range execResultSet {
		logutil.Debug(
			"cdc.frontend.dao.show_tasks_row_count",
			zap.Int("row-count", int(result.GetRowCount())),
		)
		for rowIdx, rowCnt := uint64(0), result.GetRowCount(); rowIdx < rowCnt; rowIdx++ {
			for colIdx, colName := range queryAttrs {
				switch colName {
				case "task_id":
					if taskId, err = result.GetString(ctx, rowIdx, uint64(colIdx)); err != nil {
						return
					}
				case "task_name":
					if taskName, err = result.GetString(ctx, rowIdx, uint64(colIdx)); err != nil {
						return
					}
				case "source_uri":
					if sourceUri, err = result.GetString(ctx, rowIdx, uint64(colIdx)); err != nil {
						return
					}
				case "sink_uri":
					if sinkUri, err = result.GetString(ctx, rowIdx, uint64(colIdx)); err != nil {
						return
					}
				case "state":
					if state, err = result.GetString(ctx, rowIdx, uint64(colIdx)); err != nil {
						return
					}
				case "err_msg":
					if errMsg, err = result.GetString(ctx, rowIdx, uint64(colIdx)); err != nil {
						return
					}
				}
			}
			var uriInfo cdc.UriInfo
			if err = cdc.JsonDecode(sourceUri, &uriInfo); err != nil {
				return
			}
			if err = cdc.JsonDecode(sinkUri, &uriInfo); err != nil {
				return
			}

			if watermarkStr, err = t.GetTaskWatermark(
				ctx,
				uint64(t.ses.GetTenantInfo().GetTenantID()),
				taskId,
			); err != nil {
				return
			}

			resultSet.AddRow([]interface{}{
				taskId,
				taskName,
				sourceUri,
				sinkUri,
				state,
				errMsg,
				watermarkStr,
				snapTS,
			})
		}
	}

	return
}

func (t *CDCDao) GetTaskWatermark(
	ctx context.Context,
	accountId uint64,
	taskId string,
) (res string, err error) {
	var (
		executor     = t.MustGetBGExecutor(ctx)
		queryAttrs   = cdc.CDCSQLTemplates[cdc.CDCGetWatermarkSqlTemplate_Idx].OutputAttrs
		watermarkStr string
		errMsg       string
		dbName       string
		tableName    string
	)

	sql := cdc.CDCSQLBuilder.GetWatermarkSQL(accountId, taskId)
	executor.ClearExecResultSet()
	if err = executor.Exec(ctx, sql); err != nil {
		return
	}

	resultSet, err := getResultSet(ctx, executor)
	if err != nil {
		return
	}

	res = "{\n"

	for _, result := range resultSet {
		// columns: CDCSQLTemplates[CDCGetWatermarkSqlTemplate_Idx].OutputAttrs
		// 0: db_name
		// 1: table_name
		// 2: watermark
		// 3: err_msg
		for rowIdx, rowCnt := uint64(0), result.GetRowCount(); rowIdx < rowCnt; rowIdx++ {
			for colIdx, colName := range queryAttrs {
				switch colName {
				case "watermark":
					if watermarkStr, err = result.GetString(ctx, rowIdx, uint64(colIdx)); err != nil {
						return
					}
					watermarkStr, err = TransformStdTimeString(watermarkStr)
					if err != nil {
						return
					}
				case "err_msg":
					if errMsg, err = result.GetString(ctx, rowIdx, uint64(colIdx)); err != nil {
						return
					}
				case "db_name":
					if dbName, err = result.GetString(ctx, rowIdx, uint64(colIdx)); err != nil {
						return
					}
				case "table_name":
					if tableName, err = result.GetString(ctx, rowIdx, uint64(colIdx)); err != nil {
						return
					}
				default:
					err = moerr.NewInternalErrorf(ctx, "unknown column: %s", colName)
					return
				}
			}
			if len(errMsg) == 0 {
				res += fmt.Sprintf("  \"%s.%s\": %s,\n", dbName, tableName, watermarkStr)
			} else {
				res += fmt.Sprintf("  \"%s.%s\": %s(Failed, error: %s),\n", dbName, tableName, watermarkStr, errMsg)
			}
		}
	}

	res += "}"

	return
}

func (t *CDCDao) DeleteManyWatermark(
	ctx context.Context,
	keys map[taskservice.CDCTaskKey]struct{},
) (deletedCnt int64, err error) {
	var (
		cnt      int64
		executor = t.MustGetSQLExecutor(ctx)
	)
	for key := range keys {
		sql := cdc.CDCSQLBuilder.DeleteWatermarkSQL(
			key.AccountId,
			key.TaskId,
		)
		logutil.Debug(
			"cdc.dao.delete_watermark_sql",
			zap.Uint64("account-id", key.AccountId),
			zap.String("task-id", key.TaskId),
			zap.String("sql", sql),
		)
		if cnt, err = ExecuteAndGetRowsAffected(ctx, executor, sql); err != nil {
			return
		}
		deletedCnt += cnt
	}

	return
}

// if taskName is empty, delete all tasks belong to accountId
func (t *CDCDao) DeleteTaskByName(
	ctx context.Context,
	accountId uint64,
	taskName string,
) (deletedCnt int64, err error) {
	var (
		executor = t.MustGetSQLExecutor(ctx)
	)
	sql := cdc.CDCSQLBuilder.DeleteTaskSQL(accountId, taskName)
	deletedCnt, err = ExecuteAndGetRowsAffected(ctx, executor, sql)
	return
}

func (t *CDCDao) DeleteTaskAndWatermark(
	ctx context.Context,
	accountId uint64,
	taskName string,
	keys map[taskservice.CDCTaskKey]struct{},
) (res DeleteCDCArtifactsResult, err error) {
	var (
		executor = t.MustGetSQLExecutor(ctx)
	)

	_, isTxn := executor.(*sql.Tx)
	res.LocalTxnOpened = !isTxn

	logutil.Debug(
		"cdc.dao.delete_task_and_watermark.begin",
		zap.Uint64("account-id", accountId),
		zap.String("task-name", taskName),
		zap.Int("key-count", len(keys)),
		zap.Bool("executor-is-tx", isTxn),
		zap.Bool("local-tx-opened", res.LocalTxnOpened),
	)

	if res.LocalTxnOpened {
		if _, err = executor.ExecContext(ctx, "BEGIN"); err != nil {
			logutil.Error(
				"cdc.dao.delete_task_and_watermark.begin_failed",
				zap.Uint64("account-id", accountId),
				zap.String("task-name", taskName),
				zap.Error(err),
			)
			return
		}
		defer func() {
			finalSQL := "COMMIT"
			if err != nil {
				finalSQL = "ROLLBACK"
			}
			if _, e := executor.ExecContext(ctx, finalSQL); e != nil {
				logutil.Error(
					"cdc.dao.delete_task_and_watermark.finalize_failed",
					zap.Uint64("account-id", accountId),
					zap.String("task-name", taskName),
					zap.String("sql", finalSQL),
					zap.Error(e),
				)
				if err == nil {
					err = e
				}
			}
		}()
	}

	if res.TaskRows, err = t.DeleteTaskByName(ctx, accountId, taskName); err != nil {
		return
	}

	if res.WatermarkRows, err = t.DeleteManyWatermark(ctx, keys); err != nil {
		return
	}

	logutil.Debug(
		"cdc.dao.delete_task_and_watermark.done",
		zap.Uint64("account-id", accountId),
		zap.String("task-name", taskName),
		zap.Int64("task-rows", res.TaskRows),
		zap.Int64("watermark-rows", res.WatermarkRows),
		zap.Bool("executor-is-tx", isTxn),
		zap.Bool("local-tx-opened", res.LocalTxnOpened),
	)

	return
}

func (t *CDCDao) GetTaskKeys(
	ctx context.Context,
	accountId uint64,
	taskName string,
	keys map[taskservice.CDCTaskKey]struct{},
) (cnt int64, err error) {
	var (
		executor = t.MustGetSQLExecutor(ctx)
	)
	if cnt, err = ForeachQueriedRow(
		ctx,
		executor,
		cdc.CDCSQLBuilder.GetTaskIdSQL(accountId, taskName),
		func(ctx context.Context, rows *sql.Rows) (bool, error) {
			var (
				key  taskservice.CDCTaskKey
				err2 error
			)
			if err2 = rows.Scan(&key.TaskId); err2 != nil {
				return false, err2
			}
			key.AccountId = accountId
			keys[key] = struct{}{}
			return true, nil
		},
	); err != nil {
		return
	}
	if cnt == 0 && taskName != "" {
		err = moerr.NewInternalErrorf(
			ctx,
			"no cdc task found, accountId: %d, taskName: %s",
			accountId,
			taskName,
		)
	}
	return
}

// update `state` only
func (t *CDCDao) PrepareUpdateTask(
	ctx context.Context,
	accountId uint64,
	taskName string,
	targetState string,
) (affectedRows int64, err error) {
	var (
		executor = t.MustGetSQLExecutor(ctx)
		prepare  *sql.Stmt
		result   sql.Result
	)
	sql := cdc.CDCSQLBuilder.UpdateTaskStateSQL(accountId, taskName)
	if prepare, err = executor.PrepareContext(ctx, sql); err != nil {
		return
	}
	defer prepare.Close()

	if result, err = prepare.ExecContext(ctx, targetState); err != nil {
		return
	}
	affectedRows, err = result.RowsAffected()

	return
}

func (t *CDCDao) syncCommitTimestamp(ctx context.Context) error {
	if t.ses == nil || t.ses.proc == nil || t.ses.proc.Base == nil {
		return moerr.NewInternalError(ctx, "session or process is nil")
	}
	base := t.ses.proc.Base
	qc := base.QueryClient
	if qc == nil {
		return moerr.NewInternalError(ctx, "query client is nil")
	}
	txnClient := base.TxnClient
	if txnClient == nil {
		return moerr.NewInternalError(ctx, "txn client is nil")
	}

	cluster := clusterservice.GetMOCluster(qc.ServiceID())
	if cluster == nil {
		return moerr.NewInternalError(ctx, "cluster service is nil")
	}

	addresses := make([]string, 0, 4)
	cluster.GetCNService(
		clusterservice.NewSelectAll(),
		func(cn metadata.CNService) bool {
			if addr := cn.QueryAddress; addr != "" {
				addresses = append(addresses, addr)
			}
			return true
		},
	)
	if len(addresses) == 0 {
		return nil
	}

	var (
		maxCommitTS timestamp.Timestamp
		hasTS       bool
	)

	for _, addr := range addresses {
		req := qc.NewRequest(querypb.CmdMethod_GetCommit)
		ctxReq, cancel := context.WithTimeoutCause(ctx, 5*time.Second, moerr.CauseSyncLatestCommitT)
		resp, err := qc.SendMessage(ctxReq, addr, req)
		cancel()
		if err != nil {
			logutil.Warn(
				"cdc.syncCommitTimestamp: send get commit failed",
				zap.String("address", addr),
				zap.Error(err),
			)
			continue
		}
		if resp != nil {
			if commitResp := resp.GetCommit; commitResp != nil {
				if !hasTS || maxCommitTS.Less(commitResp.CurrentCommitTS) {
					maxCommitTS = commitResp.CurrentCommitTS
					hasTS = true
				}
			}
			qc.Release(resp)
		}
	}

	if !hasTS {
		return nil
	}

	txnClient.SyncLatestCommitTS(maxCommitTS)
	return nil
}

var ForeachQueriedRow = func(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	query string,
	onEachRow func(context.Context, *sql.Rows) (bool, error),
) (cnt int64, err error) {
	var (
		ok   bool
		rows *sql.Rows
	)
	if rows, err = tx.QueryContext(ctx, query); err != nil {
		return
	}
	if rows.Err() != nil {
		err = rows.Err()
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		if ok, err = onEachRow(ctx, rows); err != nil {
			return
		}
		if ok {
			cnt++
		}
	}
	return
}
