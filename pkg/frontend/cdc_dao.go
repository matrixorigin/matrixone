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
	if err != nil {
		return
	}

	// Sync commit timestamp to ensure the user session can see the created task
	if err = t.syncCommitTimestamp(ctx); err != nil {
		logutil.Errorf("failed to sync commit timestamp after creating CDC task: %v", err)
		// Don't fail the task creation if sync fails, just log it
		err = nil
	}
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
	logutil.Infof("XXX-DEBUG-21848-show-task-snap-ts: %s", startTS.ToString())
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

	logutil.Infof("XXX-DEBUG-21848-show-task-result length %d", len(execResultSet))
	for _, result := range execResultSet {
		logutil.Infof("XXX-DEBUG-21848-show-task-result row count %d", result.GetRowCount())
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

// syncCommitTimestamp syncs the commit timestamp across all CNs to ensure
// the current session can see the committed changes from other CNs.
// This is necessary when using mysqlstore transactions which don't automatically
// update the session's latestCommitTS.
func (t *CDCDao) syncCommitTimestamp(ctx context.Context) error {
	// Get query client from session's process
	if t.ses == nil || t.ses.proc == nil {
		return moerr.NewInternalError(ctx, "session or process is nil")
	}

	queryClient := t.ses.proc.GetQueryClient()
	if queryClient == nil {
		return moerr.NewInternalError(ctx, "query client is nil")
	}

	// Get MOCluster to find all CN services
	moCluster := clusterservice.GetMOCluster(t.ses.GetService())
	if moCluster == nil {
		return moerr.NewInternalError(ctx, "mo cluster is nil")
	}

	// Collect all CN addresses
	var cnAddrs []string
	moCluster.GetCNService(
		clusterservice.NewSelector(),
		func(c metadata.CNService) bool {
			cnAddrs = append(cnAddrs, c.QueryAddress)
			return true
		},
	)

	if len(cnAddrs) == 0 {
		logutil.Warn("no CN services found for commit timestamp sync")
		return nil
	}

	// Create a context with timeout for the sync operation
	syncCtx, cancel := context.WithTimeoutCause(ctx, time.Second*10, moerr.CauseHandleSyncCommit)
	defer cancel()

	// Step 1: Get the maximum commit timestamp from all CNs
	maxCommitTS := timestamp.Timestamp{}
	for _, addr := range cnAddrs {
		req := queryClient.NewRequest(querypb.CmdMethod_GetCommit)
		resp, err := queryClient.SendMessage(syncCtx, addr, req)
		if err != nil {
			logutil.Warnf("failed to get commit ts from CN %s: %v", addr, err)
			continue
		}
		if resp.GetCommit != nil && maxCommitTS.Less(resp.GetCommit.CurrentCommitTS) {
			maxCommitTS = resp.GetCommit.CurrentCommitTS
		}
		queryClient.Release(resp)
	}

	if maxCommitTS.IsEmpty() {
		logutil.Warn("failed to get any valid commit timestamp from CNs")
		return nil
	}

	// Step 2: Get the txn client from the process
	if t.ses.proc.Base.TxnClient == nil {
		return moerr.NewInternalError(ctx, "txn client is nil")
	}

	// Step 3: Sync the commit timestamp to the current CN
	t.ses.proc.Base.TxnClient.SyncLatestCommitTS(maxCommitTS)

	logutil.Infof("synced commit timestamp to %s after CDC task creation", maxCommitTS.DebugString())
	return nil
}
