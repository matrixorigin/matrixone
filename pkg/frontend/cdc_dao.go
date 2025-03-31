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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type CDCDao struct {
	ses *Session
	ts  taskservice.TaskService
}

func NewCDCDao(
	ses *Session,
) (t CDCDao) {
	t.ses = ses
	t.ts = getPu(ses.GetService()).TaskService
	return
}

func (t *CDCDao) BuildCreateOpts(
	ctx context.Context, req *CDCCreateTaskRequest,
) (opts CDCCreateTaskOptions, err error) {
	err = opts.ValidateAndFill(ctx, t.ses, req)
	return
}

func (t *CDCDao) BuildShowOpts(
	ctx context.Context, req *CDCShowTaskRequest,
) (opts CDCShowCDCTaskOptions, err error) {
	opts.AccountId = uint64(t.ses.GetTenantInfo().GetTenantID())
	opts.ShowAll = req.Option.All
	opts.TaskName = string(req.Option.TaskName)
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

	_, err = t.ts.AddCdcTask(
		ctx, opts.BuildTaskMetadata(), details, creatTaskJob,
	)
	return
}

func (t *CDCDao) ShowTasks(
	ctx context.Context,
	req *CDCShowTaskRequest,
) (err error) {
	var (
		taskId       string
		taskName     string
		sourceUri    string
		sinkUri      string
		state        string
		errMsg       string
		watermarkStr string
		executor     = t.ses.GetBackgroundExec(ctx)
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

	for _, result := range execResultSet {
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
				executor,
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
	executor BackgroundExec,
) (res string, err error) {
	var (
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
