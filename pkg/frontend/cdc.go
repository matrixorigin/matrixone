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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

const (
	CdcRunning   = "running"
	CdcPaused    = "paused"
	CdcFailed    = "failed"
	maxErrMsgLen = 256
)

var showCdcOutputColumns = [8]Column{
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "task_id",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "task_name",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "source_uri",
			columnType: defines.MYSQL_TYPE_TEXT,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "sink_uri",
			columnType: defines.MYSQL_TYPE_TEXT,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "state",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "err_msg",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "checkpoint",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "timestamp",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
}

var queryTable = func(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	query string,
	callback func(ctx context.Context, rows *sql.Rows) (bool, error)) (bool, error) {
	var rows *sql.Rows
	var err error
	rows, err = tx.QueryContext(ctx, query)
	if err != nil {
		return false, err
	}
	if rows.Err() != nil {
		return false, rows.Err()
	}
	defer func() {
		_ = rows.Close()
	}()

	var ret bool
	for rows.Next() {
		ret, err = callback(ctx, rows)
		if err != nil {
			return false, err
		}
		if ret {
			return true, nil
		}
	}
	return false, nil
}

var initAesKeyByInternalExecutor = func(ctx context.Context, cdcTask *CDCTaskExecutor, accountId uint32) error {
	return cdcTask.initAesKeyByInternalExecutor(ctx, accountId)
}

func handleDropCdc(ses *Session, execCtx *ExecCtx, st *tree.DropCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handlePauseCdc(ses *Session, execCtx *ExecCtx, st *tree.PauseCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handleResumeCdc(ses *Session, execCtx *ExecCtx, st *tree.ResumeCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handleRestartCdc(ses *Session, execCtx *ExecCtx, st *tree.RestartCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func updateCdc(ctx context.Context, ses *Session, st tree.Statement) (err error) {
	var targetTaskStatus task.TaskStatus
	var taskName string

	conds := make([]taskservice.Condition, 0)
	appendCond := func(cond ...taskservice.Condition) {
		conds = append(conds, cond...)
	}
	accountId := ses.GetTenantInfo().GetTenantID()

	switch stmt := st.(type) {
	case *tree.DropCDC:
		targetTaskStatus = task.TaskStatus_CancelRequested
		if stmt.Option == nil {
			return moerr.NewInternalErrorf(ctx, "invalid cdc option")
		}
		if stmt.Option.All {
			appendCond(
				taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		} else {
			taskName = stmt.Option.TaskName.String()
			appendCond(
				taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskName(taskservice.EQ, stmt.Option.TaskName.String()),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		}
	case *tree.PauseCDC:
		targetTaskStatus = task.TaskStatus_PauseRequested
		if stmt.Option == nil {
			return moerr.NewInternalErrorf(ctx, "invalid cdc option")
		}
		if stmt.Option.All {
			appendCond(
				taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		} else {
			taskName = stmt.Option.TaskName.String()
			appendCond(taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskName(taskservice.EQ, stmt.Option.TaskName.String()),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		}
	case *tree.RestartCDC:
		targetTaskStatus = task.TaskStatus_RestartRequested
		taskName = stmt.TaskName.String()
		if len(taskName) == 0 {
			return moerr.NewInternalErrorf(ctx, "invalid task name")
		}
		appendCond(
			taskservice.WithAccountID(taskservice.EQ, accountId),
			taskservice.WithTaskName(taskservice.EQ, stmt.TaskName.String()),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
	case *tree.ResumeCDC:
		targetTaskStatus = task.TaskStatus_ResumeRequested
		taskName = stmt.TaskName.String()
		if len(taskName) == 0 {
			return moerr.NewInternalErrorf(ctx, "invalid task name")
		}
		appendCond(
			taskservice.WithAccountID(taskservice.EQ, accountId),
			taskservice.WithTaskName(taskservice.EQ, stmt.TaskName.String()),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
	}

	return runUpdateCdcTask(ctx, targetTaskStatus, uint64(accountId), taskName, ses.GetService(), conds...)
}

func runUpdateCdcTask(
	ctx context.Context,
	targetTaskStatus task.TaskStatus,
	accountId uint64,
	taskName string,
	service string,
	conds ...taskservice.Condition) (err error) {
	ts := getPu(service).TaskService
	if ts == nil {
		return nil
	}
	updateCdcTaskFunc := func(
		ctx context.Context,
		targetStatus task.TaskStatus,
		taskKeyMap map[taskservice.CdcTaskKey]struct{},
		tx taskservice.SqlExecutor,
	) (int, error) {
		return updateCdcTask(
			ctx,
			targetStatus,
			taskKeyMap,
			tx,
			accountId,
			taskName,
		)
	}
	_, err = ts.UpdateCdcTask(ctx,
		targetTaskStatus,
		updateCdcTaskFunc,
		conds...,
	)
	return err
}

func updateCdcTask(
	ctx context.Context,
	targetStatus task.TaskStatus,
	taskKeyMap map[taskservice.CdcTaskKey]struct{},
	tx taskservice.SqlExecutor,
	accountId uint64,
	taskName string,
) (int, error) {
	var taskId string
	var affectedCdcRow int
	var cnt int64

	query := cdc.CDCSQLBuilder.GetTaskIdSQL(accountId, taskName)

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	defer func() {
		_ = rows.Close()
	}()

	empty := true
	for rows.Next() {
		empty = false
		if err = rows.Scan(&taskId); err != nil {
			return 0, err
		}
		tInfo := taskservice.CdcTaskKey{AccountId: accountId, TaskId: taskId}
		taskKeyMap[tInfo] = struct{}{}
	}

	if taskName != "" && empty {
		return 0, moerr.NewInternalErrorf(ctx, "no cdc task found, task name: %s", taskName)
	}

	var prepare *sql.Stmt
	//step2: update or cancel cdc task
	if targetStatus != task.TaskStatus_CancelRequested {
		//Update cdc task
		//updating mo_cdc_task table
		updateSql := cdc.CDCSQLBuilder.UpdateTaskStateSQL(accountId, taskName)
		prepare, err = tx.PrepareContext(ctx, updateSql)
		if err != nil {
			return 0, err
		}
		defer func() {
			_ = prepare.Close()
		}()

		if prepare != nil {
			//execute update cdc status in mo_cdc_task
			var targetCdcStatus string
			if targetStatus == task.TaskStatus_PauseRequested {
				targetCdcStatus = CdcPaused
			} else {
				targetCdcStatus = CdcRunning
			}
			res, err := prepare.ExecContext(ctx, targetCdcStatus)
			if err != nil {
				return 0, err
			}
			affected, err := res.RowsAffected()
			if err != nil {
				return 0, err
			}
			affectedCdcRow += int(affected)
		}

		if targetStatus == task.TaskStatus_RestartRequested {
			//delete mo_cdc_watermark
			cnt, err = deleteWatermark(ctx, tx, taskKeyMap)
			if err != nil {
				return 0, err
			}
			affectedCdcRow += int(cnt)
		}
	} else {
		//Cancel cdc task
		//deleting mo_cdc_task
		deleteSql := cdc.CDCSQLBuilder.DeleteTaskSQL(accountId, taskName)
		cnt, err = ExecuteAndGetRowsAffected(ctx, tx, deleteSql)
		if err != nil {
			return 0, err
		}
		affectedCdcRow += int(cnt)

		//delete mo_cdc_watermark
		cnt, err = deleteWatermark(ctx, tx, taskKeyMap)
		if err != nil {
			return 0, err
		}
		affectedCdcRow += int(cnt)
	}
	return affectedCdcRow, nil
}

func deleteWatermark(ctx context.Context, tx taskservice.SqlExecutor, taskKeyMap map[taskservice.CdcTaskKey]struct{}) (int64, error) {
	tCount := int64(0)
	cnt := int64(0)
	var err error
	//deleting mo_cdc_watermark belongs to cancelled cdc task
	for tInfo := range taskKeyMap {
		deleteSql2 := cdc.CDCSQLBuilder.DeleteWatermarkSQL(tInfo.AccountId, tInfo.TaskId)
		cnt, err = ExecuteAndGetRowsAffected(ctx, tx, deleteSql2)
		if err != nil {
			return 0, err
		}
		tCount += cnt
	}
	return tCount, nil
}

func handleShowCdc(ses *Session, execCtx *ExecCtx, st *tree.ShowCDC) (err error) {
	var (
		taskId        string
		taskName      string
		sourceUri     string
		sinkUri       string
		state         string
		errMsg        string
		ckpStr        string
		sourceUriInfo cdc.UriInfo
		sinkUriInfo   cdc.UriInfo
	)

	ctx := defines.AttachAccountId(execCtx.reqCtx, catalog.System_Account)
	pu := getPu(ses.GetService())
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	rs := &MysqlResultSet{}
	ses.SetMysqlResultSet(rs)
	for _, column := range showCdcOutputColumns {
		rs.AddColumn(column)
	}

	// current timestamp
	txnOp, err := cdc.GetTxnOp(ctx, pu.StorageEngine, pu.TxnClient, "cdc-handleShowCdc")
	if err != nil {
		return err
	}
	defer func() {
		cdc.FinishTxnOp(ctx, err, txnOp, pu.StorageEngine)
	}()
	timestamp := txnOp.SnapshotTS().ToStdTime().In(time.Local).String()

	// get from task table
	sql := cdc.CDCSQLBuilder.ShowTaskSQL(uint64(ses.GetTenantInfo().GetTenantID()), st.Option.All, string(st.Option.TaskName))

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			if taskId, err = result.GetString(ctx, i, 0); err != nil {
				return
			}
			if taskName, err = result.GetString(ctx, i, 1); err != nil {
				return
			}
			if sourceUri, err = result.GetString(ctx, i, 2); err != nil {
				return
			}
			if sinkUri, err = result.GetString(ctx, i, 3); err != nil {
				return
			}
			if state, err = result.GetString(ctx, i, 4); err != nil {
				return
			}
			if errMsg, err = result.GetString(ctx, i, 5); err != nil {
				return
			}

			// decode uriInfo
			if err = cdc.JsonDecode(sourceUri, &sourceUriInfo); err != nil {
				return
			}
			if err = cdc.JsonDecode(sinkUri, &sinkUriInfo); err != nil {
				return
			}

			// get watermarks
			if ckpStr, err = getTaskCkp(ctx, bh, ses.GetTenantInfo().TenantID, taskId); err != nil {
				return
			}

			rs.AddRow([]interface{}{
				taskId,
				taskName,
				sourceUriInfo.String(),
				sinkUriInfo.String(),
				state,
				errMsg,
				ckpStr,
				timestamp,
			})
		}
	}
	return
}

func getTaskCkp(ctx context.Context, bh BackgroundExec, accountId uint32, taskId string) (s string, err error) {
	var (
		dbName           string
		tblName          string
		watermarkStdTime string
		errMsg           string
	)

	getWatermarkStdTime := func(result ExecResult, i uint64) (watermarkStr string, err error) {
		var watermarkTs timestamp.Timestamp
		if watermarkStr, err = result.GetString(ctx, i, 2); err != nil {
			return
		}
		if watermarkTs, err = timestamp.ParseTimestamp(watermarkStr); err != nil {
			return
		}
		watermarkStr = watermarkTs.ToStdTime().In(time.Local).String()
		return
	}

	s = "{\n"

	sql := cdc.CDCSQLBuilder.GetWatermarkSQL(uint64(accountId), taskId)
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			if dbName, err = result.GetString(ctx, i, 0); err != nil {
				return
			}
			if tblName, err = result.GetString(ctx, i, 1); err != nil {
				return
			}
			if watermarkStdTime, err = getWatermarkStdTime(result, i); err != nil {
				return
			}
			if errMsg, err = result.GetString(ctx, i, 3); err != nil {
				return
			}

			if len(errMsg) == 0 {
				s += fmt.Sprintf("  \"%s.%s\": %s,\n", dbName, tblName, watermarkStdTime)
			} else {
				s += fmt.Sprintf("  \"%s.%s\": %s(Failed, error: %s),\n", dbName, tblName, watermarkStdTime, errMsg)
			}
		}
	}

	s += "}"
	return
}
