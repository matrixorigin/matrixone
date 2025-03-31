// Copyright 2023 Matrix Origin
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type CDCCreateTaskRequest = tree.CreateCDC
type CDCShowTaskRequest = tree.ShowCDC

// All handle functions
// 1. handleCreateCdc: create a cdc task
// 2. handleDropCdc: drop a cdc task
// 3. handlePauseCdc: pause a cdc task
// 4. handleResumeCdc: resume a cdc task
// 5. handleRestartCdc: restart a cdc task
// 6. handleShowCDCTaskRequest: show a cdc task
// 7. handleShowCdc: show a cdc task

func handleCreateCdc(ses *Session, execCtx *ExecCtx, create *tree.CreateCDC) error {
	return handleCreateCDCTaskRequest(execCtx.reqCtx, ses, create)
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

func handleShowCdc(
	ses *Session,
	execCtx *ExecCtx,
	st *tree.ShowCDC,
) (err error) {
	var (
		taskId        string
		taskName      string
		sourceUri     string
		sinkUri       string
		state         string
		errMsg        string
		watermarkStr  string
		sourceUriInfo cdc.UriInfo
		sinkUriInfo   cdc.UriInfo
	)

	ctx := defines.AttachAccountId(execCtx.reqCtx, catalog.System_Account)
	pu := getPu(ses.GetService())
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	rs := GetCDCShowOutputResultSet()
	ses.SetMysqlResultSet(rs)

	// current timestamp
	txnOp, err := cdc.GetTxnOp(
		ctx,
		pu.StorageEngine,
		pu.TxnClient,
		"cdc-handleShowCdc",
	)
	if err != nil {
		return err
	}
	defer func() {
		cdc.FinishTxnOp(ctx, err, txnOp, pu.StorageEngine)
	}()
	timestamp := txnOp.SnapshotTS().ToStdTime().In(time.Local).String()

	// get from task table
	sql := cdc.CDCSQLBuilder.ShowTaskSQL(
		uint64(ses.GetTenantInfo().GetTenantID()),
		st.Option.All,
		string(st.Option.TaskName),
	)

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	var dao CDCDao

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
			if watermarkStr, err = dao.GetTaskWatermark(
				ctx,
				uint64(ses.GetTenantInfo().GetTenantID()),
				taskId,
				bh,
			); err != nil {
				return
			}
			rs.AddRow([]interface{}{
				taskId,
				taskName,
				sourceUriInfo.String(),
				sinkUriInfo.String(),
				state,
				errMsg,
				watermarkStr,
				timestamp,
			})
		}
	}
	return
}

func handleCreateCDCTaskRequest(
	ctx context.Context,
	ses *Session,
	req *CDCCreateTaskRequest,
) (err error) {
	dao, err := NewCDCDao(ses)
	if err != nil {
		return
	}

	err = dao.CreateTask(ctx, req)
	return
}

// func handleShowCDCTaskRequest(
// 	ctx context.Context,
// 	ses *Session,
// 	req *CDCShowTaskRequest,
// ) (err error) {
// 	dao, err := NewCDCDao(ses)
// 	if err != nil {
// 		return
// 	}

// 	err = dao.ShowTasks(ctx, req)
// 	return
// }
