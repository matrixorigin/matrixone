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
	"database/sql"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

type CreateTaskRequest = tree.CreateCDC

// All handle functions
// 1. handleCreateCdc: create a cdc task

func handleCreateCdc(ses *Session, execCtx *ExecCtx, create *tree.CreateCDC) error {
	return handleCreateCDCTaskRequest(execCtx.reqCtx, ses, create)
}

func handleCreateCDCTaskRequest(
	ctx context.Context,
	ses *Session,
	req *CreateTaskRequest,
) (err error) {
	// init task service
	taskService := getPu(ses.GetService()).TaskService
	if taskService == nil {
		return moerr.NewInternalError(ctx, "task service not found")
	}

	// validate and fill the request options
	var opts CreateTaskRequestOptions
	if err = opts.ValidateAndFill(ctx, ses, req); err != nil {
		return
	}

	var (
		details *task.Details
	)
	if details, err = opts.BuildTaskDetails(); err != nil {
		return
	}

	// create cdc task job: TODO
	creatTaskJob := func(
		ctx context.Context,
		tx taskservice.SqlExecutor,
	) (ret int, err error) {
		var (
			insertSql    string
			result       sql.Result
			rowsAffected int64
		)
		if insertSql, err = opts.ToInsertTaskSQL(ctx, tx, ses.GetService()); err != nil {
			return
		}
		if result, err = tx.ExecContext(ctx, insertSql); err != nil {
			return
		}
		if rowsAffected, err = result.RowsAffected(); err != nil {
			return
		}
		return int(rowsAffected), nil
	}

	// schedule the cdc task job
	_, err = taskService.AddCdcTask(
		ctx,
		opts.BuildTaskMetadata(),
		details,
		creatTaskJob,
	)

	return
}
