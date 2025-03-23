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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

type CDCDao struct {
	ses *Session
	ts  taskservice.TaskService
}

func NewCDCDao(
	ses *Session,
) (t CDCDao, err error) {
	t.ses = ses
	t.ts = getPu(ses.GetService()).TaskService
	if t.ts == nil {
		err = moerr.NewInternalError(context.Background(), "task service not found")
		return
	}
	return
}

func (t *CDCDao) BuildCreateOpts(
	ctx context.Context, req *CreateTaskRequest,
) (opts CreateTaskRequestOptions, err error) {
	err = opts.ValidateAndFill(ctx, t.ses, req)
	return
}

func (t *CDCDao) CreateTask(
	ctx context.Context,
	req *CreateTaskRequest,
) (err error) {
	var opts CreateTaskRequestOptions
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
