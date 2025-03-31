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
	dao := NewCDCDao(ses)
	return dao.ShowTasks(execCtx.reqCtx, st)
}

func handleCreateCDCTaskRequest(
	ctx context.Context,
	ses *Session,
	req *CDCCreateTaskRequest,
) (err error) {
	dao := NewCDCDao(ses)

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
