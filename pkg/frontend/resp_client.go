// Copyright 2021 - 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func setResponse(ses *Session, isLastStmt bool, rspLen uint64) *Response {
	return ses.SetNewResponse(OkResponse, rspLen, int(COM_QUERY), "", isLastStmt)
}

// response the client
func respClientWhenSuccess(ses *Session,
	execCtx *ExecCtx) (err error) {
	if execCtx.skipRespClient {
		return nil
	}
	err = respClientWithoutFlush(ses, execCtx)
	if err != nil {
		return err
	}

	err = ses.GetMysqlProtocol().Flush()
	if err != nil {
		return err
	}

	if ses.GetQueryInExecute() {
		logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, success, nil)
	} else {
		logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, moerr.NewInternalError(execCtx.reqCtx, "query is killed"))
	}
	return err
}

func respClientWithoutFlush(ses *Session,
	execCtx *ExecCtx) (err error) {
	if execCtx.skipRespClient {
		return nil
	}
	switch execCtx.stmt.StmtKind().RespType() {
	case tree.RESP_STREAM_RESULT_ROW:
		err = respStreamResultRow(ses, execCtx)
	case tree.RESP_PREBUILD_RESULT_ROW:
		err = respPrebuildResultRow(ses, execCtx)
	case tree.RESP_MIXED_RESULT_ROW:
		err = respMixedResultRow(ses, execCtx)
	case tree.RESP_NOTHING:
	case tree.RESP_BY_SITUATION:
	case tree.RESP_STATUS:
		err = respStatus(ses, execCtx)
	}
	return err
}
