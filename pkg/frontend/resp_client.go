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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
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
	err = execCtx.resper.RespPostMeta(execCtx, nil)
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

func (resper *MysqlResp) respClientWithoutFlush(ses *Session,
	execCtx *ExecCtx) (err error) {
	if execCtx.skipRespClient {
		return nil
	}
	switch execCtx.stmt.StmtKind().RespType() {
	case tree.RESP_STREAM_RESULT_ROW:
		err = resper.respStreamResultRow(ses, execCtx)
	case tree.RESP_PREBUILD_RESULT_ROW:
		err = resper.respPrebuildResultRow(ses, execCtx)
	case tree.RESP_MIXED_RESULT_ROW:
		err = resper.respMixedResultRow(ses, execCtx)
	case tree.RESP_NOTHING:
	case tree.RESP_BY_SITUATION:
		err = resper.respBySituation(ses, execCtx)
	case tree.RESP_STATUS:
		err = resper.respStatus(ses, execCtx)
	}
	return err
}

var _ Responser = &MysqlResp{}
var dumpResper Responser = &NullResp{}

type MysqlResp struct {
	mysqlWr MysqlWriter
	s3Wr    S3Writer
}

func NewMysqlResp(mysqlWr MysqlWriter) *MysqlResp {
	return &MysqlResp{
		mysqlWr: mysqlWr,
	}
}

func (resper *MysqlResp) GetProperty(name string) any {
	lname := strings.ToLower(name)
	switch lname {
	case "dbname", "databasename":
		return resper.mysqlWr.GetDatabaseName()
	case "uname", "username":
		return resper.mysqlWr.GetUserName()
	case "connid":
		return resper.mysqlWr.ConnectionID()
	case "peer":
		return resper.mysqlWr.Peer()
	default:
		return nil
	}
}

func (resper *MysqlResp) SetProperty(name string, val any) {
	lname := strings.ToLower(name)
	switch lname {
	case "dbname", "databasename":
		resper.mysqlWr.SetDatabaseName(val.(string))
	case "uname", "username":
		resper.mysqlWr.SetUserName(val.(string))
	}
}

func (resper *MysqlResp) ResetStatistics() {
	resper.mysqlWr.ResetStatistics()
}

func (resper *MysqlResp) RespPreMeta(execCtx *ExecCtx, meta any) (err error) {
	columns := meta.([]any)
	return resper.respColumnDefsWithoutFlush(execCtx.ses.(*Session), execCtx, columns)
}

func (resper *MysqlResp) RespResult(execCtx *ExecCtx, bat *batch.Batch) (err error) {
	ses := execCtx.ses.(*Session)
	ec := ses.GetExportConfig()

	if ec.needExportToFile() {
		err = ec.Write(execCtx, bat)
	} else {
		err = resper.mysqlWr.Write(execCtx, bat)
	}
	return
}

func (resper *MysqlResp) RespPostMeta(execCtx *ExecCtx, meta any) (err error) {
	return resper.respClientWithoutFlush(execCtx.ses.(*Session), execCtx)
}

func (resper *MysqlResp) Close() {
	if resper.mysqlWr != nil {
		resper.mysqlWr.Close()
	}
	if resper.s3Wr != nil {
		resper.s3Wr.Close()
	}
}

type NullResp struct {
	username string
	database string
}

func (resper *NullResp) GetProperty(name string) any {
	lname := strings.ToLower(name)
	switch lname {
	case "dbname", "databasename":
		return resper.database
	case "uname", "username":
		return resper.username
	case "connid":
		return fakeConnectionID
	case "peer":
		return "0.0.0.0:0"
	default:
		return nil
	}
}

func (resper *NullResp) SetProperty(name string, val any) {
	lname := strings.ToLower(name)
	switch lname {
	case "dbname", "databasename":
		resper.database = val.(string)
	case "uname", "username":
		resper.username = val.(string)
	}
}

func (resper *NullResp) ResetStatistics() {

}

func (resper *NullResp) RespPreMeta(ctx *ExecCtx, a any) error {
	return nil
}

func (resper *NullResp) RespResult(ctx *ExecCtx, b *batch.Batch) error {
	return nil
}

func (resper *NullResp) RespPostMeta(execCtx *ExecCtx, a any) error {
	//for sequence, "set @var = nextval('xxxx')" need
	//refresh the sequence values.
	if ses, ok := execCtx.ses.(*Session); ok && execCtx.stmt != nil {
		switch execCtx.stmt.(type) {
		case *tree.Select:
			if len(execCtx.proc.SessionInfo.SeqAddValues) != 0 {
				ses.AddSeqValues(execCtx.proc)
			}
			ses.SetSeqLastValue(execCtx.proc)
		}
	}

	return nil
}

func (resper *NullResp) Close() {

}
