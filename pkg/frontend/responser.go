// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

var _ Responser = &MysqlResp{}
var _ Responser = &NullResp{}

type MysqlResp struct {
	mysqlWr MysqlWriter
	csvWr   CsvWriter
	s3Wr    S3Writer
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

}

func NewMysqlResp() *MysqlResp {
	return &MysqlResp{}
}

func (resper *MysqlResp) RespPreMeta(execCtx *ExecCtx, meta any) (err error) {
	columns := meta.([]any)
	return resper.respColumnDefsWithoutFlush(execCtx.ses.(*Session), execCtx, columns)
}

func (resper *MysqlResp) RespResult(execCtx *ExecCtx, batch *batch.Batch) error {

	return nil
}

func (resper *MysqlResp) RespPostMeta(execCtx *ExecCtx, meta any) (err error) {
	return resper.respClientWithoutFlush(execCtx.ses.(*Session), execCtx)
}

func (resper *MysqlResp) Close() {
	resper.mysqlWr.Close()
	resper.csvWr.Close()
	resper.s3Wr.Close()
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

func (resper *NullResp) RespPostMeta(ctx *ExecCtx, a any) error {
	return nil
}

func (resper *NullResp) Close() {

}
