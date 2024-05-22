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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

var _ Responser = &MysqlResp{}

type MysqlResp struct {
	mysqlWr MysqlWriter
	csvWr   CsvWriter
	s3Wr    S3Writer
}

func NewMysqlResp() *MysqlResp {
	return &MysqlResp{}
}

func (m *MysqlResp) RespPreMeta(execCtx *ExecCtx, meta any) (err error) {
	columns := meta.([]any)

	//send column count
	err = m.mysqlWr.WriteLengthEncodedNumber(len(columns))
	if err != nil {
		return err
	}
	mrs := execCtx.ses.GetMysqlResultSet()
	//send columns
	//column_count * Protocol::ColumnDefinition packets
	cmd := execCtx.ses.GetCmd()
	for _, c := range columns {
		mysqlc := c.(Column)
		mrs.AddColumn(mysqlc)
		/*
			mysql COM_QUERY response: send the column definition per column
		*/
		err = m.mysqlWr.WriteColumnDef(execCtx.reqCtx, mysqlc, int(cmd))
		if err != nil {
			return
		}
	}

	/*
		mysql COM_QUERY response: End after the column has been sent.
		send EOF packet
	*/
	err = m.mysqlWr.WriteEOFIF(0, execCtx.ses.GetTxnHandler().GetServerStatus())
	if err != nil {
		return
	}
	return
}

func (m *MysqlResp) RespResult(execCtx *ExecCtx, batch *batch.Batch) error {

}

func (m *MysqlResp) RespPostMeta(execCtx *ExecCtx, meta any) error {

}

func (m *MysqlResp) Close() {

}
