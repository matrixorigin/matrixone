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

func (resp *MysqlResp) RespPreMeta(execCtx *ExecCtx, meta any) (err error) {
	columns := meta.([]any)
	return resp.respColumnDefsWithoutFlush(execCtx.ses.(*Session), execCtx, columns)
}

func (resp *MysqlResp) RespResult(execCtx *ExecCtx, batch *batch.Batch) error {

	return nil
}

func (resp *MysqlResp) RespPostMeta(execCtx *ExecCtx, meta any) (err error) {
	return resp.respClientWithoutFlush(execCtx.ses.(*Session), execCtx)
}

func (resp *MysqlResp) Close() {

}
