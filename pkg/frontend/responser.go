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
	"context"

	"github.com/fagongzi/goetty/v2"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ Responser = &MysqlResp{}
var _ Responser = &NullResp{}

type MysqlResp struct {
	mysqlWr MysqlWriter
	csvWr   CsvWriter
	s3Wr    S3Writer
}

func (resper *MysqlResp) CalculateOutTrafficBytes(b bool) (int64, int64) {
	//TODO implement me
	panic("implement me")
}

func (resper *MysqlResp) SendResultSetTextBatchRowSpeedup(mrs *MysqlResultSet, count uint64) error {
	//TODO implement me
	panic("implement me")
}

func (resper *MysqlResp) sendLocalInfileRequest(filepath string) error {
	//TODO implement me
	panic("implement me")
}

func (resper *MysqlResp) Read(options goetty.ReadOptions) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (resper *MysqlResp) SetSequenceID(i any) {
	//TODO implement me
	panic("implement me")
}

func (resper *MysqlResp) GetSequenceId() int {
	//TODO implement me
	panic("implement me")
}

func (resper *MysqlResp) ResetStatistics() {
	//TODO implement me
	panic("implement me")
}

func (resper *MysqlResp) ParseExecuteData(ctx context.Context, getProcess *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	//TODO implement me
	panic("implement me")
}

func (resper *MysqlResp) ParseSendLongData(ctx context.Context, getProcess *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	//TODO implement me
	panic("implement me")
}

func (resper *MysqlResp) GetCapability() uint32 {
	//TODO implement me
	panic("implement me")
}

func (resper *MysqlResp) SetCapability(u uint32) {
	//TODO implement me
	panic("implement me")
}

func (resper *MysqlResp) Peer() string {
	return resper.mysqlWr.Peer()
}

func (resper *MysqlResp) SetUserName(s string) {
	resper.mysqlWr.SetUserName(s)
}

func (resper *MysqlResp) ConnectionID() uint32 {
	return resper.mysqlWr.ConnectionID()
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

func (resper *MysqlResp) SetDatabaseName(s string) {
	resper.mysqlWr.SetDatabaseName(s)
}

func (resper *MysqlResp) GetDatabaseName() string {
	return resper.mysqlWr.GetDatabaseName()
}

func (resper *MysqlResp) GetUserName() string {
	return resper.mysqlWr.GetUserName()
}

func (resper *MysqlResp) Close() {

}

type NullResp struct {
	username string
	database string
}

func (resper *NullResp) CalculateOutTrafficBytes(b bool) (int64, int64) {
	//TODO implement me
	panic("implement me")
}

func (resper *NullResp) SendResultSetTextBatchRowSpeedup(mrs *MysqlResultSet, count uint64) error {
	//TODO implement me
	panic("implement me")
}

func (resper *NullResp) sendLocalInfileRequest(filepath string) error {
	//TODO implement me
	panic("implement me")
}

func (resper *NullResp) Read(options goetty.ReadOptions) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (resper *NullResp) SetSequenceID(i any) {
	//TODO implement me
	panic("implement me")
}

func (resper *NullResp) GetSequenceId() int {
	//TODO implement me
	panic("implement me")
}

func (resper *NullResp) ResetStatistics() {
	//TODO implement me
	panic("implement me")
}

func (resper *NullResp) ParseExecuteData(ctx context.Context, getProcess *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	//TODO implement me
	panic("implement me")
}

func (resper *NullResp) ParseSendLongData(ctx context.Context, getProcess *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	//TODO implement me
	panic("implement me")
}

func (resper *NullResp) GetCapability() uint32 {
	//TODO implement me
	panic("implement me")
}

func (resper *NullResp) SetCapability(u uint32) {
	//TODO implement me
	panic("implement me")
}

func (resper *NullResp) Peer() string {
	return "0.0.0.0:0"
}

func (resper *NullResp) SetUserName(s string) {
	resper.username = s
}

func (resper *NullResp) ConnectionID() uint32 {
	return fakeConnectionID
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

func (resper *NullResp) SetDatabaseName(s string) {
	resper.database = s
}

func (resper *NullResp) GetDatabaseName() string {
	return resper.database
}

func (resper *NullResp) GetUserName() string {
	return resper.username
}
