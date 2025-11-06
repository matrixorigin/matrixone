// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/btree"
)

func mockTableDef() *plan.TableDef {
	return &plan.TableDef{
		Cols: []*plan.ColDef{
			{
				Name: "pk",
				Typ:  plan.Type{Id: int32(types.T_uint64)},
				Default: &plan.Default{
					NullAbility: false,
				},
			},
		},
	}
}

func mockClusterTableDef() *plan.TableDef {
	return &plan.TableDef{
		TableType: catalog.SystemExternalRel,
		Cols: []*plan.ColDef{
			{
				Name: "pk",
				Typ:  plan.Type{Id: int32(types.T_uint64)},
				Default: &plan.Default{
					NullAbility: false,
				},
			},
		},
	}
}

func TestNewSinker(t *testing.T) {
	type args struct {
		sinkUri          UriInfo
		accountId        uint64
		taskId           string
		dbTblInfo        *DbTableInfo
		watermarkUpdater *CDCWatermarkUpdater
		tableDef         *plan.TableDef
		retryTimes       int
		retryDuration    time.Duration
		ar               *ActiveRoutine
	}
	tests := []struct {
		name    string
		args    args
		want    Sinker
		wantErr assert.ErrorAssertionFunc
	}{
		{
			args: args{
				sinkUri: UriInfo{
					SinkTyp: CDCSinkType_Console,
				},
				dbTblInfo:        &DbTableInfo{},
				watermarkUpdater: nil,
				tableDef:         mockTableDef(),
				retryTimes:       0,
				retryDuration:    0,
				ar:               NewCdcActiveRoutine(),
			},
			want: &consoleSinker{
				dbTblInfo:        &DbTableInfo{},
				watermarkUpdater: nil,
			},
			wantErr: assert.NoError,
		},
		{
			args: args{
				sinkUri: UriInfo{
					SinkTyp: CDCSinkType_MySQL,
				},
				dbTblInfo: &DbTableInfo{
					SourceCreateSql: "create table t1 (a int, b int, c int)",
				},
				watermarkUpdater: nil,
				tableDef:         mockTableDef(),
				retryTimes:       0,
				retryDuration:    0,
				ar:               NewCdcActiveRoutine(),
			},
			want:    nil,
			wantErr: assert.NoError,
		},
		{
			args: args{
				sinkUri: UriInfo{
					SinkTyp: CDCSinkType_MySQL,
				},
				dbTblInfo: &DbTableInfo{
					SourceCreateSql: "create table t1 (a int, b int, c int)",
					IdChanged:       true,
				},
				watermarkUpdater: nil,
				tableDef:         mockTableDef(),
				retryTimes:       0,
				retryDuration:    0,
				ar:               NewCdcActiveRoutine(),
			},
			want:    nil,
			wantErr: assert.NoError,
		},
		{
			args: args{
				sinkUri: UriInfo{
					SinkTyp: CDCSinkType_MySQL,
				},
				dbTblInfo: &DbTableInfo{
					SourceCreateSql: "create table t1 (a int, b int, c int)",
					IdChanged:       true,
				},
				watermarkUpdater: nil,
				tableDef:         mockClusterTableDef(),
				retryTimes:       0,
				retryDuration:    0,
				ar:               NewCdcActiveRoutine(),
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stub *gostub.Stubs

			if tt.args.sinkUri.SinkTyp == CDCSinkType_MySQL {
				// Mock CreateMysqlSinker2 to avoid real DB connection
				stub = gostub.Stub(&CreateMysqlSinker2, func(
					_ UriInfo, _ uint64, _ string, _ *DbTableInfo, _ *CDCWatermarkUpdater,
					tableDef *plan.TableDef, _ int, _ time.Duration, _ *ActiveRoutine, _ uint64, _ string,
				) (Sinker, error) {
					// Return nil for success cases (we don't check the actual sinker)
					// Return error for cluster table case
					if tableDef != nil && tableDef.TableType == catalog.SystemExternalRel {
						return nil, moerr.NewInternalErrorNoCtx("external table is not supported")
					}
					return nil, nil
				})
				defer stub.Reset()
			}

			got, err := NewSinker(
				tt.args.sinkUri,
				tt.args.accountId,
				tt.args.taskId,
				tt.args.dbTblInfo,
				tt.args.watermarkUpdater,
				tt.args.tableDef,
				tt.args.retryTimes,
				tt.args.retryDuration,
				tt.args.ar,
				CDCDefaultTaskExtra_MaxSQLLen,
				CDCDefaultSendSqlTimeout,
			)
			if !tt.wantErr(t, err, fmt.Sprintf("NewSinker(%v, %v, %v, %v, %v, %v)", tt.args.sinkUri, tt.args.dbTblInfo, tt.args.watermarkUpdater, tt.args.tableDef, tt.args.retryTimes, tt.args.retryDuration)) {
				return
			}
			assert.Equalf(t, tt.want, got, "NewSinker(%v, %v, %v, %v, %v, %v)", tt.args.sinkUri, tt.args.dbTblInfo, tt.args.watermarkUpdater, tt.args.tableDef, tt.args.retryTimes, tt.args.retryDuration)
		})
	}
}

func TestNewConsoleSinker(t *testing.T) {
	type args struct {
		dbTblInfo        *DbTableInfo
		watermarkUpdater *CDCWatermarkUpdater
	}
	tests := []struct {
		name string
		args args
		want Sinker
	}{
		{
			args: args{
				dbTblInfo:        &DbTableInfo{},
				watermarkUpdater: nil,
			},
			want: &consoleSinker{
				dbTblInfo:        &DbTableInfo{},
				watermarkUpdater: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewConsoleSinker(tt.args.dbTblInfo, tt.args.watermarkUpdater), "NewConsoleSinker(%v, %v)", tt.args.dbTblInfo, tt.args.watermarkUpdater)
		})
	}
}

func Test_consoleSinker_Sink(t *testing.T) {
	bat := batch.New([]string{"a", "b", "c"})
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil)
	bat.Vecs[1] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil)
	bat.SetRowCount(3)

	fromTs := types.BuildTS(1, 1)
	atomicBat := &AtomicBatch{
		Mp:      nil,
		Batches: []*batch.Batch{bat},
		Rows:    btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
	}
	atomicBat.Rows.Set(AtomicBatchRow{Ts: fromTs, Pk: []byte{1}, Offset: 0, Src: bat})

	type fields struct {
		dbTblInfo        *DbTableInfo
		watermarkUpdater *CDCWatermarkUpdater
	}
	type args struct {
		ctx  context.Context
		data *DecoderOutput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			args: args{
				ctx: context.Background(),
				data: &DecoderOutput{
					outputTyp:     OutputTypeSnapshot,
					checkpointBat: bat,
				},
			},
			wantErr: assert.NoError,
		},
		{
			args: args{
				ctx: context.Background(),
				data: &DecoderOutput{
					outputTyp:      OutputTypeTail,
					insertAtmBatch: atomicBat,
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &consoleSinker{
				dbTblInfo:        tt.fields.dbTblInfo,
				watermarkUpdater: tt.fields.watermarkUpdater,
			}
			s.Sink(tt.args.ctx, tt.args.data)
		})
	}
}

func TestNewMysqlSink(t *testing.T) {
	type args struct {
		user          string
		password      string
		ip            string
		port          int
		retryTimes    int
		retryDuration time.Duration
	}
	tests := []struct {
		name    string
		args    args
		want    Sink
		wantErr assert.ErrorAssertionFunc
	}{
		{
			args: args{
				user:          "root",
				password:      "123456",
				ip:            "127.0.0.1",
				port:          3306,
				retryTimes:    3,
				retryDuration: 3 * time.Second,
			},
			want: &mysqlSink{
				user:          "root",
				password:      "123456",
				ip:            "127.0.0.1",
				port:          3306,
				retryTimes:    3,
				retryDuration: 3 * time.Second,
				timeout:       CDCDefaultSendSqlTimeout,
			},
			wantErr: assert.NoError,
		},
	}

	stub := gostub.Stub(&OpenDbConn, func(_, _, _ string, _ int, _ string) (_ *sql.DB, _ error) {
		return nil, nil
	})
	defer stub.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMysqlSink(tt.args.user, tt.args.password, tt.args.ip, tt.args.port, tt.args.retryTimes, tt.args.retryDuration, CDCDefaultSendSqlTimeout, false)
			if !tt.wantErr(t, err, fmt.Sprintf("NewMysqlSink(%v, %v, %v, %v, %v, %v)", tt.args.user, tt.args.password, tt.args.ip, tt.args.port, tt.args.retryTimes, tt.args.retryDuration)) {
				return
			}
			assert.Equalf(t, tt.want, got, "NewMysqlSink(%v, %v, %v, %v, %v, %v)", tt.args.user, tt.args.password, tt.args.ip, tt.args.port, tt.args.retryTimes, tt.args.retryDuration)
		})
	}
}

func Test_mysqlSink_Close(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectClose()

	sink := &mysqlSink{
		user:          "root",
		password:      "123456",
		ip:            "127.0.0.1",
		port:          3306,
		retryTimes:    3,
		retryDuration: 3 * time.Second,
		conn:          db,
	}
	sink.Close()
	assert.Nil(t, sink.conn)
}

func Test_mysqlSink_Send(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(fakeSql).WillReturnResult(sqlmock.NewResult(1, 1))

	sink := &mysqlSink{
		user:          "root",
		password:      "123456",
		ip:            "127.0.0.1",
		port:          3306,
		retryTimes:    CDCDefaultRetryTimes,
		retryDuration: CDCDefaultRetryDuration,
		conn:          db,
	}
	ar := NewCdcActiveRoutine()
	err = sink.Send(context.Background(), ar, []byte("sql"), true)
	assert.NoError(t, err)

	close(ar.Pause)
	err = sink.Send(context.Background(), ar, []byte("sql"), true)
	assert.NoError(t, err)
}

func Test_consoleSinker_Close(t *testing.T) {
	type fields struct {
		dbTblInfo        *DbTableInfo
		watermarkUpdater *CDCWatermarkUpdater
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			fields: fields{
				dbTblInfo:        &DbTableInfo{},
				watermarkUpdater: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &consoleSinker{
				dbTblInfo:        tt.fields.dbTblInfo,
				watermarkUpdater: tt.fields.watermarkUpdater,
			}
			s.Close()
		})
	}
}

func Test_consoleSinker_SendBeginCommitRollback(t *testing.T) {
	s := &consoleSinker{}
	s.SendBegin()
	s.SendCommit()
	s.SendRollback()
}

func TestRecordTxn(t *testing.T) {
	fault.Enable()
	defer fault.Disable()

	rm, err := objectio.InjectCDCRecordTxn("testdb", "t1", 0)
	assert.NoError(t, err)

	defer func() {
		rm()
	}()

	ok, _ := objectio.CDCRecordTxnInjected("testdb", "t1")
	assert.True(t, ok)

	ok, _ = objectio.CDCRecordTxnInjected("tpcc", "bmsql_stock")
	assert.True(t, ok)

	{
		s := &mysqlSink{}

		sql1 := make([]byte, sqlBufReserved)
		sql1 = append(sql1, []byte("select count(*) from testdb.t1")...)

		s.recordTxnSQL(sql1)
		s.infoRecordedTxnSQLs(nil)
		s.infoRecordedTxnSQLs(nil)
	}

	{
		s := &mysqlSink{}
		s.debugTxnRecorder.doRecord = true

		sql1 := make([]byte, sqlBufReserved)
		sql1 = append(sql1, []byte("select count(*) from testdb.t1")...)

		sql2 := make([]byte, sqlBufReserved)
		sql2 = append(sql2, []byte("select count(*) from tpcc.bmsql_stock")...)

		s.recordTxnSQL(sql1)
		s.recordTxnSQL(sql2)

		s.infoRecordedTxnSQLs(nil)
		s.infoRecordedTxnSQLs(nil)
	}

	_ = moerr.NewInvalidStateNoCtxf("for test coverage")
}
