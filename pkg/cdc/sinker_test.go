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
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sink Sink
			var stub *gostub.Stubs

			if tt.args.sinkUri.SinkTyp == CDCSinkType_MySQL {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()

				// create db + use + create table
				expectCount := 3
				if tt.args.dbTblInfo != nil && tt.args.dbTblInfo.IdChanged {
					// create db + use + drop + create table
					expectCount = 4
				}

				for i := 0; i < expectCount; i++ {
					mock.ExpectExec(fakeSql).WillReturnResult(sqlmock.NewResult(1, 1))
				}

				sink = &mysqlSink{
					conn: db,
				}

				stub = gostub.Stub(&NewMysqlSink, func(_, _, _ string, _, _ int, _ time.Duration, _ string, _ bool) (Sink, error) {
					return sink, nil
				})
				defer stub.Reset()
			}

			sinkerStub := gostub.Stub(&NewMysqlSinker, func(Sink, uint64, string, *DbTableInfo, *CDCWatermarkUpdater, *plan.TableDef, *ActiveRoutine, uint64, bool) Sinker {
				return nil
			})
			defer sinkerStub.Reset()

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

func TestNewMysqlSinker(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery("SELECT @@max_allowed_packet").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(CDCDefaultTaskExtra_MaxSQLLen))

	sink := &mysqlSink{
		user:          "root",
		password:      "123456",
		ip:            "127.0.0.1",
		port:          3306,
		retryTimes:    3,
		retryDuration: 3 * time.Second,
		conn:          db,
	}

	dbTblInfo := &DbTableInfo{
		SinkDbName:  "dbName",
		SinkTblName: "tblName",
	}

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{
				Name: "pk",
				Typ:  plan.Type{Id: int32(types.T_uint64)},
			},
		},
		Name2ColIndex: map[string]int32{"pk": 0},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"pk"},
		},
	}
	NewMysqlSinker(
		sink,
		1,
		"task-1",
		dbTblInfo,
		nil,
		tableDef,
		NewCdcActiveRoutine(),
		CDCDefaultTaskExtra_MaxSQLLen,
		false,
	)
}

func Test_mysqlSinker_appendSqlBuf(t *testing.T) {
	ctx := context.Background()

	tsInsertPrefix := "/* tsInsertPrefix */REPLACE INTO `db`.`table` VALUES "
	tsDeletePrefix := "/* tsDeletePrefix */DELETE FROM `db`.`table` WHERE a IN ("

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
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
	s := &mysqlSinker{
		mysql:          sink,
		tsInsertPrefix: []byte(tsInsertPrefix),
		tsDeletePrefix: []byte(tsDeletePrefix),
		preRowType:     NoOp,
		ar:             ar,
		sqlBufSendCh:   make(chan []byte),
	}
	s.insertSuffix = []byte(";")
	s.insertRowPrefix = []byte("(")
	s.insertColSeparator = []byte(",")
	s.insertRowSuffix = []byte(")")
	s.insertRowSeparator = []byte(",")
	s.sqlBufs[0] = make([]byte, sqlBufReserved, len(tsDeletePrefix)+8+sqlBufReserved)
	s.sqlBufs[1] = make([]byte, sqlBufReserved, len(tsDeletePrefix)+8+sqlBufReserved)
	s.curBufIdx = 0
	s.sqlBuf = s.sqlBufs[s.curBufIdx]
	s.ClearError()
	go s.Run(ctx, ar)
	defer func() {
		// call dummy to guarantee sqls has been sent, then close
		s.SendDummy()
		s.Close()
	}()

	prefix := "\000\000\000\000\000"

	// test insert
	s.sqlBuf = append(s.sqlBuf[:sqlBufReserved], s.tsInsertPrefix...)
	s.rowBuf = []byte("insert")
	// not exceed cap
	err = s.appendSqlBuf(InsertRow)
	assert.NoError(t, err)
	assert.Equal(t, []byte(prefix+tsInsertPrefix+"insert"), s.sqlBuf)
	// exceed cap
	err = s.appendSqlBuf(InsertRow)
	assert.NoError(t, err)
	assert.Equal(t, []byte(prefix+tsInsertPrefix+"insert"), s.sqlBuf)

	// test delete
	s.sqlBuf = append(s.sqlBuf[:sqlBufReserved], s.tsDeletePrefix...)
	s.rowBuf = []byte("delete")
	// not exceed cap
	err = s.appendSqlBuf(DeleteRow)
	assert.NoError(t, err)
	assert.Equal(t, []byte(prefix+tsDeletePrefix+"delete"), s.sqlBuf)
	// exceed cap
	err = s.appendSqlBuf(DeleteRow)
	assert.NoError(t, err)
	assert.Equal(t, []byte(prefix+tsDeletePrefix+"delete"), s.sqlBuf)
}

func Test_mysqlSinker_getDeleteRowBuf(t *testing.T) {
	// single col pk
	s := &mysqlSinker{
		rowBuf:    make([]byte, 0, 1024),
		deleteRow: []any{uint64(1)},
		deleteTypes: []*types.Type{
			{Oid: types.T_uint64},
		},
	}
	s.deleteRowPrefix = []byte("(")
	s.deleteColSeparator = []byte(",")
	s.deleteRowSuffix = []byte(")")
	s.deleteRowSeparator = []byte(",")

	err := s.getDeleteRowBuf(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []byte("(1)"), s.rowBuf)

	// multi cols pk
	s = &mysqlSinker{
		rowBuf:    make([]byte, 0, 1024),
		deleteRow: []any{[]byte{}},
		deleteTypes: []*types.Type{
			{Oid: types.T_uint64},
			{Oid: types.T_uint64},
		},
	}
	s.deleteSuffix = []byte(");")
	s.deleteRowPrefix = []byte("(")
	s.deleteColSeparator = []byte(",")
	s.deleteRowSuffix = []byte(")")
	s.deleteRowSeparator = []byte(",")

	stub := gostub.Stub(&unpackWithSchema, func(_ []byte) (types.Tuple, []types.T, error) {
		return types.Tuple{uint64(1), uint64(2)}, nil, nil
	})
	defer stub.Reset()

	err = s.getDeleteRowBuf(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []byte("(1,2)"), s.rowBuf)
}

func Test_mysqlSinker_getInsertRowBuf(t *testing.T) {
	s := &mysqlSinker{
		rowBuf:    make([]byte, 0, 1024),
		insertRow: []any{uint64(1), []byte("a")},
		insertTypes: []*types.Type{
			{Oid: types.T_uint64},
			{Oid: types.T_varchar},
		},
	}
	s.insertRowPrefix = []byte("(")
	s.insertColSeparator = []byte(",")
	s.insertRowSuffix = []byte(")")
	s.insertRowSeparator = []byte(",")

	err := s.getInsertRowBuf(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []byte("(1,'a')"), s.rowBuf)
}

func Test_mysqlSinker_Sink(t *testing.T) {
	ctx := context.Background()
	t0 := types.BuildTS(0, 1)
	t1 := types.BuildTS(1, 1)
	t2 := types.BuildTS(2, 1)

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery("SELECT @@max_allowed_packet").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(CDCDefaultTaskExtra_MaxSQLLen))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))

	sink := &mysqlSink{
		user:          "root",
		password:      "123456",
		ip:            "127.0.0.1",
		port:          3306,
		retryTimes:    3,
		retryDuration: 3 * time.Second,
		conn:          db,
	}

	dbTblInfo := &DbTableInfo{
		SinkDbName:  "dbName",
		SinkTblName: "tblName",
	}

	u, _ := InitCDCWatermarkUpdaterForTest(t)
	u.Start()
	defer u.Stop()
	u.UpdateWatermarkOnly(context.Background(), &WatermarkKey{
		AccountId: 1,
		TaskId:    "taskID-1",
		DBName:    "db1",
		TableName: "t1",
	}, &t0)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{
				Name: "pk",
				Typ:  plan.Type{Id: int32(types.T_uint64)},
			},
		},
		Name2ColIndex: map[string]int32{"pk": 0},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"pk"},
		},
	}

	ar := NewCdcActiveRoutine()

	s := NewMysqlSinker(
		sink,
		1,
		"task-1",
		dbTblInfo,
		u,
		tableDef,
		ar,
		CDCDefaultTaskExtra_MaxSQLLen,
		false,
	)
	s.ClearError()
	go s.Run(ctx, ar)
	defer func() {
		// call dummy to guarantee sqls has been sent, then close
		s.SendDummy()
		s.Close()
	}()

	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer {
			return types.NewPacker()
		},
		func(packer *types.Packer) {
			packer.Reset()
		},
		func(packer *types.Packer) {
			packer.Close()
		},
	)
	var packer *types.Packer
	put := packerPool.Get(&packer)
	defer put.Put()

	// first receive a ckp
	ckpBat := batch.New([]string{"a", "ts"})
	ckpBat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1, 2, 3}, nil)
	ckpBat.Vecs[1] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil)
	ckpBat.SetRowCount(3)

	s.Sink(ctx, &DecoderOutput{
		outputTyp:     OutputTypeSnapshot,
		fromTs:        t0,
		toTs:          t1,
		checkpointBat: ckpBat,
	})
	assert.NoError(t, err)
	s.Sink(ctx, &DecoderOutput{
		noMoreData: true,
		fromTs:     t0,
		toTs:       t1,
	})
	assert.NoError(t, err)

	// receive a tail
	insertAtomicBat := NewAtomicBatch(testutil.TestUtilMp)
	insertBat := batch.New([]string{"a", "ts"})
	insertBat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1, 2, 3}, nil)
	insertBat.Vecs[1] = testutil.MakeTSVector([]types.TS{t1}, nil)
	insertBat.SetRowCount(3)
	insertAtomicBat.Append(packer, insertBat, 1, 0)

	deleteAtomicBat := NewAtomicBatch(testutil.TestUtilMp)
	deleteBat := batch.New([]string{"a", "ts"})
	deleteBat.Vecs[0] = testutil.MakeUint64Vector([]uint64{4}, nil)
	deleteBat.Vecs[1] = testutil.MakeTSVector([]types.TS{t1}, nil)
	deleteBat.SetRowCount(1)
	deleteAtomicBat.Append(packer, deleteBat, 1, 0)

	s.Sink(ctx, &DecoderOutput{
		outputTyp:      OutputTypeTail,
		fromTs:         t1,
		toTs:           t2,
		insertAtmBatch: insertAtomicBat,
		deleteAtmBatch: deleteAtomicBat,
	})
	assert.NoError(t, err)

	s.Sink(ctx, &DecoderOutput{
		outputTyp:      OutputTypeTail,
		fromTs:         t1,
		toTs:           t2,
		insertAtmBatch: insertAtomicBat,
		deleteAtmBatch: NewAtomicBatch(testutil.TestUtilMp),
	})
	assert.NoError(t, err)

	s.Sink(ctx, &DecoderOutput{
		outputTyp:      OutputTypeTail,
		fromTs:         t1,
		toTs:           t2,
		insertAtmBatch: NewAtomicBatch(testutil.TestUtilMp),
		deleteAtmBatch: deleteAtomicBat,
	})
	assert.NoError(t, err)

	s.Sink(ctx, &DecoderOutput{
		noMoreData: true,
		fromTs:     t1,
		toTs:       t2,
	})
	assert.NoError(t, err)
}

func Test_mysqlSinker_Sink_NoMoreData(t *testing.T) {
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(".*").WillReturnError(moerr.NewInternalErrorNoCtx(""))

	dbTblInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
	}

	watermarkUpdater, _ := InitCDCWatermarkUpdaterForTest(t)
	watermarkUpdater.Start()
	defer watermarkUpdater.Stop()
	wmTS := types.BuildTS(0, 1)
	wmKey := WatermarkKey{
		AccountId: 0,
		TaskId:    "taskID-1",
		DBName:    "db1",
		TableName: "t1",
	}
	getTS, err := watermarkUpdater.GetOrAddCommitted(
		context.Background(),
		&wmKey,
		&wmTS,
	)
	assert.NoError(t, err)
	assert.Equal(t, wmTS, getTS)

	ar := NewCdcActiveRoutine()

	s := &mysqlSinker{
		mysql: &mysqlSink{
			user:          "root",
			password:      "123456",
			ip:            "127.0.0.1",
			port:          3306,
			retryTimes:    3,
			retryDuration: 5 * time.Millisecond,
			conn:          db,
		},
		ar:               ar,
		accountId:        wmKey.AccountId,
		taskId:           wmKey.TaskId,
		dbTblInfo:        dbTblInfo,
		watermarkUpdater: watermarkUpdater,
		preRowType:       DeleteRow,
	}
	s.sqlBufs[0] = make([]byte, 128, 1024)
	s.sqlBufs[1] = make([]byte, 0, 1024)
	s.curBufIdx = 0
	s.sqlBuf = s.sqlBufs[s.curBufIdx]
	s.preSqlBufLen = 128
	s.sqlBufSendCh = make(chan []byte)
	s.ClearError()
	go s.Run(ctx, ar)
	defer func() {
		// call dummy to guarantee sqls has been sent, then close
		s.SendDummy()
		s.Close()
	}()

	s.Sink(ctx, &DecoderOutput{
		noMoreData: true,
		toTs:       types.BuildTS(1, 1),
	})
	s.SendDummy()
	err = s.Error()
	assert.Error(t, err)
}

func Test_mysqlSinker_sinkSnapshot(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(".*").WillReturnError(moerr.NewInternalErrorNoCtx(""))

	sinker := &mysqlSinker{
		mysql: &mysqlSink{
			user:          "root",
			password:      "123456",
			ip:            "127.0.0.1",
			port:          3306,
			retryTimes:    3,
			retryDuration: 3 * time.Second,
			conn:          db,
		},
		ar:     NewCdcActiveRoutine(),
		sqlBuf: make([]byte, 1024),
	}

	insertBat := batch.New([]string{"a", "ts"})
	insertBat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1}, nil)
	insertBat.Vecs[1] = testutil.MakeTSVector([]types.TS{types.BuildTS(0, 1)}, nil)
	sinker.sinkSnapshot(context.Background(), insertBat)
}

func Test_mysqlSinker_sinkDelete(t *testing.T) {
	type fields struct {
		mysql            Sink
		dbTblInfo        *DbTableInfo
		watermarkUpdater *CDCWatermarkUpdater
		sqlBuf           []byte
		rowBuf           []byte
		insertPrefix     []byte
		deletePrefix     []byte
		tsInsertPrefix   []byte
		tsDeletePrefix   []byte
		insertTypes      []*types.Type
		deleteTypes      []*types.Type
		insertRow        []any
		deleteRow        []any
		preRowType       RowType
	}
	type args struct {
		ctx        context.Context
		insertIter *atomicBatchRowIter
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &mysqlSinker{
				mysql:            tt.fields.mysql,
				dbTblInfo:        tt.fields.dbTblInfo,
				watermarkUpdater: tt.fields.watermarkUpdater,
				sqlBuf:           tt.fields.sqlBuf,
				rowBuf:           tt.fields.rowBuf,
				insertPrefix:     tt.fields.insertPrefix,
				deletePrefix:     tt.fields.deletePrefix,
				tsInsertPrefix:   tt.fields.tsInsertPrefix,
				tsDeletePrefix:   tt.fields.tsDeletePrefix,
				insertTypes:      tt.fields.insertTypes,
				deleteTypes:      tt.fields.deleteTypes,
				insertRow:        tt.fields.insertRow,
				deleteRow:        tt.fields.deleteRow,
				preRowType:       tt.fields.preRowType,
			}
			tt.wantErr(t, s.sinkDelete(tt.args.ctx, tt.args.insertIter), fmt.Sprintf("sinkDelete(%v, %v)", tt.args.ctx, tt.args.insertIter))
		})
	}
}

func Test_mysqlSinker_sinkInsert(t *testing.T) {
	type fields struct {
		mysql            Sink
		dbTblInfo        *DbTableInfo
		watermarkUpdater *CDCWatermarkUpdater
		sqlBuf           []byte
		rowBuf           []byte
		insertPrefix     []byte
		deletePrefix     []byte
		tsInsertPrefix   []byte
		tsDeletePrefix   []byte
		insertTypes      []*types.Type
		deleteTypes      []*types.Type
		insertRow        []any
		deleteRow        []any
		preRowType       RowType
	}
	type args struct {
		ctx        context.Context
		insertIter *atomicBatchRowIter
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &mysqlSinker{
				mysql:            tt.fields.mysql,
				dbTblInfo:        tt.fields.dbTblInfo,
				watermarkUpdater: tt.fields.watermarkUpdater,
				sqlBuf:           tt.fields.sqlBuf,
				rowBuf:           tt.fields.rowBuf,
				insertPrefix:     tt.fields.insertPrefix,
				deletePrefix:     tt.fields.deletePrefix,
				tsInsertPrefix:   tt.fields.tsInsertPrefix,
				tsDeletePrefix:   tt.fields.tsDeletePrefix,
				insertTypes:      tt.fields.insertTypes,
				deleteTypes:      tt.fields.deleteTypes,
				insertRow:        tt.fields.insertRow,
				deleteRow:        tt.fields.deleteRow,
				preRowType:       tt.fields.preRowType,
			}
			tt.wantErr(t, s.sinkInsert(tt.args.ctx, tt.args.insertIter), fmt.Sprintf("sinkInsert(%v, %v)", tt.args.ctx, tt.args.insertIter))
		})
	}
}

func Test_mysqlsink(t *testing.T) {
	wmark, _ := InitCDCWatermarkUpdaterForTest(t)
	wmark.Start()
	defer wmark.Stop()
	ts := types.BuildTS(100, 100)
	wmark.UpdateWatermarkOnly(context.Background(), &WatermarkKey{
		AccountId: 0,
		TaskId:    "taskID-1",
		DBName:    "db1",
		TableName: "t1",
	}, &ts)
	sink := &mysqlSinker{
		watermarkUpdater: wmark,
		dbTblInfo: &DbTableInfo{
			SourceTblId:   0,
			SourceTblName: "t1",
			SourceDbName:  "db1",
		},
	}
	ts = types.BuildTS(100, 100)
	sink.watermarkUpdater.UpdateWatermarkOnly(context.Background(), &WatermarkKey{
		AccountId: 0,
		TaskId:    "taskID-1",
		DBName:    "db1",
		TableName: "t1",
	},
		&ts,
	)
	sink.Sink(context.Background(), &DecoderOutput{})
}

func Test_mysqlSinker_sinkTail(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))

	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer {
			return types.NewPacker()
		},
		func(packer *types.Packer) {
			packer.Reset()
		},
		func(packer *types.Packer) {
			packer.Close()
		},
	)
	var packer *types.Packer
	put := packerPool.Get(&packer)
	defer put.Put()

	sinker := &mysqlSinker{
		mysql: &mysqlSink{
			user:          "root",
			password:      "123456",
			ip:            "127.0.0.1",
			port:          3306,
			retryTimes:    3,
			retryDuration: 3 * time.Second,
			conn:          db,
		},
		ar:           NewCdcActiveRoutine(),
		sqlBuf:       make([]byte, 1024),
		sqlBufSendCh: make(chan []byte, 1024),
	}
	sinker.sqlBufs[0] = make([]byte, sqlBufReserved)
	sinker.sqlBufs[1] = make([]byte, sqlBufReserved)

	insertAtomicBat := NewAtomicBatch(testutil.TestUtilMp)
	insertBat := batch.New([]string{"a", "ts"})
	insertBat.Vecs[0] = testutil.MakeUint64Vector([]uint64{1}, nil)
	insertBat.Vecs[1] = testutil.MakeTSVector([]types.TS{types.BuildTS(0, 1)}, nil)
	insertBat.SetRowCount(3)
	insertAtomicBat.Append(packer, insertBat, 1, 0)

	deleteAtomicBat := NewAtomicBatch(testutil.TestUtilMp)
	sinker.sinkTail(context.Background(), insertAtomicBat, deleteAtomicBat)
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

func Test_mysqlSinker_Close(t *testing.T) {
	sink := &mysqlSink{
		user:          "root",
		password:      "123456",
		ip:            "127.0.0.1",
		port:          3306,
		retryTimes:    3,
		retryDuration: 3 * time.Second,
	}

	sinker := &mysqlSinker{
		mysql:        sink,
		sqlBufSendCh: make(chan []byte),
	}

	sinker.Close()
}

func Test_mysqlSinker_SendBeginCommitRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	ar := NewCdcActiveRoutine()
	s := &mysqlSinker{
		mysql: &mysqlSink{
			retryTimes:    3,
			retryDuration: 3 * time.Second,
			conn:          db,
		},
		ar:           ar,
		sqlBufSendCh: make(chan []byte),
	}
	s.ClearError()
	go s.Run(context.Background(), ar)
	defer func() {
		// call dummy to guarantee sqls has been sent, then close
		s.SendDummy()
		s.Close()
	}()

	mock.ExpectBegin()
	mock.ExpectCommit()
	s.SendBegin()
	assert.NoError(t, err)
	s.SendCommit()
	assert.NoError(t, err)
	s.SendDummy()

	mock.ExpectBegin()
	mock.ExpectRollback()
	s.SendBegin()
	assert.NoError(t, err)
	s.SendRollback()
	assert.NoError(t, err)
	s.SendDummy()

	// begin error
	mock.ExpectBegin().WillReturnError(moerr.NewInternalErrorNoCtx("begin error"))
	s.SendBegin()
	s.SendDummy()
	assert.Error(t, s.Error())
	s.ClearError()

	// commit error
	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(moerr.NewInternalErrorNoCtx("commit error"))
	s.SendBegin()
	s.SendCommit()
	s.SendDummy()
	assert.Error(t, s.Error())
	s.ClearError()

	// rollback error
	mock.ExpectBegin()
	mock.ExpectRollback().WillReturnError(moerr.NewInternalErrorNoCtx("rollback error"))
	s.SendBegin()
	s.SendRollback()
	s.SendDummy()
	assert.Error(t, s.Error())
	s.ClearError()
}

func Test_consoleSinker_SendBeginCommitRollback(t *testing.T) {
	s := &consoleSinker{}
	s.SendBegin()
	s.SendCommit()
	s.SendRollback()
}

func Test_mysqlSinker_ClearError(t *testing.T) {
	s := &mysqlSinker{}
	s.SetError(moerr.NewInternalErrorNoCtx("test err"))
	assert.Error(t, s.Error())

	s.ClearError()
	assert.Nil(t, s.Error())
}

func Test_mysqlSinker_Reset(t *testing.T) {
	s := &mysqlSinker{}
	s.sqlBufs[0] = make([]byte, sqlBufReserved, 1024)
	s.sqlBufs[1] = make([]byte, sqlBufReserved, 1024)
	s.curBufIdx = 0
	s.sqlBuf = s.sqlBufs[s.curBufIdx]
	s.Reset()
}

func Test_Error(t *testing.T) {

	tsInsertPrefix := "/* tsInsertPrefix */REPLACE INTO `db`.`table` VALUES "
	tsDeletePrefix := "/* tsDeletePrefix */DELETE FROM `db`.`table` WHERE a IN ("

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	sink := &mysqlSink{
		user:          "root",
		password:      "123456",
		ip:            "127.0.0.1",
		port:          3306,
		retryTimes:    3,
		retryDuration: 3 * time.Second,
		conn:          db,
	}
	defer sink.Close()

	ar := NewCdcActiveRoutine()
	s := &mysqlSinker{
		mysql:          sink,
		tsInsertPrefix: []byte(tsInsertPrefix),
		tsDeletePrefix: []byte(tsDeletePrefix),
		preRowType:     NoOp,
		ar:             ar,
		sqlBufSendCh:   make(chan []byte),
	}
	defer s.Close()
	s.SetError(errors.ErrUnsupported)
	assert.Equal(t, "internal error: convert go error to mo error unsupported operation", s.Error().Error())
	s.SetError(moerr.NewFileNotFound(context.Background(), "test error"))
	assert.True(t, moerr.IsMoErrCode(s.Error(), moerr.ErrFileNotFound))

	var merr *moerr.Error
	s.SetError(merr)

	assert.False(t, moerr.IsMoErrCode(s.Error(), moerr.ErrFileNotFound))

	s.SetError(nil)
	assert.Nil(t, s.Error())
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
