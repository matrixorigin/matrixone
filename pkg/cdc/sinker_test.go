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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
)

func TestNewSinker(t *testing.T) {
	type args struct {
		sinkUri          UriInfo
		dbTblInfo        *DbTableInfo
		watermarkUpdater *WatermarkUpdater
		tableDef         *plan.TableDef
		retryTimes       int
		retryDuration    time.Duration
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
					SinkTyp: ConsoleSink,
				},
				dbTblInfo:        &DbTableInfo{},
				watermarkUpdater: nil,
				tableDef:         nil,
				retryTimes:       0,
				retryDuration:    0,
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
					SinkTyp: MysqlSink,
				},
				dbTblInfo:        &DbTableInfo{},
				watermarkUpdater: nil,
				tableDef:         nil,
				retryTimes:       0,
				retryDuration:    0,
			},
			want:    nil,
			wantErr: assert.NoError,
		},
	}

	sinkStub := gostub.Stub(&NewMysqlSink, func(_, _, _ string, _, _ int, _ time.Duration) (Sink, error) {
		return nil, nil
	})
	defer sinkStub.Reset()

	sinkerStub := gostub.Stub(&NewMysqlSinker, func(_ Sink, _ *DbTableInfo, _ *WatermarkUpdater, _ *plan.TableDef) Sinker {
		return nil
	})
	defer sinkerStub.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewSinker(tt.args.sinkUri, tt.args.dbTblInfo, tt.args.watermarkUpdater, tt.args.tableDef, tt.args.retryTimes, tt.args.retryDuration)
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
		watermarkUpdater *WatermarkUpdater
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
	type fields struct {
		dbTblInfo        *DbTableInfo
		watermarkUpdater *WatermarkUpdater
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
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &consoleSinker{
				dbTblInfo:        tt.fields.dbTblInfo,
				watermarkUpdater: tt.fields.watermarkUpdater,
			}
			tt.wantErr(t, s.Sink(tt.args.ctx, tt.args.data), fmt.Sprintf("Sink(%v, %v)", tt.args.ctx, tt.args.data))
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
			},
			wantErr: assert.NoError,
		},
	}

	stub := gostub.Stub(&openDbConn, func(_, _, _ string, _ int) (_ *sql.DB, _ error) {
		return nil, nil
	})
	defer stub.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMysqlSink(tt.args.user, tt.args.password, tt.args.ip, tt.args.port, tt.args.retryTimes, tt.args.retryDuration)
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
	mock.ExpectExec("sql").WillReturnResult(sqlmock.NewResult(1, 1))

	sink := &mysqlSink{
		user:          "root",
		password:      "123456",
		ip:            "127.0.0.1",
		port:          3306,
		retryTimes:    DefaultRetryTimes,
		retryDuration: DefaultRetryDuration,
		conn:          db,
	}
	err = sink.Send(context.Background(), "sql")
	assert.NoError(t, err)
}

func TestNewMysqlSinker(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery("SELECT @@max_allowed_packet").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(DefaultMaxAllowedPacket))

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
	NewMysqlSinker(sink, dbTblInfo, nil, tableDef)
}

func Test_mysqlSinker_Sink(t *testing.T) {
	type fields struct {
		mysql            Sink
		dbTblInfo        *DbTableInfo
		watermarkUpdater *WatermarkUpdater
		maxAllowedPacket uint64
		sqlBuf           []byte
		rowBuf           []byte
		insertPrefix     []byte
		deletePrefix     []byte
		tsInsertPrefix   []byte
		tsDeletePrefix   []byte
		insertIters      []RowIterator
		deleteIters      []RowIterator
		insertTypes      []*types.Type
		deleteTypes      []*types.Type
		insertRow        []any
		deleteRow        []any
		preRowType       RowType
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
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &mysqlSinker{
				mysql:            tt.fields.mysql,
				dbTblInfo:        tt.fields.dbTblInfo,
				watermarkUpdater: tt.fields.watermarkUpdater,
				maxAllowedPacket: tt.fields.maxAllowedPacket,
				sqlBuf:           tt.fields.sqlBuf,
				rowBuf:           tt.fields.rowBuf,
				insertPrefix:     tt.fields.insertPrefix,
				deletePrefix:     tt.fields.deletePrefix,
				tsInsertPrefix:   tt.fields.tsInsertPrefix,
				tsDeletePrefix:   tt.fields.tsDeletePrefix,
				insertIters:      tt.fields.insertIters,
				deleteIters:      tt.fields.deleteIters,
				insertTypes:      tt.fields.insertTypes,
				deleteTypes:      tt.fields.deleteTypes,
				insertRow:        tt.fields.insertRow,
				deleteRow:        tt.fields.deleteRow,
				preRowType:       tt.fields.preRowType,
			}
			tt.wantErr(t, s.Sink(tt.args.ctx, tt.args.data), fmt.Sprintf("Sink(%v, %v)", tt.args.ctx, tt.args.data))
		})
	}
}

func Test_mysqlSinker_appendSqlBuf(t *testing.T) {
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
		retryTimes:    DefaultRetryTimes,
		retryDuration: DefaultRetryDuration,
		conn:          db,
	}

	s := &mysqlSinker{
		mysql:          sink,
		sqlBuf:         make([]byte, 0, len(tsDeletePrefix)+len("delete")+2),
		tsInsertPrefix: []byte(tsInsertPrefix),
		tsDeletePrefix: []byte(tsDeletePrefix),
		preRowType:     NoOp,
	}

	// test insert
	s.sqlBuf = append(s.sqlBuf[:0], s.tsInsertPrefix...)
	s.rowBuf = []byte("insert")
	// not exceed cap
	err = s.appendSqlBuf(context.Background(), InsertRow)
	assert.NoError(t, err)
	assert.Equal(t, []byte(tsInsertPrefix+"insert"), s.sqlBuf)
	// exceed cap
	err = s.appendSqlBuf(context.Background(), InsertRow)
	assert.NoError(t, err)
	assert.Equal(t, []byte(tsInsertPrefix+"insert"), s.sqlBuf)

	// test delete
	s.sqlBuf = append(s.sqlBuf[:0], s.tsDeletePrefix...)
	s.rowBuf = []byte("delete")
	// not exceed cap
	err = s.appendSqlBuf(context.Background(), DeleteRow)
	assert.NoError(t, err)
	assert.Equal(t, []byte(tsDeletePrefix+"delete"), s.sqlBuf)
	// exceed cap
	err = s.appendSqlBuf(context.Background(), DeleteRow)
	assert.NoError(t, err)
	assert.Equal(t, []byte(tsDeletePrefix+"delete"), s.sqlBuf)
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
	err := s.getInsertRowBuf(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []byte("(1,'a')"), s.rowBuf)
}

func Test_mysqlSinker_sinkCkp(t *testing.T) {
	type fields struct {
		mysql            Sink
		dbTblInfo        *DbTableInfo
		watermarkUpdater *WatermarkUpdater
		maxAllowedPacket uint64
		sqlBuf           []byte
		rowBuf           []byte
		insertPrefix     []byte
		deletePrefix     []byte
		tsInsertPrefix   []byte
		tsDeletePrefix   []byte
		insertIters      []RowIterator
		deleteIters      []RowIterator
		insertTypes      []*types.Type
		deleteTypes      []*types.Type
		insertRow        []any
		deleteRow        []any
		preRowType       RowType
	}
	type args struct {
		ctx context.Context
		bat *batch.Batch
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
				maxAllowedPacket: tt.fields.maxAllowedPacket,
				sqlBuf:           tt.fields.sqlBuf,
				rowBuf:           tt.fields.rowBuf,
				insertPrefix:     tt.fields.insertPrefix,
				deletePrefix:     tt.fields.deletePrefix,
				tsInsertPrefix:   tt.fields.tsInsertPrefix,
				tsDeletePrefix:   tt.fields.tsDeletePrefix,
				insertIters:      tt.fields.insertIters,
				deleteIters:      tt.fields.deleteIters,
				insertTypes:      tt.fields.insertTypes,
				deleteTypes:      tt.fields.deleteTypes,
				insertRow:        tt.fields.insertRow,
				deleteRow:        tt.fields.deleteRow,
				preRowType:       tt.fields.preRowType,
			}
			tt.wantErr(t, s.sinkCkp(tt.args.ctx, tt.args.bat), fmt.Sprintf("sinkCkp(%v, %v)", tt.args.ctx, tt.args.bat))
		})
	}
}

func Test_mysqlSinker_sinkDelete(t *testing.T) {
	type fields struct {
		mysql            Sink
		dbTblInfo        *DbTableInfo
		watermarkUpdater *WatermarkUpdater
		maxAllowedPacket uint64
		sqlBuf           []byte
		rowBuf           []byte
		insertPrefix     []byte
		deletePrefix     []byte
		tsInsertPrefix   []byte
		tsDeletePrefix   []byte
		insertIters      []RowIterator
		deleteIters      []RowIterator
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
				maxAllowedPacket: tt.fields.maxAllowedPacket,
				sqlBuf:           tt.fields.sqlBuf,
				rowBuf:           tt.fields.rowBuf,
				insertPrefix:     tt.fields.insertPrefix,
				deletePrefix:     tt.fields.deletePrefix,
				tsInsertPrefix:   tt.fields.tsInsertPrefix,
				tsDeletePrefix:   tt.fields.tsDeletePrefix,
				insertIters:      tt.fields.insertIters,
				deleteIters:      tt.fields.deleteIters,
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
		watermarkUpdater *WatermarkUpdater
		maxAllowedPacket uint64
		sqlBuf           []byte
		rowBuf           []byte
		insertPrefix     []byte
		deletePrefix     []byte
		tsInsertPrefix   []byte
		tsDeletePrefix   []byte
		insertIters      []RowIterator
		deleteIters      []RowIterator
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
				maxAllowedPacket: tt.fields.maxAllowedPacket,
				sqlBuf:           tt.fields.sqlBuf,
				rowBuf:           tt.fields.rowBuf,
				insertPrefix:     tt.fields.insertPrefix,
				deletePrefix:     tt.fields.deletePrefix,
				tsInsertPrefix:   tt.fields.tsInsertPrefix,
				tsDeletePrefix:   tt.fields.tsDeletePrefix,
				insertIters:      tt.fields.insertIters,
				deleteIters:      tt.fields.deleteIters,
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

func Test_mysqlSinker_sinkRemain(t *testing.T) {
	type fields struct {
		mysql            Sink
		dbTblInfo        *DbTableInfo
		watermarkUpdater *WatermarkUpdater
		maxAllowedPacket uint64
		sqlBuf           []byte
		rowBuf           []byte
		insertPrefix     []byte
		deletePrefix     []byte
		tsInsertPrefix   []byte
		tsDeletePrefix   []byte
		insertIters      []RowIterator
		deleteIters      []RowIterator
		insertTypes      []*types.Type
		deleteTypes      []*types.Type
		insertRow        []any
		deleteRow        []any
		preRowType       RowType
	}
	type args struct {
		ctx context.Context
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
				maxAllowedPacket: tt.fields.maxAllowedPacket,
				sqlBuf:           tt.fields.sqlBuf,
				rowBuf:           tt.fields.rowBuf,
				insertPrefix:     tt.fields.insertPrefix,
				deletePrefix:     tt.fields.deletePrefix,
				tsInsertPrefix:   tt.fields.tsInsertPrefix,
				tsDeletePrefix:   tt.fields.tsDeletePrefix,
				insertIters:      tt.fields.insertIters,
				deleteIters:      tt.fields.deleteIters,
				insertTypes:      tt.fields.insertTypes,
				deleteTypes:      tt.fields.deleteTypes,
				insertRow:        tt.fields.insertRow,
				deleteRow:        tt.fields.deleteRow,
				preRowType:       tt.fields.preRowType,
			}
			tt.wantErr(t, s.sinkRemain(tt.args.ctx), fmt.Sprintf("sinkRemain(%v)", tt.args.ctx))
		})
	}
}

func Test_mysqlSinker_sinkTail(t *testing.T) {
	type fields struct {
		mysql            Sink
		dbTblInfo        *DbTableInfo
		watermarkUpdater *WatermarkUpdater
		maxAllowedPacket uint64
		sqlBuf           []byte
		rowBuf           []byte
		insertPrefix     []byte
		deletePrefix     []byte
		tsInsertPrefix   []byte
		tsDeletePrefix   []byte
		insertIters      []RowIterator
		deleteIters      []RowIterator
		insertTypes      []*types.Type
		deleteTypes      []*types.Type
		insertRow        []any
		deleteRow        []any
		preRowType       RowType
	}
	type args struct {
		ctx         context.Context
		insertBatch *AtomicBatch
		deleteBatch *AtomicBatch
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
				maxAllowedPacket: tt.fields.maxAllowedPacket,
				sqlBuf:           tt.fields.sqlBuf,
				rowBuf:           tt.fields.rowBuf,
				insertPrefix:     tt.fields.insertPrefix,
				deletePrefix:     tt.fields.deletePrefix,
				tsInsertPrefix:   tt.fields.tsInsertPrefix,
				tsDeletePrefix:   tt.fields.tsDeletePrefix,
				insertIters:      tt.fields.insertIters,
				deleteIters:      tt.fields.deleteIters,
				insertTypes:      tt.fields.insertTypes,
				deleteTypes:      tt.fields.deleteTypes,
				insertRow:        tt.fields.insertRow,
				deleteRow:        tt.fields.deleteRow,
				preRowType:       tt.fields.preRowType,
			}
			tt.wantErr(t, s.sinkTail(tt.args.ctx, tt.args.insertBatch, tt.args.deleteBatch), fmt.Sprintf("sinkTail(%v, %v, %v)", tt.args.ctx, tt.args.insertBatch, tt.args.deleteBatch))
		})
	}
}
