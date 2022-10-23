// Copyright 2021 Matrix Origin
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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func Test_readTextFile(t *testing.T) {
	data, err := os.ReadFile("test/loadfile.csv")
	require.NoError(t, err)
	fmt.Printf("%v\n", data)
}

/*func Test_loadJSON(t *testing.T) {
	convey.Convey("loadJSON succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockTxnEngine(ctrl)
    eng.EXPECT().Hints().Return(engine.Hints{
      CommitOrRollbackTimeout: time.Second,
    }).AnyTimes()
		txn := mock_frontend.NewMockTxn(ctrl)
		txn.EXPECT().GetCtx().Return(nil).AnyTimes()
		txn.EXPECT().Commit().Return(nil).AnyTimes()
		txn.EXPECT().Rollback().Return(nil).AnyTimes()
		txn.EXPECT().String().Return("txn0").AnyTimes()
		eng.EXPECT().StartTxn(nil).Return(txn, nil).AnyTimes()

		db := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		tableDefs := []engine.TableDef{
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_json},
					Name: "a"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_varchar},
					Name: "b"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint8},
					Name: "c"}},
		}
		ctx := context.TODO()
		rel.EXPECT().TableDefs(gomock.Any()).Return(tableDefs, nil).AnyTimes()
		cnt := 0
		rel.EXPECT().Write(ctx, gomock.Any()).DoAndReturn(
			func(a, b interface{}) error {
				cnt++
				if cnt == 1 {
					return nil
				} else if cnt == 2 {
					return context.DeadlineExceeded
				}

				return nil
			},
		).AnyTimes()
		db.EXPECT().Relation(ctx, gomock.Any()).Return(rel, nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(db, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()

		cws := []ComputationWrapper{}
		var self_handle_sql = []string{
			"load data " +
				"infile 'test/loadfile6' " +
				"ignore " +
				"INTO TABLE T.A " +
				"FIELDS TERMINATED BY '\t' " +
				"ignore 1 lines ",
			"load data " +
				"infile 'test/loadfile6' " +
				"ignore " +
				"INTO TABLE T.A " +
				"FIELDS TERMINATED BY '\t' " +
				"ignore 1 lines " +
				"(a, b, c)",
		}
		for i := 0; i < len(self_handle_sql); i++ {
			select_2 := mock_frontend.NewMockComputationWrapper(ctrl)
			stmts, err := parsers.Parse(dialect.MYSQL, self_handle_sql[i])
			convey.So(err, convey.ShouldBeNil)
			select_2.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
			select_2.EXPECT().SetDatabaseName(gomock.Any()).Return(nil).AnyTimes()
			select_2.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			select_2.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()

			cws = append(cws, select_2)
		}

		stubs := gostub.StubFunc(&GetComputationWrapper, cws, nil)
		defer stubs.Reset()

		stubs2 := gostub.StubFunc(&PathExists, true, true, nil)
		defer stubs2.Reset()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng)
		convey.So(err, convey.ShouldBeNil)

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		guestMmu := guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu)
		config.StorageEngine = eng
		defer func() {
			config.StorageEngine = nil
		}()
		ses := NewSession(proto, guestMmu, pu.Mempool, pu, gSysVariables)
    ses.SetRequestContext(ctx)

		mce := NewMysqlCmdExecutor()

		mce.PrepareSessionBeforeExecRequest(ses)

		req := &Request{
			cmd:  int(COM_QUERY),
			data: []byte("test anywhere"),
		}

		resp, err := mce.ExecRequest(req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldBeNil)
	})
}*/

/*func Test_load(t *testing.T) {
	convey.Convey("load succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockTxnEngine(ctrl)
    eng.EXPECT().Hints().Return(engine.Hints{
      CommitOrRollbackTimeout: time.Second,
    }).AnyTimes()
		txn := mock_frontend.NewMockTxn(ctrl)
		txn.EXPECT().GetCtx().Return(nil).AnyTimes()
		txn.EXPECT().GetID().Return(uint64(0)).AnyTimes()
		txn.EXPECT().Commit().Return(nil).AnyTimes()
		txn.EXPECT().Rollback().Return(nil).AnyTimes()
		txn.EXPECT().String().Return("txn0").AnyTimes()
		eng.EXPECT().StartTxn(nil).Return(txn, nil).AnyTimes()

		db := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		//table def
		tableDefs := []engine.TableDef{
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_char},
					Name: "a"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_varchar},
					Name: "b"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint8},
					Name: "c"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int8},
					Name: "d"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint16},
					Name: "e"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int16},
					Name: "f"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint32},
					Name: "g"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int32},
					Name: "h"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint64},
					Name: "i"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int64},
					Name: "j"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_float32},
					Name: "k"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_float64},
					Name: "l"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_date},
					Name: "m"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_datetime},
					Name: "n"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{
						Oid:       types.T_decimal64,
						Size:      0,
						Width:     10,
						Scale:     2,
						Precision: 0,
					},
					Name: "o"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{
						Oid:       types.T_decimal128,
						Size:      0,
						Width:     20,
						Scale:     2,
						Precision: 0,
					},
					Name: "p"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{
						Oid:       types.T_timestamp,
						Size:      0,
						Width:     0,
						Scale:     0,
						Precision: 6,
					},
					Name: "r"}},
		}
		ctx := context.TODO()
		rel.EXPECT().TableDefs(gomock.Any()).Return(tableDefs, nil).AnyTimes()
		cnt := 0
		rel.EXPECT().Write(ctx, gomock.Any()).DoAndReturn(
			func(a, b interface{}) error {
				cnt++
				if cnt == 1 {
					return nil
				} else if cnt == 2 {
					return context.DeadlineExceeded
				}

				return nil
			},
		).AnyTimes()
		db.EXPECT().Relation(ctx, gomock.Any()).Return(rel, nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(db, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		cws := []ComputationWrapper{}

		var self_handle_sql = []string{
			"load data " +
				"infile 'test/loadfile5' " +
				"ignore " +
				"INTO TABLE T.A " +
				"FIELDS TERMINATED BY ',' ",
			"load data " +
				"infile 'test/loadfile5' " +
				"ignore " +
				"INTO TABLE T.A " +
				"FIELDS TERMINATED BY ',' " +
				"(@s,@t,c,d,e,f)",
		}

		for i := 0; i < len(self_handle_sql); i++ {
			select_2 := mock_frontend.NewMockComputationWrapper(ctrl)
			stmts, err := parsers.Parse(dialect.MYSQL, self_handle_sql[i])
			convey.So(err, convey.ShouldBeNil)
			select_2.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
			select_2.EXPECT().SetDatabaseName(gomock.Any()).Return(nil).AnyTimes()
			select_2.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			select_2.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()

			cws = append(cws, select_2)
		}

		stubs := gostub.StubFunc(&GetComputationWrapper, cws, nil)
		defer stubs.Reset()

		stubs2 := gostub.StubFunc(&PathExists, true, true, nil)
		defer stubs2.Reset()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng)
		convey.So(err, convey.ShouldBeNil)

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		guestMmu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)
		ses := NewSession(proto, guestMmu, pu.Mempool, pu, gSysVariables)
    ses.SetRequestContext(ctx)

		mce := NewMysqlCmdExecutor()

		mce.PrepareSessionBeforeExecRequest(ses)

		req := &Request{
			cmd:  int(COM_QUERY),
			data: []byte("test anywhere"),
		}

		resp, err := mce.ExecRequest(ctx, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldBeNil)
	})

	convey.Convey("load failed", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockTxnEngine(ctrl)
    eng.EXPECT().Hints().Return(engine.Hints{
      CommitOrRollbackTimeout: time.Second,
    }).AnyTimes()
		txn := mock_frontend.NewMockTxn(ctrl)
		txn.EXPECT().GetCtx().Return(nil).AnyTimes()
		txn.EXPECT().Commit().Return(nil).AnyTimes()
		txn.EXPECT().Rollback().Return(nil).AnyTimes()
		txn.EXPECT().String().Return("txn0").AnyTimes()
		eng.EXPECT().StartTxn(nil).Return(txn, nil).AnyTimes()

		db := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		//table def
		tableDefs := []engine.TableDef{
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_char},
					Name: "a"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_varchar},
					Name: "b"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint8},
					Name: "c"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int8},
					Name: "d"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint16},
					Name: "e"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int16},
					Name: "f"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint32},
					Name: "g"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int32},
					Name: "h"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint64},
					Name: "i"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int64},
					Name: "j"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_float32},
					Name: "k"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_float64},
					Name: "l"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_date},
					Name: "m"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_datetime},
					Name: "n"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{
						Oid:       types.T_decimal64,
						Size:      0,
						Width:     10,
						Scale:     2,
						Precision: 0,
					},
					Name: "o"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{
						Oid:       types.T_decimal128,
						Size:      0,
						Width:     20,
						Scale:     2,
						Precision: 0,
					},
					Name: "p"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{
						Oid:       types.T_timestamp,
						Size:      0,
						Width:     0,
						Scale:     0,
						Precision: 6,
					},
					Name: "r"}},
		}
		ctx := context.TODO()
		rel.EXPECT().TableDefs(ctx).Return(tableDefs, nil).AnyTimes()
		cnt := 0
		rel.EXPECT().Write(ctx, gomock.Any()).DoAndReturn(
			func(a, b interface{}) error {
				cnt++
				if cnt == 1 {
					return moerr.NewInternalError("fake error")
				} else if cnt == 2 {
					return moerr.NewInternalError("exec timeout")
				}

				return nil
			},
		).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), gomock.Any()).Return(rel, nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(db, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		cws := []*tree.Load{}

		type kase struct {
			sql  string
			fail bool
		}

		kases := []kase{
			{
				sql: "load data " +
					"infile 'test/loadfile5' " +
					"INTO TABLE T.A " +
					"FIELDS TERMINATED BY ',' ",
				fail: true,
			},
			{
				sql: "load data " +
					"infile 'test/loadfile5' " +
					"INTO TABLE T.A " +
					"FIELDS TERMINATED BY ',' " +
					"(@a,b,d,e)",
				fail: true,
			},
			{
				sql: "load data " +
					"infile 'test/loadfile5' " +
					"ignore " +
					"INTO TABLE T.A " +
					"FIELDS TERMINATED BY ',' " +
					"(@a,b,d,e)",
				fail: false,
			},
			{
				sql: "load data " +
					"infile 'test/loadfile5' " +
					"INTO TABLE T.A " +
					"FIELDS TERMINATED BY ',' ",
				fail: false,
			},
		}

		for i := 0; i < len(kases); i++ {
			stmts, err := parsers.Parse(dialect.MYSQL, kases[i].sql)
			convey.So(err, convey.ShouldBeNil)
			cws = append(cws, stmts[0].(*tree.Load))
		}

		stubs2 := gostub.StubFunc(&PathExists, true, true, nil)
		defer stubs2.Reset()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng)
		convey.So(err, convey.ShouldBeNil)

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		guestMmu := guest.New(pu.SV.GuestMmuLimitation, pu.HostMmu)

		ses := NewSession(proto, guestMmu, pu.Mempool, pu, gSysVariables)
    ses.SetRequestContext(ctx)

		mce := NewMysqlCmdExecutor()

		mce.PrepareSessionBeforeExecRequest(ses)

		var row2col *gostub.Stubs = nil
		for i := 0; i < len(kases); i++ {
			if i == 3 {
				row2col = gostub.Stub(&row2colChoose, false)
			}

			_, err := mce.LoadLoop(context.TODO(), cws[i], db, rel, "T")
			if kases[i].fail {
				convey.So(err, convey.ShouldBeError)
			} else {
				convey.So(err, convey.ShouldBeNil)
			}

			if i == 3 {
				row2col.Reset()
			}
		}
	})
}*/

func Test_rowToColumnAndSaveToStorage(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("rowToColumnAndSaveToStorage succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().Write(ctx, gomock.Any()).Return(nil).AnyTimes()

		// XXX the test is so strange, curBatchSize is used as both batch size and column count?
		var curBatchSize = 13
		handler := &WriteBatchHandler{
			SharePart: SharePart{
				tableHandler:               rel,
				lineIdx:                    curBatchSize,
				maxFieldCnt:                curBatchSize,
				simdCsvLineArray:           make([][]string, curBatchSize),
				dataColumnId2TableColumnId: make([]int, curBatchSize),
				ses:                        &Session{timeZone: time.Local},
				result:                     &LoadResult{},
				ignoreFieldError:           true},
			batchData: &batch.Batch{
				Vecs:  make([]*vector.Vector, curBatchSize),
				Attrs: make([]string, curBatchSize),
				Cnt:   1,
			},
			ThreadInfo: &ThreadInfo{},
		}
		mp, err := mpool.NewMPool("session", 0, mpool.NoFixed)
		if err != nil {
			panic(err)
		}
		proc := process.New(
			ctx,
			mp,
			nil,
			nil,
			nil,
		)
		field := [][]string{{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "8", "9"}}
		Oid := []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16,
			types.T_uint32, types.T_uint64, types.T_float32, types.T_float64, types.T_char, types.T_date, types.T_datetime}
		fmt.Println(Oid)
		handler.simdCsvLineArray[0] = field[0]
		for i := 0; i < curBatchSize; i++ {
			handler.dataColumnId2TableColumnId[i] = i
			handler.batchData.Vecs[i] = vector.PreAllocType(Oid[i].ToType(), curBatchSize, curBatchSize, proc.Mp())
			handler.batchData.Vecs[i].SetOriginal(false)
		}
		var force = false
		convey.So(rowToColumnAndSaveToStorage(handler, proc, force, row2colChoose), convey.ShouldBeNil)

		row2colChoose = false
		handler.lineIdx = 1
		convey.So(rowToColumnAndSaveToStorage(handler, proc, force, row2colChoose), convey.ShouldBeNil)

		handler.maxFieldCnt = 0
		convey.So(rowToColumnAndSaveToStorage(handler, proc, force, row2colChoose), convey.ShouldBeNil)

		handler.batchData.Clean(proc.Mp())
		row2colChoose = true

		handler.batchData.Vecs = make([]*vector.Vector, curBatchSize)
		handler.ignoreFieldError = false
		for i := 0; i < curBatchSize; i++ {
			if Oid[i] == types.T_char {
				continue
			}
			// XXX Vecs[0]?   What are we testing?
			handler.batchData.Vecs[i] = vector.PreAllocType(Oid[i].ToType(), curBatchSize, curBatchSize, proc.Mp())
			convey.So(rowToColumnAndSaveToStorage(handler, proc, force, row2colChoose), convey.ShouldNotBeNil)
			vector.Clean(handler.batchData.Vecs[i], proc.Mp())
		}

		row2colChoose = false
		handler.maxFieldCnt = curBatchSize
		for i := 0; i < curBatchSize; i++ {
			if Oid[i] == types.T_char {
				continue
			}
			// XXX Vecs[0]?   What are we testing?
			handler.batchData.Vecs[i] = vector.PreAllocType(Oid[i].ToType(), curBatchSize, curBatchSize, proc.Mp())
			convey.So(rowToColumnAndSaveToStorage(handler, proc, force, row2colChoose), convey.ShouldNotBeNil)
			vector.Clean(handler.batchData.Vecs[i], proc.Mp())
		}
		a := proc.Mp().Stats().NumAlloc.Load()
		b := proc.Mp().Stats().NumFree.Load()
		convey.So(a, convey.ShouldEqual, b)
	})
}

func Test_PrintThreadInfo(t *testing.T) {
	convey.Convey("PrintThreadInfo succ", t, func() {
		handler := &ParseLineHandler{
			threadInfo: make(map[int]*ThreadInfo),
		}
		handler.threadInfo[1] = &ThreadInfo{}
		handler.threadInfo[1].SetTime(time.Now())
		handler.threadInfo[1].SetCnt(1)
		close := &CloseFlag{}
		var a time.Duration = 1
		go func() {
			time.Sleep(a * time.Second)
			close.Close()
		}()

		PrintThreadInfo(handler, close, a)

	})
}
