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
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/buf"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/simdcsv"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func Test_readTextFile(t *testing.T) {
	data, err := os.ReadFile("test/loadfile.csv")
	require.NoError(t, err)
	fmt.Printf("%v\n", data)
}

func Test_load(t *testing.T) {
	convey.Convey("load succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
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
		rel.EXPECT().TableDefs(nil).Return(tableDefs).AnyTimes()
		cnt := 0
		rel.EXPECT().Write(gomock.Any(), gomock.Any(), nil).DoAndReturn(
			func(a, b, c interface{}) error {
				cnt++
				if cnt == 1 {
					return nil
				} else if cnt == 2 {
					return fmt.Errorf("exec timeout")
				}

				return fmt.Errorf("fake error")
			},
		).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), nil).Return(rel, nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), nil).Return(db, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()

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

		epochgc := getPCI()

		guestMmu := guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu)

		ses := NewSession(proto, epochgc, guestMmu, pu.Mempool, pu, gSysVariables)

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

	convey.Convey("load failed", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
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
		rel.EXPECT().TableDefs(nil).Return(tableDefs).AnyTimes()
		cnt := 0
		rel.EXPECT().Write(gomock.Any(), gomock.Any(), nil).DoAndReturn(
			func(a, b, c interface{}) error {
				cnt++
				if cnt == 1 {
					return fmt.Errorf("fake error")
				} else if cnt == 2 {
					return fmt.Errorf("exec timeout")
				}

				return nil
			},
		).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), nil).Return(rel, nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), nil).Return(db, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()

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
				fail: false,
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

		epochgc := getPCI()

		guestMmu := guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu)

		ses := NewSession(proto, epochgc, guestMmu, pu.Mempool, pu, gSysVariables)

		mce := NewMysqlCmdExecutor()

		mce.PrepareSessionBeforeExecRequest(ses)

		var row2col *gostub.Stubs = nil
		for i := 0; i < len(kases); i++ {
			if i == 3 {
				row2col = gostub.Stub(&row2colChoose, false)
			}
			_, err = ses.txnHandler.StartByAutocommitIfNeeded()
			convey.So(err, convey.ShouldBeNil)

			_, err := mce.LoadLoop(cws[i], db, rel, "T")
			if kases[i].fail {
				convey.So(err, convey.ShouldBeError)
				//err = ses.txnHandler.RollbackAfterAutocommitOnly()
				//convey.So(err, convey.ShouldBeNil)
			} else {
				convey.So(err, convey.ShouldBeNil)
				//err = ses.txnHandler.CommitAfterAutocommitOnly()
				//convey.So(err, convey.ShouldBeNil)
			}

			if i == 3 {
				row2col.Reset()
			}
		}
	})
}

func getParsedLinesChan(simdCsvGetParsedLinesChan chan simdcsv.LineOut) {
	var str [][]string = [][]string{{"123"}, {"456"}, {"789"}, {"78910"}}
	for i := 0; i < len(str); i++ {
		simdCsvGetParsedLinesChan <- simdcsv.LineOut{Lines: nil, Line: str[i]}
	}
	simdCsvGetParsedLinesChan <- simdcsv.LineOut{Lines: nil, Line: nil}
}
func Test_getLineOutFromSimdCsvRoutine(t *testing.T) {
	convey.Convey("getLineOutFromSimdCsvRoutine succ", t, func() {
		handler := &ParseLineHandler{
			closeRef:                  &CloseLoadData{stopLoadData: make(chan interface{}, 1)},
			simdCsvGetParsedLinesChan: atomic.Value{},
			SharePart: SharePart{
				load:             &tree.Load{IgnoredLines: 1},
				simdCsvLineArray: make([][]string, 100)},
		}
		handler.simdCsvGetParsedLinesChan.Store(make(chan simdcsv.LineOut, 100))
		handler.closeRef.stopLoadData <- 1
		gostub.StubFunc(&saveLinesToStorage, nil)
		convey.So(handler.getLineOutFromSimdCsvRoutine(), convey.ShouldBeNil)

		handler.closeRef.stopLoadData <- 1
		gostub.StubFunc(&saveLinesToStorage, errors.New("1"))
		convey.So(handler.getLineOutFromSimdCsvRoutine(), convey.ShouldNotBeNil)

		getParsedLinesChan(getLineOutChan(handler.simdCsvGetParsedLinesChan))
		stubs := gostub.StubFunc(&saveLinesToStorage, nil)
		defer stubs.Reset()
		convey.So(handler.getLineOutFromSimdCsvRoutine(), convey.ShouldNotBeNil)

		handler.maxEntryBytesForCube = 5
		convey.So(handler.getLineOutFromSimdCsvRoutine(), convey.ShouldBeNil)
	})
}

func Test_rowToColumnAndSaveToStorage(t *testing.T) {
	convey.Convey("rowToColumnAndSaveToStorage succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().Write(gomock.Any(), gomock.Any(), nil).Return(nil).AnyTimes()

		var curBatchSize int = 13
		handler := &WriteBatchHandler{
			SharePart: SharePart{
				tableHandler:               rel,
				lineIdx:                    curBatchSize,
				maxFieldCnt:                curBatchSize,
				simdCsvLineArray:           make([][]string, curBatchSize),
				dataColumnId2TableColumnId: make([]int, curBatchSize),
				result:                     &LoadResult{},
				ignoreFieldError:           true},
			batchData: &batch.Batch{
				Vecs:  make([]*vector.Vector, curBatchSize),
				Attrs: make([]string, curBatchSize),
			},
			ThreadInfo: &ThreadInfo{},
		}
		field := [][]string{{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "8", "9"}}
		Oid := []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16,
			types.T_uint32, types.T_uint64, types.T_float32, types.T_float64, types.T_char, types.T_date, types.T_datetime}
		Col := []interface{}{make([]int8, curBatchSize), make([]int16, curBatchSize), make([]int32, curBatchSize), make([]int64, curBatchSize),
			make([]uint8, curBatchSize), make([]uint16, curBatchSize), make([]uint32, curBatchSize), make([]uint64, curBatchSize),
			make([]float32, curBatchSize), make([]float64, curBatchSize), &types.Bytes{
				Offsets: make([]uint32, curBatchSize), Lengths: make([]uint32, curBatchSize),
			}, make([]types.Date, curBatchSize), make([]types.Datetime, curBatchSize)}
		fmt.Println(Oid)
		handler.simdCsvLineArray[0] = field[0]
		for i := 0; i < curBatchSize; i++ {
			handler.dataColumnId2TableColumnId[i] = i
			handler.batchData.Vecs[i] = &vector.Vector{
				Nsp: &nulls.Nulls{},
				Typ: types.Type{Oid: Oid[i]},
				Col: Col[i],
			}
		}
		var force bool = false
		convey.So(rowToColumnAndSaveToStorage(handler, force, row2colChoose), convey.ShouldBeNil)

		row2colChoose = false
		handler.lineIdx = 1
		convey.So(rowToColumnAndSaveToStorage(handler, force, row2colChoose), convey.ShouldBeNil)

		handler.maxFieldCnt = 0
		convey.So(rowToColumnAndSaveToStorage(handler, force, row2colChoose), convey.ShouldBeNil)

		row2colChoose = true
		handler.ignoreFieldError = false
		for i := 0; i < curBatchSize; i++ {
			if Oid[i] == types.T_char {
				continue
			}
			handler.batchData.Vecs[0] = &vector.Vector{
				Nsp: &nulls.Nulls{},
				Typ: types.Type{Oid: Oid[i]},
				Col: Col[i],
			}
			convey.So(rowToColumnAndSaveToStorage(handler, force, row2colChoose), convey.ShouldNotBeNil)
		}

		row2colChoose = false
		handler.maxFieldCnt = curBatchSize
		for i := 0; i < curBatchSize; i++ {
			if Oid[i] == types.T_char {
				continue
			}
			handler.batchData.Vecs[0] = &vector.Vector{
				Nsp: &nulls.Nulls{},
				Typ: types.Type{Oid: Oid[i]},
				Col: Col[i],
			}
			convey.So(rowToColumnAndSaveToStorage(handler, force, row2colChoose), convey.ShouldNotBeNil)
		}
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
