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
	"fmt"
	"github.com/fagongzi/goetty/buf"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func Test_readTextFile(t *testing.T) {
	data, err := os.ReadFile("test/loadfile.csv")
	require.NoError(t, err)
	fmt.Printf("%v\n", data)
}

func Test_load(t *testing.T) {
	convey.Convey("load succ",t, func() {
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
					Name:"a"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_varchar},
					Name:"b"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint8},
					Name:"c"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int8},
					Name:"d"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint16},
					Name:"e"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int16},
					Name:"f"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint32},
					Name:"g"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int32},
					Name:"h"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint64},
					Name:"i"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int64},
					Name:"j"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_float32},
					Name:"k"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_float64},
					Name:"l"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_date},
					Name:"m"}},
			//&engine.AttributeDef{
			//	Attr: engine.Attribute{
			//		Type: types.Type{Oid: types.T_datetime},
			//		Name:"n"}},
		}
		rel.EXPECT().TableDefs().Return(tableDefs).AnyTimes()
		cnt := 0
		rel.EXPECT().Write(gomock.Any(),gomock.Any()).DoAndReturn(
			func(a,b interface{}) error{
				cnt++
				if cnt == 1 {
					return nil
				}else if cnt == 2{
					return fmt.Errorf("exec timeout")
				}

				return fmt.Errorf("fake error")
			},
		).AnyTimes()
		db.EXPECT().Relation(gomock.Any()).Return(
			rel,nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any()).Return(
			db,
			nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()

		cws := []ComputationWrapper{}

		var self_handle_sql = []string{
			"load data " +
				"infile 'test/loadfile5' " +
				"ignore " +
				"INTO TABLE T.A "+
				"FIELDS TERMINATED BY ',' ",
			"load data " +
				"infile 'test/loadfile5' " +
				"ignore " +
				"INTO TABLE T.A "+
				"FIELDS TERMINATED BY ',' " +
				"(c,d,e,f)",
		}

		for i := 0; i < len(self_handle_sql); i++ {
			select_2 := mock_frontend.NewMockComputationWrapper(ctrl)
			stmts, err := parsers.Parse(dialect.MYSQL, self_handle_sql[i])
			if err != nil {
				t.Error(err)
			}
			select_2.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
			select_2.EXPECT().SetDatabaseName(gomock.Any()).Return(nil).AnyTimes()
			select_2.EXPECT().Compile(gomock.Any(),gomock.Any()).Return(nil).AnyTimes()
			select_2.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()

			cws = append(cws,select_2)
		}

		stubs := gostub.StubFunc(&GetComputationWrapper, cws, nil)
		defer stubs.Reset()

		stubs2 := gostub.StubFunc(&PathExists,true,true,nil)
		defer stubs2.Reset()

		pu, err := getParameterUnit("test/system_vars_config.toml",eng)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		epochgc := getPCI()

		guestMmu := guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu)

		ses := NewSession(proto,epochgc,guestMmu,pu.Mempool,pu)

		mce := NewMysqlCmdExecutor()

		mce.PrepareSessionBeforeExecRequest(ses)

		req := &Request{
			cmd : int(COM_QUERY),
			data : []byte("test anywhere"),
		}

		resp, err := mce.ExecRequest(req)
		convey.So(err,convey.ShouldBeNil)
		convey.So(resp,convey.ShouldBeNil)
	})

	convey.Convey("load failed",t, func() {
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
					Name:"a"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_varchar},
					Name:"b"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint8},
					Name:"c"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int8},
					Name:"d"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint16},
					Name:"e"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int16},
					Name:"f"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint32},
					Name:"g"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int32},
					Name:"h"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_uint64},
					Name:"i"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_int64},
					Name:"j"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_float32},
					Name:"k"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_float64},
					Name:"l"}},
			&engine.AttributeDef{
				Attr: engine.Attribute{
					Type: types.Type{Oid: types.T_date},
					Name:"m"}},
			//&engine.AttributeDef{
			//	Attr: engine.Attribute{
			//		Type: types.Type{Oid: types.T_datetime},
			//		Name:"n"}},
		}
		rel.EXPECT().TableDefs().Return(tableDefs).AnyTimes()
		cnt := 0
		rel.EXPECT().Write(gomock.Any(),gomock.Any()).DoAndReturn(
			func(a,b interface{}) error{
				cnt++
				if cnt == 1 {
					return fmt.Errorf("fake error")
				}else if cnt == 2{
					return fmt.Errorf("exec timeout")
				}

				return nil
			},
		).AnyTimes()
		db.EXPECT().Relation(gomock.Any()).Return(
			rel,nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any()).Return(
			db,
			nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()

		cws := []*tree.Load{}

		type kase struct {
			sql string
			fail bool
		}

		kases := []kase{
			{
				sql:"load data " +
				"infile 'test/loadfile5' " +
				"INTO TABLE T.A "+
				"FIELDS TERMINATED BY ',' ",
				fail:false,
			},
			{
				sql:"load data " +
					"infile 'test/loadfile5' " +
					"INTO TABLE T.A "+
					"FIELDS TERMINATED BY ',' " +
					"(@a,b,d,e)",
				fail:true,
			},
			{
				sql:"load data " +
					"infile 'test/loadfile5' " +
					"ignore "+
					"INTO TABLE T.A "+
					"FIELDS TERMINATED BY ',' " +
					"(@a,b,d,e)",
				fail:false,
			},
			{
				sql:"load data " +
					"infile 'test/loadfile5' " +
					"INTO TABLE T.A "+
					"FIELDS TERMINATED BY ',' ",
				fail:false,
			},
		}

		for i := 0; i < len(kases); i++ {
			stmts, err := parsers.Parse(dialect.MYSQL, kases[i].sql)
			if err != nil {
				t.Error(err)
			}
			cws = append(cws,stmts[0].(*tree.Load))
		}

		stubs2 := gostub.StubFunc(&PathExists,true,true,nil)
		defer stubs2.Reset()

		pu, err := getParameterUnit("test/system_vars_config.toml",eng)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		epochgc := getPCI()

		guestMmu := guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu)

		ses := NewSession(proto,epochgc,guestMmu,pu.Mempool,pu)

		mce := NewMysqlCmdExecutor()

		mce.PrepareSessionBeforeExecRequest(ses)

		var row2col *gostub.Stubs = nil
		for i:=0 ; i< len(kases); i++ {
			if i == 3 {
				row2col = gostub.Stub(&row2colChoose,false)
			}
			_, err := mce.LoadLoop(cws[i],db,rel)
			if kases[i].fail {
				convey.So(err,convey.ShouldBeError)
			}else{
				convey.So(err,convey.ShouldBeNil)
			}

			if i == 3 {
				row2col.Reset()
			}
		}
	})
}