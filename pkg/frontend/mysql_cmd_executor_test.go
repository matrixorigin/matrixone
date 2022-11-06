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
	"fmt"
	"github.com/google/uuid"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
)

func init() {
	trace.Init(context.Background(), trace.EnableTracer(false))
}

func mockRecordStatement(ctx context.Context) (context.Context, *gostub.Stubs) {
	stm := &trace.StatementInfo{}
	ctx = trace.ContextWithStatement(ctx, stm)
	stubs := gostub.Stub(&RecordStatement, func(context.Context, *Session, *process.Process, ComputationWrapper, time.Time, string, bool) context.Context {
		return ctx
	})
	return ctx, stubs
}

func Test_mce(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("boot mce succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx, rsStubs := mockRecordStatement(ctx)
		defer rsStubs.Reset()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		eng.EXPECT().Database(ctx, gomock.Any(), txnOperator).Return(nil, nil).AnyTimes()

		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New().Return(txnOperator, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()

		use_t := mock_frontend.NewMockComputationWrapper(ctrl)
		use_t.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
		stmts, err := parsers.Parse(dialect.MYSQL, "use T")
		if err != nil {
			t.Error(err)
		}
		use_t.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		use_t.EXPECT().RecordExecPlan(ctx).Return(nil).AnyTimes()

		runner := mock_frontend.NewMockComputationRunner(ctrl)
		runner.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()

		create_1 := mock_frontend.NewMockComputationWrapper(ctrl)
		stmts, err = parsers.Parse(dialect.MYSQL, "create table A(a varchar(100),b int,c float)")
		if err != nil {
			t.Error(err)
		}
		create_1.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		create_1.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
		create_1.EXPECT().SetDatabaseName(gomock.Any()).Return(nil).AnyTimes()
		create_1.EXPECT().Compile(gomock.Any(), gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
		create_1.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()
		create_1.EXPECT().GetLoadTag().Return(false).AnyTimes()
		create_1.EXPECT().GetAffectedRows().Return(uint64(0)).AnyTimes()
		create_1.EXPECT().RecordExecPlan(ctx).Return(nil).AnyTimes()

		select_1 := mock_frontend.NewMockComputationWrapper(ctrl)
		stmts, err = parsers.Parse(dialect.MYSQL, "select a,b,c from A")
		if err != nil {
			t.Error(err)
		}
		select_1.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		select_1.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
		select_1.EXPECT().SetDatabaseName(gomock.Any()).Return(nil).AnyTimes()
		select_1.EXPECT().Compile(gomock.Any(), gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
		select_1.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()
		select_1.EXPECT().GetLoadTag().Return(false).AnyTimes()
		select_1.EXPECT().RecordExecPlan(ctx).Return(nil).AnyTimes()

		cola := &MysqlColumn{}
		cola.SetName("a")
		cola.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		colb := &MysqlColumn{}
		colb.SetName("b")
		colb.SetColumnType(defines.MYSQL_TYPE_LONG)
		colc := &MysqlColumn{}
		colc.SetName("c")
		colc.SetColumnType(defines.MYSQL_TYPE_FLOAT)
		cols := []interface{}{
			cola,
			colb,
			colc,
		}
		select_1.EXPECT().GetColumns().Return(cols, nil).AnyTimes()

		cws := []ComputationWrapper{
			use_t,
			create_1,
			select_1,
		}

		var self_handle_sql = []string{
			"SELECT DATABASE()",
			"SELECT @@max_allowed_packet",
			"SELECT @@version_comment",
			"SELECT @@tx_isolation",
			"set @@tx_isolation=`READ-COMMITTED`",
			//TODO:fix it after parser is ready
			//"set a = b",
			"drop database T",
		}

		sql1Col := &MysqlColumn{}
		sql1Col.SetName("DATABASE()")
		sql1Col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

		sql2Col := &MysqlColumn{}
		sql2Col.SetName("@@max_allowed_packet")
		sql2Col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

		sql3Col := &MysqlColumn{}
		sql3Col.SetName("@@version_comment")
		sql3Col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

		sql4Col := &MysqlColumn{}
		sql4Col.SetName("@@tx_isolation")
		sql4Col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

		var self_handle_sql_columns = [][]interface{}{
			{
				sql1Col,
			},
			{
				sql2Col,
			},
			{
				sql3Col,
			},
			{
				sql4Col,
			},
			{},
			{},
		}

		for i := 0; i < len(self_handle_sql); i++ {
			select_2 := mock_frontend.NewMockComputationWrapper(ctrl)
			stmts, err = parsers.Parse(dialect.MYSQL, self_handle_sql[i])
			if err != nil {
				t.Error(err)
			}
			select_2.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
			select_2.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
			select_2.EXPECT().SetDatabaseName(gomock.Any()).Return(nil).AnyTimes()
			select_2.EXPECT().Compile(gomock.Any(), gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
			select_2.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()
			select_2.EXPECT().GetLoadTag().Return(false).AnyTimes()
			select_2.EXPECT().GetAffectedRows().Return(uint64(0)).AnyTimes()
			select_2.EXPECT().GetColumns().Return(self_handle_sql_columns[i], nil).AnyTimes()
			select_2.EXPECT().RecordExecPlan(ctx).Return(nil).AnyTimes()
			cws = append(cws, select_2)
		}

		stubs := gostub.StubFunc(&GetComputationWrapper, cws, nil)
		defer stubs.Reset()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		ses := NewSession(proto, nil, pu, &gSys)
		ses.SetRequestContext(ctx)

		mce := NewMysqlCmdExecutor()

		mce.PrepareSessionBeforeExecRequest(ses)

		req := &Request{
			cmd:  COM_QUERY,
			data: []byte("test anywhere"),
		}

		resp, err := mce.ExecRequest(ctx, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldBeNil)

		req = &Request{
			cmd:  COM_QUERY,
			data: []byte("kill"),
		}
		resp, err = mce.ExecRequest(ctx, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldNotBeNil)

		req = &Request{
			cmd:  COM_QUERY,
			data: []byte("kill 10"),
		}
		mce.SetRoutineManager(&RoutineManager{})
		resp, err = mce.ExecRequest(ctx, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldNotBeNil)

		req = &Request{
			cmd:  COM_INIT_DB,
			data: []byte("test anywhere"),
		}

		_, err = mce.ExecRequest(ctx, req)
		convey.So(err, convey.ShouldBeNil)
		//COM_INIT_DB replaced by changeDB()
		//convey.So(resp.category, convey.ShouldEqual, OkResponse)

		req = &Request{
			cmd:  COM_PING,
			data: []byte("test anywhere"),
		}

		resp, err = mce.ExecRequest(ctx, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.category, convey.ShouldEqual, OkResponse)

		req = &Request{
			cmd:  COM_QUIT,
			data: []byte("test anywhere"),
		}

		resp, err = mce.ExecRequest(ctx, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldBeNil)

	})
}

func Test_mce_selfhandle(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("handleChangeDB", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()
		cnt := 0
		eng.EXPECT().Database(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx2 context.Context, db string, dump interface{}) (engine.Database, error) {
				cnt++
				if cnt == 1 {
					return nil, nil
				}
				return nil, moerr.NewInternalError("fake error")
			},
		).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(ctx).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New().Return(txnOperator, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(proto, nil, pu, &gSys)
		ses.SetRequestContext(ctx)

		mce := NewMysqlCmdExecutor()
		mce.PrepareSessionBeforeExecRequest(ses)
		err = mce.handleChangeDB(ctx, "T")
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetDatabaseName(), convey.ShouldEqual, "T")

		err = mce.handleChangeDB(ctx, "T")
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("handleSelectDatabase/handleMaxAllowedPacket/handleVersionComment/handleCmdFieldList/handleSetVar", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx, rsStubs := mockRecordStatement(ctx)
		defer rsStubs.Reset()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		ses := NewSession(proto, nil, pu, &gSys)
		ses.SetRequestContext(ctx)
		ses.Mrs = &MysqlResultSet{}

		mce := NewMysqlCmdExecutor()
		mce.PrepareSessionBeforeExecRequest(ses)

		ses.Mrs = &MysqlResultSet{}
		st1, err := parsers.ParseOne(dialect.MYSQL, "select @@max_allowed_packet")
		convey.So(err, convey.ShouldBeNil)
		sv1 := st1.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.VarExpr)
		err = mce.handleSelectVariables(sv1)
		convey.So(err, convey.ShouldBeNil)

		ses.Mrs = &MysqlResultSet{}
		st2, err := parsers.ParseOne(dialect.MYSQL, "select @@version_comment")
		convey.So(err, convey.ShouldBeNil)
		sv2 := st2.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.VarExpr)
		err = mce.handleSelectVariables(sv2)
		convey.So(err, convey.ShouldBeNil)

		ses.Mrs = &MysqlResultSet{}
		st3, err := parsers.ParseOne(dialect.MYSQL, "select @@global.version_comment")
		convey.So(err, convey.ShouldBeNil)
		sv3 := st3.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.VarExpr)
		err = mce.handleSelectVariables(sv3)
		convey.So(err, convey.ShouldBeNil)

		ses.Mrs = &MysqlResultSet{}
		st4, err := parsers.ParseOne(dialect.MYSQL, "select @version_comment")
		convey.So(err, convey.ShouldBeNil)
		sv4 := st4.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.VarExpr)
		err = mce.handleSelectVariables(sv4)
		convey.So(err, convey.ShouldBeNil)

		ses.Mrs = &MysqlResultSet{}
		queryData := []byte("A")
		queryData = append(queryData, 0)
		query := string(queryData)
		cflStmt, err := parseCmdFieldList(makeCmdFieldListSql(query))
		convey.So(err, convey.ShouldBeNil)
		err = mce.handleCmdFieldList(ctx, cflStmt)
		convey.So(err, convey.ShouldBeError)

		ses.SetMysqlResultSet(&MysqlResultSet{})
		ses.SetDatabaseName("T")
		mce.tableInfos = make(map[string][]ColumnInfo)
		mce.tableInfos["A"] = []ColumnInfo{&engineColumnInfo{
			name: "a",
			typ:  types.Type{Oid: types.T_varchar},
		}}

		err = mce.handleCmdFieldList(ctx, cflStmt)
		convey.So(err, convey.ShouldBeNil)

		mce.db = ses.GetDatabaseName()
		err = mce.handleCmdFieldList(ctx, cflStmt)
		convey.So(err, convey.ShouldBeNil)

		set := "set @@tx_isolation=`READ-COMMITTED`"
		setVar, err := parsers.ParseOne(dialect.MYSQL, set)
		convey.So(err, convey.ShouldBeNil)

		err = mce.handleSetVar(setVar.(*tree.SetVar))
		convey.So(err, convey.ShouldBeNil)

		req := &Request{
			cmd:  COM_FIELD_LIST,
			data: []byte{'A', 0},
		}

		resp, err := mce.ExecRequest(ctx, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldBeNil)
	})
}

func Test_getDataFromPipeline(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("getDataFromPipeline", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		ses := NewSession(proto, nil, pu, &gSys)
		ses.SetRequestContext(ctx)
		ses.Mrs = &MysqlResultSet{}
		proto.ses = ses

		// mce := NewMysqlCmdExecutor()
		// mce.PrepareSessionBeforeExecRequest(ses)

		genBatch := func() *batch.Batch {
			return allocTestBatch(
				[]string{
					"a", "b", "c", "d", "e", "f",
					"g", "h", "i", "j", "k", "l",
					"m", "n",
				},
				[]types.Type{
					{Oid: types.T_int8},
					{Oid: types.T_uint8},
					{Oid: types.T_int16},
					{Oid: types.T_uint16},
					{Oid: types.T_int32},
					{Oid: types.T_uint32},
					{Oid: types.T_int64},
					{Oid: types.T_uint64},
					{Oid: types.T_float32},
					{Oid: types.T_float64},
					{Oid: types.T_char},
					{Oid: types.T_varchar},
					{Oid: types.T_date},
					{Oid: types.T_time},
					{Oid: types.T_datetime},
					{Oid: types.T_json},
				},
				3)
		}

		batchCase1 := genBatch()

		err = getDataFromPipeline(ses, batchCase1)
		convey.So(err, convey.ShouldBeNil)

		batchCase2 := func() *batch.Batch {
			bat := genBatch()
			for i := 0; i < len(bat.Attrs); i++ {
				for j := 0; j < vector.Length(bat.Vecs[0]); j++ {
					nulls.Add(bat.Vecs[i].Nsp, uint64(j))
				}
			}
			return bat
		}()

		err = getDataFromPipeline(ses, batchCase2)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("getDataFromPipeline fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses := mock_frontend.NewMockIOSession(ctrl)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		ses := NewSession(proto, nil, pu, &gSys)
		ses.SetRequestContext(ctx)
		ses.Mrs = &MysqlResultSet{}
		proto.ses = ses

		convey.So(getDataFromPipeline(ses, nil), convey.ShouldBeNil)

		genBatch := func() *batch.Batch {
			return allocTestBatch(
				[]string{
					"a", "b", "c", "d", "e", "f",
					"g", "h", "i", "j", "k", "l",
					"m", "n",
				},
				[]types.Type{
					{Oid: types.T_int8},
					{Oid: types.T_uint8},
					{Oid: types.T_int16},
					{Oid: types.T_uint16},
					{Oid: types.T_int32},
					{Oid: types.T_uint32},
					{Oid: types.T_int64},
					{Oid: types.T_uint64},
					{Oid: types.T_float32},
					{Oid: types.T_float64},
					{Oid: types.T_char},
					{Oid: types.T_varchar},
					{Oid: types.T_date},
					{Oid: types.T_time},
					{Oid: types.T_datetime},
					{Oid: types.T_json},
				},
				3)
		}
		batchCase2 := func() *batch.Batch {
			bat := genBatch()

			for i := 0; i < len(bat.Attrs); i++ {
				for j := 0; j < 1; j++ {
					nulls.Add(bat.Vecs[i].Nsp, uint64(j))
				}
			}
			return bat
		}()

		err = getDataFromPipeline(ses, batchCase2)
		convey.So(err, convey.ShouldBeNil)

		batchCase2.Vecs = append(batchCase2.Vecs, &vector.Vector{Typ: types.Type{Oid: 88}})
		err = getDataFromPipeline(ses, batchCase2)
		convey.So(err, convey.ShouldNotBeNil)

	})
}

func Test_typeconvert(t *testing.T) {
	convey.Convey("convertEngineTypeToMysqlType", t, func() {
		input := []types.T{
			types.T_int8,
			types.T_uint8,
			types.T_int16,
			types.T_uint16,
			types.T_int32,
			types.T_uint32,
			types.T_int64,
			types.T_uint64,
			types.T_float32,
			types.T_float64,
			types.T_char,
			types.T_varchar,
			types.T_date,
			types.T_time,
			types.T_datetime,
			types.T_json,
		}

		type kase struct {
			tp     defines.MysqlType
			signed bool
		}
		output := []kase{
			{tp: defines.MYSQL_TYPE_TINY, signed: true},
			{tp: defines.MYSQL_TYPE_TINY},
			{tp: defines.MYSQL_TYPE_SHORT, signed: true},
			{tp: defines.MYSQL_TYPE_SHORT},
			{tp: defines.MYSQL_TYPE_LONG, signed: true},
			{tp: defines.MYSQL_TYPE_LONG},
			{tp: defines.MYSQL_TYPE_LONGLONG, signed: true},
			{tp: defines.MYSQL_TYPE_LONGLONG},
			{tp: defines.MYSQL_TYPE_FLOAT, signed: true},
			{tp: defines.MYSQL_TYPE_DOUBLE, signed: true},
			{tp: defines.MYSQL_TYPE_STRING, signed: true},
			{tp: defines.MYSQL_TYPE_VARCHAR, signed: true},
			{tp: defines.MYSQL_TYPE_DATE, signed: true},
			{tp: defines.MYSQL_TYPE_TIME, signed: true},
			{tp: defines.MYSQL_TYPE_DATETIME, signed: true},
			{tp: defines.MYSQL_TYPE_JSON, signed: true},
		}

		convey.So(len(input), convey.ShouldEqual, len(output))

		for i := 0; i < len(input); i++ {
			col := &MysqlColumn{}
			err := convertEngineTypeToMysqlType(input[i], col)
			convey.So(err, convey.ShouldBeNil)
			convey.So(col.columnType, convey.ShouldEqual, output[i].tp)
			convey.So(col.IsSigned() && output[i].signed ||
				!col.IsSigned() && !output[i].signed, convey.ShouldBeTrue)
		}
	})
}

func allocTestBatch(attrName []string, tt []types.Type, batchSize int) *batch.Batch {
	batchData := batch.New(true, attrName)

	//alloc space for vector
	for i := 0; i < len(attrName); i++ {
		vec := vector.PreAllocType(tt[i], batchSize, batchSize, testutil.TestUtilMp)
		batchData.Vecs[i] = vec
	}

	batchData.Zs = make([]int64, batchSize)
	for i := 0; i < batchSize; i++ {
		batchData.Zs[i] = 2
	}

	return batchData
}

func Test_mysqlerror(t *testing.T) {
	convey.Convey("mysql error", t, func() {
		err := moerr.NewBadDB("T")
		convey.So(err.MySQLCode(), convey.ShouldEqual, moerr.ER_BAD_DB_ERROR)
	})
}

func Test_handleSelectVariables(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("handleSelectVariables succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(proto, nil, pu, &gSys)
		ses.SetRequestContext(ctx)
		ses.Mrs = &MysqlResultSet{}
		mce := &MysqlCmdExecutor{}
		mce.PrepareSessionBeforeExecRequest(ses)
		st2, err := parsers.ParseOne(dialect.MYSQL, "select @@tx_isolation")
		convey.So(err, convey.ShouldBeNil)
		sv2 := st2.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.VarExpr)
		convey.So(mce.handleSelectVariables(sv2), convey.ShouldBeNil)

		st3, err := parsers.ParseOne(dialect.MYSQL, "select @@XXX")
		convey.So(err, convey.ShouldBeNil)
		sv3 := st3.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.VarExpr)
		convey.So(mce.handleSelectVariables(sv3), convey.ShouldNotBeNil)

	})
}

func Test_handleShowVariables(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("handleShowVariables succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(proto, nil, pu, &gSys)
		ses.SetRequestContext(ctx)
		ses.Mrs = &MysqlResultSet{}
		mce := &MysqlCmdExecutor{}
		mce.PrepareSessionBeforeExecRequest(ses)

		sv := &tree.ShowVariables{Global: true}
		convey.So(mce.handleShowVariables(sv, nil), convey.ShouldBeNil)
	})
}

func Test_GetColumns(t *testing.T) {
	convey.Convey("GetColumns succ", t, func() {
		//cw := &ComputationWrapperImpl{exec: &compile.Exec{}}
		//mysqlCols, err := cw.GetColumns()
		//convey.So(mysqlCols, convey.ShouldBeEmpty)
		//convey.So(err, convey.ShouldBeNil)
	})
}

func Test_GetComputationWrapper(t *testing.T) {
	convey.Convey("GetComputationWrapper succ", t, func() {
		db, sql, user := "T", "SHOW TABLES", "root"
		var eng engine.Engine
		proc := &process.Process{}
		ses := &Session{}
		cw, err := GetComputationWrapper(db, sql, user, eng, proc, ses)
		convey.So(cw, convey.ShouldNotBeEmpty)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_handleShowColumns(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("handleShowColumns succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(proto, nil, pu, &gSys)
		ses.SetRequestContext(ctx)
		data := make([][]interface{}, 1)
		data[0] = make([]interface{}, primaryKeyPos+1)
		data[0][0] = []byte("col1")
		typ, err := types.Encode(types.New(types.T_int8, 0, 0, 0))
		convey.So(err, convey.ShouldBeNil)
		data[0][1] = typ
		data[0][2] = []byte("NULL")
		data[0][3] = int8(2)
		defaultV, err := types.Encode(&plan2.Default{NullAbility: true, Expr: nil, OriginString: "", XXX_NoUnkeyedLiteral: struct{}{}, XXX_unrecognized: []byte{}, XXX_sizecache: 0})
		convey.So(err, convey.ShouldBeNil)
		data[0][5] = defaultV
		data[0][primaryKeyPos] = []byte("p")
		ses.SetData(data)
		proto.ses = ses

		ses.Mrs = &MysqlResultSet{}
		err = handleShowColumns(ses)
		convey.So(err, convey.ShouldBeNil)
	})
}

func runTestHandle(funName string, t *testing.T, handleFun func(*MysqlCmdExecutor) error) {
	ctx := context.TODO()
	convey.Convey(fmt.Sprintf("%s succ", funName), t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(proto, nil, pu, &gSys)
		ses.SetRequestContext(ctx)
		ses.Mrs = &MysqlResultSet{}
		mce := &MysqlCmdExecutor{}
		mce.PrepareSessionBeforeExecRequest(ses)

		convey.So(handleFun(mce), convey.ShouldBeNil)
	})
}

func Test_HandlePrepareStmt(t *testing.T) {
	stmt, err := parsers.ParseOne(dialect.MYSQL, "prepare stmt1 from select 1, 2")
	if err != nil {
		t.Errorf("parser sql error %v", err)
	}
	runTestHandle("handlePrepareStmt", t, func(mce *MysqlCmdExecutor) error {
		stmt := stmt.(*tree.PrepareStmt)
		_, err := mce.handlePrepareStmt(stmt)
		return err
	})
}

func Test_HandleDeallocate(t *testing.T) {
	stmt, err := parsers.ParseOne(dialect.MYSQL, "deallocate prepare stmt1")
	if err != nil {
		t.Errorf("parser sql error %v", err)
	}
	runTestHandle("handleDeallocate", t, func(mce *MysqlCmdExecutor) error {
		stmt := stmt.(*tree.Deallocate)
		return mce.handleDeallocate(stmt)
	})
}

func Test_CMD_FIELD_LIST(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("cmd field list", t, func() {
		queryData := []byte("A")
		queryData = append(queryData, 0)
		query := string(queryData)
		cmdFieldListQuery := makeCmdFieldListSql(query)
		convey.So(isCmdFieldListSql(cmdFieldListQuery), convey.ShouldBeTrue)
		stmt, err := parseCmdFieldList(cmdFieldListQuery)
		convey.So(err, convey.ShouldBeNil)
		convey.So(stmt, convey.ShouldNotBeNil)
		s := stmt.String()
		convey.So(isCmdFieldListSql(s), convey.ShouldBeTrue)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx, rsStubs := mockRecordStatement(ctx)
		defer rsStubs.Reset()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		db := mock_frontend.NewMockDatabase(ctrl)
		db.EXPECT().Relations(ctx).Return([]string{"t"}, nil).AnyTimes()

		table := mock_frontend.NewMockRelation(ctrl)
		db.EXPECT().Relation(ctx, "t").Return(table, nil).AnyTimes()
		defs := []engine.TableDef{
			&engine.AttributeDef{Attr: engine.Attribute{Name: "a", Type: types.T_char.ToType()}},
			&engine.AttributeDef{Attr: engine.Attribute{Name: "b", Type: types.T_int32.ToType()}},
		}

		table.EXPECT().TableDefs(ctx).Return(defs, nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(db, nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(ctx).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New().Return(txnOperator, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		pu.StorageEngine = eng
		pu.TxnClient = txnClient
		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(proto, nil, pu, &gSys)
		ses.SetRequestContext(ctx)
		ses.Mrs = &MysqlResultSet{}
		ses.SetDatabaseName("t")
		mce := &MysqlCmdExecutor{}
		mce.PrepareSessionBeforeExecRequest(ses)

		err = mce.doComQuery(ctx, cmdFieldListQuery)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_fakeoutput(t *testing.T) {
	convey.Convey("fake outout", t, func() {
		mrs := &MysqlResultSet{}
		fo := newFakeOutputQueue(mrs)
		_, _ = fo.getEmptyRow()
		_ = fo.flush()
	})
}

func Test_statement_type(t *testing.T) {
	convey.Convey("statement", t, func() {
		type kase struct {
			stmt tree.Statement
		}
		kases := []kase{
			{&tree.CreateTable{}},
			{&tree.Insert{}},
			{&tree.BeginTransaction{}},
			{&tree.ShowTables{}},
			{&tree.Use{}},
		}

		for _, k := range kases {
			ret, _ := StatementCanBeExecutedInUncommittedTransaction(nil, k.stmt)
			convey.So(ret, convey.ShouldBeTrue)
		}

		convey.So(IsDDL(&tree.CreateTable{}), convey.ShouldBeTrue)
		convey.So(IsDropStatement(&tree.DropTable{}), convey.ShouldBeTrue)
		convey.So(IsAdministrativeStatement(&tree.CreateAccount{}), convey.ShouldBeTrue)
		convey.So(IsParameterModificationStatement(&tree.SetVar{}), convey.ShouldBeTrue)
		convey.So(NeedToBeCommittedInActiveTransaction(&tree.SetVar{}), convey.ShouldBeTrue)
		convey.So(NeedToBeCommittedInActiveTransaction(&tree.DropTable{}), convey.ShouldBeTrue)
		convey.So(NeedToBeCommittedInActiveTransaction(&tree.CreateAccount{}), convey.ShouldBeTrue)
		convey.So(NeedToBeCommittedInActiveTransaction(nil), convey.ShouldBeFalse)
	})
}

func Test_convert_type(t *testing.T) {
	convey.Convey("type conversion", t, func() {
		convertEngineTypeToMysqlType(types.T_any, &MysqlColumn{})
		convertEngineTypeToMysqlType(types.T_bool, &MysqlColumn{})
		convertEngineTypeToMysqlType(types.T_timestamp, &MysqlColumn{})
		convertEngineTypeToMysqlType(types.T_decimal64, &MysqlColumn{})
		convertEngineTypeToMysqlType(types.T_decimal128, &MysqlColumn{})
		convertEngineTypeToMysqlType(types.T_blob, &MysqlColumn{})
		convertEngineTypeToMysqlType(types.T_text, &MysqlColumn{})
	})
}

func Test_handleLoadData(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("call handleLoadData func", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		proc := &process.Process{}

		mce := NewMysqlCmdExecutor()
		ses := &Session{
			protocol: proto,
		}
		mce.ses = ses
		load := &tree.Import{
			Local: true,
		}
		err = mce.handleLoadData(ctx, proc, load)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestHandleDump(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("call handleDump func", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		mce := NewMysqlCmdExecutor()
		ses := &Session{
			protocol: proto,
		}
		mce.ses = ses
		dump := &tree.MoDump{
			OutFile: "test",
		}
		err = mce.handleDump(ctx, dump)
		convey.So(err, convey.ShouldNotBeNil)
		dump.MaxFileSize = 1024
		err = mce.handleDump(ctx, dump)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestDump2File(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("call dumpData2File func", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		reader := mock_frontend.NewMockReader(ctrl)
		cnt := 0
		reader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(attrs []string, b, c interface{}) (*batch.Batch, error) {
			cnt += 1
			if cnt == 1 {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.New(types.T_int64.ToType())
				err := bat.Vecs[0].Append(int64(1), false, testutil.TestUtilMp)
				convey.So(err, convey.ShouldBeNil)
			}
			return nil, nil
		}).AnyTimes()
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().NewReader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]engine.Reader{reader}, nil).AnyTimes()
		db := mock_frontend.NewMockDatabase(ctrl)
		db.EXPECT().Relation(ctx, gomock.Any()).Return(rel, nil).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		mce := NewMysqlCmdExecutor()
		ses := &Session{
			protocol: proto,
		}
		mce.ses = ses
		dump := &tree.MoDump{
			OutFile: "test_dump_" + strconv.Itoa(int(time.Now().Unix())),
		}
		err = mce.dumpData2File(ctx, dump, "", []*dumpTable{{"a", "", rel, []string{"a"}, false}}, false)
		convey.So(err, convey.ShouldBeNil)
		os.RemoveAll(dump.OutFile)
	})
}

func TestSerializePlanToJson(t *testing.T) {
	sqls := []string{
		"SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC",                                                                                  //test alias
		"SELECT N_NAME, count(distinct N_REGIONKEY) FROM NATION group by N_NAME",                                                                                          //test distinct agg function
		"SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10",                                                                        //test agg
		"SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20", //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY >= N_NATIONKEY or (N_NAME like '%ddd' and N_REGIONKEY >0.5)",                                                    //test more expr
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY",
		"SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 0",
		"SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE NATION2.R_REGIONKEY > 0",
		"select n_name from nation intersect all select n_name from nation2",
		"select col1 from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a(col1, col2) where col2 > 0 order by col1",
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
	}

	for _, sql := range sqls {
		mock := plan.NewMockOptimizer()
		plan, err := buildSingleSql(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		json, rows, bytes := serializePlanToJson(plan, uuid.New())
		require.Equal(t, int64(0), rows)
		require.Equal(t, int64(0), bytes)
		t.Logf("SQL plan to json : %s\n", string(json))
	}
}

func buildSingleSql(opt plan.Optimizer, t *testing.T, sql string) (*plan.Plan, error) {
	stmts, err := mysql.Parse(sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	// this sql always return one stmt
	ctx := opt.CurrentContext()
	return plan.BuildPlan(ctx, stmts[0])
}
