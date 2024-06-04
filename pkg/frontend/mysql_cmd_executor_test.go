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
	"io"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	motrace.Init(context.Background(), motrace.EnableTracer(false))
	motrace.DisableLogErrorReport(true)
}

func mockRecordStatement(ctx context.Context) (context.Context, *gostub.Stubs) {
	stubs := gostub.Stub(&RecordStatement, func(context.Context, *Session, *process.Process, ComputationWrapper, time.Time, string, string, bool) (context.Context, error) {
		return ctx, nil
	})
	return ctx, stubs
}

func Test_mce(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
	convey.Convey("boot mce succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx, rsStubs := mockRecordStatement(ctx)
		defer rsStubs.Reset()

		srStub := gostub.Stub(&parsers.HandleSqlForRecord, func(sql string) []string {
			return make([]string, 7)
		})
		defer srStub.Reset()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), txnOperator).Return(nil, nil).AnyTimes()

		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		use_t := mock_frontend.NewMockComputationWrapper(ctrl)
		use_t.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
		use_t.EXPECT().Clear().AnyTimes()
		stmts, err := parsers.Parse(ctx, dialect.MYSQL, "use T", 1, 0)
		if err != nil {
			t.Error(err)
		}
		use_t.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		use_t.EXPECT().RecordExecPlan(ctx).Return(nil).AnyTimes()

		runner := mock_frontend.NewMockComputationRunner(ctrl)
		runner.EXPECT().Run(gomock.Any()).Return(nil, nil).AnyTimes()

		create_1 := mock_frontend.NewMockComputationWrapper(ctrl)
		stmts, err = parsers.Parse(ctx, dialect.MYSQL, "create table A(a varchar(100),b int,c float)", 1, 0)
		if err != nil {
			t.Error(err)
		}
		create_1.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		create_1.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
		create_1.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
		create_1.EXPECT().Run(gomock.Any()).Return(nil, nil).AnyTimes()
		create_1.EXPECT().GetLoadTag().Return(false).AnyTimes()
		create_1.EXPECT().RecordExecPlan(ctx).Return(nil).AnyTimes()
		create_1.EXPECT().Clear().AnyTimes()
		create_1.EXPECT().Free().AnyTimes()
		create_1.EXPECT().Plan().Return(&plan.Plan{}).AnyTimes()

		select_1 := mock_frontend.NewMockComputationWrapper(ctrl)
		stmts, err = parsers.Parse(ctx, dialect.MYSQL, "select a,b,c from A", 1, 0)
		if err != nil {
			t.Error(err)
		}
		select_1.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		select_1.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
		select_1.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
		select_1.EXPECT().Run(gomock.Any()).Return(nil, nil).AnyTimes()
		select_1.EXPECT().GetLoadTag().Return(false).AnyTimes()
		select_1.EXPECT().RecordExecPlan(ctx).Return(nil).AnyTimes()
		select_1.EXPECT().Clear().AnyTimes()
		select_1.EXPECT().Free().AnyTimes()
		select_1.EXPECT().Plan().Return(&plan.Plan{}).AnyTimes()

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
		select_1.EXPECT().GetColumns(gomock.Any()).Return(cols, nil).AnyTimes()

		cws := []ComputationWrapper{
			//use_t,
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
			//"drop database T",
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
			stmts, err = parsers.Parse(ctx, dialect.MYSQL, self_handle_sql[i], 1, 0)
			convey.So(err, convey.ShouldBeNil)
			select_2.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
			select_2.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
			select_2.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
			select_2.EXPECT().Run(gomock.Any()).Return(nil, nil).AnyTimes()
			select_2.EXPECT().GetLoadTag().Return(false).AnyTimes()
			select_2.EXPECT().GetColumns(gomock.Any()).Return(self_handle_sql_columns[i], nil).AnyTimes()
			select_2.EXPECT().RecordExecPlan(ctx).Return(nil).AnyTimes()
			select_2.EXPECT().Clear().AnyTimes()
			select_2.EXPECT().Free().AnyTimes()
			select_2.EXPECT().Plan().Return(&plan.Plan{}).AnyTimes()
			cws = append(cws, select_2)
		}

		stubs := gostub.StubFunc(&GetComputationWrapper, cws, nil)
		defer stubs.Reset()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		convey.So(err, convey.ShouldBeNil)
		setGlobalPu(pu)
		pu.SV.SkipCheckPrivilege = true

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		ses := NewSession(ctx, proto, nil)
		ses.SetTenantInfo(&TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		})
		proto.SetSession(ses)

		sysVarStubs := gostub.StubFunc(&ExeSqlInBgSes, nil, nil)
		defer sysVarStubs.Reset()
		_ = ses.InitSystemVariables(ctx)

		ctx = context.WithValue(ctx, config.ParameterUnitKey, pu)

		// A mock autoincrcache manager.
		req := &Request{
			cmd:  COM_QUERY,
			data: []byte("test anywhere"),
		}

		ec := newTestExecCtx(ctx, ctrl)

		resp, err := ExecRequest(ses, ec, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldBeNil)

		req = &Request{
			cmd:  COM_INIT_DB,
			data: []byte("test anywhere"),
		}

		_, err = ExecRequest(ses, ec, req)
		convey.So(err, convey.ShouldBeNil)

		req = &Request{
			cmd:  COM_PING,
			data: []byte("test anywhere"),
		}

		resp, err = ExecRequest(ses, ec, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.category, convey.ShouldEqual, OkResponse)

		req = &Request{
			cmd:  COM_QUIT,
			data: []byte("test anywhere"),
		}

		resp, err = ExecRequest(ses, ec, req)
		convey.So(err, convey.ShouldBeError)
		convey.So(resp, convey.ShouldBeNil)

	})
}

func Test_mce_selfhandle(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("handleChangeDB", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()
		cnt := 0
		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx2 context.Context, db string, dump interface{}) (engine.Database, error) {
				cnt++
				if cnt == 1 {
					return mockDbMeta, nil
				}
				return nil, moerr.NewInternalError(ctx2, "fake error")
			},
		).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Commit(ctx).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, proto, nil)
		ec := newTestExecCtx(ctx, ctrl)

		err = handleChangeDB(ses, ec, "T")
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetDatabaseName(), convey.ShouldEqual, "T")

		err = handleChangeDB(ses, ec, "T")
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("handleSelectDatabase/handleMaxAllowedPacket/handleVersionComment/handleCmdFieldList/handleSetVar", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx, rsStubs := mockRecordStatement(ctx)
		defer rsStubs.Reset()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second * 10}).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, proto, nil)
		ses.mrs = &MysqlResultSet{}
		proto.SetSession(ses)

		ses.mrs = &MysqlResultSet{}
		queryData := []byte("A")
		queryData = append(queryData, 0)
		query := string(queryData)
		cflStmt, err := parseCmdFieldList(ctx, makeCmdFieldListSql(query))
		convey.So(err, convey.ShouldBeNil)
		ec := newTestExecCtx(ctx, ctrl)

		err = handleCmdFieldList(ses, ec, cflStmt)
		convey.So(err, convey.ShouldBeError)

		ses.SetMysqlResultSet(&MysqlResultSet{})
		ses.SetDatabaseName("T")

		ses.SetTenantInfo(&TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		})
		sysVarStubs := gostub.StubFunc(&ExeSqlInBgSes, nil, nil)
		defer sysVarStubs.Reset()
		_ = ses.InitSystemVariables(ctx)

		err = handleCmdFieldList(ses, ec, cflStmt)
		convey.So(err, convey.ShouldBeNil)

		err = handleCmdFieldList(ses, ec, cflStmt)
		convey.So(err, convey.ShouldBeNil)

		set := "set @@tx_isolation=`READ-COMMITTED`"
		setVar, err := parsers.ParseOne(ctx, dialect.MYSQL, set, 1, 0)
		convey.So(err, convey.ShouldBeNil)

		err = handleSetVar(ses, ec, setVar.(*tree.SetVar), "")
		convey.So(err, convey.ShouldBeNil)

		req := &Request{
			cmd:  COM_FIELD_LIST,
			data: []byte{'A', 0},
		}

		resp, err := ExecRequest(ses, ec, req)
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
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, proto, nil)
		ses.mrs = &MysqlResultSet{}
		proto.ses = ses

		genBatch := func() *batch.Batch {
			return allocTestBatch(
				[]string{
					"a", "b", "c", "d", "e", "f",
					"g", "h", "i", "j", "k", "l",
					"m", "n",
				},
				[]types.Type{
					types.T_int8.ToType(),
					types.T_uint8.ToType(),
					types.T_int16.ToType(),
					types.T_uint16.ToType(),
					types.T_int32.ToType(),
					types.T_uint32.ToType(),
					types.T_int64.ToType(),
					types.T_uint64.ToType(),
					types.T_float32.ToType(),
					types.T_float64.ToType(),
					types.T_char.ToType(),
					types.T_varchar.ToType(),
					types.T_date.ToType(),
					types.T_time.ToType(),
					types.T_datetime.ToType(),
					types.T_json.ToType(),
				},
				3)
		}

		batchCase1 := genBatch()
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		err = getDataFromPipeline(ses, ec, batchCase1)
		convey.So(err, convey.ShouldBeNil)

		batchCase2 := func() *batch.Batch {
			bat := genBatch()
			for i := 0; i < len(bat.Attrs); i++ {
				for j := 0; j < bat.Vecs[0].Length(); j++ {
					nulls.Add(bat.Vecs[i].GetNulls(), uint64(j))
				}
			}
			return bat
		}()

		err = getDataFromPipeline(ses, ec, batchCase2)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("getDataFromPipeline fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses := mock_frontend.NewMockIOSession(ctrl)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)
		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, proto, nil)
		ses.mrs = &MysqlResultSet{}
		proto.ses = ses
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		convey.So(getDataFromPipeline(ses, ec, nil), convey.ShouldBeNil)

		genBatch := func() *batch.Batch {
			return allocTestBatch(
				[]string{
					"a", "b", "c", "d", "e", "f",
					"g", "h", "i", "j", "k", "l",
					"m", "n",
				},
				[]types.Type{
					types.T_int8.ToType(),
					types.T_uint8.ToType(),
					types.T_int16.ToType(),
					types.T_uint16.ToType(),
					types.T_int32.ToType(),
					types.T_uint32.ToType(),
					types.T_int64.ToType(),
					types.T_uint64.ToType(),
					types.T_float32.ToType(),
					types.T_float64.ToType(),
					types.T_char.ToType(),
					types.T_varchar.ToType(),
					types.T_date.ToType(),
					types.T_time.ToType(),
					types.T_datetime.ToType(),
					types.T_json.ToType(),
				},
				3)
		}
		batchCase2 := func() *batch.Batch {
			bat := genBatch()

			for i := 0; i < len(bat.Attrs); i++ {
				for j := 0; j < 1; j++ {
					nulls.Add(bat.Vecs[i].GetNulls(), uint64(j))
				}
			}
			return bat
		}()

		err = getDataFromPipeline(ses, ec, batchCase2)
		convey.So(err, convey.ShouldBeNil)

		batchCase2.Vecs = append(batchCase2.Vecs, vector.NewVec(types.T_any.ToType()))
		err = getDataFromPipeline(ses, ec, batchCase2)
		convey.So(err, convey.ShouldNotBeNil)

	})
}

func Test_typeconvert(t *testing.T) {
	ctx := context.TODO()
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
			types.T_array_float32,
			types.T_array_float64,
			types.T_bit,
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
			{tp: defines.MYSQL_TYPE_VAR_STRING, signed: true},
			{tp: defines.MYSQL_TYPE_DATE, signed: true},
			{tp: defines.MYSQL_TYPE_TIME, signed: true},
			{tp: defines.MYSQL_TYPE_DATETIME, signed: true},
			{tp: defines.MYSQL_TYPE_JSON, signed: true},
			{tp: defines.MYSQL_TYPE_VARCHAR, signed: true},
			{tp: defines.MYSQL_TYPE_VARCHAR, signed: true},
			{tp: defines.MYSQL_TYPE_BIT},
		}

		convey.So(len(input), convey.ShouldEqual, len(output))

		for i := 0; i < len(input); i++ {
			col := &MysqlColumn{}
			err := convertEngineTypeToMysqlType(ctx, input[i], col)
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
		vec := vector.NewVec(tt[i])
		if err := vec.PreExtend(batchSize, testutil.TestUtilMp); err != nil {
			panic(err)
		}
		vec.SetLength(batchSize)
		batchData.Vecs[i] = vec
	}

	batchData.SetRowCount(batchSize)
	return batchData
}

func Test_mysqlerror(t *testing.T) {
	convey.Convey("mysql error", t, func() {
		err := moerr.NewBadDB(context.TODO(), "T")
		convey.So(err.MySQLCode(), convey.ShouldEqual, moerr.ER_BAD_DB_ERROR)
	})
}

func Test_handleShowVariables(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), 0)
	convey.Convey("handleShowVariables succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), nil).Return(nil, nil).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)

		pu.StorageEngine = eng
		pu.TxnClient = txnClient
		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, proto, nil)
		tenant := &TenantInfo{
			Tenant:   "sys",
			TenantID: 0,
			User:     DefaultTenantMoAdmin,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}
		ses.SetDatabaseName("t")

		sysVarStubs := gostub.StubFunc(&ExeSqlInBgSes, nil, nil)
		defer sysVarStubs.Reset()
		_ = ses.InitSystemVariables(ctx)

		proto.SetSession(ses)
		ec := newTestExecCtx(ctx, ctrl)

		sv := &tree.ShowVariables{Global: false}
		convey.So(handleShowVariables(ses, ec, sv), convey.ShouldBeNil)
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

		sysVars := make(map[string]interface{})
		for name, sysVar := range gSysVarsDefs {
			sysVars[name] = sysVar.Default
		}
		ses := &Session{planCache: newPlanCache(1),
			feSessionImpl: feSessionImpl{
				gSysVars: &SystemVariables{sysVars: sysVars},
			},
		}
		ctrl := gomock.NewController(t)
		ec := newTestExecCtx(context.Background(), ctrl)
		ec.ses = ses
		ec.input = &UserInput{sql: sql}

		cw, err := GetComputationWrapper(ec, db, user, eng, proc, ses)
		convey.So(cw, convey.ShouldNotBeEmpty)
		convey.So(err, convey.ShouldBeNil)
	})
}

func runTestHandle(funName string, t *testing.T, handleFun func(ses *Session) error) {
	ctx := context.TODO()
	convey.Convey(fmt.Sprintf("%s succ", funName), t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, proto, nil)
		ses.mrs = &MysqlResultSet{}
		ses.txnCompileCtx.execCtx = &ExecCtx{reqCtx: ctx, proc: testutil.NewProc(), ses: ses}

		convey.So(handleFun(ses), convey.ShouldBeNil)
	})
}

func Test_HandlePrepareStmt(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "Prepare stmt1 from select 1, 2", 1, 0)
	if err != nil {
		t.Errorf("parser sql error %v", err)
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ec := newTestExecCtx(ctx, ctrl)

	runTestHandle("handlePrepareStmt", t, func(ses *Session) error {
		stmt := stmt.(*tree.PrepareStmt)
		_, err := handlePrepareStmt(ses, ec, stmt)
		return err
	})
}

func Test_HandleDeallocate(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "deallocate Prepare stmt1", 1, 0)
	if err != nil {
		t.Errorf("parser sql error %v", err)
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ec := newTestExecCtx(ctx, ctrl)

	runTestHandle("handleDeallocate", t, func(ses *Session) error {
		stmt := stmt.(*tree.Deallocate)
		return handleDeallocate(ses, ec, stmt)
	})
}

func Test_CMD_FIELD_LIST(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("cmd field list", t, func() {
		runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
		queryData := []byte("A")
		queryData = append(queryData, 0)
		query := string(queryData)
		cmdFieldListQuery := makeCmdFieldListSql(query)
		convey.So(isCmdFieldListSql(cmdFieldListQuery), convey.ShouldBeTrue)
		stmt, err := parseCmdFieldList(ctx, cmdFieldListQuery)
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
		db := mock_frontend.NewMockDatabase(ctrl)
		db.EXPECT().Relations(ctx).Return([]string{"t"}, nil).AnyTimes()

		table := mock_frontend.NewMockRelation(ctrl)
		db.EXPECT().Relation(ctx, "t", nil).Return(table, nil).AnyTimes()
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
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)

		pu.StorageEngine = eng
		pu.TxnClient = txnClient
		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, proto, nil)
		proto.SetSession(ses)

		ses.mrs = &MysqlResultSet{}
		ses.SetDatabaseName("t")
		ses.seqLastValue = new(string)

		ec := newTestExecCtx(ctx, ctrl)

		err = doComQuery(ses, ec, &UserInput{sql: cmdFieldListQuery})
		convey.So(err, convey.ShouldBeNil)
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
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ses := newTestSession(t, ctrl)
		for _, k := range kases {
			ret, _ := statementCanBeExecutedInUncommittedTransaction(context.TODO(), ses, k.stmt)
			convey.So(ret, convey.ShouldBeTrue)
		}

		convey.So(IsDDL(&tree.CreateTable{}), convey.ShouldBeTrue)
		convey.So(IsDropStatement(&tree.DropTable{}), convey.ShouldBeTrue)
		convey.So(IsAdministrativeStatement(&tree.CreateAccount{}), convey.ShouldBeTrue)
		convey.So(IsParameterModificationStatement(&tree.SetVar{}), convey.ShouldBeTrue)
		convey.So(NeedToBeCommittedInActiveTransaction(&tree.SetVar{}), convey.ShouldBeTrue)
		convey.So(NeedToBeCommittedInActiveTransaction(&tree.DropTable{}), convey.ShouldBeFalse)
		convey.So(NeedToBeCommittedInActiveTransaction(&tree.CreateAccount{}), convey.ShouldBeTrue)
		convey.So(NeedToBeCommittedInActiveTransaction(nil), convey.ShouldBeFalse)
	})
}

func Test_convert_type(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("type conversion", t, func() {
		convertEngineTypeToMysqlType(ctx, types.T_any, &MysqlColumn{})
		convertEngineTypeToMysqlType(ctx, types.T_bool, &MysqlColumn{})
		convertEngineTypeToMysqlType(ctx, types.T_timestamp, &MysqlColumn{})
		convertEngineTypeToMysqlType(ctx, types.T_decimal64, &MysqlColumn{})
		convertEngineTypeToMysqlType(ctx, types.T_decimal128, &MysqlColumn{})
		convertEngineTypeToMysqlType(ctx, types.T_blob, &MysqlColumn{})
		convertEngineTypeToMysqlType(ctx, types.T_text, &MysqlColumn{})
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
		mock := plan.NewMockOptimizer(false)
		plan, err := buildSingleSql(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		uid, _ := uuid.NewV7()
		stm := &motrace.StatementInfo{StatementID: uid, Statement: sql, RequestAt: time.Now()}
		h := NewMarshalPlanHandler(mock.CurrentContext().GetContext(), stm, plan)
		json := h.Marshal(mock.CurrentContext().GetContext())
		_, stats := h.Stats(mock.CurrentContext().GetContext(), nil)
		require.Equal(t, int64(0), stats.RowsRead)
		require.Equal(t, int64(0), stats.BytesScan)
		t.Logf("SQL plan to json : %s\n", string(json))
	}
}

func buildSingleSql(opt plan.Optimizer, t *testing.T, sql string) (*plan.Plan, error) {
	stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1, 0)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	// this sql always return one stmt
	ctx := opt.CurrentContext()
	return plan.BuildPlan(ctx, stmts[0], false)
}

func Test_getSqlType(t *testing.T) {
	convey.Convey("call genSqlSourceType func", t, func() {
		sql := "use db"
		ses := &Session{}
		ui := &UserInput{sql: sql}
		ui.genSqlSourceType(ses)
		convey.So(ui.getSqlSourceTypes()[0], convey.ShouldEqual, constant.InternalSql)

		user := "special_user"
		tenant := &TenantInfo{
			User: user,
		}
		ses.SetTenantInfo(tenant)
		SetSpecialUser(user, nil)
		ui = &UserInput{sql: sql}
		ui.genSqlSourceType(ses)
		convey.So(ui.getSqlSourceTypes()[0], convey.ShouldEqual, constant.InternalSql)

		tenant.User = "dump"
		ui = &UserInput{sql: sql}
		ui.genSqlSourceType(ses)
		convey.So(ui.getSqlSourceTypes()[0], convey.ShouldEqual, constant.ExternSql)

		sql = "/* cloud_user */ use db"
		ui = &UserInput{sql: sql}
		ui.genSqlSourceType(ses)
		convey.So(ui.getSqlSourceTypes()[0], convey.ShouldEqual, constant.CloudUserSql)

		sql = "/* cloud_nonuser */ use db"
		ui = &UserInput{sql: sql}
		ui.genSqlSourceType(ses)
		convey.So(ui.getSqlSourceTypes()[0], convey.ShouldEqual, constant.CloudNoUserSql)

		sql = "/* json */ use db"
		ui = &UserInput{sql: sql}
		ui.genSqlSourceType(ses)
		convey.So(ui.getSqlSourceTypes()[0], convey.ShouldEqual, constant.ExternSql)
	})
}

func TestProcessLoadLocal(t *testing.T) {
	convey.Convey("call processLoadLocal func", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				Filepath: "test.csv",
			},
		}
		proc := testutil.NewProc()
		var writer *io.PipeWriter
		proc.LoadLocalReader, writer = io.Pipe()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)
		cnt := 0
		ioses.EXPECT().Read(gomock.Any()).DoAndReturn(func(options goetty.ReadOptions) (pkt any, err error) {
			if cnt == 0 {
				pkt = &Packet{Length: 5, Payload: []byte("hello"), SequenceID: 1}
			} else if cnt == 1 {
				pkt = &Packet{Length: 5, Payload: []byte("world"), SequenceID: 2}
			} else {
				err = moerr.NewInvalidInput(context.TODO(), "length 0")
			}
			cnt++
			return
		}).AnyTimes()
		ioses.EXPECT().Close().AnyTimes()
		proto := &testMysqlWriter{
			ioses: ioses,
		}

		ses := &Session{
			feSessionImpl: feSessionImpl{
				respr: NewMysqlResp(proto),
			},
		}
		buffer := make([]byte, 4096)
		go func(buf []byte) {
			tmp := buf
			for {
				n, err := proc.LoadLocalReader.Read(tmp)
				if err != nil {
					break
				}
				tmp = tmp[n:]
			}
		}(buffer)
		ec := newTestExecCtx(context.Background(), ctrl)
		err := processLoadLocal(ses, ec, param, writer)
		convey.So(err, convey.ShouldBeNil)
		convey.So(buffer[:10], convey.ShouldResemble, []byte("helloworld"))
		convey.So(buffer[10:], convey.ShouldResemble, make([]byte, 4096-10))
	})
}

func Test_StatementClassify(t *testing.T) {
	type arg struct {
		stmt tree.Statement
		want bool
	}

	args := []arg{
		{&tree.ShowCreateTable{}, true},
		{&tree.ShowCreateView{}, true},
		{&tree.ShowCreateDatabase{}, true},
		{&tree.ShowColumns{}, true},
		{&tree.ShowDatabases{}, true},
		{&tree.ShowTarget{}, true},
		{&tree.ShowTableStatus{}, true},
		{&tree.ShowGrants{}, true},
		{&tree.ShowTables{}, true},
		{&tree.ShowProcessList{}, true},
		{&tree.ShowErrors{}, true},
		{&tree.ShowWarnings{}, true},
		{&tree.ShowCollation{}, true},
		{&tree.ShowVariables{}, true},
		{&tree.ShowStatus{}, true},
		{&tree.ShowIndex{}, true},
		{&tree.ShowFunctionOrProcedureStatus{}, true},
		{&tree.ShowNodeList{}, true},
		{&tree.ShowLocks{}, true},
		{&tree.ShowTableNumber{}, true},
		{&tree.ShowColumnNumber{}, true},
		{&tree.ShowTableValues{}, true},
		{&tree.ShowAccounts{}, true},
		{&tree.ShowAccountUpgrade{}, true},
		{&tree.ShowPublications{}, true},
		{&tree.ShowCreatePublications{}, true},
		{&tree.ShowBackendServers{}, true},
	}
	ses := &Session{
		feSessionImpl: feSessionImpl{},
	}
	for _, a := range args {
		ret, err := statementCanBeExecutedInUncommittedTransaction(context.TODO(), ses, a.stmt)
		assert.Nil(t, err)
		assert.Equal(t, ret, a.want)
	}
}

func TestMysqlCmdExecutor_HandleShowBackendServers(t *testing.T) {
	ctx := context.TODO()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)

	ioses := mock_frontend.NewMockIOSession(ctrl)
	ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
	ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
	ioses.EXPECT().Ref().AnyTimes()
	ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
	pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
	if err != nil {
		t.Error(err)
	}
	setGlobalPu(pu)

	proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

	ses := NewSession(ctx, proto, nil)
	proto.SetSession(ses)
	//ses.proto = proto

	convey.Convey("no labels", t, func() {
		ses.mrs = &MysqlResultSet{}
		cluster := clusterservice.NewMOCluster(
			nil,
			0,
			clusterservice.WithDisableRefresh(),
			clusterservice.WithServices(
				[]metadata.CNService{
					{
						ServiceID:  "s1",
						SQLAddress: "addr1",
						WorkState:  metadata.WorkState_Working,
					},
					{
						ServiceID:  "s2",
						SQLAddress: "addr2",
						WorkState:  metadata.WorkState_Working,
					},
				},
				nil,
			),
		)
		runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)
		ses.SetTenantInfo(&TenantInfo{Tenant: "t1"})
		proto.connectAttrs = map[string]string{}
		ec := newTestExecCtx(ctx, ctrl)

		err = handleShowBackendServers(ses, ec)
		require.NoError(t, err)
		rs := ses.GetMysqlResultSet()
		require.Equal(t, uint64(4), rs.GetColumnCount())
		require.Equal(t, uint64(2), rs.GetRowCount())
	})

	convey.Convey("filter label", t, func() {
		ses.mrs = &MysqlResultSet{}
		cluster := clusterservice.NewMOCluster(
			nil,
			0,
			clusterservice.WithDisableRefresh(),
			clusterservice.WithServices(
				[]metadata.CNService{
					{
						ServiceID:  "s1",
						SQLAddress: "addr1",
						Labels: map[string]metadata.LabelList{
							"account": {Labels: []string{"t1"}},
						},
						WorkState: metadata.WorkState_Working,
					},
					{
						ServiceID:  "s2",
						SQLAddress: "addr2",
						Labels: map[string]metadata.LabelList{
							"account": {Labels: []string{"t2"}},
						},
						WorkState: metadata.WorkState_Working,
					},
					{
						ServiceID:  "s3",
						SQLAddress: "addr3",
						WorkState:  metadata.WorkState_Working,
					},
				},
				nil,
			),
		)
		runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)
		ses.SetTenantInfo(&TenantInfo{Tenant: "t1"})
		proto.connectAttrs = map[string]string{}
		ec := newTestExecCtx(ctx, ctrl)

		err = handleShowBackendServers(ses, ec)
		require.NoError(t, err)
		rs := ses.GetMysqlResultSet()
		require.Equal(t, uint64(4), rs.GetColumnCount())
		require.Equal(t, uint64(1), rs.GetRowCount())

		row, err := rs.GetRow(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, "s1", row[0])
		require.Equal(t, "addr1", row[1])
	})
}

func Test_RecordParseErrorStatement(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)

	proc := &process.Process{
		Ctx: context.TODO(),
	}

	motrace.GetTracerProvider().SetEnable(true)
	_, err := RecordParseErrorStatement(context.TODO(), ses, proc, time.Now(), nil, nil, moerr.NewInternalErrorNoCtx("test"))
	assert.Nil(t, err)

	_, err = RecordParseErrorStatement(context.TODO(), ses, proc, time.Now(), []string{"abc", "def"}, []string{constant.ExternSql, constant.ExternSql}, moerr.NewInternalErrorNoCtx("test"))
	assert.Nil(t, err)

}

func Test_getExplainOption(t *testing.T) {
	ctx := context.TODO()
	var option *explain.ExplainOptions
	var err error

	// verbose
	option, err = getExplainOption(ctx, []tree.OptionElem{{Name: "verbose", Value: "true"}})
	require.Nil(t, err)
	require.Equal(t, option.Verbose, true)

	option, err = getExplainOption(ctx, []tree.OptionElem{{Name: "verbose", Value: "NULL"}})
	require.Nil(t, err)
	require.Equal(t, option.Verbose, true)

	option, err = getExplainOption(ctx, []tree.OptionElem{{Name: "verbose", Value: "false"}})
	require.Nil(t, err)
	require.Equal(t, option.Verbose, false)

	_, err = getExplainOption(ctx, []tree.OptionElem{{Name: "verbose", Value: "???"}})
	require.NotNil(t, err)

	// analyze
	option, err = getExplainOption(ctx, []tree.OptionElem{{Name: "analyze", Value: "true"}})
	require.Nil(t, err)
	require.Equal(t, option.Analyze, true)

	option, err = getExplainOption(ctx, []tree.OptionElem{{Name: "analyze", Value: "NULL"}})
	require.Nil(t, err)
	require.Equal(t, option.Analyze, true)

	option, err = getExplainOption(ctx, []tree.OptionElem{{Name: "analyze", Value: "false"}})
	require.Nil(t, err)
	require.Equal(t, option.Analyze, false)

	_, err = getExplainOption(ctx, []tree.OptionElem{{Name: "analyze", Value: "???"}})
	require.NotNil(t, err)

	// format
	option, err = getExplainOption(ctx, []tree.OptionElem{{Name: "format", Value: "text"}})
	require.Nil(t, err)
	require.Equal(t, option.Format, explain.EXPLAIN_FORMAT_TEXT)

	_, err = getExplainOption(ctx, []tree.OptionElem{{Name: "format", Value: "json"}})
	require.NotNil(t, err)

	_, err = getExplainOption(ctx, []tree.OptionElem{{Name: "format", Value: "dot"}})
	require.NotNil(t, err)

	_, err = getExplainOption(ctx, []tree.OptionElem{{Name: "format", Value: "???"}})
	require.NotNil(t, err)

	// other
	_, err = getExplainOption(ctx, []tree.OptionElem{{Name: "???", Value: "???"}})
	require.NotNil(t, err)
}

func Test_ExecRequest(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("boot mce succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx, rsStubs := mockRecordStatement(ctx)
		defer rsStubs.Reset()

		srStub := gostub.Stub(&parsers.HandleSqlForRecord, func(sql string) []string {
			return make([]string, 7)
		})
		defer srStub.Reset()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), txnOperator).Return(nil, nil).AnyTimes()

		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		use_t := mock_frontend.NewMockComputationWrapper(ctrl)
		use_t.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
		stmts, err := parsers.Parse(ctx, dialect.MYSQL, "use T", 1, 0)
		if err != nil {
			t.Error(err)
		}
		use_t.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		use_t.EXPECT().RecordExecPlan(ctx).Return(nil).AnyTimes()
		use_t.EXPECT().Clear().AnyTimes()

		runner := mock_frontend.NewMockComputationRunner(ctrl)
		runner.EXPECT().Run(gomock.Any()).Return(nil, nil).AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		convey.So(err, convey.ShouldBeNil)
		setGlobalPu(pu)

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, proto, nil)
		proto.SetSession(ses)
		ses.txnHandler = &TxnHandler{
			storage: &engine.EntireEngine{Engine: pu.StorageEngine},
		}

		ctx = context.WithValue(ctx, config.ParameterUnitKey, pu)

		// A mock autoincrcache manager.
		req := &Request{
			cmd:  COM_SET_OPTION,
			data: []byte("123"),
		}
		ec := newTestExecCtx(ctx, ctrl)

		_, err = ExecRequest(ses, ec, req)
		convey.So(err, convey.ShouldBeNil)

		req = &Request{
			cmd:  COM_SET_OPTION,
			data: []byte("1"),
		}
		_, err = ExecRequest(ses, ec, req)
		convey.So(err, convey.ShouldBeNil)
	})
}
