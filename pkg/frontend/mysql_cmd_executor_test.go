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
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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
	plan0 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
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

func TestRecordStatementResetsDivByZeroErrorMode(t *testing.T) {
	ctx := context.Background()
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))

	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	proc := ses.GetProc()
	require.NotNil(t, proc)

	atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, 0)
	cw := InitTxnComputationWrapper(ses, &tree.Insert{}, proc)
	statementCtx, err := RecordStatement(ctx, ses, proc, cw, time.Now(), "insert into t values (1, 10 / 0)", constant.ExternSql, true)
	require.NoError(t, err)
	require.NotNil(t, resource.RootFromContext(statementCtx))

	require.Equal(t, int32(-1), atomic.LoadInt32(&proc.Base.DivByZeroErrorMode))
	require.Equal(t, "Insert", ses.GetStmtType())
	require.Equal(t, tree.QueryTypeDML, ses.GetQueryType())
}

func TestRecordStatementSetsIgnoreForInsertIgnore(t *testing.T) {
	ctx := context.Background()
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))

	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	proc := ses.GetProc()
	require.NotNil(t, proc)

	insertIgnore := &tree.Insert{OnDuplicateUpdate: tree.UpdateExprs{nil}}
	cw := InitTxnComputationWrapper(ses, insertIgnore, proc)
	_, err := RecordStatement(ctx, ses, proc, cw, time.Now(), "insert ignore into t values (1, 10 / 0)", constant.ExternSql, true)
	require.NoError(t, err)
	require.True(t, ses.GetStmtProfile().GetDivByZeroIgnore())

	insert := &tree.Insert{}
	cw = InitTxnComputationWrapper(ses, insert, proc)
	_, err = RecordStatement(ctx, ses, proc, cw, time.Now(), "insert into t values (1, 10 / 0)", constant.ExternSql, true)
	require.NoError(t, err)
	require.False(t, ses.GetStmtProfile().GetDivByZeroIgnore())
}

func TestRefreshProcessStmtProfileForPreparedStmtUsesInnerInsert(t *testing.T) {
	ctx := context.Background()
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))

	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	proc := ses.GetProc()
	require.NotNil(t, proc)

	cw := InitTxnComputationWrapper(ses, &tree.Execute{}, proc)
	_, err := RecordStatement(ctx, ses, proc, cw, time.Now(), "execute stmt1", constant.ExternSql, true)
	require.NoError(t, err)
	require.Equal(t, "Execute", ses.GetStmtType())
	require.Equal(t, tree.QueryTypeOth, ses.GetQueryType())

	atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, 0)
	insertIgnore := &tree.Insert{OnDuplicateUpdate: tree.UpdateExprs{nil}}
	refreshProcessDivByZeroProfileForPreparedStmt(proc, insertIgnore)

	stmtType, queryType, ignore := proc.GetStmtProfile().GetDivByZeroRuntimeProfile()
	require.Equal(t, "Insert", stmtType)
	require.Equal(t, tree.QueryTypeDML, queryType)
	require.True(t, ignore)

	refreshProcessDivByZeroProfileForPreparedStmt(proc, &tree.Insert{})
	_, _, ignore = proc.GetStmtProfile().GetDivByZeroRuntimeProfile()
	require.False(t, ignore)

	refreshProcessDivByZeroProfileForPreparedStmt(proc, nil)
	// nil statement should be a no-op; previous runtime profile remains.
	_, _, ignore = proc.GetStmtProfile().GetDivByZeroRuntimeProfile()
	require.False(t, ignore)
}

func TestTxnComputationWrapperCompileRefreshesProfileForBinaryExecute(t *testing.T) {
	ctx := context.Background()
	ctx = statistic.ContextWithStatsInfo(ctx, statistic.NewStatsInfo())
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))

	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	proc := ses.GetProc()
	require.NotNil(t, proc)
	proc.Base.SessionInfo.StorageEngine = &disttae.Engine{}

	stmtName := getPrepareStmtName(1)
	prepareString := tree.NewPrepareString(tree.Identifier(stmtName), "select 1")
	stmts, err := mysql.Parse(ctx, prepareString.Sql, 1)
	require.NoError(t, err)
	preparePlan, err := buildPlan(ctx, nil, plan.NewEmptyCompilerContext(), prepareString)
	require.NoError(t, err)

	prepareStmt := &PrepareStmt{
		Name:                stmtName,
		Sql:                 prepareString.Sql,
		PreparePlan:         preparePlan,
		PrepareStmt:         stmts[0],
		compile:             compile.NewCompile("", "", prepareString.Sql, "", "", nil, proc, stmts[0], false, nil, time.Now()),
		getFromSendLongData: make(map[int]struct{}),
	}
	defer prepareStmt.Close()
	require.NoError(t, ses.SetPrepareStmt(ctx, stmtName, prepareStmt))

	cw := InitTxnComputationWrapper(ses, stmts[0], proc)
	cw.plan = preparePlan.GetDcl().GetPrepare().Plan
	execCtx := &ExecCtx{
		reqCtx: ctx,
		input: &UserInput{
			stmtName:            stmtName,
			isBinaryProtExecute: true,
		},
	}

	atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, 0)
	ret, err := cw.Compile(execCtx, nil)
	require.NoError(t, err)
	require.NotNil(t, ret)
	stmtType, queryType, ignore := proc.GetStmtProfile().GetDivByZeroRuntimeProfile()
	require.Equal(t, "Select", stmtType)
	require.Equal(t, tree.QueryTypeDQL, queryType)
	require.False(t, ignore)
}

func Test_mce(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
	convey.Convey("boot mce succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx, rsStubs := mockRecordStatement(ctx)
		defer rsStubs.Reset()

		srStub := gostub.Stub(&parsers.HandleSqlForRecord, func(sql string) []string {
			return []string{sql}
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
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		tConn := &testConn{}
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())
		ioses, err := NewIOSession(tConn, pu, "")
		convey.So(err, convey.ShouldBeNil)
		use_t := mock_frontend.NewMockComputationWrapper(ctrl)
		use_t.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
		use_t.EXPECT().Clear().AnyTimes()
		stmts, err := parsers.Parse(ctx, dialect.MYSQL, "use T", 1)
		if err != nil {
			t.Error(err)
		}
		use_t.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		use_t.EXPECT().RecordExecPlan(ctx, nil).Return(nil).AnyTimes()
		use_t.EXPECT().Plan().Return(&plan.Plan{}).AnyTimes()
		use_t.EXPECT().Free().AnyTimes()

		runner := mock_frontend.NewMockComputationRunner(ctrl)
		runner.EXPECT().Run(gomock.Any()).Return(nil, nil).AnyTimes()

		create_1 := mock_frontend.NewMockComputationWrapper(ctrl)
		stmts, err = parsers.Parse(ctx, dialect.MYSQL, "create table A(a varchar(100),b int,c float)", 1)
		if err != nil {
			t.Error(err)
		}
		create_1.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		create_1.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
		create_1.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
		create_1.EXPECT().Run(gomock.Any()).Return(nil, nil).AnyTimes()
		create_1.EXPECT().GetLoadTag().Return(false).AnyTimes()
		create_1.EXPECT().RecordExecPlan(ctx, nil).Return(nil).AnyTimes()
		create_1.EXPECT().Clear().AnyTimes()
		create_1.EXPECT().Free().AnyTimes()
		create_1.EXPECT().Plan().Return(&plan.Plan{}).AnyTimes()

		select_1 := mock_frontend.NewMockComputationWrapper(ctrl)
		stmts, err = parsers.Parse(ctx, dialect.MYSQL, "select a,b,c from A", 1)
		if err != nil {
			t.Error(err)
		}
		select_1.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		select_1.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
		select_1.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
		select_1.EXPECT().Run(gomock.Any()).Return(nil, nil).AnyTimes()
		select_1.EXPECT().GetLoadTag().Return(false).AnyTimes()
		select_1.EXPECT().RecordExecPlan(ctx, nil).Return(nil).AnyTimes()
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
			stmts, err = parsers.Parse(ctx, dialect.MYSQL, self_handle_sql[i], 1)
			convey.So(err, convey.ShouldBeNil)
			select_2.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
			select_2.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
			select_2.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
			select_2.EXPECT().Run(gomock.Any()).Return(nil, nil).AnyTimes()
			select_2.EXPECT().GetLoadTag().Return(false).AnyTimes()
			select_2.EXPECT().GetColumns(gomock.Any()).Return(self_handle_sql_columns[i], nil).AnyTimes()
			select_2.EXPECT().RecordExecPlan(ctx, nil).Return(nil).AnyTimes()
			select_2.EXPECT().RecordCompoundStmt(ctx, statistic.StatsArray{}).Return(nil).AnyTimes()
			select_2.EXPECT().Clear().AnyTimes()
			select_2.EXPECT().Free().AnyTimes()
			select_2.EXPECT().Plan().Return(&plan.Plan{}).AnyTimes()
			cws = append(cws, select_2)
		}

		stubs := gostub.Stub(&GetComputationWrapper, func(
			execCtx *ExecCtx,
			db string,
			user string,
			eng engine.Engine,
			proc *process.Process,
			ses *Session,
		) ([]ComputationWrapper, error) {
			if strings.HasPrefix(strings.ToLower(execCtx.input.getSql()), "use ") {
				return []ComputationWrapper{use_t}, nil
			}
			return cws, nil
		})
		defer stubs.Reset()

		pu, err = getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		convey.So(err, convey.ShouldBeNil)
		setPu("", pu)
		pu.SV.SkipCheckPrivilege = true

		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)
		ses := NewSession(ctx, "", proto, nil)
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
		_ = ses.InitSystemVariables(ctx, nil)

		ctx = context.WithValue(ctx, config.ParameterUnitKey, pu)

		// A mock autoincrcache manager.
		query := "create table A(a varchar(100),b int,c float);select a,b,c from A;" +
			strings.Join(self_handle_sql, ";")
		req := &Request{
			cmd:  COM_QUERY,
			data: []byte(query),
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
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		pu, err = getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setPu("", pu)

		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
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
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
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
		_ = ses.InitSystemVariables(ctx, nil)

		err = handleCmdFieldList(ses, ec, cflStmt)
		convey.So(err, convey.ShouldBeNil)

		err = handleCmdFieldList(ses, ec, cflStmt)
		convey.So(err, convey.ShouldBeNil)

		set := "set @@tx_isolation=`READ-COMMITTED`"
		setVar, err := parsers.ParseOne(ctx, dialect.MYSQL, set, 1)
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
		setRowCount(ses, ses.GetProc(), 7)

		resp, err = ExecRequest(ses, ec, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldBeNil)
		convey.So(ses.GetLastAffectedRows(), convey.ShouldEqual, int64(-1))
		convey.So(ses.GetProc().GetAffectedRows(), convey.ShouldEqual, int64(-1))
	})
}

func Test_getDataFromPipeline(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("getDataFromPipeline", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZeroNoFixed()
		defer mpool.DeleteMPool(mp)

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
		ses.mrs = &MysqlResultSet{}
		proto.ses = ses

		genBatch := func() *batch.Batch {
			return allocTestBatch(
				mp,
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

		err = getDataFromPipeline(ses, ec, batchCase1, nil)
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

		err = getDataFromPipeline(ses, ec, batchCase2, nil)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("getDataFromPipeline fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mp := mpool.MustNewZeroNoFixed()
		defer mpool.DeleteMPool(mp)

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
		ses.mrs = &MysqlResultSet{}
		proto.ses = ses
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		convey.So(getDataFromPipeline(ses, ec, nil, nil), convey.ShouldBeNil)

		genBatch := func() *batch.Batch {
			return allocTestBatch(
				mp,
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

		err = getDataFromPipeline(ses, ec, batchCase2, nil)
		convey.So(err, convey.ShouldBeNil)

		batchCase2.Vecs = append(batchCase2.Vecs, vector.NewVec(types.T_any.ToType()))
		err = getDataFromPipeline(ses, ec, batchCase2, nil)
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
			types.T_uuid,
			types.T_date,
			types.T_time,
			types.T_datetime,
			types.T_json,
			types.T_geometry,
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
			{tp: defines.MYSQL_TYPE_VAR_STRING, signed: true},
			{tp: defines.MYSQL_TYPE_DATE, signed: true},
			{tp: defines.MYSQL_TYPE_TIME, signed: true},
			{tp: defines.MYSQL_TYPE_DATETIME, signed: true},
			{tp: defines.MYSQL_TYPE_JSON, signed: true},
			{tp: defines.MYSQL_TYPE_GEOMETRY, signed: true},
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

func allocTestBatch(mp *mpool.MPool, attrName []string, tt []types.Type, batchSize int) *batch.Batch {
	batchData := batch.New(attrName)

	//alloc space for vector
	for i := 0; i < len(attrName); i++ {
		vec := vector.NewVec(tt[i])
		if err := vec.PreExtend(batchSize, mp); err != nil {
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
		setSessionAlloc("", NewLeakCheckAllocator())
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), nil).Return(nil, nil).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		pu.StorageEngine = eng
		pu.TxnClient = txnClient
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
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
		_ = ses.InitSystemVariables(ctx, nil)

		proto.SetSession(ses)
		ec := newTestExecCtx(ctx, ctrl)

		shv := &tree.ShowVariables{Global: false}
		convey.So(handleShowVariables(ses, ec, shv), convey.ShouldBeNil)

		// Ensure global shows global value even if session differs.
		ses.sesSysVars.Set("interactive_timeout", int64(30100))
		ses.gSysVars.Set("interactive_timeout", int64(86400))
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "show global variables like 'interactive_timeout'", 1)
		convey.So(err, convey.ShouldBeNil)
		showVars, ok := stmt.(*tree.ShowVariables)
		convey.So(ok, convey.ShouldBeTrue)
		ses.SetMysqlResultSet(&MysqlResultSet{})
		convey.So(handleShowVariables(ses, ec, showVars), convey.ShouldBeNil)
		mrs := ses.GetMysqlResultSet()
		found := false
		for _, row := range mrs.Data {
			if len(row) >= 2 && row[0] == "interactive_timeout" {
				convey.So(row[1], convey.ShouldEqual, int64(86400))
				found = true
				break
			}
		}
		convey.So(found, convey.ShouldBeTrue)
	})
}

func TestShowGlobalVariablesRefreshesGlobalSysVarCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	ses := newSes(nil, ctrl)
	ses.SetMysqlResultSet(&MysqlResultSet{})
	ses.gSysVars.Set("long_query_time", float64(10))
	require.NoError(t, ses.SetSessionSysVar(ctx, "interactive_timeout", int64(30100)))
	require.NoError(t, ses.SetSessionSysVar(ctx, "enable_remap_hint", "on"))
	require.True(t, ses.rewriteEnabled.Load())

	bh := &backgroundExecTest{}
	bh.init()
	sql := getSqlForGetSystemVariablesWithAccount(sysAccountID)
	bh.sql2result[sql] = newMrsForGlobalSystemVariables([][]interface{}{
		{"long_query_time", "1.1"},
	})

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "show global variables like 'long_query_time'", 1)
	require.NoError(t, err)
	showVars, ok := stmt.(*tree.ShowVariables)
	require.True(t, ok)

	ec := newTestExecCtx(ctx, ctrl)
	require.NoError(t, doShowVariables(ses, ec, showVars))

	mrs := ses.GetMysqlResultSet()
	require.Equal(t, uint64(1), mrs.GetRowCount())
	row, err := mrs.GetRow(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, "long_query_time", row[0])
	require.Equal(t, float64(1.1), row[1])
	require.Contains(t, bh.executedSQLs, sql)

	sessionTimeout, err := ses.GetSessionSysVar("interactive_timeout")
	require.NoError(t, err)
	require.Equal(t, int64(30100), sessionTimeout)
	enableRemapHint, err := ses.GetSessionSysVar("enable_remap_hint")
	require.NoError(t, err)
	require.Equal(t, int8(1), enableRemapHint)
	require.True(t, ses.rewriteEnabled.Load())
}

func TestShowGlobalVariablesReturnsRefreshError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	ses := newSes(nil, ctrl)
	ses.SetMysqlResultSet(&MysqlResultSet{})

	bh := &backgroundExecTest{}
	bh.init()
	sql := getSqlForGetSystemVariablesWithAccount(sysAccountID)
	bh.sql2err[sql] = moerr.NewInternalErrorNoCtx("refresh global sysvars failed")

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "show global variables like 'long_query_time'", 1)
	require.NoError(t, err)
	showVars, ok := stmt.(*tree.ShowVariables)
	require.True(t, ok)

	err = doShowVariables(ses, newTestExecCtx(ctx, ctrl), showVars)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refresh global sysvars failed")
	require.Contains(t, bh.executedSQLs, sql)
}

func newMrsForGlobalSystemVariables(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("variable_name")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col1)

	col2 := &MysqlColumn{}
	col2.SetName("variable_value")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col2)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
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
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

		sysVars := make(map[string]interface{})
		for name, sysVar := range gSysVarsDefs {
			sysVars[name] = sysVar.Default
		}
		ses := &Session{planCache: newPlanCache(1),
			feSessionImpl: feSessionImpl{
				gSysVars: &SystemVariables{mp: sysVars},
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

func TestGetComputationWrapperKeepsSchedulingSQLPerStatement(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	execCtx.input = &UserInput{sql: "select /*+ SET_VAR(query_max_workers=1) */ 1;; " +
		"select /*+ SET_VAR(query_pool_strict=on) */ 2"}

	cws, err := GetComputationWrapper(execCtx, "", "root", nil, proc, ses)
	require.NoError(t, err)
	require.Len(t, cws, 2)
	defer func() {
		for _, cw := range cws {
			cw.Free()
		}
	}()

	first := cws[0].(interface{ SchedulingSQL() string }).SchedulingSQL()
	second := cws[1].(interface{ SchedulingSQL() string }).SchedulingSQL()
	require.Contains(t, first, "query_max_workers=1")
	require.NotContains(t, first, "query_pool_strict")
	require.Contains(t, second, "query_pool_strict=on")
	require.NotContains(t, second, "query_max_workers")
}

func TestGetComputationWrapperKeepsExecutableCommentStatementWhole(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	const sql = "/*!40101 use mysql_ddl_test_db_3; */"
	execCtx.input = &UserInput{sql: sql}

	cws, err := GetComputationWrapper(execCtx, "", "root", nil, proc, ses)
	require.NoError(t, err)
	require.Len(t, cws, 1)
	defer cws[0].Free()

	require.Equal(t, sql, cws[0].(interface{ SchedulingSQL() string }).SchedulingSQL())
	records, err := sqlForRecordByStatement(ctx, sql)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Contains(t, records[0], "use mysql_ddl_test_db_3")
}

func TestGetComputationWrapperKeepsRemapPerStatement(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	require.NoError(t, ses.SetSessionSysVar(ctx, "enable_remap_hint", int64(1)))
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	tests := []struct {
		name string
		sql  string
		want []map[string]string
	}{
		{
			name: "later statement only",
			sql:  `select 1; /*+ {"remapdb":{"src":"second_db"}} */ analyze table src.t(id)`,
			want: []map[string]string{nil, {"src": "second_db"}},
		},
		{
			name: "distinct consecutive overrides",
			sql: `/*+ {"remapdb":{"src":"first_db"}} */ select * from src.t; ` +
				`/*+ {"remapdb":{"src":"second_db"}} */ analyze table src.t(id)`,
			want: []map[string]string{{"src": "first_db"}, {"src": "second_db"}},
		},
		{
			name: "session policy restored after inline override",
			sql: `/*+ {"remapdb":{"src":"session_db"}} */ select * from src.t; ` +
				`/*+ {"remapdb":{"src":"inline_db"}} */ analyze table src.t(id); ` +
				`/*+ {"remapdb":{"src":"session_db"}} */ select * from src.t`,
			want: []map[string]string{{"src": "session_db"}, {"src": "inline_db"}, {"src": "session_db"}},
		},
	}

	type remapCarrier interface {
		GetRemapDb() map[string]string
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			execCtx := newTestExecCtx(ctx, ctrl)
			execCtx.ses = ses
			execCtx.input = &UserInput{sql: tt.sql}
			cws, err := GetComputationWrapper(execCtx, "src", "root", nil, proc, ses)
			require.NoError(t, err)
			require.Len(t, cws, len(tt.want))
			defer func() {
				for _, cw := range cws {
					cw.Free()
				}
			}()
			for i, cw := range cws {
				carrier, ok := cw.(remapCarrier)
				require.True(t, ok, "wrapper %d does not carry statement remap", i)
				require.Equal(t, tt.want[i], carrier.GetRemapDb(), "wrapper %d", i)
				if dst := tt.want[i]["src"]; dst != "" {
					formatted := tree.String(cw.GetAst(), dialect.MYSQL)
					require.Contains(t, formatted, dst+".t", "wrapper %d AST", i)
					require.NotContains(t, formatted, "src.t", "wrapper %d AST", i)
				}
			}
		})
	}

}

func TestGetComputationWrapperUsesRequestRewriteSnapshot(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	ses.rewriteEnabled.Store(false)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	execCtx.input = &UserInput{
		sql: `/*+ {"rewrites":{"src.t":["select * from src.t where keep = 1"]},` +
			`"remapdb":{"src":"dst"}} */ select * from src.t`,
		rewritePolicy:             &rewritePolicySnapshot{enabled: true},
		rewritePolicyMaterialized: true,
	}
	cws, err := GetComputationWrapper(execCtx, "src", "root", nil, proc, ses)
	require.NoError(t, err)
	require.Len(t, cws, 1)
	defer cws[0].Free()

	carrier, ok := cws[0].(interface{ GetRemapDb() map[string]string })
	require.True(t, ok)
	require.Equal(t, map[string]string{"src": "dst"}, carrier.GetRemapDb())
	require.Contains(t, tree.String(cws[0].GetAst(), dialect.MYSQL), "dst.t")
	selectStmt, ok := cws[0].GetAst().(*tree.Select)
	require.True(t, ok)
	require.NotNil(t, selectStmt.RewriteOption)
	require.Len(t, selectStmt.RewriteOption.Rewrites["src.t"], 1)
}

func TestGetComputationWrapperRestoresStatementRemapOnPlanCacheHit(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	ses.planCache = newPlanCache(1)
	require.NoError(t, ses.SetSessionSysVar(ctx, "enable_remap_hint", int64(1)))
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sql := `/*+ {"remapdb":{"src":"first_db"}} */ select * from src.t; ` +
		`/*+ {"remapdb":{"src":"second_db"}} */ select * from src.t`
	input := &UserInput{sql: sql}
	input.genHash()
	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	ses.planCache.cache(input.getHash(), stmts, []*plan.Plan{{}, {}})
	t.Cleanup(ses.planCache.clean)

	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	execCtx.input = input
	cws, err := GetComputationWrapper(execCtx, "src", "root", nil, proc, ses)
	require.NoError(t, err)
	require.Len(t, cws, 2)
	type remapCarrier interface {
		GetRemapDb() map[string]string
	}
	for i, want := range []string{"first_db", "second_db"} {
		carrier, ok := cws[i].(remapCarrier)
		require.True(t, ok)
		require.Equal(t, want, carrier.GetRemapDb()["src"], "wrapper %d", i)
	}
}

func TestPrepareStringStatementAppliesRemapPolicy(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	ses.rewriteEnabled.Store(true)
	ses.ruleCache = make(map[string]string)
	require.NoError(t, ses.SetSessionSysVar(ctx, "remap_rewrites",
		`{"remapdb":{"src":"session_db"}}`))
	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	execCtx.rewriteEnabled = true

	tests := []struct {
		name      string
		sql       string
		want      string
		qualified bool
	}{
		{
			name:      "session policy",
			sql:       "select * from src.t",
			want:      "session_db",
			qualified: true,
		},
		{
			name:      "inline policy overrides session",
			sql:       `/*+ {"remapdb":{"src":"inline_db"}} */ select * from src.t`,
			want:      "inline_db",
			qualified: true,
		},
		{
			name: "unqualified table stays unqualified",
			sql:  "select * from t",
			want: "session_db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outerSQL, err := rewriteSQL(ctx, ses,
				`prepare test_stmt from 'select 1'`)
			require.NoError(t, err)
			execCtx.sqlOfStmt = outerSQL
			rewritten, stmt, remap, err := prepareStringStatement(execCtx, ses, tt.sql)
			require.NoError(t, err)
			require.NotNil(t, stmt)
			defer stmt.Free()
			require.NotEmpty(t, rewritten)
			require.Equal(t, tt.want, remap["src"])
			formatted := tree.String(stmt, dialect.MYSQL)
			if tt.qualified {
				require.Contains(t, formatted, tt.want+".t")
				require.NotContains(t, formatted, "src.t")
			} else {
				require.Equal(t, "select * from t", formatted)
			}
		})
	}

	t.Run("inner inline works without outer policy", func(t *testing.T) {
		execCtx.sqlOfStmt = `prepare inline_only from 'select 1'`
		_, stmt, remap, err := prepareStringStatement(execCtx, ses,
			`/*+ {"remapdb":{"src":"inline_only_db"}} */ select * from src.t`)
		require.NoError(t, err)
		defer stmt.Free()
		require.Equal(t, "inline_only_db", remap["src"])
		require.Contains(t, tree.String(stmt, dialect.MYSQL), "inline_only_db.t")
	})

	t.Run("uses materialized outer snapshot after session changes", func(t *testing.T) {
		outerSQL, err := rewriteSQL(ctx, ses,
			`prepare snap from 'select * from src.t'`)
		require.NoError(t, err)
		require.NoError(t, ses.SetSessionSysVar(ctx, "remap_rewrites",
			`{"remapdb":{"src":"new_db"}}`))
		execCtx.sqlOfStmt = outerSQL

		_, stmt, remap, err := prepareStringStatement(execCtx, ses, "select * from src.t")
		require.NoError(t, err)
		defer stmt.Free()
		require.Equal(t, "session_db", remap["src"])
		require.Contains(t, tree.String(stmt, dialect.MYSQL), "session_db.t")
	})
}

func TestPrepareStringStatementCombinesSQLModeAndRemapPolicy(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	ses.rewriteEnabled.Store(true)
	ses.ruleCache = make(map[string]string)
	require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", "ANSI_QUOTES"))
	require.NoError(t, ses.SetSessionSysVar(ctx, "remap_rewrites",
		`{"remapdb":{"src":"session_db"}}`))

	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	execCtx.rewriteEnabled = true
	outerSQL, err := rewriteSQL(ctx, ses, `prepare test_stmt from 'select 1'`)
	require.NoError(t, err)
	execCtx.sqlOfStmt = outerSQL

	_, stmt, remap, err := prepareStringStatement(execCtx, ses,
		`select "c" from src.t`)
	require.NoError(t, err)
	require.NotNil(t, stmt)
	defer stmt.Free()
	require.Equal(t, "session_db", remap["src"])

	selectStmt, ok := stmt.(*tree.Select)
	require.True(t, ok)
	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	require.True(t, ok)
	require.Len(t, selectClause.Exprs, 1)
	name, ok := selectClause.Exprs[0].Expr.(*tree.UnresolvedName)
	require.True(t, ok, "ANSI_QUOTES must parse a double-quoted token as an identifier")
	require.Equal(t, "c", name.ColName())
	require.Contains(t, tree.String(stmt, dialect.MYSQL), "session_db.t")
}

func TestGetComputationWrapperRestoresPreparedStatementRemap(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	stmt := &tree.Select{}
	prepareString := tree.NewPrepareString("stmt1", "select 1")
	preparePlan, err := buildPlan(ctx, nil, plan.NewEmptyCompilerContext(), prepareString)
	require.NoError(t, err)
	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	execCtx.input = &UserInput{
		sql:         "execute stmt1",
		stmt:        stmt,
		preparePlan: preparePlan,
		remapDb:     map[string]string{"src": "prepared_db"},
	}

	cws, err := GetComputationWrapper(execCtx, "src", "root", nil, proc, ses)
	require.NoError(t, err)
	require.Len(t, cws, 1)
	carrier, ok := cws[0].(interface{ GetRemapDb() map[string]string })
	require.True(t, ok)
	require.Equal(t, "prepared_db", carrier.GetRemapDb()["src"])
	cws[0].Free()
}

func TestInstallStatementRemapClearsPreviousPolicy(t *testing.T) {
	execCtx := &ExecCtx{remapDb: map[string]string{"src": "stale_db"}}
	first := &TxnComputationWrapper{}
	first.SetRemapDb(map[string]string{"src": "first_db"})
	second := &TxnComputationWrapper{}
	second.SetRemapDb(map[string]string{"src": "second_db"})
	withoutRemap := &TxnComputationWrapper{}

	installStatementRemap(execCtx, first)
	require.Equal(t, "first_db", execCtx.remapDb["src"])
	installStatementRemap(execCtx, second)
	require.Equal(t, "second_db", execCtx.remapDb["src"])
	installStatementRemap(execCtx, withoutRemap)
	require.Nil(t, execCtx.remapDb)
}

func Test_GetComputationWrapper_ShowVariablesGlobal(t *testing.T) {
	convey.Convey("GetComputationWrapper show global variables", t, func() {
		sql := "show global variables like 'interactive_timeout'"
		var eng engine.Engine
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

		sysVars := make(map[string]interface{})
		for name, sysVar := range gSysVarsDefs {
			sysVars[name] = sysVar.Default
		}
		ses := &Session{planCache: newPlanCache(1),
			feSessionImpl: feSessionImpl{
				gSysVars: &SystemVariables{mp: sysVars},
			},
		}
		ctrl := gomock.NewController(t)
		ec := newTestExecCtx(context.Background(), ctrl)
		ec.ses = ses
		ec.input = &UserInput{sql: sql}

		cws, err := GetComputationWrapper(ec, "", "", eng, proc, ses)
		convey.So(err, convey.ShouldBeNil)
		convey.So(len(cws) > 0, convey.ShouldBeTrue)
		stmt := cws[0].GetAst()
		sv, ok := stmt.(*tree.ShowVariables)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(sv.Global, convey.ShouldBeTrue)
	})
}

// Test_GetComputationWrapper_InternalCmds tests the internal command parsing in GetComputationWrapper
func Test_GetComputationWrapper_InternalCmds(t *testing.T) {
	ctx := context.Background()

	convey.Convey("GetComputationWrapper internal commands", t, func() {
		db, user := "T", "root"
		var eng engine.Engine
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

		sysVars := make(map[string]interface{})
		for name, sysVar := range gSysVarsDefs {
			sysVars[name] = sysVar.Default
		}
		ses := &Session{
			feSessionImpl: feSessionImpl{
				gSysVars: &SystemVariables{mp: sysVars},
			},
		}
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Test internal_get_snapshot_ts command
		convey.Convey("internal_get_snapshot_ts command", func() {
			sql := makeGetSnapshotTsSql("snap1", "account1", "pub1")
			ec := newTestExecCtx(ctx, ctrl)
			ec.ses = ses
			ec.input = &UserInput{sql: sql}

			cw, err := GetComputationWrapper(ec, db, user, eng, proc, ses)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cw, convey.ShouldNotBeEmpty)
			convey.So(len(cw), convey.ShouldEqual, 1)
			_, ok := cw[0].GetAst().(*InternalCmdGetSnapshotTs)
			convey.So(ok, convey.ShouldBeTrue)
		})

		// Test internal_get_databases command
		convey.Convey("internal_get_databases command", func() {
			sql := makeGetDatabasesSql("snap1", "account1", "pub1", "account", "db1", "tbl1")
			ec := newTestExecCtx(ctx, ctrl)
			ec.ses = ses
			ec.input = &UserInput{sql: sql}

			cw, err := GetComputationWrapper(ec, db, user, eng, proc, ses)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cw, convey.ShouldNotBeEmpty)
			convey.So(len(cw), convey.ShouldEqual, 1)
			_, ok := cw[0].GetAst().(*InternalCmdGetDatabases)
			convey.So(ok, convey.ShouldBeTrue)
		})

		// Test internal_get_mo_indexes command
		convey.Convey("internal_get_mo_indexes command", func() {
			sql := makeGetMoIndexesSql(123, "account1", "pub1", "snap1")
			ec := newTestExecCtx(ctx, ctrl)
			ec.ses = ses
			ec.input = &UserInput{sql: sql}

			cw, err := GetComputationWrapper(ec, db, user, eng, proc, ses)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cw, convey.ShouldNotBeEmpty)
			convey.So(len(cw), convey.ShouldEqual, 1)
			_, ok := cw[0].GetAst().(*InternalCmdGetMoIndexes)
			convey.So(ok, convey.ShouldBeTrue)
		})

		// Test internal_get_ddl command
		convey.Convey("internal_get_ddl command", func() {
			sql := makeGetDdlSql("snap1", "account1", "pub1", "table", "db1", "tbl1")
			ec := newTestExecCtx(ctx, ctrl)
			ec.ses = ses
			ec.input = &UserInput{sql: sql}

			cw, err := GetComputationWrapper(ec, db, user, eng, proc, ses)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cw, convey.ShouldNotBeEmpty)
			convey.So(len(cw), convey.ShouldEqual, 1)
			_, ok := cw[0].GetAst().(*InternalCmdGetDdl)
			convey.So(ok, convey.ShouldBeTrue)
		})

		// Test internal_get_object command
		convey.Convey("internal_get_object command", func() {
			sql := makeGetObjectSql("account1", "pub1", "object1", 0)
			ec := newTestExecCtx(ctx, ctrl)
			ec.ses = ses
			ec.input = &UserInput{sql: sql}

			cw, err := GetComputationWrapper(ec, db, user, eng, proc, ses)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cw, convey.ShouldNotBeEmpty)
			convey.So(len(cw), convey.ShouldEqual, 1)
			_, ok := cw[0].GetAst().(*InternalCmdGetObject)
			convey.So(ok, convey.ShouldBeTrue)
		})

		// Test internal_object_list command
		convey.Convey("internal_object_list command", func() {
			sql := makeObjectListSql("snap1", "snap0", "account1", "pub1")
			ec := newTestExecCtx(ctx, ctrl)
			ec.ses = ses
			ec.input = &UserInput{sql: sql}

			cw, err := GetComputationWrapper(ec, db, user, eng, proc, ses)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cw, convey.ShouldNotBeEmpty)
			convey.So(len(cw), convey.ShouldEqual, 1)
			_, ok := cw[0].GetAst().(*InternalCmdObjectList)
			convey.So(ok, convey.ShouldBeTrue)
		})

		// Test internal_check_snapshot_flushed command
		convey.Convey("internal_check_snapshot_flushed command", func() {
			sql := makeCheckSnapshotFlushedSql("snap1", "account1", "pub1")
			ec := newTestExecCtx(ctx, ctrl)
			ec.ses = ses
			ec.input = &UserInput{sql: sql}

			cw, err := GetComputationWrapper(ec, db, user, eng, proc, ses)
			convey.So(err, convey.ShouldBeNil)
			convey.So(cw, convey.ShouldNotBeEmpty)
			convey.So(len(cw), convey.ShouldEqual, 1)
			_, ok := cw[0].GetAst().(*InternalCmdCheckSnapshotFlushed)
			convey.So(ok, convey.ShouldBeTrue)
		})
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

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)
		ses := NewSession(ctx, "", proto, nil)
		ses.respr = &MysqlResp{mysqlRrWr: proto}
		ses.mrs = &MysqlResultSet{}
		ses.txnCompileCtx.execCtx = &ExecCtx{reqCtx: ctx, proc: testutil.NewProc(t), ses: ses}

		convey.So(handleFun(ses), convey.ShouldBeNil)
	})
}

func Test_HandlePrepareStmt(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "Prepare stmt1 from select 1, 2", 1)
	if err != nil {
		t.Errorf("parser sql error %v", err)
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ec := newTestExecCtx(ctx, ctrl)

	runTestHandle("handlePrepareStmt", t, func(ses *Session) error {
		stmt := stmt.(*tree.PrepareStmt)
		ec.resper = ses.respr
		_, err := handlePrepareStmt(ses, ec, stmt, "Prepare stmt1 from select 1, 2")
		return err
	})
}

func TestHandlePrepareStmtNameContainingFrom(t *testing.T) {
	setSessionAlloc("", NewLeakCheckAllocator())
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	const sql = "prepare fromx from select 1"
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	execCtx := newTestExecCtx(ctx, ctrl)

	runTestHandle("handlePrepareStmt name containing from", t, func(ses *Session) error {
		execCtx.resper = ses.respr
		prepared, err := handlePrepareStmt(ses, execCtx, stmt.(*tree.PrepareStmt), sql)
		if err != nil {
			return err
		}
		require.Equal(t, "select 1", prepared.Sql)
		return nil
	})
}

func TestHandlePrepareStmtExecutableCommentDelimiter(t *testing.T) {
	setSessionAlloc("", NewLeakCheckAllocator())
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	const sql = "prepare fromx /*! from */ select 1"
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	execCtx := newTestExecCtx(ctx, ctrl)

	runTestHandle("handlePrepareStmt executable comment delimiter", t, func(ses *Session) error {
		execCtx.resper = ses.respr
		prepared, err := handlePrepareStmt(ses, execCtx, stmt.(*tree.PrepareStmt), sql)
		if err != nil {
			return err
		}
		require.Equal(t, "select 1", prepared.Sql)
		return nil
	})
}

func TestHandlePrepareStmtQuotedCommentTerminator(t *testing.T) {
	setSessionAlloc("", NewLeakCheckAllocator())
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	const sql = "prepare fromx /*! from select 'x*/y' */"
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	execCtx := newTestExecCtx(ctx, ctrl)

	runTestHandle("handlePrepareStmt quoted comment terminator", t, func(ses *Session) error {
		execCtx.resper = ses.respr
		prepared, err := handlePrepareStmt(ses, execCtx, stmt.(*tree.PrepareStmt), sql)
		if err != nil {
			return err
		}
		require.Equal(t, "select 'x*/y'", prepared.Sql)
		return nil
	})
}

func TestExtractPrepareStmtSQL(t *testing.T) {
	testCases := []struct {
		name    string
		sql     string
		sqlMode string
		want    string
	}{
		{
			name: "name contains delimiter text",
			sql:  "prepare fromx from select 1",
			want: "select 1",
		},
		{
			name: "quoted delimiter name",
			sql:  "prepare `from` from\n\tselect 2",
			want: "select 2",
		},
		{
			name:    "ansi quoted delimiter name",
			sql:     `prepare "from" from select 3`,
			sqlMode: "ANSI_QUOTES",
			want:    "select 3",
		},
		{
			name: "preserve inner comment",
			sql:  "prepare from_name /* before delimiter */ from /* inner */ select 4",
			want: "/* inner */ select 4",
		},
		{
			name: "leading comment",
			sql:  "/* rewrite hint */ prepare fromx from select 5",
			want: "select 5",
		},
		{
			name: "executable comment delimiter",
			sql:  "prepare fromx /*! from */ select 1",
			want: "select 1",
		},
		{
			name: "statement inside executable comment",
			sql:  "prepare fromx /*! from select 2 */",
			want: "select 2",
		},
		{
			name: "quoted comment terminator",
			sql:  "prepare fromx /*! from select 'x*/y' */",
			want: "select 'x*/y'",
		},
		{
			name: "preserve comment after executable delimiter",
			sql:  "prepare fromx /*! from */ /* inner */ select 3",
			want: "/* inner */ select 3",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := extractPrepareStmtSQL(context.Background(), testCase.sql, testCase.sqlMode)
			require.NoError(t, err)
			require.Equal(t, testCase.want, got)
		})
	}
}

func TestExtractPrepareStmtSQLRejectsInvalidInput(t *testing.T) {
	for _, sql := range []string{
		"select 1",
		"prepare",
		"prepare stmt select 1",
		"prepare stmt /*! from select 1",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := extractPrepareStmtSQL(context.Background(), sql, "")
			require.Error(t, err)
		})
	}
}

func TestHandlePrepareStmtStoresRemapPolicy(t *testing.T) {
	setSessionAlloc("", NewLeakCheckAllocator())
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	parsed, err := parsers.ParseOne(ctx, dialect.MYSQL, "prepare stmt1 from select 1", 1)
	require.NoError(t, err)
	ctrl := gomock.NewController(t)
	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.remapDb = map[string]string{"src": "prepared_db"}

	runTestHandle("handlePrepareStmt stores remap", t, func(ses *Session) error {
		execCtx.resper = ses.respr
		prepared, err := handlePrepareStmt(ses, execCtx, parsed.(*tree.PrepareStmt), tree.String(parsed, dialect.MYSQL))
		if err != nil {
			return err
		}
		require.Equal(t, "prepared_db", prepared.remapDb["src"])
		return nil
	})
}

func Test_HandlePrepareStringUsesSessionSQLMode(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	setSessionAlloc("", NewLeakCheckAllocator())
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ec := newTestExecCtx(ctx, ctrl)

	runTestHandle("handlePrepareStringUsesSessionSQLMode", t, func(ses *Session) error {
		require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", "PIPES_AS_CONCAT"))
		ec.resper = ses.respr
		preStmt, err := handlePrepareString(ses, ec, tree.NewPrepareString("stmt_sql_mode", "select 'a'||'b'"))
		if err != nil {
			return err
		}
		defer preStmt.Close()
		requirePreparedSelectConcat(t, preStmt)
		return nil
	})
}

func Test_HandlePrepareVarUsesSessionSQLMode(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	setSessionAlloc("", NewLeakCheckAllocator())
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ec := newTestExecCtx(ctx, ctrl)

	runTestHandle("handlePrepareVarUsesSessionSQLMode", t, func(ses *Session) error {
		require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", "PIPES_AS_CONCAT"))
		require.NoError(t, ses.SetUserDefinedVar("stmt_var_sql", "select 'a'||'b'", "set @stmt_var_sql = 'select ''a''||''b'''"))
		ec.resper = ses.respr
		preStmt, err := handlePrepareVar(ses, ec, tree.NewPrepareVar("stmt_sql_mode_var", tree.NewVarExpr("stmt_var_sql", false, false, nil)))
		if err != nil {
			return err
		}
		defer preStmt.Close()
		requirePreparedSelectConcat(t, preStmt)
		return nil
	})
}

func TestHandlePrepareVarWithNonStringValue(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	setSessionAlloc("", NewLeakCheckAllocator())
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ec := newTestExecCtx(ctx, ctrl)

	runTestHandle("handlePrepareVarWithNonStringValue", t, func(ses *Session) error {
		ec.resper = ses.respr
		for _, testCase := range []struct {
			name  string
			value any
		}{
			{name: "integer_zero", value: int64(0)},
			{name: "integer_one", value: int64(1)},
			{name: "integer_min", value: int64(math.MinInt64)},
			{name: "integer_max", value: int64(math.MaxInt64)},
			{name: "unsigned_integer_max", value: uint64(math.MaxUint64)},
			{name: "float", value: float64(1.5)},
			{name: "boolean", value: true},
			{name: "array", value: []float32{1}},
			{name: "null", value: nil},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				var prepared *PrepareStmt
				var err error
				stmt := tree.NewPrepareVar(
					tree.Identifier("stmt_"+testCase.name),
					tree.NewVarExpr(testCase.name, false, false, nil),
				)
				defer stmt.Free()
				require.NoError(t, ses.SetUserDefinedVar(testCase.name, testCase.value, ""))
				require.NotPanics(t, func() {
					prepared, err = handlePrepareVar(ses, ec, stmt)
				})
				require.Nil(t, prepared)
				require.Error(t, err)
				require.Contains(t, err.Error(), "syntax error")
				require.NotContains(t, err.Error(), "panic")
				_, err = ses.GetPrepareStmt(ctx, "stmt_"+testCase.name)
				require.Error(t, err)
			})
		}

		require.NoError(t, ses.SetUserDefinedVar("valid_after_error", "select 1", ""))
		stmt := tree.NewPrepareVar(
			"stmt_valid_after_error",
			tree.NewVarExpr("valid_after_error", false, false, nil),
		)
		defer stmt.Free()
		prepared, err := handlePrepareVar(ses, ec, stmt)
		require.NoError(t, err)
		defer prepared.Close()
		return nil
	})
}

func requirePreparedSelectConcat(t *testing.T, preStmt *PrepareStmt) {
	t.Helper()
	selectStmt, ok := preStmt.PrepareStmt.(*tree.Select)
	require.True(t, ok)
	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	require.True(t, ok)
	require.Len(t, selectClause.Exprs, 1)
	fn, ok := selectClause.Exprs[0].Expr.(*tree.FuncExpr)
	require.True(t, ok)
	name, ok := fn.Func.FunctionReference.(*tree.UnresolvedName)
	require.True(t, ok)
	require.Equal(t, "concat", name.ColName())
}

func TestPrepareSQLModeStagedExecution(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", ""))

	tests := []struct {
		name      string
		sql       string
		staged    bool
		wantError bool
		first     string
	}{
		{
			name:   "expression value",
			sql:    `set sql_mode=concat('ANSI_','QUOTES'); select "c"`,
			staged: true,
			first:  `set sql_mode=concat('ANSI_','QUOTES');`,
		},
		{
			name:   "user variable value",
			sql:    `set @mode='ANSI_QUOTES'; set sql_mode=@mode; select "c"`,
			staged: true,
			first:  `set @mode='ANSI_QUOTES';`,
		},
		{
			name:   "failing expression precedes mode-sensitive syntax error",
			sql:    `set sql_mode=unknown_function(); select 'a\'; select 1`,
			staged: true,
			first:  `set sql_mode=unknown_function();`,
		},
		{
			name:   "compound statement",
			sql:    `set sql_mode='ANSI_QUOTES'; begin select 1; end; select "c"`,
			staged: true,
			first:  `set sql_mode='ANSI_QUOTES';`,
		},
		{
			name:   "mode set is final statement",
			sql:    `select 1; set sql_mode='ANSI_QUOTES'`,
			staged: false,
		},
		{
			name:   "global mode does not affect session parser",
			sql:    `set global sql_mode='ANSI_QUOTES'; select "c"`,
			staged: false,
		},
		{
			name:      "unrelated parse error",
			sql:       `select from`,
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first, _, staged, err := prepareSQLModeStagedExecution(ctx, ses, ses.GetMySQLParser(), test.sql)
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.staged, staged)
			require.Equal(t, test.first, first)
		})
	}
}

func TestNextSQLModeStatementUsesCurrentSessionMode(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", ""))

	input := &UserInput{sql: `set sql_mode=concat('ANSI_','QUOTES'); select "c"`}
	first, remaining, staged, err := prepareSQLModeStagedExecution(ctx, ses, ses.GetMySQLParser(), input.sql)
	require.NoError(t, err)
	require.True(t, staged)
	require.NotEmpty(t, first)

	require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", "ANSI_QUOTES"))
	nextInput, rest, err := nextSQLModeStatementInput(ctx, ses, ses.GetMySQLParser(), input, remaining)
	require.NoError(t, err)
	require.Empty(t, rest)

	execCtx := &ExecCtx{reqCtx: ctx, ses: ses, input: nextInput}
	stmts, err := parseSql(execCtx, ses.GetMySQLParser())
	require.NoError(t, err)
	defer freeStatements(stmts)
	require.Len(t, stmts, 1)
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	name, ok := selectClause.Exprs[0].Expr.(*tree.UnresolvedName)
	require.True(t, ok)
	require.Equal(t, "c", name.ColName())
}

func TestRefreshStatementScopedSessionInfo(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	proc := &process.Process{Base: &process.BaseProcess{}}

	require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", "ANSI_QUOTES"))
	refreshStatementScopedSessionInfo(ses, proc)
	require.False(t, proc.Base.SessionInfo.MatrixOneNativeMode)

	require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", "ANSI_QUOTES,MATRIXONE_NATIVE"))
	refreshStatementScopedSessionInfo(ses, proc)
	require.True(t, proc.Base.SessionInfo.MatrixOneNativeMode)
}

func TestBackgroundSessionInheritsUpstreamSQLMode(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", "PIPES_AS_CONCAT,MATRIXONE_NATIVE"))

	backSes := &backSession{}
	backSes.upstream = ses
	proc := &process.Process{Base: &process.BaseProcess{}}

	mode, err := backSes.GetSessionSysVar("sql_mode")
	require.NoError(t, err)
	require.Equal(t, "", mode)

	refreshBackgroundStatementScopedSessionInfo(backSes, &UserInput{}, proc)
	require.True(t, proc.Base.SessionInfo.MatrixOneNativeMode)
}

func TestBackgroundExplicitSQLModeOverridesUpstream(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", "MATRIXONE_NATIVE"))

	backSes := &backSession{}
	backSes.upstream = ses
	proc := &process.Process{Base: &process.BaseProcess{}}

	refreshBackgroundStatementScopedSessionInfo(backSes, &UserInput{
		useParserSQLMode: true,
		parserSQLMode:    "ANSI_QUOTES",
	}, proc)
	require.False(t, proc.Base.SessionInfo.MatrixOneNativeMode)

	refreshBackgroundStatementScopedSessionInfo(backSes, &UserInput{
		useParserSQLMode: true,
		parserSQLMode:    "ANSI_QUOTES,MATRIXONE_NATIVE",
	}, proc)
	require.True(t, proc.Base.SessionInfo.MatrixOneNativeMode)

	require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", ""))
	refreshBackgroundStatementScopedSessionInfo(backSes, &UserInput{}, proc)
	require.False(t, proc.Base.SessionInfo.MatrixOneNativeMode)
}

func TestNestedBackgroundSessionInheritsEffectiveSQLMode(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)

	tests := []struct {
		name         string
		sessionMode  string
		explicitMode *string
		wantNative   bool
	}{
		{name: "upstream_native", sessionMode: "MATRIXONE_NATIVE", wantNative: true},
		{name: "explicit_native", sessionMode: "", explicitMode: ptrTo("MATRIXONE_NATIVE"), wantNative: true},
		{name: "explicit_default", sessionMode: "MATRIXONE_NATIVE", explicitMode: ptrTo(""), wantNative: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", tc.sessionMode))

			parent := (&backSession{}).initFeSes(ses, nil, "", nil)
			parent.upstream = ses
			defer parent.Close()

			input := &UserInput{}
			if tc.explicitMode != nil {
				input.useParserSQLMode = true
				input.parserSQLMode = *tc.explicitMode
			}
			parentProc := &process.Process{Base: &process.BaseProcess{}}
			refreshBackgroundStatementScopedSessionInfo(parent, input, parentProc)
			require.Equal(t, tc.wantNative, parentProc.Base.SessionInfo.MatrixOneNativeMode)

			child := (&backSession{}).initFeSes(parent, nil, "", nil)
			defer child.Close()
			childProc := &process.Process{Base: &process.BaseProcess{}}
			refreshBackgroundStatementScopedSessionInfo(child, &UserInput{}, childProc)
			require.Equal(t, tc.wantNative, childProc.Base.SessionInfo.MatrixOneNativeMode)

			nextMode := "MATRIXONE_NATIVE"
			nextNative := true
			if tc.wantNative {
				nextMode = ""
				nextNative = false
			}
			refreshBackgroundStatementScopedSessionInfo(parent, &UserInput{
				useParserSQLMode: true,
				parserSQLMode:    nextMode,
			}, parentProc)
			refreshBackgroundStatementScopedSessionInfo(child, &UserInput{}, childProc)
			require.Equal(t, nextNative, childProc.Base.SessionInfo.MatrixOneNativeMode)
		})
	}
}

func TestSQLModeStagingDefersRewriteWithRequestSnapshot(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", ""))
	require.NoError(t, ses.SetSessionSysVar(ctx, "remap_rewrites", `{"remapdb":{"src":"dst"}}`))
	ses.rewriteEnabled.Store(true)
	ses.ruleCache = map[string]string{}

	policy, err := captureRewritePolicy(ctx, ses)
	require.NoError(t, err)
	input := &UserInput{
		sql:           `set sql_mode='NO_BACKSLASH_ESCAPES'; select 'a\'; select * from src.t`,
		rewritePolicy: policy,
	}
	first, remaining, staged, err := prepareSQLModeStagedExecution(ctx, ses, ses.GetMySQLParser(), input.sql)
	require.NoError(t, err)
	require.True(t, staged)
	_, err = rewriteSQLStatementInput(ctx, ses, newSQLStatementInput(input, ses, first))
	require.NoError(t, err)

	// Simulate earlier staged statements changing both the SQL mode and rewrite
	// state. Parsing follows the new mode; materialization follows the request
	// snapshot.
	require.NoError(t, ses.SetSessionSysVar(ctx, "sql_mode", "NO_BACKSLASH_ESCAPES"))
	ses.rewriteEnabled.Store(false)
	require.NoError(t, ses.SetSessionSysVar(ctx, "remap_rewrites", ""))

	second, remaining, err := nextSQLModeStatementInput(ctx, ses, ses.GetMySQLParser(), input, remaining)
	require.NoError(t, err)
	second, err = rewriteSQLStatementInput(ctx, ses, second)
	require.NoError(t, err)
	assertMaterializedRemap(t, ctx, second.sql, map[string]string{"src": "dst"})

	third, remaining, err := nextSQLModeStatementInput(ctx, ses, ses.GetMySQLParser(), input, remaining)
	require.NoError(t, err)
	require.Empty(t, remaining)
	third, err = rewriteSQLStatementInput(ctx, ses, third)
	require.NoError(t, err)
	assertMaterializedRemap(t, ctx, third.sql, map[string]string{"src": "dst"})
}

func assertMaterializedRemap(t *testing.T, ctx context.Context, sql string, want map[string]string) {
	t.Helper()
	content, ok := leadingHintContent(sql)
	require.True(t, ok, "missing materialized hint in %q", sql)
	_, remapDb, err := parsers.DecodeRewriteHint(ctx, content)
	require.NoError(t, err)
	require.Equal(t, want, remapDb)
}

func Test_HandleDeallocate(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "deallocate Prepare stmt1", 1)
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

func Test_doResetClearsPreparedBinaryState(t *testing.T) {
	ctx := context.TODO()
	ses := &Session{
		prepareStmts: make(map[string]*PrepareStmt),
	}
	proc := testutil.NewProc(t)
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("long-data"), false, proc.Mp()))
	params.GetNulls().Set(0)

	stmtName := "stmt1"
	prepareStmt := &PrepareStmt{
		Name:                stmtName,
		proc:                proc,
		params:              params,
		getFromSendLongData: map[int]struct{}{0: {}},
	}
	defer prepareStmt.Close()
	ses.prepareStmts[stmtName] = prepareStmt

	require.NoError(t, doReset(ctx, ses, tree.NewReset(tree.Identifier(stmtName))))
	require.False(t, prepareStmt.params.GetNulls().Any())
	require.Empty(t, prepareStmt.getFromSendLongData)
}

func Test_ExecRequestPrepareCommandMissingStmt(t *testing.T) {
	ctx := context.TODO()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tc := range []struct {
		name string
		cmd  CommandType
		want int64
	}{
		{name: "close", cmd: COM_STMT_CLOSE, want: 7},
		{name: "reset", cmd: COM_STMT_RESET, want: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newTestSession(t, ctrl)
			ec := newTestExecCtx(ctx, ctrl)
			stmtID := uint32(123)
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, stmtID)
			setRowCount(ses, ses.GetProc(), 7)

			resp, err := ExecRequest(ses, ec, &Request{cmd: tc.cmd, data: data})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, ErrorResponse, resp.category)
			require.Equal(t, int(tc.cmd), resp.cmd)
			require.Error(t, resp.GetData().(error))
			require.Equal(t, tc.want, ses.GetLastAffectedRows())
			require.Equal(t, tc.want, ses.GetProc().GetAffectedRows())
		})
	}
}

func Test_ExecRequestStmtExecuteErrorClearsPreparedBinaryState(t *testing.T) {
	ctx := context.TODO()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	ec := newTestExecCtx(ctx, ctrl)
	stmtID := uint32(321)
	stmtName := getPrepareStmtName(stmtID)
	st := tree.NewPrepareString(tree.Identifier(stmtName), "select ?, ?")
	stmts, err := mysql.Parse(ctx, st.Sql, 1)
	require.NoError(t, err)
	compCtx := plan.NewEmptyCompilerContext()
	preparePlan, err := buildPlan(ctx, nil, compCtx, st)
	require.NoError(t, err)

	proc := ses.GetProc()
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("leftover"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("leftover"), false, proc.Mp()))
	params.GetNulls().Set(1)

	prepareStmt := &PrepareStmt{
		Name:                stmtName,
		PreparePlan:         preparePlan,
		PrepareStmt:         stmts[0],
		proc:                proc,
		params:              params,
		getFromSendLongData: map[int]struct{}{0: {}},
	}
	defer prepareStmt.Close()
	require.NoError(t, ses.SetPrepareStmt(ctx, stmtName, prepareStmt))

	data := make([]byte, 0, 10)
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, stmtID)
	data = append(data, buf...)
	data = append(data, 0)          // flag
	data = append(data, 0, 0, 0, 0) // iteration-count
	data = append(data, 0)          // null bitmap
	data = append(data, 0)          // use existing ParamTypes, which are empty

	resp, err := ExecRequest(ses, ec, &Request{cmd: COM_STMT_EXECUTE, data: data})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, ErrorResponse, resp.category)
	require.Nil(t, prepareStmt.params)
	require.Empty(t, prepareStmt.getFromSendLongData)
}

func Test_CMD_FIELD_LIST(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("cmd field list", t, func() {

		sid := ""
		runtime.RunTest(
			sid,
			func(rt runtime.Runtime) {
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
				txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
				txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
				txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
				txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()

				txnClient := mock_frontend.NewMockTxnClient(ctrl)
				txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

				sv, err := getSystemVariables("test/system_vars_config.toml")
				if err != nil {
					t.Error(err)
				}
				pu := config.NewParameterUnit(sv, eng, txnClient, nil)
				pu.SV.SkipCheckUser = true
				setPu("", pu)
				ioses, err := NewIOSession(&testConn{}, pu, "")
				convey.So(err, convey.ShouldBeNil)
				pu.StorageEngine = eng
				pu.TxnClient = txnClient
				proto := NewMysqlClientProtocol(sid, 0, ioses, 1024, pu.SV)

				ses := NewSession(ctx, sid, proto, nil)
				proto.SetSession(ses)

				ses.mrs = &MysqlResultSet{}
				ses.SetDatabaseName("t")
				ses.seqLastValue = new(string)

				ec := newTestExecCtx(ctx, ctrl)

				err = doComQuery(ses, ec, &UserInput{sql: cmdFieldListQuery})
				convey.So(err, convey.ShouldBeNil)
			},
		)
	})
}

func Test_statement_type(t *testing.T) {
	convey.Convey("statement", t, func() {
		type kase struct {
			stmt tree.Statement
		}
		kases := []kase{
			{&tree.CreateTable{}},
			{&tree.CreateTable{IsAsSelect: true}},
			{&tree.Insert{}},
			{&tree.BeginTransaction{}},
			{&tree.ShowTables{}},
			{&tree.LockTableStmt{}},
			{&tree.UnLockTableStmt{}},
			{&tree.Use{}},
			{&tree.AnalyzeStmt{}},
			{&tree.CheckTableStmt{}},
			{&tree.ShowProfileStmt{}},
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
		convey.So(NeedToBeCommittedInActiveTransaction(&tree.LockTableStmt{}), convey.ShouldBeTrue)
		convey.So(NeedToBeCommittedInActiveTransaction(&tree.UnLockTableStmt{}), convey.ShouldBeFalse)
		convey.So(NeedToBeCommittedInActiveTransaction(&tree.DropTable{}), convey.ShouldBeFalse)
		convey.So(NeedToBeCommittedInActiveTransaction(&tree.CreateAccount{}), convey.ShouldBeTrue)
		convey.So(NeedToBeCommittedInActiveTransaction(nil), convey.ShouldBeFalse)
	})
}

func TestLockTablesSessionState(t *testing.T) {
	ses := &Session{}

	lockCtx := &ExecCtx{
		reqCtx: context.Background(),
		stmt:   &tree.LockTableStmt{},
	}
	_, err := execInFrontend(ses, lockCtx)
	require.NoError(t, err)
	require.True(t, ses.hasLockedTables.Load())
	require.False(t, lockCtx.txnOpt.byCommit)

	unlockCtx := &ExecCtx{
		reqCtx: context.Background(),
		stmt:   &tree.UnLockTableStmt{},
	}
	_, err = execInFrontend(ses, unlockCtx)
	require.NoError(t, err)
	require.True(t, unlockCtx.txnOpt.byCommit)
	require.False(t, ses.hasLockedTables.Load())

	unlockAgainCtx := &ExecCtx{
		reqCtx: context.Background(),
		stmt:   &tree.UnLockTableStmt{},
	}
	_, err = execInFrontend(ses, unlockAgainCtx)
	require.NoError(t, err)
	require.False(t, unlockAgainCtx.txnOpt.byCommit)
	require.False(t, ses.hasLockedTables.Load())
}

func TestCanExecuteDataBranchMergePickInUncommittedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, txnCase := range []struct {
		name       string
		optionBits uint32
	}{
		{name: "explicit begin", optionBits: OPTION_BEGIN},
		{name: "autocommit off", optionBits: OPTION_NOT_AUTOCOMMIT},
	} {
		for _, stmtCase := range []struct {
			name    string
			stmt    tree.Statement
			allowed bool
		}{
			{name: "merge", stmt: &tree.DataBranchMerge{}, allowed: txnCase.optionBits == OPTION_BEGIN},
			{name: "pick", stmt: &tree.DataBranchPick{}, allowed: false},
		} {
			t.Run(txnCase.name+"/"+stmtCase.name, func(t *testing.T) {
				ses := newTestSession(t, ctrl)
				ses.GetTxnHandler().SetOptionBits(txnCase.optionBits)
				can, err := statementCanBeExecutedInUncommittedTransaction(context.TODO(), ses, stmtCase.stmt)
				require.NoError(t, err)
				require.Equal(t, stmtCase.allowed, can)
				if stmtCase.allowed {
					require.NoError(t, canExecuteStatementInUncommittedTransaction(context.TODO(), ses, stmtCase.stmt))
					return
				}
				err = canExecuteStatementInUncommittedTransaction(context.TODO(), ses, stmtCase.stmt)
				require.Error(t, err)
				require.Contains(t, err.Error(), dataBranchMergePickTxnErrorInfo())
			})
		}
	}
}

func TestDataBranchMergePickTransactionFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, txnCase := range []struct {
		name            string
		optionBits      uint32
		byBegin         bool
		mergeNotAllowed bool
		pickNotAllowed  bool
	}{
		{
			name:           "ordinary autocommit",
			pickNotAllowed: false,
		},
		{
			name:           "explicit begin",
			optionBits:     OPTION_BEGIN,
			byBegin:        true,
			pickNotAllowed: true,
		},
		{
			name:            "autocommit off before active txn",
			optionBits:      OPTION_NOT_AUTOCOMMIT,
			mergeNotAllowed: true,
			pickNotAllowed:  true,
		},
		{
			name:           "explicit begin with autocommit off",
			optionBits:     OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT,
			byBegin:        true,
			pickNotAllowed: true,
		},
	} {
		t.Run(txnCase.name, func(t *testing.T) {
			ses := newTestSession(t, ctrl)
			ses.GetTxnHandler().SetOptionBits(txnCase.optionBits)
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{ByBegin: txnCase.byBegin}).AnyTimes()
			ses.proc.Base.TxnOperator = txnOperator
			require.Equal(t, txnCase.mergeNotAllowed, dataBranchMergeTxnNotAllowed(ses))
			require.Equal(t, txnCase.pickNotAllowed, dataBranchPickTxnNotAllowed(ses))
		})
	}
}

func TestUnsupportedFrontendParserStatements(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}

	run := func(stmt tree.Statement) error {
		execCtx := &ExecCtx{
			reqCtx: ctx,
			stmt:   stmt,
			ses:    ses,
		}
		defer execCtx.Close()
		_, err := execInFrontend(ses, execCtx)
		return err
	}

	err := run(&tree.CheckTableStmt{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "CHECK TABLE is not supported in MatrixOne")

	err = run(&tree.ShowProfileStmt{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "SHOW PROFILE is not supported in MatrixOne")

	err = run(&tree.AnalyzeStmt{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ANALYZE TABLE requires at least one table")
}

func TestHandleAnalyzeStmtRestoresOuterExecCtxOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	outerExecCtx := &ExecCtx{
		reqCtx: context.Background(),
		ses:    ses,
	}
	staleExecCtx := &ExecCtx{reqCtx: context.Background()}
	ses.GetTxnCompileCtx().SetExecCtx(staleExecCtx)

	err := handleAnalyzeStmt(ses, outerExecCtx, &tree.AnalyzeStmt{})
	require.Error(t, err)
	require.Same(t, outerExecCtx, ses.GetTxnCompileCtx().execCtx)
}

func TestCreatePrepareStmtRestoresCurrentExecCtx(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	staleExecCtx := &ExecCtx{reqCtx: context.Background()}
	currentExecCtx := &ExecCtx{reqCtx: context.Background(), ses: ses, proc: testutil.NewProc(t)}
	ses.GetTxnCompileCtx().SetExecCtx(staleExecCtx)

	oldBuildPlanWithAuthorization := buildPlanWithAuthorization
	defer func() {
		buildPlanWithAuthorization = oldBuildPlanWithAuthorization
	}()
	checked := false
	buildPlanWithAuthorization = func(
		ctx context.Context,
		_ FeSession,
		compilerCtx plan.CompilerContext,
		_ tree.Statement,
	) (*plan.Plan, error) {
		checked = true
		require.Same(t, currentExecCtx, compilerCtx.(*TxnCompilerContext).execCtx)
		return nil, moerr.NewInternalError(ctx, "stop after context check")
	}

	_, err := createPrepareStmt(currentExecCtx, ses, "select 1",
		tree.NewPrepareStmt("s", &tree.Select{}), &tree.Select{})
	require.Error(t, err)
	require.True(t, checked)
}

func TestBuildAnalyzeDerivedSQLQuotesIdentifiers(t *testing.T) {
	entry := &tree.AnalyzeTableEntry{
		Table: tree.NewTableName(
			tree.Identifier("tick`table"),
			tree.ObjectNamePrefix{SchemaName: tree.Identifier("select-db"), ExplicitSchema: true},
			nil,
		),
		Cols: tree.IdentifierList{"select", "a-b", "tick`name"},
	}
	require.Equal(t,
		"select approx_count_distinct(`select`),approx_count_distinct(`a-b`),approx_count_distinct(`tick``name`) from `select-db`.`tick``table`",
		buildAnalyzeDerivedSQL(entry, entry.Cols),
	)
}

func TestInheritAnalyzeRewriteHint(t *testing.T) {
	jsonHint := ` {"rewrites":{"src.t":"select * from dst.t where keep = 1"},"remapdb":{"src":"dst"}} `
	tests := []struct {
		name, outer, derived, want string
	}{
		{"merged json", "/*+" + jsonHint + "*/ analyze table src.t(a)", "select approx_count_distinct(`a`) from `dst`.`t`", "/*+" + jsonHint + "*/ select approx_count_distinct(`a`) from `dst`.`t`"},
		{"mysql json", "/*!+" + jsonHint + "*/ analyze table src.t(a)", "select 1", "/*+" + jsonHint + "*/ select 1"},
		{"optimizer hint ignored", "/*+ force_index(t) */ analyze table t(a)", "select 1", "select 1"},
		{"no hint", "analyze table t(a)", "select 1", "select 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, inheritAnalyzeRewriteHint(tt.outer, tt.derived))
		})
	}

	t.Run("merged rewrite chain is parser consumable", func(t *testing.T) {
		chainHint := ` {"rewrites":{"src.t":["select * from src.t where role_keep = 1","select * from src.t where session_keep = 1","select * from dst.t where inline_keep = 1"]},"remapdb":{"src":"dst"}} `
		derived := "select approx_count_distinct(`a`) from `dst`.`t`"
		inherited := inheritAnalyzeRewriteHint("/*+"+chainHint+"*/ analyze table src.t(a)", derived)
		require.Equal(t, "/*+"+chainHint+"*/ "+derived, inherited)
		require.Equal(t, 1, strings.Count(inherited, "/*+"))

		stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, inherited, 1)
		require.NoError(t, err)
		require.NoError(t, parsers.AddRewriteHints(context.Background(), stmts, inherited))
		require.Len(t, stmts, 1)
		sel, ok := stmts[0].(*tree.Select)
		require.True(t, ok)
		require.NotNil(t, sel.RewriteOption)
		chain := sel.RewriteOption.Rewrites["src.t"]
		require.Len(t, chain, 3)
		wantBodies := []string{
			"select * from src.t where role_keep = 1",
			"select * from src.t where session_keep = 1",
			"select * from dst.t where inline_keep = 1",
		}
		for i, rewrite := range chain {
			require.Equal(t, "src", rewrite.DbName)
			require.Equal(t, "t", rewrite.TableName)
			require.Equal(t, wantBodies[i], tree.String(rewrite.Stmt, dialect.MYSQL), "rewrite chain index %d", i)
		}
		require.Equal(t, "dst", sel.RewriteOption.RemapDb["src"])
	})
}

func TestHandleAnalyzeStmtInheritsCurrentStatementRewriteOnly(t *testing.T) {
	jsonHint := ` {"rewrites":{"db.t":"select * from db.t where inline_keep = 1"}} `
	tests := []struct {
		name, commandSQL, statementSQL, wantDerived string
	}{
		{
			name:         "second statement inline is inherited",
			commandSQL:   "select 1; /*+" + jsonHint + "*/ analyze table db.t(id)",
			statementSQL: "/*+" + jsonHint + "*/ analyze table db.t(id)",
			wantDerived:  "/*+" + jsonHint + "*/ select approx_count_distinct(`id`) from `db`.`t`",
		},
		{
			name:         "first statement inline is not inherited by later analyze",
			commandSQL:   "/*+" + jsonHint + "*/ select 1; analyze table db.t(id)",
			statementSQL: "analyze table db.t(id)",
			wantDerived:  "select approx_count_distinct(`id`) from `db`.`t`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ses, execCtx := newAnalyzeHandlerTestSession(t, ctrl)
			ses.rewriteEnabled.Store(false)
			execCtx.rewriteEnabled = true
			execCtx.input = &UserInput{
				sql:           tt.commandSQL,
				rewritePolicy: &rewritePolicySnapshot{enabled: true},
			}
			execCtx.sqlOfStmt = tt.statementSQL
			var gotDerived string
			stub := gostub.Stub(&GetComputationWrapper, func(innerExecCtx *ExecCtx, _ string, _ string, _ engine.Engine, proc *process.Process, innerSes *Session) ([]ComputationWrapper, error) {
				require.NotNil(t, innerExecCtx.input.rewritePolicy)
				require.True(t, innerExecCtx.input.rewritePolicy.enabled)
				require.True(t, innerExecCtx.input.rewritePolicyMaterialized)
				gotDerived = innerExecCtx.input.getSql()
				stmts, err := parsers.Parse(innerExecCtx.reqCtx, dialect.MYSQL, gotDerived, 1)
				require.NoError(t, err)
				results := map[string]*result{
					gotDerived: {gen: func(*Session) *MysqlResultSet {
						return makeAnalyzeCountResult("approx_count_distinct(id)", 2)
					}},
				}
				return []ComputationWrapper{newMockWrapper(ctrl, innerSes, results, nil, gotDerived, stmts[0], proc)}, nil
			})
			defer stub.Reset()

			stmt := &tree.AnalyzeStmt{Entries: []*tree.AnalyzeTableEntry{{
				Table: tree.NewTableName("t", tree.ObjectNamePrefix{SchemaName: "db", ExplicitSchema: true}, nil),
				Cols:  tree.IdentifierList{"id"},
			}}}
			require.NoError(t, handleAnalyzeStmt(ses, execCtx, stmt))
			require.Equal(t, tt.wantDerived, gotDerived)
		})
	}
}

func TestResolveAnalyzeDatabaseUsesRemappedDefault(t *testing.T) {
	tcc := &TxnCompilerContext{
		dbName: "dbxxx",
		execCtx: &ExecCtx{
			remapDb: map[string]string{"dbxxx": "dbyyy"},
		},
	}
	unqualified := tree.NewTableName("t", tree.ObjectNamePrefix{}, nil)
	require.Equal(t, "dbyyy", resolveAnalyzeDatabase(tcc, unqualified))
	qualified := tree.NewTableName("t", tree.ObjectNamePrefix{
		SchemaName: "explicit", ExplicitSchema: true,
	}, nil)
	require.Equal(t, "explicit", resolveAnalyzeDatabase(tcc, qualified))
}

type countingMysqlWriter struct {
	*testMysqlWriter
	responses []*Response
	strProps  map[PropertyID]string
	u32Props  map[PropertyID]uint32
	u8Props   map[PropertyID]uint8
	boolProps map[PropertyID]bool
}

func (w *countingMysqlWriter) GetStr(id PropertyID) string    { return w.strProps[id] }
func (w *countingMysqlWriter) GetU32(id PropertyID) uint32    { return w.u32Props[id] }
func (w *countingMysqlWriter) GetU8(id PropertyID) uint8      { return w.u8Props[id] }
func (w *countingMysqlWriter) GetBool(id PropertyID) bool     { return w.boolProps[id] }
func (w *countingMysqlWriter) SetStr(id PropertyID, v string) { w.strProps[id] = v }
func (w *countingMysqlWriter) SetU32(id PropertyID, v uint32) { w.u32Props[id] = v }
func (w *countingMysqlWriter) SetU8(id PropertyID, v uint8)   { w.u8Props[id] = v }
func (w *countingMysqlWriter) SetBool(id PropertyID, v bool)  { w.boolProps[id] = v }
func (w *countingMysqlWriter) GetCapability() uint32          { return w.u32Props[CAPABILITY] }
func (w *countingMysqlWriter) ConnectionID() uint32           { return w.u32Props[CONNID] }
func (w *countingMysqlWriter) Peer() string                   { return w.strProps[PEER] }
func (w *countingMysqlWriter) GetSequenceId() uint8           { return w.u8Props[SEQUENCEID] }
func (w *countingMysqlWriter) IsEstablished() bool            { return w.boolProps[ESTABLISHED] }
func (w *countingMysqlWriter) IsTlsEstablished() bool         { return w.boolProps[TLS_ESTABLISHED] }

func (w *countingMysqlWriter) WriteResponse(_ context.Context, resp *Response) error {
	w.responses = append(w.responses, resp)
	return nil
}

func TestExecuteAnalyzeDerivedQueryRestoresResponderOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	writer := &countingMysqlWriter{testMysqlWriter: &testMysqlWriter{}}
	live := NewMysqlResp(writer)
	ses.ReplaceResponser(live)
	outerExecCtx := &ExecCtx{reqCtx: context.Background(), ses: ses}

	_, err := executeAnalyzeDerivedQuery(ses, outerExecCtx, "select from")
	require.Error(t, err)
	require.Same(t, live, ses.GetResponser())
	require.Same(t, outerExecCtx, ses.GetTxnCompileCtx().execCtx)
	require.Zero(t, writer.responses)
}

func TestExecuteAnalyzeDerivedQueryRestoresResponderOnSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	writer := &countingMysqlWriter{testMysqlWriter: &testMysqlWriter{}}
	live := NewMysqlResp(writer)
	ses.ReplaceResponser(live)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	outerExecCtx := &ExecCtx{reqCtx: ctx, ses: ses}
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
	txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0), nil).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	ses.txnHandler.storage = eng
	ses.txnHandler.txnOp = txnOperator
	ses.txnHandler.txnCtx = outerExecCtx.reqCtx

	result, err := executeAnalyzeDerivedQuery(ses, outerExecCtx, "select 1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, uint64(1), result.GetColumnCount())
	require.Equal(t, uint64(1), result.GetRowCount())
	value, err := result.GetValue(ctx, 0, 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, value)
	require.Same(t, live, ses.GetResponser())
	require.Same(t, outerExecCtx, ses.GetTxnCompileCtx().execCtx)
	require.Zero(t, writer.responses)
}

func TestExecuteAnalyzeDerivedQueryPreservesResponderProperties(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	writer := &countingMysqlWriter{
		testMysqlWriter: &testMysqlWriter{},
		strProps:        map[PropertyID]string{USERNAME: "caller", DBNAME: "db", PEER: "192.0.2.1:6001", AuthString: "auth"},
		u32Props:        map[PropertyID]uint32{CONNID: 24659, CAPABILITY: CLIENT_MULTI_RESULTS},
		u8Props:         map[PropertyID]uint8{SEQUENCEID: 7},
		boolProps:       map[PropertyID]bool{ESTABLISHED: true, TLS_ESTABLISHED: true},
	}
	live := NewMysqlResp(writer)
	ses.ReplaceResponser(live)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	outerExecCtx := &ExecCtx{reqCtx: ctx, ses: ses}
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
	txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).DoAndReturn(func(context.CancelFunc, string) (uint64, error) {
		derived := ses.GetResponser()
		require.Equal(t, uint32(24659), derived.GetU32(CONNID))
		require.Equal(t, "192.0.2.1:6001", derived.GetStr(PEER))
		require.Equal(t, "auth", derived.GetStr(AuthString))
		require.Equal(t, uint32(CLIENT_MULTI_RESULTS), derived.GetU32(CAPABILITY))
		require.Equal(t, live.GetU8(SEQUENCEID), derived.GetU8(SEQUENCEID))
		require.Equal(t, live.GetBool(ESTABLISHED), derived.GetBool(ESTABLISHED))
		require.Equal(t, live.GetBool(TLS_ESTABLISHED), derived.GetBool(TLS_ESTABLISHED))
		proto := derived.MysqlRrWr()
		require.Equal(t, uint32(24659), proto.GetU32(CONNID))
		require.Equal(t, "192.0.2.1:6001", proto.GetStr(PEER))
		require.Equal(t, uint32(CLIENT_MULTI_RESULTS), proto.GetU32(CAPABILITY))
		protocolState := proto.(interface {
			ConnectionID() uint32
			Peer() string
			GetCapability() uint32
			GetSequenceId() uint8
			IsEstablished() bool
			IsTlsEstablished() bool
		})
		require.Equal(t, uint32(24659), protocolState.ConnectionID())
		require.Equal(t, "192.0.2.1:6001", protocolState.Peer())
		require.Equal(t, uint32(CLIENT_MULTI_RESULTS), protocolState.GetCapability())
		require.Equal(t, uint8(7), protocolState.GetSequenceId())
		require.True(t, protocolState.IsEstablished())
		require.True(t, protocolState.IsTlsEstablished())
		derived.SetStr(DBNAME, "derived")
		derived.SetU32(CONNID, 1)
		derived.SetU8(SEQUENCEID, 1)
		derived.SetBool(ESTABLISHED, false)
		return 0, nil
	}).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	ses.txnHandler.storage = eng
	ses.txnHandler.txnOp = txnOperator
	ses.txnHandler.txnCtx = outerExecCtx.reqCtx

	_, err := executeAnalyzeDerivedQuery(ses, outerExecCtx, "select 1")
	require.NoError(t, err)
	require.Equal(t, uint64(24659), ses.proc.Base.SessionInfo.ConnectionID)
	require.Same(t, live, ses.GetResponser())
	require.Equal(t, "db", writer.strProps[DBNAME])
	require.Equal(t, uint32(24659), writer.u32Props[CONNID])
	require.Equal(t, uint8(7), writer.u8Props[SEQUENCEID])
	require.True(t, writer.boolProps[ESTABLISHED])
	require.Zero(t, writer.responses)
}

func TestAnalyzeStmtUsesSituationResponse(t *testing.T) {
	kind := (&tree.AnalyzeStmt{}).StmtKind()
	require.Equal(t, tree.OUTPUT_UNDEFINED, kind.OutputType())
	require.Equal(t, tree.RESP_BY_SITUATION, kind.RespType())
	require.Equal(t, tree.EXEC_IN_FRONTEND, kind.ExecLocation())
}

func TestAnalyzeSituationResponseSendsAllResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	writer := &countingMysqlWriter{testMysqlWriter: &testMysqlWriter{}}
	resper := NewMysqlResp(writer)
	first := makeAnalyzeCountResult("approx_count_distinct(a)", 2)
	second := makeAnalyzeCountResult("approx_count_distinct(x)", 4)
	execCtx := &ExecCtx{
		reqCtx:     context.Background(),
		ses:        ses,
		isLastStmt: true,
		results:    []ExecResult{first, second},
	}
	require.NoError(t, resper.respBySituation(ses, execCtx))
	require.Len(t, writer.responses, 2)
	require.NotZero(t, writer.responses[0].GetStatus()&SERVER_MORE_RESULTS_EXISTS)
	require.Zero(t, writer.responses[1].GetStatus()&SERVER_MORE_RESULTS_EXISTS)
	require.Same(t, first, writer.responses[0].GetData().(*MysqlExecutionResult).Mrs())
	require.Same(t, second, writer.responses[1].GetData().(*MysqlExecutionResult).Mrs())
	require.Nil(t, execCtx.results)
}

func TestCallSituationResponseSendsFinalAffectedRows(t *testing.T) {
	for _, tc := range []struct {
		name             string
		isLastStmt       bool
		wantFinalMoreBit bool
	}{
		{name: "last statement", isLastStmt: true},
		{name: "followed by another statement", wantFinalMoreBit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ses := newTestSession(t, ctrl)
			writer := &countingMysqlWriter{testMysqlWriter: &testMysqlWriter{}}
			resper := NewMysqlResp(writer)
			execCtx := &ExecCtx{
				reqCtx:     context.Background(),
				ses:        ses,
				stmt:       &tree.CallStmt{},
				isLastStmt: tc.isLastStmt,
				results: []ExecResult{
					makeAnalyzeCountResult("first", 1),
					makeAnalyzeCountResult("second", 2),
				},
				runResult: &util.RunResult{AffectRows: 7},
			}

			require.NoError(t, resper.respBySituation(ses, execCtx))
			require.Len(t, writer.responses, 3)
			require.Equal(t, ResultResponse, writer.responses[0].category)
			require.Equal(t, ResultResponse, writer.responses[1].category)
			require.NotZero(t, writer.responses[0].GetStatus()&SERVER_MORE_RESULTS_EXISTS)
			require.NotZero(t, writer.responses[1].GetStatus()&SERVER_MORE_RESULTS_EXISTS)
			require.Equal(t, OkResponse, writer.responses[2].category)
			require.Equal(t, uint64(7), writer.responses[2].affectedRows)
			if tc.wantFinalMoreBit {
				require.NotZero(t, writer.responses[2].GetStatus()&SERVER_MORE_RESULTS_EXISTS)
			} else {
				require.Zero(t, writer.responses[2].GetStatus()&SERVER_MORE_RESULTS_EXISTS)
			}
		})
	}
}

func TestAnalyzeSituationResponsePreservesOuterMoreResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	writer := &countingMysqlWriter{testMysqlWriter: &testMysqlWriter{}}
	resper := NewMysqlResp(writer)
	execCtx := &ExecCtx{
		reqCtx:     context.Background(),
		ses:        ses,
		isLastStmt: false,
		results: []ExecResult{
			makeAnalyzeCountResult("approx_count_distinct(a)", 2),
			makeAnalyzeCountResult("approx_count_distinct(x)", 4),
		},
	}

	require.NoError(t, resper.respBySituation(ses, execCtx))
	require.Len(t, writer.responses, 2)
	require.NotZero(t, writer.responses[0].GetStatus()&SERVER_MORE_RESULTS_EXISTS)
	require.NotZero(t, writer.responses[1].GetStatus()&SERVER_MORE_RESULTS_EXISTS)
}

func TestSituationResponsePropagatesAffectedRows(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	writer := &countingMysqlWriter{testMysqlWriter: &testMysqlWriter{}}
	resper := NewMysqlResp(writer)
	execCtx := &ExecCtx{
		reqCtx:     context.Background(),
		ses:        ses,
		isLastStmt: true,
		runResult:  &util.RunResult{AffectRows: 7},
	}

	require.NoError(t, resper.respBySituation(ses, execCtx))
	require.Len(t, writer.responses, 1)
	require.Equal(t, uint64(7), writer.responses[0].affectedRows)
}

func TestNormalizeProcedureAffectedRows(t *testing.T) {
	require.Equal(t, uint64(0), normalizeProcedureAffectedRows(-1))
	require.Equal(t, uint64(7), normalizeProcedureAffectedRows(7))
}

func TestProcedureCallerAffectedRows(t *testing.T) {
	require.Equal(t, int64(0), procedureCallerAffectedRows(&ExecCtx{}))
	proc := &process.Process{Base: &process.BaseProcess{AffectedRows: new(int64)}}
	proc.SetAffectedRows(7)
	require.Equal(t, int64(7), procedureCallerAffectedRows(&ExecCtx{proc: proc}))
}

func TestHandleAnalyzeStmtCollectsDerivedResultsInEntryOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses, execCtx := newAnalyzeHandlerTestSession(t, ctrl)

	firstSQL := "select approx_count_distinct(`a`) from `first_table`"
	secondSQL := "select approx_count_distinct(`x`) from `second_table`"
	results := map[string]*result{
		firstSQL:  {gen: func(*Session) *MysqlResultSet { return makeAnalyzeCountResult("approx_count_distinct(a)", 2) }},
		secondSQL: {gen: func(*Session) *MysqlResultSet { return makeAnalyzeCountResult("approx_count_distinct(x)", 4) }},
	}
	var derivedSQL []string
	stub := gostub.Stub(&GetComputationWrapper, func(innerExecCtx *ExecCtx, _ string, _ string, _ engine.Engine, proc *process.Process, innerSes *Session) ([]ComputationWrapper, error) {
		sql := innerExecCtx.input.getSql()
		derivedSQL = append(derivedSQL, sql)
		stmts, err := parsers.Parse(innerExecCtx.reqCtx, dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		return []ComputationWrapper{newMockWrapper(ctrl, innerSes, results, nil, sql, stmts[0], proc)}, nil
	})
	defer stub.Reset()

	stmt := &tree.AnalyzeStmt{Entries: []*tree.AnalyzeTableEntry{
		{Table: tree.NewTableName("first_table", tree.ObjectNamePrefix{}, nil), Cols: tree.IdentifierList{"a"}},
		{Table: tree.NewTableName("second_table", tree.ObjectNamePrefix{}, nil), Cols: tree.IdentifierList{"x"}},
	}}
	require.NoError(t, handleAnalyzeStmt(ses, execCtx, stmt))
	require.Equal(t, []string{firstSQL, secondSQL}, derivedSQL)
	require.Len(t, execCtx.results, 2)
	requireAnalyzeCountValue(t, execCtx.reqCtx, execCtx.results[0], 2)
	requireAnalyzeCountValue(t, execCtx.reqCtx, execCtx.results[1], 4)
}

func TestHandleAnalyzeStmtDoesNotPublishPartialResultsOnDerivedError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses, execCtx := newAnalyzeHandlerTestSession(t, ctrl)

	firstSQL := "select approx_count_distinct(`a`) from `first_table`"
	secondSQL := "select approx_count_distinct(`x`) from `second_table`"
	results := map[string]*result{
		firstSQL: {gen: func(*Session) *MysqlResultSet { return makeAnalyzeCountResult("approx_count_distinct(a)", 2) }},
	}
	var derivedSQL []string
	stub := gostub.Stub(&GetComputationWrapper, func(innerExecCtx *ExecCtx, _ string, _ string, _ engine.Engine, proc *process.Process, innerSes *Session) ([]ComputationWrapper, error) {
		sql := innerExecCtx.input.getSql()
		derivedSQL = append(derivedSQL, sql)
		if sql == secondSQL {
			return nil, moerr.NewInternalError(innerExecCtx.reqCtx, "second derived query failed")
		}
		stmts, err := parsers.Parse(innerExecCtx.reqCtx, dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		return []ComputationWrapper{newMockWrapper(ctrl, innerSes, results, nil, sql, stmts[0], proc)}, nil
	})
	defer stub.Reset()

	stmt := &tree.AnalyzeStmt{Entries: []*tree.AnalyzeTableEntry{
		{Table: tree.NewTableName("first_table", tree.ObjectNamePrefix{}, nil), Cols: tree.IdentifierList{"a"}},
		{Table: tree.NewTableName("second_table", tree.ObjectNamePrefix{}, nil), Cols: tree.IdentifierList{"x"}},
	}}
	err := handleAnalyzeStmt(ses, execCtx, stmt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "second derived query failed")
	require.Equal(t, []string{firstSQL, secondSQL}, derivedSQL)
	require.Nil(t, execCtx.results)
}

func newAnalyzeHandlerTestSession(t *testing.T, ctrl *gomock.Controller) (*Session, *ExecCtx) {
	t.Helper()
	ses := newTestSession(t, ctrl)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses}
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
	txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	ses.txnHandler.storage = eng
	ses.txnHandler.txnOp = txnOperator
	ses.txnHandler.txnCtx = ctx
	return ses, execCtx
}

func makeAnalyzeCountResult(name string, value uint64) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	col := &MysqlColumn{}
	col.SetName(name)
	col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(col)
	mrs.AddRow([]any{value})
	return mrs
}

func requireAnalyzeCountValue(t *testing.T, ctx context.Context, result ExecResult, expected uint64) {
	t.Helper()
	mrs := result.(*MysqlResultSet)
	require.Equal(t, uint64(1), mrs.GetColumnCount())
	require.Equal(t, uint64(1), mrs.GetRowCount())
	value, err := mrs.GetValue(ctx, 0, 0)
	require.NoError(t, err)
	require.EqualValues(t, expected, value)
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
		stm := &motrace.StatementInfo{StatementID: uid, Statement: []byte(sql), RequestAt: time.Now()}
		h := NewMarshalPlanHandler(mock.CurrentContext().GetContext(), stm, plan, nil)
		json := h.Marshal(mock.CurrentContext().GetContext())
		_, stats := h.Stats(mock.CurrentContext().GetContext(), nil)
		require.Equal(t, int64(0), stats.RowsRead)
		require.Equal(t, int64(0), stats.BytesScan)
		t.Logf("SQL plan to json : %s\n", string(json))
	}
}

func TestMarshalPlanHandlerSanitizesNonFinitePlanStats(t *testing.T) {
	uid, err := uuid.NewV7()
	require.NoError(t, err)
	stmt := &motrace.StatementInfo{
		StatementID: uid,
		Statement:   []byte("select 1"),
		RequestAt:   time.Now().Add(-2 * time.Second),
	}
	logicPlan := &plan0.Plan{
		Plan: &plan0.Plan_Query{
			Query: &plan0.Query{
				Nodes: []*plan0.Node{
					{
						NodeId:   0,
						NodeType: plan0.Node_VALUE_SCAN,
						Stats: &plan0.Stats{
							Cost: math.Inf(1),
						},
					},
				},
				Steps: []int32{0},
			},
		},
	}

	h := NewMarshalPlanHandler(context.Background(), stmt, logicPlan, nil)
	jsonBytes := h.Marshal(context.Background())

	require.NotContains(t, string(jsonBytes), "serialize plan to json error")
}

func TestMarshalPlanHandlerPersistsDistributedSchedulingTraceWithoutFullPlan(t *testing.T) {
	recorder := new(schedule.TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordQuery(attempt, schedule.QueryDecision{
		ExecKind:  schedule.QueryExecAPMultiCN,
		CurrentCN: schedule.Worker{ID: "local"},
		Workers: schedule.Workers{
			{ID: "cn-a", Addr: "cn-a:6001", Mcpu: 4},
			{ID: "cn-b", Addr: "cn-b:6001", Mcpu: 8},
		},
		CandidateResolution: schedule.CandidateResolution{
			DiscoverySource: schedule.CandidateSourceEngineNodes,
			PoolResolution:  schedule.PoolResolutionLegacyEngineNodes,
			DiscoveredCount: 2,
		},
		ResolvedCandidateCount: 2,
		Reason:                 schedule.ReasonMultiCN,
		CurrentCNPolicy:        schedule.CurrentCNAllowed,
		Satisfied:              true,
	})
	traceSnapshot := recorder.Snapshot()
	h := &marshalPlanHandler{
		query: &plan0.Query{},
		marshalPlanConfig: marshalPlanConfig{
			schedulingTrace: &traceSnapshot,
		},
	}
	defer h.Free()

	jsonBytes := h.Marshal(context.Background())
	var payload struct {
		Scheduling schedule.Trace `json:"scheduling"`
	}
	require.NoError(t, json.Unmarshal(jsonBytes, &payload))
	require.Equal(t, 1, payload.Scheduling.AttemptCount)
	require.Equal(t, 2, payload.Scheduling.Attempts[0].Query.SelectedCount)
	require.True(t, payload.Scheduling.Attempts[0].Query.Selected[0].Routable)
	require.True(t, payload.Scheduling.Attempts[0].Query.Selected[1].Routable)
	require.NotContains(t, string(jsonBytes), "cn-a:6001")
	require.NotContains(t, string(jsonBytes), "cn-b:6001")
	require.NotContains(t, string(jsonBytes), "\"addr\"")
	require.NotContains(t, string(jsonBytes), "\"steps\"")
}

func TestAppendSchedulingExplainRendersPreviewAndHandlesNil(t *testing.T) {
	recorder := new(schedule.TraceRecorder)
	recorder.SetMode(schedule.TraceModePreview)
	attempt := recorder.StartAttempt()
	recorder.RecordFailure(attempt, "candidate-discovery", schedule.Worker{})
	buffer := explain.NewExplainDataBuffer()

	appendSchedulingExplain(buffer, recorder.Snapshot())
	require.Contains(t, strings.Join(buffer.Lines, "\n"), "Scheduling (preview):")
	require.Contains(t, strings.Join(buffer.Lines, "\n"), "candidate-discovery")

	appendSchedulingExplain(nil, recorder.Snapshot())
	before := len(buffer.Lines)
	appendSchedulingExplain(buffer, schedule.Trace{})
	require.Equal(t, before, len(buffer.Lines))
}

func TestExplainSchedulingEnabledUsesSessionOptIn(t *testing.T) {
	require.False(t, explainSchedulingEnabled(nil))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	require.False(t, explainSchedulingEnabled(ses))

	require.NoError(t, ses.SetSessionSysVar(context.Background(), enableExplainScheduling, int64(1)))
	require.True(t, explainSchedulingEnabled(ses))
	require.NoError(t, ses.SetSessionSysVar(context.Background(), enableExplainScheduling, int64(0)))
	require.False(t, explainSchedulingEnabled(ses))
}

func TestQuerySchedulingIntentUsesSessionAndSetVarCapableVariables(t *testing.T) {
	require.Equal(t, schedule.SchedulingIntent{
		PoolFallback:      schedule.PoolFallbackLegacyCompatible,
		EmptyWorkerPolicy: schedule.EmptyWorkerLocalFallback,
		CurrentCNPolicy:   schedule.CurrentCNAllowed,
		WorkerSet:         schedule.WorkerSetPolicy{Mode: schedule.WorkerSetAll},
	}, querySchedulingIntent(nil))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	require.NoError(t, ses.SetSessionSysVar(context.Background(), queryMaxWorkers, int64(3)))
	require.NoError(t, ses.SetSessionSysVar(context.Background(), queryPoolStrict, int64(1)))

	intent := querySchedulingIntent(ses)
	require.True(t, intent.Explicit)
	require.Equal(t, schedule.PoolFallbackStrict, intent.PoolFallback)
	require.Equal(t, schedule.EmptyWorkerFail, intent.EmptyWorkerPolicy)
	require.Equal(t, schedule.WorkerSetMax, intent.WorkerSet.Mode)
	require.Equal(t, 3, intent.WorkerSet.MaxWorkers)
	require.True(t, gSysVarsDefs[queryMaxWorkers].SetVarHintApplies)
	require.True(t, gSysVarsDefs[queryPoolStrict].SetVarHintApplies)
	maxWorkersType := gSysVarsDefs[queryMaxWorkers].Type.(SystemVariableIntType)
	require.Equal(t, int64(2147483647), maxWorkersType.maximum)
}

func TestQuerySchedulingIntentAppliesStatementSetVarOverrides(t *testing.T) {
	intent := querySchedulingIntentForStatement(nil,
		"select /*+ SET_VAR(query_max_workers=2) SET_VAR(query_pool_strict='ON') */ 1")
	require.True(t, intent.Explicit)
	require.Equal(t, schedule.WorkerSetMax, intent.WorkerSet.Mode)
	require.Equal(t, 2, intent.WorkerSet.MaxWorkers)
	require.Equal(t, schedule.PoolFallbackStrict, intent.PoolFallback)
	require.Equal(t, schedule.EmptyWorkerFail, intent.EmptyWorkerPolicy)

	// Optimizer hints use first-wins semantics for duplicate variables.
	intent = querySchedulingIntentForStatement(nil,
		"select /*+ SET_VAR(query_max_workers=1) SET_VAR(query_max_workers=2) */ 1")
	require.Equal(t, 1, intent.WorkerSet.MaxWorkers)

	// Hint-looking text outside optimizer-hint comments must not affect intent.
	intent = querySchedulingIntentForStatement(nil,
		"select '/*+ SET_VAR(query_max_workers=3) */' /* SET_VAR(query_pool_strict=on) */")
	require.False(t, intent.Explicit)
	require.Equal(t, schedule.WorkerSetAll, intent.WorkerSet.Mode)

	// SET_VAR-looking text nested in another hint or its quoted arguments is
	// not a top-level optimizer hint and must not become scheduling policy.
	intent = querySchedulingIntentForStatement(nil,
		"select /*+ QB_NAME('SET_VAR(query_max_workers=3)') OTHER(SET_VAR(query_pool_strict=on)) */ 1")
	require.False(t, intent.Explicit)
	require.Equal(t, schedule.WorkerSetAll, intent.WorkerSet.Mode)
}

func TestQuerySchedulingIntentScannerRespectsNoBackslashEscapes(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "sql_mode", "NO_BACKSLASH_ESCAPES"))

	// With NO_BACKSLASH_ESCAPES, the quote after the backslash closes the
	// string, so the following optimizer comment is in SQL lexical space.
	intent := querySchedulingIntentForStatement(ses,
		`select 'value\' /*+ SET_VAR(query_max_workers=3) */`)
	require.True(t, intent.Explicit)
	require.Equal(t, schedule.WorkerSetMax, intent.WorkerSet.Mode)
	require.Equal(t, 3, intent.WorkerSet.MaxWorkers)
}

func TestQuerySchedulingIntentRejectsInvalidStatementSetVar(t *testing.T) {
	for _, sql := range []string{
		"select /*+ SET_VAR(query_max_workers=-1) */ 1",
		"select /*+ SET_VAR(query_max_workers=2147483648) */ 1",
		"select /*+ SET_VAR(query_pool_strict=maybe) */ 1",
		"select /*+ SET_VAR(query_max_workers) */ 1",
	} {
		intent := querySchedulingIntentForStatement(nil, sql)
		require.True(t, intent.Explicit, sql)
		require.False(t, intent.PoolFallback.Valid(), sql)
	}
}

func TestWithSchedulingTraceTakesIndependentOwnership(t *testing.T) {
	recorder := new(schedule.TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordFailure(attempt, "candidate-discovery", schedule.Worker{})
	trace := recorder.Snapshot()
	config := marshalPlanConfig{}

	WithSchedulingTrace(trace)(&config)
	trace.Attempts[0].Failures[0].Category = "changed"

	require.NotNil(t, config.schedulingTrace)
	require.Equal(t, "candidate-discovery", config.schedulingTrace.Attempts[0].Failures[0].Category)
}

type successfulSchedulingPreviewEngine struct {
	engine.Engine
}

func (*successfulSchedulingPreviewEngine) DiscoverQueryCandidates(context.Context) (engine.QueryCandidates, error) {
	return engine.QueryCandidates{{
		Service: metadata.CNService{
			ServiceID:              "preview-cn",
			PipelineServiceAddress: "preview-cn:6001",
		},
		Mcpu: 1,
	}}, nil
}

func (*successfulSchedulingPreviewEngine) ResolveQueryCandidatePool(
	_ context.Context,
	_ engine.QueryCandidates,
	_ engine.QueryCandidatePoolRequest,
) (engine.ResolvedQueryPool, error) {
	return engine.ResolvedQueryPool{
		Nodes:             engine.Nodes{{Id: "preview-cn", Addr: "preview-cn:6001", Mcpu: 1}},
		RequestedIdentity: "preview",
		Identity:          "preview",
		Resolution:        engine.QueryPoolResolutionAllCompatible,
	}, nil
}

type strictNoSelectorSchedulingPreviewEngine struct {
	*disttae.Engine
	candidates engine.QueryCandidates
}

func (e *strictNoSelectorSchedulingPreviewEngine) DiscoverQueryCandidates(
	ctx context.Context,
) (engine.QueryCandidates, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return e.candidates, nil
}

func TestDirectSessionStrictPoolWithoutLabelSelectorFailsClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.SetTenantInfo(&TenantInfo{Tenant: "tenant-a", User: "user-a"})
	ses.txnHandler.storage = &strictNoSelectorSchedulingPreviewEngine{
		Engine: new(disttae.Engine),
		candidates: engine.QueryCandidates{
			{Service: metadata.CNService{
				ServiceID: "tenant-a", PipelineServiceAddress: "tenant-a:6001",
				Labels: map[string]metadata.LabelList{
					"account": {Labels: []string{"tenant-a"}},
				},
				WorkState: metadata.WorkState_Working,
			}, Mcpu: 4},
			{Service: metadata.CNService{
				ServiceID: "tenant-b", PipelineServiceAddress: "tenant-b:6001",
				Labels: map[string]metadata.LabelList{
					"account": {Labels: []string{"tenant-b"}},
				},
				WorkState: metadata.WorkState_Working,
			}, Mcpu: 4},
		},
	}
	require.Empty(t, ses.getCNLabels())
	require.NoError(t, ses.SetSessionSysVar(context.Background(), queryPoolStrict, int64(1)))

	trace := previewQueryScheduling(
		context.Background(),
		ses,
		&plan0.Query{Nodes: []*plan0.Node{{NodeType: plan0.Node_TABLE_SCAN}}},
		false,
	)

	require.Len(t, trace.Attempts, 1)
	require.NotNil(t, trace.Attempts[0].Query)
	require.Equal(t, schedule.ReasonNoCandidateCN, trace.Attempts[0].Query.Reason)
	require.False(t, trace.Attempts[0].Query.Satisfied)
	require.Equal(t, "strict", trace.Attempts[0].Query.PoolFallbackPolicy)
	require.Equal(t, string(engine.QueryPoolResolutionNoMatch), trace.Attempts[0].Query.ResolvedPoolResolution)
	require.Equal(t, "strict-missing-label-selector", trace.Attempts[0].Query.PoolFallbackReason)
	require.Equal(t, 2, trace.Attempts[0].Query.DiscoveredCount)
	require.Zero(t, trace.Attempts[0].Query.ResolvedCount)
}

func TestDoExplainStmtIncludesSchedulingPreviewWithoutFailingDiscovery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.txnHandler.storage = &successfulSchedulingPreviewEngine{}
	oldBuildPlanWithAuthorization := buildPlanWithAuthorization
	defer func() {
		buildPlanWithAuthorization = oldBuildPlanWithAuthorization
	}()
	buildPlanWithAuthorization = func(
		context.Context,
		FeSession,
		plan.CompilerContext,
		tree.Statement,
	) (*plan.Plan, error) {
		return &plan.Plan{
			Plan: &plan0.Plan_Query{
				Query: &plan0.Query{
					Nodes: []*plan0.Node{{NodeId: 0, NodeType: plan0.Node_VALUE_SCAN}},
					Steps: []int32{0},
				},
			},
		}, nil
	}

	require.NoError(t, ses.SetSessionSysVar(context.Background(), enableExplainScheduling, int64(1)))
	stmt := tree.NewExplainStmt(&tree.Select{}, "text")
	err := doExplainStmt(
		context.Background(),
		ses,
		stmt,
		"explain select /*+ SET_VAR(query_max_workers=1) */ 1",
	)
	require.NoError(t, err)

	var output strings.Builder
	for i := uint64(0); i < ses.GetMysqlResultSet().GetRowCount(); i++ {
		row, rowErr := ses.GetMysqlResultSet().GetRow(context.Background(), i)
		require.NoError(t, rowErr)
		require.Len(t, row, 1)
		output.WriteString(row[0].(string))
		output.WriteByte('\n')
	}
	require.Contains(t, output.String(), "Scheduling (preview):")
	require.Contains(t, output.String(), "Intent: explicit=true")
	require.Contains(t, output.String(), "worker-set=max-workers max-workers=1")
}

func TestDoExplainExecuteUsesPreparedSchedulingSQL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.txnHandler.storage = &successfulSchedulingPreviewEngine{}
	oldBuildPlanWithAuthorization := buildPlanWithAuthorization
	defer func() {
		buildPlanWithAuthorization = oldBuildPlanWithAuthorization
	}()
	buildPlanWithAuthorization = func(
		context.Context,
		FeSession,
		plan.CompilerContext,
		tree.Statement,
	) (*plan.Plan, error) {
		return &plan.Plan{
			Plan: &plan0.Plan_Query{
				Query: &plan0.Query{
					Nodes: []*plan0.Node{{NodeId: 0, NodeType: plan0.Node_VALUE_SCAN}},
					Steps: []int32{0},
				},
			},
		}, nil
	}

	ctx := context.Background()
	require.NoError(t, ses.SetSessionSysVar(ctx, enableExplainScheduling, int64(1)))
	require.NoError(t, ses.SetPrepareStmt(ctx, "sched", &PrepareStmt{
		Name:              "sched",
		Sql:               `select 'value\' /*+ SET_VAR(query_max_workers=2) */`,
		schedulingSQLMode: "NO_BACKSLASH_ESCAPES",
	}))
	err := doExplainStmt(
		ctx,
		ses,
		tree.NewExplainStmt(tree.NewExecute(tree.Identifier("sched")), "text"),
		"explain /*+ SET_VAR(query_max_workers=1) */ execute sched",
	)
	require.NoError(t, err)

	var output strings.Builder
	for i := uint64(0); i < ses.GetMysqlResultSet().GetRowCount(); i++ {
		row, rowErr := ses.GetMysqlResultSet().GetRow(ctx, i)
		require.NoError(t, rowErr)
		output.WriteString(row[0].(string))
		output.WriteByte('\n')
	}
	require.Contains(t, output.String(), "worker-set=max-workers max-workers=2")
	require.NotContains(t, output.String(), "worker-set=max-workers max-workers=1")
}

func TestDoExplainStmtKeepsSchedulingPreviewOptIn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	oldBuildPlanWithAuthorization := buildPlanWithAuthorization
	defer func() {
		buildPlanWithAuthorization = oldBuildPlanWithAuthorization
	}()
	buildPlanWithAuthorization = func(
		context.Context,
		FeSession,
		plan.CompilerContext,
		tree.Statement,
	) (*plan.Plan, error) {
		return &plan.Plan{
			Plan: &plan0.Plan_Query{
				Query: &plan0.Query{
					Nodes: []*plan0.Node{{NodeId: 0, NodeType: plan0.Node_VALUE_SCAN}},
					Steps: []int32{0},
				},
			},
		}, nil
	}

	err := doExplainStmt(
		context.Background(),
		ses,
		tree.NewExplainStmt(&tree.Select{}, "text"),
	)
	require.NoError(t, err)

	var output strings.Builder
	for i := uint64(0); i < ses.GetMysqlResultSet().GetRowCount(); i++ {
		row, rowErr := ses.GetMysqlResultSet().GetRow(context.Background(), i)
		require.NoError(t, rowErr)
		require.Len(t, row, 1)
		output.WriteString(row[0].(string))
		output.WriteByte('\n')
	}
	require.NotContains(t, output.String(), "Scheduling (")
}

func TestDoExplainStmtDoesNotSwallowRequestCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	oldBuildPlanWithAuthorization := buildPlanWithAuthorization
	defer func() {
		buildPlanWithAuthorization = oldBuildPlanWithAuthorization
	}()
	buildPlanWithAuthorization = func(
		context.Context,
		FeSession,
		plan.CompilerContext,
		tree.Statement,
	) (*plan.Plan, error) {
		return &plan.Plan{
			Plan: &plan0.Plan_Query{
				Query: &plan0.Query{
					Nodes: []*plan0.Node{{NodeId: 0, NodeType: plan0.Node_VALUE_SCAN}},
					Steps: []int32{0},
				},
			},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := doExplainStmt(ctx, ses, tree.NewExplainStmt(&tree.Select{}, "text"))
	require.ErrorIs(t, err, context.Canceled)
}

type blockingSchedulingPreviewEngine struct {
	engine.Engine
}

func (*blockingSchedulingPreviewEngine) DiscoverQueryCandidates(
	ctx context.Context,
) (engine.QueryCandidates, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*blockingSchedulingPreviewEngine) ResolveQueryCandidatePool(
	context.Context,
	engine.QueryCandidates,
	engine.QueryCandidatePoolRequest,
) (engine.ResolvedQueryPool, error) {
	return engine.ResolvedQueryPool{}, moerr.NewInternalErrorNoCtx("pool resolution should not run")
}

func TestSchedulingPreviewHasIndependentTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.txnHandler.storage = &blockingSchedulingPreviewEngine{}

	started := time.Now()
	trace := previewQueryScheduling(
		context.Background(),
		ses,
		&plan0.Query{Nodes: []*plan0.Node{{NodeType: plan0.Node_TABLE_SCAN}}},
		false,
	)

	require.Less(t, time.Since(started), time.Second)
	require.Equal(t, schedule.TraceModePreview, trace.Mode)
	require.Equal(t, "candidate-discovery", trace.Attempts[0].Failures[0].Category)
}

type blockingPoolResolutionPreviewEngine struct {
	engine.Engine
}

func (*blockingPoolResolutionPreviewEngine) DiscoverQueryCandidates(
	context.Context,
) (engine.QueryCandidates, error) {
	return nil, nil
}

func (*blockingPoolResolutionPreviewEngine) ResolveQueryCandidatePool(
	ctx context.Context,
	_ engine.QueryCandidates,
	_ engine.QueryCandidatePoolRequest,
) (engine.ResolvedQueryPool, error) {
	<-ctx.Done()
	return engine.ResolvedQueryPool{}, ctx.Err()
}

func TestSchedulingPreviewTimeoutBoundsPoolResolution(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.txnHandler.storage = &blockingPoolResolutionPreviewEngine{}

	started := time.Now()
	trace := previewQueryScheduling(
		context.Background(),
		ses,
		&plan0.Query{Nodes: []*plan0.Node{{NodeType: plan0.Node_TABLE_SCAN}}},
		false,
	)

	require.Less(t, time.Since(started), time.Second)
	require.Equal(t, schedule.TraceModePreview, trace.Mode)
	require.Equal(t, "pool-resolution", trace.Attempts[0].Failures[0].Category)
}

type blockingLegacySchedulingPreviewEngine struct {
	engine.Engine
	called  chan struct{}
	release chan struct{}
}

func (e *blockingLegacySchedulingPreviewEngine) Nodes(
	bool,
	string,
	string,
	map[string]string,
) (engine.Nodes, error) {
	close(e.called)
	<-e.release
	return nil, nil
}

func TestSchedulingPreviewDoesNotCallBlockingLegacyEngine(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	legacy := &blockingLegacySchedulingPreviewEngine{
		called:  make(chan struct{}),
		release: make(chan struct{}),
	}
	ses.txnHandler.storage = legacy

	done := make(chan schedule.Trace, 1)
	go func() {
		done <- previewQueryScheduling(
			context.Background(),
			ses,
			&plan0.Query{Nodes: []*plan0.Node{{NodeType: plan0.Node_TABLE_SCAN}}},
			false,
		)
	}()

	var trace schedule.Trace
	select {
	case trace = <-done:
		close(legacy.release)
	case <-time.After(time.Second):
		close(legacy.release)
		<-done
		t.Fatal("scheduling preview called blocking legacy Engine.Nodes")
	}
	select {
	case <-legacy.called:
		t.Fatal("scheduling preview must not call legacy Engine.Nodes")
	default:
	}
	require.Equal(t, schedule.TraceModePreview, trace.Mode)
	require.Equal(t, "candidate-provider", trace.Attempts[0].Failures[0].Category)
}

func TestSchedulingTraceFromComputationWrapperReturnsIndependentSnapshot(t *testing.T) {
	cw := &TxnComputationWrapper{}
	attempt := cw.schedulingTrace.StartAttempt()
	cw.schedulingTrace.RecordFailure(attempt, "failure", schedule.Worker{})

	trace := schedulingTraceFromComputationWrapper(cw)
	require.Equal(t, "failure", trace.Attempts[0].Failures[0].Category)
	trace.Attempts[0].Failures[0].Category = "changed"
	require.Equal(t, "failure", cw.SchedulingTrace().Attempts[0].Failures[0].Category)
}

func TestSchedulingTraceForExplainUsesSessionOptIn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	cw := &TxnComputationWrapper{}
	attempt := cw.schedulingTrace.StartAttempt()
	cw.schedulingTrace.RecordFailure(attempt, "failure", schedule.Worker{})

	require.True(t, schedulingTraceForExplain(ses, cw).Empty())
	require.NoError(t, ses.SetSessionSysVar(context.Background(), enableExplainScheduling, int64(1)))
	require.Equal(t, "failure", schedulingTraceForExplain(ses, cw).Attempts[0].Failures[0].Category)
}

func TestBuildMoExplainPhyPlanAppendsActualSchedulingTrace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	recorder := new(schedule.TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordFailure(attempt, "runtime-ineligible-selected-worker", schedule.Worker{ID: "cn"})
	var rows []string
	fill := func(_ FeSession, _ *ExecCtx, bat *batch.Batch, _ *perfcounter.CounterSet) error {
		if bat == nil {
			return nil
		}
		for i := 0; i < bat.RowCount(); i++ {
			rows = append(rows, bat.Vecs[0].GetStringAt(i))
		}
		return nil
	}

	err := buildMoExplainPhyPlan(
		&ExecCtx{reqCtx: context.Background()},
		"QUERY PLAN",
		bufio.NewReader(strings.NewReader("Physical plan")),
		ses,
		fill,
		recorder.Snapshot(),
	)
	require.NoError(t, err)
	require.Equal(t, "Physical plan", rows[0])
	output := strings.Join(rows, "\n")
	require.Contains(t, output, "Scheduling (execution):")
	require.Contains(t, output, "runtime-ineligible-selected-worker")
}

func TestMarshalPlanHandlerDoesNotAmplifyShortLocalSchedulingTrace(t *testing.T) {
	recorder := new(schedule.TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordQuery(attempt, schedule.QueryDecision{
		ExecKind:  schedule.QueryExecTP,
		CurrentCN: schedule.Worker{ID: "local"},
		Workers:   schedule.Workers{{ID: "local"}},
		Reason:    schedule.ReasonLocalExecType,
		CandidateResolution: schedule.CandidateResolution{
			DiscoverySource: schedule.CandidateSourceNotRequired,
			PoolResolution:  schedule.PoolResolutionNotRequired,
		},
		CurrentCNPolicy: schedule.CurrentCNAllowed,
		Satisfied:       true,
	})
	traceSnapshot := recorder.Snapshot()
	h := &marshalPlanHandler{
		query: &plan0.Query{},
		marshalPlanConfig: marshalPlanConfig{
			schedulingTrace: &traceSnapshot,
		},
	}
	defer h.Free()

	require.Equal(t, sqlQueryIgnoreExecPlan, h.Marshal(context.Background()))
}

func TestMarshalPlanHandlerRequestsSchedulingSnapshotOnlyWhenNeeded(t *testing.T) {
	recorder := new(schedule.TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordQuery(attempt, schedule.QueryDecision{
		ExecKind:        schedule.QueryExecTP,
		CurrentCN:       schedule.Worker{ID: "local"},
		Workers:         schedule.Workers{{ID: "local"}},
		Reason:          schedule.ReasonLocalExecType,
		CurrentCNPolicy: schedule.CurrentCNAllowed,
		Satisfied:       true,
	})
	logicPlan := &plan0.Plan{
		Plan: &plan0.Plan_Query{Query: &plan0.Query{
			Nodes: []*plan0.Node{{NodeId: 0, NodeType: plan0.Node_VALUE_SCAN}},
			Steps: []int32{0},
		}},
	}

	shortStmt := &motrace.StatementInfo{RequestAt: time.Now()}
	shortHandler := NewMarshalPlanHandler(
		context.Background(), shortStmt, logicPlan, nil,
		WithWaitActiveCost(time.Hour),
		withSchedulingTraceRecorder(recorder),
	)
	defer shortHandler.Free()
	require.Nil(t, shortHandler.schedulingTrace)
	require.False(t, shortHandler.persistSchedulingTrace)
	require.Equal(t, sqlQueryIgnoreExecPlan, shortHandler.Marshal(context.Background()))

	longStmt := &motrace.StatementInfo{RequestAt: time.Now().Add(-motrace.GetLongQueryTime() - time.Second)}
	longHandler := NewMarshalPlanHandler(
		context.Background(), longStmt, logicPlan, nil,
		withSchedulingTraceRecorder(recorder),
	)
	defer longHandler.Free()
	require.NotNil(t, longHandler.schedulingTrace)
	require.False(t, longHandler.persistSchedulingTrace)
	require.Equal(t, schedule.QueryExecTP.String(), longHandler.schedulingTrace.Attempts[0].Query.ExecKind)
}

func TestMarshalPlanHandlerRequestsStandaloneTraceWithoutQueryPlan(t *testing.T) {
	recorder := new(schedule.TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordFailure(attempt, "candidate-discovery", schedule.Worker{})
	stmt := &motrace.StatementInfo{RequestAt: time.Now()}

	h := NewMarshalPlanHandler(
		context.Background(), stmt, nil, nil,
		withSchedulingTraceRecorder(recorder),
	)
	defer h.Free()
	require.True(t, h.persistSchedulingTrace)
	require.NotNil(t, h.schedulingTrace)
	var payload struct {
		Scheduling schedule.Trace `json:"scheduling"`
	}
	require.NoError(t, json.Unmarshal(h.Marshal(context.Background()), &payload))
	require.Equal(t, "candidate-discovery", payload.Scheduling.Attempts[0].Failures[0].Category)
}

func TestMarshalPlanHandlerIncludesLocalSchedulingTraceWithFullPlan(t *testing.T) {
	recorder := new(schedule.TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordQuery(attempt, schedule.QueryDecision{
		ExecKind:  schedule.QueryExecTP,
		CurrentCN: schedule.Worker{ID: "local"},
		Workers:   schedule.Workers{{ID: "local"}},
		Reason:    schedule.ReasonLocalExecType,
		CandidateResolution: schedule.CandidateResolution{
			DiscoverySource: schedule.CandidateSourceNotRequired,
			PoolResolution:  schedule.PoolResolutionNotRequired,
		},
		CurrentCNPolicy: schedule.CurrentCNAllowed,
		Satisfied:       true,
	})
	stmt := &motrace.StatementInfo{
		Statement: []byte("select 1"),
		RequestAt: time.Now().Add(-motrace.GetLongQueryTime() - time.Second),
	}
	logicPlan := &plan0.Plan{
		Plan: &plan0.Plan_Query{
			Query: &plan0.Query{
				Nodes: []*plan0.Node{{NodeId: 0, NodeType: plan0.Node_VALUE_SCAN}},
				Steps: []int32{0},
			},
		},
	}
	h := NewMarshalPlanHandler(
		context.Background(),
		stmt,
		logicPlan,
		nil,
		WithSchedulingTrace(recorder.Snapshot()),
	)
	defer h.Free()

	jsonBytes := h.Marshal(context.Background())
	var payload struct {
		Scheduling schedule.Trace `json:"scheduling"`
		Steps      []any          `json:"steps"`
	}
	require.NoError(t, json.Unmarshal(jsonBytes, &payload))
	require.Equal(t, 1, payload.Scheduling.AttemptCount)
	require.NotNil(t, payload.Steps)
}

func TestMarshalPlanHandlerPersistsFailureTraceWithoutQueryPlan(t *testing.T) {
	recorder := new(schedule.TraceRecorder)
	attempt := recorder.StartAttempt()
	recorder.RecordFailure(attempt, "candidate-discovery", schedule.Worker{})
	stmt := &motrace.StatementInfo{RequestAt: time.Now()}
	h := NewMarshalPlanHandler(
		context.Background(),
		stmt,
		nil,
		nil,
		WithSchedulingTrace(recorder.Snapshot()),
	)
	defer h.Free()

	jsonBytes := h.Marshal(context.Background())
	var payload struct {
		Scheduling schedule.Trace `json:"scheduling"`
	}
	require.NoError(t, json.Unmarshal(jsonBytes, &payload))
	require.Equal(t, "candidate-discovery", payload.Scheduling.Attempts[0].Failures[0].Category)
}

func buildSingleSql(opt plan.Optimizer, t *testing.T, sql string) (*plan.Plan, error) {
	stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
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
		proc := testutil.NewProc(t)
		var writer *io.PipeWriter
		proc.Base.LoadLocalReader, writer = io.Pipe()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		tConn := &testConn{}
		pkts := []*Packet{{Length: 5, Payload: []byte("hello"), SequenceID: 1},
			{Length: 5, Payload: []byte("world"), SequenceID: 2},
			{Length: 0, Payload: []byte(""), SequenceID: 3}}
		writeExceptResult(tConn, pkts)
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(tConn, pu, "")
		convey.So(err, convey.ShouldBeNil)
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
				n, err := proc.Base.LoadLocalReader.Read(tmp)
				if err != nil {
					break
				}
				tmp = tmp[n:]
			}
		}(buffer)
		ec := newTestExecCtx(context.Background(), ctrl)
		err = processLoadLocal(ses, ec, param, writer, proc.GetLoadLocalReader())
		convey.So(err, convey.ShouldBeNil)
		convey.So(buffer[:10], convey.ShouldResemble, []byte("helloworld"))
		convey.So(buffer[10:], convey.ShouldResemble, make([]byte, 4096-10))
	})
}

func TestProcessLoadLocalCheckLockTableBindsErrorBeforeRead(t *testing.T) {
	convey.Convey("processLoadLocal returns lock table bind check error before first read", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				Filepath: "test.csv",
			},
		}
		proc := testutil.NewProc(t)
		var writer *io.PipeWriter
		proc.Base.LoadLocalReader, writer = io.Pipe()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		tConn := &testConn{}
		writeExceptResult(tConn, []*Packet{{Length: 5, Payload: []byte("hello"), SequenceID: 1}})
		sv, err := getSystemVariables("test/system_vars_config.toml")
		convey.So(err, convey.ShouldBeNil)
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(tConn, pu, "")
		convey.So(err, convey.ShouldBeNil)
		ses := &Session{
			feSessionImpl: feSessionImpl{
				respr: NewMysqlResp(&testMysqlWriter{ioses: ioses}),
			},
		}
		ctx := context.Background()
		ec := newTestExecCtx(ctx, ctrl)
		op := newTestTxnOp()
		expected := moerr.NewLockTableBindChangedNoCtx()
		op.checkLockTableBinds = func(context.Context) error {
			return expected
		}
		proc.Base.TxnOperator = op
		proc.Ctx = ctx
		ec.proc = proc

		err = processLoadLocal(ses, ec, param, writer, proc.GetLoadLocalReader())
		convey.So(err, convey.ShouldEqual, expected)
		convey.So(op.checkLockTableChecks, convey.ShouldEqual, 1)
	})
}

func TestProcessLoadLocalCheckLockTableBindsErrorInLoop(t *testing.T) {
	convey.Convey("processLoadLocal returns lock table bind check error after one packet", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				Filepath: "test.csv",
			},
		}
		proc := testutil.NewProc(t)
		var writer *io.PipeWriter
		proc.Base.LoadLocalReader, writer = io.Pipe()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		tConn := &testConn{}
		pkts := []*Packet{{Length: 5, Payload: []byte("hello"), SequenceID: 1},
			{Length: 5, Payload: []byte("world"), SequenceID: 2},
			{Length: 0, Payload: []byte(""), SequenceID: 3}}
		writeExceptResult(tConn, pkts)
		sv, err := getSystemVariables("test/system_vars_config.toml")
		convey.So(err, convey.ShouldBeNil)
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(tConn, pu, "")
		convey.So(err, convey.ShouldBeNil)
		ses := &Session{
			feSessionImpl: feSessionImpl{
				respr: NewMysqlResp(&testMysqlWriter{ioses: ioses}),
			},
		}
		buffer := make([]byte, 4096)
		go func(buf []byte) {
			tmp := buf
			for {
				n, err := proc.Base.LoadLocalReader.Read(tmp)
				if err != nil {
					break
				}
				tmp = tmp[n:]
			}
		}(buffer)
		ctx := context.Background()
		ec := newTestExecCtx(ctx, ctrl)
		op := newTestTxnOp()
		expected := moerr.NewLockTableBindChangedNoCtx()
		op.checkLockTableBinds = func(context.Context) error {
			if op.checkLockTableChecks == 2 {
				return expected
			}
			return nil
		}
		proc.Base.TxnOperator = op
		proc.Ctx = ctx
		ec.proc = proc

		err = processLoadLocal(ses, ec, param, writer, proc.GetLoadLocalReader())
		convey.So(err, convey.ShouldEqual, expected)
		convey.So(op.checkLockTableChecks, convey.ShouldEqual, 2)
		convey.So(buffer[:5], convey.ShouldResemble, []byte("hello"))
	})
}

// networkTimeoutError implements net.Error interface for testing network timeout
type networkTimeoutError struct {
	msg string
}

func (e *networkTimeoutError) Error() string   { return e.msg }
func (e *networkTimeoutError) Timeout() bool   { return true }
func (e *networkTimeoutError) Temporary() bool { return true }

// timeoutTestConn is a test connection that returns timeout error on read
type timeoutTestConn struct {
	testConn
	returnTimeout bool
}

func (tc *timeoutTestConn) Read(b []byte) (n int, err error) {
	if tc.returnTimeout {
		return 0, &networkTimeoutError{msg: "read timeout"}
	}
	return tc.testConn.Read(b)
}

func TestProcessLoadLocal_NetworkTimeout(t *testing.T) {
	convey.Convey("processLoadLocal handles network timeout", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				Filepath: "test.csv",
			},
		}
		proc := testutil.NewProc(t)
		var writer *io.PipeWriter
		proc.Base.LoadLocalReader, writer = io.Pipe()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Create a timeout connection that will return timeout error
		tConn := &timeoutTestConn{returnTimeout: true}

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setSessionAlloc("", NewLeakCheckAllocator())
		setPu("", pu)
		ioses, err := NewIOSession(tConn, pu, "")
		convey.So(err, convey.ShouldBeNil)
		proto := &testMysqlWriter{
			ioses: ioses,
		}

		ses := &Session{
			feSessionImpl: feSessionImpl{
				respr: NewMysqlResp(proto),
			},
		}

		// Read in background to avoid pipe block
		go func() {
			buf := make([]byte, 4096)
			for {
				_, err := proc.Base.LoadLocalReader.Read(buf)
				if err != nil {
					break
				}
			}
		}()

		ec := newTestExecCtx(context.Background(), ctrl)
		err = processLoadLocal(ses, ec, param, writer, proc.GetLoadLocalReader())

		// Should return error containing "network read timeout"
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "network read timeout")
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
		{&tree.AnalyzeStmt{}, true},
		{&tree.CheckTableStmt{}, true},
		{&tree.ShowProfileStmt{}, true},
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
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			ctx := context.TODO()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			eng.EXPECT().Database(ctx, gomock.Any(), nil).Return(nil, nil).AnyTimes()
			txnClient := mock_frontend.NewMockTxnClient(ctrl)

			sv, err := getSystemVariables("test/system_vars_config.toml")
			if err != nil {
				t.Error(err)
			}
			pu := config.NewParameterUnit(sv, eng, txnClient, nil)
			pu.SV.SkipCheckUser = true
			setPu("", pu)
			ioses, err := NewIOSession(&testConn{}, pu, "")
			if err != nil {
				t.Error(err)
			}
			proto := NewMysqlClientProtocol(sid, 0, ioses, 1024, pu.SV)

			ses := NewSession(ctx, sid, proto, nil)
			proto.SetSession(ses)
			//ses.proto = proto

			convey.Convey("no labels", t, func() {
				ses.mrs = &MysqlResultSet{}
				cluster := clusterservice.NewMOCluster(
					sid,
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
				runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.ClusterService, cluster)
				defer cluster.Close()
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
					sid,
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
				runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.ClusterService, cluster)
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

			convey.Convey("filter label sys account", t, func() {
				ses.mrs = &MysqlResultSet{}
				cluster := clusterservice.NewMOCluster(
					sid,
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
				runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.ClusterService, cluster)
				ses.SetTenantInfo(&TenantInfo{Tenant: "sys", User: "dump"})
				proto.connectAttrs = map[string]string{}
				ec := newTestExecCtx(ctx, ctrl)

				err = handleShowBackendServers(ses, ec)
				require.NoError(t, err)
				rs := ses.GetMysqlResultSet()
				require.Equal(t, uint64(4), rs.GetColumnCount())
				require.Equal(t, uint64(3), rs.GetRowCount())
			})
		},
	)

}

func Test_RecordParseErrorStatement(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)

	proc := &process.Process{
		Base: &process.BaseProcess{},
		Ctx:  context.TODO(),
	}

	motrace.GetTracerProvider().SetEnable(true)
	_, err := RecordParseErrorStatement(context.TODO(), ses, proc, time.Now(), nil, nil, moerr.NewInternalErrorNoCtx("test"))
	assert.Nil(t, err)

	_, err = RecordParseErrorStatement(context.TODO(), ses, proc, time.Now(), []string{"abc", "def"}, []string{constant.ExternSql, constant.ExternSql}, moerr.NewInternalErrorNoCtx("test"))
	assert.Nil(t, err)
	assert.Nil(t, ses.GetStmtInfo())

	ses.beginResponseAccounting()
	_, err = RecordParseErrorStatement(context.TODO(), ses, proc, time.Now(), []string{"abc"}, []string{constant.ExternSql}, moerr.NewInternalErrorNoCtx("test"))
	assert.Nil(t, err)
	assert.NotNil(t, ses.GetStmtInfo())
	ses.finishResponseAccounting(context.TODO(), moerr.NewInternalErrorNoCtx("test"), true)
	assert.Nil(t, ses.GetStmtInfo())

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
		txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		use_t := mock_frontend.NewMockComputationWrapper(ctrl)
		use_t.EXPECT().GetUUID().Return(make([]byte, 16)).AnyTimes()
		stmts, err := parsers.Parse(ctx, dialect.MYSQL, "use T", 1)
		if err != nil {
			t.Error(err)
		}
		use_t.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		use_t.EXPECT().RecordExecPlan(ctx, nil).Return(nil).AnyTimes()
		use_t.EXPECT().Clear().AnyTimes()

		runner := mock_frontend.NewMockComputationRunner(ctrl)
		runner.EXPECT().Run(gomock.Any()).Return(nil, nil).AnyTimes()

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
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

// Benchmark_RecordStatement_IsTrue
// goos: darwin
// goarch: arm64
// pkg: github.com/matrixorigin/matrixone/pkg/frontend
// Benchmark_RecordStatement_IsTrue/SystemVariableBoolType_check_intVal
// Benchmark_RecordStatement_IsTrue/SystemVariableBoolType_check_intVal-10         	569427550	         2.130 ns/op
// Benchmark_RecordStatement_IsTrue/SystemVariableBoolType_check_boolVal
// Benchmark_RecordStatement_IsTrue/SystemVariableBoolType_check_boolVal-10        	579195536	         2.064 ns/op
// Benchmark_RecordStatement_IsTrue/atomic.Bool
// Benchmark_RecordStatement_IsTrue/atomic.Bool-10                                 	1000000000	         0.5303 ns/op
// Benchmark_RecordStatement_IsTrue/raw_bool
// Benchmark_RecordStatement_IsTrue/raw_bool-10                                    	1000000000	         0.3175 ns/op
// Benchmark_RecordStatement_IsTrue/check_time
// Benchmark_RecordStatement_IsTrue/check_time-10                                  	571145943	         2.049 ns/op
func Benchmark_RecordStatement_IsTrue(b *testing.B) {

	var intVal int64 = 0
	var boolVal = false
	var boolSync atomic.Bool
	var cnt = 0
	var boolVar = InitSystemVariableBoolType("_")

	b.Run("SystemVariableBoolType check intVal", func(b *testing.B) {
		cnt := 0
		for i := 0; i < b.N; i++ {
			if boolVar.IsTrue(intVal) {
				cnt++
			}
		}
	})

	b.Run("SystemVariableBoolType check boolVal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if boolVar.IsTrue(boolVal) {
				cnt++
			}
		}
	})

	b.Run("atomic.Bool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if boolSync.Load() {
				cnt++
			}
		}
	})

	b.Run("raw bool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if boolVal {
				cnt++
			}
		}
	})

	endTime := time.Now().Add(time.Minute)
	now := time.Now()
	b.Run("check time", func(b *testing.B) {
		cnt := 0
		for i := 0; i < b.N; i++ {
			if now.After(endTime) {
				cnt++
			}
		}
	})
}

func Test_ExecRequest_SidecarSuccess(t *testing.T) {
	ctx := context.TODO()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	saved := debugHTTPAddr
	defer func() { debugHTTPAddr = saved }()
	debugHTTPAddr = ":8888"

	// Mock sidecar returning a valid JSONCompact response.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"meta":[{"name":"x","type":"INTEGER"}],"data":[[42]],"rows":1}`))
	}))
	defer srv.Close()

	ses := newTestSession(t, ctrl)
	ses.txnHandler = &TxnHandler{}
	err := ses.SetSessionSysVar(ctx, "sidecar_url", srv.URL)
	require.NoError(t, err)
	ses.SetDatabaseName("testdb")
	setRowCount(ses, ses.GetProc(), 7)

	ec := newTestExecCtx(ctx, ctrl)
	req := &Request{
		cmd:  COM_QUERY,
		data: []byte("/*+ SIDECAR */ SELECT 42 AS x FROM testdb.t1"),
	}

	resp, err := ExecRequest(ses, ec, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, ResultResponse, resp.category)
	assert.Equal(t, int64(-1), ses.GetLastAffectedRows())
	assert.Equal(t, int64(-1), ses.GetProc().GetAffectedRows())
}

func Test_ExecRequest_SidecarError(t *testing.T) {
	ctx := context.TODO()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	saved := debugHTTPAddr
	defer func() { debugHTTPAddr = saved }()
	debugHTTPAddr = ":8888"

	// Mock sidecar that returns 500.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("boom"))
	}))
	defer srv.Close()

	ses := newTestSession(t, ctrl)
	ses.txnHandler = &TxnHandler{}
	err := ses.SetSessionSysVar(ctx, "sidecar_url", srv.URL)
	require.NoError(t, err)
	ses.SetDatabaseName("testdb")
	setRowCount(ses, ses.GetProc(), 7)

	ec := newTestExecCtx(ctx, ctrl)
	req := &Request{
		cmd:  COM_QUERY,
		data: []byte("/*+ SIDECAR */ SELECT 1 FROM testdb.t1"),
	}

	resp, err := ExecRequest(ses, ec, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	// Should be an error response (from sidecar), not a success.
	assert.Equal(t, ErrorResponse, resp.category)
	assert.Equal(t, int64(-1), ses.GetLastAffectedRows())
	assert.Equal(t, int64(-1), ses.GetProc().GetAffectedRows())
}

func TestExecRequestRewriteFailureMarksRowCountFailed(t *testing.T) {
	for _, cmd := range []CommandType{COM_QUERY, COM_STMT_PREPARE} {
		t.Run(cmd.String(), func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ses := newTestSession(t, ctrl)
			ses.txnHandler = &TxnHandler{}
			ses.rewriteEnabled.Store(true)
			ses.ruleCache = map[string]string{}
			setRowCount(ses, ses.GetProc(), 7)

			ec := newTestExecCtx(ctx, ctrl)
			req := &Request{
				cmd:  cmd,
				data: []byte(`/*+ {"rewrites":{"db.t":123}} */ select * from db.t`),
			}
			resp, err := ExecRequest(ses, ec, req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, ErrorResponse, resp.category)
			assert.Equal(t, int64(-1), ses.GetLastAffectedRows())
			assert.Equal(t, int64(-1), ses.GetProc().GetAffectedRows())
		})
	}
}

func TestExecRequestProtocolCommandRowCount(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	ses.txnHandler = &TxnHandler{}
	ec := newTestExecCtx(ctx, ctrl)
	require.Equal(t, int64(-1), ses.GetLastAffectedRows())
	require.Equal(t, int64(-1), ses.GetProc().GetAffectedRows())

	setRowCount(ses, ses.GetProc(), 7)
	resp, err := ExecRequest(ses, ec, &Request{cmd: COM_PING})
	require.NoError(t, err)
	require.Equal(t, OkResponse, resp.category)
	require.Equal(t, int64(0), ses.GetLastAffectedRows())
	require.Equal(t, int64(0), ses.GetProc().GetAffectedRows())

	setRowCount(ses, ses.GetProc(), 7)
	resp, err = ExecRequest(ses, ec, &Request{cmd: COM_SET_OPTION, data: []byte{0, 0}})
	require.NoError(t, err)
	require.Equal(t, OkResponse, resp.category)
	require.Equal(t, int64(-1), ses.GetLastAffectedRows())
	require.Equal(t, int64(-1), ses.GetProc().GetAffectedRows())

	for _, data := range [][]byte{{0}, {2, 0}} {
		setRowCount(ses, ses.GetProc(), 7)
		resp, err = ExecRequest(ses, ec, &Request{cmd: COM_SET_OPTION, data: data})
		require.NoError(t, err)
		require.Equal(t, ErrorResponse, resp.category)
		require.Equal(t, int64(-1), ses.GetLastAffectedRows())
		require.Equal(t, int64(-1), ses.GetProc().GetAffectedRows())
	}

	setRowCount(ses, ses.GetProc(), 7)
	resp, err = ExecRequest(ses, ec, &Request{cmd: CommandType(0xff)})
	require.NoError(t, err)
	require.Equal(t, ErrorResponse, resp.category)
	require.Equal(t, int64(-1), ses.GetLastAffectedRows())
	require.Equal(t, int64(-1), ses.GetProc().GetAffectedRows())

	setRowCount(ses, ses.GetProc(), 7)
	resp, err = ExecRequest(ses, ec, &Request{cmd: COM_STMT_CLOSE, data: []byte{1, 2, 3}})
	require.NoError(t, err)
	require.Equal(t, ErrorResponse, resp.category)
	require.Equal(t, int64(7), ses.GetLastAffectedRows())
	require.Equal(t, int64(7), ses.GetProc().GetAffectedRows())
}

func TestExecRequestStmtSendLongDataRowCount(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	execCtx := newTestExecCtx(ctx, ctrl)
	stmtID := uint32(1)
	stmtName := getPrepareStmtName(stmtID)
	st := tree.NewPrepareString(tree.Identifier(stmtName), "select ?")
	stmts, err := mysql.Parse(ctx, st.Sql, 1)
	require.NoError(t, err)
	preparePlan, err := buildPlan(ctx, nil, plan.NewEmptyCompilerContext(), st)
	require.NoError(t, err)
	prepareStmt := &PrepareStmt{
		Name:                stmtName,
		PreparePlan:         preparePlan,
		PrepareStmt:         stmts[0],
		getFromSendLongData: make(map[int]struct{}),
	}
	defer prepareStmt.Close()
	require.NoError(t, ses.SetPrepareStmt(ctx, stmtName, prepareStmt))

	setRowCount(ses, ses.GetProc(), 7)
	payload := make([]byte, 6)
	binary.LittleEndian.PutUint32(payload, stmtID)
	binary.LittleEndian.PutUint16(payload[4:], 0)
	payload = append(payload, "long data"...)
	resp, err := ExecRequest(ses, execCtx, &Request{cmd: COM_STMT_SEND_LONG_DATA, data: payload})
	require.NoError(t, err)
	require.Nil(t, resp)
	require.Equal(t, int64(7), ses.GetLastAffectedRows())
	require.Equal(t, int64(7), ses.GetProc().GetAffectedRows())

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "malformed packet", data: []byte{1, 2, 3}},
		{name: "unknown statement", data: []byte{2, 0, 0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setRowCount(ses, ses.GetProc(), 7)
			resp, err := ExecRequest(ses, execCtx, &Request{cmd: COM_STMT_SEND_LONG_DATA, data: tc.data})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, ErrorResponse, resp.category)
			require.Equal(t, int64(-1), ses.GetLastAffectedRows())
			require.Equal(t, int64(-1), ses.GetProc().GetAffectedRows())
		})
	}
}

func Test_ExecRequest_SidecarFallthrough(t *testing.T) {
	// SIDECAR hint present but sidecar not configured → strips hint, falls through.
	// doComQuery will fail (no engine), but we verify the fallthrough happened.
	ctx := context.TODO()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	saved := debugHTTPAddr
	defer func() { debugHTTPAddr = saved }()
	debugHTTPAddr = "" // no manifest URL → errSidecarNotConfigured

	ses := newTestSession(t, ctrl)
	ses.txnHandler = &TxnHandler{}

	ec := newTestExecCtx(ctx, ctrl)
	req := &Request{
		cmd:  COM_QUERY,
		data: []byte("/*+ SIDECAR */ SELECT 1"),
	}

	// ExecRequest won't return an error even though doComQuery panics/fails;
	// the deferred recover catches it. We just verify it doesn't panic.
	resp, _ := ExecRequest(ses, ec, req)
	_ = resp
}

func Test_unsupportedCommand(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	execCtx := &ExecCtx{
		ses: ses,
	}
	req := &Request{
		cmd: COM_SLEEP,
	}

	resp, err := ExecRequest(ses, execCtx, req)
	assert.Nil(t, err)
	assert.NotNil(t, resp)
	respErr := resp.GetData().(*moerr.Error)
	assert.Equal(t, "internal error: unsupported command. 0x0", respErr.Error())
}

func Test_ExecRequestStmtExecuteErrorClearsPreparedParamState(t *testing.T) {
	ctx := context.TODO()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	execCtx := &ExecCtx{
		ses:    ses,
		reqCtx: ctx,
	}

	st := tree.NewPrepareString(tree.Identifier(getPrepareStmtName(1)), "select ?")
	stmts, err := mysql.Parse(ctx, st.Sql, 1)
	require.NoError(t, err)

	compCtx := plan.NewEmptyCompilerContext()
	preparePlan, err := buildPlan(ctx, nil, compCtx, st)
	require.NoError(t, err)

	prepareStmt := &PrepareStmt{
		Name:                preparePlan.GetDcl().GetPrepare().GetName(),
		PreparePlan:         preparePlan,
		PrepareStmt:         stmts[0],
		getFromSendLongData: make(map[int]struct{}),
	}
	require.NoError(t, ses.SetPrepareStmt(ctx, prepareStmt.Name, prepareStmt))

	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, 1)
	payload = append(payload, 0)          // flag
	payload = append(payload, 0, 0, 0, 0) // iteration-count
	payload = append(payload, 0)          // null bitmap
	payload = append(payload, 1)          // new param bound flag
	payload = append(payload, uint8(defines.MYSQL_TYPE_VAR_STRING), 0)
	payload = append(payload, 5, 'a', 'b') // truncated lenenc string

	resp, err := ExecRequest(ses, execCtx, &Request{cmd: COM_STMT_EXECUTE, data: payload})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, prepareStmt.params)
	require.Empty(t, prepareStmt.getFromSendLongData)
}

func Test_panic(t *testing.T) {
	fault.EnableDomain(fault.DomainFrontend)
	defer fault.DisableDomain(fault.DomainFrontend)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runPanic := func(panicChoice int64) {
		fault.AddFaultPointInDomain(context.Background(), fault.DomainFrontend, "exec_request_panic", ":::", "panic", panicChoice, "has panic", false)
		defer fault.RemoveFaultPointFromDomain(context.Background(), fault.DomainFrontend, "exec_request_panic")

		ses := newTestSession(t, ctrl)
		execCtx := &ExecCtx{
			ses: ses,
		}
		req := &Request{
			cmd:  COM_SET_OPTION,
			data: []byte("123"),
		}

		_, err := ExecRequest(ses, execCtx, req)
		assert.NotNil(t, err)
	}

	runPanic(fault.PanicUseMoErr)
	runPanic(fault.PanicUseNonMoErr)
}

func Test_run_panic(t *testing.T) {
	fault.EnableDomain(fault.DomainFrontend)
	defer fault.DisableDomain(fault.DomainFrontend)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runPanic := func(panicChoice int64) {
		fault.AddFaultPointInDomain(context.Background(), fault.DomainFrontend, "executeStmtWithWorkspace_panic", ":::", "panic", panicChoice, "has panic", false)
		defer fault.RemoveFaultPointFromDomain(context.Background(), fault.DomainFrontend, "executeStmtWithWorkspace_panic")

		ses := newTestSession(t, ctrl)
		execCtx := &ExecCtx{
			ses: ses,
		}

		err := executeStmtWithWorkspace(ses, nil, execCtx)
		assert.NotNil(t, err)
	}

	runPanic(fault.PanicUseMoErr)
	runPanic(fault.PanicUseNonMoErr)
}

func Test_handleShowTableStatus(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), 1)
	newShowTableStatusFixture := func(
		reqCtx context.Context,
		ctrl *gomock.Controller,
		tableType, tableName string,
		roleID uint32,
		tenant *TenantInfo,
		dbName string,
	) (*Session, *ExecCtx, *tree.ShowTableStatus) {
		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{TableType: tableType}).AnyTimes()

		database := mock_frontend.NewMockDatabase(ctrl)
		database.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		database.EXPECT().Relation(gomock.Any(), gomock.Any(), nil).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), nil).Return(database, nil).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		pu.StorageEngine = eng
		pu.TxnClient = txnClient
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(reqCtx, "", proto, nil)
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}
		ses.SetDatabaseName(dbName)
		ses.data = [][]interface{}{{
			[]byte(tableName), nil, nil, nil, nil, nil, nil, nil, nil,
			nil, nil, nil, nil, nil, nil, nil, nil, roleID, nil,
		}}
		proto.SetSession(ses)
		return ses, newTestExecCtx(reqCtx, ctrl), &tree.ShowTableStatus{DbName: dbName}
	}

	convey.Convey("handleShowTableStatus succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{TableType: catalog.SystemViewRel}).AnyTimes()

		database := mock_frontend.NewMockDatabase(ctrl)
		database.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		database.EXPECT().Relation(gomock.Any(), gomock.Any(), nil).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), nil).Return(database, nil).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		pu.StorageEngine = eng
		pu.TxnClient = txnClient
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
		tenant := &TenantInfo{
			Tenant:   "sys",
			TenantID: 0,
			User:     DefaultTenantMoAdmin,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}
		ses.SetDatabaseName("t")
		ses.data = [][]interface{}{{[]byte("t"), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, uint32(0), nil}}

		proto.SetSession(ses)

		ec := newTestExecCtx(ctx, ctrl)
		shv := &tree.ShowTableStatus{}
		convey.So(handleShowTableStatus(ses, ec, shv), convey.ShouldBeNil)

		ec = newTestExecCtx(context.Background(), ctrl)
		convey.So(handleShowTableStatus(ses, ec, shv), convey.ShouldNotBeNil)
	})

	convey.Convey("handleShowTableStatus statement_info fallback rows", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
		}).AnyTimes()

		database := mock_frontend.NewMockDatabase(ctrl)
		database.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		database.EXPECT().Relation(gomock.Any(), gomock.Any(), nil).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), nil).Return(database, nil).AnyTimes()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		pu.StorageEngine = eng
		pu.TxnClient = txnClient
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
		tenant := &TenantInfo{
			Tenant:        "acc_fallback",
			TenantID:      1,
			User:          "admin",
			DefaultRole:   accountAdminRoleName,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}
		ses.SetDatabaseName(catalog.MO_SYSTEM)
		ses.data = [][]interface{}{{
			[]byte(catalog.MO_STATEMENT), nil, nil, nil, nil, nil, nil, nil, nil,
			nil, nil, nil, nil, nil, nil, nil, nil, uint32(moAdminRoleID), nil,
		}}

		statsRet := mock_frontend.NewMockExecResult(ctrl)
		statsRet.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		statsRet.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return(catalog.MO_STATEMENT, nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(1)).Return(int64(0), nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(2)).Return(int64(128), nil).AnyTimes()

		countRet := mock_frontend.NewMockExecResult(ctrl)
		countRet.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		countRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(0)).Return(int64(7), nil).AnyTimes()

		calls := 0
		execStub := gostub.Stub(&ExeSqlInBgSes, func(reqCtx context.Context, bh BackgroundExec, sql string) ([]ExecResult, error) {
			calls++
			switch {
			case strings.Contains(sql, "mo_table_rows(db, tbl)"):
				return []ExecResult{statsRet}, nil
			case strings.Contains(sql, "select count(*) from `system`.`statement_info`"):
				return []ExecResult{countRet}, nil
			default:
				return nil, moerr.NewInternalErrorf(reqCtx, "unexpected sql in test: %s", sql)
			}
		})
		defer execStub.Reset()

		proto.SetSession(ses)
		ec := newTestExecCtx(ctx, ctrl)
		shv := &tree.ShowTableStatus{DbName: catalog.MO_SYSTEM}
		convey.So(handleShowTableStatus(ses, ec, shv), convey.ShouldBeNil)
		convey.So(calls, convey.ShouldEqual, 2)
		convey.So(ses.data[0][3], convey.ShouldEqual, int64(7))
		convey.So(ses.data[0][4], convey.ShouldEqual, int64(0))
		convey.So(ses.data[0][5], convey.ShouldEqual, int64(128))
	})

	convey.Convey("handleShowTableStatus statement_info with rows should skip fallback", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses, ec, shv := newShowTableStatusFixture(
			ctx, ctrl,
			catalog.SystemOrdinaryRel, catalog.MO_STATEMENT,
			uint32(moAdminRoleID),
			&TenantInfo{
				Tenant:        "acc_fallback",
				TenantID:      1,
				User:          "admin",
				DefaultRole:   accountAdminRoleName,
				DefaultRoleID: moAdminRoleID,
			},
			catalog.MO_SYSTEM,
		)

		statsRet := mock_frontend.NewMockExecResult(ctrl)
		statsRet.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		statsRet.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return(catalog.MO_STATEMENT, nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(1)).Return(int64(3), nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(2)).Return(int64(90), nil).AnyTimes()

		countCalls := 0
		execStub := gostub.Stub(&ExeSqlInBgSes, func(reqCtx context.Context, bh BackgroundExec, sql string) ([]ExecResult, error) {
			switch {
			case strings.Contains(sql, "mo_table_rows(db, tbl)"):
				return []ExecResult{statsRet}, nil
			case strings.Contains(sql, "select count(*) from `system`.`statement_info`"):
				countCalls++
				return nil, moerr.NewInternalErrorf(reqCtx, "count query should not be called, sql: %s", sql)
			default:
				return nil, moerr.NewInternalErrorf(reqCtx, "unexpected sql in test: %s", sql)
			}
		})
		defer execStub.Reset()

		convey.So(handleShowTableStatus(ses, ec, shv), convey.ShouldBeNil)
		convey.So(countCalls, convey.ShouldEqual, 0)
		convey.So(ses.data[0][3], convey.ShouldEqual, int64(3))
		convey.So(ses.data[0][4], convey.ShouldEqual, int64(30))
		convey.So(ses.data[0][5], convey.ShouldEqual, int64(90))
	})

	convey.Convey("handleShowTableStatus non-special table should not fallback", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses, ec, shv := newShowTableStatusFixture(
			ctx, ctrl,
			catalog.SystemOrdinaryRel, "not_special",
			uint32(moAdminRoleID),
			&TenantInfo{
				Tenant:        "acc_fallback",
				TenantID:      1,
				User:          "admin",
				DefaultRole:   accountAdminRoleName,
				DefaultRoleID: moAdminRoleID,
			},
			catalog.MO_SYSTEM,
		)

		statsRet := mock_frontend.NewMockExecResult(ctrl)
		statsRet.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		statsRet.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("not_special", nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(1)).Return(int64(5), nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(2)).Return(int64(100), nil).AnyTimes()

		countCalls := 0
		execStub := gostub.Stub(&ExeSqlInBgSes, func(reqCtx context.Context, bh BackgroundExec, sql string) ([]ExecResult, error) {
			switch {
			case strings.Contains(sql, "mo_table_rows(db, tbl)"):
				return []ExecResult{statsRet}, nil
			case strings.Contains(sql, "select count(*)"):
				countCalls++
				return nil, moerr.NewInternalErrorf(reqCtx, "count query should not be called, sql: %s", sql)
			default:
				return nil, moerr.NewInternalErrorf(reqCtx, "unexpected sql in test: %s", sql)
			}
		})
		defer execStub.Reset()

		convey.So(handleShowTableStatus(ses, ec, shv), convey.ShouldBeNil)
		convey.So(countCalls, convey.ShouldEqual, 0)
		convey.So(ses.data[0][3], convey.ShouldEqual, int64(5))
		convey.So(ses.data[0][4], convey.ShouldEqual, int64(20))
		convey.So(ses.data[0][5], convey.ShouldEqual, int64(100))
	})

	convey.Convey("handleShowTableStatus statement_info fallback empty result should keep mo_table_rows", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses, ec, shv := newShowTableStatusFixture(
			ctx, ctrl,
			catalog.SystemOrdinaryRel, catalog.MO_STATEMENT,
			uint32(moAdminRoleID),
			&TenantInfo{
				Tenant:        "acc_fallback",
				TenantID:      1,
				User:          "admin",
				DefaultRole:   accountAdminRoleName,
				DefaultRoleID: moAdminRoleID,
			},
			catalog.MO_SYSTEM,
		)

		statsRet := mock_frontend.NewMockExecResult(ctrl)
		statsRet.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		statsRet.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return(catalog.MO_STATEMENT, nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(1)).Return(int64(0), nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(2)).Return(int64(90), nil).AnyTimes()

		countRet := mock_frontend.NewMockExecResult(ctrl)
		countRet.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()

		calls := 0
		execStub := gostub.Stub(&ExeSqlInBgSes, func(reqCtx context.Context, bh BackgroundExec, sql string) ([]ExecResult, error) {
			calls++
			switch {
			case strings.Contains(sql, "mo_table_rows(db, tbl)"):
				return []ExecResult{statsRet}, nil
			case strings.Contains(sql, "select count(*) from `system`.`statement_info`"):
				return []ExecResult{countRet}, nil
			default:
				return nil, moerr.NewInternalErrorf(reqCtx, "unexpected sql in test: %s", sql)
			}
		})
		defer execStub.Reset()

		convey.So(handleShowTableStatus(ses, ec, shv), convey.ShouldBeNil)
		convey.So(calls, convey.ShouldEqual, 2)
		convey.So(ses.data[0][3], convey.ShouldEqual, int64(0))
		convey.So(ses.data[0][4], convey.ShouldEqual, int64(0))
		convey.So(ses.data[0][5], convey.ShouldEqual, int64(90))
	})

	convey.Convey("handleShowTableStatus statement_info fallback count query error", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses, ec, shv := newShowTableStatusFixture(
			ctx, ctrl,
			catalog.SystemOrdinaryRel, catalog.MO_STATEMENT,
			uint32(moAdminRoleID),
			&TenantInfo{
				Tenant:        "acc_fallback",
				TenantID:      1,
				User:          "admin",
				DefaultRole:   accountAdminRoleName,
				DefaultRoleID: moAdminRoleID,
			},
			catalog.MO_SYSTEM,
		)

		statsRet := mock_frontend.NewMockExecResult(ctrl)
		statsRet.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		statsRet.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return(catalog.MO_STATEMENT, nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(1)).Return(int64(0), nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(2)).Return(int64(90), nil).AnyTimes()

		execStub := gostub.Stub(&ExeSqlInBgSes, func(reqCtx context.Context, bh BackgroundExec, sql string) ([]ExecResult, error) {
			switch {
			case strings.Contains(sql, "mo_table_rows(db, tbl)"):
				return []ExecResult{statsRet}, nil
			case strings.Contains(sql, "select count(*) from `system`.`statement_info`"):
				return nil, moerr.NewInternalErrorf(reqCtx, "mock count query error")
			default:
				return nil, moerr.NewInternalErrorf(reqCtx, "unexpected sql in test: %s", sql)
			}
		})
		defer execStub.Reset()

		convey.So(handleShowTableStatus(ses, ec, shv), convey.ShouldNotBeNil)
	})

	convey.Convey("handleShowTableStatus statement_info fallback count decode error", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses, ec, shv := newShowTableStatusFixture(
			ctx, ctrl,
			catalog.SystemOrdinaryRel, catalog.MO_STATEMENT,
			uint32(moAdminRoleID),
			&TenantInfo{
				Tenant:        "acc_fallback",
				TenantID:      1,
				User:          "admin",
				DefaultRole:   accountAdminRoleName,
				DefaultRoleID: moAdminRoleID,
			},
			catalog.MO_SYSTEM,
		)

		statsRet := mock_frontend.NewMockExecResult(ctrl)
		statsRet.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		statsRet.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return(catalog.MO_STATEMENT, nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(1)).Return(int64(0), nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(2)).Return(int64(90), nil).AnyTimes()

		countRet := mock_frontend.NewMockExecResult(ctrl)
		countRet.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		countRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(0)).Return(int64(0), moerr.NewInternalErrorNoCtx("mock decode error")).AnyTimes()

		execStub := gostub.Stub(&ExeSqlInBgSes, func(reqCtx context.Context, bh BackgroundExec, sql string) ([]ExecResult, error) {
			switch {
			case strings.Contains(sql, "mo_table_rows(db, tbl)"):
				return []ExecResult{statsRet}, nil
			case strings.Contains(sql, "select count(*) from `system`.`statement_info`"):
				return []ExecResult{countRet}, nil
			default:
				return nil, moerr.NewInternalErrorf(reqCtx, "unexpected sql in test: %s", sql)
			}
		})
		defer execStub.Reset()

		convey.So(handleShowTableStatus(ses, ec, shv), convey.ShouldNotBeNil)
	})

	convey.Convey("handleShowTableStatus sys account should skip special rows fallback", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		sysCtx := defines.AttachAccountId(context.TODO(), sysAccountID)
		ses, ec, shv := newShowTableStatusFixture(
			sysCtx, ctrl,
			catalog.SystemOrdinaryRel, catalog.MO_DATABASE,
			uint32(moAdminRoleID),
			&TenantInfo{
				Tenant:   "sys",
				TenantID: 0,
				User:     DefaultTenantMoAdmin,
			},
			catalog.MO_SYSTEM,
		)

		statsRet := mock_frontend.NewMockExecResult(ctrl)
		statsRet.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		statsRet.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return(catalog.MO_DATABASE, nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(1)).Return(int64(11), nil).AnyTimes()
		statsRet.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(2)).Return(int64(121), nil).AnyTimes()

		countCalls := 0
		execStub := gostub.Stub(&ExeSqlInBgSes, func(reqCtx context.Context, bh BackgroundExec, sql string) ([]ExecResult, error) {
			switch {
			case strings.Contains(sql, "mo_table_rows(db, tbl)"):
				return []ExecResult{statsRet}, nil
			case strings.Contains(sql, "select count(*)"):
				countCalls++
				return nil, moerr.NewInternalErrorf(reqCtx, "count query should not be called, sql: %s", sql)
			default:
				return nil, moerr.NewInternalErrorf(reqCtx, "unexpected sql in test: %s", sql)
			}
		})
		defer execStub.Reset()

		convey.So(handleShowTableStatus(ses, ec, shv), convey.ShouldBeNil)
		convey.So(countCalls, convey.ShouldEqual, 0)
		convey.So(ses.data[0][3], convey.ShouldEqual, int64(11))
		convey.So(ses.data[0][4], convey.ShouldEqual, int64(11))
		convey.So(ses.data[0][5], convey.ShouldEqual, int64(121))
	})
}

func Test_checkModify(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	resolveFn := func(schemaName, tableName string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
		if schemaName == "err_db" {
			return nil, nil, &moerr.Error{}
		}
		return nil, nil, nil
	}
	tests := []struct {
		node          *plan.Node
		expected_flag bool
		expected_err  bool
	}{
		{
			node:          &plan.Node{},
			expected_flag: false,
			expected_err:  false,
		},
		{
			node: &plan.Node{
				TableDef: &plan.TableDef{},
				ObjRef: &plan.ObjectRef{
					SchemaName: "err_db",
				},
			},
			expected_flag: true,
			expected_err:  true,
		},
		{
			node: &plan.Node{
				TableDef: &plan.TableDef{
					Name:    "test",
					TblId:   1,
					Version: 1,
				},
				ObjRef: &plan.ObjectRef{
					SchemaName: "test_db",
				},
			},
			expected_flag: true,
			expected_err:  false,
		},
		{
			node: &plan.Node{
				InsertCtx: &plan0.InsertCtx{},
			},
			expected_flag: true,
			expected_err:  false,
		},
		{
			node: &plan.Node{
				DeleteCtx: &plan0.DeleteCtx{},
			},
			expected_flag: true,
			expected_err:  false,
		},
		{
			node: &plan.Node{
				PreInsertCtx: &plan0.PreInsertCtx{},
			},
			expected_flag: true,
			expected_err:  false,
		},
	}

	flag, _ := checkModify(nil, resolveFn)
	assert.Equal(t, true, flag)

	for _, test := range tests {
		flag, err := checkModify(&plan.Plan{
			Plan: &plan.Plan_Query{
				Query: &plan.Query{
					Nodes: []*plan.Node{
						test.node,
					},
				},
			},
		}, resolveFn)
		assert.Equal(t, test.expected_flag, flag)
		if !test.expected_err {
			assert.Nil(t, err)
		}
	}
}

// testMysqlWriterWithError is a test mysql writer that can return error from ParseSendLongData
type testMysqlWriterWithError struct {
	*testMysqlWriter
	returnError error
}

func (fp *testMysqlWriterWithError) ParseSendLongData(ctx context.Context, proc *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	if fp.returnError != nil {
		return fp.returnError
	}
	return nil
}

func Test_parseStmtSendLongData(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("parseStmtSendLongData", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Test case 1: data length < 4 bytes - should return error
		convey.Convey("data length less than 4 bytes", func() {
			ses := newTestSession(t, ctrl)
			data := []byte{1, 2, 3} // only 3 bytes
			err := parseStmtSendLongData(ctx, ses, data)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(moerr.IsMoErrCode(err, moerr.ErrInvalidInput), convey.ShouldBeTrue)
		})

		// Test case 2: GetPrepareStmt returns error
		convey.Convey("GetPrepareStmt returns error", func() {
			ses := newTestSession(t, ctrl)
			stmtID := uint32(123)
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, stmtID)
			// Add some additional data
			data = append(data, []byte("additional data")...)

			err := parseStmtSendLongData(ctx, ses, data)
			convey.So(err, convey.ShouldNotBeNil)
		})

		// Test case 3: Normal flow with IsCloudNonuser = false
		convey.Convey("normal flow with IsCloudNonuser false", func() {
			ses := newTestSession(t, ctrl)
			stmtID := uint32(456)
			stmtName := getPrepareStmtName(stmtID)

			// Create a prepare statement
			preStmt := &PrepareStmt{
				Name:           stmtName,
				Sql:            "SELECT ? FROM t",
				IsCloudNonuser: false,
			}
			ses.mu.Lock()
			if ses.prepareStmts == nil {
				ses.prepareStmts = make(map[string]*PrepareStmt)
			}
			ses.prepareStmts[stmtName] = preStmt
			ses.mu.Unlock()

			// Create test mysql writer
			sv, err := getSystemVariables("test/system_vars_config.toml")
			convey.So(err, convey.ShouldBeNil)
			pu := config.NewParameterUnit(sv, nil, nil, nil)
			ioses, err := NewIOSession(&testConn{}, pu, "")
			convey.So(err, convey.ShouldBeNil)
			testWriter := &testMysqlWriter{ioses: ioses}
			ses.respr = NewMysqlResp(testWriter)

			// Create data with stmtID
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, stmtID)
			data = append(data, []byte("long data content")...)

			err = parseStmtSendLongData(ctx, ses, data)
			convey.So(err, convey.ShouldBeNil)
		})

		// Test case 4: Normal flow with IsCloudNonuser = true
		convey.Convey("normal flow with IsCloudNonuser true", func() {
			ses := newTestSession(t, ctrl)
			stmtID := uint32(789)
			stmtName := getPrepareStmtName(stmtID)

			// Create a prepare statement with IsCloudNonuser = true
			preStmt := &PrepareStmt{
				Name:           stmtName,
				Sql:            "SELECT ? FROM t",
				IsCloudNonuser: true,
			}
			ses.mu.Lock()
			if ses.prepareStmts == nil {
				ses.prepareStmts = make(map[string]*PrepareStmt)
			}
			ses.prepareStmts[stmtName] = preStmt
			ses.mu.Unlock()

			// Create test mysql writer
			sv, err := getSystemVariables("test/system_vars_config.toml")
			convey.So(err, convey.ShouldBeNil)
			pu := config.NewParameterUnit(sv, nil, nil, nil)
			ioses, err := NewIOSession(&testConn{}, pu, "")
			convey.So(err, convey.ShouldBeNil)
			testWriter := &testMysqlWriter{ioses: ioses}
			ses.respr = NewMysqlResp(testWriter)

			// Create data with stmtID
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, stmtID)
			data = append(data, []byte("long data content")...)

			err = parseStmtSendLongData(ctx, ses, data)
			convey.So(err, convey.ShouldBeNil)
		})

		// Test case 5: ParseSendLongData returns error
		convey.Convey("ParseSendLongData returns error", func() {
			ses := newTestSession(t, ctrl)
			stmtID := uint32(999)
			stmtName := getPrepareStmtName(stmtID)

			// Create a prepare statement
			preStmt := &PrepareStmt{
				Name:           stmtName,
				Sql:            "SELECT ? FROM t",
				IsCloudNonuser: false,
			}
			ses.mu.Lock()
			if ses.prepareStmts == nil {
				ses.prepareStmts = make(map[string]*PrepareStmt)
			}
			ses.prepareStmts[stmtName] = preStmt
			ses.mu.Unlock()

			// Create test mysql writer that returns error
			expectedErr := moerr.NewInternalError(ctx, "parse send long data error")
			sv, err := getSystemVariables("test/system_vars_config.toml")
			convey.So(err, convey.ShouldBeNil)
			pu := config.NewParameterUnit(sv, nil, nil, nil)
			ioses, err := NewIOSession(&testConn{}, pu, "")
			convey.So(err, convey.ShouldBeNil)
			baseWriter := &testMysqlWriter{ioses: ioses}
			testWriter := &testMysqlWriterWithError{
				testMysqlWriter: baseWriter,
				returnError:     expectedErr,
			}
			ses.respr = NewMysqlResp(testWriter)

			// Create data with stmtID
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, stmtID)
			data = append(data, []byte("long data content")...)

			err = parseStmtSendLongData(ctx, ses, data)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldEqual, expectedErr)
		})

		// Test case 6: Empty data after stmtID (only 4 bytes)
		convey.Convey("empty data after stmtID", func() {
			ses := newTestSession(t, ctrl)
			stmtID := uint32(111)
			stmtName := getPrepareStmtName(stmtID)

			// Create a prepare statement
			preStmt := &PrepareStmt{
				Name:           stmtName,
				Sql:            "SELECT ? FROM t",
				IsCloudNonuser: false,
			}
			ses.mu.Lock()
			if ses.prepareStmts == nil {
				ses.prepareStmts = make(map[string]*PrepareStmt)
			}
			ses.prepareStmts[stmtName] = preStmt
			ses.mu.Unlock()

			// Create test mysql writer
			sv, err := getSystemVariables("test/system_vars_config.toml")
			convey.So(err, convey.ShouldBeNil)
			pu := config.NewParameterUnit(sv, nil, nil, nil)
			ioses, err := NewIOSession(&testConn{}, pu, "")
			convey.So(err, convey.ShouldBeNil)
			testWriter := &testMysqlWriter{ioses: ioses}
			ses.respr = NewMysqlResp(testWriter)

			// Create data with only stmtID (4 bytes)
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, stmtID)

			err = parseStmtSendLongData(ctx, ses, data)
			convey.So(err, convey.ShouldBeNil)
		})

		// Test case 7: Large stmtID
		convey.Convey("large stmtID", func() {
			ses := newTestSession(t, ctrl)
			stmtID := uint32(0xFFFFFFFF) // max uint32
			stmtName := getPrepareStmtName(stmtID)

			// Create a prepare statement
			preStmt := &PrepareStmt{
				Name:           stmtName,
				Sql:            "SELECT ? FROM t",
				IsCloudNonuser: false,
			}
			ses.mu.Lock()
			if ses.prepareStmts == nil {
				ses.prepareStmts = make(map[string]*PrepareStmt)
			}
			ses.prepareStmts[stmtName] = preStmt
			ses.mu.Unlock()

			// Create test mysql writer
			sv, err := getSystemVariables("test/system_vars_config.toml")
			convey.So(err, convey.ShouldBeNil)
			pu := config.NewParameterUnit(sv, nil, nil, nil)
			ioses, err := NewIOSession(&testConn{}, pu, "")
			convey.So(err, convey.ShouldBeNil)
			testWriter := &testMysqlWriter{ioses: ioses}
			ses.respr = NewMysqlResp(testWriter)

			// Create data with stmtID
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, stmtID)
			data = append(data, []byte("test data")...)

			err = parseStmtSendLongData(ctx, ses, data)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}
