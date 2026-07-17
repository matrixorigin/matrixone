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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCompile struct {
	runFunc     func(uint64) (*util2.RunResult, error)
	getPlanFunc func() *plan.Plan
	releaseFunc func()
}

func TestResourceAttemptOwnerEligible(t *testing.T) {
	require.True(t, resourceAttemptOwnerEligible(&Session{}))
	require.False(t, resourceAttemptOwnerEligible(&backSession{}))
}

func (m *mockCompile) Run(ts uint64) (*util2.RunResult, error) { return m.runFunc(ts) }
func (m *mockCompile) GetPlan() *plan.Plan                     { return m.getPlanFunc() }
func (m *mockCompile) Release()                                { m.releaseFunc() }
func (m *mockCompile) SetOriginSQL(s string)                   {}

func TestTxnComputationWrapper_Run(t *testing.T) {
	expectedResult := &util2.RunResult{AffectRows: 10}
	expectedPlan := &plan.Plan{}

	mockComp := &mockCompile{
		runFunc: func(ts uint64) (*util2.RunResult, error) {
			return expectedResult, nil
		},
		getPlanFunc: func() *plan.Plan {
			return expectedPlan
		},
		releaseFunc: func() {},
	}

	cwft := &TxnComputationWrapper{
		compile: mockComp,
	}

	// Test successful run
	res, err := cwft.Run(100)
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, res)
	assert.Equal(t, expectedPlan, cwft.plan)
	assert.Equal(t, expectedResult, cwft.runResult)
	assert.Nil(t, cwft.compile) // Should be cleared after Run
}

func TestTxnComputationWrapper_Run_Error(t *testing.T) {
	expectedErr := assert.AnError
	expectedPlan := &plan.Plan{}

	mockComp := &mockCompile{
		runFunc: func(ts uint64) (*util2.RunResult, error) {
			return nil, expectedErr
		},
		getPlanFunc: func() *plan.Plan {
			return expectedPlan
		},
		releaseFunc: func() {},
	}

	cwft := &TxnComputationWrapper{
		compile: mockComp,
	}

	// Test error run
	res, err := cwft.Run(100)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, res)
	assert.Equal(t, expectedPlan, cwft.plan)
	assert.Nil(t, cwft.compile)
}

// newPreparedExecuteEnv sets up a session holding a prepared "select 1" and a
// computation wrapper that executes it through the binary protocol, so tests
// can drive cw.Compile through initExecuteStmtParam.
func newPreparedExecuteEnv(t *testing.T, stmtID uint32) (*Session, *PrepareStmt, *TxnComputationWrapper, *ExecCtx) {
	return newPreparedExecuteEnvForSQL(t, stmtID, "select 1")
}

func newPreparedExecuteEnvForSQL(t *testing.T, stmtID uint32, sql string) (*Session, *PrepareStmt, *TxnComputationWrapper, *ExecCtx) {
	ctx := statistic.ContextWithStatsInfo(context.Background(), statistic.NewStatsInfo())
	ctx = defines.AttachAccount(ctx, sysAccountID, rootID, moAdminRoleID)
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))

	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	proc := ses.GetProc()
	require.NotNil(t, proc)
	proc.Base.SessionInfo.StorageEngine = &disttae.Engine{}

	stmtName := getPrepareStmtName(stmtID)
	prepareString := tree.NewPrepareString(tree.Identifier(stmtName), sql)
	stmts, err := mysql.Parse(ctx, prepareString.Sql, 1)
	require.NoError(t, err)
	preparePlan, err := buildPlan(ctx, nil, plan2.NewEmptyCompilerContext(), prepareString)
	require.NoError(t, err)

	prepareStmt := &PrepareStmt{
		Name:                stmtName,
		Sql:                 prepareString.Sql,
		PreparePlan:         preparePlan,
		PrepareStmt:         stmts[0],
		getFromSendLongData: make(map[int]struct{}),
	}
	require.NoError(t, ses.SetPrepareStmt(ctx, stmtName, prepareStmt))

	cw := InitTxnComputationWrapper(ses, stmts[0], proc)
	cw.plan = preparePlan.GetDcl().GetPrepare().Plan
	execCtx := &ExecCtx{
		reqCtx: ctx,
		ses:    ses,
		proc:   proc,
		resper: ses.GetResponser(),
		input: &UserInput{
			stmtName:            stmtName,
			isBinaryProtExecute: true,
		},
	}
	ses.GetTxnCompileCtx().SetExecCtx(execCtx)
	proc.SetResolveVariableFunc(ses.txnCompileCtx.ResolveVariable)
	proc.SetResolveVariableIsBinFunc(ses.txnCompileCtx.ResolveVariableIsBin)
	return ses, prepareStmt, cw, execCtx
}

func TestInitExecuteStmtParamPreservesBinaryFlagPerUserVariable(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 102, "select ?, ?")
	defer prepareStmt.Close()

	require.NoError(t, ses.setUserDefinedVar("binary_param", "AB\x00\x00", "", true))
	require.NoError(t, ses.SetUserDefinedVar("text_param", "text", ""))
	isBin, err := ses.txnCompileCtx.ResolveVariableIsBin("binary_param", false, false)
	require.NoError(t, err)
	require.True(t, isBin)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("text_param", false, false)
	require.NoError(t, err)
	require.False(t, isBin)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("system_var", true, false)
	require.NoError(t, err)
	require.False(t, isBin)
	_, err = ses.txnCompileCtx.ResolveVariableIsBin("missing", false, false)
	require.Error(t, err)
	cw.proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		variable, err := ses.GetUserDefinedVar(name)
		if err != nil {
			return nil, err
		}
		return variable.Value, nil
	})
	execPlan := &plan.Execute{
		Name: prepareStmt.Name,
		Args: []*plan.Expr{
			{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "binary_param"}}},
			{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "text_param"}}},
		},
	}

	_, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	require.True(t, cw.proc.GetPrepareParamIsBin(0))
	require.False(t, cw.proc.GetPrepareParamIsBin(1))
	require.Equal(t, plan2.ParamValue{Value: "AB\x00\x00", IsBin: true}, cw.paramVals[0])
	require.Equal(t, plan2.ParamValue{Value: "text", IsBin: false}, cw.paramVals[1])

	params := cw.proc.GetPrepareParams()
	require.NoError(t, ses.SetUserDefinedVar("binary_param", "now-text", ""))
	_, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	require.Zero(t, params.Length(), "the previous owned params must be released on successful replacement")
	require.Nil(t, params.GetData())
	require.False(t, cw.proc.GetPrepareParamIsBin(0))
	require.Equal(t, "now-text", cw.proc.GetPrepareParams().GetStringAt(0))

	current := cw.proc.GetPrepareParams()
	cw.proc.SetPrepareParams(vector.NewVec(types.T_text.ToType()))
	require.Zero(t, current.Length())
	require.Nil(t, current.GetData())
	require.False(t, cw.proc.GetPrepareParamIsBin(0), "binary metadata must not leak into the next execution")
	cw.proc.GetPrepareParams().Free(cw.proc.Mp())
	cw.proc.SetPrepareParams(nil)
}

func TestInitExecuteStmtParamFreesParamsOnResolveError(t *testing.T) {
	ses, prepareStmt, cw, _ := newPreparedExecuteEnvForSQL(t, 103, "select ?, ?")
	defer prepareStmt.Close()
	require.NoError(t, ses.SetUserDefinedVar("first", "allocated", ""))
	cw.proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "second" {
			return nil, assert.AnError
		}
		variable, err := ses.GetUserDefinedVar(name)
		if err != nil {
			return nil, err
		}
		return variable.Value, nil
	})
	execPlan := &plan.Execute{
		Name: prepareStmt.Name,
		Args: []*plan.Expr{
			{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "first"}}},
			{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "second"}}},
		},
	}
	params, _, _, err := buildExecuteUserParams(cw.proc, execPlan.Args)
	require.ErrorIs(t, err, assert.AnError)
	require.Zero(t, params.Length())
	require.Nil(t, params.GetData())
	require.Nil(t, params.GetArea())
}

func TestResolveVariableIsBinHonorsStoredProcedureScope(t *testing.T) {
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnv(t, 104)
	defer prepareStmt.Close()
	require.NoError(t, ses.setUserDefinedVar("v1", "session-binary", "", true))
	require.NoError(t, ses.setUserDefinedVar("session_only", "session-binary", "", true))
	scopes := []map[string]interface{}{
		{"v1": int64(10)},
		{"v1": int64(20), "inner": int64(30)},
	}
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.VarScopeKey{}, &scopes)
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.InSp{}, true)

	value, err := ses.txnCompileCtx.ResolveVariable("V1", false, false)
	require.NoError(t, err)
	require.Equal(t, int64(20), value)
	isBin, err := ses.txnCompileCtx.ResolveVariableIsBin("V1", false, false)
	require.NoError(t, err)
	require.False(t, isBin)

	value, err = ses.txnCompileCtx.ResolveVariable("session_only", false, false)
	require.NoError(t, err)
	require.Equal(t, "session-binary", value)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("session_only", false, false)
	require.NoError(t, err)
	require.True(t, isBin)
}

func TestBuildExecuteUserParamsHonorsStoredProcedureScope(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 105)
	defer prepareStmt.Close()
	require.NoError(t, ses.setUserDefinedVar("local_shadow", "session-binary", "", true))
	require.NoError(t, ses.setUserDefinedVar("session_only", "session-binary", "", true))
	scopes := []map[string]interface{}{
		{"local_only": int64(10), "local_shadow": int64(20)},
	}
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.VarScopeKey{}, &scopes)
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.InSp{}, true)

	args := []*plan.Expr{
		{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "local_only"}}},
		{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "local_shadow"}}},
		{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "session_only"}}},
	}
	params, paramVals, paramIsBin, err := buildExecuteUserParams(cw.proc, args)
	require.NoError(t, err)
	defer params.Free(cw.proc.Mp())

	require.Equal(t, []bool{false, false, true}, paramIsBin)
	require.Equal(t, []any{
		plan2.ParamValue{Value: int64(10), IsBin: false},
		plan2.ParamValue{Value: int64(20), IsBin: false},
		plan2.ParamValue{Value: "session-binary", IsBin: true},
	}, paramVals)
	require.Equal(t, "10", params.GetStringAt(0))
	require.Equal(t, "20", params.GetStringAt(1))
	require.Equal(t, "session-binary", params.GetStringAt(2))
}

// A nil cached compile means the statement was rejected for prepare-time
// compile (e.g. AP query hitting ErrCantCompileForPrepare). Execute must not
// retry that doomed compile on every run; the cache stays nil and the regular
// compile path (isPrepare=false) takes over.
func TestInitExecuteStmtParamSkipsPrepareCompileWithoutCache(t *testing.T) {
	_, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 100)
	defer prepareStmt.Close()

	ret, err := cw.Compile(execCtx, nil)
	require.NoError(t, err)
	require.NotNil(t, ret)
	require.Nil(t, prepareStmt.compile)
}

// Without a schema change the cached compile must be reused as-is instead of
// being released and rebuilt on every execute: the per-execution recompilation
// regressed TPCC by 25%. Stale pipeline state is cleared by Compile.Reset
// (see Scope.resetForReuse) before the reused compile runs.
// See https://github.com/matrixorigin/matrixone/issues/25614.
func TestInitExecuteStmtParamReusesCachedCompileWhenNoSchemaChange(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()

	// The sentinel cached compile carries no plan; a recompilation would
	// replace it with a freshly built compile.
	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel

	retComp, retPlan, retStmt, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, sentinel, retComp)
	require.Same(t, sentinel, prepareStmt.compile)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
}

func TestInitExecuteStmtParamRebuildsWhenTempTableMappingChanges(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 102)
	defer prepareStmt.Close()

	oldColDefData := [][]byte{[]byte("old-int-column")}
	newColDefData := [][]byte{[]byte("new-varchar-column")}
	prepareStmt.ColDefData = oldColDefData
	execCtx.prepareColDef = oldColDefData
	w := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	w.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		return newColDefData, nil
	}

	oldPlan := prepareStmt.PreparePlan
	ses.AddTempTable("db1", "unrelated", "temp-unrelated")

	retComp, retPlan, retStmt, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotSame(t, oldPlan, prepareStmt.PreparePlan)
	require.Same(t, prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan, retPlan)
	require.NotNil(t, retStmt)
	require.Equal(t, ses.GetTempTableVersion(), prepareStmt.tempTableVersion)
	require.Equal(t, newColDefData, prepareStmt.ColDefData)
	require.Equal(t, newColDefData, execCtx.prepareColDef)
}

func TestInitExecuteStmtParamKeepsOldStateWhenColumnMetadataRefreshFails(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 103)
	defer prepareStmt.Close()

	oldPlan := prepareStmt.PreparePlan
	oldColDefData := [][]byte{[]byte("old-int-column")}
	prepareStmt.ColDefData = oldColDefData
	execCtx.prepareColDef = oldColDefData
	w := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	w.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		return nil, errors.New("column metadata refresh failed")
	}

	ses.AddTempTable("db1", "unrelated", "temp-unrelated")
	_, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.EqualError(t, err, "column metadata refresh failed")
	require.Same(t, oldPlan, prepareStmt.PreparePlan)
	require.Equal(t, oldColDefData, prepareStmt.ColDefData)
	require.Equal(t, oldColDefData, execCtx.prepareColDef)
	require.NotEqual(t, ses.GetTempTableVersion(), prepareStmt.tempTableVersion)
}

func TestInitExecuteStmtParamRebuildsPreparedPlanWhenSQLModePresenceChanges(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 106)
	defer prepareStmt.Close()

	originalPlan := prepareStmt.PreparePlan
	require.False(t, prepareStmt.NativeMode)

	execCtx.reqCtx = defines.AttachAccountId(execCtx.reqCtx, catalog.System_Account)
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "sql_mode", "MATRIXONE_NATIVE"))

	retComp, retPlan, retStmt, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
	require.True(t, prepareStmt.NativeMode)
	require.NotSame(t, originalPlan, prepareStmt.PreparePlan)
}

func TestInitExecuteStmtParamBypassesButRetainsCachedTopologyForExplicitSchedulingIntent(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 102)
	defer prepareStmt.Close()

	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel
	require.NoError(t, ses.SetSessionSysVar(context.Background(), queryMaxWorkers, int64(2)))

	retComp, retPlan, retStmt, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.Same(t, sentinel, prepareStmt.compile)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
}

func TestTxnComputationWrapperRunPanicStillReleases(t *testing.T) {
	var released bool
	mockComp := &mockCompile{
		runFunc: func(uint64) (*util2.RunResult, error) {
			panic("run panic")
		},
		getPlanFunc: func() *plan.Plan {
			return nil
		},
		releaseFunc: func() {
			released = true
		},
	}
	cwft := &TxnComputationWrapper{compile: mockComp}

	assert.PanicsWithValue(t, "run panic", func() {
		_, _ = cwft.Run(100)
	})
	assert.True(t, released)
	assert.Nil(t, cwft.compile)
}

func TestTxnComputationWrapperPreservesSchedulingTraceOnCompileError(t *testing.T) {
	const failureCategory = "candidate-discovery"
	stmt := &motrace.StatementInfo{
		RequestAt:     time.Now(),
		Status:        motrace.StatementStatusSuccess,
		StatementType: "Select",
		SqlSourceType: "external_sql",
	}
	assert.True(t, motrace.StatementInfoFilter(stmt))
	ses := &Session{}
	ses.SetTStmt(stmt)
	cwft := &TxnComputationWrapper{ses: ses}
	attempt := cwft.schedulingTrace.StartAttempt()
	cwft.schedulingTrace.RecordFailure(attempt, failureCategory, schedule.Worker{})

	cwft.recordSchedulingTraceOnCompileError(context.Background())
	if assert.NotNil(t, stmt.ExecPlan) {
		defer stmt.ExecPlan.Free()
		jsonBytes := stmt.ExecPlan.Marshal(context.Background())
		var payload struct {
			Scheduling schedule.Trace `json:"scheduling"`
		}
		assert.NoError(t, json.Unmarshal(jsonBytes, &payload))
		assert.Equal(t, failureCategory, payload.Scheduling.Attempts[0].Failures[0].Category)
		assert.Nil(t, payload.Scheduling.Attempts[0].Failures[0].Worker)
	}
	assert.True(t, stmt.ResponseAt.IsZero())
	assert.False(t, motrace.StatementInfoFilter(stmt))
}

func TestTxnComputationWrapperDoesNotPersistNormalLocalTraceOnCompileError(t *testing.T) {
	stmt := &motrace.StatementInfo{
		RequestAt:     time.Now(),
		Status:        motrace.StatementStatusSuccess,
		StatementType: "Select",
		SqlSourceType: "external_sql",
	}
	assert.True(t, motrace.StatementInfoFilter(stmt))
	ses := &Session{}
	ses.SetTStmt(stmt)
	cwft := &TxnComputationWrapper{ses: ses}
	attempt := cwft.schedulingTrace.StartAttempt()
	cwft.schedulingTrace.RecordQuery(attempt, schedule.QueryDecision{
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

	cwft.recordSchedulingTraceOnCompileError(context.Background())
	assert.Nil(t, stmt.ExecPlan)
	assert.True(t, motrace.StatementInfoFilter(stmt))
}
