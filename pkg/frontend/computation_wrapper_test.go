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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCompile struct {
	runFunc               func(uint64) (*util2.RunResult, error)
	getPlanFunc           func() *plan.Plan
	releaseFunc           func()
	planGenerationRebuilt bool
	planSnapshotTS        timestamp.Timestamp
	hasPlanSnapshotTS     bool
}

func TestResourceAttemptOwnerEligible(t *testing.T) {
	require.True(t, resourceAttemptOwnerEligible(&Session{}))
	require.False(t, resourceAttemptOwnerEligible(&backSession{}))
	derived := &Session{}
	derived.ReplaceDerivedStmt(true)
	require.False(t, resourceAttemptOwnerEligible(derived))
}

func (m *mockCompile) Run(ts uint64) (*util2.RunResult, error) { return m.runFunc(ts) }
func (m *mockCompile) GetPlan() *plan.Plan                     { return m.getPlanFunc() }
func (m *mockCompile) PlanGenerationRebuilt() bool             { return m.planGenerationRebuilt }
func (m *mockCompile) PlanSnapshotTS() (timestamp.Timestamp, bool) {
	return m.planSnapshotTS, m.hasPlanSnapshotTS
}
func (m *mockCompile) Release()              { m.releaseFunc() }
func (m *mockCompile) SetOriginSQL(s string) {}

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

func TestTxnComputationWrapperRunMarksInvalidPreparedPlanForRebuild(t *testing.T) {
	prepared := &PrepareStmt{}
	mockComp := &mockCompile{
		runFunc: func(uint64) (*util2.RunResult, error) {
			return &util2.RunResult{}, nil
		},
		getPlanFunc:           func() *plan.Plan { return &plan.Plan{} },
		releaseFunc:           func() {},
		planGenerationRebuilt: true,
	}
	cwft := &TxnComputationWrapper{
		compile:      mockComp,
		preparedStmt: prepared,
	}

	_, err := cwft.Run(100)
	require.NoError(t, err)
	require.True(t, prepared.needsRebuild)
	require.True(t, prepared.compileNeedsRebuild)
}

func TestCompleteCompileExecutionFromProductionRunnerKeepsTerminalOwnership(t *testing.T) {
	prepared := &PrepareStmt{}
	newPlan := &plan.Plan{}
	newTS := timestamp.Timestamp{PhysicalTime: 20}
	released := 0
	running := &mockCompile{
		runFunc:               func(uint64) (*util2.RunResult, error) { return &util2.RunResult{}, nil },
		getPlanFunc:           func() *plan.Plan { return newPlan },
		releaseFunc:           func() { released++ },
		planGenerationRebuilt: true,
		planSnapshotTS:        newTS,
		hasPlanSnapshotTS:     true,
	}
	cwft := &TxnComputationWrapper{preparedStmt: prepared}

	// Production runs the returned Compile directly, then invokes the terminal
	// generation hook from executeStmt/executeStmtInBack before their sole
	// Release. The hook must update frontend state without taking that owner.
	_, err := running.Run(0)
	require.NoError(t, err)
	cwft.completeCompileExecution(running, err)
	require.Same(t, newPlan, cwft.plan)
	require.Equal(t, newTS, cwft.planSnapshotTS)
	require.True(t, prepared.needsRebuild)
	require.True(t, prepared.compileNeedsRebuild)
	require.Zero(t, released)

	running.Release()
	require.Equal(t, 1, released)
}

func TestTxnComputationWrapperRunPublishesRebuiltSessionCachedPlan(t *testing.T) {
	ses := &Session{planCache: newPlanCache(1)}
	stmt := &trackedStatement{}
	oldPlan := &plan.Plan{}
	newPlan := &plan.Plan{}
	oldTS := timestamp.Timestamp{PhysicalTime: 10}
	newTS := timestamp.Timestamp{PhysicalTime: 20}
	ses.cachePlanWithSnapshots(
		"cached", []tree.Statement{stmt}, []*plan.Plan{oldPlan},
		[]timestamp.Timestamp{oldTS})

	released := 0
	cwft := &TxnComputationWrapper{
		stmt:                 stmt,
		stmtBorrowed:         true,
		plan:                 oldPlan,
		ses:                  ses,
		cachedPlanSQL:        "cached",
		cachedPlanGeneration: oldPlan,
		compile: &mockCompile{
			runFunc: func(uint64) (*util2.RunResult, error) {
				return &util2.RunResult{}, nil
			},
			getPlanFunc:           func() *plan.Plan { return newPlan },
			releaseFunc:           func() { released++ },
			planGenerationRebuilt: true,
			planSnapshotTS:        newTS,
			hasPlanSnapshotTS:     true,
		},
	}

	_, err := cwft.Run(100)
	require.NoError(t, err)
	require.Equal(t, 1, released)
	require.Same(t, newPlan, ses.planCache.get("cached").plans[0])
	require.Equal(t, newTS, ses.planCache.get("cached").planSnapshotTS[0])
	require.Zero(t, stmt.freed)

	cwft.Free()
	ses.cleanCache()
	require.Equal(t, 1, stmt.freed)
}

func TestTxnComputationWrapperRunLazilyInvalidatesFailedRebuild(t *testing.T) {
	ses := &Session{planCache: newPlanCache(1)}
	stmt := &trackedStatement{}
	oldPlan := &plan.Plan{}
	ses.cachePlanWithSnapshots(
		"cached", []tree.Statement{stmt}, []*plan.Plan{oldPlan},
		[]timestamp.Timestamp{{PhysicalTime: 10}})

	cwft := &TxnComputationWrapper{
		stmt:                 stmt,
		stmtBorrowed:         true,
		plan:                 oldPlan,
		ses:                  ses,
		cachedPlanSQL:        "cached",
		cachedPlanGeneration: oldPlan,
		compile: &mockCompile{
			runFunc:               func(uint64) (*util2.RunResult, error) { return nil, assert.AnError },
			getPlanFunc:           func() *plan.Plan { return oldPlan },
			releaseFunc:           func() {},
			planGenerationRebuilt: true,
		},
	}

	_, err := cwft.Run(100)
	require.ErrorIs(t, err, assert.AnError)
	require.False(t, ses.isCached("cached"))
	require.Zero(t, stmt.freed, "the running wrapper still borrows the cached AST")

	cwft.Free()
	require.Nil(t, ses.getCachedPlan("cached"))
	require.Equal(t, 1, stmt.freed)
}

func TestPrepareStmtInvalidatesCachedCompileWithoutDoubleRelease(t *testing.T) {
	proc := testutil.NewProcess(t)
	cached := compile.NewCompile("", "", "", "", "", nil, proc, nil, false, nil, time.Now())
	cached.SetIsPrepare(true)
	prepared := &PrepareStmt{compile: cached}

	invalidated := prepared.invalidateCachedCompile()
	require.Same(t, cached, invalidated)
	require.Nil(t, prepared.compile)
	require.True(t, prepared.needsRebuild)
	require.True(t, prepared.compileNeedsRebuild)

	// The execution wrapper remains the sole owner of the matching release.
	cached.Release()
}

// newPreparedExecuteEnv sets up a session holding a prepared "select 1" and a
// computation wrapper that executes it through the binary protocol, so tests
// can drive cw.Compile through initExecuteStmtParam.
func newPreparedExecuteEnv(t testing.TB, stmtID uint32) (*Session, *PrepareStmt, *TxnComputationWrapper, *ExecCtx) {
	return newPreparedExecuteEnvForSQL(t, stmtID, "select 1")
}

func newPreparedExecuteEnvForSQL(t testing.TB, stmtID uint32, sql string) (*Session, *PrepareStmt, *TxnComputationWrapper, *ExecCtx) {
	return newPreparedExecuteEnvForSQLWithCompilerContext(
		t, stmtID, sql, plan2.NewEmptyCompilerContext())
}

func newPreparedExecuteEnvForSQLWithCompilerContext(
	t testing.TB,
	stmtID uint32,
	sql string,
	compilerContext plan2.CompilerContext,
) (*Session, *PrepareStmt, *TxnComputationWrapper, *ExecCtx) {
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
	preparePlan, err := buildPlan(ctx, nil, compilerContext, prepareString)
	require.NoError(t, err)

	fixedIntegerParamPositions, hasPaginationParams, hasLagLeadParams :=
		preparedFixedIntegerParamPositions(preparePlan.GetDcl().GetPrepare().Plan)
	prepareStmt := &PrepareStmt{
		Name:                       stmtName,
		Sql:                        prepareString.Sql,
		PreparePlan:                preparePlan,
		PrepareStmt:                stmts[0],
		NativeMode:                 ses.sqlModeHasMatrixOneNative(),
		OnlyFullGroupBy:            ses.sqlModeHasOnlyFullGroupBy(),
		BoolSumAvg:                 ses.sqlModeHasEnableBoolSumAvg(),
		sqlModeFlagsSet:            true,
		getFromSendLongData:        make(map[int]struct{}),
		protocolVersion:            currentProtocolVersion(proc),
		directResultParamPositions: plan2.PreparedPlanDirectResultParamPositions(preparePlan.GetDcl().GetPrepare().Plan),
		fixedIntegerParamPositions: fixedIntegerParamPositions,
		hasPaginationParams:        hasPaginationParams,
		hasLagLeadParams:           hasLagLeadParams,
	}
	prepareStmt.refreshNumericPrefixConsumer(
		preparePlan.GetDcl().GetPrepare().Plan,
		len(preparePlan.GetDcl().GetPrepare().ParamTypes),
	)
	require.NoError(t, ses.SetPrepareStmt(ctx, stmtName, prepareStmt))

	cw := InitTxnComputationWrapper(ses, stmts[0], proc)
	cw.plan = preparePlan.GetDcl().GetPrepare().Plan
	cw.binaryPrepare = true
	cw.stmtBorrowed = true
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
	proc.SetResolveVariablePrepareParamKindFunc(ses.txnCompileCtx.ResolveVariablePrepareParamKind)
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
	value, err := ses.txnCompileCtx.ResolveVariable("missing", false, false)
	require.NoError(t, err)
	require.Nil(t, value)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("missing", false, false)
	require.NoError(t, err)
	require.False(t, isBin)
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

	_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	require.True(t, cw.proc.GetPrepareParamIsBin(0))
	require.False(t, cw.proc.GetPrepareParamIsBin(1))
	require.Equal(t, plan2.ParamValue{
		Value: "AB\x00\x00", IsBin: true, EnableNumericPrefix: true,
		SourceType: types.T_varbinary.ToType(), HasSourceType: true,
	}, cw.paramVals[0])
	require.Equal(t, plan2.ParamValue{
		Value: "text", IsBin: false, EnableNumericPrefix: true,
	}, cw.paramVals[1])

	params := cw.proc.GetPrepareParams()
	require.Equal(t, types.StringSourceSQLPrepare, params.GetStringSourceAt(0))
	require.Equal(t, types.StringSourceSQLPrepare, params.GetStringSourceAt(1))
	require.NoError(t, ses.SetUserDefinedVar("binary_param", "now-text", ""))
	_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	require.Zero(t, params.Length(), "the previous owned params must be released on successful replacement")
	require.Nil(t, params.GetData())
	require.False(t, cw.proc.GetPrepareParamIsBin(0))
	require.Equal(t, "now-text", cw.proc.GetPrepareParams().GetStringAt(0))
	require.Equal(t, types.StringSourceSQLPrepare, cw.proc.GetPrepareParams().GetStringSourceAt(0))

	current := cw.proc.GetPrepareParams()
	cw.proc.SetPrepareParams(vector.NewVec(types.T_text.ToType()))
	require.Zero(t, current.Length())
	require.Nil(t, current.GetData())
	require.False(t, cw.proc.GetPrepareParamIsBin(0), "binary metadata must not leak into the next execution")
	cw.proc.GetPrepareParams().Free(cw.proc.Mp())
	cw.proc.SetPrepareParams(nil)
}

func TestPreparedParamValuesPreservesNullProtocolProvenance(t *testing.T) {
	_, prepareStmt, cw, _ := newPreparedExecuteEnvForSQL(t, 112, "select ?")
	defer prepareStmt.Close()

	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, nil, true, cw.proc.Mp()))
	cw.proc.SetPrepareParamsWithMeta(
		params,
		[]bool{false},
		[]vector.PrepareParamKind{vector.PrepareParamDecimal},
	)
	defer func() {
		cw.proc.SetPrepareParams(nil)
		params.Free(cw.proc.Mp())
	}()

	values, err := preparedParamValues(cw.proc, []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0})
	require.NoError(t, err)
	require.Equal(t, []any{plan2.ParamValue{
		IsBinaryProtocol:    true,
		PrepareParamKind:    vector.PrepareParamDecimal,
		EnableNumericPrefix: true,
	}}, values)
}

func TestIssue27640InitExecuteStmtParamAcceptsODBCIntegerTextPagination(t *testing.T) {
	for index, test := range []struct {
		name       string
		sql        string
		values     []string
		mysqlTypes []defines.MysqlType
		pagination []int32
		prefix     []bool
	}{
		{name: "limit", sql: "select 1 limit ?", values: []string{"2"}},
		{name: "limit offset", sql: "select 1 limit ? offset ?", values: []string{"2", "1"}},
		{name: "offset", sql: "select 1 offset ?", values: []string{"1"}},
		{
			name: "having and limit", sql: "select sum(1) having sum(1) > ? limit ?",
			values: []string{"0", "1"},
			mysqlTypes: []defines.MysqlType{
				defines.MYSQL_TYPE_BLOB,
				defines.MYSQL_TYPE_STRING,
			},
			pagination: []int32{1},
			prefix:     []bool{false, false},
		},
		{
			name: "having and limit offset", sql: "select sum(1) having sum(1) > ? limit ? offset ?",
			values: []string{"0", "1", "0"},
			mysqlTypes: []defines.MysqlType{
				defines.MYSQL_TYPE_BLOB,
				defines.MYSQL_TYPE_STRING,
				defines.MYSQL_TYPE_STRING,
			},
			pagination: []int32{1, 2},
			prefix:     []bool{false, false, false},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
				t, uint32(113+index), test.sql)
			defer func() {
				cw.proc.SetPrepareParams(nil)
				prepareStmt.Close()
			}()
			if test.pagination != nil {
				require.Equal(t, test.pagination, plan2.PreparedPaginationParamPositions(
					prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan))
				require.Equal(t, test.pagination, prepareStmt.fixedIntegerParamPositions,
					"prepared-plan installation must cache fixed integer parameter positions")
			}

			// Connector/ODBC sends integer bindings as MYSQL_TYPE_STRING in
			// COM_STMT_EXECUTE, so reproduce that wire representation directly.
			prepareStmt.params = vector.NewVec(types.T_text.ToType())
			prepareStmt.ParamTypes = make([]byte, 0, len(test.values)*2)
			wantParamVals := make([]any, 0, len(test.values))
			for valueIndex, value := range test.values {
				require.NoError(t, vector.AppendBytes(
					prepareStmt.params, []byte(value), false, cw.proc.Mp()))
				mysqlType := defines.MYSQL_TYPE_STRING
				if valueIndex < len(test.mysqlTypes) {
					mysqlType = test.mysqlTypes[valueIndex]
				}
				prepareStmt.ParamTypes = append(prepareStmt.ParamTypes,
					byte(mysqlType), 0)
				enableNumericPrefix := true
				if valueIndex < len(test.prefix) {
					enableNumericPrefix = test.prefix[valueIndex]
				}
				wantParamVals = append(wantParamVals, plan2.ParamValue{
					Value: value, IsBinaryProtocol: true, EnableNumericPrefix: enableNumericPrefix,
				})
			}

			retComp, _, executionStmt, _, owned, err := initExecuteStmtParam(
				execCtx, ses, cw, nil, prepareStmt.Name)
			require.NoError(t, err)
			require.Nil(t, retComp, "pagination parameters must not reuse a value-filled compile")
			require.Equal(t, wantParamVals, cw.paramVals)
			if owned {
				executionStmt.Free()
			}
		})
	}
}

func TestInitExecuteStmtParamDirectResultSpecializationUsesBoundedCache(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 212, "select ? as result")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	require.Equal(t, []int32{0}, prepareStmt.directResultParamPositions)

	ordinaryPlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	resultExpr := func(queryPlan *plan.Plan) *plan.Expr {
		query := queryPlan.GetQuery()
		return query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
	}
	ordinaryCompile := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = ordinaryCompile
	ordinaryColDef := [][]byte{[]byte("prepare-time-text")}
	prepareStmt.ColDefData = ordinaryColDef

	var metadataTypes []plan.Type
	writer := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	writer.makeColumnDefDataFunc = func(_ context.Context, columns []*plan.ColDef) ([][]byte, error) {
		require.Len(t, columns, 1)
		metadataTypes = append(metadataTypes, columns[0].Typ)
		return [][]byte{[]byte(fmt.Sprintf("%d:%d:%d", columns[0].Typ.Id, columns[0].Typ.Width, columns[0].Typ.Scale))}, nil
	}

	install := func(value string, mysqlType defines.MysqlType, isNull bool) {
		t.Helper()
		cw.proc.SetPrepareParams(nil)
		if prepareStmt.params != nil {
			prepareStmt.params.Free(cw.proc.Mp())
		}
		prepareStmt.params = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(value), isNull, cw.proc.Mp()))
		prepareStmt.ParamTypes = []byte{byte(mysqlType), 0}
		execCtx.prepareColDef = ordinaryColDef
	}

	install("-42", defines.MYSQL_TYPE_LONGLONG, false)
	retComp, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Equal(t, types.StringSourceCOMStmt, prepareStmt.params.GetStringSourceAt(0))
	require.Nil(t, retComp)
	require.NotSame(t, ordinaryPlan, runtimePlan)
	require.Equal(t, int32(types.T_int64), resultExpr(runtimePlan).Typ.Id)
	require.Equal(t, int32(types.T_int64), metadataTypes[len(metadataTypes)-1].Id)
	require.NotEqual(t, ordinaryColDef, execCtx.prepareColDef)
	require.Equal(t, ordinaryColDef, prepareStmt.ColDefData, "execution metadata must not mutate PREPARE metadata")

	runtimeCompile := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	require.True(t, cw.installRuntimeCacheCandidate(runtimeCompile))
	install("-43", defines.MYSQL_TYPE_LONGLONG, false)
	retComp, reusedPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, runtimeCompile, retComp)
	require.Same(t, runtimePlan, reusedPlan)

	executor, err := colexec.NewExpressionExecutor(cw.proc, resultExpr(reusedPlan))
	require.NoError(t, err)
	defer executor.Free()
	input := batch.New(nil)
	input.SetRowCount(1)
	result, err := executor.Eval(cw.proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, types.T_int64, result.GetType().Oid)
	require.Equal(t, int64(-43), vector.GetFixedAtNoTypeCheck[int64](result, 0),
		"cached direct-result plan must read the current parameter value")

	decimalText := "-12345678901234567890.123456789"
	install(decimalText, defines.MYSQL_TYPE_NEWDECIMAL, false)
	retComp, decimalPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotSame(t, runtimePlan, decimalPlan)
	decimalType := resultExpr(decimalPlan).Typ
	require.Equal(t, int32(types.T_decimal128), decimalType.Id)
	require.Equal(t, int32(29), decimalType.Width)
	require.Equal(t, int32(9), decimalType.Scale)
	require.Equal(t, decimalType, metadataTypes[len(metadataTypes)-1])
	require.Same(t, runtimeCompile, prepareStmt.runtimeCompile,
		"a category miss must retain the preceding live cache until compile succeeds")

	decimalCompile := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	require.True(t, cw.installRuntimeCacheCandidate(decimalCompile))

	// NULL has no stable runtime result type. It must return to the immutable
	// prepare-time plan/metadata rather than leaking the preceding DECIMAL domain.
	install("", defines.MYSQL_TYPE_NULL, true)
	retComp, nullPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, ordinaryCompile, retComp)
	require.Same(t, ordinaryPlan, nullPlan)
	require.Equal(t, ordinaryColDef, execCtx.prepareColDef)
	require.Same(t, decimalCompile, prepareStmt.runtimeCompile,
		"an untyped NULL execution may leave the bounded numeric cache dormant")
}

func TestInitExecuteStmtParamDirectTextIgnoresNestedNumericMarker(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 213, "select ? as direct_value, abs(?) as nested_value")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	require.Equal(t, []int32{0}, prepareStmt.directResultParamPositions)

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte("text"), false, cw.proc.Mp()))
	require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte("42"), false, cw.proc.Mp()))
	prepareStmt.ParamTypes = []byte{
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
		byte(defines.MYSQL_TYPE_LONG), 0,
	}
	ordinaryPlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	prepareStmt.compile = compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())

	retComp, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp, "the nested ABS marker uses main's regular runtime specialization")
	require.NotSame(t, ordinaryPlan, runtimePlan)
	require.False(t, cw.runtimeDirectResultSpecialization,
		"the unrelated nested numeric marker must not expand direct-result admission")
	root := runtimePlan.GetQuery().Nodes[runtimePlan.GetQuery().Steps[len(runtimePlan.GetQuery().Steps)-1]]
	require.Equal(t, int32(types.T_text), root.ProjectList[0].Typ.Id,
		"the direct VAR_STRING result must retain TEXT metadata")
}

func TestInitExecuteStmtParamDirectNumericPreservesTextSibling(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 214, "select ? as direct_number, ? as direct_text")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	require.Equal(t, []int32{0, 1}, prepareStmt.directResultParamPositions)

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte("42"), false, cw.proc.Mp()))
	require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte("text"), false, cw.proc.Mp()))
	prepareStmt.ParamTypes = []byte{
		byte(defines.MYSQL_TYPE_LONGLONG), 0,
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
	}

	originalPlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	originalRoot := originalPlan.GetQuery().Nodes[originalPlan.GetQuery().Steps[len(originalPlan.GetQuery().Steps)-1]]
	require.Len(t, originalRoot.ProjectList, 2)
	originalTextType := originalRoot.ProjectList[1].Typ

	retComp, runtimePlan, _, _, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	runtimeRoot := runtimePlan.GetQuery().Nodes[runtimePlan.GetQuery().Steps[len(runtimePlan.GetQuery().Steps)-1]]
	require.Equal(t, int32(types.T_int64), runtimeRoot.ProjectList[0].Typ.Id)
	require.Equal(t, originalTextType, runtimeRoot.ProjectList[1].Typ,
		"a nonnumeric direct sibling must keep its prepare-time charset and type")
}

func TestInitExecuteStmtParamRestoresBooleanRuntimeType(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 105, "select ?")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()

	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte("1"), false, cw.proc.Mp()))
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_TINY), 0}
	prepareStmt.directResultParamPositions = []int32{0}
	prepareStmt.directResultParamPositionsSet = true

	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Len(t, cw.paramVals, 1)
	param, ok := cw.paramVals[0].(plan2.ParamValue)
	require.True(t, ok)
	require.Equal(t, vector.PrepareParamBoolean, param.PrepareParamKind)
	require.True(t, param.HasRuntimeType)
	require.Equal(t, types.T_bool.ToType(), param.RuntimeType)
}

func TestInitExecuteStmtParamValidatesCachedLagLeadOffsets(t *testing.T) {
	binaryParam := func(value string, mysqlType defines.MysqlType) func(
		*testing.T, *Session, *PrepareStmt, *TxnComputationWrapper,
	) *plan.Execute {
		return func(t *testing.T, _ *Session, prepareStmt *PrepareStmt, cw *TxnComputationWrapper) *plan.Execute {
			prepareStmt.params = vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(value), false, cw.proc.Mp()))
			prepareStmt.ParamTypes = []byte{byte(mysqlType), 0}
			return nil
		}
	}
	textBooleanParam := func(value bool) func(
		*testing.T, *Session, *PrepareStmt, *TxnComputationWrapper,
	) *plan.Execute {
		return func(t *testing.T, ses *Session, prepareStmt *PrepareStmt, _ *TxnComputationWrapper) *plan.Execute {
			require.NoError(t, ses.SetUserDefinedVar("offset_value", value, ""))
			return &plan.Execute{
				Name: prepareStmt.Name,
				Args: []*plan.Expr{{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "offset_value"}}}},
			}
		}
	}

	tests := []struct {
		name      string
		sql       string
		wantParam string
		wantError bool
		configure func(*testing.T, *Session, *PrepareStmt, *TxnComputationWrapper) *plan.Execute
	}{
		{name: "binary float", sql: "select lag(1, ?) over ()", wantError: true, configure: binaryParam("1", defines.MYSQL_TYPE_DOUBLE)},
		{name: "binary signed tiny zero lag", sql: "select lag(1, ?) over ()", wantParam: "0", configure: binaryParam("0", defines.MYSQL_TYPE_TINY)},
		{name: "binary signed tiny one lag", sql: "select lag(1, ?) over ()", wantParam: "1", configure: binaryParam("1", defines.MYSQL_TYPE_TINY)},
		{name: "binary signed tiny zero lead", sql: "select lead(1, ?) over ()", wantParam: "0", configure: binaryParam("0", defines.MYSQL_TYPE_TINY)},
		{name: "binary signed tiny one lead", sql: "select lead(1, ?) over ()", wantParam: "1", configure: binaryParam("1", defines.MYSQL_TYPE_TINY)},
		{name: "text boolean false lag", sql: "select lag(1, ?) over ()", wantParam: "0", configure: textBooleanParam(false)},
		{name: "text boolean true lag", sql: "select lag(1, ?) over ()", wantParam: "1", configure: textBooleanParam(true)},
		{name: "text boolean false lead", sql: "select lead(1, ?) over ()", wantParam: "0", configure: textBooleanParam(false)},
		{name: "text boolean true lead", sql: "select lead(1, ?) over ()", wantParam: "1", configure: textBooleanParam(true)},
		{name: "binary integer control", sql: "select lag(1, ?) over ()", wantParam: "1", configure: binaryParam("1", defines.MYSQL_TYPE_LONGLONG)},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, uint32(120+i), test.sql)
			defer func() {
				cw.proc.SetPrepareParams(nil)
				prepareStmt.Close()
			}()

			sentinel := compile.NewCompile(
				"", "", prepareStmt.Sql, "", "", nil,
				cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
			prepareStmt.compile = sentinel
			execPlan := test.configure(t, ses, prepareStmt, cw)

			retComp, _, executionStmt, _, owned, err := initExecuteStmtParam(
				execCtx, ses, cw, execPlan, prepareStmt.Name)
			if test.wantError {
				require.Error(t, err)
				moErr, ok := err.(*moerr.Error)
				require.True(t, ok)
				require.Equal(t, moerr.ER_WRONG_ARGUMENTS, moErr.MySQLCode())
				require.Same(t, sentinel, prepareStmt.compile)
				return
			}

			require.NoError(t, err)
			require.Same(t, sentinel, retComp)
			require.Equal(t, test.wantParam, cw.proc.GetPrepareParams().GetStringAt(0))
			if owned {
				executionStmt.Free()
			}
		})
	}
}

func TestBinaryProtocolPrepareParamKind(t *testing.T) {
	for _, test := range []struct {
		mysqlType  defines.MysqlType
		isUnsigned bool
		value      string
		want       vector.PrepareParamKind
	}{
		{defines.MYSQL_TYPE_TINY, false, "0", vector.PrepareParamBoolean},
		{defines.MYSQL_TYPE_TINY, false, "1", vector.PrepareParamBoolean},
		{defines.MYSQL_TYPE_TINY, true, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_TINY, false, "2", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_TINY, false, "-1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_SHORT, false, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_INT24, false, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_LONG, false, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_LONGLONG, false, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_YEAR, false, "2024", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_FLOAT, false, "1.5", vector.PrepareParamFloat},
		{defines.MYSQL_TYPE_DOUBLE, false, "1.5", vector.PrepareParamFloat},
		{defines.MYSQL_TYPE_DECIMAL, false, "1.5", vector.PrepareParamDecimal},
		{defines.MYSQL_TYPE_NEWDECIMAL, false, "1.5", vector.PrepareParamDecimal},
		{defines.MYSQL_TYPE_BIT, false, "1", vector.PrepareParamInteger},
		{defines.MYSQL_TYPE_VAR_STRING, false, "1", vector.PrepareParamNone},
	} {
		require.Equal(t, test.want,
			binaryProtocolPrepareParamKind(test.mysqlType, test.isUnsigned, []byte(test.value)),
			"type %v unsigned %t value %q", test.mysqlType, test.isUnsigned, test.value)
	}
}

func TestBinaryProtocolPrepareParamType(t *testing.T) {
	decimal, ok := binaryProtocolPrepareParamType(
		defines.MYSQL_TYPE_NEWDECIMAL,
		false,
		[]byte("-12345678901234567890.123456789"),
	)
	require.True(t, ok)
	require.Equal(t, types.T_decimal128, decimal.Oid)
	require.Equal(t, int32(29), decimal.Width)
	require.Equal(t, int32(9), decimal.Scale)
	exponentDecimal, ok := binaryProtocolPrepareParamType(
		defines.MYSQL_TYPE_NEWDECIMAL,
		false,
		[]byte("1e3"),
	)
	require.True(t, ok)
	require.Equal(t, types.T_decimal64, exponentDecimal.Oid)

	for _, test := range []struct {
		name       string
		mysqlType  defines.MysqlType
		isUnsigned bool
		want       types.T
	}{
		{name: "signed integer", mysqlType: defines.MYSQL_TYPE_LONG, want: types.T_int32},
		{name: "unsigned integer", mysqlType: defines.MYSQL_TYPE_LONGLONG, isUnsigned: true, want: types.T_uint64},
		{name: "double", mysqlType: defines.MYSQL_TYPE_DOUBLE, want: types.T_float64},
		{name: "string", mysqlType: defines.MYSQL_TYPE_VAR_STRING, want: types.T_text},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := binaryProtocolPrepareParamType(test.mysqlType, test.isUnsigned, []byte("5"))
			require.True(t, ok)
			require.Equal(t, test.want, got.Oid)
		})
	}

	for _, test := range []struct {
		value     string
		wantWidth int32
		wantScale int32
	}{
		{value: "123", wantWidth: 3},
		{value: "0.00", wantWidth: 1, wantScale: 0},
		{value: "0e-30", wantWidth: 1, wantScale: 0},
		{value: "1e-30", wantWidth: 30, wantScale: 30},
	} {
		got, ok := binaryProtocolPrepareParamType(
			defines.MYSQL_TYPE_NEWDECIMAL, false, []byte(test.value))
		require.True(t, ok, test.value)
		require.Equal(t, test.wantWidth, got.Width, test.value)
		require.Equal(t, test.wantScale, got.Scale, test.value)
	}

	decimal256, ok := binaryProtocolPrepareParamType(
		defines.MYSQL_TYPE_NEWDECIMAL, false, []byte(strings.Repeat("9", 65)))
	require.True(t, ok)
	require.Equal(t, types.T_decimal256, decimal256.Oid)
	require.Equal(t, int32(65), decimal256.Width)
	_, ok = binaryProtocolPrepareParamType(
		defines.MYSQL_TYPE_NEWDECIMAL, false, []byte(strings.Repeat("9", 77)))
	require.False(t, ok)

	_, ok = binaryProtocolPrepareParamType(defines.MYSQL_TYPE_NULL, false, nil)
	require.False(t, ok)
}

func TestBinaryProtocolRuntimeParamTypesDoesNotScanDecimalPayload(t *testing.T) {
	params := vector.NewVec(types.T_text.ToType())
	mp := mpool.MustNewZero()
	defer params.Free(mp)
	payload := append([]byte(strings.Repeat("0", 1<<20)), '1', '.', '0')
	require.NoError(t, vector.AppendBytes(params, payload, false, mp))
	paramTypes := []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}

	var runtimeTypes []types.Type
	allocs := testing.AllocsPerRun(20, func() {
		runtimeTypes = binaryProtocolRuntimeParamTypes(paramTypes, params)
	})
	require.Len(t, runtimeTypes, 1)
	require.True(t, runtimeTypes[0].IsNumeric())
	require.LessOrEqual(t, allocs, float64(1),
		"OID-only text-comparison admission must not allocate an input-sized DECIMAL string")
	require.Equal(t, payload, params.GetRawBytesAt(0), "category admission must not mutate packet provenance")
}

func TestRuntimeParamTypesContainText(t *testing.T) {
	require.False(t, runtimeParamTypesContainText(nil))
	require.False(t, runtimeParamTypesContainText([]types.Type{
		types.T_int64.ToType(), types.T_decimal128.ToType(), {},
	}))
	for _, oid := range []types.T{types.T_char, types.T_varchar, types.T_text} {
		require.True(t, runtimeParamTypesContainText([]types.Type{
			types.T_int64.ToType(), oid.ToType(),
		}), oid.String())
	}
}

func BenchmarkBinaryProtocolRuntimeParamTypesLargeDecimal(b *testing.B) {
	params := vector.NewVec(types.T_text.ToType())
	mp := mpool.MustNewZero()
	defer params.Free(mp)
	payload := append([]byte(strings.Repeat("0", 1<<20)), '1', '.', '0')
	require.NoError(b, vector.AppendBytes(params, payload, false, mp))
	paramTypes := []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		runtimeTypes := binaryProtocolRuntimeParamTypes(paramTypes, params)
		if len(runtimeTypes) != 1 || !runtimeTypes[0].IsNumeric() {
			b.Fatal("DECIMAL packet was not classified as numeric")
		}
	}
}

func TestPreparedParamValuesCarriesBothDecimalDomains(t *testing.T) {
	_, prepareStmt, cw, _ := newPreparedExecuteEnvForSQL(t, 214, "select ?")
	defer prepareStmt.Close()

	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("0e+77"), false, cw.proc.Mp()))
	cw.proc.SetPrepareParamsWithMeta(
		params, []bool{false}, []vector.PrepareParamKind{vector.PrepareParamDecimal})
	defer func() {
		cw.proc.SetPrepareParams(nil)
		params.Free(cw.proc.Mp())
	}()
	values, err := preparedParamValues(cw.proc, []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0})
	require.NoError(t, err)
	require.Len(t, values, 1)
	value := values[0].(plan2.ParamValue)
	require.Equal(t, types.New(types.T_decimal64, 1, 0), value.RuntimeType)
	require.True(t, value.HasRuntimeType)
	require.Equal(t, types.New(types.T_decimal64, 1, 0), value.DirectResultType)
	require.True(t, value.HasDirectResultType)
	require.Equal(t, "0", value.MaterializedValue)
	require.Equal(t, "0", cw.proc.GetPrepareParams().GetStringAt(0),
		"the restored typed ParamRef must execute against the bounded canonical lexeme")
}

func TestPreparedParamValuesBoundsInvalidDecimalError(t *testing.T) {
	_, prepareStmt, cw, _ := newPreparedExecuteEnvForSQL(t, 215, "select ?")
	defer prepareStmt.Close()

	payload := strings.Repeat("x", 1<<20)
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte(payload), false, cw.proc.Mp()))
	cw.proc.SetPrepareParamsWithMeta(
		params, []bool{false}, []vector.PrepareParamKind{vector.PrepareParamDecimal})
	defer func() {
		cw.proc.SetPrepareParams(nil)
		params.Free(cw.proc.Mp())
	}()

	_, err := preparedParamValues(cw.proc, []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0})
	require.Error(t, err)
	require.NotContains(t, err.Error(), payload[:128])
	require.Contains(t, err.Error(), "1048576 bytes")
	require.Less(t, len(err.Error()), 160)
}

func BenchmarkBinaryDirectResultDecimalLargeLexeme(b *testing.B) {
	value := strings.Repeat("0", 1<<20) + "1.0"
	paramTypes := []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}
	positions := []int32{0}
	paramVals := []any{plan2.ParamValue{Value: value}}
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	for b.Loop() {
		normalized, visible, canonical, hasVisible, ok := binaryProtocolPrepareParamDomains(
			defines.MYSQL_TYPE_NEWDECIMAL, false, value)
		if !ok || !hasVisible {
			b.Fatal("large valid DECIMAL lexeme rejected")
		}
		param := paramVals[0].(plan2.ParamValue)
		param.RuntimeType = normalized
		param.HasRuntimeType = true
		param.DirectResultType = visible
		param.HasDirectResultType = true
		param.MaterializedValue = canonical
		paramVals[0] = param
		if err := applyBinaryDirectResultDecimalTypes(
			context.Background(), paramVals, paramTypes, positions); err != nil {
			b.Fatal(err)
		}
	}
}

func TestApplyBinaryDirectResultDecimalTypesPreservesLexicalScale(t *testing.T) {
	values := []any{
		plan2.ParamValue{
			Value: "0.00", RuntimeType: types.New(types.T_decimal64, 1, 0), HasRuntimeType: true,
			DirectResultType: types.New(types.T_decimal64, 2, 2), HasDirectResultType: true,
		},
		plan2.ParamValue{
			Value: "9.00", RuntimeType: types.New(types.T_decimal64, 1, 0), HasRuntimeType: true,
			DirectResultType: types.New(types.T_decimal64, 3, 2), HasDirectResultType: true,
		},
	}
	err := applyBinaryDirectResultDecimalTypes(
		context.Background(), values,
		[]byte{
			byte(defines.MYSQL_TYPE_NEWDECIMAL), 0,
			byte(defines.MYSQL_TYPE_NEWDECIMAL), 0,
		},
		[]int32{0},
	)
	require.NoError(t, err)
	direct := values[0].(plan2.ParamValue)
	require.Equal(t, types.T_decimal64, direct.RuntimeType.Oid)
	require.Equal(t, int32(2), direct.RuntimeType.Width)
	require.Equal(t, int32(2), direct.RuntimeType.Scale)
	unrelated := values[1].(plan2.ParamValue)
	require.Equal(t, int32(1), unrelated.RuntimeType.Width)
	require.Zero(t, unrelated.RuntimeType.Scale)
}

func TestBinaryProtocolDecimalRebindPreservesExactAbsDomain(t *testing.T) {
	_, prepareStmt, cw, _ := newPreparedExecuteEnvForSQL(t, 113, "select abs(?)")
	defer prepareStmt.Close()

	value := "12345678901234567890123456789012345.6789"
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte(value), false, cw.proc.Mp()))
	defer func() {
		cw.proc.SetPrepareParams(nil)
		params.Free(cw.proc.Mp())
	}()
	cw.proc.SetPrepareParamsWithMeta(
		params,
		[]bool{false},
		[]vector.PrepareParamKind{vector.PrepareParamDecimal},
	)
	paramTypes := []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}
	values, err := preparedParamValues(cw.proc, paramTypes)
	require.NoError(t, err)
	require.Len(t, values, 1)
	param, ok := values[0].(plan2.ParamValue)
	require.True(t, ok)
	require.Equal(t, value, param.Value)
	require.Equal(t, vector.PrepareParamDecimal, param.PrepareParamKind)
	require.True(t, param.HasRuntimeType)
	require.Equal(t, types.T_decimal256, param.RuntimeType.Oid)
	require.Equal(t, int32(39), param.RuntimeType.Width)
	require.Equal(t, int32(4), param.RuntimeType.Scale)

	runtimePlan, specialized, err := plan2.FillValuesOfParamsInPlanWithPreparedNumericOverload(
		context.Background(), prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
	require.NoError(t, err)
	require.True(t, specialized)
	var abs *plan.Expr
	for _, node := range runtimePlan.GetQuery().Nodes {
		if node == nil {
			continue
		}
		for _, projection := range node.ProjectList {
			if projection.GetF() != nil && projection.GetF().Func.GetObjName() == "abs" {
				abs = projection
				break
			}
		}
		if abs != nil {
			break
		}
	}
	require.NotNil(t, abs)
	require.Equal(t, int32(types.T_decimal256), abs.Typ.Id)
	require.Equal(t, int32(types.T_decimal256), abs.GetF().Args[0].Typ.Id)
	require.Equal(t, "cast", abs.GetF().Args[0].GetF().Func.GetObjName())
	require.Equal(t, value, abs.GetF().Args[0].GetF().Args[0].GetLit().GetSval())
}

func TestInitExecuteStmtParamSpecializesBinaryRuntimePlan(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 110, "select ?")
	defer prepareStmt.Close()

	originalPlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(
		params,
		[]byte("-12345678901234567890.123456789"),
		false,
		cw.proc.Mp(),
	))
	prepareStmt.params = params
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}
	prepareStmt.directResultParamPositions = []int32{0}
	prepareStmt.directResultParamPositionsSet = true

	var resultColumns []*plan.ColDef
	writer := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	writer.makeColumnDefDataFunc = func(_ context.Context, columns []*plan.ColDef) ([][]byte, error) {
		resultColumns = columns
		return [][]byte{[]byte("runtime-decimal")}, nil
	}

	_, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.NotNil(t, runtimePlan)
	require.NotSame(t, originalPlan, runtimePlan)
	projectNode := runtimePlan.GetQuery().Nodes[runtimePlan.GetQuery().Steps[len(runtimePlan.GetQuery().Steps)-1]]
	require.Equal(t, int32(types.T_decimal128), projectNode.ProjectList[0].Typ.Id)
	require.Equal(t, int32(29), projectNode.ProjectList[0].Typ.Width)
	require.Equal(t, int32(9), projectNode.ProjectList[0].Typ.Scale)
	require.Len(t, resultColumns, 1)
	require.Equal(t, int32(types.T_decimal128), resultColumns[0].Typ.Id)
	require.Equal(t, [][]byte{[]byte("runtime-decimal")}, execCtx.prepareColDef)
	value, parseErr := types.ParseDecimal128("-12345678901234567890.123456789", 29, 9)
	require.NoError(t, parseErr)
	executor, execErr := colexec.NewExpressionExecutor(cw.proc, projectNode.ProjectList[0])
	require.NoError(t, execErr)
	defer executor.Free()
	input := batch.New(nil)
	input.SetRowCount(1)
	vec, evalErr := executor.Eval(cw.proc, []*batch.Batch{input}, nil)
	require.NoError(t, evalErr)
	require.Equal(t, types.T_decimal128, vec.GetType().Oid)
	require.Equal(t, value, vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, 0))
	originalProjectNode := originalPlan.GetQuery().Nodes[originalPlan.GetQuery().Steps[len(originalPlan.GetQuery().Steps)-1]]
	require.Equal(t, int32(types.T_text), originalProjectNode.ProjectList[0].Typ.Id)
}

func TestInitExecuteStmtParamKeepsDirectResultSpecializationAcrossNoOpPlanScan(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 114, "select ?, ? = ?")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()

	originalPlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	for _, value := range []string{"7", "same", "same"} {
		require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(value), false, cw.proc.Mp()))
	}
	prepareStmt.ParamTypes = []byte{
		byte(defines.MYSQL_TYPE_LONGLONG), 0,
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
		byte(defines.MYSQL_TYPE_VAR_STRING), 0,
	}
	prepareStmt.directResultParamPositions = []int32{0}
	prepareStmt.directResultParamPositionsSet = true
	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel

	retComp, runtimePlan, stmt, _, owned, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	if owned && stmt != nil {
		defer stmt.Free()
	}
	require.Nil(t, retComp, "the TEXT compile must not execute the INT64 direct-result plan")
	require.NotSame(t, originalPlan, runtimePlan)
	projectNode := runtimePlan.GetQuery().Nodes[runtimePlan.GetQuery().Steps[len(runtimePlan.GetQuery().Steps)-1]]
	require.Equal(t, int32(types.T_int64), projectNode.ProjectList[0].Typ.Id)
	require.Equal(t, int32(types.T_text), originalPlan.GetQuery().Nodes[originalPlan.GetQuery().Steps[len(originalPlan.GetQuery().Steps)-1]].ProjectList[0].Typ.Id)
}

func TestInitExecuteStmtParamSpecializesSQLExecuteCommonTypePlan(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 111, "select ?")
	defer prepareStmt.Close()
	decimalType := types.New(types.T_decimal64, 10, 2)
	decimalPeer, parseErr := types.ParseDecimal64("1.25", decimalType.Width, decimalType.Scale)
	require.NoError(t, parseErr)
	commonExpr, bindErr := plan2.BindFuncExprImplByPlanExpr(context.Background(), "coalesce", []*plan.Expr{
		{
			Typ:  plan.Type{Id: int32(types.T_text)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		},
		{
			Typ: plan.Type{Id: int32(decimalType.Oid), Width: decimalType.Width, Scale: decimalType.Scale},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: int64(decimalPeer)}},
			}},
		},
	})
	require.NoError(t, bindErr)
	manualPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Headings: []string{"coalesce"},
		Nodes: []*plan.Node{{
			NodeType:    plan.Node_VALUE_SCAN,
			ProjectList: []*plan.Expr{commonExpr},
		}},
	}}, IsPrepare: true}
	prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan = manualPlan
	prepareStmt.directResultParamPositions = plan2.PreparedPlanDirectResultParamPositions(manualPlan)
	cw.plan = manualPlan
	prepareStmt.refreshNumericPrefixConsumer(manualPlan, 1)
	require.True(t, prepareStmt.numericPrefixConsumer)

	require.NoError(t, ses.SetUserDefinedVar("numeric_text", "12.5tail", ""))
	execCtx.input.isBinaryProtExecute = false
	execPlan := &plan.Execute{
		Name: prepareStmt.Name,
		Args: []*plan.Expr{{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "numeric_text"}}}},
	}
	originalPlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel
	writer := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	writer.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		return nil, errors.New("SQL EXECUTE must not build binary-protocol column definitions")
	}

	retComp, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	require.Nil(t, retComp,
		"the prepare-time compile cannot execute a common-type-specialized plan: plan=%s params=%#v",
		runtimePlan.String(), cw.paramVals)
	require.Same(t, sentinel, prepareStmt.compile, "specialization must not replace the cached compile")
	require.NotSame(t, originalPlan, runtimePlan)
	require.Len(t, cw.paramVals, 1)
	paramValue, ok := cw.paramVals[0].(plan2.ParamValue)
	require.True(t, ok)
	require.False(t, paramValue.IsBinaryProtocol)
	require.True(t, paramValue.EnableNumericPrefix)
	require.True(t, paramValue.RetainParamRef)
	require.Nil(t, prepareStmt.runtimePlan, "candidate plan must remain outside the live cache")
	require.Empty(t, prepareStmt.runtimeSpecializationKey)
	require.Same(t, runtimePlan, cw.runtimeCachePlan)

	// Complete the first execution's compile installation, then prove that an
	// equivalent SQL EXECUTE category reuses both bounded cache entries instead
	// of deep-copying and recompiling the plan again.
	runtimeCompile := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	require.True(t, cw.installRuntimeCacheCandidate(runtimeCompile))
	require.Same(t, runtimePlan, prepareStmt.runtimePlan)
	require.NotEmpty(t, prepareStmt.runtimeSpecializationKey)
	require.NoError(t, ses.SetUserDefinedVar("numeric_text", "12.50tail", ""))
	retComp, secondRuntimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, execPlan, "")
	require.NoError(t, err)
	require.Same(t, runtimeCompile, retComp)
	require.Same(t, runtimePlan, secondRuntimePlan)

	projectNode := secondRuntimePlan.GetQuery().Nodes[secondRuntimePlan.GetQuery().Steps[len(secondRuntimePlan.GetQuery().Steps)-1]]
	project := projectNode.ProjectList[0]
	require.True(t, types.T(project.Typ.Id).IsDecimal(), project.String())
	requiresV26, scanErr := plan.RequiresMORPCVersion30NumericPrefix(project)
	require.NoError(t, scanErr)
	require.True(t, requiresV26, project.String())

	executor, execErr := colexec.NewExpressionExecutor(cw.proc, project)
	require.NoError(t, execErr)
	defer executor.Free()
	input := batch.New(nil)
	input.SetRowCount(1)
	vec, evalErr := executor.Eval(cw.proc, []*batch.Batch{input}, nil)
	require.NoError(t, evalErr)
	require.Equal(t, types.T_decimal64, vec.GetType().Oid)
	want, parseErr := types.ParseDecimal64("12.50", project.Typ.Width, project.Typ.Scale)
	require.NoError(t, parseErr)
	require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, 0))

	originalProjectNode := originalPlan.GetQuery().Nodes[originalPlan.GetQuery().Steps[len(originalPlan.GetQuery().Steps)-1]]
	requiresV26, scanErr = plan.RequiresMORPCVersion30NumericPrefix(originalProjectNode.ProjectList[0])
	require.NoError(t, scanErr)
	require.False(t, requiresV26)
}

func TestSpecializePreparedExecutionPlanSkipsIneligibleSQLPlan(t *testing.T) {
	ctx := context.Background()
	floatType := types.T_float32.ToType()
	predicate, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		{
			Typ:  plan.Type{Id: int32(floatType.Oid)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
		},
		{
			Typ:  plan.Type{Id: int32(types.T_text)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		},
	})
	require.NoError(t, err)
	original := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeType:   plan.Node_VALUE_SCAN,
			FilterList: []*plan.Expr{predicate},
		}},
	}}, IsPrepare: true}

	runtimePlan, specialized, applied, err := specializePreparedExecutionPlan(ctx, original, []any{
		plan2.ParamValue{
			Value: "1.2345678", PrepareParamKind: vector.PrepareParamDecimal, EnableNumericPrefix: true,
		},
	}, false, false, false, false, nil, false, false)
	require.NoError(t, err)
	require.False(t, specialized)
	require.False(t, applied)
	require.Same(t, original, runtimePlan, "ineligible SQL EXECUTE must not deep-copy the cached plan")
}

func TestPreparedRuntimeTextComparisonTypesSkipsNonStringParams(t *testing.T) {
	require.Nil(t, preparedRuntimeTextComparisonTypes([]any{
		plan2.ParamValue{Value: "1.5", HasSourceType: true, SourceType: types.T_decimal128.ToType()},
		plan2.ParamValue{Value: int64(1), HasRuntimeType: true, RuntimeType: types.T_int64.ToType(), IsBinaryProtocol: true},
	}))
	require.NotNil(t, preparedRuntimeTextComparisonTypes([]any{
		plan2.ParamValue{Value: "text", HasSourceType: true, SourceType: types.T_varchar.ToType()},
	}))
}

func TestSpecializePreparedExecutionPlanAppliesBitTextComparison(t *testing.T) {
	ctx := context.Background()
	bitType := plan.Type{Id: int32(types.T_bit), Width: 64}
	predicate, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		{
			Typ: bitType,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 0,
			}},
		},
		{
			Typ:  plan.Type{Id: int32(types.T_text)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		},
	})
	require.NoError(t, err)
	original := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeType:   plan.Node_VALUE_SCAN,
			FilterList: []*plan.Expr{predicate},
		}},
	}}, IsPrepare: true}

	runtimePlan, specialized, applied, err := specializePreparedExecutionPlan(ctx, original, []any{
		plan2.ParamValue{
			Value:            "9007199254740993",
			RuntimeType:      types.T_text.ToType(),
			HasRuntimeType:   true,
			IsBinaryProtocol: true,
		},
	}, true, false, false, false, nil, false, false)
	require.NoError(t, err)
	require.True(t, specialized)
	require.True(t, applied)
	comparison := runtimePlan.GetQuery().Nodes[0].FilterList[0]
	require.Equal(t, int32(types.T_bit), comparison.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(types.T_bit), comparison.GetF().Args[1].Typ.Id)
}

func TestPreparedPlanHasNumericPrefixConsumerCachesOnlyStaticDecimalContexts(t *testing.T) {
	ctx := context.Background()
	makePlan := func(peerType types.T) *plan.Plan {
		predicate, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
			{Typ: plan.Type{Id: int32(peerType)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
			{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
		})
		require.NoError(t, err)
		return &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
			StmtType: plan.Query_SELECT, Steps: []int32{0}, Nodes: []*plan.Node{{
				NodeType: plan.Node_VALUE_SCAN, FilterList: []*plan.Expr{predicate},
			}},
		}}, IsPrepare: true}
	}

	integerPlan := makePlan(types.T_int64)
	floatPlan := makePlan(types.T_float64)
	require.True(t, preparedPlanHasNumericPrefixConsumer(integerPlan, 1),
		"an exact-integer peer must admit a potential runtime DECIMAL packet")
	require.True(t, preparedPlanHasNumericPrefixConsumer(makePlan(types.T_decimal128), 1),
		"a static DECIMAL peer is a cached numeric-prefix consumer")
	require.False(t, preparedPlanHasNumericPrefixConsumer(floatPlan, 1),
		"an approximate FLOAT peer must remain outside numeric-prefix specialization")

	prepareStmt := &PrepareStmt{}
	prepareStmt.refreshNumericPrefixConsumer(integerPlan, 1)
	require.Same(t, integerPlan, prepareStmt.numericPrefixConsumerPlan)
	require.True(t, prepareStmt.numericPrefixConsumer)

	// Parameter count is part of the immutable prepared-plan generation. A
	// repeated execution must trust the cached decision instead of walking the
	// plan again.
	prepareStmt.refreshNumericPrefixConsumer(integerPlan, 0)
	require.True(t, prepareStmt.numericPrefixConsumer)

	prepareStmt.refreshNumericPrefixConsumer(floatPlan, 1)
	require.Same(t, floatPlan, prepareStmt.numericPrefixConsumerPlan)
	require.False(t, prepareStmt.numericPrefixConsumer,
		"a replacement plan must not inherit the previous generation's capability")

	prepareStmt.numericPrefixConsumer = true
	prepareStmt.refreshNumericPrefixConsumer(nil, 0)
	require.Nil(t, prepareStmt.numericPrefixConsumerPlan)
	require.False(t, prepareStmt.numericPrefixConsumer,
		"an absent plan must clear stale specialization capability")
}

func TestBinaryDecimalIntegerConsumerSpecializesAndReusesSemanticCategory(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 206, "select ?")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()

	predicate, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "=", []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
		{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
	})
	require.NoError(t, err)
	manualPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_SELECT, Steps: []int32{0}, Nodes: []*plan.Node{{
			NodeType: plan.Node_VALUE_SCAN, FilterList: []*plan.Expr{predicate},
		}},
	}}, IsPrepare: true}
	prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan = manualPlan
	prepareStmt.directResultParamPositions = plan2.PreparedPlanDirectResultParamPositions(manualPlan)
	prepareStmt.refreshNumericPrefixConsumer(manualPlan, 1)
	require.True(t, prepareStmt.numericPrefixConsumer)

	install := func(value string) *vector.Vector {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte(value), false, cw.proc.Mp()))
		prepareStmt.params = params
		prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}
		return params
	}
	firstParams := install("9.0")
	retComp, firstPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, manualPlan, prepareStmt.numericPrefixConsumerPlan)
	require.True(t, prepareStmt.numericPrefixConsumer)
	require.Nil(t, retComp)
	require.NotSame(t, manualPlan, firstPlan)
	require.Empty(t, prepareStmt.runtimeSpecializationKey)
	require.Nil(t, prepareStmt.runtimePlan)
	require.Same(t, firstPlan, cw.runtimeCachePlan)
	require.NotNil(t, cw.paramVals)

	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	require.True(t, cw.installRuntimeCacheCandidate(sentinel))
	cw.proc.SetPrepareParams(nil)
	secondParams := install("8.0")
	firstParams.Free(cw.proc.Mp())
	retComp, secondPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, sentinel, retComp)
	require.Same(t, firstPlan, secondPlan)
	require.Same(t, secondParams, cw.proc.GetPrepareParams())

	oldKey := prepareStmt.runtimeSpecializationKey
	cw.proc.SetPrepareParams(nil)
	thirdParams := install("99.0")
	secondParams.Free(cw.proc.Mp())
	retComp, thirdPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotSame(t, firstPlan, thirdPlan)
	require.Equal(t, oldKey, prepareStmt.runtimeSpecializationKey,
		"a category miss must retain the preceding live key until compile succeeds")
	require.Same(t, firstPlan, prepareStmt.runtimePlan)
	require.Same(t, sentinel, prepareStmt.runtimeCompile)
	require.NotEqual(t, oldKey, cw.runtimeCacheKey)
	require.Same(t, thirdPlan, cw.runtimeCachePlan)

	replacement := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	require.True(t, cw.completeRuntimeCacheCandidate(replacement, nil))
	// Evicting the previous semantic-category compile must not clear the
	// parameter vector borrowed by the execution that installs its replacement.
	require.Same(t, thirdParams, cw.proc.GetPrepareParams())
	require.Same(t, thirdPlan, prepareStmt.runtimePlan)
	require.Same(t, replacement, prepareStmt.runtimeCompile)
	require.NotEqual(t, oldKey, prepareStmt.runtimeSpecializationKey)

	cw.proc.SetPrepareParams(nil)
	prepareStmt.clearRuntimeSpecializationCache()
	textParams := install("9.0")
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_VAR_STRING), 0}
	thirdParams.Free(cw.proc.Mp())
	retComp, textPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotSame(t, manualPlan, textPlan)
	require.Same(t, textParams, cw.proc.GetPrepareParams())
}

func TestPreparedNumericOverloadSpecializationReusesRuntimeCategory(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 209, "select abs(?)")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	prepareStmt.numericOverloadParamPositions = plan2.PreparedPlanNumericFallbackParamPositions(preparePlan)
	require.Equal(t, []int32{0}, prepareStmt.numericOverloadParamPositions)

	install := func(value string, mysqlType defines.MysqlType) *vector.Vector {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte(value), false, cw.proc.Mp()))
		prepareStmt.params = params
		prepareStmt.ParamTypes = []byte{byte(mysqlType), 0}
		return params
	}

	firstParams := install("-9007199254740993", defines.MYSQL_TYPE_LONGLONG)
	retComp, firstPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotSame(t, preparePlan, firstPlan)
	require.Same(t, firstPlan, cw.runtimeCachePlan)

	runtimeCompile := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	require.True(t, cw.installRuntimeCacheCandidate(runtimeCompile))

	cw.proc.SetPrepareParams(nil)
	secondParams := install("-7", defines.MYSQL_TYPE_LONGLONG)
	firstParams.Free(cw.proc.Mp())
	retComp, secondPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, runtimeCompile, retComp,
		"a repeated INT64 execution must reuse the specialized compile")
	require.Same(t, firstPlan, secondPlan,
		"a repeated INT64 execution must reuse the specialized plan")

	cw.proc.SetPrepareParams(nil)
	floatParams := install("-1.5", defines.MYSQL_TYPE_DOUBLE)
	secondParams.Free(cw.proc.Mp())
	retComp, floatPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotSame(t, firstPlan, floatPlan,
		"changing the runtime numeric category must build a new bounded variant")
	cw.proc.SetPrepareParams(nil)
	floatParams.Free(cw.proc.Mp())
}

func TestPreparedExplicitDoubleAbsReusesOriginalCachedCompile(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 210, "select abs(cast(? as double))")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	prepareStmt.numericOverloadParamPositions = plan2.PreparedPlanNumericFallbackParamPositions(preparePlan)
	require.Empty(t, prepareStmt.numericOverloadParamPositions,
		"the parser-produced explicit cast must not be a deferred overload")

	cachedCompile := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = cachedCompile

	install := func(value string, mysqlType defines.MysqlType) *vector.Vector {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte(value), false, cw.proc.Mp()))
		prepareStmt.params = params
		prepareStmt.ParamTypes = []byte{byte(mysqlType), 0}
		return params
	}

	integerParams := install("-9007199254740993", defines.MYSQL_TYPE_LONGLONG)
	retComp, firstPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, cachedCompile, retComp)
	require.Same(t, preparePlan, firstPlan)
	require.Nil(t, cw.runtimeCachePlan)
	require.Nil(t, prepareStmt.runtimePlan)

	cw.proc.SetPrepareParams(nil)
	floatParams := install("-1.5", defines.MYSQL_TYPE_DOUBLE)
	integerParams.Free(cw.proc.Mp())
	retComp, secondPlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, cachedCompile, retComp,
		"changing packet category under an explicit DOUBLE cast must reuse the original compile")
	require.Same(t, preparePlan, secondPlan)
	require.Nil(t, cw.runtimeCachePlan)
	require.Nil(t, prepareStmt.runtimePlan)
	cw.proc.SetPrepareParams(nil)
	floatParams.Free(cw.proc.Mp())
}

func TestPreparedArithmeticDMLReusesStableRuntimeCategory(t *testing.T) {
	optimizer := plan2.NewMockOptimizer(false)
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQLWithCompilerContext(
		t,
		216,
		"update nation set n_regionkey = n_regionkey + ? where n_nationkey = ?",
		optimizer.CurrentContext(),
	)
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	require.True(t, plan2.PreparedPlanNeedsRuntimeSpecialization(preparePlan),
		"the arithmetic UPDATE must exercise the TPCC runtime-specialization path")

	install := func(values []string, nulls []bool, mysqlTypes []defines.MysqlType) {
		require.Len(t, values, len(mysqlTypes))
		require.Len(t, nulls, len(values))
		oldParams := prepareStmt.params
		if oldParams != nil {
			if cw.proc.GetPrepareParams() == oldParams {
				cw.proc.SetPrepareParams(nil)
			}
			oldParams.Free(cw.proc.Mp())
		}
		params := vector.NewVec(types.T_text.ToType())
		paramTypes := make([]byte, 0, len(mysqlTypes)*2)
		for i, value := range values {
			require.NoError(t, vector.AppendBytes(params, []byte(value), nulls[i], cw.proc.Mp()))
			paramTypes = append(paramTypes, byte(mysqlTypes[i]), 0)
		}
		prepareStmt.params = params
		prepareStmt.ParamTypes = paramTypes
	}
	freeStmt := func(stmt tree.Statement, owned bool) {
		if owned && stmt != nil {
			stmt.Free()
		}
	}

	install(
		[]string{"1", "7"},
		[]bool{false, false},
		[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_LONGLONG},
	)
	retComp, runtimePlan, stmt, _, owned, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	freeStmt(stmt, owned)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotSame(t, preparePlan, runtimePlan)
	require.Same(t, runtimePlan, cw.runtimeCachePlan,
		"the first stable runtime category must stage one reusable plan")

	runtimeCompile := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	require.True(t, cw.installRuntimeCacheCandidate(runtimeCompile))
	positions := make(map[int32]struct{})
	require.NoError(t, plan.VisitExpressionsInOwner(runtimePlan, func(expr *plan.Expr) error {
		return plan.VisitExprTree(expr, func(candidate *plan.Expr) error {
			if param := candidate.GetP(); param != nil {
				positions[param.Pos] = struct{}{}
			}
			return nil
		})
	}))
	require.Contains(t, positions, int32(0))
	require.Contains(t, positions, int32(1))

	install(
		[]string{"2", "8"},
		[]bool{false, false},
		[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_LONGLONG},
	)
	retComp, secondPlan, stmt, _, owned, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	freeStmt(stmt, owned)
	require.NoError(t, err)
	require.Same(t, runtimeCompile, retComp,
		"same-domain values must reuse the specialized compile")
	require.Same(t, runtimePlan, secondPlan,
		"same-domain values must not deep-copy the plan again")
	require.Equal(t, "2", cw.proc.GetPrepareParams().GetStringAt(0))
	require.Equal(t, "8", cw.proc.GetPrepareParams().GetStringAt(1))

	install(
		[]string{"1.5", "8"},
		[]bool{false, false},
		[]defines.MysqlType{defines.MYSQL_TYPE_DOUBLE, defines.MYSQL_TYPE_LONGLONG},
	)
	retComp, floatPlan, stmt, _, owned, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	freeStmt(stmt, owned)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotSame(t, runtimePlan, floatPlan,
		"a runtime-domain switch must not reuse the integer plan")
	require.Same(t, floatPlan, cw.runtimeCachePlan)
	require.Same(t, runtimeCompile, prepareStmt.runtimeCompile,
		"the old category remains live until the replacement compile succeeds")
	floatCompile := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	require.True(t, cw.installRuntimeCacheCandidate(floatCompile))

	install(
		[]string{"4", "8"},
		[]bool{false, false},
		[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_LONGLONG},
	)
	retComp, integerPlan, stmt, _, owned, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	freeStmt(stmt, owned)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotSame(t, floatPlan, integerPlan,
		"the bounded cache must miss when execution returns to the integer domain")
	require.Same(t, integerPlan, cw.runtimeCachePlan)
	require.Same(t, floatCompile, prepareStmt.runtimeCompile)
	integerCompile := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	require.True(t, cw.installRuntimeCacheCandidate(integerCompile))

	install(
		[]string{"", "8"},
		[]bool{true, false},
		[]defines.MysqlType{defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_LONGLONG},
	)
	retComp, _, stmt, _, owned, err = initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	freeStmt(stmt, owned)
	require.NoError(t, err)
	require.NotSame(t, integerCompile, retComp,
		"NULL has no stable runtime category and must not reuse the integer compile")
	require.Same(t, integerCompile, prepareStmt.runtimeCompile,
		"a non-cacheable execution must not evict the last valid category")
	require.Nil(t, cw.runtimeCachePlan)
}

func BenchmarkInitExecuteStmtParamRepeatedTPCCArithmeticUpdate(b *testing.B) {
	optimizer := plan2.NewMockOptimizer(false)
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQLWithCompilerContext(
		b,
		217,
		"update nation set n_regionkey = n_regionkey + ? where n_nationkey = ?",
		optimizer.CurrentContext(),
	)
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	for _, value := range []string{"1", "7"} {
		require.NoError(b, vector.AppendBytes(
			prepareStmt.params, []byte(value), false, cw.proc.Mp()))
	}
	prepareStmt.ParamTypes = []byte{
		byte(defines.MYSQL_TYPE_LONGLONG), 0,
		byte(defines.MYSQL_TYPE_LONGLONG), 0,
	}
	_, runtimePlan, stmt, _, owned, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(b, err)
	if owned && stmt != nil {
		stmt.Free()
	}
	cacheCandidate := cw.runtimeCachePlan != nil
	var runtimeCompile *compile.Compile
	if cacheCandidate {
		runtimeCompile = compile.NewCompile(
			"", "", prepareStmt.Sql, "", "", nil,
			cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
		require.True(b, cw.installRuntimeCacheCandidate(runtimeCompile))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		retComp, currentPlan, currentStmt, _, currentOwned, runErr := initExecuteStmtParam(
			execCtx, ses, cw, nil, prepareStmt.Name)
		if runErr != nil {
			b.Fatal(runErr)
		}
		if cacheCandidate && (retComp != runtimeCompile || currentPlan != runtimePlan) {
			b.Fatalf("runtime cache miss: comp=%p plan=%p", retComp, currentPlan)
		}
		if currentOwned && currentStmt != nil {
			currentStmt.Free()
		}
	}
}

func BenchmarkInitExecuteStmtParamRepeatedNonConsumerDecimal(b *testing.B) {
	optimizer := plan2.NewMockOptimizer(false)
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQLWithCompilerContext(
		b, 220, "update nation set n_name = ? where n_nationkey = 1", optimizer.CurrentContext())
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	require.NoError(b, vector.AppendBytes(
		prepareStmt.params, []byte("9.0"), false, cw.proc.Mp()))
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}
	preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	require.False(b, preparedPlanHasNumericPrefixConsumer(preparePlan, 1))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, currentPlan, currentStmt, _, owned, err := initExecuteStmtParam(
			execCtx, ses, cw, nil, prepareStmt.Name)
		if err != nil || currentPlan != preparePlan {
			b.Fatalf("unexpected execution result: plan=%p want=%p err=%v",
				currentPlan, preparePlan, err)
		}
		if owned && currentStmt != nil {
			currentStmt.Free()
		}
	}
}

func TestPreparedRuntimeCacheSupportsMixedAndStringCategories(t *testing.T) {
	decimal := func(value string) plan2.ParamValue {
		return plan2.ParamValue{Value: value, PrepareParamKind: vector.PrepareParamDecimal,
			RuntimeType: types.New(types.T_decimal64, 2, 1), HasRuntimeType: true}
	}
	integer := func(value string) plan2.ParamValue {
		return plan2.ParamValue{Value: value, PrepareParamKind: vector.PrepareParamInteger,
			RuntimeType: types.T_int64.ToType(), HasRuntimeType: true}
	}
	mixedA := []any{decimal("9.0"), integer("1")}
	mixedB := []any{decimal("8.0"), integer("2")}
	require.True(t, preparedRuntimeCacheSupports(mixedA))
	require.Equal(t, preparedRuntimeSemanticKey(mixedA), preparedRuntimeSemanticKey(mixedB))

	textA := []any{plan2.ParamValue{
		Value: "9.0", PrepareParamKind: vector.PrepareParamDecimal,
		RuntimeType: types.T_text.ToType(), HasRuntimeType: true,
	}}
	textB := []any{plan2.ParamValue{
		Value: "8.0", PrepareParamKind: vector.PrepareParamDecimal,
		RuntimeType: types.T_text.ToType(), HasRuntimeType: true,
	}}
	require.True(t, preparedRuntimeCacheSupports(textA))
	require.Equal(t, preparedRuntimeSemanticKey(textA), preparedRuntimeSemanticKey(textB))
	require.False(t, preparedRuntimeCacheSupports([]any{plan2.ParamValue{}}))
}

func TestPreparedRuntimeSemanticKeyKeepsValueAndSQLSourceDomains(t *testing.T) {
	integer := func(value string) []any {
		return []any{plan2.ParamValue{
			Value: value, PrepareParamKind: vector.PrepareParamInteger,
			SourceType: types.T_int64.ToType(), HasSourceType: true,
		}}
	}
	require.NotEqual(t, preparedRuntimeSemanticKey(integer("200")), preparedRuntimeSemanticKey(integer("10")),
		"SQL source metadata must not replace the value-derived comparison domain")

	decimal := func(value string, width, scale int32) []any {
		return []any{plan2.ParamValue{
			Value: value, PrepareParamKind: vector.PrepareParamDecimal,
			SourceType: types.New(types.T_decimal128, width, scale), HasSourceType: true,
		}}
	}
	require.Equal(t,
		preparedRuntimeSemanticKey(decimal("2.5", 20, 5)),
		preparedRuntimeSemanticKey(decimal("3.5", 20, 5)),
		"values in one SQL arithmetic domain should reuse the specialized plan")
	require.NotEqual(t,
		preparedRuntimeSemanticKey(decimal("2.5", 20, 5)),
		preparedRuntimeSemanticKey(decimal("2.5", 30, 8)),
		"a different SQL source domain must not reuse stale arithmetic metadata")
}

func TestPreparedDirectResultSemanticKeyPreservesDecimalMetadataDomain(t *testing.T) {
	decimal := func(value string, width, scale int32) []any {
		return []any{plan2.ParamValue{
			Value: value, PrepareParamKind: vector.PrepareParamDecimal,
			RuntimeType: types.New(types.T_decimal64, width, scale), HasRuntimeType: true,
		}}
	}
	positions := []int32{0}
	fixedScale := preparedDirectResultSemanticKey(decimal("9.0", 2, 1), positions)
	sameMetadataDomain := preparedDirectResultSemanticKey(decimal("8.0", 2, 1), positions)
	trailingZeroScale := preparedDirectResultSemanticKey(decimal("9.00", 3, 2), positions)

	require.Equal(t, fixedScale, sameMetadataDomain)
	require.NotEqual(t, fixedScale, trailingZeroScale,
		"direct DECIMAL scale is visible metadata and must participate in the cache category")
}

func TestRuntimeSpecializationReplacementCommitsOnlyAfterCompileSuccess(t *testing.T) {
	_, prepareStmt, cw, _ := newPreparedExecuteEnvForSQL(t, 208, "select ?")
	defer prepareStmt.Close()

	oldPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT}}}
	oldCompile := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.installRuntimeSpecializationCache("old", oldPlan, oldCompile)
	require.Nil(t, prepareStmt.installRuntimeSpecializationCache("old", oldPlan, oldCompile),
		"reinstalling the live compile must not retire it")

	failedPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT}}}
	cw.runtimeCacheTarget = prepareStmt
	cw.runtimeCacheKey = "failed"
	cw.runtimeCachePlan = failedPlan
	require.False(t, cw.completeRuntimeCacheCandidate(nil, assert.AnError))
	require.Equal(t, "old", prepareStmt.runtimeSpecializationKey)
	require.Same(t, oldPlan, prepareStmt.runtimePlan)
	require.Same(t, oldCompile, prepareStmt.runtimeCompile)
	require.Nil(t, cw.runtimeCacheTarget)
	require.Nil(t, cw.runtimeCachePlan)

	newPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT}}}
	newCompile := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	newMessageBoard := cw.proc.GetMessageBoard()
	require.NotNil(t, newMessageBoard)
	cw.runtimeCacheTarget = prepareStmt
	cw.runtimeCacheKey = "new"
	cw.runtimeCachePlan = newPlan
	require.True(t, cw.completeRuntimeCacheCandidate(newCompile, nil))
	require.Equal(t, "new", prepareStmt.runtimeSpecializationKey)
	require.Same(t, newPlan, prepareStmt.runtimePlan)
	require.Same(t, newCompile, prepareStmt.runtimeCompile)
	require.Nil(t, cw.runtimeCacheTarget)
	require.Nil(t, cw.runtimeCachePlan)
	require.Same(t, newMessageBoard, cw.proc.GetMessageBoard(),
		"publishing the replacement must not release the old compile into the shared Process")
	require.Len(t, cw.runtimeCacheRetiredCompiles, 1)
	require.Same(t, oldCompile, cw.runtimeCacheRetiredCompiles[0].compile)

	cw.releaseRuntimeCacheRetiredCompiles()
	require.Empty(t, cw.runtimeCacheRetiredCompiles)
	require.Nil(t, cw.proc.GetMessageBoard(),
		"the displaced compile is released only after the candidate statement finishes")

	prepareStmt.clearRuntimeSpecializationCache()
	require.Empty(t, prepareStmt.runtimeSpecializationKey)
	require.Nil(t, prepareStmt.runtimePlan)
	require.Nil(t, prepareStmt.runtimeCompile)
}

func BenchmarkInitExecuteStmtParamRepeatedDecimalSemanticCategoryNoPagination(b *testing.B) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(b, 207, "select ?")
	defer func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	}()
	predicate, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "=", []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
		{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
	})
	require.NoError(b, err)
	manualPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_SELECT, Steps: []int32{0}, Nodes: []*plan.Node{{
			NodeType: plan.Node_VALUE_SCAN, FilterList: []*plan.Expr{predicate},
		}},
	}}, IsPrepare: true}
	prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan = manualPlan
	prepareStmt.directResultParamPositions = plan2.PreparedPlanDirectResultParamPositions(manualPlan)
	prepareStmt.refreshNumericPrefixConsumer(manualPlan, 1)
	prepareStmt.refreshFixedIntegerParamPositions(manualPlan)
	require.True(b, prepareStmt.numericPrefixConsumer)
	require.Empty(b, prepareStmt.fixedIntegerParamPositions,
		"the no-pagination hot path must reuse empty fixed-position metadata")
	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	require.NoError(b, vector.AppendBytes(prepareStmt.params, []byte("9.0"), false, cw.proc.Mp()))
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}
	_, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(b, err)
	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	sentinel.SetIsPrepare(true)
	require.True(b, cw.installRuntimeCacheCandidate(sentinel))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		retComp, currentPlan, stmt, _, owned, runErr := initExecuteStmtParam(
			execCtx, ses, cw, nil, prepareStmt.Name)
		if runErr != nil || retComp != sentinel || currentPlan != runtimePlan {
			b.Fatalf("runtime cache miss: comp=%p plan=%p err=%v", retComp, currentPlan, runErr)
		}
		if owned && stmt != nil {
			stmt.Free()
		}
	}
}

func TestSQLVariablePrepareParamKind(t *testing.T) {
	for _, test := range []struct {
		oid  types.T
		want vector.PrepareParamKind
	}{
		{types.T_bool, vector.PrepareParamBoolean},
		{types.T_bit, vector.PrepareParamInteger},
		{types.T_int64, vector.PrepareParamInteger},
		{types.T_uint64, vector.PrepareParamInteger},
		{types.T_year, vector.PrepareParamInteger},
		{types.T_float64, vector.PrepareParamFloat},
		{types.T_decimal128, vector.PrepareParamDecimal},
		{types.T_varchar, vector.PrepareParamNone},
	} {
		require.Equal(t, test.want, prepareParamKindFromType(test.oid), "type %v", test.oid)
	}
	require.Equal(t, vector.PrepareParamBoolean, prepareParamKindFromValue(true))
	require.Equal(t, vector.PrepareParamInteger, prepareParamKindFromValue(uint64(5)))
	require.Equal(t, vector.PrepareParamFloat, prepareParamKindFromValue(float64(5)))
	require.Equal(t, vector.PrepareParamNone, prepareParamKindFromValue("5"))
}

func TestTransparentPrepareParamKind(t *testing.T) {
	ses, prepareStmt, cw, _ := newPreparedExecuteEnvForSQL(t, 109, "select ?")
	defer prepareStmt.Close()

	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("5"), false, cw.proc.Mp()))
	defer params.Free(cw.proc.Mp())
	cw.proc.SetPrepareParamsWithMeta(
		params, nil, []vector.PrepareParamKind{vector.PrepareParamFloat})
	defer cw.proc.SetPrepareParams(nil)

	kind, err := transparentPrepareParamKind(
		&tree.ParenExpr{Expr: tree.NewParamExpr(1)}, ses)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamFloat, kind)

	require.NoError(t, ses.setUserDefinedVarWithKind(
		"decimal_value", "5.9", "", false, vector.PrepareParamDecimal))
	kind, err = transparentPrepareParamKind(
		tree.NewVarExpr("decimal_value", false, false, nil), ses)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamDecimal, kind)

	kind, err = transparentPrepareParamKind(tree.NewParamExpr(2), ses)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamNone, kind)
}

func TestPreparedSetExpressionParamsAfterInit(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 108, "set @prepared_set_value = ? + 1")
	defer prepareStmt.Close()

	install := func(value string) *vector.Vector {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, []byte(value), false, cw.proc.Mp()))
		prepareStmt.params = params

		_, _, stmt, _, _, err := initExecuteStmtParam(
			execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		require.IsType(t, &tree.SetVar{}, stmt)
		require.Equal(t, value, cw.proc.GetPrepareParams().GetStringAt(0))
		return params
	}

	first := install("41")
	second := install("9")
	first.Free(cw.proc.Mp())
	require.Equal(t, "9", cw.proc.GetPrepareParams().GetStringAt(0))
	require.Same(t, second, prepareStmt.params)

	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_NEWDECIMAL), 0}
	wide := strings.Repeat("9", 65)
	third := install(wide)
	second.Free(cw.proc.Mp())
	require.Same(t, third, prepareStmt.params)
	require.Len(t, cw.paramVals, 1)
	param := cw.paramVals[0].(plan2.ParamValue)
	require.Equal(t, types.T_decimal256, param.RuntimeType.Oid)
	require.Equal(t, int32(65), param.RuntimeType.Width)

	overflow := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(overflow, []byte(strings.Repeat("9", 77)), false, cw.proc.Mp()))
	prepareStmt.params = overflow
	third.Free(cw.proc.Mp())
	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.ErrorContains(t, err, "exceeds DECIMAL(76)")
}

func TestPreparedAnalyzeSkipsEngineCompile(t *testing.T) {
	_, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 109, "select 1")
	defer prepareStmt.Close()

	prepareStmt.PrepareStmt.Free()
	prepareStmt.PrepareStmt = tree.NewAnalyzeStmt(nil)
	innerPlan := &plan.Plan{IsPrepare: true}
	prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan = innerPlan
	cw.plan = innerPlan

	compiled, err := cw.Compile(execCtx, nil)
	require.NoError(t, err)
	require.Nil(t, compiled)
	require.Nil(t, cw.compile)
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
	params, _, _, _, _, err := buildExecuteUserParams(cw.proc, execPlan.Args, nil)
	require.ErrorIs(t, err, assert.AnError)
	require.Zero(t, params.Length())
	require.Nil(t, params.GetData())
	require.Nil(t, params.GetArea())
}

func TestInitExecuteStmtParamKeepsConcreteTypeOnlyForJSONComparison(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 117, "select json_extract('18446744073709551615', '$') = ?")
	defer prepareStmt.Close()

	prepareStmt.jsonComparisonParamPositions = plan2.PreparedJSONComparisonParamPositions(
		prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan)
	require.Equal(t, []int32{0}, prepareStmt.jsonComparisonParamPositions)
	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(
		prepareStmt.params, []byte("9223372036854775807"), false, cw.proc.Mp()))
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0}

	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Equal(t, types.T_int64, cw.proc.GetPrepareParamType(0))
	require.Equal(t, vector.PrepareParamInteger, cw.proc.GetPrepareParamKind(0))

	require.NoError(t, vector.SetStringAt(
		prepareStmt.params, 0, "16777216", cw.proc.Mp()))
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_FLOAT), 0}
	_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Equal(t, types.T_float32, cw.proc.GetPrepareParamType(0))
	require.Equal(t, vector.PrepareParamFloat, cw.proc.GetPrepareParamKind(0))
}

func TestBuildExecuteUserParamsPreservesBoundConcreteTypes(t *testing.T) {
	ses, prepareStmt, cw, _ := newPreparedExecuteEnv(t, 118)
	defer prepareStmt.Close()

	tests := []struct {
		name  string
		typ   types.T
		value any
	}{
		{name: "int8", typ: types.T_int8, value: int8(1)},
		{name: "int16", typ: types.T_int16, value: int16(1)},
		{name: "int32", typ: types.T_int32, value: int32(1)},
		{name: "int64", typ: types.T_int64, value: int64(1)},
		{name: "uint8", typ: types.T_uint8, value: uint8(1)},
		{name: "uint16", typ: types.T_uint16, value: uint16(1)},
		{name: "uint32", typ: types.T_uint32, value: uint32(1)},
		{name: "uint64", typ: types.T_uint64, value: uint64(1)},
		{name: "float32", typ: types.T_float32, value: float32(1)},
		{name: "typed null", typ: types.T_int8, value: nil},
	}
	args := make([]*plan.Expr, 0, len(tests))
	typedPositions := make([]int32, 0, len(tests))
	wantTypes := make([]types.T, 0, len(tests))
	for i, test := range tests {
		require.NoError(t, ses.setUserDefinedVarWithType(
			test.name, test.value, "", false, plan.Type{Id: int32(test.typ)}))
		args = append(args, &plan.Expr{
			Typ:  plan.Type{Id: int32(test.typ)},
			Expr: &plan.Expr_V{V: &plan.VarRef{Name: test.name}},
		})
		typedPositions = append(typedPositions, int32(i))
		wantTypes = append(wantTypes, test.typ)
	}

	params, _, _, paramKinds, paramTypes, err := buildExecuteUserParams(
		cw.proc, args, typedPositions)
	require.NoError(t, err)
	defer params.Free(cw.proc.Mp())
	require.Equal(t, wantTypes, paramTypes)
	for i, test := range tests {
		wantKind := vector.PrepareParamInteger
		if test.typ == types.T_float32 {
			wantKind = vector.PrepareParamFloat
		}
		require.Equal(t, wantKind, paramKinds[i], test.name)
	}
	require.True(t, params.IsNull(uint64(len(tests)-1)))
}

func TestBuildExecuteUserParamsRejectsBoundTypeKindMismatch(t *testing.T) {
	ses, prepareStmt, cw, _ := newPreparedExecuteEnv(t, 119)
	defer prepareStmt.Close()
	require.NoError(t, ses.setUserDefinedVarWithKind(
		"mismatched", int8(1), "", false, vector.PrepareParamFloat))

	params, _, _, _, _, err := buildExecuteUserParams(cw.proc, []*plan.Expr{{
		Typ:  plan.Type{Id: int32(types.T_int8)},
		Expr: &plan.Expr_V{V: &plan.VarRef{Name: "mismatched"}},
	}}, []int32{0})
	require.ErrorContains(t, err, "EXECUTE parameter type TINYINT does not match kind")
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
		{"v1": int64(10), "declared_only_outer": "5.0"},
		{
			"v1":                  int64(20),
			"inner":               int64(30),
			"decimal_value":       "5.00",
			"year_value":          "2024",
			"string_value":        "5",
			"declared_only_outer": "5.0",
		},
	}
	typeScopes := []map[string]plan.Type{
		{"declared_only_outer": {Id: int32(types.T_decimal64)}},
		{
			"v1":            {Id: int32(types.T_int64)},
			"decimal_value": {Id: int32(types.T_decimal64)},
			"year_value":    {Id: int32(types.T_year)},
			"string_value":  {Id: int32(types.T_varchar)},
		},
	}
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.VarScopeKey{}, &scopes)
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.VarScopeTypeKey{}, &typeScopes)
	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, defines.InSp{}, true)

	value, err := ses.txnCompileCtx.ResolveVariable("V1", false, false)
	require.NoError(t, err)
	require.Equal(t, int64(20), value)
	isBin, err := ses.txnCompileCtx.ResolveVariableIsBin("V1", false, false)
	require.NoError(t, err)
	require.False(t, isBin)
	kind, err := ses.txnCompileCtx.ResolveVariablePrepareParamKind("V1", false, false)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamInteger, kind)

	for _, test := range []struct {
		name     string
		expected vector.PrepareParamKind
	}{
		{name: "decimal_value", expected: vector.PrepareParamDecimal},
		{name: "year_value", expected: vector.PrepareParamInteger},
		{name: "string_value", expected: vector.PrepareParamNone},
		{name: "declared_only_outer", expected: vector.PrepareParamNone},
	} {
		kind, err = ses.txnCompileCtx.ResolveVariablePrepareParamKind(test.name, false, false)
		require.NoError(t, err)
		require.Equal(t, test.expected, kind, test.name)
	}

	value, err = ses.txnCompileCtx.ResolveVariable("session_only", false, false)
	require.NoError(t, err)
	require.Equal(t, "session-binary", value)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("session_only", false, false)
	require.NoError(t, err)
	require.True(t, isBin)
	kind, err = ses.txnCompileCtx.ResolveVariablePrepareParamKind("session_only", false, false)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamNone, kind)

	value, err = ses.txnCompileCtx.ResolveVariable("missing_user_var", false, false)
	require.NoError(t, err)
	require.Nil(t, value)
	isBin, err = ses.txnCompileCtx.ResolveVariableIsBin("missing_user_var", false, false)
	require.NoError(t, err)
	require.False(t, isBin)
	kind, err = ses.txnCompileCtx.ResolveVariablePrepareParamKind("missing_user_var", false, false)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamNone, kind)
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
	params, paramVals, paramIsBin, paramKinds, paramTypes, err := buildExecuteUserParams(
		cw.proc, args, []int32{0, 1, 2})
	require.NoError(t, err)
	defer params.Free(cw.proc.Mp())

	require.Equal(t, []bool{false, false, true}, paramIsBin)
	require.Equal(t, []vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamInteger,
		vector.PrepareParamNone,
	}, paramKinds)
	require.Equal(t, []types.T{types.T_int64, types.T_int64, types.T_any}, paramTypes)
	require.Equal(t, []any{
		plan2.ParamValue{
			Value: int64(10), IsBin: false, PrepareParamKind: vector.PrepareParamInteger, EnableNumericPrefix: true,
		},
		plan2.ParamValue{
			Value: int64(20), IsBin: false, PrepareParamKind: vector.PrepareParamInteger, EnableNumericPrefix: true,
		},
		plan2.ParamValue{
			Value: "session-binary", IsBin: true, EnableNumericPrefix: true,
			SourceType: types.T_varbinary.ToType(), HasSourceType: true,
		},
	}, paramVals)
	require.Equal(t, "10", params.GetStringAt(0))
	require.Equal(t, "20", params.GetStringAt(1))
	require.Equal(t, "session-binary", params.GetStringAt(2))
}

func TestBuildExecuteUserParamsRetainsExecuteArgumentSourceType(t *testing.T) {
	ses, prepareStmt, cw, _ := newPreparedExecuteEnv(t, 106)
	defer prepareStmt.Close()

	decimalType := plan.Type{Id: int32(types.T_decimal128), Width: 12, Scale: 3}
	require.NoError(t, ses.setUserDefinedVarWithTypeAndKind(
		"runtime_decimal", "2.500", "", false, decimalType, vector.PrepareParamDecimal))
	require.NoError(t, ses.SetUserDefinedVar("runtime_text", "2.500", ""))
	binaryTextType := plan.Type{
		Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetBinary),
	}
	require.NoError(t, ses.setUserDefinedVarWithType(
		"runtime_binary", "12.5tail", "", false, binaryTextType))

	args := []*plan.Expr{
		{
			Typ:  decimalType,
			Expr: &plan.Expr_V{V: &plan.VarRef{Name: "runtime_decimal"}},
		},
		{
			Expr: &plan.Expr_V{V: &plan.VarRef{Name: "runtime_text"}},
		},
		{
			Typ:  binaryTextType,
			Expr: &plan.Expr_V{V: &plan.VarRef{Name: "runtime_binary"}},
		},
	}
	params, paramVals, _, _, _, err := buildExecuteUserParams(cw.proc, args, nil)
	require.NoError(t, err)
	defer params.Free(cw.proc.Mp())

	require.Equal(t, "2.500", params.GetStringAt(0))
	decimalParam, ok := paramVals[0].(plan2.ParamValue)
	require.True(t, ok)
	require.True(t, decimalParam.HasSourceType)
	require.Equal(t, types.New(types.T_decimal128, 12, 3), decimalParam.SourceType)
	require.Equal(t, vector.PrepareParamDecimal, decimalParam.PrepareParamKind)

	textParam, ok := paramVals[1].(plan2.ParamValue)
	require.True(t, ok)
	require.False(t, textParam.HasSourceType,
		"an unresolved execute argument must keep the existing text fallback")

	binaryParam, ok := paramVals[2].(plan2.ParamValue)
	require.True(t, ok)
	require.True(t, binaryParam.HasSourceType)
	require.Equal(t, types.NewWithCharset(types.T_varbinary, 8, 0, types.CharsetBinary), binaryParam.SourceType)
}

// A nil cached compile means the statement was rejected for prepare-time
// compile (e.g. AP query hitting ErrCantCompileForPrepare). Execute must not
// retry that doomed compile on every run; the cache stays nil and the regular
// compile path (isPrepare=false) takes over.
func TestPreparedDDLNeedsCatalogRefresh(t *testing.T) {
	testCases := []struct {
		sql      string
		expected bool
	}{
		{sql: "create pitr p for account range 1 'd'", expected: true},
		{sql: "create database db", expected: false},
		{sql: "create database sub from pub publication p", expected: true},
		{sql: "drop database db", expected: true},
		{sql: "create table dst clone src", expected: true},
		{sql: "truncate table t", expected: false},
	}
	for _, testCase := range testCases {
		statements, err := mysql.Parse(context.Background(), testCase.sql, 1)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		require.Equal(t, testCase.expected, preparedDDLNeedsCatalogRefresh(statements[0]))
		statements[0].Free()
	}
}

func TestPrepareSchemaAccountID(t *testing.T) {
	require.Equal(t, uint32(7), prepareSchemaAccountID(7, &plan.ObjectRef{SchemaName: "db", ObjName: "t"}))
	require.Equal(t, uint32(11), prepareSchemaAccountID(7, &plan.ObjectRef{
		SchemaName: "db", ObjName: "t", PubInfo: &plan.PubInfo{TenantId: 11},
	}))
	require.Equal(t, uint32(sysAccountID), prepareSchemaAccountID(7, &plan.ObjectRef{
		SchemaName: catalog.MO_SYSTEM, ObjName: catalog.MO_STATEMENT,
	}))
}

func TestPreparedSubscriptionSchemaChanged(t *testing.T) {
	expected := &plan.ObjectRef{
		Server: 4, Db: 2, Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
	}
	resolve := func(
		ref *plan.ObjectRef,
		def *plan.TableDef,
		err error,
	) preparedSchemaResolver {
		return func(databaseName, tableName string, _ *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
			require.Equal(t, "sub", databaseName)
			require.Equal(t, "src", tableName)
			return ref, def, err
		}
	}
	stableRef := &plan.ObjectRef{
		Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
	}
	stableDef := &plan.TableDef{DbId: 2, TblId: 3, Version: 4}
	stableResolver := resolve(stableRef, stableDef, nil)

	changed, err := preparedSubscriptionSchemaChanged(stableResolver, expected)
	require.NoError(t, err)
	require.False(t, changed)

	changed, err = preparedSubscriptionSchemaChanged(stableResolver, &plan.ObjectRef{
		SubscriptionName: "sub",
		ObjName:          "src",
	})
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = preparedSubscriptionSchemaChanged(resolve(nil, nil, nil), expected)
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = preparedSubscriptionSchemaChanged(resolve(
		&plan.ObjectRef{
			Obj: 3, SchemaName: "publisher_db", ObjName: "src",
			SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 12},
		},
		stableDef,
		nil,
	), expected)
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = preparedSubscriptionSchemaChanged(resolve(
		stableRef,
		&plan.TableDef{DbId: 2, TblId: 3, Version: 5},
		nil,
	), expected)
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = preparedSubscriptionSchemaChanged(resolve(
		nil,
		nil,
		assert.AnError,
	), expected)
	require.ErrorIs(t, err, assert.AnError)
	require.False(t, changed)
}

func TestPreparedSubscriptionSchemaChangedUsesLogicalName(t *testing.T) {
	expected := &plan.ObjectRef{
		Server: 4, Db: 2, Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
	}
	changed, err := preparedSubscriptionSchemaChanged(func(
		databaseName, tableName string, _ *plan.Snapshot,
	) (*plan.ObjectRef, *plan.TableDef, error) {
		require.Equal(t, "sub", databaseName)
		require.Equal(t, "src", tableName)
		return &plan.ObjectRef{
				Obj: 3, SchemaName: "publisher_db", ObjName: "src",
				SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
			},
			&plan.TableDef{DbId: 2, TblId: 3, Version: 4}, nil
	}, expected)
	require.NoError(t, err)
	require.False(t, changed)
}

func TestPreparedSubscriptionSchemaChangedUsesDependencySnapshot(t *testing.T) {
	snapshot := &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 7},
	}
	expected := &plan.ObjectRef{
		Server: 4, Db: 2, Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
		Snapshot: snapshot,
	}
	calls := 0
	changed, err := preparedSubscriptionSchemaChanged(func(
		databaseName, tableName string, gotSnapshot *plan.Snapshot,
	) (*plan.ObjectRef, *plan.TableDef, error) {
		calls++
		if calls == 1 {
			require.Nil(t, gotSnapshot)
		} else {
			require.Equal(t, snapshot, gotSnapshot)
		}
		return &plan.ObjectRef{
				Obj: 3, SchemaName: "publisher_db", ObjName: "src",
				SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
			},
			&plan.TableDef{DbId: 2, TblId: 3, Version: 4}, nil
	}, expected)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, 2, calls)
}

func TestPreparedSubscriptionsNeedValidation(t *testing.T) {
	require.False(t, preparedSubscriptionsNeedValidation(
		timestamp.Timestamp{PhysicalTime: 100},
		timestamp.Timestamp{PhysicalTime: 100},
		timestamp.Timestamp{},
	))
	require.True(t, preparedSubscriptionsNeedValidation(
		timestamp.Timestamp{PhysicalTime: 200},
		timestamp.Timestamp{PhysicalTime: 100},
		timestamp.Timestamp{},
	))
	require.False(t, preparedSubscriptionsNeedValidation(
		timestamp.Timestamp{PhysicalTime: 200},
		timestamp.Timestamp{PhysicalTime: 100},
		timestamp.Timestamp{PhysicalTime: 200},
	))
}

func TestPreparedNamedSnapshotsNeedValidation(t *testing.T) {
	namedSnapshot := &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42},
		ExtraInfo: &plan.SnapshotExtraInfo{
			Name: "snap",
		},
	}
	unnamedSnapshot := &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42},
	}
	metadataTS := timestamp.Timestamp{PhysicalTime: 200}
	prepareTS := timestamp.Timestamp{PhysicalTime: 100}

	require.True(t, preparedNamedSnapshotsNeedValidation(
		[]*plan.ObjectRef{{Snapshot: namedSnapshot}},
		metadataTS,
		prepareTS,
		timestamp.Timestamp{},
	))
	require.False(t, preparedNamedSnapshotsNeedValidation(
		[]*plan.ObjectRef{{Snapshot: unnamedSnapshot}},
		metadataTS,
		prepareTS,
		timestamp.Timestamp{},
	))
	require.False(t, preparedNamedSnapshotsNeedValidation(
		[]*plan.ObjectRef{{Snapshot: namedSnapshot}},
		metadataTS,
		prepareTS,
		metadataTS,
	))
}

func TestCurrentTxnSnapshotTS(t *testing.T) {
	ses, prepareStmt, _, _ := newPreparedExecuteEnv(t, 100)
	defer prepareStmt.Close()

	snapshot := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().SnapshotTS().Return(snapshot)
	ses.proc.Base.TxnOperator = txnOperator

	require.Equal(t, snapshot, currentTxnSnapshotTS(ses))
}

func TestInitExecuteStmtParamUsesTxnSnapshotAfterRebuild(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()

	snapshot := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().SnapshotTS().Return(snapshot)
	txnOperator.EXPECT().NextSequence().Return(uint64(1)).AnyTimes()
	ses.proc.Base.TxnOperator = txnOperator
	ses.advanceDDLVersion()

	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Equal(t, snapshot, prepareStmt.Ts)
	require.Equal(t, ses.getDDLVersion(), prepareStmt.ddlVersion)
}

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

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, sentinel, retComp)
	require.Same(t, sentinel, prepareStmt.compile)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
}

func TestInitExecuteStmtParamReusesCachedCompileForTextParameter(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(t, 111, "select ?")
	defer prepareStmt.Close()

	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("plain text"), false, cw.proc.Mp()))
	prepareStmt.params = params
	prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_VAR_STRING), 0}
	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel

	retComp, retPlan, _, _, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Same(t, sentinel, retComp,
		"unchanged text parameter domain must not force a full recompilation")
	require.Same(t, sentinel, prepareStmt.compile)
	require.NotNil(t, retPlan)
}

func TestInitExecuteStmtParamTransfersFreshCloneOwnership(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()

	clone := &tree.CloneTable{}
	clone.SrcTable.ObjectName = "src"
	clone.CreateTable.Table.ObjectName = "dst"
	prepareStmt.cloneSQL = preparedCloneSQL(clone, "prepare_db")

	_, _, executionStmt, _, owned, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.True(t, owned)

	executionClone := executionStmt.(*tree.CloneTable)
	require.Equal(t, tree.Identifier("prepare_db"), executionClone.SrcTable.SchemaName)
	cw.stmt = executionStmt
	cw.stmtBorrowed = !owned
	cw.Free()

	require.Empty(t, executionClone.SrcTable.SchemaName)
	require.Empty(t, executionClone.SrcTable.ObjectName)
	require.Empty(t, executionClone.CreateTable.Table.SchemaName)
	require.Empty(t, executionClone.CreateTable.Table.ObjectName)
}

func TestTxnComputationWrapperKeepsSharedPreparedStatement(t *testing.T) {
	stmt := &trackedStatement{}
	cw := &TxnComputationWrapper{
		stmt:         stmt,
		stmtBorrowed: true,
	}

	cw.Free()
	require.Zero(t, stmt.freed)
}

func TestGetComputationWrapperMarksBinaryPreparedStatementBorrowed(t *testing.T) {
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()

	stmt := &trackedStatement{}
	execCtx.input.stmt = stmt
	execCtx.input.preparePlan = prepareStmt.PreparePlan

	wrappers, err := GetComputationWrapper(execCtx, "", "root", nil, execCtx.proc, ses)
	require.NoError(t, err)
	require.Len(t, wrappers, 1)

	wrapper := wrappers[0].(*TxnComputationWrapper)
	require.True(t, wrapper.stmtBorrowed)
	wrapper.Free()
	require.Zero(t, stmt.freed, "an early binary EXECUTE error must not free the shared prepared AST")
}

func TestTxnComputationWrapperResetTransfersStatementOwnership(t *testing.T) {
	borrowed := &trackedStatement{}
	replanned := &trackedStatement{}
	wrapper := &TxnComputationWrapper{
		stmt:         borrowed,
		stmtBorrowed: true,
	}

	wrapper.ResetPlanAndStmt(replanned)
	require.Zero(t, borrowed.freed)
	require.False(t, wrapper.stmtBorrowed)

	wrapper.Free()
	require.Equal(t, 1, replanned.freed)
}

func TestCompilerContextReleasesFreshCloneForExplainExecute(t *testing.T) {
	ses, prepareStmt, cw, _ := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()
	ses.GetTxnCompileCtx().tcw = cw

	clone := &tree.CloneTable{}
	clone.SrcTable.ObjectName = "src"
	clone.CreateTable.Table.ObjectName = "dst"
	prepareStmt.cloneSQL = preparedCloneSQL(clone, "prepare_db")

	_, stmt, err := ses.GetTxnCompileCtx().InitExecuteStmtParam(
		&plan.Execute{Name: prepareStmt.Name},
	)
	require.NoError(t, err)
	require.Nil(t, stmt, "EXPLAIN consumes only the prepared plan and must release its fresh CLONE AST")
}

func TestInitExecuteStmtParamReusesStableSubscriptionSelect(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 101)
	defer prepareStmt.Close()

	expected := &plan.ObjectRef{
		Server: 4, Db: 2, Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
	}
	prepareStmt.PreparePlan.GetDcl().GetPrepare().Schemas = []*plan.ObjectRef{expected}
	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel

	retComp, _, _, _, _, err := initExecuteStmtParamWithResolver(
		execCtx, ses, cw, nil, prepareStmt.Name,
		func(string, string, *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
			return &plan.ObjectRef{
					Obj: 3, SchemaName: "publisher_db", ObjName: "src",
					SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
				},
				&plan.TableDef{DbId: 2, TblId: 3, Version: 4}, nil
		},
	)
	require.NoError(t, err)
	require.Same(t, sentinel, retComp)
	require.Same(t, sentinel, prepareStmt.compile)
}

func BenchmarkInitExecuteStmtParamReusesStableSubscriptionSelect(b *testing.B) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(b, 101)
	defer prepareStmt.Close()

	expected := &plan.ObjectRef{
		Server: 4, Db: 2, Obj: 3, SchemaName: "publisher_db", ObjName: "src",
		SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
	}
	prepareStmt.PreparePlan.GetDcl().GetPrepare().Schemas = []*plan.ObjectRef{expected}
	resolve := func(string, string, *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
		return &plan.ObjectRef{
				Obj: 3, SchemaName: "publisher_db", ObjName: "src",
				SubscriptionName: "sub", PubInfo: &plan.PubInfo{TenantId: 11},
			},
			&plan.TableDef{DbId: 2, TblId: 3, Version: 4}, nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _, _, _, _, err := initExecuteStmtParamWithResolver(
			execCtx, ses, cw, nil, prepareStmt.Name, resolve)
		if err != nil {
			b.Fatal(err)
		}
	}
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
	oldNumericPrefixConsumerPlan := prepareStmt.numericPrefixConsumerPlan
	ses.AddTempTable("db1", "unrelated", "temp-unrelated")

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotSame(t, oldPlan, prepareStmt.PreparePlan)
	require.Same(t, prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan, retPlan)
	require.NotSame(t, oldNumericPrefixConsumerPlan, prepareStmt.numericPrefixConsumerPlan)
	require.Same(t, retPlan, prepareStmt.numericPrefixConsumerPlan)
	require.NotNil(t, retStmt)
	require.Equal(t, ses.GetTempTableVersion(), prepareStmt.tempTableVersion)
	require.Equal(t, newColDefData, prepareStmt.ColDefData)
	require.Equal(t, newColDefData, execCtx.prepareColDef)
}

func TestInitExecuteStmtParamRebuildsWhenProtocolVersionChanges(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)

	for i, test := range []struct {
		name string
		from int64
		to   int64
	}{
		{name: "existing upgrade", from: defines.MORPCVersion4, to: defines.MORPCVersion5},
		{name: "existing rollback", from: defines.MORPCVersion5, to: defines.MORPCVersion4},
		{name: "upgrade", from: defines.MORPCVersion7, to: defines.MORPCVersion8},
		{name: "rollback", from: defines.MORPCVersion8, to: defines.MORPCVersion7},
		{name: "numeric prefix upgrade", from: defines.MORPCVersion25, to: defines.MORPCVersion30},
		{name: "numeric prefix rollback", from: defines.MORPCVersion30, to: defines.MORPCVersion25},
	} {
		t.Run(test.name, func(t *testing.T) {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, test.from)
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, uint32(107+i))
			defer prepareStmt.Close()
			require.Equal(t, test.from, prepareStmt.protocolVersion)
			oldPlan := prepareStmt.PreparePlan

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, test.to)
			retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(
				execCtx, ses, cw, nil, prepareStmt.Name)
			require.NoError(t, err)
			require.Nil(t, retComp)
			require.NotNil(t, retPlan)
			require.NotNil(t, retStmt)
			require.NotSame(t, oldPlan, prepareStmt.PreparePlan)
			require.Equal(t, test.to, prepareStmt.protocolVersion)
		})
	}
}

func TestInitExecuteStmtParamRebuildsUntilPlanSnapshotProtocolIsActive(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion31)

	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 115)
	defer prepareStmt.Close()
	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel
	for range 2 {
		oldPlan := prepareStmt.PreparePlan
		oldCompile := prepareStmt.compile
		oldCompiledPlan := oldCompile.GetPlan()
		retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(
			execCtx, ses, cw, nil, prepareStmt.Name)
		require.NoError(t, err)
		require.NotNil(t, retPlan)
		require.NotNil(t, retStmt)
		require.NotSame(t, oldPlan, prepareStmt.PreparePlan)
		require.Equal(t, defines.MORPCVersion31, prepareStmt.protocolVersion)
		require.NotSame(t, oldCompiledPlan, retComp.GetPlan())
		require.Same(t, retPlan, retComp.GetPlan())
		require.Same(t, retComp, prepareStmt.compile)
		require.False(t, cw.planGenerationReused)
	}

	// The protocol transition itself rebuilds once. Only the following v32
	// execution admits that generation as reusable.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion32)
	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.False(t, cw.planGenerationReused)
	_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.True(t, cw.planGenerationReused)
}

func TestInitExecuteStmtParamKeepsOldStateWhenColumnMetadataRefreshFails(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 103)
	defer prepareStmt.Close()

	oldPlan := prepareStmt.PreparePlan
	oldNumericPrefixConsumerPlan := prepareStmt.numericPrefixConsumerPlan
	oldColDefData := [][]byte{[]byte("old-int-column")}
	prepareStmt.ColDefData = oldColDefData
	execCtx.prepareColDef = oldColDefData
	w := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	w.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		return nil, errors.New("column metadata refresh failed")
	}

	ses.AddTempTable("db1", "unrelated", "temp-unrelated")
	_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.EqualError(t, err, "column metadata refresh failed")
	require.Same(t, oldPlan, prepareStmt.PreparePlan)
	require.Same(t, oldNumericPrefixConsumerPlan, prepareStmt.numericPrefixConsumerPlan)
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

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
	require.True(t, prepareStmt.NativeMode)
	require.NotSame(t, originalPlan, prepareStmt.PreparePlan)
}

func TestInitExecuteStmtParamRebuildsPreparedPlanWhenOnlyFullGroupByChanges(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 109)
	defer prepareStmt.Close()

	execCtx.reqCtx = defines.AttachAccountId(execCtx.reqCtx, catalog.System_Account)
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "sql_mode", ""))
	prepareStmt.OnlyFullGroupBy = false
	prepareStmt.sqlModeFlagsSet = true
	originalPlan := prepareStmt.PreparePlan
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "sql_mode", "ONLY_FULL_GROUP_BY"))

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
	require.True(t, prepareStmt.OnlyFullGroupBy)
	require.NotSame(t, originalPlan, prepareStmt.PreparePlan)
}

// ENABLE_BOOL_SUMAVG is captured at PREPARE like ONLY_FULL_GROUP_BY, so an
// EXECUTE after the token changes rebuilds the plan under the current mode in
// both directions.
func TestInitExecuteStmtParamRebuildsPreparedPlanWhenBoolSumAvgChanges(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 111)
	defer prepareStmt.Close()

	execCtx.reqCtx = defines.AttachAccountId(execCtx.reqCtx, catalog.System_Account)
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "sql_mode", "ONLY_FULL_GROUP_BY"))
	prepareStmt.OnlyFullGroupBy = true
	prepareStmt.BoolSumAvg = false
	prepareStmt.sqlModeFlagsSet = true
	originalPlan := prepareStmt.PreparePlan
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "sql_mode", "ONLY_FULL_GROUP_BY,ENABLE_BOOL_SUMAVG"))

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
	require.True(t, prepareStmt.BoolSumAvg)
	require.True(t, prepareStmt.OnlyFullGroupBy)
	require.NotSame(t, originalPlan, prepareStmt.PreparePlan)

	// Dropping the token rebuilds again; an unchanged mode reuses the plan.
	rebuiltPlan := prepareStmt.PreparePlan
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "sql_mode", "ONLY_FULL_GROUP_BY"))
	_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.False(t, prepareStmt.BoolSumAvg)
	require.NotSame(t, rebuiltPlan, prepareStmt.PreparePlan)
}

func TestInitExecuteStmtParamRebuildsPlanInvalidatedDuringRun(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 110)
	defer prepareStmt.Close()

	originalPlan := prepareStmt.PreparePlan
	prepareStmt.needsRebuild = true
	prepareStmt.compileNeedsRebuild = true

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
	require.NotSame(t, originalPlan, prepareStmt.PreparePlan)
	require.False(t, prepareStmt.needsRebuild)
	require.False(t, prepareStmt.compileNeedsRebuild)
	require.Equal(t, prepareStmt.compile, retComp)
}

func TestInitExecuteStmtParamBypassesButRetainsCachedTopologyForExplicitSchedulingIntent(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 102)
	defer prepareStmt.Close()

	sentinel := compile.NewCompile(
		"", "", prepareStmt.Sql, "", "", nil,
		cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
	prepareStmt.compile = sentinel
	require.NoError(t, ses.SetSessionSysVar(context.Background(), queryMaxWorkers, int64(2)))

	retComp, retPlan, retStmt, _, _, err := initExecuteStmtParam(
		execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.Same(t, sentinel, prepareStmt.compile)
	require.NotNil(t, retPlan)
	require.NotNil(t, retStmt)
}

func TestInitExecuteStmtParamBypassesCachedTopologyForPreparedSiriusExecution(t *testing.T) {
	for _, protocol := range []struct {
		name     string
		execPlan func(string) *plan.Execute
		stmtName func(string) string
	}{
		{
			name:     "binary",
			execPlan: func(string) *plan.Execute { return nil },
			stmtName: func(name string) string { return name },
		},
		{
			name:     "text",
			execPlan: func(name string) *plan.Execute { return &plan.Execute{Name: name} },
			stmtName: func(string) string { return "" },
		},
	} {
		t.Run(protocol.name, func(t *testing.T) {
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
				t, 110, "/*+ SIDECAR */ select 1")
			defer prepareStmt.Close()

			sentinel := compile.NewCompile(
				"", "", prepareStmt.Sql, "", "", nil,
				cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
			prepareStmt.compile = sentinel

			for range 2 {
				retComp, retPlan, retStmt, originSQL, _, err := initExecuteStmtParam(
					execCtx,
					ses,
					cw,
					protocol.execPlan(prepareStmt.Name),
					protocol.stmtName(prepareStmt.Name),
				)
				require.NoError(t, err)
				require.Nil(t, retComp)
				require.Same(t, sentinel, prepareStmt.compile)
				require.NotNil(t, retPlan)
				require.NotNil(t, retStmt)
				require.True(t, siriusStatementSelected(originSQL, retStmt))
			}
		})
	}
}

func TestCompileStatementContextsPreserveCounterWithoutLeakingSelection(t *testing.T) {
	for _, test := range []struct {
		name          string
		sql           string
		separateChild bool
	}{
		{name: "selected", sql: "/*+ SIDECAR */ select 1", separateChild: true},
		{name: "unselected", sql: "select 1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			counter := new(perfcounter.CounterSet)
			requestCtx, compileCtx := compileStatementContexts(
				context.Background(), test.sql, &tree.Select{}, counter)

			attached, ok := compileCtx.Value(perfcounter.CompilePlanMarkKey{}).(*perfcounter.CounterSet)
			require.True(t, ok)
			require.Same(t, counter, attached)
			require.Equal(t, test.separateChild, requestCtx != compileCtx)
			perfcounter.Update(compileCtx, func(set *perfcounter.CounterSet) {
				set.FileService.S3.Get.Add(1)
			})
			require.Equal(t, int64(1), counter.FileService.S3.Get.Load())
		})
	}

	requestCtx, _ := compileStatementContexts(
		context.Background(), "/*+ SIDECAR */ select 1", &tree.Select{}, new(perfcounter.CounterSet))
	requestCtx, unselectedCompileCtx := compileStatementContexts(
		requestCtx, "select 2", &tree.Select{}, new(perfcounter.CounterSet))
	require.True(t, requestCtx == unselectedCompileCtx,
		"the selected statement's Sirius marker must not enter its sibling's request context")
}

func TestRebuildPreparePlanUsesPreparedRootSQL(t *testing.T) {
	const preparedSQL = "create view v as select 1"
	const executeSQL = "execute prepared_view"
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnvForSQL(t, 106, preparedSQL)
	defer prepareStmt.Close()
	prepareStmt.Name = "prepared_view"
	ses.SetSql(executeSQL)

	rebuilt, err := rebuildPreparePlan(
		execCtx,
		ses,
		prepareStmt,
		func(_ context.Context, _ FeSession, compilerCtx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
			return plan2.BuildPlan(&preparedViewCompilerContext{CompilerContext: compilerCtx}, stmt, false)
		},
	)
	require.NoError(t, err)
	prepareStmt.PreparePlan = rebuilt
	requirePreparedViewRootSQL(t, prepareStmt, preparedSQL)
	require.Equal(t, executeSQL, ses.GetSql())
	require.Equal(t, executeSQL, ses.GetTxnCompileCtx().GetRootSql())
}

func TestRebuildPreparePlanUsesFreshCloneStatement(t *testing.T) {
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnv(t, 108)
	defer prepareStmt.Close()
	prepareStmt.PrepareStmt.Free()
	clone := &tree.CloneTable{}
	clone.SrcTable.ObjectName = "src"
	clone.CreateTable.Table.ObjectName = "dst"
	prepareStmt.PrepareStmt = clone
	prepareStmt.cloneSQL = preparedCloneSQL(clone, "prepare_db")
	prepareStmt.defaultDatabase = "prepare_db"

	buildCount := 0
	buildFn := func(
		_ context.Context,
		_ FeSession,
		_ plan2.CompilerContext,
		stmt tree.Statement,
	) (*plan2.Plan, error) {
		buildCount++
		inner := stmt.(*tree.PrepareStmt).Stmt.(*tree.CloneTable)
		require.Equal(t, tree.Identifier("prepare_db"), inner.SrcTable.SchemaName)
		require.Equal(t, tree.Identifier("prepare_db"), inner.CreateTable.Table.SchemaName)
		inner.SrcTable.SchemaName = "planner_mutation"
		inner.CreateTable.Table.SchemaName = "planner_mutation"
		return &plan2.Plan{}, nil
	}

	_, err := rebuildPreparePlan(execCtx, ses, prepareStmt, buildFn)
	require.NoError(t, err)
	_, err = rebuildPreparePlan(execCtx, ses, prepareStmt, buildFn)
	require.NoError(t, err)
	require.Equal(t, 2, buildCount)
	require.Empty(t, clone.SrcTable.SchemaName)
	require.Empty(t, clone.CreateTable.Table.SchemaName)
}

func TestModeMismatchRebuildsPreparedViewWithPreparedRootSQL(t *testing.T) {
	const preparedSQL = "create view v as select 1"
	const executeSQL = "execute prepared_view"
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnvForSQL(t, 107, preparedSQL)
	defer prepareStmt.Close()
	ses.SetSql(executeSQL)
	execCtx.reqCtx = defines.AttachAccountId(execCtx.reqCtx, catalog.System_Account)
	require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "sql_mode", "MATRIXONE_NATIVE"))
	modeMismatch := prepareStmt.NativeMode != ses.sqlModeHasMatrixOneNative()
	require.True(t, modeMismatch)
	require.True(t, preparePlanNeedsRebuild(false, modeMismatch, false))
	require.True(t, preparePlanNeedsRebuild(false, false, true))

	rebuilt, err := rebuildPreparePlan(
		execCtx,
		ses,
		prepareStmt,
		func(_ context.Context, _ FeSession, compilerCtx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
			return plan2.BuildPlan(&preparedViewCompilerContext{CompilerContext: compilerCtx}, stmt, false)
		},
	)
	require.NoError(t, err)
	prepareStmt.PreparePlan = rebuilt
	requirePreparedViewRootSQL(t, prepareStmt, preparedSQL)
	require.Equal(t, executeSQL, ses.GetSql())
	require.Equal(t, executeSQL, ses.GetTxnCompileCtx().GetRootSql())
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
