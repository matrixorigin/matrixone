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

package compile

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type remoteWarningSession struct {
	warnings []struct {
		code uint16
		msg  string
	}
	totalWarnings uint64
}

func (*remoteWarningSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*remoteWarningSession) AddTempTable(string, string, string)        {}
func (*remoteWarningSession) RemoveTempTable(string, string)             {}
func (*remoteWarningSession) RemoveTempTableByRealName(string)           {}
func (*remoteWarningSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }
func (s *remoteWarningSession) AppendWarningDiagnostic(code uint16, msg string) {
	s.totalWarnings++
	s.warnings = append(s.warnings, struct {
		code uint16
		msg  string
	}{code: code, msg: msg})
}
func (s *remoteWarningSession) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	s.totalWarnings += total
	for i := 0; i < len(codes) && i < len(messages); i++ {
		s.warnings = append(s.warnings, struct {
			code uint16
			msg  string
		}{code: codes[i], msg: messages[i]})
	}
}

func TestRemoteNumericCastWarningAppearsAtExecution(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &remoteWarningSession{}
	proc.Session = session
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "s" && !system {
			return "12abc", nil
		}
		return nil, moerr.NewInternalErrorNoCtx("variable not found")
	})

	variable := makeTestVarExpr("s")
	variable.GetV().System = false
	targetType := types.T_float64.ToType()
	cast, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{
		variable,
		{
			Typ:  plan2.MakePlan2Type(&targetType),
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		},
	})
	require.NoError(t, err)

	folded, err := foldVarExprsInExprInPlace(cast, proc)
	require.NoError(t, err)
	require.True(t, folded)
	require.Empty(t, session.warnings)

	input := batch.NewWithSize(1)
	dummy := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(dummy, int64(1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(dummy, int64(2), false, proc.Mp()))
	defer dummy.Free(proc.Mp())
	input.Vecs[0] = dummy
	input.SetRowCount(2)
	vec, free, err := colexec.GetReadonlyResultFromExpression(proc, cast, []*batch.Batch{input})
	require.NoError(t, err)
	require.Equal(t, 2, vec.Length())
	free()
	require.Len(t, session.warnings, 2)
	require.Equal(t, moerr.ER_TRUNCATED_WRONG_VALUE, session.warnings[0].code)
	require.Contains(t, session.warnings[0].msg, "12abc")
}

func TestRemoteNumericCastWarningCountIsIndependentOfBatching(t *testing.T) {
	buildCast := func(proc *process.Process) *plan.Expr {
		proc.Session = &remoteWarningSession{}
		proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
			if name == "s" && !system {
				return "12abc", nil
			}
			return nil, moerr.NewInternalErrorNoCtx("variable not found")
		})

		variable := makeTestVarExpr("s")
		variable.GetV().System = false
		targetType := types.T_float64.ToType()
		cast, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{
			variable,
			{
				Typ:  plan2.MakePlan2Type(&targetType),
				Expr: &plan.Expr_T{T: &plan.TargetType{}},
			},
		})
		require.NoError(t, err)
		folded, err := foldVarExprsInExprInPlace(cast, proc)
		require.NoError(t, err)
		require.True(t, folded)
		return cast
	}

	run := func(layout []int) uint64 {
		proc := testutil.NewProcess(t)
		cast := buildCast(proc)
		executor, err := colexec.NewExpressionExecutor(proc, cast)
		require.NoError(t, err)
		defer executor.Free()

		for _, rows := range layout {
			input := batch.NewWithSize(1)
			input.Vecs[0] = testutil.MakeInt64Vector(make([]int64, rows), nil, proc.Mp())
			input.SetRowCount(rows)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, rows, result.Length())
			input.Vecs[0].Free(proc.Mp())
		}
		session := proc.Session.(*remoteWarningSession)
		return session.totalWarnings
	}

	// A single two-row batch and two one-row batches represent the same
	// logical result. Constant folding must not make their diagnostics differ.
	require.Equal(t, uint64(2), run([]int{2}))
	require.Equal(t, uint64(2), run([]int{1, 1}))
	require.Equal(t, uint64(0), run([]int{0}))
}

func TestRemoteNumericCoercionWarningsFollowEvaluatedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &remoteWarningSession{}
	proc.Session = session
	sourceType := types.T_text.ToType()
	source := &plan.Expr{
		Typ:  plan2.MakePlan2Type(&sourceType),
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
	}
	targetType := types.T_float64.ToType()
	cast, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{
		source,
		{
			Typ:  plan2.MakePlan2Type(&targetType),
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		},
	})
	require.NoError(t, err)

	input := batch.NewWithSize(1)
	values := testutil.MakeVarlenaVector(
		[][]byte{[]byte("12abc"), []byte("12abc")}, nil,
		types.T_text.ToType(), proc.Mp())
	defer values.Free(proc.Mp())
	input.Vecs[0] = values
	input.SetRowCount(2)
	vec, free, err := colexec.GetReadonlyResultFromExpression(proc, cast, []*batch.Batch{input})
	require.NoError(t, err)
	require.Equal(t, 2, vec.Length())
	free()
	require.Len(t, session.warnings, 2)

	session.warnings = nil
	input.SetRowCount(0)
	vec, free, err = colexec.GetReadonlyResultFromExpression(proc, cast, []*batch.Batch{input})
	require.NoError(t, err)
	require.Equal(t, 0, vec.Length())
	free()
	require.Empty(t, session.warnings)
}

func TestFoldVarExprRemoteNumericCastSkipsUnselectedBranchWarning(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &remoteWarningSession{}
	proc.Session = session
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "s" && !system {
			return "12abc", nil
		}
		return nil, moerr.NewInternalErrorNoCtx("variable not found")
	})

	variable := makeTestVarExpr("s")
	variable.GetV().System = false
	targetType := types.T_float64.ToType()
	cast, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{
		variable,
		{
			Typ:  plan2.MakePlan2Type(&targetType),
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		},
	})
	require.NoError(t, err)
	zeroType := types.T_int64.ToType()
	zero := &plan.Expr{
		Typ: plan2.MakePlan2Type(&zeroType),
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_I64Val{I64Val: 0},
		}},
	}
	condition := makeTestConstBoolExpr(false)
	iff, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "if", []*plan.Expr{
		condition, cast, zero,
	})
	require.NoError(t, err)

	folded, err := foldVarExprsInExprInPlace(iff, proc)
	require.NoError(t, err)
	require.True(t, folded)
	input := batch.NewWithSize(1)
	input.SetRowCount(2)
	vec, free, err := colexec.GetReadonlyResultFromExpression(proc, iff, []*batch.Batch{input})
	require.NoError(t, err)
	require.Equal(t, 2, vec.Length())
	free()
	require.Empty(t, session.warnings)
}

func TestRemoteTerminalWarningsAreForwardedToInitiatingSession(t *testing.T) {
	session := &remoteWarningSession{}
	sender := &messageSenderOnClient{warningSink: session}
	data, err := json.Marshal(remoteTerminalEnvelope{
		WarningCount: 2,
		WarningDiagnostics: []remoteWarningDiagnostic{
			{Code: moerr.ER_TRUNCATED_WRONG_VALUE, Message: "first"},
			{Code: moerr.ER_TRUNCATED_WRONG_VALUE, Message: "second"},
		},
	})
	require.NoError(t, err)
	require.NoError(t, sender.dealRemoteTerminal(data))
	require.Len(t, session.warnings, 2)
	require.Equal(t, uint64(2), session.totalWarnings)
	require.Equal(t, "first", session.warnings[0].msg)
	require.Equal(t, "second", session.warnings[1].msg)
}

func TestRemoteWarningCollectorBoundsRetention(t *testing.T) {
	collector := &remoteWarningCollector{maxRetained: 3}
	for i := 0; i < 1000; i++ {
		collector.AppendWarningDiagnostic(1292, "truncated")
	}

	total, retained := collector.SnapshotWarnings()
	require.Equal(t, uint64(1000), total)
	require.Len(t, retained, 3)

	data, err := json.Marshal(remoteTerminalEnvelope{
		WarningCount:       total,
		WarningDiagnostics: retained,
	})
	require.NoError(t, err)
	require.Less(t, len(data), 1024)
}

func TestRemoteWarningCollectorMergesDescendantCountsAndRecords(t *testing.T) {
	collector := &remoteWarningCollector{maxRetained: 2}
	collector.AppendWarningBatch(100, []uint16{1, 2, 3}, []string{"a", "b", "c"})
	collector.AppendWarningBatch(50, []uint16{4}, []string{"d"})

	total, retained := collector.SnapshotWarnings()
	require.Equal(t, uint64(150), total)
	require.Len(t, retained, 2)
	require.Equal(t, uint16(1), retained[0].Code)
	require.Equal(t, uint16(2), retained[1].Code)
}

func TestScopeContainsVarExpr(t *testing.T) {
	scope := newScope(Normal)
	proj := projection.NewArgument()
	proj.ProjectList = []*plan.Expr{makeTestVarExpr("sql_mode")}
	f := filter.NewArgument()
	f.FilterExprs = []*plan.Expr{makeTestConstBoolExpr(true)}
	f.AppendChild(proj)
	scope.setRootOperator(f)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestAssertFilterRemoteRoundTrip(t *testing.T) {
	ctx := &scopeContext{
		id:     1,
		root:   &scopeContext{},
		parent: &scopeContext{},
	}
	proc := testutil.NewProcess(t)
	original := filter.NewArgument()
	original.FilterExprs = []*plan.Expr{makeTestConstBoolExpr(true)}
	original.IsAssert = true

	_, wire, err := convertToPipelineInstruction(original, proc, ctx, 1)
	require.NoError(t, err)
	require.True(t, wire.FilterIsAssert)

	payload, err := wire.Marshal()
	require.NoError(t, err)
	wireRoundTrip := new(pipeline.Instruction)
	require.NoError(t, wireRoundTrip.Unmarshal(payload))
	require.True(t, wireRoundTrip.FilterIsAssert)

	restored, err := convertToVmOperator(wireRoundTrip, ctx, nil)
	require.NoError(t, err)
	require.True(t, restored.(*filter.Filter).IsAssert)
}

func TestAggregateConfigTypeRemoteRoundTrip(t *testing.T) {
	expected := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfGroupConcat,
		true,
		[]*plan.Expr{makeTestVarExpr("value")},
		[]byte{1, 2, 3},
		plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
	)

	wire := convertToPipelineAggregates([]aggexec.AggFuncExecExpression{expected})
	require.Len(t, wire, 1)
	require.Equal(
		t,
		plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
		wire[0].ConfigType,
	)

	actual := convertToAggregates(wire)
	require.Len(t, actual, 1)
	require.Equal(t, expected.GetExtraConfig(), actual[0].GetExtraConfig())
	require.Equal(t, expected.GetConfigType(), actual[0].GetConfigType())
}

func TestOrderedAggregateRemoteProtocolValidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := runtime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
	ordered := []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfGroupConcat,
		false,
		[]*plan.Expr{makeTestVarExpr("value")},
		[]byte{1, 2, 3},
		plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
	)}

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion5)
	require.ErrorContains(
		t,
		validateRemoteAggregateProtocol(proc, ordered),
		"requires MORPC protocol version 6",
	)

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion6)
	require.NoError(t, validateRemoteAggregateProtocol(proc, ordered))

	require.NoError(t, validateRemoteAggregateProtocol(proc, []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(
			aggexec.AggIdOfGroupConcat,
			false,
			[]*plan.Expr{makeTestVarExpr("value")},
			[]byte(","),
			plan.AggregateConfigType_AGG_CONFIG_NONE,
		),
	}))
}

func TestOrderedSetPercentileRemoteProtocolValidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := runtime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
	percentile := []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfPercentileCont,
		false,
		[]*plan.Expr{makeTestVarExpr("value")},
		aggexec.EncodeOrderedPercentileConfig([]byte("0.5"), false),
		plan.AggregateConfigType_AGG_CONFIG_NONE,
	)}

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion16)
	require.ErrorContains(
		t,
		validateRemoteAggregateProtocol(proc, percentile),
		"requires MORPC protocol version 17",
	)

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion17)
	require.NoError(t, validateRemoteAggregateProtocol(proc, percentile))
}

func TestTextMinMaxRemoteProtocolValidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := runtime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
	textMinForCharset := func(charset uint32) []aggexec.AggFuncExecExpression {
		expr := makeTestVarExpr("value")
		expr.Typ.Charset = charset
		return []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(
			aggexec.AggIdOfMin,
			false,
			[]*plan.Expr{expr},
			nil,
		)}
	}
	generalCIMin := textMinForCharset(uint32(types.CharsetUTF8))
	binMin := textMinForCharset(uint32(types.CharsetUTF8MB4Bin))

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion13)
	for _, collationAwareMin := range [][]aggexec.AggFuncExecExpression{generalCIMin, binMin} {
		require.ErrorContains(t, validateRemoteAggregateProtocol(proc, collationAwareMin),
			"requires MORPC protocol version 14")
	}

	ordered := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfGroupConcat,
		false,
		[]*plan.Expr{makeTestVarExpr("value")},
		[]byte{1, 2, 3},
		plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
	)
	require.ErrorContains(t,
		validateRemoteAggregateProtocol(proc,
			append([]aggexec.AggFuncExecExpression{ordered}, generalCIMin...)),
		"requires MORPC protocol version 14",
	)

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion14)
	require.NoError(t, validateRemoteAggregateProtocol(proc, generalCIMin))
	require.NoError(t, validateRemoteAggregateProtocol(proc, binMin))

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion9)
	for _, charset := range []uint32{
		uint32(types.CharsetLegacy),
		uint32(types.CharsetBinary),
	} {
		binaryText := makeTestVarExpr("packed")
		binaryText.Typ.Charset = charset
		require.NoError(t, validateRemoteAggregateProtocol(proc,
			[]aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfMax, false, []*plan.Expr{binaryText}, nil)}))
	}
}

func TestOrderedSetPercentileMergeGroupRemoteProtocolValidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := runtime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)

	merge := group.NewArgumentMergeGroup()
	merge.Aggs = []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfPercentileDisc,
		false,
		[]*plan.Expr{makeTestVarExpr("value")},
		aggexec.EncodeOrderedPercentileConfig([]byte("0.5"), false),
		plan.AggregateConfigType_AGG_CONFIG_NONE,
	)}

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion16)
	_, _, err := convertToPipelineInstruction(merge, proc, &scopeContext{}, 1)
	require.ErrorContains(t, err, "requires MORPC protocol version 17")

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion17)
	_, _, err = convertToPipelineInstruction(merge, proc, &scopeContext{}, 1)
	require.NoError(t, err)
}

func TestScopeContainsVarExprInAggArguments(t *testing.T) {
	scope := newScope(Normal)
	op := group.NewArgument()
	op.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{makeTestVarExpr("sql_mode")}, nil),
	}
	scope.setRootOperator(op)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprInLockRows(t *testing.T) {
	scope := newScope(Normal)
	op := lockop.NewArgumentByEngine(nil)
	op.AddLockTarget(1, nil, 0, types.T_int64.ToType(), -1, -1, makeTestVarExpr("sql_mode"), false)
	scope.setRootOperator(op)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestFoldVarExprsInScope(t *testing.T) {
	proc := newResolveVariableProcess(t, "STRICT_TRANS_TABLES")
	scope := newScope(Normal)
	scope.DataSource = &Source{
		FilterList: []*plan.Expr{makeTestVarExpr("sql_mode")},
	}

	folded, err := foldVarExprsInScope(scope, proc)
	require.NoError(t, err)
	require.True(t, folded)
	require.False(t, scopeContainsVarExpr(scope))

	lit, ok := scope.DataSource.FilterList[0].Expr.(*plan.Expr_Lit)
	require.True(t, ok)
	require.Equal(t, "STRICT_TRANS_TABLES", lit.Lit.GetSval())
}

func TestFoldVarExprsInScopeUsesPrivateExprCopies(t *testing.T) {
	shared := makeTestVarExpr("sql_mode")
	proc1 := newResolveVariableProcess(t, "ANSI")
	proc2 := newResolveVariableProcess(t, "TRADITIONAL")

	scope1 := newScope(Normal)
	proj1 := projection.NewArgument()
	proj1.ProjectList = []*plan.Expr{shared}
	scope1.setRootOperator(proj1)

	scope2 := newScope(Normal)
	proj2 := projection.NewArgument()
	proj2.ProjectList = []*plan.Expr{shared}
	scope2.setRootOperator(proj2)

	folded, err := foldVarExprsInScope(scope1, proc1)
	require.NoError(t, err)
	require.True(t, folded)
	folded, err = foldVarExprsInScope(scope2, proc2)
	require.NoError(t, err)
	require.True(t, folded)

	require.IsType(t, &plan.Expr_V{}, shared.Expr)
	require.NotSame(t, shared, proj1.ProjectList[0])
	require.NotSame(t, shared, proj2.ProjectList[0])
	require.NotSame(t, proj1.ProjectList[0], proj2.ProjectList[0])
	require.Equal(t, "ANSI", proj1.ProjectList[0].GetLit().GetSval())
	require.Equal(t, "TRADITIONAL", proj2.ProjectList[0].GetLit().GetSval())
}

func TestFoldVarExprsInRemoteRunScopeDoesNotMutateReusableScope(t *testing.T) {
	shared := makeTestVarExpr("sql_mode")
	proc1 := newResolveVariableProcess(t, "ANSI")
	proc2 := newResolveVariableProcess(t, "TRADITIONAL")

	scope := newScope(Remote)
	proj := projection.NewArgument()
	proj.ProjectList = []*plan.Expr{shared}
	scope.setRootOperator(proj)

	remoteScope1, folded, err := foldVarExprsInRemoteRunScope(scope, proc1)
	require.NoError(t, err)
	require.True(t, folded)
	remoteProj1 := remoteScope1.RootOp.(*projection.Projection)
	require.Equal(t, "ANSI", remoteProj1.ProjectList[0].GetLit().GetSval())

	require.True(t, scopeContainsVarExpr(scope))
	require.IsType(t, &plan.Expr_V{}, shared.Expr)
	require.Same(t, shared, proj.ProjectList[0])

	remoteScope2, folded, err := foldVarExprsInRemoteRunScope(scope, proc2)
	require.NoError(t, err)
	require.True(t, folded)
	remoteProj2 := remoteScope2.RootOp.(*projection.Projection)
	require.Equal(t, "TRADITIONAL", remoteProj2.ProjectList[0].GetLit().GetSval())

	require.NotSame(t, scope, remoteScope1)
	require.NotSame(t, scope.RootOp, remoteScope1.RootOp)
	require.NotSame(t, remoteScope1.RootOp, remoteScope2.RootOp)
	require.NotSame(t, remoteProj1.ProjectList[0], remoteProj2.ProjectList[0])
	require.True(t, scopeContainsVarExpr(scope))
}

func TestFoldVarExprsInHiddenExpressionsUsePrivateCopies(t *testing.T) {
	proc := newResolveVariableProcess(t, "ANSI")
	sharedAggExpr := makeTestVarExpr("sql_mode")
	sharedLockRows := makeTestVarExpr("sql_mode")

	scope := newScope(Normal)
	groupOp := group.NewArgument()
	groupOp.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{sharedAggExpr}, nil),
	}
	lockOp := lockop.NewArgumentByEngine(nil)
	lockOp.AddLockTarget(2, nil, 0, types.T_int64.ToType(), -1, -1, nil, false)
	lockOp.AddLockTarget(1, nil, 0, types.T_int64.ToType(), -1, -1, sharedLockRows, false)
	groupOp.AppendChild(lockOp)
	scope.setRootOperator(groupOp)

	rewriteCalls := 0
	rewritten, err := lockOp.RewriteLockRowsExpressions(func(expr *plan.Expr) (*plan.Expr, bool, error) {
		require.NotNil(t, expr)
		rewriteCalls++
		return expr, false, nil
	})
	require.NoError(t, err)
	require.False(t, rewritten)
	require.Equal(t, 1, rewriteCalls)

	folded, err := foldVarExprsInScope(scope, proc)
	require.NoError(t, err)
	require.True(t, folded)

	aggArg := groupOp.Aggs[0].GetArgExpressions()[0]
	lockRows := lockOp.GetLockRowsExpressions()[0]
	require.IsType(t, &plan.Expr_V{}, sharedAggExpr.Expr)
	require.IsType(t, &plan.Expr_V{}, sharedLockRows.Expr)
	require.NotSame(t, sharedAggExpr, aggArg)
	require.NotSame(t, sharedLockRows, lockRows)
	require.Equal(t, "ANSI", aggArg.GetLit().GetSval())
	require.Equal(t, "ANSI", lockRows.GetLit().GetSval())
}

func TestScopeContainsVarExprInSource(t *testing.T) {
	scope := newScope(Normal)
	scope.DataSource = &Source{
		FilterList: []*plan.Expr{makeTestVarExpr("sql_mode")},
	}

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprReturnsFalseWithoutVar(t *testing.T) {
	scope := newScope(Normal)
	f := filter.NewArgument()
	f.FilterExprs = []*plan.Expr{makeTestConstBoolExpr(true)}
	scope.setRootOperator(f)

	require.False(t, scopeContainsVarExpr(scope))
}

func makeTestVarExpr(name string) *plan.Expr {
	typ := types.T_text.ToType()
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&typ),
		Expr: &plan.Expr_V{
			V: &plan.VarRef{
				Name:   name,
				System: true,
			},
		},
	}
}

func newResolveVariableProcess(t *testing.T, sqlMode string) *process.Process {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "sql_mode" {
			return sqlMode, nil
		}
		return nil, moerr.NewInternalErrorNoCtx("variable not found")
	})
	return proc
}

func makeTestConstBoolExpr(v bool) *plan.Expr {
	typ := types.T_bool.ToType()
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&typ),
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: v},
			},
		},
	}
}
