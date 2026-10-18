// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestIncompleteTemporalProtocolPlacementSendReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "week", []*planpb.Expr{
		{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		plan2.MakePlan2Int64ConstExprWithType(3),
	})
	require.NoError(t, err)
	require.Equal(t, function.EncodeOverloadID(function.WEEK, 5), expr.GetF().Func.Obj)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.IncompleteTemporalStrings)
	require.True(t, features.DateNumericResultContracts)
	require.False(t, features.WeekSessionDefault)
	require.Equal(t, defines.MORPCVersion120, remoteExpressionProtocolVersion(features))
	// A string DATE_ADD can also contain the upstream v115 interval normalizer.
	// Preserve both requirements: a v117 worker supports only the latter.
	interval := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_interval)}, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
		plan2.MakePlan2StringConstExprWithType("1.5"), plan2.MakePlan2StringConstExprWithType("DAY"),
	}}}}
	combined, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "date_add", []*planpb.Expr{
		expr.GetF().Args[0], interval,
	})
	require.NoError(t, err)
	combinedFeatures, err := planpb.RequiredRemoteExpressionFeatures(combined)
	require.NoError(t, err)
	require.True(t, combinedFeatures.IncompleteTemporalStrings)
	require.True(t, combinedFeatures.CalendarIntervalNormalization)
	require.Equal(t, defines.MORPCVersion120, remoteExpressionProtocolVersion(combinedFeatures))
	floor, err := plan2.RequiredPersistedExpressionProtocolVersion(combined)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion120, floor)
	// The upstream SELECT-division policy is execution-only and must compose
	// with both temporal contracts without lowering their persisted floor.
	c.proc.SetStmtProfile(&process.StmtProfile{})
	c.proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
	c.proc.GetSessionInfo().SqlMode = "ERROR_FOR_DIVISION_BY_ZERO"
	c.proc.SetResolveVariableFunc(nil)
	division, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "div", []*planpb.Expr{
		{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		plan2.MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	divisionFeatures, err := planpb.RequiredRemoteExpressionFeatures(division)
	require.NoError(t, err)
	require.True(t, divisionFeatures.ScalarDivision)
	require.Equal(t, defines.MORPCVersion116, executionRemoteExpressionProtocolVersion(c.proc, divisionFeatures))
	floor, err = plan2.RequiredPersistedExpressionProtocolVersion(division)
	require.NoError(t, err)
	require.Zero(t, floor)
	// The official collation-aware LEAST contract stays on v117. Adding an
	// incomplete temporal expression must raise the combined floor to v120.
	collationType := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
	collationType.CollationVersion = 1
	collation, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "least", []*planpb.Expr{
		{Typ: collationType.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 2}}},
		{Typ: collationType.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 3}}},
	})
	require.NoError(t, err)
	collationFeatures, err := planpb.RequiredRemoteExpressionFeatures(collation)
	require.NoError(t, err)
	require.True(t, collationFeatures.LeastGreatestCollationSemantics)
	require.False(t, collationFeatures.IncompleteTemporalStrings)
	require.Equal(t, defines.MORPCVersion117, remoteExpressionProtocolVersion(collationFeatures))
	floor, err = plan2.RequiredPersistedExpressionProtocolVersion(collation)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion117, floor)
	expressions := []*planpb.Expr{expr, combined, division, collation}
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: expressions}}, Steps: []int32{0}}
	features, err = planpb.RequiredRemoteExpressionFeatures(qry)
	require.NoError(t, err)
	require.True(t, features.LeastGreatestCollationSemantics)
	require.True(t, features.ScalarDivision)
	require.True(t, features.IncompleteTemporalStrings)
	require.True(t, features.CalendarIntervalNormalization)
	floor, err = plan2.RequiredPersistedExpressionProtocolVersion(qry)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion120, floor)
	require.Equal(t, defines.MORPCVersion120, executionRemoteExpressionProtocolVersion(c.proc, features))
	c.proc.GetSessionInfo().SqlMode = ""
	require.Equal(t, defines.MORPCVersion120, executionRemoteExpressionProtocolVersion(c.proc, features))
	c.proc.GetSessionInfo().SqlMode = "ERROR_FOR_DIVISION_BY_ZERO"
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = expressions
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, version := range []int64{defines.MORPCVersion119, defines.MORPCVersion120} {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		if version == defines.MORPCVersion119 {
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
		} else {
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
		}
	}
	// Every send must observe the actual destination, including a rollback after success.
	for _, version := range []int64{defines.MORPCVersion120, defines.MORPCVersion119, defines.MORPCVersion120} {
		client.version = version
		data, sendErr := encodeRemoteScope(scope, c.proc)
		if version == defines.MORPCVersion119 {
			require.ErrorContains(t, sendErr, "version 120")
			require.Nil(t, data)
		} else {
			require.NoError(t, sendErr)
		}
	}
	client.customResponse = true
	client.response = &query.Response{}
	_, err = encodeRemoteScope(scope, c.proc)
	// An unknown peer can fail the earlier independent division gate first.
	require.ErrorContains(t, err, "remote destination")
	require.Equal(t, client.calls, client.releases)
	p := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{InstructionList: []*pipeline.Instruction{{ProjectList: expressions}}}}}
	data, err := p.Marshal()
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, version := range []any{nil, "120", defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion119} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		decoded, decodeErr := decodeScope(data, c.proc, true, nil)
		if version != defines.MORPCVersion120 {
			if version == defines.MORPCVersion119 {
				require.ErrorContains(t, decodeErr, "version 120")
			} else {
				// Unknown floors may fail an earlier independent gate first.
				require.ErrorContains(t, decodeErr, "protocol version")
			}
			require.Nil(t, decoded)
		} else {
			require.NoError(t, decodeErr)
			require.NotNil(t, decoded)
			decoded.release()
		}
	}
}

func TestIncompleteTemporalProtocolPlanAdmissionAndCachedRun(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	// An unresolved parameter keeps the string overload through prepared binding.
	arg := &planpb.Expr{Typ: types.T_any.ToType().PlanType(), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "date_format", []*planpb.Expr{arg, plan2.MakePlan2StringConstExprWithType("%Y-%m-%d")})
	require.NoError(t, err)
	require.Equal(t, function.EncodeOverloadID(function.DATE_FORMAT, 2), expr.GetF().Func.Obj)
	for _, owner := range []*planpb.TableDef{
		{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}},
		{Cols: []*planpb.ColDef{{GeneratedCol: &planpb.GeneratedCol{Expr: expr}}}},
		{Cols: []*planpb.ColDef{{OnUpdate: &planpb.OnUpdate{Expr: expr}}}},
		{Checks: []*planpb.CheckDef{{Check: expr}}},
	} {
		pn := &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{TableDef: owner}}}}}
		data, err := pn.Marshal()
		require.NoError(t, err)
		cached := &planpb.Plan{}
		require.NoError(t, cached.Unmarshal(data))
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion120)
		require.NoError(t, validateIncompleteTemporalStringProtocol(c.proc, cached))
		// Reuse the same logical generation after the effective floor rolls back.
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion119)
		compile := &Compile{proc: c.proc, pn: cached}
		require.ErrorContains(t, compile.Compile(context.Background(), cached, nil), "version 120")
		_, err = compile.Run(0)
		require.ErrorContains(t, err, "version 120")
		for _, version := range []any{nil, "120", defines.MORPCVersion119} {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			require.ErrorContains(t, validateIncompleteTemporalStringProtocol(c.proc, cached), "version 120")
			required, err := plan2.RequiredPersistedExpressionProtocolVersion(owner)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion120, required)
			require.ErrorContains(t, plan2.RequirePersistedExpressionProtocol(c.proc.Ctx, c.proc, owner), "version 120")
		}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion120)
		require.NoError(t, plan2.RequirePersistedExpressionProtocol(c.proc.Ctx, c.proc, owner))
	}
	require.ErrorContains(t, validateIncompleteTemporalStringProtocol(nil, expr), "version 120")
	typed, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "week", []*planpb.Expr{
		{Typ: types.T_date.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}, plan2.MakePlan2Int64ConstExprWithType(3),
	})
	require.NoError(t, err)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion119)
	require.NoError(t, validateIncompleteTemporalStringProtocol(c.proc, typed))
}

func TestIncompleteTemporalWeekSessionSnapshot(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	source := &planpb.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	for _, explicit := range []bool{false, true} {
		args := []*planpb.Expr{source}
		if explicit {
			args = append(args, plan2.MakePlan2Int64ConstExprWithType(3))
		}
		expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "week", args)
		require.NoError(t, err)
		features, err := planpb.RequiredRemoteExpressionFeatures(expr)
		require.NoError(t, err)
		require.Equal(t, !explicit, features.WeekSessionDefault)
		c.proc.Base.SessionInfo.DefaultWeekFormatSet = false
		err = validateRemoteExpressionFeatures(c.proc, features)
		if explicit {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "session snapshot")
		}
		c.proc.Base.SessionInfo.DefaultWeekFormatSet = true
		require.NoError(t, validateRemoteExpressionFeatures(c.proc, features))
	}
}
