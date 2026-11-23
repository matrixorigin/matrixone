// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestJSONNumericComparisonProtocol(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	for _, name := range []string{"=", "!=", "<=>", "<", "<=", ">", ">="} {
		t.Run(name, func(t *testing.T) {
			args := []*planpb.Expr{
				{Typ: types.T_json.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
				{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
			}
			expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, name, args)
			require.NoError(t, err)
			_, overload := function.DecodeOverloadID(expr.GetF().Func.Obj)
			require.Equal(t, int32(1), overload, "older registries must reject this appended execution identity")
			features, err := planpb.RequiredRemoteExpressionFeatures(expr)
			require.NoError(t, err)
			require.True(t, features.JSONNumericComparisons)
			require.Equal(t, defines.MORPCVersion128, remoteExpressionProtocolVersion(features))
			if name == "=" {
				view := &planpb.Expr{Typ: types.T_text.ToType().PlanType(), Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.MO_VIEW_DEFINITION, 0)},
				}}}
				viewFeatures, err := planpb.RequiredRemoteExpressionFeatures(view)
				require.NoError(t, err)
				require.Equal(t, defines.MORPCVersion126, remoteExpressionProtocolVersion(viewFeatures))
				combined, err := planpb.RequiredRemoteExpressionFeatures([]*planpb.Expr{view, expr})
				require.NoError(t, err)
				require.True(t, combined.ViewMetadataFunctions)
				require.True(t, combined.JSONNumericComparisons)
				require.Equal(t, defines.MORPCVersion128, remoteExpressionProtocolVersion(combined))
				required, err := plan2.RequiredPersistedExpressionProtocolVersion([]*planpb.Expr{view, expr})
				require.NoError(t, err)
				require.Equal(t, defines.MORPCVersion128, required)
			}
			require.ErrorContains(t, validateRemoteExpressionFeatures(nil, features), "version 128")
			require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, features), "versioned remote destination")
			qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = []*planpb.Expr{expr}
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion128)
			client.version = defines.MORPCVersion126
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			_, err = encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "version 128")
			client.version = defines.MORPCVersion128
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			_, err = encodeRemoteScope(scope, c.proc)
			require.NoError(t, err)
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
			data, err := p.Marshal()
			require.NoError(t, err)
			for _, version := range []int64{defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				err := validateRemoteExpressionPipelineProtocol(c.proc, p)
				_, decodeErr := decodeScope(data, c.proc, true, nil)
				if version < defines.MORPCVersion128 {
					require.ErrorContains(t, err, "version 128")
					require.ErrorContains(t, decodeErr, "version 128")
				} else {
					require.NoError(t, err)
					require.NoError(t, decodeErr)
				}
			}
		})
	}
	for _, adapter := range []string{function.JsonComparisonParamFunctionName, function.JsonOrderingParamFunctionName} {
		expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, adapter, []*planpb.Expr{{Typ: types.T_text.ToType().PlanType(), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}})
		require.NoError(t, err)
		_, overload := function.DecodeOverloadID(expr.GetF().Func.Obj)
		require.Equal(t, int32(1), overload)
		features, err := planpb.RequiredRemoteExpressionFeatures(expr)
		require.NoError(t, err)
		require.True(t, features.JSONNumericComparisons)
	}
}

func TestJSONNumericJoinConditionRemainsResidual(t *testing.T) {
	col := func(typ types.T, rel, pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: typ.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rel, ColPos: pos}}}
	}
	bind := func(args ...*planpb.Expr) *planpb.Expr {
		e, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "=", args)
		require.NoError(t, err)
		return e
	}
	mixed := bind(col(types.T_json, 0, 0), col(types.T_int64, 1, 0))
	residual, keys := extraJoinConditions([]*planpb.Expr{mixed})
	require.Empty(t, keys)
	require.Equal(t, mixed, residual)
	key := bind(col(types.T_int64, 0, 1), col(types.T_int64, 1, 1))
	residual, keys = extraJoinConditions([]*planpb.Expr{mixed, key})
	require.Equal(t, []*planpb.Expr{key}, keys)
	require.Equal(t, mixed, residual)
}
