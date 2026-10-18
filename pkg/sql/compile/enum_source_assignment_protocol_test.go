// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestEnumSourceAssignmentProtocolPlacementSendReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	arg := &planpb.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: ""}}}}
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "cast_value_to_index", []*planpb.Expr{arg, arg, {Typ: types.T_enum.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}})
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.EnumSourceAssignment)
	features.TableCollationFunction = true
	features.LeastGreatestCollationSemantics = true
	require.Equal(t, defines.MORPCVersion123, remoteExpressionProtocolVersion(features))
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, version := range []int64{122, 123, 122} {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{scope.NodeInfo}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		_, err := encodeRemoteScope(scope, c.proc)
		if version < 123 {
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			require.ErrorContains(t, err, "version 123")
		} else {
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			require.NoError(t, err)
		}
	}

	// Placement is not an admission cache: actual destination replacement is rechecked.
	client.version = defines.MORPCVersion123
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	client.version = defines.MORPCVersion122
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 123")
	originalCtx := c.proc.Ctx
	canceled, cancel := context.WithCancel(originalCtx)
	cancel()
	c.proc.Ctx = canceled
	_, err = encodeRemoteScope(scope, c.proc)
	require.Error(t, err)
	c.proc.Ctx = originalCtx
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, features), "versioned remote destination")
	require.ErrorContains(t, validateRemoteExpressionDestination(nil, &pipeline.Pipeline{Node: &pipeline.NodeInfo{}}, features), "version 123")
	client.customResponse = true
	client.response = &query.Response{}
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 123", "an unknown destination fails closed")
	require.Equal(t, client.calls, client.releases)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, nested := range []bool{false, true} {
		p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{Op: int32(vm.Projection), ProjectList: []*planpb.Expr{expr}}}}
		if nested {
			p = &pipeline.Pipeline{Children: []*pipeline.Pipeline{p}}
		}
		data, err := p.Marshal()
		require.NoError(t, err)
		require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(nil, p), "version 123")
		for _, version := range []any{nil, "123", int64(122), int64(123)} {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			decoded, err := decodeScope(data, c.proc, true, nil)
			if decoded != nil {
				decoded.release()
			}
			if version == int64(123) {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "version 123")
				require.Nil(t, decoded, "reject before allocating operator owners")
			}
		}
	}
}

func TestEnumSourceAssignmentLocalAndCachedAdmission(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	arg := plan2.MakePlan2StringConstExprWithType("red,blue")
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "cast_value_to_index", []*planpb.Expr{arg, arg, {Typ: types.T_enum.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}})
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, pn := range []*planpb.Plan{
		{Plan: &planpb.Plan_Query{Query: &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}}}},
		{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}}}}}}},
	} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion123)
		require.NoError(t, validateEnumSourceAssignmentProtocol(c.proc, pn))
		require.Zero(t, testing.AllocsPerRun(10, func() { require.NoError(t, validateEnumSourceAssignmentProtocol(c.proc, pn)) }))
		c.pn = pn
		for _, version := range []any{int64(122), nil, "123"} {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			require.ErrorContains(t, c.Compile(context.Background(), pn, nil), "protocol version 123")
			_, err := c.Run(0)
			require.ErrorContains(t, err, "protocol version 123", "cached execution rechecks a closed capability")
		}
	}
	legacy, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "cast_value_to_index", []*planpb.Expr{arg, arg})
	require.NoError(t, err)
	require.NoError(t, validateEnumSourceAssignmentProtocol(nil, legacy))
	require.ErrorContains(t, validateEnumSourceAssignmentProtocol(nil, expr), "protocol version 123")
}
