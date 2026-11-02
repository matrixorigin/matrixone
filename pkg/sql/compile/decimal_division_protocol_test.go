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

func decimalDivisionProtocolExpr(oid types.T) *planpb.Expr {
	return &planpb.Expr{Typ: planpb.Type{Id: int32(oid), Width: 16, Scale: 6}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.DIV, 0)},
		Args: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		},
	}}}
}

func TestDecimalDivisionProtocolPlacementSendAndReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := decimalDivisionProtocolExpr(types.T_decimal128)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	place := func(version int64) {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	}
	place(defines.MORPCVersion96)
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	place(defines.MORPCVersion97)
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	var wire pipeline.Pipeline
	require.NoError(t, wire.Unmarshal(data))
	require.Equal(t, int32(6), wire.InstructionList[0].ProjectList[0].Typ.Scale)
	client.version = defines.MORPCVersion96 // worker changed after placement
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	require.Equal(t, client.calls, client.releases)

	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion96)
	// An old sender can still send a legacy DIV/0 plan to a new receiver.
	// The receiver uses the plan's result scale, so rejecting it here would
	// break old-to-new queries throughout a rolling upgrade. The sender-side
	// check above protects new-to-old execution.
	legacy := decimalDivisionProtocolExpr(types.T_decimal128)
	legacy.Typ.Scale = 8
	p.InstructionList[0].ProjectList = []*planpb.Expr{legacy}
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
}

func TestDecimalDivisionProtocolUnknownWorkerFallsBack(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.customResponse = true
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{decimalDivisionProtocolExpr(types.T_decimal128)}}}, Steps: []int32{0}}
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
}

func TestDecimalIntegerDivisionProtocol(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := decimalDivisionProtocolExpr(types.T_uint64)
	expr.Typ = planpb.Type{Id: int32(types.T_uint64), Width: 7, Scale: 0}
	expr.GetF().Func.Obj = function.EncodeOverloadID(function.INTEGER_DIV, function.DecimalIntegerDivUnsignedOverload)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	client.version = defines.MORPCVersion120
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	client.version = defines.MORPCVersion121
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	var wire pipeline.Pipeline
	require.NoError(t, wire.Unmarshal(data))
	require.Equal(t, expr.GetF().Func.Obj, wire.InstructionList[0].ProjectList[0].GetF().Func.Obj)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion120)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &wire), "version 121")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion121)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, &wire))
	required, err := plan2.RequiredPersistedExpressionProtocolVersion(expr)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion121, required)
	t.Run("dependent_program", func(t *testing.T) {
		dependent := &planpb.Expr{Typ: expr.Typ, Expr: &planpb.Expr_Dependent{Dependent: &planpb.DependentExpr{
			ProgramId: 1, Inputs: []*planpb.Expr{expr},
		}}}
		features, err := planpb.RequiredRemoteExpressionFeatures(dependent)
		require.NoError(t, err)
		require.True(t, features.DependentPrograms)
		require.True(t, features.DecimalIntegerDivisionSemantics)
		floor, err := plan2.RequiredPersistedExpressionProtocolVersion(dependent)
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion121, floor)
		// DIV capability does not grant a worker the coordinator's program owner.
		op.ProjectList = []*planpb.Expr{dependent}
		defer func() { op.ProjectList = []*planpb.Expr{expr} }()
		_, err = encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "coordinator execution owner")
		data, err := (&pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{dependent}}}}).Marshal()
		require.NoError(t, err)
		_, err = decodeScope(data, c.proc, true, nil)
		require.ErrorContains(t, err, "coordinator execution owner")
	})
	// Historical overload zero remains admitted after immediate-predecessor rollback.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion120)
	expr.GetF().Func.Obj = function.EncodeOverloadID(function.INTEGER_DIV, 0)
	expr.Typ = planpb.Type{Id: int32(types.T_int64), Width: 64, Scale: -1}
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}))
	require.Equal(t, client.calls, client.releases)
}
