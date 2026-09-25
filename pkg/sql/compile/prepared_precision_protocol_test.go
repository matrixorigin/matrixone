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

func preparedPrecisionExpr(functionID int32) *planpb.Expr {
	return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(functionID, 0)},
		Args: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			integerProtocolExpr(function.IntegerArgumentCastOverload),
		},
	}}}
}

func TestPreparedPrecisionProtocolPlacementAndSend(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := preparedPrecisionExpr(function.CEIL)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.PreparedPrecisionScalar)

	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	place := func(version int64) {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainPreparedPrecisionWorkers(qry))
	}
	place(defines.MORPCVersion94)
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	place(defines.MORPCVersion95)
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	client.version = defines.MORPCVersion94
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	require.Equal(t, client.calls, client.releases)

	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion94)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "version 95")
	data, err := p.Marshal()
	require.NoError(t, err)
	_, err = decodeScope(data, c.proc, true, nil)
	require.ErrorContains(t, err, "version 95")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion95)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
}

func temporalResultProtocolExpr(id int32, firstType, resultType types.T) *planpb.Expr {
	return &planpb.Expr{Typ: planpb.Type{Id: int32(resultType)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(id, 0)},
		Args: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(firstType)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		},
	}}}
}

func TestTemporalUnitAndWeekProtocolAdmission(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	unitExpr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.TO_INTERVAL_MICROSECOND, 0)},
		Args: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		},
	}}}
	weekExpr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint8)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.WEEK, 0)},
		Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_date)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}},
	}}}
	features, err := planpb.RequiredRemoteExpressionFeatures([]*planpb.Expr{unitExpr, weekExpr})
	require.NoError(t, err)
	require.True(t, features.NormalizedIntervalUnits)
	require.True(t, features.WeekSessionDefault)
	require.Equal(t, defines.MORPCVersion98, temporalExpressionProtocolVersion(features))
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{unitExpr, weekExpr}}}, Steps: []int32{0}}
	client.version = defines.MORPCVersion97
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainTemporalResultWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)

	c.proc.Base.SessionInfo.DefaultWeekFormat = 3
	c.proc.Base.SessionInfo.DefaultWeekFormatSet = true
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{unitExpr, weekExpr}}}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion97)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "version 98")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion98)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
	c.proc.Base.SessionInfo.DefaultWeekFormatSet = false
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "session snapshot")
	c.proc.Base.SessionInfo.DefaultWeekFormatSet = true
	legacy := *unitExpr
	legacyFn := *unitExpr.GetF()
	legacyRef := *legacyFn.Func
	legacyRef.Obj = function.EncodeOverloadID(function.TO_INTERVAL, 0)
	legacyFn.Func = &legacyRef
	legacy.Expr = &planpb.Expr_F{F: &legacyFn}
	p.InstructionList[0].ProjectList = []*planpb.Expr{&legacy}
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "legacy interval")
	qry.Nodes[0].ProjectList = []*planpb.Expr{&legacy}
	require.ErrorContains(t, c.constrainTemporalResultWorkers(qry), "legacy interval")
}

func TestTypedNumericIntervalRequiresNewRemoteOverload(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.TO_INTERVAL_MICROSECOND, 5)},
		Args: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_decimal64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		},
	}}}
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.TypedNumericIntervalOverloads)
	require.Equal(t, defines.MORPCVersion99, temporalExpressionProtocolVersion(features))

	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	client.version = defines.MORPCVersion98
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-v98-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainTemporalResultWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)

	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion98)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "version 99")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion99)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
}

func TestTemporalResultProtocolPlacementAndReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := temporalResultProtocolExpr(function.ADDTIME, types.T_varchar, types.T_varchar)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.TemporalResultContracts)
	require.False(t, features.LegacyTemporalResultContracts)
	extract := temporalResultProtocolExpr(function.EXTRACT, types.T_varchar, types.T_int64)
	extractFeatures, err := planpb.RequiredRemoteExpressionFeatures(extract)
	require.NoError(t, err)
	require.True(t, extractFeatures.TemporalResultContracts)
	require.False(t, extractFeatures.LegacyTemporalResultContracts)
	typed := temporalResultProtocolExpr(function.ADDTIME, types.T_time, types.T_time)
	typedFeatures, err := planpb.RequiredRemoteExpressionFeatures(typed)
	require.NoError(t, err)
	require.False(t, typedFeatures.TemporalResultContracts)

	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	client.version = defines.MORPCVersion96
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainTemporalResultWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "temporal result contracts")
	client.version = defines.MORPCVersion97
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainTemporalResultWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	scope.NodeInfo = engine.Node{Id: "old-worker", Addr: "remote:6001"}
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	client.version = defines.MORPCVersion96
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "temporal result contracts")

	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion96)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "version 97")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion97)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
	for _, legacy := range []*planpb.Expr{
		temporalResultProtocolExpr(function.EXTRACT, types.T_varchar, types.T_varchar),
		temporalResultProtocolExpr(function.ADDTIME, types.T_varchar, types.T_datetime),
		temporalResultProtocolExpr(function.SUBTIME, types.T_varchar, types.T_datetime),
	} {
		p.InstructionList[0].ProjectList = []*planpb.Expr{legacy}
		legacyFeatures, err := planpb.RequiredRemoteExpressionFeatures(legacy)
		require.NoError(t, err)
		require.True(t, legacyFeatures.LegacyTemporalResultContracts)
		require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "legacy temporal")
		data, err := p.Marshal()
		require.NoError(t, err)
		_, err = decodeScope(data, c.proc, true, nil)
		require.ErrorContains(t, err, "legacy temporal")
	}
	require.Equal(t, client.calls, client.releases)
}
