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
	overload := int32(0)
	if id == function.EXTRACT {
		overload = 5
	}
	if (id == function.ADDTIME || id == function.SUBTIME) && firstType.IsMySQLString() {
		overload = 9
		if id == function.SUBTIME {
			overload = 11
		}
	}
	return &planpb.Expr{Typ: planpb.Type{Id: int32(resultType)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(id, overload)},
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
	require.Equal(t, defines.MORPCVersion97, temporalExpressionProtocolVersion(features))
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{unitExpr, weekExpr}}}, Steps: []int32{0}}
	client.version = defines.MORPCVersion96
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainTemporalResultWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)

	c.proc.Base.SessionInfo.DefaultWeekFormat = 3
	c.proc.Base.SessionInfo.DefaultWeekFormatSet = true
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{unitExpr, weekExpr}}}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion96)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "version 97")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion97)
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

func TestTypedNumericIntervalRequiresTemporalProtocol(t *testing.T) {
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
	require.True(t, features.NormalizedIntervalUnits)
	require.Equal(t, defines.MORPCVersion97, temporalExpressionProtocolVersion(features))

	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	client.version = defines.MORPCVersion96
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainTemporalResultWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)

	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion96)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "version 97")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion97)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
}

func TestRawTimeIntervalOverloadsRequireV97(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	for _, functionID := range []int32{function.DATE_ADD, function.DATE_SUB} {
		for _, overloadID := range []int32{8, 15} {
			expr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_time), Scale: 6}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(functionID, overloadID)},
				Args: []*planpb.Expr{
					{Typ: planpb.Type{Id: int32(types.T_time)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
					{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
					{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: int64(types.Second)}}}},
				},
			}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(expr)
			require.NoError(t, err)
			require.True(t, features.NormalizedIntervalUnits)
			require.Equal(t, defines.MORPCVersion97, temporalExpressionProtocolVersion(features))
			qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
			client.version = defines.MORPCVersion96
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainTemporalResultWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
		}
	}
}

func TestRawDayFieldOverloadRequiresV97(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	for _, id := range []int32{function.DAY, function.YEAR, function.MONTH} {
		expr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint8)}, Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(id, 2)},
			Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}},
		}}}
		features, err := planpb.RequiredRemoteExpressionFeatures(expr)
		require.NoError(t, err)
		require.True(t, features.TemporalResultContracts)
		require.Equal(t, defines.MORPCVersion97, temporalExpressionProtocolVersion(features))
		qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		client.version = defines.MORPCVersion96
		require.NoError(t, c.constrainTemporalResultWorkers(qry))
		require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
		op := projection.NewArgument()
		op.ProjectList = []*planpb.Expr{expr}
		scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
		_, err = encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "temporal result contracts")
		client.version = defines.MORPCVersion97
		_, err = encodeRemoteScope(scope, c.proc)
		require.NoError(t, err)
		op.Release()
	}
	require.Equal(t, client.calls, client.releases)
}

func TestRelease42TemporalProtocolBoundary(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	for _, tc := range []struct {
		name         string
		id, overload int32
		result       types.T
	}{
		{"date text cast", function.CAST, 0, types.T_date},
		{"time text cast", function.CAST, 0, types.T_time},
		{"datetime text cast", function.CAST, 0, types.T_datetime},
		{"timestamp text cast", function.CAST, 0, types.T_timestamp},
		{"timestampdiff", function.TIMESTAMPDIFF, 0, types.T_int64},
		{"date conversion", function.DATE, 0, types.T_date},
		{"time conversion", function.TIME, 0, types.T_time},
		{"microsecond parser", function.MICROSECOND, 0, types.T_int64},
		{"last day parser", function.LAST_DAY, 0, types.T_varchar},
		{"from days sentinel", function.FROM_DAYS, 0, types.T_date},
		{"yearweek modes", function.YEARWEEK, 0, types.T_uint32},
		{"period arithmetic", function.PERIOD_ADD, 0, types.T_int64},
		{"unix conversion", function.UNIX_TIMESTAMP, 0, types.T_int64},
		{"dynamic date parser", function.STR_TO_DATE, 0, types.T_datetime},
		{"date formatter", function.DATE_FORMAT, 0, types.T_varchar},
		{"time formatter", function.TIME_FORMAT, 0, types.T_varchar},
		{"typed addtime", function.ADDTIME, 0, types.T_time},
		{"typed subtime", function.SUBTIME, 0, types.T_time},
		{"typed timediff", function.TIMEDIFF, 0, types.T_time},
		{"datetime addtime", function.ADDTIME, 3, types.T_datetime},
		{"datetime subtime", function.SUBTIME, 3, types.T_datetime},
		{"calendar add", function.DATE_ADD, 0, types.T_date},
		{"calendar sub", function.DATE_SUB, 0, types.T_date},
		{"timestampadd", function.TIMESTAMPADD, 0, types.T_datetime},
		{"timestamp pair", function.TIMESTAMP, 5, types.T_datetime},
		{"maketime", function.MAKETIME, 0, types.T_time},
		{"time integer add", function.DATE_ADD, 5, types.T_time},
		{"time integer sub", function.DATE_SUB, 6, types.T_time},
		{"numeric extract", function.EXTRACT, 5, types.T_int64},
		{"string addtime", function.ADDTIME, 9, types.T_varchar},
		{"prepared addtime", function.ADDTIME, 9, types.T_time},
		{"string subtime", function.SUBTIME, 11, types.T_varchar},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &planpb.Expr{Typ: planpb.Type{Id: int32(tc.result)}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(tc.id, tc.overload)},
			}}}
			if tc.id == function.CAST {
				expr.GetF().Args = []*planpb.Expr{
					{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
					{Typ: expr.Typ, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
				}
			}
			qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = []*planpb.Expr{expr}
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			// 4.2.0/4.2.1 use 9, 4.2.2..4.2.4 use 10. Also retain the direct
			// protocol-boundary negative control without calling it a release.
			for _, version := range []int64{9, 10, defines.MORPCVersion96, defines.MORPCVersion97} {
				client.version = version
				rt := moruntime.ServiceRuntime(c.proc.GetService())
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				pipeline := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
				receiveErr := validateRemoteExpressionPipelineProtocol(c.proc, pipeline)
				if version < defines.MORPCVersion97 {
					require.Error(t, receiveErr)
				} else {
					require.NoError(t, receiveErr)
				}
				c.execType = plan2.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				require.NoError(t, c.constrainTemporalResultWorkers(qry))
				_, err := encodeRemoteScope(scope, c.proc)
				if version < defines.MORPCVersion97 {
					require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
					require.ErrorContains(t, err, "temporal")
				} else {
					require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
					require.NoError(t, err)
				}
			}
			floor, err := plan2.RequiredPersistedExpressionProtocolVersion(expr)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion97, floor)
		})
	}
	// Previously serialized physical types are still valid on a new reader.
	for _, tc := range []struct {
		id, overload int32
		result       types.T
	}{
		{function.EXTRACT, 0, types.T_varchar}, {function.EXTRACT, 1, types.T_uint32},
		{function.EXTRACT, 2, types.T_varchar}, {function.EXTRACT, 3, types.T_varchar},
		{function.EXTRACT, 4, types.T_varchar}, {function.ADDTIME, 6, types.T_datetime},
		{function.SUBTIME, 6, types.T_datetime},
	} {
		expr := &planpb.Expr{Typ: planpb.Type{Id: int32(tc.result)}, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(tc.id, tc.overload)}}}}
		floor, err := plan2.RequiredPersistedExpressionProtocolVersion(expr)
		require.NoError(t, err)
		require.Zero(t, floor)
		p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
		require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
	}
	require.Equal(t, client.calls, client.releases)
}

func TestTemporalResultProtocolPlacementAndReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := temporalResultProtocolExpr(function.ADDTIME, types.T_varchar, types.T_varchar)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.TemporalResultContracts)
	require.False(t, features.InvalidTemporalResultContract)
	extract := temporalResultProtocolExpr(function.EXTRACT, types.T_varchar, types.T_int64)
	extractFeatures, err := planpb.RequiredRemoteExpressionFeatures(extract)
	require.NoError(t, err)
	require.True(t, extractFeatures.TemporalResultContracts)
	require.False(t, extractFeatures.InvalidTemporalResultContract)
	typed := temporalResultProtocolExpr(function.ADDTIME, types.T_time, types.T_time)
	typedFeatures, err := planpb.RequiredRemoteExpressionFeatures(typed)
	require.NoError(t, err)
	require.True(t, typedFeatures.TemporalResultContracts)

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
		require.True(t, legacyFeatures.InvalidTemporalResultContract)
		require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "temporal result vector")
		data, err := p.Marshal()
		require.NoError(t, err)
		_, err = decodeScope(data, c.proc, true, nil)
		require.ErrorContains(t, err, "temporal result vector")
	}
	require.Equal(t, client.calls, client.releases)
}
