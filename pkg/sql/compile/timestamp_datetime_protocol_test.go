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
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestTimestampDatetimeProtocolPlacementSendReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "timestamp", []*planpb.Expr{{Typ: types.T_datetime.ToTypeWithScale(3).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}})
	require.NoError(t, err)
	require.Equal(t, function.EncodeOverloadID(function.TIMESTAMP, 7), expr.GetF().Func.Obj)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.TimestampDatetimeResultContracts)
	require.True(t, features.Any())
	require.Equal(t, defines.MORPCVersion122, remoteExpressionProtocolVersion(features))
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	place := func(version int64, want plan2.ExecType) {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		require.Equal(t, want, c.execType)
	}
	for _, version := range []int64{defines.MORPCVersion112, defines.MORPCVersion121} {
		place(version, plan2.ExecTypeAP_ONECN)
		_, err = encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "version 122")
	}
	place(defines.MORPCVersion122, plan2.ExecTypeAP_MULTICN)
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	// Placement does not cache capability: a destination rollback rejects the send.
	client.version = defines.MORPCVersion113
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 122")
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, features), "versioned remote destination")
	client.customResponse = true
	client.response = &query.Response{}
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 122")
	require.Equal(t, client.calls, client.releases)

	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	data, err := p.Marshal()
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	// Recheck the source/receiver rollout floor, including a rollback after success.
	for _, version := range []int64{defines.MORPCVersion112, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion121} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		sourceErr := validateRemoteExpressionPipelineProtocol(c.proc, p)
		_, receiveErr := decodeScope(data, c.proc, true, nil)
		if version < defines.MORPCVersion122 {
			require.ErrorContains(t, sourceErr, "version 122")
			require.ErrorContains(t, receiveErr, "version 122")
		} else {
			require.NoError(t, sourceErr)
			require.NoError(t, receiveErr)
		}
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion122)
	// A pre-versioned PR expression cannot retain a released ID with a new vector.
	expr.GetF().Func.Obj = function.EncodeOverloadID(function.TIMESTAMP, 1)
	require.ErrorContains(t, c.constrainRemoteExpressionWorkers(qry), "rebinding")
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "contract mismatch")
}

func TestTimestampLegacyAndPairKeepTemporalProtocol(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	for oid := int32(0); oid <= 10; oid++ {
		result := types.T_timestamp
		if oid >= 5 {
			result = types.T_datetime
		}
		expr := &planpb.Expr{Typ: result.ToType().PlanType(), Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.TIMESTAMP, oid)}}}}
		features, err := planpb.RequiredRemoteExpressionFeatures(expr)
		require.NoError(t, err)
		want := defines.MORPCVersion98
		if oid >= 6 {
			want = defines.MORPCVersion122
		}
		require.Equal(t, want, remoteExpressionProtocolVersion(features))
		rt := moruntime.ServiceRuntime(c.proc.GetService())
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion113)
		if oid < 6 {
			require.NoError(t, validateRemoteExpressionFeatures(c.proc, features))
		}
		expr.Typ = types.T_time.ToType().PlanType()
		features, err = planpb.RequiredRemoteExpressionFeatures(expr)
		require.NoError(t, err)
		require.True(t, features.InvalidTemporalResultContract)
		require.ErrorContains(t, validateRemoteExpressionFeatures(c.proc, features), "contract mismatch")
	}
}

// These are the encoded contracts emitted by prepared binding (including
// legacy ADDTIME/SUBTIME identities), not newly invented function identities.
func TestTemporalResultProtocolCachedAdmissionAndTransport(t *testing.T) {
	for _, tc := range []struct {
		name         string
		id, overload int32
		input        types.T
		result       types.T
		changed      bool
	}{
		{"string addtime", function.ADDTIME, 9, types.T_varchar, types.T_time, true},
		{"datetime addtime", function.ADDTIME, 2, types.T_datetime, types.T_time, true},
		{"datetime subtime", function.SUBTIME, 2, types.T_datetime, types.T_time, true},
		{"native time", function.ADDTIME, 0, types.T_time, types.T_time, false},
		{"calendar result", function.ADDTIME, 2, types.T_datetime, types.T_datetime, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			expr := &planpb.Expr{Typ: tc.result.ToTypeWithScale(6).PlanType(), Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(tc.id, tc.overload)},
				Args: []*planpb.Expr{{Typ: tc.input.ToTypeWithScale(6).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}, plan2.MakePlan2StringConstExprWithType("02:00:00.000001")},
			}}}
			queryPlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{StmtType: planpb.Query_SELECT, Steps: []int32{0}, Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}}}}
			c.pn = queryPlan
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion121)
			err := validateTemporalResultProtocol(c.proc, queryPlan)
			if !tc.changed {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "version 122")
			require.ErrorContains(t, c.Compile(c.proc.Ctx, queryPlan, nil), "version 122")
			_, err = c.Run(0)
			require.ErrorContains(t, err, "version 122", "cached Run rejects before Prepare or statement effects")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion122)
			require.NoError(t, validateTemporalResultProtocol(c.proc, queryPlan))
			client.version = defines.MORPCVersion122
			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = []*planpb.Expr{expr}
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			var required int64
			wire, err := encodeRemoteScopeWithProtocol(scope, c.proc, &required)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion122, required, "actual encoded scope carries the stream requirement")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion121)
			_, err = decodeScope(wire, c.proc, true, nil)
			require.ErrorContains(t, err, "version 122")
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func TestCTASPopulationScalarAdmission(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else if v, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, v)
		}
	})
	for _, tc := range []struct {
		name       string
		capability any
		required   int64
		failure    string
	}{
		{"unaffected", int64(121), 0, ""},
		{"old", int64(121), 122, "version 122"},
		{"supported", int64(122), 122, ""},
		{"rollback", int64(121), 122, "version 122"},
		{"missing", nil, 122, "version 122"},
		{"invalid_capability", "122", 122, "version 122"},
		{"future", int64(122), 123, "version 123"},
		{"invalid_requirement", int64(122), -1, "requires rebinding"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.capability == nil {
				if value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion); ok {
					rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, value)
				}
			} else {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, tc.capability)
			}
			p := &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{DdlType: planpb.DataDefinition_CREATE_TABLE, Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{CreateAsSelectSql: "select 1", CtasExecutionProtocolVersion: tc.required}}}}}
			err := validateTemporalResultProtocol(c.proc, p)
			if tc.failure == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.failure)
			}
		})
	}
}

func TestTemporalAndDecimalIntegerDivisionCapabilitiesCompose(t *testing.T) {
	for _, tc := range []struct {
		name              string
		temporal, decimal bool
		required          int64
	}{
		{"decimal", false, true, defines.MORPCVersion121},
		{"temporal", true, false, defines.MORPCVersion122},
		{"mixed", true, true, defines.MORPCVersion122},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			var expressions []*planpb.Expr
			if tc.decimal {
				expr := decimalDivisionProtocolExpr(types.T_uint64)
				expr.Typ = planpb.Type{Id: int32(types.T_uint64), Width: 7, Scale: 0}
				expr.GetF().Func.Obj = function.EncodeOverloadID(function.INTEGER_DIV, function.DecimalIntegerDivUnsignedOverload)
				expressions = append(expressions, expr)
			}
			if tc.temporal {
				expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "timestamp", []*planpb.Expr{{Typ: types.T_datetime.ToTypeWithScale(3).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}})
				require.NoError(t, err)
				expressions = append(expressions, expr)
			}
			qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: expressions}}, Steps: []int32{0}}
			features, err := planpb.RequiredRemoteExpressionFeatures(qry)
			require.NoError(t, err)
			require.Equal(t, tc.decimal, features.DecimalIntegerDivisionSemantics)
			require.Equal(t, tc.temporal, features.TimestampDatetimeResultContracts)
			require.Equal(t, tc.required, remoteExpressionProtocolVersion(features))
			persisted, err := plan2.RequiredPersistedExpressionProtocolVersion(qry)
			require.NoError(t, err)
			require.Equal(t, tc.required, persisted)
			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = expressions
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			wire := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: expressions}}}
			for _, version := range []int64{defines.MORPCVersion121, defines.MORPCVersion122} {
				client.version = version
				moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(moruntime.MOProtocolVersion, version)
				c.execType = plan2.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
				_, sendErr := encodeRemoteScope(scope, c.proc)
				receiveErr := validateRemoteExpressionPipelineProtocol(c.proc, wire)
				if version < tc.required {
					require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
					require.ErrorContains(t, sendErr, "version 122")
					require.ErrorContains(t, receiveErr, "version 122")
				} else {
					require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
					require.NoError(t, sendErr)
					require.NoError(t, receiveErr)
				}
			}
			require.Equal(t, client.calls, client.releases)
		})
	}
}
