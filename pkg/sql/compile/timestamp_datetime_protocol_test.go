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
	"testing"
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
	require.Equal(t, defines.MORPCVersion114, remoteExpressionProtocolVersion(features))
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
	for _, version := range []int64{defines.MORPCVersion112, defines.MORPCVersion113} {
		place(version, plan2.ExecTypeAP_ONECN)
		_, err = encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "version 114")
	}
	place(defines.MORPCVersion114, plan2.ExecTypeAP_MULTICN)
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	// Placement does not cache capability: a destination rollback rejects the send.
	client.version = defines.MORPCVersion113
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 114")
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, features), "versioned remote destination")
	client.customResponse = true
	client.response = &query.Response{}
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 114")
	require.Equal(t, client.calls, client.releases)

	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	data, err := p.Marshal()
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	// Recheck the source/receiver rollout floor, including a rollback after success.
	for _, version := range []int64{defines.MORPCVersion112, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion113} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		sourceErr := validateRemoteExpressionPipelineProtocol(c.proc, p)
		_, receiveErr := decodeScope(data, c.proc, true, nil)
		if version < defines.MORPCVersion114 {
			require.ErrorContains(t, sourceErr, "version 114")
			require.ErrorContains(t, receiveErr, "version 114")
		} else {
			require.NoError(t, sourceErr)
			require.NoError(t, receiveErr)
		}
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion114)
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
			want = defines.MORPCVersion114
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
