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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConvBasesPlacementAndActualSender(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "conv", []*planpb.Expr{
		{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 10}}}},
	})
	require.NoError(t, err)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, version := range []int64{defines.MORPCVersion64, defines.MORPCVersion66} {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainConvBasesWorkers(qry))
		data, err := encodeRemoteScope(scope, c.proc)
		if version < defines.MORPCVersion66 {
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			require.Equal(t, c.addr, c.cnList[0].Addr)
			require.ErrorContains(t, err, "remote destination")
		} else {
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			require.NoError(t, err)
			wire := new(pipeline.Pipeline)
			require.NoError(t, wire.Unmarshal(data))
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion64)
			require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wire), "version 66")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion66)
		}
	}
	client.version = defines.MORPCVersion64
	_, err = encodeRemoteScope(scope, c.proc)
	require.Error(t, err, "a downgrade after placement must be fenced")
	require.Positive(t, client.calls)
	require.Equal(t, client.calls, client.releases)
	// Existing constant INT64 bases do not require the new capability.
	expr.GetF().Args[1] = &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 16}}}}
	calls := client.calls
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.Equal(t, calls, client.calls)
	expr.GetF().Args[1].Typ.Id = int32(types.T_uint64)
	features, err := planpb.RequiredRemoteExpressionFeatures(qry)
	require.NoError(t, err)
	require.True(t, features.RowDependentConvBases)
}
