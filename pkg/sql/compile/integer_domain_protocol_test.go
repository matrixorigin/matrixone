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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIntegerArithmeticNegotiatesWorkerVersionAndFencesSender(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	ctx := function.WithNoUnsignedSubtraction(context.Background(), true)
	for _, name := range []string{"+", "-", "*"} {
		expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}}})
		require.NoError(t, err)
		qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
		client.version = defines.MORPCVersion64
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainIntegerDomainWorkers(qry))
		require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
		require.Len(t, c.cnList, 1)
		require.Equal(t, c.addr, c.cnList[0].Addr)
		if name == "-" {
			require.Equal(t, int32(types.T_int64), expr.Typ.Id)
		} else {
			require.Equal(t, int32(types.T_uint64), expr.Typ.Id)
		}
		scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: value_scan.NewArgument(), Plan: &planpb.Plan{Plan: &planpb.Plan_Query{Query: qry}}}
		_, err = encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "remote destination")
		client.version = defines.MORPCVersion66
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainIntegerDomainWorkers(qry))
		require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
		data, err := encodeRemoteScope(scope, c.proc)
		require.NoError(t, err)
		wire := new(pipeline.Pipeline)
		require.NoError(t, wire.Unmarshal(data))
		rt := moruntime.ServiceRuntime(c.proc.GetService())
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion64)
		require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wire), "version 66")
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion66)
		client.version = defines.MORPCVersion64
		_, err = encodeRemoteScope(scope, c.proc)
		require.Error(t, err)
	}
	require.Positive(t, client.calls)
	require.Equal(t, client.calls, client.releases)
}
func TestIntegerArithmeticFeatureLeavesLegacyOperatorsUnchanged(t *testing.T) {
	for _, id := range []int32{function.PLUS, function.MINUS, function.MULTI} {
		for _, overload := range []int32{0, 1, 2, 3} {
			expr := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: int64(id)<<32 | int64(overload)}}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(expr)
			require.NoError(t, err)
			require.Equal(t, overload == 2 || (id == function.MINUS && overload == 3), features.IntegerArithmeticDomains)
		}
	}
}
