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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func jsonMinMaxPlanExpr(id int64) *plan.Expr {
	arg := &plan.Expr{Typ: plan.Type{Id: int32(types.T_json)}}
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_json)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: id},
			Args: []*plan.Expr{arg},
		}},
	}
}

func jsonMinMaxWindowExpr(id int64) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_W{W: &plan.WindowSpec{WindowFunc: jsonMinMaxPlanExpr(id)}},
	}
}

func TestJSONMinMaxCapabilityGateCoversAggregateAndWindowPlans(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	qry := &plan.Query{Nodes: []*plan.Node{{
		AggList:     []*plan.Expr{jsonMinMaxPlanExpr(aggexec.AggIdOfMin)},
		WinSpecList: []*plan.Expr{jsonMinMaxWindowExpr(aggexec.AggIdOfMax)},
	}}}
	require.True(t, queryUsesJSONMinMax(qry))

	for _, version := range []int64{defines.MORPCVersion74, defines.MORPCVersion75} {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainJSONMinMaxWorkers(qry))
		if version < defines.MORPCVersion75 {
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			require.Equal(t, c.addr, c.cnList[0].Addr)
		} else {
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
		}
	}
}

func TestJSONMinMaxRemoteDestinationRejectsLegacyComparator(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	operator := group.NewArgument()
	defer operator.Release()
	operator.NeedEval = true
	operator.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(
			aggexec.AggIdOfMin,
			false,
			[]*plan.Expr{{Typ: plan.Type{Id: int32(types.T_json)}}},
			nil,
		),
	}
	scope := &Scope{
		Magic:    Remote,
		Proc:     c.proc,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   operator,
	}

	for _, version := range []int64{defines.MORPCVersion74, defines.MORPCVersion75} {
		client.version = version
		data, err := encodeRemoteScope(scope, c.proc)
		if version < defines.MORPCVersion75 {
			require.ErrorContains(t, err, "JSON MIN/MAX remote execution requires MORPC protocol version 75")
			continue
		}
		require.NoError(t, err)
		wire := new(pipeline.Pipeline)
		require.NoError(t, wire.Unmarshal(data))
		require.Equal(t, aggexec.AggIdOfMin, wire.InstructionList[0].Agg.Aggs[0].Op)
		require.Equal(t, int32(types.T_json), wire.InstructionList[0].Agg.Aggs[0].Expr[0].Typ.Id)
	}

	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion74)
	require.ErrorContains(t,
		validateRemoteAggregateProtocol(c.proc, operator.Aggs),
		"JSON MIN/MAX remote execution requires MORPC protocol version 75")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion75)
	require.NoError(t, validateRemoteAggregateProtocol(c.proc, operator.Aggs))
}

func TestPipelineJSONMinMaxFenceFindsForwardedAggregate(t *testing.T) {
	arg := &plan.Expr{Typ: plan.Type{Id: int32(types.T_json)}}
	aggs := convertToPipelineAggregates([]aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMax, false, []*plan.Expr{arg}, nil),
	})
	child := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{
		{Agg: &pipeline.Group{Aggs: aggs}},
	}}
	require.True(t, pipelineUsesJSONMinMax(&pipeline.Pipeline{Children: []*pipeline.Pipeline{child}}))
	aggs[0].Expr = []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_varchar)}}}
	require.False(t, pipelineUsesJSONMinMax(child))
}
