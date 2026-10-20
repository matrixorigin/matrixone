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
	planbuilder "github.com/matrixorigin/matrixone/pkg/sql/plan"
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

// These tests simulate capability responses and serialize/decode scopes; they
// do not run an old binary or prove physical mixed-binary execution.
func TestJSONMinMaxCapabilityGateCoversAggregateAndWindowPlans(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	qry := &plan.Query{Nodes: []*plan.Node{{
		AggList:     []*plan.Expr{jsonMinMaxPlanExpr(aggexec.AggIdOfMin)},
		WinSpecList: []*plan.Expr{jsonMinMaxWindowExpr(aggexec.AggIdOfMax)},
	}}}
	require.True(t, hasJSONMinMaxAggregate(qry.Nodes[0]))

	for _, version := range []int64{defines.MORPCVersion75, defines.MORPCVersion80, defines.MORPCVersion81, defines.MORPCVersion82, defines.MORPCVersion83, defines.MORPCVersion84, defines.MORPCVersion85, defines.MORPCVersion86, defines.MORPCVersion87, defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion112, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126} {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		if version < defines.MORPCVersion126 {
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

	for _, version := range []int64{defines.MORPCVersion75, defines.MORPCVersion80, defines.MORPCVersion81, defines.MORPCVersion82, defines.MORPCVersion83, defines.MORPCVersion84, defines.MORPCVersion85, defines.MORPCVersion86, defines.MORPCVersion87, defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion112, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126} {
		client.version = version
		minimum := int64(0)
		data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &minimum)
		if version < defines.MORPCVersion126 {
			require.ErrorContains(t, err, "remote destination does not support versioned expression semantics (MORPC version 126)")
			continue
		}
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion126, minimum, "the execution stream must confirm the typed JSON comparator")
		wire := new(pipeline.Pipeline)
		require.NoError(t, wire.Unmarshal(data))
		require.Equal(t, aggexec.AggIdOfMin, wire.InstructionList[0].Agg.Aggs[0].Op)
		require.Equal(t, int32(types.T_json), wire.InstructionList[0].Agg.Aggs[0].Expr[0].Typ.Id)
	}

	// A placement result can retain an address after the CN inventory has
	// changed. The destination check must fail closed instead of trusting the
	// stale address or the last capability response.
	scope.NodeInfo.Addr = "stale:6001"
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err,
		"remote destination does not support versioned expression semantics (MORPC version 126)")
	scope.NodeInfo.Addr = "remote:6001"

	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion125)
	require.ErrorContains(t,
		validateRemoteAggregateProtocol(c.proc, operator.Aggs),
		"typed JSON MIN/MAX requires MORPC protocol version 126")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
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

func TestJSONMinMaxSharedFloorsLocalAndCachedAdmission(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	minID := int64(aggexec.AggIdOfMin)
	for _, id := range []int64{aggexec.AggIdOfMin, aggexec.AggIdOfMax, int64(uint64(minID) | uint64(1)<<63)} {
		expr := jsonMinMaxPlanExpr(id)
		features, err := plan.RequiredRemoteExpressionFeatures(expr)
		require.NoError(t, err)
		require.True(t, features.JSONMinMaxTypedComparison)
		require.Equal(t, defines.MORPCVersion126, requiredExpressionContractProtocolVersion(features))
		floor, err := planbuilder.RequiredPersistedExpressionProtocolVersion(expr)
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion126, floor)
		queryPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{AggList: []*plan.Expr{expr}}}}}}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion112)
		require.ErrorContains(t, validateJSONStringConsumerProtocol(c.proc, queryPlan), "protocol version 126")
		cached := &Compile{proc: c.proc, pn: queryPlan}
		require.ErrorContains(t, cached.Compile(context.Background(), queryPlan, nil), "protocol version 126")
		_, err = cached.Run(0)
		require.ErrorContains(t, err, "protocol version 126")
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
		require.NoError(t, validateJSONStringConsumerProtocol(c.proc, queryPlan))
	}
}

func TestJSONMinMaxActualPrefixKeepsELT125AndTyped126(t *testing.T) {
	elt := bindJSONStringConsumerExpr(t, "elt")
	eltFeatures, err := plan.RequiredRemoteExpressionFeatures(elt)
	require.NoError(t, err)
	require.True(t, eltFeatures.JSONStringConsumerOverload)
	require.False(t, eltFeatures.JSONMinMaxTypedComparison)
	require.Equal(t, defines.MORPCVersion125, requiredExpressionContractProtocolVersion(eltFeatures))
	minNode := &plan.Node{AggList: []*plan.Expr{jsonMinMaxPlanExpr(aggexec.AggIdOfMin)}}
	eltNode := &plan.Node{ProjectList: []*plan.Expr{elt}}
	for _, nodes := range [][]*plan.Node{{minNode, eltNode}, {eltNode, minNode}} {
		owner := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: nodes}}}
		features, err := plan.RequiredRemoteExpressionFeatures(owner)
		require.NoError(t, err)
		require.True(t, features.JSONStringConsumerOverload)
		require.True(t, features.JSONMinMaxTypedComparison)
		require.Equal(t, defines.MORPCVersion126, requiredExpressionContractProtocolVersion(features))
		floor, err := planbuilder.RequiredPersistedExpressionProtocolVersion(owner)
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion126, floor)
	}
}

// The internal shuffle decision must use the same typed floor as the outer
// expression fence. This is a component test, not an old-binary execution.
func TestJSONMinMaxShuffleUsesSharedTypedFloor(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	node, _ := newShuffleGroupTestNodes(16)
	node.AggList = []*plan.Expr{jsonMinMaxPlanExpr(aggexec.AggIdOfMin)}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
		}
	})
	for _, version := range []int64{defines.MORPCVersion111, defines.MORPCVersion112, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		require.Equal(t, version >= defines.MORPCVersion126, c.canCompileShuffleGroup(node))
	}
	// Ordinary string MIN is governed by its existing contracts, not typed JSON.
	node.AggList[0].GetF().Args[0].Typ.Id = int32(types.T_varchar)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion112)
	require.False(t, hasJSONMinMaxAggregate(node))
	require.True(t, c.canCompileShuffleGroup(node))
}
