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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestPipelineRequiresJSONAggregateOpaqueValues(t *testing.T) {
	binaryValue := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_binary)}}
	textValue := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}}
	array := &pipeline.Aggregate{Op: aggexec.AggIdOfJsonArrayAgg, Expr: []*planpb.Expr{binaryValue}}
	objectKeyOnly := &pipeline.Aggregate{Op: aggexec.AggIdOfJsonObjectAgg, Expr: []*planpb.Expr{binaryValue, textValue}}
	objectValue := &pipeline.Aggregate{Op: aggexec.AggIdOfJsonObjectAgg, Expr: []*planpb.Expr{textValue, binaryValue}}

	p := &pipeline.Pipeline{
		InstructionList: []*pipeline.Instruction{{
			Agg: &pipeline.Group{Aggs: []*pipeline.Aggregate{objectKeyOnly}},
		}},
		Children: []*pipeline.Pipeline{{
			InstructionList: []*pipeline.Instruction{{
				Agg: &pipeline.Group{Aggs: []*pipeline.Aggregate{array}},
			}},
		}},
	}
	required, err := jsonAggregateOpaqueRequirement(p)
	require.NoError(t, err)
	require.True(t, required)

	p.InstructionList[0].Agg.Aggs[0] = objectValue
	p.Children = nil
	required, err = jsonAggregateOpaqueRequirement(p)
	require.NoError(t, err)
	require.True(t, required)

	p.InstructionList[0].Agg.Aggs[0] = objectKeyOnly
	required, err = jsonAggregateOpaqueRequirement(p)
	require.NoError(t, err)
	require.False(t, required)
}

func TestJSONAggregateOpaqueRejectsPreviousCapability(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	qry := jsonAggregateOpaqueTestQuery()
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	worker := engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}

	// MORPC v75 and v76 do not include this aggregate executor.
	for _, version := range []int64{defines.MORPCVersion75, defines.MORPCVersion76} {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = worker
		require.NoError(t, c.constrainJSONAggregateOpaqueWorkers(qry))
		require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
		require.Len(t, c.cnList, 1)
		require.Equal(t, c.addr, c.cnList[0].Addr)
	}

	scope := &Scope{
		Magic:    Remote,
		Proc:     c.proc,
		NodeInfo: worker[0],
		RootOp:   value_scan.NewArgument(),
		Plan:     &planpb.Plan{Plan: &planpb.Plan_Query{Query: qry}},
	}
	t.Cleanup(scope.release)

	// Keep the coordinator at the current implementation capability so the
	// sender's destination probe is the failing boundary.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "MORPC protocol version 77")

	wire := jsonAggregateOpaqueTestPipeline()
	// The receiver-side gates must reject the same lowered plan from either
	// pre-aggregate capability.
	for _, version := range []int64{defines.MORPCVersion75, defines.MORPCVersion76} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		require.ErrorContains(t,
			validateJSONAggregateOpaquePipelineProtocol(c.proc, wire),
			"MORPC protocol version 77")
		require.ErrorContains(t,
			validateJSONAggregateOpaqueAggregateProtocol(c.proc,
				[]aggexec.AggFuncExecExpression{jsonAggregateOpaqueTestAgg()}),
			"MORPC protocol version 77")
	}

	// A v77 peer is admitted by every boundary, proving the new gate matches
	// the aggregate implementation carried by this branch.
	client.version = defines.MORPCVersion77
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion77)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = worker
	require.NoError(t, c.constrainJSONAggregateOpaqueWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	require.NoError(t, validateJSONAggregateOpaquePipelineProtocol(c.proc, wire))
	require.NoError(t, validateJSONAggregateOpaqueAggregateProtocol(c.proc,
		[]aggexec.AggFuncExecExpression{jsonAggregateOpaqueTestAgg()}))
}

func jsonAggregateOpaqueTestQuery() *planpb.Query {
	binaryValue := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_binary)}}
	array := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_json)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.JSON_ARRAYAGG, 0)},
			Args: []*planpb.Expr{binaryValue},
		}},
	}
	return &planpb.Query{
		Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{array}}},
		Steps: []int32{0},
	}
}

func jsonAggregateOpaqueTestAgg() aggexec.AggFuncExecExpression {
	return aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfJsonArrayAgg,
		false,
		[]*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_binary)}}},
		nil,
	)
}

func jsonAggregateOpaqueTestPipeline() *pipeline.Pipeline {
	return &pipeline.Pipeline{
		InstructionList: []*pipeline.Instruction{{
			Agg: &pipeline.Group{Aggs: []*pipeline.Aggregate{{
				Op:   aggexec.AggIdOfJsonArrayAgg,
				Expr: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_binary)}}},
			}}},
		}},
	}
}
