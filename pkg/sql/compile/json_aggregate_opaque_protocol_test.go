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
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	queryclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
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

	// Current main v80 and the reserved v81 do not include this aggregate
	// executor. The gate must not reuse a version already assigned to another
	// wire contract.
	for _, version := range []int64{defines.MORPCVersion80, defines.MORPCVersion81} {
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
	require.ErrorContains(t, err, "MORPC protocol version 82")

	wire := jsonAggregateOpaqueTestPipeline()
	// The receiver-side gates must reject the same lowered plan from either
	// current-main or the reserved pre-aggregate capability.
	for _, version := range []int64{defines.MORPCVersion80, defines.MORPCVersion81} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		require.ErrorContains(t,
			validateJSONAggregateOpaquePipelineProtocol(c.proc, wire),
			"MORPC protocol version 82")
		require.ErrorContains(t,
			validateJSONAggregateOpaqueAggregateProtocol(c.proc,
				[]aggexec.AggFuncExecExpression{jsonAggregateOpaqueTestAgg()}),
			"MORPC protocol version 82")
	}

	// A v82 peer is admitted by every boundary, proving the new gate matches
	// the aggregate implementation carried by this branch.
	client.version = defines.MORPCVersion82
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion82)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = worker
	require.NoError(t, c.constrainJSONAggregateOpaqueWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	require.NoError(t, validateJSONAggregateOpaquePipelineProtocol(c.proc, wire))
	require.NoError(t, validateJSONAggregateOpaqueAggregateProtocol(c.proc,
		[]aggexec.AggFuncExecExpression{jsonAggregateOpaqueTestAgg()}))
}

func TestJSONAggregateOpaqueRealMixedVersionPeer(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 4
	coordinatorService := c.proc.GetService()
	rt := moruntime.ServiceRuntime(coordinatorService)
	oldVersion, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldCluster, hadCluster := rt.GetGlobalVariables(moruntime.ClusterService)

	workerID := "json-aggregate-opaque-real-peer"
	workerPipelineAddress := "json-aggregate-opaque-real-peer:pipeline"
	workerQueryAddress := "unix:///tmp/mo-json-opaque-real-peer.sock"
	require.NoError(t, os.RemoveAll(workerQueryAddress[len("unix://"):]))
	workerRT := moruntime.ServiceRuntime(workerID)
	if workerRT == nil {
		moruntime.SetupServiceBasedRuntime(workerID, moruntime.DefaultRuntime())
		workerRT = moruntime.ServiceRuntime(workerID)
	}
	oldWorkerVersion, hadWorkerVersion := workerRT.GetGlobalVariables(moruntime.MOProtocolVersion)
	workerRT.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion80)

	cluster := clusterservice.NewMOCluster(
		coordinatorService,
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{{
			ServiceID:              workerID,
			QueryAddress:           workerQueryAddress,
			PipelineServiceAddress: workerPipelineAddress,
		}}, nil))
	rt.SetGlobalVariables(moruntime.ClusterService, cluster)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion82)

	qs, err := queryservice.NewQueryService(workerID, workerQueryAddress, morpc.Config{})
	require.NoError(t, err)
	require.NoError(t, qs.Start())
	queryClient, err := queryclient.NewQueryClient(coordinatorService, morpc.Config{})
	require.NoError(t, err)
	c.proc.Base.QueryClient = queryClient

	worker := engine.Nodes{{
		Id:   workerID,
		Addr: workerPipelineAddress,
		Mcpu: 4,
	}}
	qry := jsonAggregateOpaqueTestQuery()
	t.Cleanup(func() {
		require.NoError(t, queryClient.Close())
		require.NoError(t, qs.Close())
		cluster.Close()
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		if hadWorkerVersion {
			workerRT.SetGlobalVariables(moruntime.MOProtocolVersion, oldWorkerVersion)
		} else {
			workerRT.CompareAndDeleteGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCVersion82)
		}
		if hadCluster {
			rt.SetGlobalVariables(moruntime.ClusterService, oldCluster)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.ClusterService, cluster)
		}
	})

	// This is a real query-service MORPC peer. Its advertised v80 is the
	// current-main capability boundary, so the coordinator must fail closed
	// and fall back to one CN instead of trusting a local mock response.
	supported, err := remoteWorkersSupportProtocol(
		c.proc, worker, defines.MORPCVersion82)
	require.NoError(t, err)
	require.False(t, supported)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = worker
	require.NoError(t, c.constrainJSONAggregateOpaqueWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	require.Equal(t, c.addr, c.cnList[0].Addr)

	scope := &Scope{
		Magic:    Remote,
		Proc:     c.proc,
		NodeInfo: worker[0],
		RootOp:   value_scan.NewArgument(),
		Plan:     &planpb.Plan{Plan: &planpb.Plan_Query{Query: qry}},
	}
	t.Cleanup(scope.release)
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "MORPC protocol version 82")

	// Rolling the same live peer to v82 makes the real destination probe and
	// placement admission succeed. The receiver-local gate is checked at both
	// advertised versions as well.
	workerRT.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion82)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion80)
	wire := jsonAggregateOpaqueTestPipeline()
	require.ErrorContains(t,
		validateJSONAggregateOpaquePipelineProtocol(c.proc, wire),
		"MORPC protocol version 82")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion82)
	require.NoError(t, validateJSONAggregateOpaquePipelineProtocol(c.proc, wire))

	supported, err = remoteWorkersSupportProtocol(
		c.proc, worker, defines.MORPCVersion82)
	require.NoError(t, err)
	require.True(t, supported)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = worker
	require.NoError(t, c.constrainJSONAggregateOpaqueWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
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
