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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	queryclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
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

	// The original base and main's cumulative prefix lack this executor.
	// Reject the reused v101 and immediate predecessor v106 in particular.
	for _, version := range []int64{
		defines.MORPCVersion93,
		defines.MORPCVersion94,
		defines.MORPCVersion99,
		defines.MORPCVersion100,
		defines.MORPCVersion101,
		defines.MORPCVersion102,
		defines.MORPCVersion103,
		defines.MORPCVersion104,
		defines.MORPCVersion105,
		defines.MORPCVersion106,
	} {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = worker
		require.NoError(t, c.constrainJSONAggregateOpaqueWorkers(qry))
		require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
		require.Len(t, c.cnList, 1)
		require.Equal(t, c.addr, c.cnList[0].Addr)
		wire := jsonAggregateOpaqueTestPipeline()
		wire.Node = &pipeline.NodeInfo{Id: worker[0].Id, Addr: worker[0].Addr}
		require.ErrorContains(t, validateJSONAggregateOpaqueDestination(c.proc, wire),
			fmt.Sprintf("MORPC protocol version %d", jsonAggregateOpaqueCapabilityVersion))
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
	require.ErrorContains(t, err, fmt.Sprintf("MORPC protocol version %d", jsonAggregateOpaqueCapabilityVersion))

	wire := jsonAggregateOpaqueTestPipeline()
	// The receiver-side gates must reject the same lowered plan throughout
	// the current-main prefix.
	for _, version := range []int64{
		defines.MORPCVersion93,
		defines.MORPCVersion94,
		defines.MORPCVersion99,
		defines.MORPCVersion100,
		defines.MORPCVersion101,
		defines.MORPCVersion102,
		defines.MORPCVersion103,
		defines.MORPCVersion104,
		defines.MORPCVersion105,
		defines.MORPCVersion106,
	} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		require.ErrorContains(t,
			validateJSONAggregateOpaquePipelineProtocol(c.proc, wire),
			fmt.Sprintf("MORPC protocol version %d", jsonAggregateOpaqueCapabilityVersion))
		require.ErrorContains(t,
			validateJSONAggregateOpaqueAggregateProtocol(c.proc,
				[]aggexec.AggFuncExecExpression{jsonAggregateOpaqueTestAgg()}),
			fmt.Sprintf("MORPC protocol version %d", jsonAggregateOpaqueCapabilityVersion))
	}

	// A candidate peer is admitted by every boundary, proving the new gate matches
	// the aggregate implementation carried by this branch.
	client.version = jsonAggregateOpaqueCapabilityVersion
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, jsonAggregateOpaqueCapabilityVersion)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = worker
	require.NoError(t, c.constrainJSONAggregateOpaqueWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	require.NoError(t, validateJSONAggregateOpaquePipelineProtocol(c.proc, wire))
	require.NoError(t, validateJSONAggregateOpaqueAggregateProtocol(c.proc,
		[]aggexec.AggFuncExecExpression{jsonAggregateOpaqueTestAgg()}))
}

func TestJSONAggregateOpaqueQueryServiceCapabilityProbe(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 4
	coordinatorService := c.proc.GetService()
	rt := moruntime.ServiceRuntime(coordinatorService)
	oldVersion, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldCluster, hadCluster := rt.GetGlobalVariables(moruntime.ClusterService)

	workerID := "json-aggregate-opaque-capability-peer"
	workerPipelineAddress := "json-aggregate-opaque-capability-peer:pipeline"
	socketDir, err := os.MkdirTemp("", "mo-opaque-probe-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(socketDir)) })
	workerQueryAddress := "unix://" + filepath.Join(socketDir, "peer.sock")
	workerRT := moruntime.ServiceRuntime(workerID)
	if workerRT == nil {
		moruntime.SetupServiceBasedRuntime(workerID, moruntime.DefaultRuntime())
		workerRT = moruntime.ServiceRuntime(workerID)
	}
	oldWorkerVersion, hadWorkerVersion := workerRT.GetGlobalVariables(moruntime.MOProtocolVersion)
	workerRT.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion106)

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
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, jsonAggregateOpaqueCapabilityVersion)

	t.Cleanup(func() {
		cluster.Close()
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		if hadWorkerVersion {
			workerRT.SetGlobalVariables(moruntime.MOProtocolVersion, oldWorkerVersion)
		} else {
			current, _ := workerRT.GetGlobalVariables(moruntime.MOProtocolVersion)
			workerRT.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, current)
		}
		if hadCluster {
			rt.SetGlobalVariables(moruntime.ClusterService, oldCluster)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.ClusterService, cluster)
		}
	})

	qs, err := queryservice.NewQueryService(workerID, workerQueryAddress, morpc.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, qs.Close()) })
	require.NoError(t, qs.Start())
	queryClient, err := queryclient.NewQueryClient(coordinatorService, morpc.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, queryClient.Close()) })
	c.proc.Base.QueryClient = queryClient

	worker := engine.Nodes{{
		Id:   workerID,
		Addr: workerPipelineAddress,
		Mcpu: 4,
	}}
	qry := jsonAggregateOpaqueTestQuery()

	// This same-binary QueryService probe checks real capability RPC routing.
	// It does not execute an old decoder or an aggregate pipeline. The v106
	// response must keep opaque aggregation local.
	supported, err := remoteWorkersSupportProtocol(
		c.proc, worker, jsonAggregateOpaqueCapabilityVersion)
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
	require.ErrorContains(t, err, fmt.Sprintf("MORPC protocol version %d", jsonAggregateOpaqueCapabilityVersion))

	// Rolling the same live peer to the candidate capability makes the real destination probe and
	// placement admission succeed. The receiver-local gate is checked at both
	// advertised versions as well.
	workerRT.SetGlobalVariables(moruntime.MOProtocolVersion, jsonAggregateOpaqueCapabilityVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion106)
	wire := jsonAggregateOpaqueTestPipeline()
	require.ErrorContains(t,
		validateJSONAggregateOpaquePipelineProtocol(c.proc, wire),
		fmt.Sprintf("MORPC protocol version %d", jsonAggregateOpaqueCapabilityVersion))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, jsonAggregateOpaqueCapabilityVersion)
	require.NoError(t, validateJSONAggregateOpaquePipelineProtocol(c.proc, wire))

	supported, err = remoteWorkersSupportProtocol(
		c.proc, worker, jsonAggregateOpaqueCapabilityVersion)
	require.NoError(t, err)
	require.True(t, supported)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = worker
	require.NoError(t, c.constrainJSONAggregateOpaqueWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
}

func TestJSONAggregateOpaqueUnavailableDestination(t *testing.T) {
	for _, name := range []string{"unknown-version", "missing-version", "probe-failure", "missing-node", "stale-address", "replaced-id", "canceled"} {
		t.Run(name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.version = jsonAggregateOpaqueCapabilityVersion
			worker := engine.Node{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}
			switch name {
			case "unknown-version":
				client.version = 0
			case "missing-version":
				client.customResponse = true
				client.response = &query.Response{}
			case "probe-failure":
				client.customResponse = true
				client.response = &query.Response{}
				client.sendErr = errors.New("probe failed")
			case "missing-node":
				worker = engine.Node{}
			case "stale-address":
				worker.Addr = "stale:6001"
			case "replaced-id":
				worker.Id = "replacement"
			case "canceled":
				ctx, cancel := context.WithCancel(c.proc.Ctx)
				cancel()
				c.proc.Ctx = ctx
			}
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{worker}
			err := c.constrainJSONAggregateOpaqueWorkers(jsonAggregateOpaqueTestQuery())
			if name == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			} else {
				require.NoError(t, err)
				require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
				require.Len(t, c.cnList, 1)
				require.Equal(t, c.addr, c.cnList[0].Addr)
			}
			wire := jsonAggregateOpaqueTestPipeline()
			if name != "missing-node" {
				wire.Node = &pipeline.NodeInfo{Id: worker.Id, Addr: worker.Addr}
			}
			err = validateJSONAggregateOpaqueDestination(c.proc, wire)
			if name == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, client.calls)
			} else if name == "missing-node" {
				require.ErrorContains(t, err, "target CN protocol capability")
			} else {
				require.ErrorContains(t, err, fmt.Sprintf("MORPC protocol version %d", jsonAggregateOpaqueCapabilityVersion))
			}
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func TestJSONAggregateOpaqueFallbackRespectsPlacement(t *testing.T) {
	for _, probeFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("probe-fails=%t", probeFails), func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.version = defines.MORPCVersion106
			if probeFails {
				client.customResponse = true
				client.sendErr = errors.New("probe failed")
			}
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			c.SetQuerySchedulingIntent(schedule.SchedulingIntent{
				CurrentCNPolicy: schedule.CurrentCNExcluded,
			})

			err := c.constrainJSONAggregateOpaqueWorkers(jsonAggregateOpaqueTestQuery())
			require.ErrorContains(t, err, schedule.ReasonExcludedCurrentCN)
			require.False(t, c.queryPlacement.Satisfied)
			require.Empty(t, c.cnList)
			require.Equal(t, 1, client.calls)
		})
	}
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
