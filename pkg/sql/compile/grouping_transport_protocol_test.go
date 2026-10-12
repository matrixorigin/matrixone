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
	"time"

	"github.com/google/uuid"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestGroupingTransportDestinations(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	groupingPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Nodes: []*plan.Node{{GroupingFlag: []bool{true, false}}},
	}}}
	// No Group instruction: this scope forwards inherited grouping vectors.
	p := &pipeline.Pipeline{Qry: groupingPlan, Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}
	client.version = defines.MORPCVersion87
	require.NoError(t, validateGroupingTransportDestinations(c.proc, p))
	require.Equal(t, client.calls, client.releases)
	client.version = defines.MORPCVersion86
	require.ErrorContains(t, validateGroupingTransportDestinations(c.proc, p), "version 87")
	// Placement passed, then the worker rolled back before transmission.
	client.version = defines.MORPCVersion87
	require.NoError(t, requireGroupingTransportWorkers(c.proc, engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}))
	client.version = defines.MORPCVersion86
	require.Error(t, validateGroupingTransportDestinations(c.proc, p))

	// The execution destination is current, but its dispatch consumer is old.
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	cluster, _ := rt.GetGlobalVariables(moruntime.ClusterService)
	cluster.(*schedulerTestCluster).cns = append(cluster.(*schedulerTestCluster).cns,
		metadata.CNService{ServiceID: c.proc.GetService(), QueryAddress: "local:9000", PipelineServiceAddress: "local:6001"})
	p.Node = &pipeline.NodeInfo{Id: c.proc.GetService(), Addr: "local:6001"}
	p.InstructionList = []*pipeline.Instruction{{Dispatch: &pipeline.Dispatch{RemoteConnector: []*pipeline.WrapNode{{NodeAddr: "remote:6001"}}}}}
	require.Error(t, validateGroupingTransportDestinations(c.proc, p))
	client.version = defines.MORPCVersion87
	require.NoError(t, validateGroupingTransportDestinations(c.proc, p))
	p.InstructionList[0].Dispatch.RemoteConnector[0].NodeAddr = "unknown:6001"
	require.Error(t, validateGroupingTransportDestinations(c.proc, p))
	p.InstructionList = nil
	p.UuidsToRegIdx = []*pipeline.UuidToRegIdx{{FromAddr: "remote:6001"}}
	client.version = defines.MORPCVersion86
	require.Error(t, validateGroupingTransportDestinations(c.proc, p))

	// Ordinary GROUP BY does not acquire the new transport requirement.
	groupingPlan.GetQuery().Nodes[0].GroupingFlag = []bool{true}
	before := client.calls
	require.NoError(t, validateGroupingTransportDestinations(c.proc, p))
	require.Equal(t, before, client.calls)
}

func TestGroupingTransportScopeInheritance(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCVersion87
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{GroupingFlag: []bool{false}}}}}}
	child := &Scope{}
	root := &Scope{PreScopes: []*Scope{child}}
	attachGroupingTransportPlan([]*Scope{root}, p)
	require.Same(t, p, root.Plan)
	require.Same(t, p, child.Plan)
	// The nested pipeline intentionally omits Qry, as the production encoder
	// does. A decoded forwarding child must carry the requirement on redispatch.
	ctx := &scopeContext{regs: make(map[*process.WaitRegister]int32)}
	ctx.root = ctx
	decoded, err := generateScope(c.proc, &pipeline.Pipeline{Qry: p, Children: []*pipeline.Pipeline{{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}}}, ctx, true)
	require.NoError(t, err)
	t.Cleanup(func() { ReleaseScopes([]*Scope{decoded}) })
	require.Same(t, p, decoded.PreScopes[0].Plan)
	_, err = encodeRemoteScope(decoded.PreScopes[0], c.proc)
	require.NoError(t, err)
	client.version = defines.MORPCVersion86
	_, err = encodeRemoteScope(decoded.PreScopes[0], c.proc)
	require.ErrorContains(t, err, "version 87")
}

func TestGroupingTransportStrippedDispatchAndLocalPlacement(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{GroupingFlag: []bool{false}}}}}}
	c.cnList = engine.Nodes{{Id: c.proc.GetService(), Addr: c.addr}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion86)
	require.NoError(t, c.validateGroupingTransportPlacement(p.GetQuery()), "local-only placement needs no new wire protocol")
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	require.Error(t, c.validateGroupingTransportPlacement(p.GetQuery()))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion87)
	output := dispatch.NewArgument()
	t.Cleanup(output.Release)
	output.RemoteRegs = []colexec.ReceiveInfo{{NodeAddr: "remote:6001"}}
	s := &Scope{Plan: p, Proc: c.proc, RootOp: output, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}}
	client.version = defines.MORPCVersion86
	_, _, _, _, err := prepareRemoteRunSendingData("", s, c.proc, nil, uuid.Nil)
	require.ErrorContains(t, err, "version 87")
	require.Same(t, output, s.RootOp)
	client.version = defines.MORPCVersion87
	c.proc.Base.TxnOperator = fakeTxnOperator{}
	c.proc.Base.SessionInfo.TimeZone = time.UTC
	_, withoutOutput, _, _, err := prepareRemoteRunSendingData("", s, c.proc, nil, uuid.Nil)
	require.NoError(t, err)
	require.False(t, withoutOutput)
}
