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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestScalarSetDependentQueryAdmissionBeforeSourceWork(t *testing.T) {
	// The dependent entry point cannot reconstruct a marked fallback without
	// its attempt lease. No process/engine is installed: reject before opening
	// either execution owner, including when the marker is in a producer step.
	for _, kind := range []plan.Node_NodeType{plan.Node_WINDOW, plan.Node_JOIN} {
		q := &plan.Query{
			Correlation: &plan.CorrelationSpec{},
			Nodes:       []*plan.Node{{NodeType: kind, ScalarSetReaggregation: true}},
		}
		_, err := (&Compile{}).compileQuery(q)
		require.ErrorContains(t, err, "bounded scalar set reaggregation cannot execute inside dependent programs")
	}
}

func TestScalarSetMarkerProtocolAndPlacement(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	n := &plan.Node{NodeType: plan.Node_WINDOW, ScalarSetReaggregation: true}
	require.True(t, plan2.DeepCopyNode(n).ScalarSetReaggregation)
	data, err := n.Marshal()
	require.NoError(t, err)
	decoded := &plan.Node{}
	require.NoError(t, decoded.Unmarshal(data))
	require.True(t, decoded.ScalarSetReaggregation)
	features, err := plan.RequiredRemoteExpressionFeatures(decoded)
	require.NoError(t, err)
	require.True(t, features.ScalarSetReaggregation)
	require.False(t, features.SetOperationFloatKeys)
	require.Equal(t, defines.MORPCVersion120, remoteExpressionProtocolVersion(features))
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ScalarSetReaggregation: true}}}
	data, err = p.Marshal()
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, version := range []int64{defines.MORPCVersion119, defines.MORPCVersion120} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		if version == defines.MORPCVersion119 {
			require.ErrorContains(t, validateRemoteExpressionFeatures(c.proc, features), "version 120")
		} else {
			require.NoError(t, validateRemoteExpressionFeatures(c.proc, features))
		}
		require.Error(t, validateRemoteExpressionDestination(c.proc, nil, features))
		require.Error(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
		_, err = decodeScope(data, c.proc, true, nil)
		require.Error(t, err, "detached marked join must be rejected before decode at all versions")
	}
	local := engine.Node{Id: "coordinator", Addr: "local", Mcpu: 1}
	qry := &plan.Query{Nodes: []*plan.Node{n}}
	root := &Scope{NodeInfo: local, PreScopes: []*Scope{{NodeInfo: local}}}
	require.NoError(t, validateScalarSetScopePlacement(qry, local, []*Scope{root}))
	root.PreScopes[0].NodeInfo.Mcpu = 2
	require.Error(t, validateScalarSetScopePlacement(qry, local, []*Scope{root}))
	root.PreScopes[0].NodeInfo = engine.Node{Id: "worker", Addr: "remote", Mcpu: 1}
	require.Error(t, validateScalarSetScopePlacement(qry, local, []*Scope{root}))
	n.ScalarSetReaggregation = false
	require.NoError(t, validateScalarSetScopePlacement(qry, local, []*Scope{root}))
}

func TestScalarSetPermitRequiresTerminalProof(t *testing.T) {
	for _, mode := range []string{"success", "exporter-panic", "terminal-unavailable", "owner-clear-panic", "init-rollback-panic"} {
		t.Run(mode, func(t *testing.T) {
			budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
			defer budget.Close()
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			defer generation.Close()
			registry, err := generation.AllocationAccountRegistry()
			require.NoError(t, err)
			c := newTestAllocationLifecycleCompile(t, registry, func(mpool.AllocationAccountTerminalSnapshot) {
				if mode == "exporter-panic" {
					panic("export failure")
				}
			})
			defer c.proc.Free()
			require.NoError(t, c.proc.SetExecutionResourceBudgetForTesting(generation))
			c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{ScalarSetReaggregation: true}}}}}
			first := &allocationLifecycleOwnerOperator{MockOperator: colexec.NewMockOperator(), panicClear: mode == "owner-clear-panic" || mode == "init-rollback-panic"}
			c.allocationAccountOwners = []executionAllocationAccountOwner{first}
			if mode == "init-rollback-panic" {
				c.allocationAccountOwners = append(c.allocationAccountOwners, &allocationLifecycleOwnerOperator{MockOperator: colexec.NewMockOperator(), failSet: true})
			}
			a, err := c.beginAllocationAccountAttempt()
			if mode == "init-rollback-panic" {
				require.Error(t, err)
				require.Nil(t, a)
			} else {
				require.NoError(t, err)
				if mode == "terminal-unavailable" {
					a.registry = nil
				}
				_, err = a.finish()
				if mode == "success" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			}
			extra, err := generation.AcquireScalarSetExecution()
			require.NoError(t, err)
			defer extra.Close()
			next, err := generation.AcquireScalarSetExecution()
			if mode == "success" || mode == "exporter-panic" {
				require.NoError(t, err)
				next.Close()
			} else {
				require.Error(t, err)
			}
			if a != nil && mode == "terminal-unavailable" {
				_, _, err = registry.CompleteTerminal(a.account)
				require.NoError(t, err)
				a.scalarSetLease.Close()
			}
			require.Zero(t, generation.Snapshot().Used)
		})
	}
}
