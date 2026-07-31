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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestMaterializedSpillBudgetUsesProcessLimits(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.Lim.SpillSize = 128
	budget := newMaterializedSpillBudget(proc)

	disk, err := budget.ReserveDisk(128)
	require.NoError(t, err)
	_, err = budget.ReserveDisk(1)
	require.Error(t, err, "materialized spill must honor the query SpillSize limit")
	require.True(t, disk.Release())

	memory, err := budget.ReserveMemory(1)
	require.NoError(t, err)
	require.True(t, memory.Release())
	fd, err := budget.ReserveFD(1)
	require.NoError(t, err)
	require.True(t, fd.Release())
	proc.SetStmtProfile(nil)
}

func TestCTESinkFanoutRegistersEveryConsumer(t *testing.T) {
	c := NewMockCompile(t)
	c.nodeRegs = make(map[[2]int32]*process.WaitRegister)
	c.stepRegs = make(map[int32][][2]int32)
	query := &plan.Query{Nodes: []*plan.Node{
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}},
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}},
		{NodeType: plan.Node_SINK, ExtraOptions: materialized.CTESinkOption},
	}, Steps: []int32{2}}
	c.anal = &AnalyzeModule{qry: query}

	require.NoError(t, c.compileSinkScan(query, 0))
	require.NoError(t, c.compileSinkScan(query, 1))
	require.Len(t, c.getStepRegs(0), 2)

	left, err := c.compileSinkScanNode(query.Nodes[0], 0)
	require.NoError(t, err)
	right, err := c.compileSinkScanNode(query.Nodes[1], 1)
	require.NoError(t, err)
	leftScan := left[0].RootOp.(*merge.Merge)
	rightScan := right[0].RootOp.(*merge.Merge)
	require.Equal(t, Merge, left[0].Magic)
	require.Equal(t, 1, left[0].NodeInfo.Mcpu)
	require.Equal(t, Merge, right[0].Magic)
	require.Equal(t, 1, right[0].NodeInfo.Mcpu)
	require.NotNil(t, leftScan.MaterializedSource)
	require.Same(t, leftScan.MaterializedSource, rightScan.MaterializedSource)
	require.NotEqual(t, leftScan.MaterializedReaderID, rightScan.MaterializedReaderID)

	producer := generateScopeWithRootOperator(c.proc, []vm.OpType{vm.TableScan})
	scopes, err := c.compileSinkNode(&plan.Node{NodeType: plan.Node_SINK}, []*Scope{producer}, 0)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	require.Equal(t, Merge, scopes[0].Magic)
	require.Equal(t, 1, scopes[0].NodeInfo.Mcpu)
	require.Equal(t, []*Scope{producer}, scopes[0].PreScopes)
	fanout, ok := scopes[0].RootOp.(*dispatch.Dispatch)
	require.True(t, ok)
	require.True(t, fanout.IsSink)
	require.Equal(t, dispatch.SendToAllLocalFunc, fanout.FuncId)
	require.Len(t, fanout.LocalRegs, 2)
	require.Same(t, leftScan.MaterializedSource, fanout.MaterializedSource)
}

func TestMaterializedCTESinkGroupsShuffleBucketsByCN(t *testing.T) {
	c := NewMockCompile(t)
	c.cnList = engine.Nodes{
		{Addr: "cn1:6001", Mcpu: 2},
		{Addr: "cn2:6001", Mcpu: 2},
	}
	c.addr = "cn1:6001"
	c.nodeRegs = make(map[[2]int32]*process.WaitRegister)
	c.stepRegs = make(map[int32][][2]int32)
	query := &plan.Query{Nodes: []*plan.Node{
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}},
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}},
		{NodeType: plan.Node_SINK, ExtraOptions: materialized.CTESinkOption},
	}, Steps: []int32{2}}
	c.anal = &AnalyzeModule{qry: query}

	require.NoError(t, c.compileSinkScan(query, 0))
	require.NoError(t, c.compileSinkScan(query, 1))

	addrs := []string{"cn1:6001", "cn1:6001", "cn2:6001", "cn2:6001"}
	buckets := make([]*Scope, len(addrs))
	for i, addr := range addrs {
		buckets[i] = &Scope{
			Magic:    Remote,
			NodeInfo: engine.Node{Addr: addr, Mcpu: 1},
			Proc:     c.proc.NewContextChildProc(1),
		}
		buckets[i].setRootOperator(merge.NewArgument())
	}

	// A grouped producer has one shuffle source per CN. Each source dispatches
	// to both same-CN buckets, so the buckets must travel in one RemoteRun tree.
	buckets[0].PreScopes = append(buckets[0].PreScopes,
		newDispatchSrcScopeForTest(c.proc, "cn1:6001",
			[]*Scope{buckets[0], buckets[1]}, []*Scope{buckets[2], buckets[3]}))
	buckets[2].PreScopes = append(buckets[2].PreScopes,
		newDispatchSrcScopeForTest(c.proc, "cn2:6001",
			[]*Scope{buckets[2], buckets[3]}, []*Scope{buckets[0], buckets[1]}))

	scopes, err := c.compileSinkNode(&plan.Node{NodeType: plan.Node_SINK}, buckets, 0)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	require.Len(t, scopes[0].PreScopes, 2, "materialized producer must have one send tree per CN")
	for _, cnScope := range scopes[0].PreScopes {
		require.Len(t, cnScope.PreScopes, 2)
		require.True(t, checkPipelineStandaloneExecutableAtRemote(cnScope))
	}
}

func TestMaterializedCTEStepGuards(t *testing.T) {
	c := NewMockCompile(t)
	tests := []struct {
		name  string
		query *plan.Query
		step  int32
		want  bool
	}{
		{
			name: "eligible marker",
			query: &plan.Query{Nodes: []*plan.Node{
				{NodeType: plan.Node_SINK, ExtraOptions: materialized.CTESinkOption},
			}, Steps: []int32{0}},
			want: true,
		},
		{
			name: "ordinary sink",
			query: &plan.Query{Nodes: []*plan.Node{
				{NodeType: plan.Node_SINK},
			}, Steps: []int32{0}},
		},
		{
			name: "recursive sink",
			query: &plan.Query{Nodes: []*plan.Node{
				{NodeType: plan.Node_SINK, RecursiveSink: true, ExtraOptions: materialized.CTESinkOption},
			}, Steps: []int32{0}},
		},
		{
			name:  "invalid step",
			query: &plan.Query{},
			step:  1,
		},
		{name: "nil query"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, c.isMaterializedCTEStep(test.query, test.step))
		})
	}
}

func TestSingleSinkScanKeepsStreamingPath(t *testing.T) {
	c := NewMockCompile(t)
	c.nodeRegs = make(map[[2]int32]*process.WaitRegister)
	c.stepRegs = make(map[int32][][2]int32)
	query := &plan.Query{Nodes: []*plan.Node{
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}},
		{NodeType: plan.Node_SINK, ExtraOptions: materialized.CTESinkOption},
	}, Steps: []int32{1}}
	c.anal = &AnalyzeModule{qry: query}

	require.NoError(t, c.compileSinkScan(query, 0))
	scopes, err := c.compileSinkScanNode(query.Nodes[0], 0)
	require.NoError(t, err)
	scan := scopes[0].RootOp.(*merge.Merge)
	require.Nil(t, scan.MaterializedSource)

	producer := generateScopeWithRootOperator(c.proc, []vm.OpType{vm.TableScan})
	scopes, err = c.compileSinkNode(&plan.Node{NodeType: plan.Node_SINK}, []*Scope{producer}, 0)
	require.NoError(t, err)
	require.Nil(t, scopes[0].RootOp.(*dispatch.Dispatch).MaterializedSource)
}

func TestOrdinaryMultiConsumerSinkKeepsStreamingPath(t *testing.T) {
	c := NewMockCompile(t)
	c.nodeRegs = make(map[[2]int32]*process.WaitRegister)
	c.stepRegs = make(map[int32][][2]int32)
	query := &plan.Query{Nodes: []*plan.Node{
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}},
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}},
		{NodeType: plan.Node_SINK},
	}, Steps: []int32{2}}
	c.anal = &AnalyzeModule{qry: query}

	require.NoError(t, c.compileSinkScan(query, 0))
	require.NoError(t, c.compileSinkScan(query, 1))
	left, err := c.compileSinkScanNode(query.Nodes[0], 0)
	require.NoError(t, err)
	right, err := c.compileSinkScanNode(query.Nodes[1], 1)
	require.NoError(t, err)
	require.Nil(t, left[0].RootOp.(*merge.Merge).MaterializedSource)
	require.Nil(t, right[0].RootOp.(*merge.Merge).MaterializedSource)
}
