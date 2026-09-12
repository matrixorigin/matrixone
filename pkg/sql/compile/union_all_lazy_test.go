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
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unionall"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newLazyUnionAllTestCompile(t *testing.T) *Compile {
	c := NewMockCompile(t)
	c.anal = &AnalyzeModule{curNodeIdx: 1, isFirst: true}
	c.isPrepare = true
	c.counterSet = &perfcounter.CounterSet{}
	return c
}

func newLazyUnionAllLeaf(c *Compile, op vm.Operator) *Scope {
	return &Scope{
		Magic:    Normal,
		NodeInfo: engine.Node{Mcpu: 1},
		Proc:     c.proc.NewNoContextChildProc(0),
		RootOp:   op,
	}
}

type preparingLazyUnionAllOperator struct {
	*colexec.MockOperator
	prepareCalls *atomic.Int32
}

func (op *preparingLazyUnionAllOperator) Prepare(proc *process.Process) error {
	op.prepareCalls.Add(1)
	return op.MockOperator.Prepare(proc)
}

func newPreparingLazyUnionAllLeaf(
	c *Compile,
	bat *batch.Batch,
	prepareCalls *atomic.Int32,
) *Scope {
	mock := colexec.NewMockOperator()
	if bat != nil {
		mock.WithBatchs([]*batch.Batch{bat})
	}
	return newLazyUnionAllLeaf(c, &preparingLazyUnionAllOperator{
		MockOperator: mock,
		prepareCalls: prepareCalls,
	})
}

func newLazyUnionAllInt8Batch(c *Compile, values ...int8) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt8Vector(values, nil, c.proc.Mp())
	bat.SetRowCount(len(values))
	return bat
}

func freeLazyUnionAllTestScope(c *Compile, scope *Scope) {
	scope.FreeOperator(c)
	scope.release()
	c.proc.Free()
}

func TestCompileUnionAllKeepsConcurrentTopologyWithoutDemand(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	left := newLazyUnionAllLeaf(c, colexec.NewMockOperator())
	right := newLazyUnionAllLeaf(c, colexec.NewMockOperator())

	result := c.compileUnionAll(&planpb.Node{}, []*Scope{left}, []*Scope{right}, false)
	require.Len(t, result, 1)
	root := result[0]
	require.False(t, root.LazyPreScopes)
	require.Len(t, root.PreScopes, 2)
	require.Same(t, left, root.PreScopes[0])
	require.Same(t, right, root.PreScopes[1])

	unionOp, ok := root.RootOp.(*unionall.UnionAll)
	require.True(t, ok)
	require.Zero(t, unionOp.SequentialBranches)
	mergeOp, ok := unionOp.GetChildren(0).(*merge.Merge)
	require.True(t, ok)
	require.False(t, mergeOp.Partial)

	freeLazyUnionAllTestScope(c, root)
}

func TestCompileUnionAllBuildsLazyBranchScopesWithDemand(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	left := newLazyUnionAllLeaf(c, colexec.NewMockOperator())
	right := newLazyUnionAllLeaf(c, colexec.NewMockOperator())

	root := c.compileUnionAll(
		&planpb.Node{}, []*Scope{left}, []*Scope{right}, true,
	)[0]
	require.True(t, root.LazyPreScopes)
	require.Len(t, root.PreScopes, 2)
	require.IsType(t, &merge.Merge{}, root.PreScopes[0].RootOp.GetOperatorBase().GetChildren(0))
	require.IsType(t, &merge.Merge{}, root.PreScopes[1].RootOp.GetOperatorBase().GetChildren(0))

	unionOp := root.RootOp.(*unionall.UnionAll)
	require.Equal(t, 2, unionOp.SequentialBranches)
	mergeOp := unionOp.GetChildren(0).(*merge.Merge)
	require.True(t, mergeOp.Partial)
	require.Equal(t, int32(0), mergeOp.StartIDX)
	require.Equal(t, int32(1), mergeOp.EndIDX)

	freeLazyUnionAllTestScope(c, root)
}

func TestLazyUnionAllLimitReleasesUnstartedMaterializedReader(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	source := materialized.NewSource(2)
	require.NoError(t, source.Begin(c.proc.Mp()))
	input := newLazyUnionAllInt8Batch(c, 7)
	for range 4 {
		require.NoError(t, source.Append(input))
	}
	source.Finish(nil)
	require.Positive(t, source.CurrentBytes())

	newReader := func(readerID int) *Scope {
		reader := merge.NewArgument().WithSinkScan(true)
		reader.MaterializedSource = source
		reader.MaterializedReaderID = readerID
		return newLazyUnionAllLeaf(c, reader)
	}
	root := c.compileUnionAll(
		&planpb.Node{}, []*Scope{newReader(0)}, []*Scope{newReader(1)}, true,
	)[0]
	root = c.compileLimit(&planpb.Node{
		Limit: plan2.MakePlan2Uint64ConstExprWithType(1),
	}, []*Scope{root})[0]
	c.scopes = []*Scope{root}
	c.InitPipelineContextToExecuteQuery()

	// Four retained batches exceed the ordinary two-reader broadcast spool's
	// two slots. LIMIT starts only reader 0; scope teardown must also release
	// reader 1 even though its branch pipeline was never submitted.
	require.NoError(t, root.MergeRun(c))
	require.Zero(t, source.CurrentBytes())
	require.NoError(t, source.Begin(c.proc.Mp()),
		"all producer and reader owners must be retired before prepared reuse")
	source.Finish(nil)
	source.ReleaseReader(0)
	source.ReleaseReader(1)

	input.Clean(c.proc.Mp())
	freeLazyUnionAllTestScope(c, root)
}

func TestStreamingUnionAllDemandStopsAtBlockingOperators(t *testing.T) {
	limit := plan2.MakePlan2Uint64ConstExprWithType(1)
	tests := []struct {
		name        string
		node        *planpb.Node
		outerDemand bool
		want        bool
	}{
		{name: "project limit", node: &planpb.Node{NodeType: planpb.Node_PROJECT, Limit: limit}, want: true},
		{name: "filter from parent", node: &planpb.Node{NodeType: planpb.Node_FILTER}, outerDemand: true, want: true},
		{name: "offset carrier from parent", node: &planpb.Node{NodeType: planpb.Node_SORT, Offset: limit}, outerDemand: true, want: true},
		{name: "union limit", node: &planpb.Node{NodeType: planpb.Node_UNION_ALL, Limit: limit}, want: true},
		{name: "ordered union", node: &planpb.Node{NodeType: planpb.Node_UNION_ALL, Limit: limit, OrderBy: []*planpb.OrderBySpec{{}}}},
		{name: "assertion", node: &planpb.Node{NodeType: planpb.Node_ASSERT}, outerDemand: true},
		{name: "barrier filter", node: &planpb.Node{NodeType: planpb.Node_FILTER, FilterIsBarrier: true}, outerDemand: true},
		{name: "window", node: &planpb.Node{NodeType: planpb.Node_WINDOW}, outerDemand: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, streamingUnionAllDemand(test.node, test.outerDemand))
		})
	}
}

func nestedUnionAllPlanNodes(withLimit bool) []*planpb.Node {
	nodes := []*planpb.Node{
		{NodeType: planpb.Node_VALUE_SCAN},
		{NodeType: planpb.Node_VALUE_SCAN},
		{NodeType: planpb.Node_UNION_ALL, Children: []int32{0, 1}},
		{NodeType: planpb.Node_VALUE_SCAN},
		{NodeType: planpb.Node_UNION_ALL, Children: []int32{2, 3}},
	}
	if withLimit {
		nodes = append(nodes, &planpb.Node{
			NodeType: planpb.Node_PROJECT,
			Children: []int32{4},
			Limit:    plan2.MakePlan2Uint64ConstExprWithType(1),
		})
	}
	return nodes
}

func TestCompilePlanScopePropagatesLimitToNestedUnionAll(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	nodes := nestedUnionAllPlanNodes(true)

	scopes, err := c.compilePlanScope(0, int32(len(nodes)-1), nodes)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	outer := scopes[0]
	require.True(t, outer.LazyPreScopes)
	require.Len(t, outer.PreScopes, 3)

	var outerUnion *unionall.UnionAll
	require.NoError(t, vm.HandleAllOp(outer.RootOp, func(_ vm.Operator, op vm.Operator) error {
		if candidate, ok := op.(*unionall.UnionAll); ok {
			outerUnion = candidate
		}
		return nil
	}))
	require.NotNil(t, outerUnion)
	require.Equal(t, 3, outerUnion.SequentialBranches)

	for i, branch := range outer.PreScopes {
		branchConnector, ok := branch.RootOp.(*connector.Connector)
		require.True(t, ok)
		child := branchConnector.GetOperatorBase().GetChildren(0)
		if i < 2 {
			marker, ok := child.(*unionall.UnionAll)
			require.True(t, ok)
			require.Equal(t, 2, marker.GetOperatorBase().Idx)
			require.Zero(t, marker.SequentialBranches)
			child = marker.GetOperatorBase().GetChildren(0)
		}
		require.IsType(t, &merge.Merge{}, child)
	}

	freeLazyUnionAllTestScope(c, outer)
}

func TestLazyUnionAllDoesNotAbsorbNestedLimit(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	inner := c.compileUnionAll(
		&planpb.Node{},
		[]*Scope{newLazyUnionAllLeaf(c, colexec.NewMockOperator())},
		[]*Scope{newLazyUnionAllLeaf(c, colexec.NewMockOperator())},
		true,
	)[0]
	inner = c.compileLimit(&planpb.Node{
		Limit: plan2.MakePlan2Uint64ConstExprWithType(1),
	}, []*Scope{inner})[0]
	outer := c.compileUnionAll(
		&planpb.Node{},
		[]*Scope{inner},
		[]*Scope{newLazyUnionAllLeaf(c, colexec.NewMockOperator())},
		true,
	)[0]

	require.True(t, outer.LazyPreScopes)
	require.Len(t, outer.PreScopes, 2)
	require.Len(t, outer.PreScopes[0].PreScopes, 1)
	require.Same(t, inner, outer.PreScopes[0].PreScopes[0])
	require.True(t, inner.LazyPreScopes)

	freeLazyUnionAllTestScope(c, outer)
}

func TestCompilePlanScopeKeepsNestedUnionAllConcurrentWithoutLimit(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	nodes := nestedUnionAllPlanNodes(false)

	scopes, err := c.compilePlanScope(0, int32(len(nodes)-1), nodes)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	outer := scopes[0]
	require.False(t, outer.LazyPreScopes)
	require.Len(t, outer.PreScopes, 2)
	inner := outer.PreScopes[0]
	require.False(t, inner.LazyPreScopes)
	require.Len(t, inner.PreScopes, 2)

	freeLazyUnionAllTestScope(c, outer)
}

func TestScalarUnionAllRunsBranchesInStatementOrder(t *testing.T) {
	compilerCtx := plan2.NewMockCompilerContext(true)
	statements, err := mysql.Parse(
		compilerCtx.GetContext(),
		"SELECT 3 UNION ALL SELECT 1 UNION ALL SELECT 2",
		1,
	)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	logicPlan, err := plan2.BuildPlan(compilerCtx, statements[0], false)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Len(t, query.Steps, 1)

	c := newLazyUnionAllTestCompile(t)
	c.pn = logicPlan
	nodes, rootIdx := query.Nodes, query.Steps[0]
	require.Equal(t, planpb.Node_PROJECT, nodes[rootIdx].NodeType)
	require.Len(t, nodes[rootIdx].Children, 1)
	unionIdx := nodes[rootIdx].Children[0]
	require.Truef(t, orderedScalarUnionAll(unionIdx, nodes), "unexpected scalar UNION ALL plan: %v", query)
	var valueScan *planpb.Node
	for _, node := range nodes {
		if node.NodeType == planpb.Node_VALUE_SCAN {
			valueScan = node
			break
		}
	}
	require.NotNil(t, valueScan)
	valueScan.RowsetData = &planpb.RowsetData{}
	require.False(t, orderedScalarUnionAll(unionIdx, nodes), "VALUES inputs keep the general UNION ALL topology")
	valueScan.RowsetData = nil

	scopes, err := c.compilePlanScope(0, rootIdx, nodes)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	root := scopes[0]
	require.True(t, root.LazyPreScopes)
	require.Len(t, root.PreScopes, 3)
	var unionOp *unionall.UnionAll
	require.NoError(t, vm.HandleAllOp(root.RootOp, func(_ vm.Operator, op vm.Operator) error {
		if candidate, ok := op.(*unionall.UnionAll); ok && candidate.SequentialBranches > 0 {
			unionOp = candidate
		}
		return nil
	}))
	require.NotNil(t, unionOp)
	require.Equal(t, 3, unionOp.SequentialBranches)

	var got []int64
	root.setRootOperator(output.NewArgument().WithFunc(
		func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
			if bat == nil {
				return nil
			}
			got = append(got, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])...)
			return nil
		},
	))
	c.scopes = []*Scope{root}
	c.InitPipelineContextToExecuteQuery()

	require.NoError(t, root.MergeRun(c))
	require.Equal(t, []int64{3, 1, 2}, got)

	freeLazyUnionAllTestScope(c, root)
}

func TestScalarUnionAllInsideJoinKeepsConcurrentTopology(t *testing.T) {
	nodes := []*planpb.Node{
		{NodeType: planpb.Node_VALUE_SCAN},
		{NodeType: planpb.Node_PROJECT, Children: []int32{0}},
		{NodeType: planpb.Node_VALUE_SCAN},
		{NodeType: planpb.Node_PROJECT, Children: []int32{2}},
		{NodeType: planpb.Node_UNION_ALL, Children: []int32{1, 3}},
		{NodeType: planpb.Node_TABLE_SCAN},
		{NodeType: planpb.Node_JOIN, Children: []int32{4, 5}},
		{NodeType: planpb.Node_PROJECT, Children: []int32{6}},
	}
	query := &planpb.Query{Steps: []int32{7}, Nodes: nodes}

	require.True(t, orderedScalarUnionAll(4, nodes))
	require.False(t, orderedScalarUnionAllResult(0, 4, query))

	query.Steps[0] = 7
	nodes[7].Children[0] = 4
	require.True(t, orderedScalarUnionAllResult(0, 4, query))
}

func TestOrderedScalarUnionAllRejectsMalformedPlans(t *testing.T) {
	tests := []struct {
		name    string
		nodeIdx int32
		nodes   []*planpb.Node
	}{
		{name: "negative node index", nodeIdx: -1},
		{name: "node index past end", nodeIdx: 1, nodes: []*planpb.Node{{}}},
		{name: "nil node", nodes: []*planpb.Node{nil}},
		{name: "unsupported node type", nodes: []*planpb.Node{{NodeType: planpb.Node_TABLE_SCAN}}},
		{name: "union without two children", nodes: []*planpb.Node{{NodeType: planpb.Node_UNION_ALL}}},
		{name: "project without one child", nodes: []*planpb.Node{{NodeType: planpb.Node_PROJECT}}},
		{
			name:  "project with invalid child",
			nodes: []*planpb.Node{{NodeType: planpb.Node_PROJECT, Children: []int32{1}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.False(t, orderedScalarUnionAll(test.nodeIdx, test.nodes))
		})
	}
}

func TestOrderedScalarUnionAllResultRejectsMalformedPlans(t *testing.T) {
	tests := []struct {
		name    string
		step    int32
		nodeIdx int32
		query   *planpb.Query
	}{
		{name: "nil query"},
		{name: "negative step", step: -1, query: &planpb.Query{}},
		{name: "step past end", step: 1, query: &planpb.Query{Steps: []int32{0}}},
		{name: "negative node index", nodeIdx: -1, query: &planpb.Query{Steps: []int32{0}}},
		{
			name:    "node index past end",
			nodeIdx: 1,
			query:   &planpb.Query{Steps: []int32{0}, Nodes: []*planpb.Node{{}}},
		},
		{
			name:  "invalid result root",
			query: &planpb.Query{Steps: []int32{1}, Nodes: []*planpb.Node{{}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.False(t, orderedScalarUnionAllResult(test.step, test.nodeIdx, test.query))
		})
	}
}

func compileNestedLazyUnionAllTestScope(
	c *Compile,
	first *Scope,
	second *Scope,
	third *Scope,
	limit *planpb.Expr,
) *Scope {
	inner := c.compileUnionAll(
		&planpb.Node{}, []*Scope{first}, []*Scope{second}, true,
	)[0]
	outer := c.compileUnionAll(
		&planpb.Node{}, []*Scope{inner}, []*Scope{third}, true,
	)[0]
	if limit != nil {
		outer = c.compileLimit(&planpb.Node{Limit: limit}, []*Scope{outer})[0]
	}
	return outer
}

func TestLazyNestedUnionAllLimitStartsOnlyFirstLeaf(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	var firstPrepares, secondPrepares, thirdPrepares atomic.Int32
	root := compileNestedLazyUnionAllTestScope(
		c,
		newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, 1, 1), &firstPrepares),
		newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, 2), &secondPrepares),
		newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, 3), &thirdPrepares),
		plan2.MakePlan2Uint64ConstExprWithType(1),
	)
	c.scopes = []*Scope{root}
	c.InitPipelineContextToExecuteQuery()

	require.NoError(t, root.MergeRun(c))
	require.Equal(t, int32(1), firstPrepares.Load())
	require.Zero(t, secondPrepares.Load())
	require.Zero(t, thirdPrepares.Load())

	freeLazyUnionAllTestScope(c, root)
}

func TestLazyNestedUnionAllEmptyFirstLeafAdvancesOnce(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	var firstPrepares, secondPrepares, thirdPrepares atomic.Int32
	root := compileNestedLazyUnionAllTestScope(
		c,
		newPreparingLazyUnionAllLeaf(c, nil, &firstPrepares),
		newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, 2, 2), &secondPrepares),
		newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, 3), &thirdPrepares),
		plan2.MakePlan2Uint64ConstExprWithType(1),
	)
	c.scopes = []*Scope{root}
	c.InitPipelineContextToExecuteQuery()

	require.NoError(t, root.MergeRun(c))
	require.Equal(t, int32(1), firstPrepares.Load())
	require.Equal(t, int32(1), secondPrepares.Load())
	require.Zero(t, thirdPrepares.Load())

	freeLazyUnionAllTestScope(c, root)
}

func TestLazyNestedUnionAllExhaustionStartsEveryLeafOnce(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	var firstPrepares, secondPrepares, thirdPrepares atomic.Int32
	root := compileNestedLazyUnionAllTestScope(
		c,
		newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, 1), &firstPrepares),
		newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, 2), &secondPrepares),
		newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, 3), &thirdPrepares),
		nil,
	)
	c.scopes = []*Scope{root}
	c.InitPipelineContextToExecuteQuery()

	require.NoError(t, root.MergeRun(c))
	require.Equal(t, int32(1), firstPrepares.Load())
	require.Equal(t, int32(1), secondPrepares.Load())
	require.Equal(t, int32(1), thirdPrepares.Load())

	freeLazyUnionAllTestScope(c, root)
}

func TestLazyUnionAllLimitLeavesSecondBranchUnstarted(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	leftBatch := batch.NewWithSize(1)
	leftBatch.Vecs[0] = testutil.MakeInt8Vector([]int8{1, 1}, nil, c.proc.Mp())
	leftBatch.SetRowCount(2)
	rightBatch := batch.NewWithSize(1)
	rightBatch.Vecs[0] = testutil.MakeInt8Vector([]int8{2, 2}, nil, c.proc.Mp())
	rightBatch.SetRowCount(2)

	var leftCalls atomic.Int32
	var rightCalls atomic.Int32
	leftOp := colexec.NewMockOperator().WithBatchs([]*batch.Batch{leftBatch}).
		WithBatchCallback(func(int) { leftCalls.Add(1) })
	rightOp := colexec.NewMockOperator().WithBatchs([]*batch.Batch{rightBatch}).
		WithBatchCallback(func(int) { rightCalls.Add(1) })
	root := c.compileUnionAll(
		&planpb.Node{},
		[]*Scope{newLazyUnionAllLeaf(c, leftOp)},
		[]*Scope{newLazyUnionAllLeaf(c, rightOp)},
		true,
	)[0]
	root = c.compileLimit(&planpb.Node{
		Limit: plan2.MakePlan2Uint64ConstExprWithType(1),
	}, []*Scope{root})[0]
	c.scopes = []*Scope{root}
	c.InitPipelineContextToExecuteQuery()

	require.NoError(t, root.MergeRun(c))
	require.Equal(t, int32(1), leftCalls.Load())
	require.Zero(t, rightCalls.Load())

	freeLazyUnionAllTestScope(c, root)
}

func TestLazyUnionAllStartsSecondBranchAfterFirstExhausts(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	rightBatch := batch.NewWithSize(1)
	rightBatch.Vecs[0] = testutil.MakeInt8Vector([]int8{2}, nil, c.proc.Mp())
	rightBatch.SetRowCount(1)

	var rightCalls atomic.Int32
	leftOp := colexec.NewMockOperator()
	rightOp := colexec.NewMockOperator().WithBatchs([]*batch.Batch{rightBatch}).
		WithBatchCallback(func(int) { rightCalls.Add(1) })
	root := c.compileUnionAll(
		&planpb.Node{},
		[]*Scope{newLazyUnionAllLeaf(c, leftOp)},
		[]*Scope{newLazyUnionAllLeaf(c, rightOp)},
		true,
	)[0]
	c.scopes = []*Scope{root}
	c.InitPipelineContextToExecuteQuery()

	require.NoError(t, root.MergeRun(c))
	require.Equal(t, int32(1), rightCalls.Load())

	freeLazyUnionAllTestScope(c, root)
}

func TestLazyUnionAllCancellationLeavesSecondBranchUnstarted(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	leftBatch := batch.NewWithSize(1)
	leftBatch.Vecs[0] = testutil.MakeInt8Vector([]int8{1}, nil, c.proc.Mp())
	leftBatch.SetRowCount(1)
	rightBatch := batch.NewWithSize(1)
	rightBatch.Vecs[0] = testutil.MakeInt8Vector([]int8{2}, nil, c.proc.Mp())
	rightBatch.SetRowCount(1)

	var rightCalls atomic.Int32
	leftOp := colexec.NewMockOperator().WithBatchs([]*batch.Batch{leftBatch}).
		WithBatchCallback(func(int) {
			if c.proc.Cancel != nil {
				c.proc.Cancel(context.Canceled)
			}
		})
	rightOp := colexec.NewMockOperator().WithBatchs([]*batch.Batch{rightBatch}).
		WithBatchCallback(func(int) { rightCalls.Add(1) })
	root := c.compileUnionAll(
		&planpb.Node{},
		[]*Scope{newLazyUnionAllLeaf(c, leftOp)},
		[]*Scope{newLazyUnionAllLeaf(c, rightOp)},
		true,
	)[0]
	c.scopes = []*Scope{root}
	c.InitPipelineContextToExecuteQuery()

	_ = root.MergeRun(c)
	require.Zero(t, rightCalls.Load())

	freeLazyUnionAllTestScope(c, root)
}
