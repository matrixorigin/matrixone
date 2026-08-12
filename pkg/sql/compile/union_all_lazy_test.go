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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unionall"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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

func freeLazyUnionAllTestScope(c *Compile, scope *Scope) {
	scope.FreeOperator(c)
	scope.release()
	c.proc.Free()
}

func TestCompileUnionAllBuildsLazyBranchScopes(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	left := newLazyUnionAllLeaf(c, colexec.NewMockOperator())
	right := newLazyUnionAllLeaf(c, colexec.NewMockOperator())

	result := c.compileUnionAll(&planpb.Node{}, []*Scope{left}, []*Scope{right})
	require.Len(t, result, 1)
	root := result[0]
	require.True(t, root.UnionAllBranches)
	require.False(t, root.LazyPreScopes)
	require.Len(t, root.PreScopes, 2)
	require.IsType(t, &merge.Merge{}, root.PreScopes[0].RootOp.GetOperatorBase().GetChildren(0))
	require.IsType(t, &merge.Merge{}, root.PreScopes[1].RootOp.GetOperatorBase().GetChildren(0))

	unionOp, ok := root.RootOp.(*unionall.UnionAll)
	require.True(t, ok)
	require.Zero(t, unionOp.SequentialBranches)
	mergeOp, ok := unionOp.GetChildren(0).(*merge.Merge)
	require.True(t, ok)
	require.False(t, mergeOp.Partial)

	limited := c.compileLimit(&planpb.Node{
		Limit: plan2.MakePlan2Uint64ConstExprWithType(1),
	}, result)
	require.Len(t, limited, 1)
	require.Same(t, root, limited[0])
	require.True(t, root.LazyPreScopes)
	require.Equal(t, 2, unionOp.SequentialBranches)
	require.True(t, mergeOp.Partial)
	require.Equal(t, int32(0), mergeOp.StartIDX)
	require.Equal(t, int32(1), mergeOp.EndIDX)

	freeLazyUnionAllTestScope(c, root)
}

func TestCompileLimitKeepsUnionConcurrentBelowBlockingOperator(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	root := c.compileUnionAll(
		&planpb.Node{},
		[]*Scope{newLazyUnionAllLeaf(c, colexec.NewMockOperator())},
		[]*Scope{newLazyUnionAllLeaf(c, colexec.NewMockOperator())},
	)[0]
	unionOp := root.RootOp.(*unionall.UnionAll)
	mergeOp := unionOp.GetChildren(0).(*merge.Merge)
	root.setRootOperator(order.NewArgument())

	result := c.compileLimit(&planpb.Node{
		Limit: plan2.MakePlan2Uint64ConstExprWithType(1),
	}, []*Scope{root})
	require.Len(t, result, 1)
	require.False(t, root.LazyPreScopes)
	require.Zero(t, unionOp.SequentialBranches)
	require.False(t, mergeOp.Partial)

	freeLazyUnionAllTestScope(c, root)
}

func TestCompileLimitEnablesLazyUnionThroughUnaryStreamingOperators(t *testing.T) {
	streamingOperators := []struct {
		name string
		new  func() vm.Operator
	}{
		{name: "projection", new: func() vm.Operator { return projection.NewArgument() }},
		{name: "filter", new: func() vm.Operator { return filter.NewArgument() }},
		{name: "offset", new: func() vm.Operator { return offset.NewArgument() }},
	}

	for _, test := range streamingOperators {
		t.Run(test.name, func(t *testing.T) {
			c := newLazyUnionAllTestCompile(t)
			root := c.compileUnionAll(
				&planpb.Node{},
				[]*Scope{newLazyUnionAllLeaf(c, colexec.NewMockOperator())},
				[]*Scope{newLazyUnionAllLeaf(c, colexec.NewMockOperator())},
			)[0]
			root.setRootOperator(test.new())

			enableLazyUnionAllForLimit([]*Scope{root})
			require.True(t, root.LazyPreScopes)

			freeLazyUnionAllTestScope(c, root)
		})
	}
}

func TestCompileLimitKeepsUnionConcurrentBelowNonUnaryStreamingOperator(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	root := c.compileUnionAll(
		&planpb.Node{},
		[]*Scope{newLazyUnionAllLeaf(c, colexec.NewMockOperator())},
		[]*Scope{newLazyUnionAllLeaf(c, colexec.NewMockOperator())},
	)[0]
	wrapper := projection.NewArgument()
	root.setRootOperator(wrapper)
	wrapper.AppendChild(colexec.NewMockOperator())

	enableLazyUnionAllForLimit([]*Scope{root})
	require.False(t, root.LazyPreScopes)

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
	)[0]
	enableLazyUnionAllForLimit([]*Scope{root})
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
	)[0]
	enableLazyUnionAllForLimit([]*Scope{root})
	c.scopes = []*Scope{root}
	c.InitPipelineContextToExecuteQuery()

	_ = root.MergeRun(c)
	require.Zero(t, rightCalls.Load())

	freeLazyUnionAllTestScope(c, root)
}
