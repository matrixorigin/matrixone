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
	"errors"
	"math"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorquery"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScalarVectorQueryCompileLocalSources(t *testing.T) {
	c := NewMockCompile(t)
	defer c.proc.Free()
	c.addr = "local:6001"
	c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 1}}
	q := &plan.Query{Nodes: []*plan.Node{
		{NodeId: 0, NodeType: plan.Node_VALUE_SCAN, ProjectList: []*plan.Expr{plan2.MakePlan2Int64ConstExprWithType(1)}, Stats: plan2.DefaultStats()},
		{NodeId: 1, NodeType: plan.Node_VECTOR_QUERY_SOURCE, VectorQuerySourceId: 7},
		{NodeId: 2, NodeType: plan.Node_VECTOR_QUERY_SOURCE, VectorQuerySourceId: 7},
		{NodeId: 3, NodeType: plan.Node_VECTOR_QUERY_TOP, VectorQuerySourceId: 7, Children: []int32{0, 1, 2}, Limit: plan2.MakePlan2Uint64ConstExprWithType(1)},
	}, Steps: []int32{3}}
	c.anal = &AnalyzeModule{qry: q, isFirst: true}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
	ss, err := c.compileVectorQueryTop(0, q.Nodes[3], q.Nodes, 3)
	require.NoError(t, err)
	defer func() {
		for _, s := range ss {
			s.FreeOperator(c)
		}
		ReleaseScopes(ss)
	}()
	require.Len(t, ss, 1)
	require.False(t, ss[0].LazyPreScopes, "consumers must not append dependencies to the selector's lazy scheduling domain")
	require.Len(t, ss[0].PreScopes, 1)
	selector := ss[0].PreScopes[0]
	require.True(t, selector.LazyPreScopes)
	require.Len(t, selector.PreScopes, 3)
	op := selector.RootOp.GetOperatorBase().GetChildren(0).(*vectorquery.VectorQuery)
	require.Same(t, c.materializedSources[-8], op.Source)
	require.Equal(t, []int32{1, 2}, c.materializedSinkScanNodes[-8])
	require.Equal(t, 0, c.materializedReaderIDs[[2]int32{-8, 1}])
	require.Equal(t, 1, c.materializedReaderIDs[[2]int32{-8, 2}])
	require.True(t, op.GetChildren(0).(*merge.Merge).Partial)
	clear, deferred, err := installSequentialBranchStarter(op, func(int) error { return nil }, func(int) error { return nil })
	require.NoError(t, err)
	require.True(t, deferred)
	clear()
	_, err = c.compileVectorQueryTop(0, q.Nodes[3], q.Nodes, 3)
	require.ErrorContains(t, err, "duplicate")
}

func TestScalarVectorQueryJoinScope(t *testing.T) {
	for _, tc := range []struct {
		name         string
		limit        uint64
		nullProvider bool
		outputError  bool
		cancel       bool
		want         []int64
	}{
		{name: "ann_self_join", limit: 1, want: []int64{1}},
		{name: "null_fallback", limit: 1, nullProvider: true},
		{name: "zero_demand"},
		{name: "consumer_error", limit: 1, outputError: true},
		{name: "consumer_cancel", limit: 1, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newLazyUnionAllTestCompile(t)
			c.MessageBoard = message.NewMessageBoard()
			c.proc.SetMessageBoard(c.MessageBoard)
			c.addr = "local:6001"
			c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 1}}
			q := &plan.Query{}
			for i := int32(0); i < 8; i += 4 {
				value := plan2.MakePlan2Int64ConstExprWithType(1)
				if tc.nullProvider {
					value.GetLit().Isnull = true
				}
				q.Nodes = append(q.Nodes,
					&plan.Node{NodeId: i, NodeType: plan.Node_VALUE_SCAN, ProjectList: []*plan.Expr{value}, Stats: plan2.DefaultStats()},
					&plan.Node{NodeId: i + 1, NodeType: plan.Node_VECTOR_QUERY_SOURCE, VectorQuerySourceId: i},
					&plan.Node{NodeId: i + 2, NodeType: plan.Node_VECTOR_QUERY_SOURCE, VectorQuerySourceId: i},
					&plan.Node{NodeId: i + 3, NodeType: plan.Node_VECTOR_QUERY_TOP, VectorQuerySourceId: i, Children: []int32{i, i + 1, i + 2}, Limit: plan2.MakePlan2Uint64ConstExprWithType(tc.limit)},
				)
			}
			c.anal.qry = q
			c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
			var roots []*Scope
			var registry *mpool.AllocationAccountRegistry
			var account *mpool.AllocationAccount
			var owners []executionAllocationAccountOwner
			t.Cleanup(func() {
				for _, root := range roots {
					root.FreeOperator(c)
				}
				c.MessageBoard.CloseAndDrain()
				for _, source := range c.materializedSources {
					source.Close()
				}
				for _, owner := range owners {
					assert.NoError(t, owner.ClearAllocationAccount(account))
				}
				ReleaseScopes(roots)
				if account != nil {
					snapshot, _, err := registry.CompleteTerminal(account)
					assert.NoError(t, err)
					assert.Zero(t, snapshot.Used)
				}
				c.proc.Free()
				require.Zero(t, c.proc.Mp().CurrNB())
			})
			probe, err := c.compileVectorQueryTop(0, q.Nodes[3], q.Nodes, 3)
			require.NoError(t, err)
			roots = append(roots, probe...)
			build, err := c.compileVectorQueryTop(0, q.Nodes[7], q.Nodes, 7)
			require.NoError(t, err)
			roots = append(roots, build...)
			require.False(t, probe[0].LazyPreScopes)
			require.False(t, build[0].LazyPreScopes)
			join := newShuffleJoinTestNode(1)
			join.Stats.HashmapStats.Shuffle = false
			join.OnList = []*plan.Expr{makeMarkJoinTestCondition(t, "=", 0, false)}
			join.ProjectList = []*plan.Expr{makeMarkJoinTestColumn(0, 0, false)}
			left := &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(0, 0, false)}}
			right := &plan.Node{ProjectList: []*plan.Expr{makeMarkJoinTestColumn(1, 0, false)}}
			roots = c.compileJoin(join, left, right, probe, build)
			require.Len(t, roots, 1)
			root := roots[0]
			require.False(t, root.LazyPreScopes)
			require.Len(t, root.PreScopes, 2, "the build must be a concurrently started sibling of the selector")
			require.True(t, root.PreScopes[0].LazyPreScopes)
			require.Len(t, root.PreScopes[0].PreScopes, 3)
			sentinel := errors.New("join output failed")
			var got []int64
			root.setRootOperator(output.NewArgument().WithFunc(func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
				if bat != nil && bat.RowCount() > 0 {
					if tc.cancel {
						c.proc.Cancel(sentinel)
					}
					if tc.outputError || tc.cancel {
						return sentinel
					}
					got = append(got, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])...)
				}
				return nil
			}))
			registry, err = mpool.NewAllocationAccountRegistry(1, 1<<14)
			require.NoError(t, err)
			account, err = registry.Open(math.MaxInt64)
			require.NoError(t, err)
			owners, err = collectAllocationAccountOwners(roots)
			require.NoError(t, err)
			_, err = configureAllocationAccountOwners(owners, account)
			require.NoError(t, err)
			for _, source := range c.materializedSources {
				require.NoError(t, source.Begin(c.proc.Mp(), materialized.SpillConfig{AllocationAccount: account}))
			}
			c.scopes = roots
			c.InitPipelineContextToExecuteQuery()
			err = root.MergeRun(c)
			if tc.outputError || tc.cancel {
				require.ErrorIs(t, err, sentinel)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestScalarVectorQueryOuterZeroDemand(t *testing.T) {
	c := NewMockCompile(t)
	defer c.proc.Free()
	c.addr = "local:6001"
	c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 1}}
	q := &plan.Query{Nodes: []*plan.Node{
		{NodeId: 0, NodeType: plan.Node_VALUE_SCAN, ProjectList: []*plan.Expr{plan2.MakePlan2Int64ConstExprWithType(1)}, Stats: plan2.DefaultStats()},
		{NodeId: 1, NodeType: plan.Node_VECTOR_QUERY_SOURCE, VectorQuerySourceId: 7},
		{NodeId: 2, NodeType: plan.Node_VECTOR_QUERY_SOURCE, VectorQuerySourceId: 7},
		{NodeId: 3, NodeType: plan.Node_VECTOR_QUERY_TOP, VectorQuerySourceId: 7, Children: []int32{0, 1, 2}, Limit: plan2.MakePlan2Uint64ConstExprWithType(2)},
		{NodeId: 4, NodeType: plan.Node_PROJECT, Children: []int32{3}, Limit: plan2.MakePlan2Uint64ConstExprWithType(0)},
	}, Steps: []int32{4}}
	c.anal = &AnalyzeModule{qry: q, isFirst: true}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
	ss, err := c.compilePlanScope(0, 4, q.Nodes)
	require.NoError(t, err)
	defer func() {
		for _, s := range ss {
			s.FreeOperator(c)
		}
		ReleaseScopes(ss)
	}()
	require.Len(t, ss, 1)
	require.Empty(t, ss[0].PreScopes, "zero outer demand must not construct any lazy branch")
	require.Empty(t, c.materializedSources, "the pruned selector must not allocate a source")
	require.Empty(t, c.materializedReaderIDs)
}

func TestScalarVectorQueryLazyScope(t *testing.T) {
	for _, tc := range []struct {
		name     string
		limit    uint64
		provider []int8
		want     [3]int32
	}{
		{"zero", 0, []int8{1}, [3]int32{}},
		{"ann", 2, []int8{1}, [3]int32{1, 1, 0}},
		{"empty", 2, nil, [3]int32{1, 0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newLazyUnionAllTestCompile(t)
			var prepares [3]atomic.Int32
			branches := []*Scope{
				newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, tc.provider...), &prepares[0]),
				newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, 11), &prepares[1]),
				newPreparingLazyUnionAllLeaf(c, newLazyUnionAllInt8Batch(c, 22), &prepares[2]),
			}
			root := c.newMergeScope(branches)
			root.LazyPreScopes = true
			root.RootOp.(*merge.Merge).WithPartial(0, 0)
			op := vectorquery.NewArgument()
			op.LimitExpr = plan2.MakePlan2Uint64ConstExprWithType(tc.limit)
			op.Source = materialized.NewSource(2)
			registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
			require.NoError(t, err)
			account, err := registry.Open(math.MaxInt64)
			require.NoError(t, err)
			require.NoError(t, op.Source.Begin(c.proc.Mp(), materialized.SpillConfig{AllocationAccount: account}))
			root.setRootOperator(op)
			t.Cleanup(func() {
				root.FreeOperator(c)
				op.Source.Close()
				root.release()
				snapshot, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
				require.Zero(t, snapshot.Used)
				c.proc.Free()
				require.Zero(t, c.proc.Mp().CurrNB())
			})
			c.scopes = []*Scope{root}
			c.InitPipelineContextToExecuteQuery()
			require.NoError(t, root.MergeRun(c))
			for i := range prepares {
				require.Equal(t, tc.want[i], prepares[i].Load())
			}
		})
	}
}

func TestScalarVectorQueryCompileGuards(t *testing.T) {
	c := NewMockCompile(t)
	defer c.proc.Free()
	_, err := c.compileVectorQueryTop(0, &plan.Node{}, nil, 0)
	require.ErrorContains(t, err, "invalid vector query plan")
	_, err = c.compileVectorQuerySource(&plan.Node{VectorQuerySourceId: 5})
	require.ErrorContains(t, err, "consumer")
	c.materializedSources = map[int32]*materialized.Source{-6: materialized.NewSource(2)}
	c.materializedSinkScanNodes = map[int32][]int32{-6: {1, 2}}
	_, err = c.compileVectorQuerySource(&plan.Node{VectorQuerySourceId: 5})
	require.ErrorContains(t, err, "consumer")
	for _, exec := range []plan2.ExecType{plan2.ExecTypeTP, plan2.ExecTypeAP_ONECN, plan2.ExecTypeAP_MULTICN} {
		require.Equal(t, exec, vectorQueryExecType(exec, nil))
		require.Equal(t, exec, vectorQueryExecType(exec, &plan.Query{}))
		require.Equal(t, plan2.ExecTypeAP_ONECN, vectorQueryExecType(exec, &plan.Query{Nodes: []*plan.Node{{NodeType: plan.Node_VECTOR_QUERY_TOP}}}))
	}
}
