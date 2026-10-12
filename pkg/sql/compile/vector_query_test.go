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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorquery"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type vectorQueryTestProducer struct {
	*colexec.MockOperator
	prepared, reset, freed atomic.Int32
	prepareErr, callErr    error
}

func (p *vectorQueryTestProducer) Prepare(*process.Process) error {
	p.prepared.Add(1)
	return p.prepareErr
}
func (p *vectorQueryTestProducer) Call(proc *process.Process) (vm.CallResult, error) {
	if p.callErr != nil {
		return vm.CancelResult, p.callErr
	}
	return p.MockOperator.Call(proc)
}
func (p *vectorQueryTestProducer) Reset(proc *process.Process, failed bool, err error) {
	p.reset.Add(1)
	p.MockOperator.Reset(proc, failed, err)
}
func (p *vectorQueryTestProducer) Free(proc *process.Process, failed bool, err error) {
	p.freed.Add(1)
	p.MockOperator.Free(proc, failed, err)
}

func TestVectorQueryScopeLifecycle(t *testing.T) {
	sentinel := errors.New("vector provider failed")
	for _, tc := range []struct {
		name                           string
		rows                           int
		null                           bool
		limit                          uint64
		prepareErr, callErr, initPanic bool
	}{
		{name: "ann", rows: 1, limit: 2},
		{name: "null", rows: 1, null: true, limit: 2},
		{name: "empty", limit: 2},
		{name: "zero", rows: 1, limit: 0},
		{name: "prepare_error", limit: 2, prepareErr: true},
		{name: "call_error", limit: 2, callErr: true},
		{name: "initialization_panic", limit: 2, initPanic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewMockCompile(t)
			c.hasMergeOp = true
			c.execType = plan2.ExecTypeAP_ONECN
			root := newScope(Merge)
			root.VectorQueryScope = true
			root.Proc = c.proc.NewContextChildProc(3)
			selector := vectorquery.NewArgument()
			selector.Source = materialized.NewSource(2)
			require.NoError(t, selector.Source.Begin(c.proc.Mp()))
			source := selector.Source
			selector.LimitExpr = &plan.Expr{Typ: plan.Type{Id: int32(types.T_uint64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: tc.limit}}}}
			selector.AppendChild(merge.NewArgument().WithPartial(0, 0))
			root.RootOp = selector
			producers := make([]*vectorQueryTestProducer, 3)
			for i := range producers {
				producer := &vectorQueryTestProducer{MockOperator: colexec.NewMockOperator()}
				producers[i] = producer
				branch := newScope(Normal)
				branch.Proc = root.Proc.NewContextChildProc(0)
				out := connector.NewArgument().WithReg(root.Proc.Reg.MergeReceivers[i])
				out.AppendChild(producer)
				branch.RootOp = out
				root.PreScopes = append(root.PreScopes, branch)
			}
			if tc.rows > 0 {
				b := batch.NewWithSize(1)
				b.Vecs[0] = vector.NewVec(types.T_array_float32.ToType())
				require.NoError(t, vector.AppendBytes(b.Vecs[0], types.ArrayToBytes([]float32{1, 2, 3}), tc.null, c.proc.Mp()))
				b.SetRowCount(1)
				producers[0].WithBatchs([]*batch.Batch{b})
			}
			if tc.initPanic {
				root.PreScopes[0].DataSource = &Source{}
			}
			if tc.prepareErr {
				producers[0].prepareErr = sentinel
			}
			if tc.callErr {
				producers[0].callErr = sentinel
			}
			selected := 1
			if tc.null || tc.rows == 0 {
				selected = 2
			}
			// 未选择的 reader 是故意无效的；初始化它会失败，证明全树初始化未穿透 selector。
			unused := 3 - selected
			root.PreScopes[unused].DataSource = &Source{}
			require.NoError(t, root.InitAllDataSource(c))
			t.Cleanup(func() { source.Close(); root.release(); require.Zero(t, c.proc.Mp().CurrNB()) })
			done := make(chan error, 1)
			go func() { done <- root.MergeRun(c) }()
			select {
			case err := <-done:
				if tc.initPanic {
					require.Error(t, err)
				} else if tc.prepareErr || tc.callErr {
					require.ErrorIs(t, err, sentinel)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				root.Proc.Cancel(errors.New("test timeout"))
				t.Fatal("selector scope did not close all started branches")
			}
			for i, p := range producers {
				expected := int32(0)
				if tc.limit > 0 && !tc.initPanic && (i == 0 || i == selected && !tc.prepareErr && !tc.callErr) {
					expected = 1
				}
				require.Equal(t, expected, p.prepared.Load(), "branch %d prepare", i)
				require.Equal(t, int32(1), p.reset.Load(), "branch %d reset", i)
				require.Equal(t, int32(1), p.freed.Load(), "branch %d free", i)
			}
		})
	}
}

func TestVectorQueryCompileOwnership(t *testing.T) {
	c := NewMockCompile(t)
	q := &plan.Query{Nodes: []*plan.Node{
		{NodeType: plan.Node_VALUE_SCAN, NodeId: 0, Stats: &plan.Stats{}},
		{NodeType: plan.Node_VECTOR_QUERY_SOURCE, NodeId: 1, VectorQuerySourceId: 42, Stats: &plan.Stats{}},
		{NodeType: plan.Node_VECTOR_QUERY_SOURCE, NodeId: 2, VectorQuerySourceId: 42, Stats: &plan.Stats{}},
		{NodeType: plan.Node_VECTOR_QUERY_TOP, NodeId: 3, VectorQuerySourceId: 42, Children: []int32{0, 1, 2}, Stats: &plan.Stats{},
			Limit: &plan.Expr{Typ: plan.Type{Id: int32(types.T_uint64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}}}},
	}}
	c.anal = &AnalyzeModule{qry: q}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
	c.execType = plan2.ExecTypeAP_ONECN
	_, err := c.compileVectorQuerySource(q.Nodes[1])
	require.ErrorContains(t, err, "source consumer")
	_, err = c.compileVectorQueryTop(0, &plan.Node{}, q.Nodes, 3)
	require.ErrorContains(t, err, "invalid vector query plan")
	scopes, err := c.compileVectorQueryTop(0, q.Nodes[3], q.Nodes, 3)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, s := range scopes {
			s.FreeOperator(c)
		}
		ReleaseScopes(scopes)
		for _, source := range c.materializedSources {
			source.Close()
		}
	})
	require.Len(t, scopes, 1)
	require.False(t, scopes[0].VectorQueryScope, "外层消费者必须保持普通调度域")
	require.True(t, scopes[0].PreScopes[0].VectorQueryScope)
	require.Len(t, c.materializedSources, 1)
	key := vectorQuerySourceKey(q.Nodes[3])
	require.Equal(t, []int32{1, 2}, c.materializedSinkScanNodes[key])
	require.Equal(t, 0, c.materializedReaderIDs[[2]int32{key, 1}])
	require.Equal(t, 1, c.materializedReaderIDs[[2]int32{key, 2}])
	_, err = c.compileVectorQuerySource(q.Nodes[1])
	require.ErrorContains(t, err, "source consumer")
	_, err = c.compileVectorQueryTop(0, q.Nodes[3], q.Nodes, 3)
	require.ErrorContains(t, err, "duplicate")

	// 复用同一物理计划连续执行零 LIMIT：两个 reader 都从不 Prepare，
	// 旧代的 released 标记不能让下一代 Source.Begin 失败。
	c.isPrepare = true
	c.e = newStubEngine()
	c.counterSet = &perfcounter.CounterSet{}
	c.scopes = scopes
	selectorScope := scopes[0].PreScopes[0]
	selector := selectorScope.RootOp.GetOperatorBase().Children[0].(*vectorquery.VectorQuery)
	selector.LimitExpr = &plan.Expr{Typ: plan.Type{Id: int32(types.T_uint64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 0}}}}
	for generation := 0; generation < 3; generation++ {
		c.InitPipelineContextToExecuteQuery()
		require.NoError(t, c.materializedSources[key].Begin(c.proc.Mp()))
		require.NoError(t, scopes[0].MergeRun(c))
		var resetTree func(*Scope)
		resetTree = func(s *Scope) {
			require.NoError(t, s.resetForReuse(c))
			for _, child := range s.PreScopes {
				resetTree(child)
			}
		}
		resetTree(scopes[0])
	}
}

func TestVectorQueryReaderClone(t *testing.T) {
	op := merge.NewArgument()
	defer op.Release()
	op.MaterializedSource = materialized.NewSource(2)
	op.MaterializedReaderID = 1
	op.MaterializedSingleBatch = true
	clone := dupOperator(op, 0, 1).(*merge.Merge)
	defer clone.Release()
	require.Same(t, op.MaterializedSource, clone.MaterializedSource)
	require.Equal(t, op.MaterializedReaderID, clone.MaterializedReaderID)
	require.True(t, clone.MaterializedSingleBatch)
}

func TestVectorQueryExecutionBoundary(t *testing.T) {
	require.Equal(t, plan2.ExecTypeAP_MULTICN, vectorQueryExecType(plan2.ExecTypeAP_MULTICN, nil))
	q := &plan.Query{Nodes: []*plan.Node{nil, {NodeType: plan.Node_TABLE_SCAN}}}
	require.Equal(t, plan2.ExecTypeAP_MULTICN, vectorQueryExecType(plan2.ExecTypeAP_MULTICN, q))
	q.Nodes = append(q.Nodes, &plan.Node{NodeType: plan.Node_VECTOR_QUERY_TOP})
	require.Equal(t, plan2.ExecTypeAP_ONECN, vectorQueryExecType(plan2.ExecTypeAP_MULTICN, q))
	for _, id := range []int32{0, 1, 42} {
		require.Less(t, vectorQuerySourceKey(&plan.Node{VectorQuerySourceId: id}), int32(0))
	}
	s := &Scope{VectorQueryScope: true}
	require.ErrorContains(t, s.holdAnyCannotRemoteOperator(), "coordinator")
	require.Error(t, (&Scope{PreScopes: []*Scope{s}}).holdAnyCannotRemoteOperator())
}
