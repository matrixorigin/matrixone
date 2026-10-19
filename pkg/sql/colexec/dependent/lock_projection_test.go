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

package dependent_test

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDependentImplicitLockProjection(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion119)
	optimizer := plan2.NewMockOptimizer(true, proc)
	stmts, err := mysql.Parse(proc.Ctx, "SELECT n.N_NATIONKEY, (SELECT (SELECT n.N_NATIONKEY)) FROM NATION n", 1)
	require.NoError(t, err)
	defer stmts[0].Free()
	bound, err := plan2.BuildPlan(optimizer.CurrentContext(), stmts[0], false)
	require.NoError(t, err)
	q := bound.GetQuery()
	step := q.MainOutputStep()
	lock := &plan.Node{NodeId: int32(len(q.Nodes)), NodeType: plan.Node_LOCK_OP, Children: []int32{q.Steps[step]}}
	q.Nodes = append(q.Nodes, lock)
	q.Steps[step] = lock.NodeId
	require.NoError(t, q.ValidateCorrelation())
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<26, budget)
	require.NoError(t, err)
	defer func() { account.Seal(); _, err := registry.Finalize(account); require.NoError(t, err) }()
	opened, closed := 0, 0
	template, err := dependent.Compile(proc.Ctx, q, func(node *plan.Node) (dependent.SourceFactory, error) {
		return func(child *process.Process, allocation *vector.AllocationAccountSelection, _ *mpool.AllocationAccount) (dependent.Source, error) {
			opened++
			width := len(node.ProjectList)
			if node.TableDef != nil {
				width = len(node.TableDef.Cols)
			}
			b := batch.NewWithSize(width)
			for i := range b.Vecs {
				b.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int32.ToType(), allocation)
				require.NoError(t, err)
				require.NoError(t, vector.AppendFixed(b.Vecs[i], int32(11), false, child.Mp()))
			}
			b.SetRowCount(1)
			return &testSource{batch: b, close: func() { closed++; b.Clean(child.Mp()) }}, nil
		}, nil
	}, func(node *plan.Node, children []*plan.Node) (dependent.KernelFactory, [][]*plan.Expr, error) {
		require.Equal(t, plan.Node_LOCK_OP, node.NodeType)
		require.Len(t, node.ProjectList, 2)
		return func(_ *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) { return ports[0], nil }, make([][]*plan.Expr, len(children)), nil
	})
	require.NoError(t, err)
	require.Empty(t, lock.ProjectList)
	for range 2 {
		op := &dependent.Operator{Template: template}
		require.NoError(t, op.SetAllocationAccount(account))
		require.NoError(t, op.Prepare(proc))
		result, err := vm.Exec(op, proc)
		require.NoError(t, err)
		require.NotNil(t, result.Batch)
		require.Equal(t, 1, result.Batch.RowCount())
		require.Len(t, result.Batch.Vecs, 2)
		for _, v := range result.Batch.Vecs {
			require.Equal(t, int32(11), vector.GetFixedAtNoTypeCheck[int32](v, 0))
		}
		result, err = vm.Exec(op, proc)
		require.NoError(t, err)
		require.Nil(t, result.Batch)
		op.Reset(proc, false, nil)
		op.Free(proc, false, nil)
		require.NoError(t, op.ClearAllocationAccount(account))
		op.Release()
		require.Equal(t, opened, closed)
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, budget.Used())
		require.Empty(t, lock.ProjectList)
	}
	require.Positive(t, opened)
	require.Equal(t, opened, closed)
}
