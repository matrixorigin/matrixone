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
	"context"
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

type testSource struct {
	batch *batch.Batch
	read  bool
	close func()
}

func (s *testSource) Read(context.Context) (*batch.Batch, error) {
	if s.read {
		return nil, nil
	}
	s.read = true
	return s.batch, nil
}
func (s *testSource) Close() error { s.close(); return nil }

func TestPublicDependentStreamingExecution(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion119)
	optimizer := plan2.NewMockOptimizer(true, proc)
	stmts, err := mysql.Parse(proc.Ctx, `SELECT n1.N_NATIONKEY, (SELECT (SELECT n1.N_NATIONKEY + n2.N_REGIONKEY FROM NATION n3 WHERE n3.N_NATIONKEY = n2.N_NATIONKEY) FROM NATION n2 WHERE n2.N_NATIONKEY = n1.N_NATIONKEY) FROM NATION n1`, 1)
	require.NoError(t, err)
	defer stmts[0].Free()
	p, err := plan2.BuildPlan(optimizer.CurrentContext(), stmts[0], false)
	require.NoError(t, err)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<26, budget)
	require.NoError(t, err)
	defer func() {
		account.Seal()
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	}()
	opened, closed := make(map[int32]int), make(map[int32]int)
	template, err := dependent.Compile(proc.Ctx, p.GetQuery(), func(node *plan.Node) (dependent.SourceFactory, error) {
		return func(child *process.Process, sourceAllocation *vector.AllocationAccountSelection, account *mpool.AllocationAccount) (dependent.Source, error) {
			opened[node.NodeId]++
			b := batch.NewWithSize(len(node.TableDef.Cols))
			keys := []int32{1, 2, 3}
			if node.NodeId == 0 {
				keys = []int32{1, 2, 2, 4}
			}
			for i, col := range node.TableDef.Cols {
				b.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int32.ToType(), sourceAllocation)
				if err != nil {
					b.Clean(child.Mp())
					return nil, err
				}
				for _, key := range keys {
					v := key
					if col.Name == "n_regionkey" {
						v = key * 10
					}
					if err = vector.AppendFixed(b.Vecs[i], v, false, child.Mp()); err != nil {
						b.Clean(child.Mp())
						return nil, err
					}
				}
			}
			b.SetRowCount(len(keys))
			return &testSource{batch: b, close: func() { closed[node.NodeId]++; b.Clean(child.Mp()) }}, nil
		}, nil
	})
	require.NoError(t, err)
	require.Empty(t, opened)
	for range 2 {
		func() {
			op := &dependent.Operator{Template: template}
			defer op.Release()
			defer func() {
				op.Free(proc, false, nil)
				require.NoError(t, op.ClearAllocationAccount(account))
			}()
			require.NoError(t, op.SetAllocationAccount(account))
			require.NoError(t, op.Prepare(proc))
			var keys, results []int32
			var nulls []bool
			for {
				result, err := vm.Exec(op, proc)
				require.NoError(t, err)
				b := result.Batch
				if b == nil {
					break
				}
				for row := 0; row < b.RowCount(); row++ {
					keys = append(keys, vector.GetFixedAtNoTypeCheck[int32](b.Vecs[0], row))
					null := b.Vecs[1].IsNull(uint64(row))
					nulls = append(nulls, null)
					if !null {
						results = append(results, vector.GetFixedAtNoTypeCheck[int32](b.Vecs[1], row))
					}
				}
			}
			require.Equal(t, []int32{1, 2, 2, 4}, keys)
			require.Equal(t, []int32{11, 22, 22}, results)
			require.Equal(t, []bool{false, false, false, true}, nulls)
			op.Reset(proc, false, nil)
			require.Equal(t, opened, closed)
			require.Zero(t, account.Snapshot().Used)
			require.Zero(t, budget.Used())
		}()
	}
	require.Equal(t, map[int32]int{0: 2, 1: 8, 3: 6}, opened)
}
