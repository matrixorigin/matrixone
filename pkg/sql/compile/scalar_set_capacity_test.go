// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Literal sources avoid storage and scheduling dependencies while retaining the
// actual WINDOW/LEFT JOIN/two-AGG scalar fallback and its allocation owners.
func TestScalarSetFallbackResourceLifecycle(t *testing.T) {
	const query = `select o.id,cast((select i.v from (select 1 id,cast(-0e0 as double) v union all select 2,cast(-0e0 as double)) i where i.id>o.id union select i.v from (select 1 id,cast(-0e0 as double) v union all select 2,cast(-0e0 as double)) i where i.id>o.id) as varchar) from (select 0 id union all select 3) o order by o.id`
	budget := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
	for index, mode := range []string{"normal", "budget-rejection", "cancel-after-materialization", "fresh-after-failures"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(defines.AttachAccountId(context.Background(), catalog.System_Account))
			defer cancel()
			proc := newPlanTestProcess(t)
			t.Cleanup(proc.Free)
			proc.Ctx = ctx
			proc.ReplaceTopCtx(ctx)
			cap := uint64(64 << 20)
			if mode == "budget-rejection" {
				cap = 4096
			}
			generation, err := budget.OpenGenerationWithCap(uint64(index+1), cap)
			require.NoError(t, err)
			t.Cleanup(generation.Close)
			require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
			registry, err := generation.AllocationAccountRegistry()
			require.NoError(t, err)
			compilerCtx := plan2.NewEmptyCompilerContext(proc)
			compilerCtx.SetContext(ctx)
			stmts, err := mysql.Parse(ctx, query, 1)
			require.NoError(t, err)
			t.Cleanup(stmts[0].Free)
			logical, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
			require.NoError(t, err)
			windows, aggs, lefts := 0, 0, 0
			for _, n := range logical.Nodes {
				if n.NodeType == plan.Node_WINDOW {
					windows++
				}
				if n.NodeType == plan.Node_AGG {
					aggs++
				}
				if n.NodeType == plan.Node_JOIN && n.JoinType == plan.Node_LEFT {
					lefts++
				}
			}
			require.Positive(t, windows)
			require.GreaterOrEqual(t, aggs, 2)
			require.Positive(t, lefts)
			proc.Base.TxnClient, proc.Base.TxnOperator = newTestTxnClientAndOpWithIsolation(gomock.NewController(t), txn.TxnIsolation_RC)
			var got []string
			var gotIDs []int64
			cancelOutput := mode == "cancel-after-materialization"
			fill := func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
				if bat == nil {
					return nil
				}
				ids := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])
				for i := 0; i < bat.RowCount(); i++ {
					gotIDs = append(gotIDs, ids[i])
					if bat.Vecs[1].IsNull(uint64(i)) {
						got = append(got, "NULL")
					} else {
						got = append(got, string(bat.Vecs[1].GetBytesAt(i)))
					}
					if ids[i] != 0 && ids[i] != 3 {
						return fmt.Errorf("unexpected outer identity %d", ids[i])
					}
				}
				if cancelOutput {
					cancel()
					return context.Canceled
				}
				return nil
			}
			c := NewCompile("test", "test", query, "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
			released := false
			t.Cleanup(func() {
				if !released {
					c.FreeOperator()
					c.Release()
				}
			})
			c.SetIsPrepare(true)
			require.NoError(t, c.Compile(ctx, &plan.Plan{Plan: &plan.Plan_Query{Query: logical}}, fill))
			_, err = c.Run(0)
			switch mode {
			case "budget-rejection":
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				t.Logf("budget rejection: %v", err)
				require.Empty(t, got)
			case "cancel-after-materialization":
				require.ErrorIs(t, err, context.Canceled)
				require.NotEmpty(t, got)
			default:
				require.NoError(t, err)
				require.Equal(t, []string{"-0", "NULL"}, got)
				require.Equal(t, []int64{0, 3}, gotIDs)
			}
			tp := c.IsTpQuery()
			if !tp {
				require.ErrorIs(t, c.Reset(proc, time.Now(), fill, query), cantCompileForPrepareErr)
			} else if mode != "budget-rejection" {
				// A new top context plus real Compile.Reset exercises cached TP
				// reuse after success and after downstream cancellation.
				require.Zero(t, generation.Snapshot().Used)
				got = nil
				gotIDs = nil
				cancelOutput = false
				proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), catalog.System_Account))
				require.NoError(t, c.Reset(proc, time.Now(), fill, query))
				_, err = c.Run(0)
				require.NoError(t, err)
				require.Equal(t, []string{"-0", "NULL"}, got)
				require.Equal(t, []int64{0, 3}, gotIDs)
			}
			require.Zero(t, generation.Snapshot().Used, "terminal Run must release reservations before fixture destruction")
			require.Zero(t, registry.LiveAllocationMetadata())
			c.FreeOperator()
			c.Release()
			released = true
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
			snapshot := generation.Snapshot()
			require.Zero(t, snapshot.Used)
			require.Zero(t, snapshot.SpillDiskUsed)
			require.Zero(t, snapshot.SpillFDUsed)
			require.Zero(t, snapshot.MemoryGrowthParticipants)
			require.Zero(t, registry.LiveAllocationMetadata())
			if mode != "budget-rejection" {
				require.Positive(t, snapshot.PeakUsed)
			}
			t.Logf("compiled fallback mode=%s TP=%v terminal=%+v", mode, tp, snapshot)
		})
		require.Zero(t, budget.AggregateUsed(), "fresh generation must not inherit live reservations")
	}
}
