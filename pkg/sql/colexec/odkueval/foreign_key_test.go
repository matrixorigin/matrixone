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

package odkueval

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestFKChangesCompareEachConstraintAndCleanUp(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
	require.NoError(t, err)
	account, err := registry.Open(8 << 20)
	require.NoError(t, err)
	defer func() {
		proc.Free()
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, mp.CurrNB())
	}()

	check := func(update bool, columns ...int32) *plan.OdkuForeignKeyCheck {
		c := &plan.OdkuForeignKeyCheck{
			CheckUpdates: update,
			Eligibility:  &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
		}
		for _, column := range columns {
			c.Columns = append(c.Columns, plan.ColRef{ColPos: column})
		}
		return c
	}
	checks := []*plan.OdkuForeignKeyCheck{check(true, 0, 1), check(false, 2), check(true, 2)}
	leftTypes := []types.Type{types.T_int32.ToType(), types.T_int32.ToType(), types.T_int32.ToType()}
	rightTypes := []types.Type{types.T_bool.ToType()}
	execs, err := CompileFKChanges(checks, leftTypes, rightTypes, proc, account)
	require.NoError(t, err)
	require.Empty(t, execs[1], "an unchanged foreign key needs no update comparator")
	defer FreeFKChanges(execs)

	makeRow := func(values [3]int32, nullMiddle bool) *batch.Batch {
		b := batch.NewWithSize(3)
		for i, value := range values {
			var nulls []uint64
			if i == 1 && nullMiddle {
				nulls = []uint64{0}
			}
			b.Vecs[i] = testutil.MakeInt32Vector([]int32{value}, nulls, mp)
		}
		b.SetRowCount(1)
		return b
	}
	before := makeRow([3]int32{1, 0, 5}, true)
	defer before.Clean(mp)
	for _, tc := range []struct {
		name       string
		values     [3]int32
		nullMiddle bool
		want       []bool
	}{
		{"unchanged including null", [3]int32{1, 0, 5}, true, []bool{false, false, false}},
		{"first composite component", [3]int32{2, 0, 5}, true, []bool{true, false, false}},
		{"null becomes value", [3]int32{1, 10, 5}, false, []bool{true, false, false}},
		{"separate foreign key", [3]int32{1, 0, 6}, true, []bool{false, false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			after := makeRow(tc.values, tc.nullMiddle)
			defer after.Clean(mp)
			eligible := []bool{true, true, true}
			require.NoError(t, EvaluateFKChanges(checks, execs, before, after, eligible, proc))
			require.Equal(t, tc.want, eligible)
		})
	}
	require.Positive(t, account.Snapshot().Used, "evaluating comparisons must allocate through the account")
	FreeFKChanges(execs)
	FreeFKChanges(execs)
	require.Zero(t, account.Snapshot().Used)

	// A later invalid constraint must release an earlier compiled comparison.
	invalid := check(true, 3)
	partial, err := CompileFKChanges([]*plan.OdkuForeignKeyCheck{checks[0], invalid}, leftTypes, rightTypes, proc, account)
	require.Error(t, err)
	require.Nil(t, partial)
	require.Zero(t, account.Snapshot().Used)
}
