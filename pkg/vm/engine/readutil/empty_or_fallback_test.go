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

package readutil

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestEmptyOrFilterFallsBackWithoutDroppingMembership(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	table := &plan.TableDef{
		Name2ColIndex: map[string]int32{"amount": 0},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "amount", Names: []string{"amount"}},
		Cols:          []*plan.ColDef{{Name: "amount", Primary: true, Typ: plan.Type{Id: int32(types.T_int64)}}},
	}
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "nested"}[nested], func(t *testing.T) {
			expr := foldedFunction("or", nil)
			if nested {
				expr = foldedFunction("or", []*plan.Expr{int64FoldedFilter("=", 2), expr})
			}
			before, heapBefore := proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()
			base, err := ConstructBasePKFilter(expr, table, proc.Mp())
			require.NoError(t, err)
			require.False(t, base.Valid)
			member := &testMembershipFilter{hits: []uint8{1, 0, 1}}
			mem, err := NewMemPKFilter(table, types.MaxTs().ToTimestamp(), newTestPackerPool(), base, engine.FilterHint{BF: member})
			require.NoError(t, err)
			require.False(t, mem.Valid())
			require.True(t, mem.HasBF)

			block, err := ConstructBlockPKFilter(false, base, member)
			require.NoError(t, err)
			require.True(t, block.Valid)
			pk := vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(pk, []int64{1, 2, 3}, nil, proc.Mp()))
			require.Equal(t, []int64{0, 2}, block.UnSortedSearchFunc(containers.Vectors{*pk}))
			pk.Free(proc.Mp())
			if block.Cleanup != nil {
				block.Cleanup()
			}
			base.Cleanup()
			require.Equal(t, before, proc.Mp().CurrNB())
			require.Equal(t, heapBefore, proc.Mp().OnHeapCurrNB())
		})
	}
}

func TestEmptyOrClusterFilterFailsOpen(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	table := makeCompositeClusterTableDef(binaryCharset)
	value, _ := serialConstForTest(t, proc, plan2.MakePlan2Int32ConstExprWithType(1))
	comparison := bindAndFold(t, proc, "prefix_eq", clusterColExpr(7), value)
	control, err := ConstructBaseClusterFilter(comparison, table, proc.Mp())
	require.NoError(t, err)
	require.True(t, control.Valid)
	control.Cleanup()
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "nested"}[nested], func(t *testing.T) {
			expr := foldedFunction("or", nil)
			if nested {
				expr = foldedFunction("or", []*plan.Expr{comparison, expr})
			}
			before, heapBefore := proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()
			base, err := ConstructBaseClusterFilter(expr, table, proc.Mp())
			require.NoError(t, err)
			require.False(t, base.Valid)
			base.Cleanup()
			filter, err := NewMemClusterKeyFilter(table, expr, types.MaxTs().ToTimestamp(), proc.Mp(), newTestPackerPool())
			require.NoError(t, err)
			require.False(t, filter.Valid())
			require.Equal(t, before, proc.Mp().CurrNB())
			require.Equal(t, heapBefore, proc.Mp().OnHeapCurrNB())
		})
	}
}
