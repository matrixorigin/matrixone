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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDictionaryPredicateOriginalRowsAndErrorOrder(t *testing.T) {
	for _, skipError := range []bool{false, true} {
		t.Run(map[bool]string{false: "original-rows", true: "skip-divide-zero"}[skipError], func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
			warnings := &process.WarningProbe{}
			proc.WarningSink = warnings
			typ := types.T_float64.ToType()
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(mp) })
			input.Vecs[0] = vector.NewVec(typ)
			values := []float64{1, 2, 3, 4}
			if skipError {
				values = []float64{0, 2}
			}
			require.NoError(t, vector.AppendFixedList(input.Vecs[0], values, nil, mp))
			input.SetRowCount(len(values))
			col := &plan.Expr{Typ: plan.Type{Id: int32(typ.Oid)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			literal := func(value float64) *plan.Expr {
				return &plan.Expr{Typ: plan.Type{Id: int32(typ.Oid)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: value}}}}
			}
			predicates := []*plan.Expr{bindTestFunction(t, proc, ">", col, literal(1)), bindTestFunction(t, proc, "<", col, literal(4))}
			want := []int64{1, 2}
			if skipError {
				predicates = []*plan.Expr{bindTestFunction(t, proc, "<>", col, literal(0)), bindTestFunction(t, proc, ">", bindTestFunction(t, proc, "/", literal(10), col), literal(0))}
				want = []int64{1}
			}
			executors, err := NewExpressionExecutorsFromPlanExpressions(proc, predicates)
			require.NoError(t, err)
			t.Cleanup(func() {
				for _, executor := range executors {
					executor.Free()
				}
			})
			var scratch []int64
			var parameter vector.FunctionParameterWrapper[bool]
			got, all, err := SelectPredicateRows(proc, input, nil, executors, nil, &scratch, &parameter, nil, nil, nil)
			require.NoError(t, err)
			require.False(t, all)
			require.Equal(t, want, got)
			require.Equal(t, len(values), input.RowCount())
			require.Equal(t, values, vector.MustFixedColNoTypeCheck[float64](input.Vecs[0]))
			require.False(t, warnings.Warned(), "rejected rows must not emit division diagnostics")
		})
	}
}

func TestPredicateSelectionRetainsOnlyReferencedColumns(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(mp) })
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	input.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int64{10, 20, 30}, nil, mp))
	for _, value := range []string{"unreferenced payload first", "unreferenced payload second", "unreferenced payload third"} {
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(value), false, mp))
	}
	input.SetRowCount(3)
	executor, err := NewExpressionExecutor(proc, &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}})
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	bases := make([]*vector.DictionaryBase, 2)
	declined := make([]bool, 2)
	t.Cleanup(func() {
		for _, base := range bases {
			if base != nil {
				base.Release()
			}
		}
	})
	first, err := selectPredicateInput(proc, input, nil, executor, []int64{2, 0}, nil, bases, declined)
	require.NoError(t, err)
	t.Cleanup(func() { first.Clean(mp) })
	require.Nil(t, first.Vecs[1])
	require.Nil(t, bases[1])
	require.True(t, first.Vecs[0].IsDictionary())
	base := bases[0]
	second, err := selectPredicateInput(proc, input, nil, executor, []int64{1}, nil, bases, declined)
	require.NoError(t, err)
	t.Cleanup(func() { second.Clean(mp) })
	require.Same(t, base, bases[0])
	require.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](second.Vecs[0], 0))
	require.Equal(t, int64(30), vector.GetFixedAtNoTypeCheck[int64](first.Vecs[0], 0))
}
