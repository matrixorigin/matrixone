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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func dependentTestAllocation(t *testing.T) (*process.Process, *vector.AllocationAccountSelection, *mpool.AllocationAccount) {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	registry, err := mpool.NewAllocationAccountRegistry(1, 256)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		account.Seal()
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	})
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerDependent, 1, 2, 3, 4)
	require.NoError(t, err)
	return proc, selection, account
}

func dependentTestInts(t *testing.T, proc *process.Process, values ...*int64) *batch.Batch {
	t.Helper()
	b := batch.NewWithSize(1)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { b.Clean(proc.Mp()) })
	for _, v := range values {
		var n int64
		if v != nil {
			n = *v
		}
		require.NoError(t, vector.AppendFixed(b.Vecs[0], n, v == nil, proc.Mp()))
	}
	b.SetRowCount(len(values))
	return b
}

func dependentTestEqual(t *testing.T, proc *process.Process) *plan.Expr {
	t.Helper()
	overload, err := function.GetFunctionByName(proc.Ctx, "=", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: overload.GetEncodedOverloadID(), ObjName: "="},
		Args: []*plan.Expr{
			{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}},
			{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1}}},
		},
	}}}
}

func TestDependentScalarCardinalityAndOwnership(t *testing.T) {
	for _, rows := range []int{0, 1, 2} {
		t.Run(string(rune('0'+rows)), func(t *testing.T) {
			proc, allocation, _ := dependentTestAllocation(t)
			reducer, err := NewDependentReducer(proc, &plan.DependentExpr{Reducer: plan.SubqueryRef_SCALAR}, []plan.Type{{Id: int32(types.T_int64)}}, nil, allocation)
			require.NoError(t, err)
			defer reducer.Close()
			value := int64(41)
			input := dependentTestInts(t, proc, &value)
			for i := 0; i < rows; i++ {
				stop, feedErr := reducer.Feed(input)
				require.False(t, stop)
				if i == 1 {
					require.True(t, moerr.IsMoErrCode(feedErr, moerr.ErrSubqueryNo1Row))
				} else {
					require.NoError(t, feedErr)
				}
			}
			// Reuse the producer batch before reading the scalar result.
			input.Vecs[0].CleanOnlyData()
			require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(99), false, proc.Mp()))
			result, err := reducer.Finish()
			if rows == 2 {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrSubqueryNo1Row))
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.Equal(t, rows == 0, result.Vecs[0].IsNull(0))
			if rows == 1 {
				require.Equal(t, int64(41), vector.GetFixedAtNoTypeCheck[int64](result.Vecs[0], 0))
			}
			again, err := reducer.Finish()
			require.NoError(t, err)
			require.Same(t, result, again)
		})
	}
}

func TestDependentQuantifiedTruthTable(t *testing.T) {
	one, two := int64(1), int64(2)
	cases := []struct {
		name             string
		values           []*int64
		any, all         bool
		anyNull, allNull bool
	}{
		{"empty", nil, false, true, false, false},
		{"equal", []*int64{&one}, true, true, false, false},
		{"different", []*int64{&two}, false, false, false, false},
		{"null", []*int64{nil}, false, false, true, true},
		{"null_then_equal", []*int64{nil, &one}, true, false, false, true},
		{"null_then_different", []*int64{nil, &two}, false, false, true, false},
	}
	for _, kind := range []plan.SubqueryRef_Type{plan.SubqueryRef_IN, plan.SubqueryRef_NOT_IN, plan.SubqueryRef_ANY, plan.SubqueryRef_ALL} {
		for _, tc := range cases {
			t.Run(kind.String()+"/"+tc.name, func(t *testing.T) {
				proc, allocation, _ := dependentTestAllocation(t)
				lhs := dependentTestInts(t, proc, &one)
				reducer, err := NewDependentReducer(proc, &plan.DependentExpr{Reducer: kind, Comparison: dependentTestEqual(t, proc)}, []plan.Type{{Id: int32(types.T_int64)}}, lhs, allocation)
				require.NoError(t, err)
				defer reducer.Close()
				_, err = reducer.Feed(dependentTestInts(t, proc, tc.values...))
				require.NoError(t, err)
				result, err := reducer.Finish()
				require.NoError(t, err)
				want, null := tc.any, tc.anyNull
				if kind == plan.SubqueryRef_ALL {
					want, null = tc.all, tc.allNull
				}
				if kind == plan.SubqueryRef_NOT_IN {
					want = !want
				}
				require.Equal(t, null, result.Vecs[0].IsNull(0))
				if !null {
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result.Vecs[0], 0))
				}
			})
		}
	}
}

func TestDependentExistsAndBudgetFailure(t *testing.T) {
	proc, allocation, account := dependentTestAllocation(t)
	value := int64(1)
	for _, kind := range []plan.SubqueryRef_Type{plan.SubqueryRef_EXISTS, plan.SubqueryRef_NOT_EXISTS} {
		for _, present := range []bool{false, true} {
			func() {
				r, err := NewDependentReducer(proc, &plan.DependentExpr{Reducer: kind}, []plan.Type{{Id: int32(types.T_int64)}}, nil, allocation)
				require.NoError(t, err)
				defer r.Close()
				if present {
					stop, err := r.Feed(dependentTestInts(t, proc, &value))
					require.NoError(t, err)
					require.True(t, stop)
				}
				result, err := r.Finish()
				require.NoError(t, err)
				want := present
				if kind == plan.SubqueryRef_NOT_EXISTS {
					want = !want
				}
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result.Vecs[0], 0))
			}()
		}
	}
	require.Zero(t, account.Snapshot().Used)
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	limited, err := registry.Open(1)
	require.NoError(t, err)
	defer func() {
		limited.Seal()
		_, err := registry.Finalize(limited)
		require.NoError(t, err)
	}()
	selection, err := vector.NewAllocationAccountSelection(limited, mpool.AllocationOwnerDependent, 1, 2, 3, 4)
	require.NoError(t, err)
	r, err := NewDependentReducer(proc, &plan.DependentExpr{Reducer: plan.SubqueryRef_SCALAR}, []plan.Type{{Id: int32(types.T_int64)}}, nil, selection)
	require.NoError(t, err)
	defer r.Close()
	_, err = r.Feed(dependentTestInts(t, proc, &value))
	require.Error(t, err)
	_, finishErr := r.Finish()
	require.ErrorIs(t, finishErr, err)
	r.Close()
	r.Close()
	require.Zero(t, limited.Snapshot().Used)
}
