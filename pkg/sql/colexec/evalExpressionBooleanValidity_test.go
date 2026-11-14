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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBooleanNonNullPreparationTransitions(t *testing.T) {
	for _, tc := range []struct {
		op       string
		want     []bool
		nullRows []bool // left rows 0/1 become NULL; right rows remain true/false.
	}{
		{"and", []bool{true, false, false, false}, []bool{true, false}},
		{"or", []bool{true, true, true, false}, []bool{false, true}},
		{"xor", []bool{false, true, true, false}, []bool{true, true}},
		{"not", []bool{false, false, true, true}, []bool{true, true}},
	} {
		t.Run(tc.op, func(t *testing.T) {
			proc, allocation, _ := dependentTestAllocation(t)
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			for i, values := range [][]bool{{true, true, false, false}, {true, false, true, false}} {
				input.Vecs[i] = vector.NewVec(types.T_bool.ToType())
				require.NoError(t, vector.AppendFixedList(input.Vecs[i], values, nil, proc.Mp()))
			}
			input.SetRowCount(4)
			args := []*plan.Expr{{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}}
			if tc.op != "not" {
				args = append(args, &plan.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}})
			}
			executor, err := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, tc.op, args...), allocation)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			root := executor.(*FunctionExpressionExecutor)
			require.Nil(t, root.dictionaryInput, "physical normalization remains required")
			checkValues := func(result *vector.Vector) {
				t.Helper()
				require.False(t, result.HasNull())
				require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[bool](result))
			}
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			checkValues(result)
			require.Zero(t, result.GetNulls().GetBitmap().ExternalStorageCapacity())
			input.Vecs[0].SetNull(0)
			input.Vecs[0].SetNull(1)
			result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			for row, want := range tc.nullRows {
				require.Equal(t, want, result.IsNull(uint64(row)))
			}
			capacity := result.GetNulls().GetBitmap().ExternalStorageCapacity()
			require.Positive(t, capacity)
			input.Vecs[0].GetNulls().Reset()
			result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			checkValues(result)
			require.Equal(t, capacity, result.GetNulls().GetBitmap().ExternalStorageCapacity())
			result, err = executor.Eval(proc, []*batch.Batch{input}, []bool{false, true, true, true})
			require.NoError(t, err)
			require.True(t, result.IsNull(0), "masked rows must keep normal NULL admission")
			for row := 1; row < 4; row++ {
				require.False(t, result.IsNull(uint64(row)))
				require.Equal(t, tc.want[row], vector.GetFixedAtNoTypeCheck[bool](result, row))
			}
			input.Vecs[0].SetNull(0)
			result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.nullRows[0], result.IsNull(0), "NULL after mask/nonnull reuse")
			input.Vecs[0].GetNulls().Reset()
			input.SetRowCount(0)
			result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Zero(t, result.Length())
		})
	}
}

func TestNumericComparisonValidityWithoutDictionaryCapability(t *testing.T) {
	for _, oid := range []types.T{types.T_uint64, types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		t.Run(oid.String(), func(t *testing.T) {
			proc, allocation, _ := dependentTestAllocation(t)
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			for side := range 2 {
				typ := oid.ToType()
				if oid.IsDecimal() && side == 0 {
					typ.Scale = 2
				}
				v := vector.NewVec(typ)
				input.Vecs[side] = v
				values := []uint64{100, 150, 90, 200}
				if side == 1 {
					values = []uint64{1, 2, 1, 2}
				}
				for _, n := range values {
					f := float64(n)
					if side == 0 {
						f /= 100
					}
					switch oid {
					case types.T_uint64:
						if side == 1 {
							n *= 100
						}
						require.NoError(t, vector.AppendFixed(v, n, false, proc.Mp()))
					case types.T_float32:
						require.NoError(t, vector.AppendFixed(v, float32(f), false, proc.Mp()))
					case types.T_float64:
						require.NoError(t, vector.AppendFixed(v, f, false, proc.Mp()))
					case types.T_decimal64:
						require.NoError(t, vector.AppendFixed(v, types.Decimal64(n), false, proc.Mp()))
					case types.T_decimal128:
						require.NoError(t, vector.AppendFixed(v, types.Decimal128{B0_63: n}, false, proc.Mp()))
					case types.T_decimal256:
						require.NoError(t, vector.AppendFixed(v, types.Decimal256{B0_63: n}, false, proc.Mp()))
					}
				}
			}
			input.SetRowCount(4)
			cols := make([]*plan.Expr, 0, len(input.Vecs))
			for side, v := range input.Vecs {
				cols = append(cols, &plan.Expr{Typ: v.GetType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(side)}}})
			}
			executor, err := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, ">=", cols...), allocation)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			root := executor.(*FunctionExpressionExecutor)
			root.dictionaryInput = nil
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, []bool{true, false, false, true}, vector.MustFixedColNoTypeCheck[bool](result))
			require.Zero(t, result.GetNulls().GetBitmap().ExternalStorageCapacity())
			input.Vecs[0].SetNull(1)
			result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.True(t, result.IsNull(1))
			require.Positive(t, result.GetNulls().GetBitmap().ExternalStorageCapacity())
		})
	}
}

func TestBooleanValidityConservativeMetadataAndConstants(t *testing.T) {
	for _, shape := range []string{"constant", "grouping", "prepared-json"} {
		t.Run(shape, func(t *testing.T) {
			proc, allocation, _ := dependentTestAllocation(t)
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			for side := range 2 {
				var err error
				input.Vecs[side], err = vector.NewConstFixed(types.T_bool.ToType(), true, 4, proc.Mp())
				require.NoError(t, err)
			}
			input.SetRowCount(4)
			if shape == "grouping" {
				input.Vecs[0].GetGrouping().Add(1)
			} else if shape == "prepared-json" {
				input.Vecs[0].SetPreparedJSONComparisonParam()
			}
			cols := make([]*plan.Expr, 0, 2)
			for side := range 2 {
				cols = append(cols, &plan.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(side)}}})
			}
			executor, err := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, "and", cols...), allocation)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, []bool{true, true, true, true}, vector.MustFixedColNoTypeCheck[bool](result))
			require.Equal(t, shape != "constant", result.GetNulls().GetBitmap().ExternalStorageCapacity() > 0)
		})
	}
}

func TestBooleanValidityAdmissionFailure(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(8)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	input := batch.NewWithSize(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], []bool{true, false, true, false, true, false, true, false}, nil, proc.Mp()))
	input.SetRowCount(8)
	column := &plan.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	executor, err := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, "not", column), allocation)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 8, account.Snapshot().Used)
	require.Zero(t, result.GetNulls().GetBitmap().ExternalStorageCapacity())
	input.Vecs[0].SetNull(0)
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity, "nullable fallback still admits its bitmap")
	input.Vecs[0].GetNulls().Reset()
	result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.False(t, result.HasNull())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], false, false, proc.Mp()))
	input.SetRowCount(9)
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity, "nonnull preparation still admits payload growth")
}
