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

package hashbuild

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestRecoveryArithmeticAndProjectionRejectInvalidShapes(t *testing.T) {
	_, err := recoveryCheckedAdd(math.MaxUint64, 1)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	value, err := recoveryCheckedAdd(1, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(3), value)
	_, err = recoveryCheckedMul(math.MaxUint64, 2)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	value, err = recoveryCheckedMul(0, math.MaxUint64)
	require.NoError(t, err)
	require.Zero(t, value)
	value, err = roundRecoveryCapacity(0)
	require.NoError(t, err)
	require.Zero(t, value)
	_, err = roundRecoveryCapacity(math.MaxUint64)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	value, err = roundRecoveryCapacity(1)
	require.NoError(t, err)
	require.Equal(t, recoveryCapacityQuantum, value)

	for _, projection := range []recoveryBatchProjection{
		{},
		{maxRows: -1},
		{maxRows: 1, columns: -1},
		{maxRows: math.MaxInt, columns: math.MaxInt, maxSelected: math.MaxUint64},
	} {
		_, err = spillRecoveryPeak(projection)
		require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	}
	peak, err := spillRecoveryPeak(recoveryBatchProjection{
		maxRows: 1, columns: 1, maxSelected: 1,
	})
	require.NoError(t, err)
	require.Positive(t, peak)

	builder := &HashmapBuilder{}
	_, err = builder.projectRetainedRecovery(nil)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	empty := batch.NewWithSize(0)
	_, err = builder.projectRetainedRecovery(empty)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	empty.SetRowCount(1)
	projection, err := builder.projectRetainedRecovery(empty)
	require.NoError(t, err)
	require.Equal(t, 1, projection.maxRows)
	require.Zero(t, projection.maxSelected)

	_, err = projectedSelectedRange(nil, 0, 0)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, err = projectedSelectedRange(empty, -1, 1)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, err = projectedSelectedRange(empty, 0, 2)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	withNil := batch.NewWithSize(1)
	withNil.SetRowCount(1)
	_, err = projectedSelectedRange(withNil, 0, 1)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
}

func TestUnionAreaProjectionBoundaryMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	physical, selected, err := unionBatchAreaProjection(nil, 0, 0)
	require.NoError(t, err)
	require.Zero(t, physical)
	require.Zero(t, selected)
	fixed := vector.NewVec(types.T_int64.ToType())
	physical, selected, err = unionBatchAreaProjection(fixed, 0, 0)
	require.NoError(t, err)
	require.Zero(t, physical)
	require.Zero(t, selected)
	fixed.Free(mp)

	flat := vector.NewVec(types.T_varchar.ToType())
	for _, value := range [][]byte{
		[]byte("inline"),
		make([]byte, types.VarlenaInlineSize+9),
		make([]byte, types.VarlenaInlineSize+17),
	} {
		require.NoError(t, vector.AppendBytes(flat, value, false, mp))
	}
	_, _, err = unionBatchAreaProjection(flat, -1, 1)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, _, err = unionBatchAreaProjection(flat, 2, 2)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	physical, selected, err = unionBatchAreaProjection(flat, 0, 0)
	require.NoError(t, err)
	require.Zero(t, physical)
	require.Zero(t, selected)
	physical, selected, err = unionBatchAreaProjection(flat, 1, 1)
	require.NoError(t, err)
	require.Positive(t, physical)
	require.Equal(t, uint64(physical), selected)
	physical, selected, err = unionBatchAreaProjection(flat, 0, flat.Length())
	require.NoError(t, err)
	require.GreaterOrEqual(t, uint64(physical), selected)
	flat.Free(mp)

	constant := vector.NewConstNull(types.T_varchar.ToType(), 0, mp)
	_, _, err = unionBatchAreaProjection(constant, 0, 1)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	constant.Free(mp)
	constant, err = vector.NewConstBytes(
		types.T_varchar.ToType(), make([]byte, types.VarlenaInlineSize+9), 3, mp)
	require.NoError(t, err)
	physical, selected, err = unionBatchAreaProjection(constant, 0, 3)
	require.NoError(t, err)
	require.Positive(t, physical)
	require.Equal(t, uint64(physical*3), selected)
	constant.Free(mp)
}

func TestSerializedRuntimeFilterBoundsNullAndAreaModes(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	keys := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(keys,
		make([]byte, types.VarlenaInlineSize+9), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(keys, nil, true, proc.Mp()))

	area, maxRow, err := serializedRuntimeFilterBounds(
		proc, []*vector.Vector{keys}, []int{0}, 2, false)
	require.NoError(t, err)
	require.Positive(t, area)
	require.Positive(t, maxRow)
	fullArea, fullMaxRow, err := serializedRuntimeFilterBounds(
		proc, []*vector.Vector{keys}, []int{0}, 2, true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, fullArea, area)
	require.GreaterOrEqual(t, fullMaxRow, maxRow)

	keys.Free(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestExpressionRecoveryInvalidAndLeafMatrix(t *testing.T) {
	_, err := expressionRecoveryBytes(nil, nil, 1, false)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, err = expressionRecoveryBytes(nil, []*plan.Expr{{}}, -1, false)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, err = expressionVectorPeak(nil, nil, 1, false)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, err = expressionVectorPeak(nil, &plan.Expr{}, -1, false)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, _, err = expressionTreePeakWithSelection(nil, nil, 1, false)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, _, err = expressionTreePeakWithSelection(nil, &plan.Expr{}, 1, false)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)

	column := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	total, output, err := expressionTreePeakWithSelection(nil, column, 10, false)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Zero(t, output)
	functionWithoutBody := &plan.Expr{Expr: &plan.Expr_F{}}
	_, _, err = expressionTreePeakWithSelection(nil, functionWithoutBody, 1, false)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	param := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	_, _, err = expressionTreePeakWithSelection(nil, param, 1, false)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)

	literal := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{}},
	}
	peak, err := expressionVectorPeak(nil, literal, 10, true)
	require.NoError(t, err)
	require.Positive(t, peak)
	peak, err = expressionTypePeak(plan.Type{Id: int32(types.T_varchar), Width: 1}, 2)
	require.NoError(t, err)
	require.Positive(t, peak)
	peak, err = expressionTypePeak(plan.Type{Id: int32(types.T_array_float32), Width: 2}, 2)
	require.NoError(t, err)
	require.Positive(t, peak)

	require.Nil(t, nodeFunctionArgs(column))
	require.Nil(t, nodeFunctionArgs(functionWithoutBody))
	require.False(t, expressionChildMayReceivePartialSelection(-1, 1, false))
	require.True(t, expressionChildMayReceivePartialSelection(function.IFF, 1, false))
	require.False(t, expressionChildMayReceivePartialSelection(function.IFF, 0, false))
	require.True(t, expressionChildMayReceivePartialSelection(function.COALESCE, 0, true))

	value, err := expressionAllocationCapacityUpperBound(0)
	require.NoError(t, err)
	require.Zero(t, value)
	value, err = expressionAllocationCapacityUpperBound(uint64(mpool.CapLimit))
	require.NoError(t, err)
	require.Equal(t, uint64(mpool.CapLimit), value)
	_, err = expressionFixedWidthPeak(math.MaxUint64, 2)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, err = expressionVarlenaWidthPeak(math.MaxUint64, 2)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, _, _, err = serialExpressionPackerBounds(nil)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, _, _, err = serialExpressionPackerBounds(&plan.Function{Args: []*plan.Expr{nil}})
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
}
