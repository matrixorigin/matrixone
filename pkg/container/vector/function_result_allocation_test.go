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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestFunctionResultAllocationAccountLifecycle(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 8)
	mp := mpool.MustNew("function-result-allocation")
	defer mpool.DeleteMPool(mp)

	fixed, err := NewFunctionResultWrapperWithAllocation(
		types.T_int64.ToType(),
		mp,
		state.selection,
	)
	require.NoError(t, err)
	require.NoError(t, fixed.PreExtendAndReset(8))
	fixedVector := fixed.GetResultVector()
	require.Same(
		t,
		state.selection,
		fixedVector.AllocationAccountSelection(),
	)
	firstUsed := state.account.Snapshot().Used
	require.Positive(t, firstUsed)

	require.NoError(t, fixed.PreExtendAndReset(2))
	require.Equal(t, firstUsed, state.account.Snapshot().Used)

	fixed.SetResultVector(nil)
	require.NoError(t, fixed.PreExtendAndReset(16))
	transferredUsed := state.account.Snapshot().Used
	require.Greater(t, transferredUsed, firstUsed)
	fixed.Free()
	require.Equal(t, firstUsed, state.account.Snapshot().Used)
	fixedVector.Free(mp)
	require.Zero(t, state.account.Snapshot().Used)

	varlen, err := NewFunctionResultWrapperWithAllocation(
		types.T_varchar.ToType(),
		mp,
		state.selection,
	)
	require.NoError(t, err)
	require.NoError(t, varlen.PreExtendAndReset(2))
	result := MustFunctionResult[types.Varlena](varlen)
	require.NoError(t, result.AppendBytes(make([]byte, 256), false))
	require.NoError(t, result.AppendBytes([]byte("small"), false))
	varlenUsed := state.account.Snapshot().Used
	require.Positive(t, varlenUsed)

	require.NoError(t, varlen.PreExtendAndReset(1))
	require.Equal(t, varlenUsed, state.account.Snapshot().Used)
	varlen.Free()
	require.Zero(t, state.account.Snapshot().Used)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestFunctionResultAllocationAccountFailure(t *testing.T) {
	zeroMP := mpool.MustNewZero()
	defer mpool.DeleteMPool(zeroMP)
	require.ErrorIs(
		t,
		func() error {
			_, err := NewFunctionResultWrapperWithAllocation(
				types.T_int64.ToType(),
				zeroMP,
				nil,
			)
			return err
		}(),
		mpool.ErrAllocationAccountInvalid,
	)

	state := newTestVectorAllocationAccount(t, 1<<20, 4)
	_, err := NewFunctionResultWrapperWithParameterAllocation(
		types.T_int64.ToType(),
		zeroMP,
		state.selection,
		nil,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	otherOwner, err := NewFunctionParameterAllocation(
		state.account,
		testVectorAllocationOwner+1,
		testVectorParamAllocationSite,
	)
	require.NoError(t, err)
	_, err = NewFunctionResultWrapperWithParameterAllocation(
		types.T_int64.ToType(),
		zeroMP,
		state.selection,
		otherOwner,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, err = NewFunctionParameterAllocation(
		nil,
		testVectorAllocationOwner,
		testVectorParamAllocationSite,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	finalizeTestVectorAllocationAccount(t, state)

	state = newTestVectorAllocationAccount(t, 7, 1)
	mp := mpool.MustNew("function-result-allocation-failure")
	defer mpool.DeleteMPool(mp)
	result, err := NewFunctionResultWrapperWithAllocation(
		types.T_int64.ToType(),
		mp,
		state.selection,
	)
	require.NoError(t, err)

	err = result.PreExtendAndReset(1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	result.Free()
	finalizeTestVectorAllocationAccount(t, state)
}

func TestFunctionResultAllocationAccountDecimalParameterScratch(t *testing.T) {
	state := newTestVectorParameterAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNew("function-parameter-allocation")
	defer mpool.DeleteMPool(mp)
	result, err := NewFunctionResultWrapperWithParameterAllocation(
		types.T_bool.ToType(),
		mp,
		state.selection,
		state.parameter,
	)
	require.NoError(t, err)
	result.UseOptFunctionParamFrame(1)

	source := NewOffHeapVecWithType(types.T_decimal64.ToType())
	for i := int64(1); i <= 32; i++ {
		require.NoError(t, AppendFixed(
			source,
			types.Decimal64(i),
			i%7 == 0,
			mp,
		))
	}
	parameter, err := OptGetParamFromWrapper[types.Decimal128](
		result,
		0,
		source,
	)
	require.NoError(t, err)
	values := parameter.UnSafeGetAllValue()
	require.Len(t, values, source.Length())
	require.Equal(t, types.Decimal128{B0_63: 1}, values[0])
	_, isNull := parameter.GetValue(6)
	require.True(t, isNull)
	first := state.account.Snapshot()
	require.Positive(t, first.Used)
	require.Equal(t, uint64(1), state.registry.LiveAllocationMetadata())

	reused, err := OptGetParamFromWrapper[types.Decimal128](
		result,
		0,
		source,
	)
	require.NoError(t, err)
	require.Same(t, parameter, reused)
	require.Equal(t, first.Used, state.account.Snapshot().Used)

	float32Source := NewOffHeapVecWithType(types.T_float32.ToType())
	require.NoError(t, AppendFixed(float32Source, float32(1.25), false, mp))
	float32Parameter, err := OptGetParamFromWrapper[types.Decimal128](
		result,
		0,
		float32Source,
	)
	require.NoError(t, err)
	expectedFloat32, err := types.Decimal128FromFloat64(1.25, 38, 7)
	require.NoError(t, err)
	require.Equal(t, expectedFloat32, float32Parameter.UnSafeGetAllValue()[0])
	require.Equal(t, int32(7), float32Parameter.GetType().Scale)

	float64Source := NewOffHeapVecWithType(types.T_float64.ToType())
	require.NoError(t, AppendFixed(float64Source, 2.5, false, mp))
	float64Parameter, err := OptGetParamFromWrapper[types.Decimal128](
		result,
		0,
		float64Source,
	)
	require.NoError(t, err)
	expectedFloat64, err := types.Decimal128FromFloat64(2.5, 38, 16)
	require.NoError(t, err)
	require.Equal(t, expectedFloat64, float64Parameter.UnSafeGetAllValue()[0])
	require.Equal(t, int32(16), float64Parameter.GetType().Scale)

	constSource, err := NewConstFixed(
		types.T_float64.ToType(),
		3.5,
		8,
		mp,
	)
	require.NoError(t, err)
	beforeConst := state.account.Snapshot().Used
	constParameter, err := OptGetParamFromWrapper[types.Decimal128](
		result,
		0,
		constSource,
	)
	require.NoError(t, err)
	expectedConst, err := types.Decimal128FromFloat64(3.5, 38, 16)
	require.NoError(t, err)
	value, isNull := constParameter.GetValue(7)
	require.False(t, isNull)
	require.Equal(t, expectedConst, value)
	require.Equal(t, beforeConst, state.account.Snapshot().Used)

	result.Free()
	require.Zero(t, state.account.Snapshot().Used)
	source.Free(mp)
	float32Source.Free(mp)
	float64Source.Free(mp)
	constSource.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestFunctionResultAllocationAccountDecimalParameterFailure(t *testing.T) {
	state := newTestVectorParameterAllocationAccount(t, 127, 4)
	mp := mpool.MustNew("function-parameter-allocation-failure")
	defer mpool.DeleteMPool(mp)
	result, err := NewFunctionResultWrapperWithParameterAllocation(
		types.T_bool.ToType(),
		mp,
		state.selection,
		state.parameter,
	)
	require.NoError(t, err)
	result.UseOptFunctionParamFrame(1)

	source := NewOffHeapVecWithType(types.T_decimal64.ToType())
	for i := 0; i < 8; i++ {
		require.NoError(t, AppendFixed(
			source,
			types.Decimal64(i),
			false,
			mp,
		))
	}
	_, err = OptGetParamFromWrapper[types.Decimal128](
		result,
		0,
		source,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())

	source.SetLength(7)
	_, err = OptGetParamFromWrapper[types.Decimal128](
		result,
		0,
		source,
	)
	require.NoError(t, err)
	require.Positive(t, state.account.Snapshot().Used)

	result.Free()
	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}
