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

	state := newTestVectorAllocationAccount(t, 7, 1)
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
