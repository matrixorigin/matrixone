// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batch

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type testBatchAllocationAccount struct {
	registry  *mpool.AllocationAccountRegistry
	account   *mpool.AllocationAccount
	selection *vector.AllocationAccountSelection
}

func newTestBatchAllocationAccount(
	t *testing.T,
	allocationSlots uint64,
) testBatchAllocationAccount {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, allocationSlots)
	require.NoError(t, err)
	account, err := registry.Open(16 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2)
	require.NoError(t, err)
	return testBatchAllocationAccount{
		registry:  registry,
		account:   account,
		selection: selection,
	}
}

func finalizeTestBatchAllocationAccount(
	t *testing.T,
	state testBatchAllocationAccount,
) {
	t.Helper()
	snapshot := state.account.Seal()
	require.Zero(t, snapshot.Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	_, err := state.registry.Finalize(state.account)
	require.NoError(t, err)
}

func newBatchAllocationTestSource(
	t *testing.T,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) *Batch {
	t.Helper()
	bat := NewWithSchema(
		true,
		[]string{"id", "value"},
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
	)
	if selection != nil {
		require.NoError(t, bat.SetAllocationAccount(selection))
	}
	for i := 0; i < 32; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), false, mp))
		require.NoError(
			t,
			vector.AppendBytes(
				bat.Vecs[1],
				[]byte("batch allocation payload that is not inline"),
				false,
				mp,
			),
		)
	}
	bat.SetRowCount(32)
	return bat
}

func TestBatchAllocationAccountCloneDupAndWindow(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	source := newBatchAllocationTestSource(t, mp, state.selection)
	sourceUsed := state.account.Snapshot().Used
	require.NotZero(t, sourceUsed)
	_, err := source.Clone(mp, false)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

	cloned, err := source.Clone(mp, true)
	require.NoError(t, err)
	require.Same(t, state.selection, cloned.AllocationAccountSelection())
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	cloned.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	dup, err := source.Dup(mp)
	require.NoError(t, err)
	require.Same(t, state.selection, dup.AllocationAccountSelection())
	dup.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	selectedColumns, err := source.CloneSelectedColumns(
		[]int{1},
		[]string{"value"},
		mp,
	)
	require.NoError(t, err)
	require.Same(t, state.selection, selectedColumns.AllocationAccountSelection())
	selectedColumns.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	window, err := source.Window(4, 12)
	require.NoError(t, err)
	require.Same(t, state.selection, window.AllocationAccountSelection())
	for _, vec := range window.Vecs {
		require.Nil(t, vec.AllocationAccountSelection())
	}
	window.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	var encoded bytes.Buffer
	data, err := source.MarshalBinaryWithBuffer(&encoded, true)
	require.NoError(t, err)
	decoded := NewOffHeapEmpty()
	require.NoError(t, decoded.SetAllocationAccount(state.selection))
	require.NoError(t, decoded.UnmarshalFromReader(bytes.NewReader(data), mp))
	require.Same(t, state.selection, decoded.AllocationAccountSelection())
	for _, vec := range decoded.Vecs {
		require.Same(t, state.selection, vec.AllocationAccountSelection())
	}
	decoded.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	aliasDecoded := NewWithSchema(
		true,
		source.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
	)
	require.NoError(t, aliasDecoded.SetAllocationAccount(state.selection))
	require.NoError(t, aliasDecoded.UnmarshalBinaryWithAnyMp(data, mp))
	require.Same(t, state.selection, aliasDecoded.AllocationAccountSelection())
	for _, vec := range aliasDecoded.Vecs {
		require.Nil(t, vec.AllocationAccountSelection())
	}
	aliasDecoded.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAllocationAccountDestinationCloneUnionAndReuse(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	source := newBatchAllocationTestSource(t, mp, nil)

	destination := NewWithSchema(
		true,
		source.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
	)
	require.NoError(t, destination.SetAllocationAccount(state.selection))
	require.NoError(t, source.CloneTo(destination, mp))
	require.NotZero(t, state.account.Snapshot().Used)
	require.Equal(t, source.RowCount(), destination.RowCount())

	destination.FreeColumns(mp)
	require.Zero(t, state.account.Snapshot().Used)
	for _, vec := range destination.Vecs {
		require.Same(t, state.selection, vec.AllocationAccountSelection())
	}

	require.NoError(t, destination.Union(source, []int64{1, 3, 5, 7}, mp))
	require.Equal(t, 4, destination.RowCount())
	require.NotZero(t, state.account.Snapshot().Used)

	destination.Clean(mp)
	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAllocationAccountCloneRollback(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 1)
	mp := mpool.MustNewZero()
	source := NewWithSchema(
		true,
		[]string{"left", "right"},
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
	)
	for i := range source.Vecs {
		require.NoError(t, vector.AppendFixed(source.Vecs[i], int64(i), false, mp))
	}
	source.SetRowCount(1)

	destination := NewWithSchema(
		true,
		source.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
	)
	require.NoError(t, destination.SetAllocationAccount(state.selection))
	err := source.CloneTo(destination, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	require.Nil(t, destination.AllocationAccountSelection())

	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAllocationAccountConfigurationIsAtomic(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 8)
	mp := mpool.MustNewZero()

	onHeap := NewWithSchema(
		false,
		nil,
		[]types.Type{types.T_int64.ToType()},
	)
	require.ErrorIs(
		t,
		onHeap.SetAllocationAccount(state.selection),
		mpool.ErrAllocationAccountInvalid,
	)

	offHeap := NewWithSchema(
		true,
		nil,
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
	)
	require.NoError(t, vector.AppendFixed(offHeap.Vecs[1], int64(1), false, mp))
	require.ErrorIs(
		t,
		offHeap.SetAllocationAccount(state.selection),
		mpool.ErrAllocationAccountInvalid,
	)
	require.Nil(t, offHeap.Vecs[0].AllocationAccountSelection())
	require.Nil(t, offHeap.AllocationAccountSelection())

	offHeap.Clean(mp)
	onHeap.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}
