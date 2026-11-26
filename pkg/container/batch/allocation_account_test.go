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

// The batch does no accounting of its own: it allocates from the pool it is
// given, and a plain pool's CurrNB is enough to check that. The tests here need
// what only an allocation account has: an accounted buffer to adopt, a
// high-water mark, or a metadata-slot limit to make an allocation fail.

type testBatchAllocationAccount struct {
	registry *mpool.AllocationAccountRegistry
	account  *mpool.AllocationAccount
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
	return testBatchAllocationAccount{
		registry: registry,
		account:  account,
	}
}

// view returns an accounted view of mp charging the test account.
func (s testBatchAllocationAccount) view(t *testing.T, mp *mpool.MPool) *mpool.MPool {
	t.Helper()
	view, err := mp.WithAccount(s.account, 1, 1, mpool.AllocationCapacityClassDefault)
	require.NoError(t, err)
	return view
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

func TestBatchSetAccountedExtraBuffer(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 8)
	mp := mpool.MustNewZero()

	var nilBatch *Batch
	require.ErrorIs(t, nilBatch.SetAccountedExtraBuffer(nil), mpool.ErrAllocationAccountInvalid)
	empty := NewWithSize(0)
	require.ErrorIs(t, empty.SetAccountedExtraBuffer(nil), mpool.ErrAllocationAccountInvalid)
	empty.Clean(mp)

	buffer, err := mpool.NewAccountedBuffer(mp, state.account, 1, 5)
	require.NoError(t, err)
	_, err = buffer.Write(bytes.Repeat([]byte("x"), 4096))
	require.NoError(t, err)
	used := state.account.Snapshot().Used
	require.Positive(t, used)

	bat := NewWithSize(0)
	require.NoError(t, bat.SetAccountedExtraBuffer(buffer))
	buffer.Free() // the payload now belongs to bat
	require.Equal(t, used, state.account.Snapshot().Used)
	require.Len(t, bat.ExtraBuf(), 4096)

	bat.Clean(mp)
	require.Zero(t, state.account.Snapshot().Used)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchCloneToDoesNotStageASecondCopy(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	source := newTwoColumnTestBatch(t, mp)

	destination := NewWithSize(2)
	destination.Vecs[0] = vector.NewVec(types.T_int64.ToType(), view)
	destination.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), view)
	require.NoError(t, source.CloneTo(destination, view))
	require.NotZero(t, state.account.Snapshot().Used)
	require.Equal(t, source.RowCount(), destination.RowCount())
	for i := 0; i < source.RowCount(); i++ {
		require.Equal(t, source.Vecs[1].GetStringAt(i), destination.Vecs[1].GetStringAt(i))
	}
	// A staged second copy would leave the high-water mark above live usage.
	snapshot := state.account.Snapshot()
	require.Equal(t, snapshot.Used, snapshot.Peak)

	// The destination is reusable after FreeColumns.
	destination.FreeColumns(view)
	require.Zero(t, state.account.Snapshot().Used)
	require.NoError(t, destination.Union(source, []int64{1, 3, 5, 7}, view))
	require.Equal(t, 4, destination.RowCount())

	destination.Clean(view)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchCloneToReleasesPartialCopyOnError(t *testing.T) {
	// One metadata slot: the first column's copy succeeds, the second fails.
	state := newTestBatchAllocationAccount(t, 1)
	mp := mpool.MustNewZero()
	source := NewWithSize(2)
	for i := range source.Vecs {
		source.Vecs[i] = vector.NewVec(types.T_int64.ToType(), mp)
		require.NoError(t, vector.AppendFixed(source.Vecs[i], int64(i), false, mp))
	}
	source.SetRowCount(1)

	view := state.view(t, mp)
	destination := NewWithSize(2)
	for i := range destination.Vecs {
		destination.Vecs[i] = vector.NewVec(types.T_int64.ToType(), view)
	}
	err := source.CloneTo(destination, view)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())

	destination.Clean(view)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
	finalizeTestBatchAllocationAccount(t, state)
}
