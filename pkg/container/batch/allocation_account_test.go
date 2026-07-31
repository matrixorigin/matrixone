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
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
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
	require.NoError(
		t,
		aliasDecoded.UnmarshalFromReader(bytes.NewReader(data), mp),
	)
	for _, vec := range aliasDecoded.Vecs {
		require.Same(t, state.selection, vec.AllocationAccountSelection())
	}
	require.Equal(t, source.RowCount(), aliasDecoded.RowCount())
	aliasDecoded.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAccountedReaderAcceptsBitmapCapacityBeyondLogicalRows(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	source := newBatchAllocationTestSource(t, mp, nil)
	source.Vecs[0].GetNulls().Add(31)
	source.Shrink([]int64{0, 1, 2, 3, 4}, false)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryTo(&encoded))
	decoded := NewOffHeapEmpty()
	require.NoError(t, decoded.SetAllocationAccount(state.selection))
	require.NoError(t, decoded.UnmarshalFromReader(&encoded, mp))
	require.Equal(t, 5, decoded.RowCount())
	require.Equal(
		t,
		int64(0),
		vector.GetFixedAtWithTypeCheck[int64](decoded.Vecs[0], 0),
	)

	decoded.Clean(mp)
	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAccountedReaderPreservesRowsWhenVectorCountChanges(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	source := newBatchAllocationTestSource(t, mp, nil)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryTo(&encoded))
	decoded := NewOffHeapWithSize(1)
	decoded.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, decoded.SetAllocationAccount(state.selection))
	decoded.SetRowCount(7)
	require.NoError(t, decoded.UnmarshalFromReader(&encoded, mp))
	require.Equal(t, source.RowCount(), decoded.RowCount())
	require.Len(t, decoded.Vecs, 2)
	require.Equal(
		t,
		int64(31),
		vector.GetFixedAtWithTypeCheck[int64](decoded.Vecs[0], 31),
	)

	decoded.Clean(mp)
	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchGroupingCodecRoundTrip(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	source := newBatchAllocationTestSource(t, mp, nil)
	source.Vecs[0].GetGrouping().Add(1, 7, 31)
	source.Vecs[1].GetGrouping().Add(2, 9)
	source.ExtraBuf = bytes.Repeat([]byte("x"), 1<<20)

	var encoded bytes.Buffer
	spillSize, err := source.MarshalBinaryWithGroupingSize()
	require.NoError(t, err)
	stableSize, err := source.MarshalBinarySize()
	require.NoError(t, err)
	require.Greater(t, stableSize-spillSize, len(source.ExtraBuf)/2)
	require.NoError(t, source.MarshalBinaryWithGroupingTo(&encoded))
	decoded := NewOffHeapEmpty()
	require.NoError(t, decoded.SetAllocationAccount(state.selection))
	require.NoError(t, decoded.UnmarshalFromReaderWithGrouping(&encoded, mp))
	require.Empty(t, decoded.Attrs)
	require.Empty(t, decoded.ExtraBuf)
	for i := range source.Vecs {
		require.True(t, decoded.Vecs[i].GetGrouping().IsSame(source.Vecs[i].GetGrouping()))
	}
	withoutGrouping := newBatchAllocationTestSource(t, mp, nil)
	encoded.Reset()
	require.NoError(t, withoutGrouping.MarshalBinaryWithGroupingTo(&encoded))
	require.NoError(t, decoded.UnmarshalFromReaderWithGrouping(&encoded, mp))
	for _, vec := range decoded.Vecs {
		require.True(t, vec.GetGrouping().IsEmpty())
	}

	decoded.Clean(mp)
	withoutGrouping.Clean(mp)
	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchGroupingCodecRejectsStableMetadataBeforePayloadAllocation(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 32)
	mp := mpool.MustNewZero()

	for _, test := range []struct {
		name  string
		attrs []string
		extra []byte
		want  string
	}{
		{
			name:  "attributes",
			attrs: []string{string(bytes.Repeat([]byte("a"), 1<<20))},
			want:  "attributes are not allowed",
		},
		{
			name:  "extra buffer",
			extra: bytes.Repeat([]byte("x"), 1<<20),
			want:  "extra buffer is not allowed",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := NewWithSize(0)
			source.Attrs = test.attrs
			source.ExtraBuf = test.extra
			var encoded bytes.Buffer
			require.NoError(t, source.MarshalBinaryTo(&encoded))

			decoded := NewOffHeapEmpty()
			require.NoError(t, decoded.SetAllocationAccount(state.selection))
			require.ErrorContains(
				t,
				decoded.UnmarshalFromReaderWithGrouping(&encoded, mp),
				test.want,
			)
			require.Zero(t, state.account.Snapshot().Used)
			decoded.Clean(mp)
		})
	}

	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchGroupingCodecRejectsMismatchedRowCount(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 32)
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(1), false, mp))
	source.SetRowCount(2)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryWithGroupingTo(&encoded))
	decoded := NewOffHeapEmpty()
	require.NoError(t, decoded.SetAllocationAccount(state.selection))
	require.ErrorContains(
		t,
		decoded.UnmarshalFromReaderWithGrouping(&encoded, mp),
		"vector length does not match row count",
	)

	decoded.Clean(mp)
	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAccountedReaderRejectsInvalidLengthsBeforeAllocation(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 16)
	mp := mpool.MustNewZero()
	encode := func(values ...[]byte) []byte {
		return bytes.Join(values, nil)
	}
	zeroRows := int64(0)
	zeroCount := int32(0)
	negative := int32(-1)
	huge := int32(1<<20 + 1)
	one := int32(1)
	oversized := int32(1 << 30)

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "negative vector count",
			data: encode(types.EncodeInt64(&zeroRows), types.EncodeInt32(&negative)),
		},
		{
			name: "huge vector count",
			data: encode(types.EncodeInt64(&zeroRows), types.EncodeInt32(&huge)),
		},
		{
			name: "negative attribute count",
			data: encode(types.EncodeInt64(&zeroRows), types.EncodeInt32(&zeroCount), types.EncodeInt32(&negative)),
		},
		{
			name: "oversized attribute payload",
			data: encode(
				types.EncodeInt64(&zeroRows),
				types.EncodeInt32(&zeroCount),
				types.EncodeInt32(&one),
				types.EncodeInt32(&oversized),
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoded := NewOffHeapEmpty()
			require.NoError(t, decoded.SetAllocationAccount(state.selection))
			require.NotPanics(t, func() {
				require.Error(t, decoded.UnmarshalFromReader(bytes.NewReader(test.data), mp))
			})
			decoded.Clean(mp)
			require.Zero(t, state.account.Snapshot().Used)
		})
	}
	finalizeTestBatchAllocationAccount(t, state)
}

func newMixedBatchAllocationSource(
	t *testing.T,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
	rows int,
) *Batch {
	t.Helper()
	bat := NewOffHeapWithSize(2)
	bat.Attrs = []string{"accounted", "unaccounted"}
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, bat.Vecs[0].SetAllocationAccount(selection))
	bat.Vecs[1] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("unaccounted"), false, mp))
	}
	bat.SetRowCount(rows)
	return bat
}

func TestMixedBatchAllocationClonePreservesVectorProvenance(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 128)
	mp := mpool.MustNewZero()
	source := newMixedBatchAllocationSource(t, mp, state.selection, 8)
	sourceUsed := state.account.Snapshot().Used
	require.NotZero(t, sourceUsed)
	require.Nil(t, source.AllocationAccountSelection())

	_, err := source.Clone(mp, false)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	for _, clone := range []func() (*Batch, error){
		func() (*Batch, error) { return source.Clone(mp, true) },
		func() (*Batch, error) { return source.Dup(mp) },
	} {
		got, err := clone()
		require.NoError(t, err)
		require.Nil(t, got.AllocationAccountSelection())
		require.Same(t, state.selection, got.Vecs[0].AllocationAccountSelection())
		require.Nil(t, got.Vecs[1].AllocationAccountSelection())
		got.Clean(mp)
		require.Equal(t, sourceUsed, state.account.Snapshot().Used)
	}

	accounted, err := source.CloneSelectedColumns([]int{0}, []string{"accounted"}, mp)
	require.NoError(t, err)
	require.Same(t, state.selection, accounted.Vecs[0].AllocationAccountSelection())
	accounted.Clean(mp)
	unaccounted, err := source.CloneSelectedColumns([]int{1}, []string{"unaccounted"}, mp)
	require.NoError(t, err)
	require.Nil(t, unaccounted.Vecs[0].AllocationAccountSelection())
	unaccounted.Clean(mp)

	source.FreeColumns(mp)
	require.Zero(t, state.account.Snapshot().Used)
	require.Same(t, state.selection, source.Vecs[0].AllocationAccountSelection())
	require.Nil(t, source.Vecs[1].AllocationAccountSelection())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(9), false, mp))
	require.NotZero(t, state.account.Snapshot().Used)
	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestMixedBatchAllocationBatchSetPreservesVectorProvenance(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 128)
	mp := mpool.MustNewZero()
	set := NewBatchSet(4)
	first := newMixedBatchAllocationSource(t, mp, state.selection, 2)
	second := newMixedBatchAllocationSource(t, mp, state.selection, 6)

	consumed, err := set.Extend(mp, first, nil)
	require.NoError(t, err)
	require.False(t, consumed)
	consumed, err = set.Extend(mp, second, nil)
	require.NoError(t, err)
	require.False(t, consumed)
	require.Equal(t, 2, set.Length())
	require.Equal(t, 8, set.RowCount())
	for i := 0; i < set.Length(); i++ {
		require.Same(t, state.selection, set.Get(i).Vecs[0].AllocationAccountSelection())
		require.Nil(t, set.Get(i).Vecs[1].AllocationAccountSelection())
	}

	first.Clean(mp)
	second.Clean(mp)
	set.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchSetStartsNewTailWhenVectorProvenanceChanges(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 128)
	mp := mpool.MustNewZero()
	set := NewBatchSet(4)
	unaccounted := newBatchAllocationTestSource(t, mp, nil)
	unaccounted.Shrink([]int64{0, 1}, false)
	mixed := newMixedBatchAllocationSource(t, mp, state.selection, 3)

	_, err := set.Extend(mp, unaccounted, nil)
	require.NoError(t, err)
	ready := set.ReadyCount()
	require.Equal(t, 1, set.ReadyDeltaFor(mixed, mixed.RowCount()))
	_, err = set.Extend(mp, mixed, nil)
	require.NoError(t, err)
	require.Equal(t, 1, set.ReadyCount()-ready)
	require.Equal(t, 2, set.Length())
	require.Equal(t, 2, set.Get(0).RowCount())
	require.Equal(t, 3, set.Get(1).RowCount())
	require.Nil(t, set.Get(0).Vecs[0].AllocationAccountSelection())
	require.Same(t, state.selection, set.Get(1).Vecs[0].AllocationAccountSelection())

	unaccountedUnion := newBatchAllocationTestSource(t, mp, nil)
	ready = set.ReadyCount()
	require.Equal(t, 1, set.ReadyDeltaFor(unaccountedUnion, 1))
	_, err = set.Union(mp, unaccountedUnion, []int32{0}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, set.ReadyCount()-ready)
	require.Equal(t, 3, set.Length())
	require.Nil(t, set.Get(2).Vecs[0].AllocationAccountSelection())

	pushed := newMixedBatchAllocationSource(t, mp, state.selection, 1)
	require.NoError(t, set.Push(mp, pushed))
	require.Equal(t, 4, set.Length())
	require.Same(t, state.selection, set.Get(3).Vecs[0].AllocationAccountSelection())
	require.Equal(t, 1, set.Get(3).RowCount())

	unaccounted.Clean(mp)
	mixed.Clean(mp)
	unaccountedUnion.Clean(mp)
	set.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchSetPreservesUniformBatchAllocationContext(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 256)
	mp := mpool.MustNewZero()
	set := NewBatchSet(16)
	first := newBatchAllocationTestSource(t, mp, state.selection)
	first.Shrink([]int64{0, 1, 2, 3, 4, 5, 6, 7}, false)
	second := newBatchAllocationTestSource(t, mp, state.selection)
	second.Shrink([]int64{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
	}, false)

	_, err := set.Extend(mp, first, nil)
	require.NoError(t, err)
	_, err = set.Extend(mp, second, nil)
	require.NoError(t, err)
	require.Equal(t, 2, set.Length())
	for i := 0; i < set.Length(); i++ {
		require.Same(t, state.selection, set.Get(i).AllocationAccountSelection())
		for _, vec := range set.Get(i).Vecs {
			require.Same(t, state.selection, vec.AllocationAccountSelection())
		}
	}

	reuse := NewWithSchema(
		true,
		first.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
	)
	require.NoError(t, reuse.SetAllocationAccount(state.selection))
	third := newBatchAllocationTestSource(t, mp, state.selection)
	third.Shrink([]int64{0, 1, 2, 3, 4, 5, 6, 7}, false)
	consumed, err := set.Extend(mp, third, reuse)
	require.NoError(t, err)
	require.True(t, consumed)
	require.Equal(t, 3, set.Length())
	require.Same(t, state.selection, set.Get(2).AllocationAccountSelection())

	set.Get(2).FreeColumns(mp)
	require.Same(t, state.selection, set.Get(2).AllocationAccountSelection())
	for _, vec := range set.Get(2).Vecs {
		require.Same(t, state.selection, vec.AllocationAccountSelection())
	}

	first.Clean(mp)
	second.Clean(mp)
	third.Clean(mp)
	set.Clean(mp)
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
