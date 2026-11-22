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

// newBatchAllocationTestSource builds a two-column batch whose vectors
// allocate through mp (an accounted view charges them).
func newBatchAllocationTestSource(
	t *testing.T,
	mp *mpool.MPool,
) *Batch {
	t.Helper()
	bat := NewWithSchema(
		true,
		[]string{"id", "value"},
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
	)
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

func TestBatchAccountedExtraBufferMoveAndClean(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 8)
	mp := mpool.MustNewZero()
	var nilBatch *Batch
	require.ErrorIs(t, nilBatch.SetAccountedExtraBuffer(nil), mpool.ErrAllocationAccountInvalid)
	nilBatch.MoveExtraBufferFrom(nil)
	empty := NewWithSize(0)
	require.ErrorIs(t, empty.SetAccountedExtraBuffer(nil), mpool.ErrAllocationAccountInvalid)
	empty.MoveExtraBufferFrom(empty)
	empty.Clean(mp)
	buffer, err := mpool.NewAccountedBuffer(mp, state.account, 1, 5)
	require.NoError(t, err)
	_, err = buffer.Write(bytes.Repeat([]byte("x"), 4096))
	require.NoError(t, err)
	used := state.account.Snapshot().Used
	require.Positive(t, used)

	source := NewWithSize(0)
	require.NoError(t, source.SetAccountedExtraBuffer(buffer))
	buffer.Free() // ownership moved to source
	require.Equal(t, used, state.account.Snapshot().Used)

	destination := NewWithSize(0)
	destination.MoveExtraBufferFrom(source)
	require.Empty(t, source.ExtraBuf)
	require.Len(t, destination.ExtraBuf, 4096)
	source.Clean(mp)
	require.Equal(t, used, state.account.Snapshot().Used)
	destination.Clean(mp)
	require.Zero(t, state.account.Snapshot().Used)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAllocationAccountCloneDupAndWindow(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	source := newBatchAllocationTestSource(t, view)
	sourceUsed := state.account.Snapshot().Used
	require.NotZero(t, sourceUsed)

	// A plain pool copy is unaccounted.
	plain, err := source.Clone(mp, false)
	require.NoError(t, err)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)
	plain.Clean(mp)

	// A view copy is off-heap and charged, whatever offHeap says.
	cloned, err := source.Clone(view, false)
	require.NoError(t, err)
	require.True(t, cloned.offHeap)
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	cloned.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	dup, err := source.Dup(view)
	require.NoError(t, err)
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	dup.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	selectedColumns, err := source.CloneSelectedColumns(
		[]int{1},
		[]string{"value"},
		view,
	)
	require.NoError(t, err)
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	selectedColumns.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	window, err := source.Window(4, 12)
	require.NoError(t, err)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)
	window.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	var encoded bytes.Buffer
	data, err := source.MarshalBinaryWithBuffer(&encoded, true)
	require.NoError(t, err)
	decoded := NewOffHeapEmpty()
	require.NoError(t, decoded.UnmarshalFromReader(bytes.NewReader(data), view))
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	decoded.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	aliasDecoded := NewWithSchema(
		true,
		source.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
	)
	require.NoError(t, aliasDecoded.UnmarshalBinaryWithAnyMp(data, view))
	// Alias decoding allocates nothing.
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)
	require.NoError(
		t,
		aliasDecoded.UnmarshalFromReader(bytes.NewReader(data), view),
	)
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	require.Equal(t, source.RowCount(), aliasDecoded.RowCount())
	aliasDecoded.Clean(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAccountedWindowBroadcastsScalarConstant(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 16)
	mp := mpool.MustNewZero()
	source := NewWithSize(2)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		source.Vecs[0], []int64{10, 20, 30, 40}, nil, mp,
	))
	var err error
	source.Vecs[1], err = vector.NewConstBytes(
		types.T_varchar.ToType(), []byte("prepared"), 1, mp,
	)
	require.NoError(t, err)
	source.SetRowCount(4)

	window, err := source.Window(2, 4)
	require.NoError(t, err)
	require.Equal(t, 2, window.RowCount())
	require.Equal(t, 2, window.Vecs[1].Length())
	require.Equal(t, []byte("prepared"), window.Vecs[1].GetBytesAt(1))
	window.Clean(mp)
	require.Zero(t, state.account.Snapshot().Used)

	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
	finalizeTestBatchAllocationAccount(t, state)
}

// A copy through the base pool is unaccounted and outlives the account.
func TestBatchDupThroughBasePoolCrossesStatementBoundary(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	source := newBatchAllocationTestSource(t, state.view(t, mp))
	sourceUsed := state.account.Snapshot().Used
	require.Positive(t, sourceUsed)

	cloned, err := source.Dup(mp)
	require.NoError(t, err)
	require.Equal(t, source.RowCount(), cloned.RowCount())
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
	require.Equal(
		t,
		int64(0),
		vector.GetFixedAtNoTypeCheck[int64](cloned.Vecs[0], 0),
	)
	require.Equal(
		t,
		[]byte("batch allocation payload that is not inline"),
		cloned.Vecs[1].GetBytesAt(0),
	)
	cloned.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestBatchAccountedReaderAcceptsBitmapCapacityBeyondLogicalRows(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	source := newBatchAllocationTestSource(t, mp)
	source.Vecs[0].GetNulls().Add(31)
	require.NoError(t, source.Shrink([]int64{0, 1, 2, 3, 4}, false, mp))

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryTo(&encoded))
	decoded := NewOffHeapEmpty()
	require.NoError(t, decoded.UnmarshalFromReader(&encoded, view))
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
	view := state.view(t, mp)
	source := newBatchAllocationTestSource(t, mp)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryTo(&encoded))
	decoded := NewOffHeapWithSize(1)
	decoded.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	decoded.SetRowCount(7)
	require.NoError(t, decoded.UnmarshalFromReader(&encoded, view))
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
	view := state.view(t, mp)
	source := newBatchAllocationTestSource(t, mp)
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
	require.NoError(t, decoded.UnmarshalFromReaderWithGrouping(&encoded, view))
	require.Empty(t, decoded.Attrs)
	require.Empty(t, decoded.ExtraBuf)
	for i := range source.Vecs {
		require.True(t, decoded.Vecs[i].GetGrouping().IsSame(source.Vecs[i].GetGrouping()))
	}
	withoutGrouping := newBatchAllocationTestSource(t, mp)
	encoded.Reset()
	require.NoError(t, withoutGrouping.MarshalBinaryWithGroupingTo(&encoded))
	require.NoError(t, decoded.UnmarshalFromReaderWithGrouping(&encoded, view))
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
	view := state.view(t, mp)

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
			var payload bytes.Buffer
			require.NoError(t, source.MarshalBinaryTo(&payload))
			var encoded bytes.Buffer
			payloadSize := int64(payload.Len())
			encoded.Write(types.EncodeInt64(&payloadSize))
			encoded.Write(payload.Bytes())

			decoded := NewOffHeapEmpty()
			require.ErrorContains(
				t,
				decoded.UnmarshalFromReaderWithGrouping(&encoded, view),
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
	view := state.view(t, mp)
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(1), false, mp))
	source.SetRowCount(2)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryWithGroupingTo(&encoded))
	decoded := NewOffHeapEmpty()
	require.ErrorContains(
		t,
		decoded.UnmarshalFromReaderWithGrouping(&encoded, view),
		"vector length does not match row count",
	)

	decoded.Clean(mp)
	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAccountedReaderRejectsInvalidLengthsBeforeAllocation(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 16)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
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
			require.NotPanics(t, func() {
				require.Error(t, decoded.UnmarshalFromReader(bytes.NewReader(test.data), view))
			})
			decoded.Clean(mp)
			require.Zero(t, state.account.Snapshot().Used)
		})
	}
	finalizeTestBatchAllocationAccount(t, state)
}

// newMixedBatchAllocationSource charges column 0 through accounted and
// leaves column 1 on the base pool mp.
func newMixedBatchAllocationSource(
	t *testing.T,
	mp *mpool.MPool,
	accounted *mpool.MPool,
	rows int,
) *Batch {
	t.Helper()
	bat := NewOffHeapWithSize(2)
	bat.Attrs = []string{"accounted", "unaccounted"}
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), false, accounted))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("unaccounted"), false, mp))
	}
	bat.SetRowCount(rows)
	return bat
}

func TestBatchViewClonesPromoteOffHeap(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 16)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	source := NewWithSize(2)
	source.Attrs = []string{"a", "b"}
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	source.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(source.Vecs[1], int64(2), false, mp))
	source.SetRowCount(1)
	require.Zero(t, state.account.Snapshot().Used)

	dup, err := source.Dup(view)
	require.NoError(t, err)
	require.True(t, dup.offHeap)
	require.Positive(t, state.account.Snapshot().Used)
	dup.Clean(mp)
	require.Zero(t, state.account.Snapshot().Used)

	selected, err := source.CloneSelectedColumns(
		[]int{0},
		[]string{"a"},
		view,
	)
	require.NoError(t, err)
	require.True(t, selected.offHeap)
	require.Positive(t, state.account.Snapshot().Used)
	selected.Clean(mp)
	require.Zero(t, state.account.Snapshot().Used)

	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

// Batches carry no allocation account, so a BatchSet coalesces chunks from
// any producer into one tail.
func TestBatchSetFillsTailAcrossProducers(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 128)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	set := NewBatchSet(4)
	first := newMixedBatchAllocationSource(t, mp, view, 2)
	second := newMixedBatchAllocationSource(t, mp, mp, 2)

	_, err := set.Extend(view, first, nil)
	require.NoError(t, err)
	require.Equal(t, 1, set.ReadyDeltaFor(second, second.RowCount()))
	_, err = set.Extend(view, second, nil)
	require.NoError(t, err)
	require.Equal(t, 1, set.Length())
	require.Equal(t, 1, set.ReadyCount())
	require.Equal(t, 4, set.Get(0).RowCount())

	first.Clean(mp)
	second.Clean(mp)
	set.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchSetCoalescesEquivalentParallelProducerChunks(t *testing.T) {
	const (
		batchMaxRows = 8192
		chunkRows    = 72
		chunkCount   = 114
	)
	state := newTestBatchAllocationAccount(t, 256)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	set := NewBatchSet(batchMaxRows)

	for range chunkCount {
		source := newMixedBatchAllocationSource(t, mp, view, chunkRows)
		_, err := set.Extend(view, source, nil)
		require.NoError(t, err)
		source.Clean(mp)
	}

	require.Equal(t, 2, set.Length())
	require.Equal(t, 1, set.ReadyCount())
	require.Equal(t, batchMaxRows, set.Get(0).RowCount())
	require.Equal(t, chunkRows*chunkCount-batchMaxRows, set.Get(1).RowCount())

	set.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchSetChargesCopiesToView(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 256)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	set := NewBatchSet(16)
	first := newBatchAllocationTestSource(t, mp)
	require.NoError(t, first.Shrink([]int64{0, 1, 2, 3, 4, 5, 6, 7}, false, mp))
	second := newBatchAllocationTestSource(t, mp)
	require.NoError(t, second.Shrink([]int64{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
	}, false, mp))

	_, err := set.Extend(view, first, nil)
	require.NoError(t, err)
	require.Positive(t, state.account.Snapshot().Used)
	_, err = set.Extend(view, second, nil)
	require.NoError(t, err)
	require.Equal(t, 2, set.Length())
	require.Equal(t, 28, set.RowCount())

	reuse := NewWithSchema(
		true,
		first.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
	)
	third := newBatchAllocationTestSource(t, mp)
	require.NoError(t, third.Shrink([]int64{0, 1, 2, 3, 4, 5, 6, 7}, false, mp))
	consumed, err := set.Extend(view, third, reuse)
	require.NoError(t, err)
	require.True(t, consumed)
	require.Equal(t, 3, set.Length())

	first.Clean(mp)
	second.Clean(mp)
	third.Clean(mp)
	set.Clean(mp)
	require.Zero(t, mp.CurrNB())
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAllocationAccountDestinationCloneUnionAndReuse(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	source := newBatchAllocationTestSource(t, mp)

	destination := NewWithSchema(
		true,
		source.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
	)
	require.NoError(t, source.CloneTo(destination, view))
	require.NotZero(t, state.account.Snapshot().Used)
	require.Equal(t, source.RowCount(), destination.RowCount())
	for i := 0; i < source.RowCount(); i++ {
		require.Equal(t, source.Vecs[1].GetStringAt(i), destination.Vecs[1].GetStringAt(i))
	}
	// A clone must not stage a second copy that would temporarily raise the
	// account high-water mark above its final live usage.
	snapshot := state.account.Snapshot()
	require.Equal(t, snapshot.Used, snapshot.Peak)

	destination.FreeColumns(mp)
	require.Zero(t, state.account.Snapshot().Used)

	require.NoError(t, destination.Union(source, []int64{1, 3, 5, 7}, view))
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
	err := source.CloneTo(destination, state.view(t, mp))
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())

	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
	finalizeTestBatchAllocationAccount(t, state)
}
