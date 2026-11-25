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
		[]string{"id", "value"},
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, mp,
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
	plain, err := source.Clone(mp)
	require.NoError(t, err)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)
	plain.Clean(mp)

	cloned, err := source.Clone(view)
	require.NoError(t, err)
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	cloned.Clean(view)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	dup, err := source.Dup(view)
	require.NoError(t, err)
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	dup.Clean(view)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	selectedColumns, err := source.CloneSelectedColumns(
		[]int{1},
		[]string{"value"},
		view,
	)
	require.NoError(t, err)
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	selectedColumns.Clean(view)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	// A window is a read-only reference: it allocates and charges nothing.
	window, err := source.Window(4, 12)
	require.NoError(t, err)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)
	window.FreeWindow()
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	var encoded bytes.Buffer
	data, err := source.MarshalBinaryWithBuffer(&encoded, true)
	require.NoError(t, err)
	decoded := NewWithSize(0)
	require.NoError(t, decoded.UnmarshalFromReader(bytes.NewReader(data), view))
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	decoded.Clean(view)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	// A receiver owned by another pool is not the decoder's to free.
	foreign := NewWithSchema(
		source.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, mp,
	)
	require.Error(t, foreign.UnmarshalBinaryWithAnyMp(data, view))
	foreign.Clean(mp)

	aliasDecoded := NewWithSchema(
		source.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, view,
	)
	require.NoError(t, aliasDecoded.UnmarshalBinaryWithAnyMp(data, view))
	// Decoding with a pool copies into it.
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	require.NoError(
		t,
		aliasDecoded.UnmarshalFromReader(bytes.NewReader(data), view),
	)
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	require.Equal(t, source.RowCount(), aliasDecoded.RowCount())
	aliasDecoded.Clean(view)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	source.Clean(view)
	require.Zero(t, mp.CurrNB())
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAccountedWindowBroadcastsScalarConstant(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 16)
	mp := mpool.MustNewZero()
	source := NewWithSize(2)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
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
	view := state.view(t, mp)
	source := newBatchAllocationTestSource(t, view)
	sourceUsed := state.account.Snapshot().Used
	require.Positive(t, sourceUsed)

	cloned, err := source.Dup(mp)
	require.NoError(t, err)
	require.Equal(t, source.RowCount(), cloned.RowCount())
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	source.Clean(view)
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
	require.NoError(t, shrinkRows(t, source, []int64{0, 1, 2, 3, 4}, false, mp))

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryTo(&encoded))
	decoded := NewWithSize(0)
	require.NoError(t, decoded.UnmarshalFromReader(&encoded, view))
	require.Equal(t, 5, decoded.RowCount())
	require.Equal(
		t,
		int64(0),
		vector.GetFixedAtWithTypeCheck[int64](decoded.Vecs[0], 0),
	)

	decoded.Clean(view)
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
	decoded := NewWithSize(1)
	decoded.Vecs[0] = vector.NewVec(types.T_int64.ToType(), view)
	decoded.SetRowCount(7)
	require.NoError(t, decoded.UnmarshalFromReader(&encoded, view))
	require.Equal(t, source.RowCount(), decoded.RowCount())
	require.Len(t, decoded.Vecs, 2)
	require.Equal(
		t,
		int64(31),
		vector.GetFixedAtWithTypeCheck[int64](decoded.Vecs[0], 31),
	)

	decoded.Clean(view)
	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchSpillCodecRoundTrip(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	source := newBatchAllocationTestSource(t, mp)
	source.ExtraBuf = bytes.Repeat([]byte("x"), 1<<20)

	var encoded bytes.Buffer
	spillSize, err := source.MarshalBinaryForSpillSize()
	require.NoError(t, err)
	stableSize, err := source.MarshalBinarySize()
	require.NoError(t, err)
	require.Greater(t, stableSize-spillSize, len(source.ExtraBuf)/2)
	require.NoError(t, source.MarshalBinaryForSpillTo(&encoded))
	decoded := NewWithSize(0)
	require.NoError(t, decoded.UnmarshalFromReaderForSpill(&encoded, view))
	require.Empty(t, decoded.Attrs)
	require.Empty(t, decoded.ExtraBuf)
	require.Equal(t, source.RowCount(), decoded.RowCount())
	require.Len(t, decoded.Vecs, len(source.Vecs))
	require.Equal(t,
		vector.MustFixedColWithTypeCheck[int64](source.Vecs[0]),
		vector.MustFixedColWithTypeCheck[int64](decoded.Vecs[0]))
	require.Equal(t, source.Vecs[1].GetBytesAt(31), decoded.Vecs[1].GetBytesAt(31))
	require.Zero(t, encoded.Len(), "spill codec must consume its whole frame")

	// Reusing the decoded batch for a second frame overwrites it in place.
	second := newBatchAllocationTestSource(t, mp)
	require.NoError(t, second.MarshalBinaryForSpillTo(&encoded))
	require.NoError(t, decoded.UnmarshalFromReaderForSpill(&encoded, view))
	require.Equal(t, second.RowCount(), decoded.RowCount())

	decoded.Clean(view)
	second.Clean(mp)
	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchSpillCodecRejectsStableMetadataBeforePayloadAllocation(t *testing.T) {
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

			decoded := NewWithSize(0)
			require.ErrorContains(
				t,
				decoded.UnmarshalFromReaderForSpill(&encoded, view),
				test.want,
			)
			require.Zero(t, state.account.Snapshot().Used)
			decoded.Clean(mp)
		})
	}

	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchSpillCodecRejectsMismatchedRowCount(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 32)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(1), false, mp))
	source.SetRowCount(2)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryForSpillTo(&encoded))
	decoded := NewWithSize(0)
	require.ErrorContains(
		t,
		decoded.UnmarshalFromReaderForSpill(&encoded, view),
		"vector length does not match row count",
	)

	decoded.Clean(view)
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
			decoded := NewWithSize(0)
			require.NotPanics(t, func() {
				require.Error(t, decoded.UnmarshalFromReader(bytes.NewReader(test.data), view))
			})
			decoded.Clean(mp)
			require.Zero(t, state.account.Snapshot().Used)
		})
	}
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchSetChargesCopiesToView(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 256)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	set := NewBatchSet(16)
	first := newBatchAllocationTestSource(t, mp)
	require.NoError(t, shrinkRows(t, first, []int64{0, 1, 2, 3, 4, 5, 6, 7}, false, mp))
	second := newBatchAllocationTestSource(t, mp)
	require.NoError(t, shrinkRows(t, second, []int64{
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

	// A reuse buffer is consumed only when it belongs to the pool the set
	// copies through.
	reuse := NewWithSchema(
		first.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, view,
	)
	third := newBatchAllocationTestSource(t, mp)
	require.NoError(t, shrinkRows(t, third, []int64{0, 1, 2, 3, 4, 5, 6, 7}, false, mp))
	consumed, err := set.Extend(view, third, reuse)
	require.NoError(t, err)
	require.True(t, consumed)
	require.Equal(t, 3, set.Length())

	first.Clean(mp)
	second.Clean(mp)
	third.Clean(mp)
	set.Clean(view)
	require.Zero(t, mp.CurrNB())
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAllocationAccountDestinationCloneUnionAndReuse(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 64)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	source := newBatchAllocationTestSource(t, mp)

	destination := NewWithSchema(
		source.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, view,
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

	destination.FreeColumns(view)
	require.Zero(t, state.account.Snapshot().Used)

	require.NoError(t, destination.Union(source, []int64{1, 3, 5, 7}, view))
	require.Equal(t, 4, destination.RowCount())
	require.NotZero(t, state.account.Snapshot().Used)

	destination.Clean(view)
	source.Clean(mp)
	finalizeTestBatchAllocationAccount(t, state)
}

func TestBatchAllocationAccountCloneRollback(t *testing.T) {
	state := newTestBatchAllocationAccount(t, 1)
	mp := mpool.MustNewZero()
	source := NewWithSchema(
		[]string{"left", "right"},
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, mp,
	)
	for i := range source.Vecs {
		require.NoError(t, vector.AppendFixed(source.Vecs[i], int64(i), false, mp))
	}
	source.SetRowCount(1)

	view := state.view(t, mp)
	destination := NewWithSchema(
		source.Attrs,
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, view,
	)
	err := source.CloneTo(destination, view)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())

	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
	finalizeTestBatchAllocationAccount(t, state)
}
