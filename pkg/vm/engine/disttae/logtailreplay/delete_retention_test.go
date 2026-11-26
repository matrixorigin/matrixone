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

package logtailreplay

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

// Delete replay keeps only the primary key, in bytes of its own: neither the
// received batch, which may be a view of the client's reused receive
// buffer, nor its discarded row-id and timestamp columns stay reachable.
func TestReplayDeleteKeepsOnlyOwnedPrimaryKey(t *testing.T) {
	for _, pkType := range []types.T{types.T_int64, types.T_varchar} {
		t.Run(pkType.String(), func(t *testing.T) {
			pool := mpool.MustNewZero()
			packer := types.NewPacker()
			defer packer.Close()
			const rows = 1000
			rowIDVec := vector.NewVec(types.T_Rowid.ToType(), pool)
			tsVec := vector.NewVec(types.T_TS.ToType(), pool)
			pkVec := vector.NewVec(pkType.ToType(), pool)
			tombstoneVec := vector.NewVec(types.T_Rowid.ToType(), pool)
			key := func(i int) []byte { return bytes.Repeat([]byte{byte('a' + i%26)}, 30+i%7) }
			for i := 0; i < rows; i++ {
				require.NoError(t, vector.AppendFixed(rowIDVec, types.RandomRowid(), false, pool))
				require.NoError(t, vector.AppendFixed(tsVec, types.BuildTS(int64(10+i), 0), false, pool))
				require.NoError(t, vector.AppendFixed(tombstoneVec, types.RandomRowid(), false, pool))
				if pkType == types.T_int64 {
					require.NoError(t, vector.AppendFixed(pkVec, int64(i), false, pool))
				} else {
					require.NoError(t, vector.AppendBytes(pkVec, key(i), false, pool))
				}
			}
			input := mustMarshalVectors(rowIDVec, tsVec, pkVec, tombstoneVec)
			for _, vec := range []*vector.Vector{rowIDVec, tsVec, pkVec, tombstoneVec} {
				vec.Free(pool)
			}
			require.Zero(t, pool.CurrNB())

			// Receive the batch as the client does: a view of the wire buffer.
			wire, err := (&api.Entry{EntryType: api.Entry_Delete, TableName: "t", Bat: api.NewEncodedBatch(input)}).Marshal()
			require.NoError(t, err)
			var received api.Entry
			require.NoError(t, received.Unmarshal(wire))
			require.True(t, received.Bat.IsView())
			state := NewPartitionState("", false, 42, false)
			state.HandleLogtailEntry(t.Context(), nil, &received, 0, packer, pool)
			received.Reset()
			clear(wire) // the client reuses the buffer

			iter := state.NewRowsIter(types.BuildTS(int64(10+rows), 0), nil, true)
			defer iter.Close()
			seen := 0
			var kept *ReplayBatch
			for iter.Next() {
				entry := iter.Entry()
				require.True(t, entry.Deleted)
				pk := entry.Batch.Vecs[0]
				if pkType == types.T_int64 {
					require.Equal(t, entry.Offset, vector.GetFixedAtNoTypeCheck[int64](pk, int(entry.Offset)))
				} else {
					require.Equal(t, key(int(entry.Offset)), pk.GetBytesAt(int(entry.Offset)))
				}
				if kept == nil {
					kept = entry.Batch
				}
				require.Same(t, kept, entry.Batch, "the rows share one kept batch")
				seen++
			}
			require.Equal(t, rows, seen)
			require.Len(t, kept.Vecs, 1, "only the primary key is kept")
			require.Equal(t, rows, kept.RowCount())
			data := kept.Vecs[0].GetData()
			require.Less(t, cap(data), len(input)/2,
				"the key's bytes are its own, not a view of the whole batch")
			require.Nil(t, kept.Vecs[0].MPool(), "and owned by the Go heap, like replayed rows")
		})
	}
}

// Insert replay keeps the user columns in one allocation of their own:
// neither the received batch, which may be a view of the client's reused
// receive buffer, nor its row-id and timestamp columns, which RowEntry
// holds, stay reachable from the rows.
func TestReplayInsertKeepsOnlyOwnedUserColumns(t *testing.T) {
	pool := mpool.MustNewZero()
	packer := types.NewPacker()
	defer packer.Close()
	const rows = 1000
	attrs := []string{"rowid", "ts", "pk", "name"}
	rowIDVec := vector.NewVec(types.T_Rowid.ToType(), pool)
	tsVec := vector.NewVec(types.T_TS.ToType(), pool)
	pkVec := vector.NewVec(types.T_int64.ToType(), pool)
	nameVec := vector.NewVec(types.T_varchar.ToType(), pool)
	name := func(i int) []byte { return bytes.Repeat([]byte{byte('a' + i%26)}, 30+i%7) }
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed(rowIDVec, types.RandomRowid(), false, pool))
		require.NoError(t, vector.AppendFixed(tsVec, types.BuildTS(10, 0), false, pool))
		require.NoError(t, vector.AppendFixed(pkVec, int64(i), false, pool))
		require.NoError(t, vector.AppendBytes(nameVec, name(i), i%5 == 0, pool))
	}
	input := mustMarshalVectors(rowIDVec, tsVec, pkVec, nameVec)
	for _, vec := range []*vector.Vector{rowIDVec, tsVec, pkVec, nameVec} {
		vec.Free(pool)
	}
	require.Zero(t, pool.CurrNB())

	checkRows := func(t *testing.T, state *PartitionState) *ReplayBatch {
		iter := state.NewRowsIter(types.BuildTS(11, 0), nil, false)
		defer iter.Close()
		seen := 0
		var kept *ReplayBatch
		for iter.Next() {
			entry := iter.Entry()
			i := int(entry.Offset)
			require.Equal(t, int64(i), vector.GetFixedAtNoTypeCheck[int64](entry.Batch.Vecs[2], i))
			nameCol := entry.Batch.Vecs[3]
			require.Equal(t, i%5 == 0, nameCol.IsNull(uint64(i)))
			if i%5 != 0 {
				require.Equal(t, name(i), nameCol.GetBytesAt(i))
			}
			if kept == nil {
				kept = entry.Batch
			}
			require.Same(t, kept, entry.Batch, "the rows share one kept batch")
			seen++
		}
		require.Equal(t, rows, seen)
		require.Equal(t, attrs, kept.Attrs)
		require.Len(t, kept.Vecs, 4, "column positions are kept")
		require.Zero(t, kept.Vecs[0].Length(), "RowEntry holds the row id")
		require.Zero(t, kept.Vecs[1].Length(), "and the commit time")
		require.Equal(t, types.T_Rowid, kept.Vecs[0].GetType().Oid)
		require.Equal(t, types.T_TS, kept.Vecs[1].GetType().Oid)
		require.Equal(t, rows, kept.RowCount())
		for _, vec := range kept.Vecs[2:] {
			require.Nil(t, vec.MPool(), "owned by the Go heap, like replayed rows")
		}
		return kept
	}

	t.Run("received", func(t *testing.T) {
		// Receive the batch as the client does: a view of the wire buffer.
		wire, err := (&api.Entry{
			EntryType: api.Entry_Insert,
			TableName: "t",
			Attrs:     attrs,
			Bat:       api.NewEncodedBatch(input),
		}).Marshal()
		require.NoError(t, err)
		var received api.Entry
		require.NoError(t, received.Unmarshal(wire))
		require.True(t, received.Bat.IsView())
		state := NewPartitionState("", false, 42, false)
		state.HandleLogtailEntry(t.Context(), nil, &received, 0, packer, pool)
		received.Reset()
		clear(wire) // the client reuses the buffer
		checkRows(t, state)
	})

	t.Run("not a view of the input", func(t *testing.T) {
		in := bytes.Clone(input)
		state := NewPartitionState("", false, 42, false)
		state.HandleRowsInsert(t.Context(), in, attrs, 0, packer, pool)
		kept := checkRows(t, state)
		for _, vec := range kept.Vecs[2:] {
			require.False(t, overlaps(vec.GetData(), in), "a view would pin the row-id and timestamp columns")
			require.False(t, overlaps(vec.GetArea(), in))
		}
	})
}

// overlaps reports whether the backing arrays of a and b share a byte.
func overlaps(a, b []byte) bool {
	if cap(a) == 0 || cap(b) == 0 {
		return false
	}
	a, b = a[:cap(a)], b[:cap(b)]
	aStart := reflect.ValueOf(a).Pointer()
	bStart := reflect.ValueOf(b).Pointer()
	return aStart < bStart+uintptr(len(b)) && bStart < aStart+uintptr(len(a))
}
