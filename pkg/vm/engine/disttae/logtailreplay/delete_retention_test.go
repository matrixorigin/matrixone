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
