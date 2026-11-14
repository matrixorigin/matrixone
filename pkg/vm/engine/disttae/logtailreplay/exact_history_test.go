// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtailreplay

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// Replay normal insert/delete versions, then transferred deletes for distinct
// older physical rows. Do not insert synthetic entries directly into either tree.
func exactHistoryFixture(t testing.TB, versions int) (*PartitionState, []types.Rowid, []byte, func(types.Rowid, int64, bool)) {
	t.Helper()
	state := NewPartitionState("", false, 42, false)
	pool := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(pool) })
	packer := types.NewPacker()
	t.Cleanup(packer.Close)
	key := bytes.Clone(readutil.EncodePrimaryKey(int64(7), packer))
	segment := objectio.NewSegmentid()
	rowIDs := make([]types.Rowid, versions)
	apply := func(row types.Rowid, ts int64, deleted bool) {
		t.Helper()
		rowVec := vector.NewVec(types.T_Rowid.ToType())
		tsVec := vector.NewVec(types.T_TS.ToType())
		pkVec := vector.NewVec(types.T_int64.ToType())
		defer rowVec.Free(pool)
		defer tsVec.Free(pool)
		defer pkVec.Free(pool)
		require.NoError(t, vector.AppendFixed(rowVec, row, false, pool))
		require.NoError(t, vector.AppendFixed(tsVec, types.BuildTS(ts, 0), false, pool))
		require.NoError(t, vector.AppendFixed(pkVec, int64(7), false, pool))
		input := &api.Batch{Attrs: []string{"rowid", "time", "pk"}, Vecs: []api.Vector{
			mustVectorToProto(rowVec), mustVectorToProto(tsVec), mustVectorToProto(pkVec),
		}}
		if deleted {
			tombstone := vector.NewVec(types.T_Rowid.ToType())
			defer tombstone.Free(pool)
			require.NoError(t, vector.AppendFixed(tombstone, types.RandomRowid(), false, pool))
			input.Vecs = append(input.Vecs, mustVectorToProto(tombstone))
			for i := range input.Vecs {
				input.Vecs[i].Data = bytes.Clone(input.Vecs[i].Data)
			}
			state.HandleRowsDelete(context.Background(), input, packer, pool)
		} else {
			for i := range input.Vecs {
				input.Vecs[i].Data = bytes.Clone(input.Vecs[i].Data)
			}
			state.HandleRowsInsert(context.Background(), input, 0, packer, pool)
		}
	}
	for i := range rowIDs {
		rowIDs[i] = objectio.NewRowid(objectio.NewBlockid(segment, uint16(i), 0), 0)
		if i > 0 {
			apply(rowIDs[i-1], int64(i+1), true)
		}
		apply(rowIDs[i], int64(i+1), false)
	}
	return state, rowIDs, key, apply
}

func TestExactPrimaryKeyTransferredHistory(t *testing.T) {
	for _, deletes := range []int{0, 1, 2, 3, 8} {
		t.Run(fmt.Sprintf("transferred_deletes=%d", deletes), func(t *testing.T) {
			state, rowIDs, key, apply := exactHistoryFixture(t, deletes+1)
			for i := 0; i < deletes; i++ {
				apply(rowIDs[i], int64(deletes+i+2), true)
			}
			check := func(ts int64, want []types.Rowid) {
				t.Helper()
				snapshot := types.BuildTS(ts, 0)
				for _, op := range []int{function.EQUAL, function.PREFIX_EQ, function.IN} {
					iter := state.NewPrimaryKeyIter(snapshot, op, [][]byte{key})
					var got []types.Rowid
					for iter.Next() {
						got = append(got, iter.Entry().RowID)
					}
					require.NoError(t, iter.Close())
					require.Equal(t, want, got, "op=%d snapshot=%d", op, ts)
				}
				iter := state.NewRowsIter(snapshot, nil, false)
				var got []types.Rowid
				for iter.Next() {
					got = append(got, iter.Entry().RowID)
				}
				require.NoError(t, iter.Close())
				require.Equal(t, want, got, "full scan snapshot=%d", ts)
			}
			check(0, nil)
			for i, rowID := range rowIDs {
				check(int64(i+1), []types.Rowid{rowID})
			}
			check(100, rowIDs[len(rowIDs)-1:])
			for i := 0; i < deletes; i++ {
				func() {
					ts := types.BuildTS(100, 0)
					iter := state.NewPrimaryKeyDelIter(&ts, rowIDs[i].BorrowBlockID(), function.EQUAL, [][]byte{key})
					defer iter.Close()
					require.True(t, iter.Next())
					require.Equal(t, rowIDs[i], iter.Entry().RowID)
					require.Equal(t, types.BuildTS(int64(deletes+i+2), 0), iter.Entry().Time)
					require.False(t, iter.Next())
				}()
			}
			// Removing the budget must not resurrect earlier physical versions.
			apply(rowIDs[len(rowIDs)-1], 101, true)
			check(101, nil)
			check(100, rowIDs[len(rowIDs)-1:])
			newRow := types.RandomRowid()
			apply(newRow, 102, false)
			check(102, []types.Rowid{newRow})
		})
	}
}

func BenchmarkExactPrimaryKeyTransferredDeletes(b *testing.B) {
	for _, deletes := range []int{1, 3, 64, 1024} {
		b.Run(fmt.Sprintf("deletes=%d", deletes), func(b *testing.B) {
			state, rows, key, apply := exactHistoryFixture(b, deletes+1)
			for i := 0; i < deletes; i++ {
				apply(rows[i], int64(deletes+i+2), true)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				iter := state.NewPrimaryKeyIter(types.MaxTs(), function.EQUAL, [][]byte{key})
				count := 0
				for iter.Next() {
					count++
				}
				_ = iter.Close()
				if count != 1 {
					b.Fatalf("rows=%d; want 1", count)
				}
			}
		})
	}
}

func TestExactStopsAtKeyBoundary(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		for _, present := range []bool{false, true} {
			t.Run(fmt.Sprintf("deleted=%t/present=%t", deleted, present), func(t *testing.T) {
				state := NewPartitionState("", false, 42, false)
				if present {
					state.rowPrimaryKeyIndex.Set(&PrimaryIndexEntry{Bytes: []byte("b"), Deleted: !deleted})
				}
				state.rowPrimaryKeyIndex.Set(&PrimaryIndexEntry{Bytes: []byte("c"), Deleted: !deleted})
				state.rowPrimaryKeyIndex.Set(&PrimaryIndexEntry{Bytes: []byte("d"), Deleted: deleted})
				iter := state.NewPrimaryKeyIter(types.MaxTs(), function.EQUAL, [][]byte{[]byte("b")})
				defer iter.Close()
				iter.specHint.isDelIter = deleted
				require.False(t, iter.spec.Move(iter))
				// The first unrelated entry terminates even if it belongs to the
				// opposite stream. A missing key must not scan unrelated history.
				require.Equal(t, []byte("c"), iter.iter.Item().Bytes)
			})
		}
	}
}

func TestExactStopsAfterNewestInsertion(t *testing.T) {
	state, rows, key, apply := exactHistoryFixture(t, 8)
	for _, deleted := range []bool{false, true} {
		if deleted {
			apply(rows[len(rows)-1], 9, true)
		}
		iter := state.NewPrimaryKeyIter(types.MaxTs(), function.EQUAL, [][]byte{key})
		defer iter.Close()
		require.Equal(t, !deleted, iter.Next())
		candidate := iter.iter.Item()
		require.Equal(t, rows[len(rows)-1], candidate.RowID)
		require.False(t, candidate.Deleted)
		require.False(t, iter.Next())
		// Even a deleted current insertion cannot resurrect an older version;
		// do not traverse the rest of a hot key's update history to prove this.
		require.Same(t, candidate, iter.iter.Item())
	}
}

func BenchmarkExactPrimaryKeyHistory(b *testing.B) {
	for _, versions := range []int{1, 100, 10000} {
		b.Run(fmt.Sprintf("versions=%d", versions), func(b *testing.B) {
			state, rows, key, apply := exactHistoryFixture(b, versions)
			for _, deleted := range []bool{false, true} {
				if deleted {
					apply(rows[len(rows)-1], int64(versions+1), true)
				}
				b.Run(fmt.Sprintf("deleted=%t", deleted), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						iter := state.NewPrimaryKeyIter(types.MaxTs(), function.EQUAL, [][]byte{key})
						count := 0
						for iter.Next() {
							count++
						}
						_ = iter.Close()
						if (!deleted && count != 1) || (deleted && count != 0) {
							b.Fatalf("deleted=%t rows=%d", deleted, count)
						}
					}
				})
			}
		})
	}
}
