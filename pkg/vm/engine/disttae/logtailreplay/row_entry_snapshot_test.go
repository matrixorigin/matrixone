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

package logtailreplay

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func TestPartitionStateRowEntrySnapshotIsolation(t *testing.T) {
	for _, noData := range []bool{false, true} {
		for _, tc := range []struct {
			name          string
			initialDelete bool
			replayDelete  bool
		}{
			{"repeat_insert", false, false},
			{"repeat_delete", true, true},
			{"insert_then_delete", false, true},
			{"delete_then_insert", true, false},
		} {
			t.Run(fmt.Sprintf("%s/noData=%t", tc.name, noData), func(t *testing.T) {
				pool := mpool.MustNewZero()
				t.Cleanup(func() { require.Zero(t, pool.CurrNB()) })
				packer := types.NewPacker()
				t.Cleanup(packer.Close)
				ts := types.BuildTS(10, 0)
				blockID := types.Blockid{1}
				rows := []types.Rowid{
					objectio.NewRowid(&blockID, 0),
					objectio.NewRowid(&blockID, 1),
				}
				initial := snapshotReplayBatch(t, pool, rows, ts, tc.initialDelete, []int{0, 1})
				// The same physical versions and values arrive in a different batch order.
				replay := snapshotReplayBatch(t, pool, rows, ts, tc.replayDelete, []int{1, 0})
				apply := func(state *PartitionState, input *api.Batch, deleted bool) {
					if deleted {
						state.HandleRowsDelete(context.Background(), input, packer, pool)
					} else {
						state.HandleRowsInsert(context.Background(), input, 0, packer, pool)
					}
				}

				partition := NewPartition("", nil, 0, 0, 42, nil)
				state, publish := partition.MutateState()
				state.noData = noData
				apply(state, initial, tc.initialDelete)
				publish()
				old := partition.Snapshot()
				pivot := &RowEntry{BlockID: blockID, RowID: rows[0], Time: ts}
				before, ok := old.rows.Get(pivot)
				require.True(t, ok)
				original := *before
				next, publish := partition.MutateState()

				// A snapshot reader does not take the partition writer lock. Its
				// accesses must remain safe while replay updates the private copy.
				start := make(chan struct{})
				done := make(chan []RowEntry, 1)
				go func() {
					<-start
					observed := make([]RowEntry, 16)
					for i := range observed {
						observed[i] = *before
					}
					done <- observed
				}()
				close(start)
				apply(next, replay, tc.replayDelete)
				observed := <-done
				for _, entry := range observed {
					require.Equal(t, original, entry)
				}

				// Check before publication as well as after it: discarding an
				// unpublished mutation must not leave changes in the old snapshot.
				require.Same(t, old, partition.Snapshot())
				require.Equal(t, original, *before)
				current, ok := next.rows.Get(pivot)
				require.True(t, ok)
				require.NotSame(t, before, current)
				require.Equal(t, original.ID, current.ID)
				require.Equal(t, original.PrimaryIndexBytes, current.PrimaryIndexBytes)
				require.Equal(t, tc.initialDelete || tc.replayDelete, current.Deleted)
				if noData {
					require.Nil(t, current.Batch)
				} else {
					require.Same(t, original.Batch, before.Batch)
					require.NotSame(t, before.Batch, current.Batch)
					require.Equal(t, int64(0), before.Offset)
					require.Equal(t, int64(1), current.Offset)
				}
				require.Equal(t, 2, next.rows.Len())
				require.Equal(t, 2, next.rowPrimaryKeyIndex.Len())
				publish()
				require.Same(t, next, partition.Snapshot())
				require.Equal(t, original, *before)

				// Exercise the real scan and primary-key readers, not only pointer identity.
				checkRows := func(s *PartitionState, deleted bool) {
					iter := s.NewRowsIter(ts, nil, deleted)
					defer iter.Close()
					for i := range rows {
						require.True(t, iter.Next())
						entry := iter.Entry()
						require.Equal(t, rows[i], entry.RowID)
						if !noData {
							values := vector.MustFixedColWithTypeCheck[int64](entry.Batch.Vecs[2])
							require.Equal(t, int64(i+1), values[entry.Offset])
						}
					}
					require.False(t, iter.Next())
				}
				checkRows(old, tc.initialDelete)
				checkRows(next, tc.initialDelete || tc.replayDelete)
				checkPK := func(s *PartitionState, visible bool) {
					iter := s.NewPrimaryKeyIter(ts, function.EQUAL, [][]byte{original.PrimaryIndexBytes})
					defer iter.Close()
					require.Equal(t, visible, iter.Next())
					if visible {
						require.Equal(t, rows[0], iter.Entry().RowID)
						require.Equal(t, original.ID, iter.Entry().ID)
					}
					require.False(t, iter.Next())
				}
				checkPK(old, !tc.initialDelete)
				checkPK(next, !(tc.initialDelete || tc.replayDelete))
			})
		}
	}
}

func snapshotReplayBatch(t *testing.T, pool *mpool.MPool, rows []types.Rowid, ts types.TS, deleted bool, order []int) *api.Batch {
	t.Helper()
	vecs := []*vector.Vector{
		vector.NewVec(types.T_Rowid.ToType()),
		vector.NewVec(types.T_TS.ToType()),
		vector.NewVec(types.T_int64.ToType()),
	}
	attrs := []string{"rowid", "time", "pk"}
	if deleted {
		vecs = append(vecs, vector.NewVec(types.T_Rowid.ToType()))
		attrs = append(attrs, "tombstone_rowid")
	}
	t.Cleanup(func() {
		for _, vec := range vecs {
			vec.Free(pool)
		}
	})
	tombstoneBlock := types.Blockid{2}
	for _, i := range order {
		require.NoError(t, vector.AppendFixed(vecs[0], rows[i], false, pool))
		require.NoError(t, vector.AppendFixed(vecs[1], ts, false, pool))
		require.NoError(t, vector.AppendFixed(vecs[2], int64(i+1), false, pool))
		if deleted {
			require.NoError(t, vector.AppendFixed(vecs[3], objectio.NewRowid(&tombstoneBlock, uint32(i)), false, pool))
		}
	}
	input := &api.Batch{Attrs: attrs}
	for _, vec := range vecs {
		input.Vecs = append(input.Vecs, mustVectorToProto(vec))
	}
	return input
}
