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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func BenchmarkPartitionStateHandleRowsInsert(b *testing.B) {
	pool := mpool.MustNewZero()
	input, attrs := newReplayInsertBatch(b, pool)

	state := NewPartitionState("", false, 42, false)
	packer := types.NewPacker()
	b.Cleanup(packer.Close)
	ctx := context.Background()
	entry := &api.Entry{EntryType: api.Entry_Insert, TableName: "t", Attrs: attrs, Bat: api.NewEncodedBatch(input)}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		state.HandleLogtailEntry(ctx, nil, entry, 0, packer, pool)
	}
}

func BenchmarkPartitionStateHandleRowsDelete(b *testing.B) {
	pool := mpool.MustNewZero()
	input := newReplayDeleteBatch(b, pool)

	state := NewPartitionState("", false, 42, false)
	packer := types.NewPacker()
	b.Cleanup(packer.Close)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		state.HandleRowsDelete(ctx, input, packer, pool)
	}
}

func newReplayInsertBatch(b *testing.B, pool *mpool.MPool) ([]byte, []string) {
	b.Helper()

	rowIDVec := vector.NewVec(types.T_Rowid.ToType(), pool)
	tsVec := vector.NewVec(types.T_TS.ToType(), pool)
	pkVec := vector.NewVec(types.T_int64.ToType(), pool)

	segmentID := objectio.NewSegmentid()
	blockID := objectio.NewBlockid(segmentID, 0, 0)
	vector.AppendFixed(rowIDVec, objectio.NewRowid(blockID, 0), false, pool)
	vector.AppendFixed(tsVec, types.BuildTS(1, 0), false, pool)
	vector.AppendFixed(pkVec, int64(1), false, pool)

	input := mustMarshalVectors(rowIDVec, tsVec, pkVec)
	rowIDVec.Free(pool)
	tsVec.Free(pool)
	pkVec.Free(pool)
	return input, []string{"rowid", "time", "pk"}
}

func newReplayDeleteBatch(b *testing.B, pool *mpool.MPool) []byte {
	b.Helper()

	rowIDVec := vector.NewVec(types.T_Rowid.ToType(), pool)
	tsVec := vector.NewVec(types.T_TS.ToType(), pool)
	pkVec := vector.NewVec(types.T_int64.ToType(), pool)
	tombstoneRowIDVec := vector.NewVec(types.T_Rowid.ToType(), pool)

	segmentID := objectio.NewSegmentid()
	blockID := objectio.NewBlockid(segmentID, 0, 0)
	vector.AppendFixed(rowIDVec, objectio.NewRowid(blockID, 0), false, pool)
	vector.AppendFixed(tsVec, types.BuildTS(2, 0), false, pool)
	vector.AppendFixed(pkVec, int64(1), false, pool)
	vector.AppendFixed(tombstoneRowIDVec, types.RandomRowid(), false, pool)

	input := mustMarshalVectors(rowIDVec, tsVec, pkVec, tombstoneRowIDVec)
	rowIDVec.Free(pool)
	tsVec.Free(pool)
	pkVec.Free(pool)
	tombstoneRowIDVec.Free(pool)
	return input
}

// Populate through the actual live delete handler, then replay an earlier
// checkpoint/live object records while newer, unrelated tombstones are in memory.
func BenchmarkTombstoneObjectReplayWithUnrelatedDeletes(b *testing.B) {
	for _, count := range []int{0, 10000, 100000} {
		for _, mode := range []struct {
			name                      string
			appendable, deleted, live bool
		}{
			{"checkpoint-creation-nonappendable", false, false, false},
			{"checkpoint-creation-appendable", true, false, false},
			{"checkpoint-deletion", true, true, false},
			{"live-deletion", true, true, true},
		} {
			b.Run(fmt.Sprintf("rows=%d/%s", count, mode.name), func(b *testing.B) {
				ctx := context.Background()
				state := NewPartitionState("", false, 42, false)
				pool := mpool.MustNewZero()
				packer := types.NewPacker()
				defer packer.Close()
				vecs := []*vector.Vector{
					vector.NewVec(types.T_Rowid.ToType(), pool), vector.NewVec(types.T_TS.ToType(), pool),
					vector.NewVec(types.T_int64.ToType(), pool), vector.NewVec(types.T_Rowid.ToType(), pool),
				}
				defer func() {
					for _, v := range vecs {
						v.Free(pool)
					}
				}()
				var rowObject, tombObject, checkpointObject types.Objectid
				rowObject[0], tombObject[0], checkpointObject[0] = 2, 3, 1
				for i := 0; i < count; i++ {
					rowBlock := types.NewBlockidWithObjectID(&rowObject, uint16(i/8192))
					tombObject[17] = byte(i / 8192)
					tombBlock := types.NewBlockidWithObjectID(&tombObject, 0)
					require.NoError(b, vector.AppendFixed(vecs[0], types.NewRowid(&rowBlock, uint32(i%8192)), false, pool))
					require.NoError(b, vector.AppendFixed(vecs[1], types.BuildTS(100, 0), false, pool))
					require.NoError(b, vector.AppendFixed(vecs[2], int64(i), false, pool))
					require.NoError(b, vector.AppendFixed(vecs[3], types.NewRowid(&tombBlock, uint32(i%8192)), false, pool))
				}
				if count > 0 {
					state.HandleRowsDelete(ctx, mustMarshalVectors(vecs...), packer, pool)
				}
				require.Equal(b, count, state.inMemTombstoneRowIdIndex.Len())
				stats := objectio.NewObjectStatsWithObjectID(&checkpointObject, mode.appendable, false, false)
				require.NoError(b, objectio.SetObjectStatsSize(stats, 1000))
				require.NoError(b, objectio.SetObjectStatsBlkCnt(stats, 1))
				require.NoError(b, objectio.SetObjectStatsRowCnt(stats, 1))
				entry := objectio.ObjectEntry{ObjectStats: *stats, CreateTime: types.BuildTS(1, 0)}
				if mode.deleted {
					entry.DeleteTime = types.BuildTS(2, 0)
				}
				var live []byte
				if mode.live {
					live = newReplayObjectListBatch(b, pool, entry, entry.DeleteTime)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if mode.deleted {
						// Reset only the metadata guard outside timing; there are no matching rows.
						// Repeated deleted-object replay would otherwise bypass the scan under test.
						b.StopTimer()
						state.tombstoneObjectsNameIndex.Delete(entry)
						b.StartTimer()
					}
					if mode.live {
						state.HandleTombstoneObjectList(ctx, &api.Entry{Bat: api.NewEncodedBatch(live)}, nil, pool)
					} else {
						if err := state.HandleObjectEntry(ctx, nil, entry, true); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				require.Equal(b, count, state.inMemTombstoneRowIdIndex.Len(), "unrelated deletes must survive")
				require.Equal(b, 1, state.tombstoneObjectsNameIndex.Len(), "object metadata is registered exactly once")
			})
		}
	}
}
