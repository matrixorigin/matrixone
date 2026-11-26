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

package txnimpl

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func workspaceDeleteFilterFixture(t testing.TB, probes, deletes int) (*txnTable, containers.Vector, containers.Vector) {
	t.Helper()
	keys := containers.MakeVector(objectio.RowidType, common.WorkspaceAllocator)
	t.Cleanup(func() { containers.ReleaseVector(keys) })
	workspace := containers.NewBatch()
	t.Cleanup(workspace.Close)
	deleted := containers.MakeVector(objectio.RowidType, common.WorkspaceAllocator)
	workspace.AddVector(objectio.TombstoneAttr_Rowid_Attr, deleted)
	for i := range probes {
		containers.AppendValue(keys, types.NewRowIDWithObjectIDBlkNumAndRowID(types.Objectid{}, 0, uint32(i)), false)
	}
	// Unsorted IDs with duplicates: workspace order is not a sort contract.
	for i := deletes - 1; i >= 0; i-- {
		var object types.Objectid
		object[0] = byte((i / 4) % 2)
		containers.AppendValue(deleted, types.NewRowIDWithObjectIDBlkNumAndRowID(object, uint16((i/2)%2), uint32(2*(i/2))), false)
	}
	table := &txnTable{tombstoneTable: &baseTable{tableSpace: &tableSpace{node: &anode{data: workspace}}}}
	return table, keys, deleted
}

func TestWorkspaceDeleteFilter(t *testing.T) {
	t.Run("all-null", func(t *testing.T) {
		table, keys, _ := workspaceDeleteFilterFixture(t, 65, 65)
		for i := range keys.Length() {
			containers.UpdateRow(keys, i, nil, true)
		}
		require.NoError(t, table.contains(t.Context(), keys, index.ZM{}))
		require.Equal(t, keys.Length(), containers.NullCount(keys))
	})
	for _, probes := range []int{0, 1, 2, 32, 33, 34, 65} {
		for _, deletes := range []int{0, 1, 2, 32, 33, 34, 65} {
			t.Run(fmt.Sprintf("probes=%d/deletes=%d", probes, deletes), func(t *testing.T) {
				table, keys, workspace := workspaceDeleteFilterFixture(t, probes, deletes)
				original := append([]types.Rowid(nil), vector.MustFixedColWithTypeCheck[types.Rowid](keys)...)
				deleted := append([]types.Rowid(nil), vector.MustFixedColWithTypeCheck[types.Rowid](workspace)...)
				if probes > 3 {
					containers.UpdateRow(keys, 3, nil, true)
				}
				for range 2 { // A repeated filtering pass must be idempotent.
					require.NoError(t, table.contains(t.Context(), keys, index.ZM{}))
					for i, rowid := range original {
						wantNull := i == 3
						for _, removed := range deleted {
							wantNull = wantNull || rowid == removed
						}
						require.Equal(t, wantNull, keys.IsNull(uint64(i)), "candidate %d", i)
						if !wantNull {
							require.Equal(t, rowid, containers.GetValue(keys, i))
						}
					}
					require.Equal(t, deleted, vector.MustFixedColWithTypeCheck[types.Rowid](workspace))
				}
				if probes > 1 {
					// No index may survive a call: a later workspace append is visible.
					containers.AppendValue(workspace, original[1], false)
					require.NoError(t, table.contains(t.Context(), keys, index.ZM{}))
					require.True(t, keys.IsNull(1))
				}
			})
		}
	}
}

func TestFindDeletesPropagatesWorkspaceTombstoneErrors(t *testing.T) {
	ctx := t.Context()
	fs, err := fileservice.NewMemoryFS("workspace-tombstones", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	const capacity = 1 << 20
	pool, err := mpool.NewMPool(t.Name(), capacity, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(pool)
	keys := containers.MakeVector(objectio.RowidType, pool)
	defer containers.ReleaseVector(keys)
	objectID := objectio.NewObjectid()
	rowID := types.NewRowIDWithObjectIDBlkNumAndRowID(objectID, 0, 0)
	containers.AppendValue(keys, rowID, false)

	pkType := types.T_int32.ToType()
	tombstones := catalog.NewCNTombstoneBatchByPKType(pkType, common.WorkspaceAllocator)
	defer tombstones.Close()
	containers.AppendValue(tombstones.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr), rowID, false)
	containers.AppendValue(tombstones.GetVectorByName(objectio.TombstoneAttr_PK_Attr), int32(1), false)
	sink := ioutil.NewTombstoneSinker(objectio.HiddenColumnSelection_None, pkType,
		common.WorkspaceAllocator, fs, ioutil.WithMemorySizeThreshold(1))
	defer func() { require.NoError(t, sink.Close()) }()
	require.NoError(t, sink.Write(ctx, containers.ToCNBatch(tombstones)))
	require.NoError(t, sink.Sync(ctx))
	stats, tail := sink.GetResult()
	require.Len(t, stats, 1)
	require.Empty(t, tail)

	schema := catalog.MockSchemaAll(3, 2)
	table := &txnTable{
		entry: catalog.MockStaloneTableEntry(1, schema),
		store: &txnStore{ctx: ctx, rt: dbutils.NewRuntime(dbutils.WithRuntimeObjectFS(fs))},
		tombstoneTable: &baseTable{
			schema: catalog.GetTombstoneSchema(schema), tableSpace: &tableSpace{stats: stats},
		},
	}
	from, to := types.TS{}, types.MaxTs()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, table.findDeletes(cancelled, keys, from, to, dedupMetricLookup), context.Canceled)
	require.False(t, keys.IsNull(0))

	// Keep the extent and zone map but point to an object that was never written.
	missing := stats[0]
	missingID := objectio.NewObjectid()
	require.NoError(t, objectio.SetObjectStatsObjectName(&missing, objectio.BuildObjectNameWithObjectID(&missingID)))
	table.tombstoneTable.tableSpace.stats = []objectio.ObjectStats{missing}
	err = table.findDeletes(ctx, keys, from, to, dedupMetricLookup)
	var fileErr *moerr.Error
	require.ErrorAs(t, err, &fileErr)
	require.Equal(t, moerr.ErrFileNotFound, fileErr.ErrorCode())
	require.False(t, keys.IsNull(0))

	table.tombstoneTable.tableSpace.stats = stats
	used := pool.CurrNB()
	func() {
		filler, allocErr := pool.Alloc(capacity-int(used), true)
		require.NoError(t, allocErr)
		defer pool.Free(filler)
		err := table.findDeletes(ctx, keys, from, to, dedupMetricLookup)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity), "%v", err)
		require.False(t, keys.IsNull(0))
		require.EqualValues(t, capacity, pool.CurrNB())
	}()
	require.Equal(t, used, pool.CurrNB())
	require.NoError(t, table.findDeletes(ctx, keys, from, to, dedupMetricLookup))
	require.True(t, keys.IsNull(0))
}

func BenchmarkWorkspaceDeleteFilter(b *testing.B) {
	for _, size := range [][2]int{{1, 1}, {16, 16}, {32, 32}, {33, 33}, {128, 128}, {4096, 4096}, {1, 4096}, {4096, 1}, {33, 4096}, {4096, 33}} {
		b.Run(fmt.Sprintf("%dx%d", size[0], size[1]), func(b *testing.B) {
			table, keys, _ := workspaceDeleteFilterFixture(b, size[0], size[1])
			rowids := vector.MustFixedColWithTypeCheck[types.Rowid](keys)
			original := append([]types.Rowid(nil), rowids...)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				copy(rowids, original)
				keys.GetNulls().Reset()
				if err := table.contains(context.Background(), keys, index.ZM{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
