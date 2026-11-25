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

package ivfflat_test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

// The actual IVF parser and relation scanner produce the ownership request;
// enumerate real replay rows and persisted file ranges at the same snapshot.
func TestIVFFlushPreservesPartitionOwnership(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	fs := testutil.NewSharedFS()
	defer fs.Close(ctx)
	const pk = "known-workload-pk"
	create, insert, snapshot := types.BuildTS(10, 0), types.BuildTS(20, 0), types.BuildTS(30, 0)
	flushStart, flushCommit := types.BuildTS(35, 0), types.BuildTS(40, 0)

	objID := objectIDOnSecondPartition(t, types.Objectid{1, 2, 3})
	blockID := objectio.NewBlockidWithObjectID(&objID, 0)
	rowID := types.NewRowid(&blockID, 0)
	row := batch.NewWithSize(3)
	row.Attrs = []string{"__mo_rowid", "__mo_commit_ts", "pk"}
	row.Vecs[0] = vector.NewVec(types.T_Rowid.ToType(), mp)
	row.Vecs[1] = vector.NewVec(types.T_TS.ToType(), mp)
	row.Vecs[2] = vector.NewVec(types.T_varchar.ToType(), mp)
	defer row.Clean(mp)
	require.NoError(t, vector.AppendFixed(row.Vecs[0], rowID, false, mp))
	require.NoError(t, vector.AppendFixed(row.Vecs[1], insert, false, mp))
	require.NoError(t, vector.AppendBytes(row.Vecs[2], []byte(pk), false, mp))
	row.SetRowCount(1)
	proto, err := batch.BatchToProtoBatch(row)
	require.NoError(t, err)
	packer := types.NewPacker()
	defer packer.Close()
	before := logtailreplay.NewPartitionState("pre-apply", false, 1234, false)
	before.HandleRowsInsert(ctx, proto, 0, packer, mp)
	decoded := func() *batch.Batch {
		mem := before.NewRowsIter(snapshot, nil, false)
		defer mem.Close()
		require.True(t, mem.Next())
		return mem.Entry().Batch
	}()
	defer decoded.Clean(mp)
	require.Equal(t, pk, decoded.Vecs[2].GetStringAt(0))
	after := before.Copy()

	// Reproduce the old appendable object's persisted representation (same ID,
	// original row commit TS) with a real writer, as flushObjTask does.
	persisted := batch.NewWithSize(2)
	persisted.Attrs = []string{"pk", "__mo_commit_ts"}
	persisted.Vecs[0] = vector.NewVec(types.T_varchar.ToType(), mp)
	persisted.Vecs[1] = vector.NewVec(types.T_TS.ToType(), mp)
	defer persisted.Clean(mp)
	require.NoError(t, vector.AppendBytes(persisted.Vecs[0], []byte(pk), false, mp))
	require.NoError(t, vector.AppendFixed(persisted.Vecs[1], insert, false, mp))
	persisted.SetRowCount(1)
	writeObject := func(id types.Objectid, appendable bool) objectio.ObjectStats {
		name := objectio.BuildObjectNameWithObjectID(&id)
		writer, err := ioutil.NewBlockWriterNew(fs, name, 0, []uint16{0, objectio.SEQNUM_COMMITTS}, false)
		require.NoError(t, err)
		if appendable {
			writer.SetAppendable()
		}
		_, err = writer.WriteBatch(persisted)
		require.NoError(t, err)
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)
		if appendable {
			return writer.GetObjectStats(objectio.WithAppendable())
		}
		return writer.GetObjectStats()
	}
	snapshotStats := writeObject(objID, true)
	require.True(t, snapshotStats.GetAppendable())
	require.Equal(t, objID, *snapshotStats.ObjectName().ObjectId())
	// Replay the real live object-list format consumed from the TN, including
	// the flush transaction start TS used as its in-memory eviction boundary.
	objectList := batch.NewWithSize(2 + len(logtail.ObjectInfoAttr))
	objectList.Attrs = append([]string{"__mo_rowid", "__mo_commit_ts"}, logtail.ObjectInfoAttr...)
	objectList.Vecs[0] = vector.NewVec(types.T_Rowid.ToType(), mp)
	objectList.Vecs[1] = vector.NewVec(types.T_TS.ToType(), mp)
	for i, typ := range logtail.ObjectInfoTypes {
		objectList.Vecs[2+i] = vector.NewVec(typ, mp)
	}
	defer objectList.Clean(mp)
	newID := objectIDOnSecondPartition(t, types.Objectid{9, 8, 7})
	immutableStats := writeObject(newID, false)
	require.False(t, immutableStats.GetAppendable())
	appendObject := func(stats objectio.ObjectStats, createdAt, deletedAt types.TS) {
		require.NoError(t, vector.AppendFixed(objectList.Vecs[0], types.Rowid{}, false, mp))
		require.NoError(t, vector.AppendFixed(objectList.Vecs[1], flushCommit, false, mp))
		require.NoError(t, vector.AppendBytes(objectList.Vecs[2], stats[:], false, mp))
		require.NoError(t, vector.AppendFixed(objectList.Vecs[3], uint64(12), false, mp))
		require.NoError(t, vector.AppendFixed(objectList.Vecs[4], uint64(1234), false, mp))
		for i, ts := range []types.TS{createdAt, deletedAt, flushStart, flushCommit, flushCommit} {
			require.NoError(t, vector.AppendFixed(objectList.Vecs[5+i], ts, false, mp))
		}
	}
	appendObject(snapshotStats, create, flushCommit)
	appendObject(immutableStats, flushCommit, types.TS{})
	objectList.SetRowCount(2)
	objects, err := batch.BatchToProtoBatch(objectList)
	require.NoError(t, err)
	after.HandleDataObjectList(ctx, &api.Entry{EntryType: api.Entry_DataObject, DatabaseId: 12, TableId: 1234, Bat: objects}, fs, mp)
	require.Zero(t, after.ApproxInMemRows(), "applied flush removes committed rows from the replay memtable")
	require.Equal(t, 1, before.ApproxInMemRows(), "other replica still precedes the flush")

	local := ivfflat.CaptureFlushOwnershipRanges(t, 2, 0, false)
	remote := ivfflat.CaptureFlushOwnershipRanges(t, 2, 1, true)
	single := ivfflat.CaptureFlushOwnershipRanges(t, 1, 0, false)
	require.NotZero(t, local.Policy&engine.Policy_CollectCommittedInmemData)
	require.Zero(t, remote.Policy&engine.Policy_CollectCommittedInmemData)
	require.NotZero(t, single.Policy&engine.Policy_CollectCommittedInmemData)
	keys := func(p *logtailreplay.PartitionState, ranges engine.RangesParam, tsRead types.TS, wantObject types.Objectid) []string {
		t.Helper()
		var out []string
		if ranges.Policy&engine.Policy_CollectCommittedInmemData != 0 {
			iter := p.NewRowsIter(tsRead, nil, false)
			defer iter.Close()
			for iter.Next() {
				out = append(out, iter.Entry().Batch.Vecs[2].GetStringAt(0))
			}
		}
		blocks := objectio.PreAllocBlockInfoSlice(2)
		err := readutil.FilterTxnObjects(ctx, ranges, tsRead.ToTimestamp(), nil, nil, nil, nil, nil, p, nil, nil, &blocks, nil, fs, false)
		require.NoError(t, err)
		for i := 0; i < blocks.Len(); i++ {
			loc := blocks.Get(i).MetaLocation()
			require.Equal(t, wantObject, loc.ObjectId(), "physical version must match the retained snapshot")
			cols := containers.NewVectors(2)
			_, release, _, readErr := ioutil.LoadColumnsData(ctx, []uint16{0, objectio.SEQNUM_COMMITTS}, []types.Type{types.T_varchar.ToType(), types.T_TS.ToType()}, fs, loc, cols, mp, 0)
			require.NoError(t, readErr)
			func() {
				defer release()
				commits := vector.MustFixedColWithTypeCheck[types.TS](&cols[1])
				for i, ts := range commits {
					if ts.LE(&tsRead) {
						out = append(out, cols[0].GetStringAt(i))
					}
				}
			}()
		}
		return out
	}

	atRetainedSnapshot := func(p *logtailreplay.PartitionState, ranges engine.RangesParam) []string {
		return keys(p, ranges, snapshot, objID)
	}
	for _, tc := range []struct {
		name          string
		ingress, peer *logtailreplay.PartitionState
	}{
		{"both before", before, before},
		{"ingress before and peer after", before, after},
		{"ingress after and peer before", after, before},
		{"both after", after, after},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, []string{pk}, atRetainedSnapshot(tc.ingress, local))
			require.Empty(t, atRetainedSnapshot(tc.peer, remote))
		})
	}
	require.Equal(t, []string{pk}, atRetainedSnapshot(after, single))
	// This is a different logical snapshot, where only the new immutable
	// object is visible. Its original hash partition must not move.
	newSnapshot := types.BuildTS(50, 0)
	require.Empty(t, keys(after, local, newSnapshot, newID))
	require.Equal(t, []string{pk}, keys(after, remote, newSnapshot, newID))
}

func objectIDOnSecondPartition(t *testing.T, id types.Objectid) types.Objectid {
	t.Helper()
	for plan2.IVFObjectIDHashToRange(id, 2) != 1 {
		id[0]++
		if id[0] == 0 {
			t.Fatal("deterministic fixture could not find a partition-1 object ID")
		}
	}
	return id
}
