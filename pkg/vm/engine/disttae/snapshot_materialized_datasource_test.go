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

package disttae

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestMaterializedSnapshotDataSourceUsesSnapshotTSForInMemRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	pState := newTestMaterializedSnapshotPartitionState(t, mp, []testMaterializedSnapshotRow{
		{rowID: buildTestMaterializedSnapshotRowID(t, 1), ts: types.BuildTS(15, 0), id: 1, payload: 100},
		{rowID: buildTestMaterializedSnapshotRowID(t, 2), ts: types.BuildTS(25, 0), id: 2, payload: 200},
	})

	source := newMaterializedSnapshotDataSource(
		context.Background(),
		nil,
		pState,
		types.BuildTS(30, 0),
		types.BuildTS(20, 0),
		nil,
		nil,
	)

	got := collectMaterializedSnapshotInt64Column(
		t,
		source,
		[]string{"payload"},
		[]types.Type{types.T_int64.ToType()},
		[]uint16{1},
		nil,
		mp,
	)
	require.Equal(t, []int64{100}, got)
}

func TestMaterializedSnapshotDataSourceUsesSnapshotTSForPrimaryKeyFilter(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	pState := newTestMaterializedSnapshotPartitionState(t, mp, []testMaterializedSnapshotRow{
		{rowID: buildTestMaterializedSnapshotRowID(t, 1), ts: types.BuildTS(15, 0), id: 1, payload: 100},
		{rowID: buildTestMaterializedSnapshotRowID(t, 2), ts: types.BuildTS(25, 0), id: 1, payload: 200},
	})

	var filter readutil.MemPKFilter
	packer := types.NewPacker()
	filter.SetFullData(function.EQUAL, false, readutil.EncodePrimaryKey(int64(1), packer))
	packer.Close()

	source := newMaterializedSnapshotDataSource(
		context.Background(),
		nil,
		pState,
		types.BuildTS(30, 0),
		types.BuildTS(20, 0),
		nil,
		nil,
	)

	got := collectMaterializedSnapshotInt64Column(
		t,
		source,
		[]string{"payload"},
		[]types.Type{types.T_int64.ToType()},
		[]uint16{1},
		&filter,
		mp,
	)
	require.Equal(t, []int64{100}, got)
}

func TestMaterializedSnapshotDataSourceUsesSnapshotTSForPersistedTombstones(t *testing.T) {
	snapshotTS := types.BuildTS(20, 0)
	tombstoner := &recordingTombstoner{}
	source := newMaterializedSnapshotDataSource(
		context.Background(),
		nil,
		nil,
		types.BuildTS(30, 0),
		snapshotTS,
		nil,
		tombstoner,
	)
	ds := source.(*materializedSnapshotDataSource)

	rowID := buildTestMaterializedSnapshotRowID(t, 9)
	bid := rowID.BorrowBlockID()

	left, err := ds.ApplyTombstones(context.Background(), bid, []int64{1, 2}, engine.Policy_CheckAll)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, left)

	bm, err := ds.GetTombstones(context.Background(), bid)
	require.NoError(t, err)
	bm.Release()

	deleted, err := ds.isDeletedByCommittedTombstones(context.Background(), rowID)
	require.NoError(t, err)
	require.False(t, deleted)

	require.Equal(t, []types.TS{snapshotTS, snapshotTS, snapshotTS}, tombstoner.snapshots)
}

type testMaterializedSnapshotRow struct {
	rowID   types.Rowid
	ts      types.TS
	id      int64
	payload int64
}

func newTestMaterializedSnapshotPartitionState(
	t *testing.T,
	mp *mpool.MPool,
	rows []testMaterializedSnapshotRow,
) *logtailreplay.PartitionState {
	t.Helper()

	pState := logtailreplay.NewPartitionState("", false, 42, false)
	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())
	idVec := vector.NewVec(types.T_int64.ToType())
	payloadVec := vector.NewVec(types.T_int64.ToType())
	defer rowIDVec.Free(mp)
	defer tsVec.Free(mp)
	defer idVec.Free(mp)
	defer payloadVec.Free(mp)

	for _, row := range rows {
		require.NoError(t, vector.AppendFixed(rowIDVec, row.rowID, false, mp))
		require.NoError(t, vector.AppendFixed(tsVec, row.ts, false, mp))
		require.NoError(t, vector.AppendFixed(idVec, row.id, false, mp))
		require.NoError(t, vector.AppendFixed(payloadVec, row.payload, false, mp))
	}

	packer := types.NewPacker()
	defer packer.Close()
	pState.HandleRowsInsert(context.Background(), &api.Batch{
		Attrs: []string{"rowid", "time", "id", "payload"},
		Vecs: []api.Vector{
			mustVectorToProtoForMaterializedSnapshotTest(t, rowIDVec),
			mustVectorToProtoForMaterializedSnapshotTest(t, tsVec),
			mustVectorToProtoForMaterializedSnapshotTest(t, idVec),
			mustVectorToProtoForMaterializedSnapshotTest(t, payloadVec),
		},
	}, 0, packer, mp)

	return pState
}

func collectMaterializedSnapshotInt64Column(
	t *testing.T,
	source engine.DataSource,
	attrs []string,
	colTypes []types.Type,
	seqNums []uint16,
	filter any,
	mp *mpool.MPool,
) []int64 {
	t.Helper()
	defer source.Close()

	out := batch.NewWithSize(len(attrs))
	out.SetAttributes(attrs)
	for i := range attrs {
		out.Vecs[i] = vector.NewVec(colTypes[i])
	}
	defer out.Clean(mp)

	var ret []int64
	for {
		_, state, err := source.Next(
			context.Background(),
			attrs,
			colTypes,
			seqNums,
			0,
			filter,
			mp,
			out,
		)
		require.NoError(t, err)
		if state == engine.End {
			return ret
		}
		ret = append(ret, vector.MustFixedColWithTypeCheck[int64](out.Vecs[0])...)
	}
}

func buildTestMaterializedSnapshotRowID(t *testing.T, rowIdx uint32) types.Rowid {
	t.Helper()
	uid, err := types.BuildUuid()
	require.NoError(t, err)
	blkID := objectio.NewBlockid(&uid, 0, 1)
	return types.NewRowid(blkID, rowIdx)
}

func mustVectorToProtoForMaterializedSnapshotTest(t *testing.T, vec *vector.Vector) api.Vector {
	t.Helper()
	ret, err := vector.VectorToProtoVector(vec)
	require.NoError(t, err)
	return ret
}

type recordingTombstoner struct {
	snapshots []types.TS
}

func (r *recordingTombstoner) Type() engine.TombstoneType {
	return engine.TombstoneData
}

func (r *recordingTombstoner) HasAnyInMemoryTombstone() bool {
	return false
}

func (r *recordingTombstoner) HasAnyTombstoneFile() bool {
	return true
}

func (r *recordingTombstoner) String() string {
	return "recordingTombstoner"
}

func (r *recordingTombstoner) StringWithPrefix(prefix string) string {
	return prefix + "recordingTombstoner"
}

func (r *recordingTombstoner) HasBlockTombstone(context.Context, *objectio.Blockid, fileservice.FileService) (bool, error) {
	return false, nil
}

func (r *recordingTombstoner) MarshalBinaryWithBuffer(*bytes.Buffer) error {
	return nil
}

func (r *recordingTombstoner) UnmarshalBinary([]byte) error {
	return nil
}

func (r *recordingTombstoner) PrefetchTombstones(string, fileservice.FileService, []objectio.Blockid) {
}

func (r *recordingTombstoner) ApplyInMemTombstones(_ *types.Blockid, rowsOffset []int64, _ *objectio.Bitmap) []int64 {
	return rowsOffset
}

func (r *recordingTombstoner) ApplyPersistedTombstones(
	_ context.Context,
	_ fileservice.FileService,
	snapshot *types.TS,
	_ *types.Blockid,
	rowsOffset []int64,
	_ *objectio.Bitmap,
) ([]int64, error) {
	r.snapshots = append(r.snapshots, *snapshot)
	return rowsOffset, nil
}

func (r *recordingTombstoner) Merge(engine.Tombstoner) error {
	return nil
}

func (r *recordingTombstoner) SortInMemory() {}

// tsFilteringTombstoner reacts to the readTS argument. It deletes a row
// only when the read timestamp is >= the row's tombstone commit timestamp.
// This makes the snapshotTS vs currentTS distinction observable.
type tsFilteringTombstoner struct {
	tombstones map[int64]types.TS // rowOffset -> tombstone commit ts
}

func (t *tsFilteringTombstoner) Type() engine.TombstoneType { return engine.TombstoneData }
func (t *tsFilteringTombstoner) HasAnyInMemoryTombstone() bool {
	return false
}
func (t *tsFilteringTombstoner) HasAnyTombstoneFile() bool        { return true }
func (t *tsFilteringTombstoner) String() string                   { return "tsFilteringTombstoner" }
func (t *tsFilteringTombstoner) StringWithPrefix(p string) string { return p + t.String() }
func (t *tsFilteringTombstoner) HasBlockTombstone(context.Context, *objectio.Blockid, fileservice.FileService) (bool, error) {
	return true, nil
}
func (t *tsFilteringTombstoner) MarshalBinaryWithBuffer(*bytes.Buffer) error { return nil }
func (t *tsFilteringTombstoner) UnmarshalBinary([]byte) error                { return nil }
func (t *tsFilteringTombstoner) PrefetchTombstones(string, fileservice.FileService, []objectio.Blockid) {
}
func (t *tsFilteringTombstoner) ApplyInMemTombstones(_ *types.Blockid, rowsOffset []int64, _ *objectio.Bitmap) []int64 {
	return rowsOffset
}
func (t *tsFilteringTombstoner) ApplyPersistedTombstones(
	_ context.Context,
	_ fileservice.FileService,
	snapshot *types.TS,
	_ *types.Blockid,
	rowsOffset []int64,
	deletedRows *objectio.Bitmap,
) ([]int64, error) {
	left := rowsOffset[:0]
	for _, off := range rowsOffset {
		ts, ok := t.tombstones[off]
		if ok && !ts.GT(snapshot) {
			if deletedRows != nil {
				deletedRows.Add(uint64(off))
			}
			continue
		}
		left = append(left, off)
	}
	return left, nil
}
func (t *tsFilteringTombstoner) Merge(engine.Tombstoner) error { return nil }
func (t *tsFilteringTombstoner) SortInMemory()                 {}

// TestMaterializedSnapshotDataSourcePersistedTombstoneFiltersBySnapshotTS proves
// that fallback uses snapshotTS (not currentTS) for persisted tombstone visibility.
// If the production code reverts to currentTS, this test fails.
func TestMaterializedSnapshotDataSourcePersistedTombstoneFiltersBySnapshotTS(t *testing.T) {
	tomb := &tsFilteringTombstoner{
		tombstones: map[int64]types.TS{
			1: types.BuildTS(25, 0), // tombstoned at 25, after snapshotTS=20
		},
	}

	source := newMaterializedSnapshotDataSource(
		context.Background(),
		nil,
		nil,
		types.BuildTS(30, 0), // currentTS — would mark row 1 deleted
		types.BuildTS(20, 0), // snapshotTS — would NOT mark row 1 deleted
		nil,
		tomb,
	)
	ds := source.(*materializedSnapshotDataSource)

	rowID := buildTestMaterializedSnapshotRowID(t, 9)
	bid := rowID.BorrowBlockID()

	left, err := ds.ApplyTombstones(context.Background(), bid, []int64{0, 1}, engine.Policy_CheckAll)
	require.NoError(t, err)
	// snapshotTS=20 < tombstone(25), so row 1 must NOT be filtered out.
	// If this test sees [0] only, the production code is reading at currentTS.
	require.Equal(t, []int64{0, 1}, left, "snapshotTS must shield row 1 from a future tombstone at TS=25")

	bm, err := ds.GetTombstones(context.Background(), bid)
	require.NoError(t, err)
	// Same invariant via GetTombstones path.
	require.Equal(t, 0, bm.Count(), "no row should be in tombstone bitmap at snapshotTS=20")
	bm.Release()
}

func TestMaterializedSnapshotDataSourceAccessors_NoRemote(t *testing.T) {
	mp := mpool.MustNewZero()
	pState := newTestMaterializedSnapshotPartitionState(t, mp, nil)

	src := newMaterializedSnapshotDataSource(
		context.Background(),
		nil,
		pState,
		types.BuildTS(30, 0),
		types.BuildTS(20, 0),
		nil,
		nil,
	).(*materializedSnapshotDataSource)

	s := src.String()
	require.Contains(t, s, "MaterializedSnapshotDataSource")
	require.Contains(t, s, "snapshot-ts=")
	require.Contains(t, s, "current-ts=")

	require.Nil(t, src.GetOrderBy())
	src.SetOrderBy(nil)
	require.Nil(t, src.GetOrderBy())
	src.SetFilterZM(objectio.ZoneMap{})
}

func TestMaterializedSnapshotDataSourceAccessors_WithRemote(t *testing.T) {
	mp := mpool.MustNewZero()
	pState := newTestMaterializedSnapshotPartitionState(t, mp, nil)

	relData := newTestSnapshotRelData(1)

	src := newMaterializedSnapshotDataSource(
		context.Background(),
		nil,
		pState,
		types.BuildTS(30, 0),
		types.BuildTS(20, 0),
		relData,
		nil,
	).(*materializedSnapshotDataSource)
	require.NotNil(t, src.remote)

	src.SetOrderBy(nil)
	_ = src.GetOrderBy()
	src.SetFilterZM(objectio.ZoneMap{})
	_ = src.String()
}
