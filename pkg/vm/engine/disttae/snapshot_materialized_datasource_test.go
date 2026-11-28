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
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
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
	rowIDVec := vector.NewVec(types.T_Rowid.ToType(), mp)
	tsVec := vector.NewVec(types.T_TS.ToType(), mp)
	idVec := vector.NewVec(types.T_int64.ToType(), mp)
	payloadVec := vector.NewVec(types.T_int64.ToType(), mp)
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
	pState.HandleRowsInsert(context.Background(), mustMarshalVectors(rowIDVec, tsVec, idVec, payloadVec), []string{"rowid", "time", "id", "payload"}, 0, packer, mp)

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
	for i := range attrs {
		out.Vecs[i] = vector.NewVec(colTypes[i], mp)
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

func (r *recordingTombstoner) PrefetchTombstones(context.Context, string, fileservice.FileService, []objectio.Blockid) {
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
func (t *tsFilteringTombstoner) PrefetchTombstones(context.Context, string, fileservice.FileService, []objectio.Blockid) {
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

func TestSnapshotStateChangeRangeProjectsEvolvedColumns(t *testing.T) {
	for _, tc := range []struct {
		name       string
		persisted  bool
		appendable bool
		hole       bool
	}{
		{name: "historical logtail"},
		{name: "logtail interior hole", hole: true},
		{name: "mixed logtail and CN object", persisted: true},
		{name: "mixed logtail and appendable object", persisted: true, appendable: true},
		{name: "persisted interior hole", persisted: true, appendable: true, hole: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := engine.WithRetainRowID(t.Context(), true)
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			fs, err := fileservice.NewLocalFS(ctx, "change", t.TempDir(), fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			defer fs.Close(ctx)

			intType, boolType := types.T_int64.ToType(), types.T_bool.ToType()
			tbl := &txnTable{tableDef: &plan.TableDef{
				FeatureFlag: features.SchemaEvolved,
				Cols: []*plan.ColDef{
					{Name: "id", Typ: intType.PlanType()},
					{Name: "payload", Seqnum: 1, Typ: intType.PlanType()},
					{Name: "extra", Seqnum: 2, Typ: boolType.PlanType()},
					{Name: objectio.PhysicalAddr_Attr, Seqnum: objectio.SEQNUM_ROWID, Typ: objectio.RowidType.PlanType()},
				},
			}}
			tbl.ensureSeqnumsAndTypesExpectRowid()
			start, end := types.BuildTS(1, 0), types.BuildTS(20, 0)
			memTS, objectTS := types.BuildTS(10, 0), types.BuildTS(12, 0)
			memRow := buildTestMaterializedSnapshotRowID(t, 1)
			state := newTestMaterializedSnapshotPartitionState(t, mp, []testMaterializedSnapshotRow{
				{rowID: memRow, ts: memTS, id: 7, payload: 70},
			})
			type result struct {
				rowID   types.Rowid
				ts      types.TS
				payload int64
			}
			want := map[int64]result{7: {memRow, memTS, 70}}
			if tc.persisted {
				seqs := []uint16{0, 1}
				if tc.hole {
					seqs[1] = 2
				}
				if tc.appendable {
					seqs = append(seqs, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT)
				}
				input := batch.NewWithSize(len(seqs))
				defer input.Clean(mp)
				input.Vecs[0] = vector.NewVec(intType, mp)
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(8), false, mp))
				if tc.hole {
					input.Vecs[1] = vector.NewVec(boolType, mp)
					require.NoError(t, vector.AppendFixed(input.Vecs[1], true, false, mp))
				} else {
					input.Vecs[1] = vector.NewVec(intType, mp)
					require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(80), false, mp))
				}
				if tc.appendable {
					input.Vecs[2] = vector.NewVec(objectio.TSType, mp)
					input.Vecs[3] = vector.NewVec(boolType, mp)
					require.NoError(t, vector.AppendFixed(input.Vecs[2], objectTS, false, mp))
					require.NoError(t, vector.AppendFixed(input.Vecs[3], false, false, mp))
				}
				input.SetRowCount(1)
				writer := ioutil.ConstructWriter(0, seqs, -1, false, false, fs)
				if tc.appendable {
					writer.SetAppendable()
				}
				_, err = writer.WriteBatch(input)
				require.NoError(t, err)
				_, _, err = writer.Sync(ctx)
				require.NoError(t, err)
				stats := writer.GetObjectStats()
				if tc.appendable {
					stats = writer.GetObjectStats(objectio.WithAppendable())
				} else {
					stats = writer.GetObjectStats(objectio.WithCNCreated())
				}
				require.NoError(t, state.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{ObjectStats: stats, CreateTime: objectTS}, false))
				blockID := objectio.NewBlockidWithObjectID(stats.ObjectName().ObjectId(), 0)
				want[8] = result{types.NewRowid(&blockID, 0), objectTS, 80}
			} else if tc.hole {
				iter := state.NewRowsIter(end, nil, false)
				require.True(t, iter.Next())
				iter.Entry().Batch.Attrs[3] = ""
				require.NoError(t, iter.Close())
			}

			h := &PartitionChangesHandle{currentPSFrom: start, currentPSTo: end, fs: fs, mp: mp}
			require.NoError(t, h.installSnapshotStateRangeHandle(ctx, tbl, state))
			defer h.Close()
			if tc.hole {
				data, _, _, err := h.currentChangeHandle.Next(ctx, mp)
				require.Nil(t, data)
				require.ErrorContains(t, err, "column sequence 1 is missing within")
				return
			}
			got := make(map[int64]result)
			for {
				data, tombstone, _, err := h.currentChangeHandle.Next(ctx, mp)
				require.NoError(t, err)
				require.Nil(t, tombstone)
				if data == nil {
					break
				}
				require.Len(t, data.Vecs, 5)
				require.Equal(t, boolType, *data.Vecs[3].GetType())
				for i := range data.RowCount() {
					require.True(t, data.Vecs[3].IsNull(uint64(i)))
					id := vector.GetFixedAtWithTypeCheck[int64](data.Vecs[1], i)
					require.NotContains(t, got, id)
					got[id] = result{
						vector.GetFixedAtWithTypeCheck[types.Rowid](data.Vecs[0], i),
						vector.GetFixedAtWithTypeCheck[types.TS](data.Vecs[4], i),
						vector.GetFixedAtWithTypeCheck[int64](data.Vecs[2], i),
					}
				}
				data.Clean(mp)
			}
			require.Equal(t, want, got)
		})
	}
}
