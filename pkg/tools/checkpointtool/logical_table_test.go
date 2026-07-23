// Copyright 2021 Matrix Origin
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

package checkpointtool

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildLogicalTableViewEmpty(t *testing.T) {
	reader := &CheckpointReader{}
	view, err := reader.BuildLogicalTableView(context.Background(), types.TS{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"object", "block", "row"}, view.Headers)
	assert.Empty(t, view.Rows)
	assert.Equal(t, 0, view.PhysicalRows)
	assert.Equal(t, 0, view.DeletedRows)
	assert.Equal(t, 0, view.VisibleRows)
}

func TestVisibleObjectEntriesFiltersAndDedupesBySnapshot(t *testing.T) {
	snapshot := types.BuildTS(15, 0)
	entries := []*ObjectEntryInfo{
		newTestObjectEntryInfo(1, 10, 0),
		newTestObjectEntryInfo(1, 12, 20),
		newTestObjectEntryInfo(2, 16, 0), // created after snapshot
		newTestObjectEntryInfo(3, 1, 14), // deleted before snapshot
	}

	visible := visibleObjectEntries(entries, snapshot)
	require.Len(t, visible, 1)
	assert.Equal(t, uint32(1), visible[0].ObjectStats.Rows())
	assert.Equal(t, types.BuildTS(10, 0), visible[0].CreateTime)
	assert.Equal(t, types.BuildTS(20, 0), visible[0].DeleteTime)
}

func TestVisibleObjectEntriesDeletedAfterSnapshotBecomesInvisibleLater(t *testing.T) {
	entries := []*ObjectEntryInfo{
		newTestObjectEntryInfo(7, 5, 0),
		newTestObjectEntryInfo(7, 8, 30),
	}

	visibleAt20 := visibleObjectEntries(entries, types.BuildTS(20, 0))
	require.Len(t, visibleAt20, 1)

	visibleAt30 := visibleObjectEntries(entries, types.BuildTS(30, 0))
	assert.Empty(t, visibleAt30)
}

func TestDedupeObjectStats(t *testing.T) {
	entries := []*ObjectEntryInfo{
		newTestObjectEntryInfo(1, 1, 0),
		newTestObjectEntryInfo(1, 2, 0),
		newTestObjectEntryInfo(2, 3, 0),
	}

	stats := dedupeObjectStats(entries)
	require.Len(t, stats, 2)
	assert.NotEqual(t, stats[0].ObjectName().String(), stats[1].ObjectName().String())
}

func TestColumnSeqNumsAndAlignRowValues(t *testing.T) {
	cols := []objecttool.ColInfo{
		{Idx: 0, SeqNum: 2},
		{Idx: 1, SeqNum: 4},
	}
	require.Equal(t, []uint16{2, 4}, columnSeqNums(cols))

	values, nulls := alignRowValuesBySeqNums(cols, []uint16{4, 2, 6}, []string{"v2", "v4"}, []bool{false, true})
	require.Equal(t, []string{"v4", "v2", ""}, values)
	require.Equal(t, []bool{true, false, true}, nulls)

	values, nulls = alignRowValuesBySeqNums(nil, []uint16{1}, []string{"x"}, []bool{false})
	require.Equal(t, []string{"x"}, values)
	require.Equal(t, []bool{false}, nulls)

	values, nulls = alignRowValuesBySeqNums(cols, nil, []string{"x"}, []bool{false})
	require.Equal(t, []string{"x"}, values)
	require.Equal(t, []bool{false}, nulls)

	values, nulls = alignRowValuesBySeqNums(cols, []uint16{4, 2}, []string{"v2"}, nil)
	require.Equal(t, []string{"", "v2"}, values)
	require.Equal(t, []bool{true, false}, nulls)

	debugLogicalObjectColumns(objectio.ObjectStats{}, nil)
}

func TestLogicalTableUsesColumnsFromAllSchemaVersions(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewFileService(ctx, fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: t.TempDir(),
		Cache:   fileservice.DisabledCacheConfig,
	}, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)

	oldStats := writeLogicalSchemaObject(t, fs, types.Uuid{1}, []uint16{0, 1}, int32(1), "old", "")
	newStats := writeLogicalSchemaObject(t, fs, types.Uuid{2}, []uint16{0, 1, 2}, int32(2), "new", "added")
	entries := []*ObjectEntryInfo{
		{ObjectStats: oldStats, CreateTime: types.BuildTS(1, 0)},
		{ObjectStats: newStats, CreateTime: types.BuildTS(2, 0)},
	}
	reader := &CheckpointReader{ctx: ctx, fs: fs, mp: mpool.MustNewZero()}

	view, err := reader.BuildLogicalTableView(ctx, types.BuildTS(10, 0), entries, nil)
	require.NoError(t, err)
	require.Equal(t, []uint16{0, 1, 2}, view.ColSeqNums)
	require.Len(t, view.Rows, 2)
	require.Equal(t, []string{"1", "old", ""}, view.DataRow(view.Rows[0]))
	require.Equal(t, []string{"2", "new", "added"}, view.DataRow(view.Rows[1]))

	var csv bytes.Buffer
	err = reader.streamTableCSV(ctx, &csv, &TableSchema{Columns: []TableColumn{
		{Name: "id", SQLType: "INT", PhysicalPosition: 0},
		{Name: "name", SQLType: "VARCHAR", PhysicalPosition: 1},
		{Name: "added", SQLType: "VARCHAR", PhysicalPosition: 2},
	}}, types.BuildTS(10, 0), entries, nil, CSVExportOptions{
		IncludeMetadata: true,
		RowOrder:        CSVRowOrderLexical,
	})
	require.NoError(t, err)
	require.Contains(t, csv.String(), "1,\"old\",\\N\n")
	require.Contains(t, csv.String(), "2,\"new\",\"added\"\n")
}

func writeLogicalSchemaObject(
	t *testing.T,
	fs fileservice.FileService,
	uuid types.Uuid,
	seqNums []uint16,
	id int32,
	name string,
	added string,
) objectio.ObjectStats {
	t.Helper()
	ctx := context.Background()
	mp := mpool.MustNewZero()
	objectName := objectio.BuildObjectName(&uuid, 1)
	writer, err := objectio.NewObjectWriter(objectName, fs, 0, seqNums, nil)
	require.NoError(t, err)

	bat := batch.NewWithSize(len(seqNums))
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], id, false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(name), false, mp))
	if len(seqNums) == 3 {
		bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte(added), false, mp))
	}
	bat.SetRowCount(1)
	defer bat.Clean(mp)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	_, err = writer.WriteEnd(ctx, objectio.WriteOptions{Type: objectio.WriteTS, Val: time.Unix(100, 0)})
	require.NoError(t, err)

	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, objectName))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
	return *stats
}

func TestVisibleObjectEntriesEmptyAndSorted(t *testing.T) {
	require.Nil(t, visibleObjectEntries(nil, types.BuildTS(10, 0)))

	visible := visibleObjectEntries([]*ObjectEntryInfo{
		newTestObjectEntryInfo(3, 1, 0),
		newTestObjectEntryInfo(1, 1, 0),
		newTestObjectEntryInfo(2, 1, 0),
	}, types.BuildTS(10, 0))
	require.Len(t, visible, 3)
	assert.Less(t, visible[0].ObjectStats.ObjectName().String(), visible[1].ObjectStats.ObjectName().String())
	assert.Less(t, visible[1].ObjectStats.ObjectName().String(), visible[2].ObjectStats.ObjectName().String())
}

func TestFilterTombstonesAndDeleteMaskEmptyInputs(t *testing.T) {
	reader := &CheckpointReader{}
	relevant, err := reader.filterTombstonesForObject(context.Background(), &objectio.ObjectId{}, nil)
	require.NoError(t, err)
	require.Nil(t, relevant)

	mask, err := reader.buildDeleteMaskForBlock(context.Background(), &types.TS{}, objectio.ObjectStats{}, 0, nil)
	require.NoError(t, err)
	require.False(t, mask.IsValid())
}

func TestFilterTombstonesAndDeleteMaskHooks(t *testing.T) {
	stats := testCheckpointObjectStats(t, 7)
	objectID := stats.ObjectName().ObjectId()
	reader := &CheckpointReader{
		filterTombstonesForTest: func(gotID *objectio.ObjectId, gotStats []objectio.ObjectStats) ([]objectio.ObjectStats, error) {
			require.Equal(t, objectID, gotID)
			require.Len(t, gotStats, 1)
			return gotStats, nil
		},
		buildDeleteMaskForTest: func(snapshotTS *types.TS, gotStats objectio.ObjectStats, blockIdx uint16, gotTombstones []objectio.ObjectStats) (objectio.Bitmap, error) {
			require.NotNil(t, snapshotTS)
			require.Equal(t, stats.ObjectName().String(), gotStats.ObjectName().String())
			require.Equal(t, uint16(2), blockIdx)
			require.Len(t, gotTombstones, 1)
			return objectio.NullBitmap, nil
		},
	}

	relevant, err := reader.filterTombstonesForObject(context.Background(), objectID, []objectio.ObjectStats{stats})
	require.NoError(t, err)
	require.Len(t, relevant, 1)

	mask, err := reader.buildDeleteMaskForBlock(context.Background(), &types.TS{}, stats, 2, []objectio.ObjectStats{stats})
	require.NoError(t, err)
	require.False(t, mask.IsValid())

	reader.filterTombstonesForTest = func(*objectio.ObjectId, []objectio.ObjectStats) ([]objectio.ObjectStats, error) {
		return nil, assert.AnError
	}
	_, err = reader.filterTombstonesForObject(context.Background(), objectID, []objectio.ObjectStats{stats})
	require.ErrorIs(t, err, assert.AnError)

	reader.buildDeleteMaskForTest = func(*types.TS, objectio.ObjectStats, uint16, []objectio.ObjectStats) (objectio.Bitmap, error) {
		return objectio.NullBitmap, assert.AnError
	}
	_, err = reader.buildDeleteMaskForBlock(context.Background(), &types.TS{}, stats, 2, []objectio.ObjectStats{stats})
	require.ErrorIs(t, err, assert.AnError)
}

func TestBuildLogicalTableViewReadsVisibleRows(t *testing.T) {
	ctx := context.Background()
	fs, stats := writeLogicalTableTestObject(t, "logical.obj")
	defer fs.Close(ctx)

	reader := &CheckpointReader{
		ctx: ctx,
		fs:  fs,
		mp:  mpool.MustNewZero(),
	}
	view, err := reader.BuildLogicalTableView(ctx, types.BuildTS(200, 0), []*ObjectEntryInfo{
		{
			ObjectStats: stats,
			CreateTime:  types.BuildTS(1, 0),
		},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"object", "block", "row", "col_0", "col_1"}, view.Headers)
	require.Len(t, view.Rows, 2)
	require.Equal(t, 2, view.PhysicalRows)
	require.Equal(t, 0, view.DeletedRows)
	require.Equal(t, 2, view.VisibleRows)
	require.Equal(t, []string{"1", "alice"}, view.DataRow(view.Rows[0]))

	limited, err := reader.BuildLogicalTableViewLimited(ctx, types.BuildTS(200, 0), []*ObjectEntryInfo{
		{ObjectStats: stats, CreateTime: types.BuildTS(1, 0)},
	}, nil, 1, 0)
	require.NoError(t, err)
	require.True(t, limited.Truncated)
	require.Len(t, limited.Rows, 1)
	require.Equal(t, 1, limited.VisibleRows)

	view, err = reader.BuildLogicalTableView(ctx, types.BuildTS(200, 0), []*ObjectEntryInfo{
		{
			ObjectStats: stats,
			CreateTime:  types.BuildTS(1, 0),
			DeleteTime:  types.BuildTS(100, 0),
		},
	}, nil)
	require.NoError(t, err)
	require.Empty(t, view.Rows)
	require.Zero(t, view.PhysicalRows)
}

func TestBuildLogicalTableViewAppliesDeleteMaskHook(t *testing.T) {
	ctx := context.Background()
	fs, stats := writeLogicalTableTestObject(t, "logical-delete.obj")
	defer fs.Close(ctx)

	reader := &CheckpointReader{
		ctx: ctx,
		fs:  fs,
		mp:  mpool.MustNewZero(),
		filterTombstonesForTest: func(_ *objectio.ObjectId, tombstones []objectio.ObjectStats) ([]objectio.ObjectStats, error) {
			return tombstones, nil
		},
		buildDeleteMaskForTest: func(_ *types.TS, _ objectio.ObjectStats, blockIdx uint16, tombstones []objectio.ObjectStats) (objectio.Bitmap, error) {
			require.Equal(t, uint16(0), blockIdx)
			require.Len(t, tombstones, 1)
			mask := objectio.GetReusableBitmap()
			mask.Add(1)
			return mask, nil
		},
	}
	view, err := reader.BuildLogicalTableView(ctx, types.BuildTS(200, 0), []*ObjectEntryInfo{
		{
			ObjectStats: stats,
			CreateTime:  types.BuildTS(1, 0),
		},
	}, []*ObjectEntryInfo{
		{
			ObjectStats: stats,
			CreateTime:  types.BuildTS(1, 0),
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, view.PhysicalRows)
	require.Equal(t, 1, view.DeletedRows)
	require.Equal(t, 1, view.VisibleRows)
	require.Len(t, view.Rows, 1)
	require.Equal(t, []string{"1", "alice"}, view.DataRow(view.Rows[0]))
}

func TestScanLogicalTableTombstoneHookErrors(t *testing.T) {
	ctx := context.Background()
	fs, stats := writeLogicalTableTestObject(t, "logical-hook-errors.obj")
	defer fs.Close(ctx)
	entries := []*ObjectEntryInfo{{ObjectStats: stats, CreateTime: types.BuildTS(1, 0)}}
	tombstones := []*ObjectEntryInfo{{ObjectStats: stats, CreateTime: types.BuildTS(1, 0)}}

	reader := &CheckpointReader{
		ctx: ctx,
		fs:  fs,
		mp:  mpool.MustNewZero(),
		filterTombstonesForTest: func(*objectio.ObjectId, []objectio.ObjectStats) ([]objectio.ObjectStats, error) {
			return nil, assert.AnError
		},
	}
	_, err := reader.scanLogicalTable(ctx, types.BuildTS(200, 0), entries, tombstones, nil, nil)
	require.ErrorIs(t, err, assert.AnError)

	reader.filterTombstonesForTest = func(_ *objectio.ObjectId, tombstones []objectio.ObjectStats) ([]objectio.ObjectStats, error) {
		return tombstones, nil
	}
	reader.buildDeleteMaskForTest = func(*types.TS, objectio.ObjectStats, uint16, []objectio.ObjectStats) (objectio.Bitmap, error) {
		return objectio.NullBitmap, assert.AnError
	}
	_, err = reader.scanLogicalTable(ctx, types.BuildTS(200, 0), entries, tombstones, nil, nil)
	require.ErrorIs(t, err, assert.AnError)
}

func TestScanLogicalTableCallbacksAndSnapshotCutoff(t *testing.T) {
	ctx := context.Background()
	fs, stats := writeLogicalTableTestObject(t, "logical-callbacks.obj")
	defer fs.Close(ctx)

	reader := &CheckpointReader{
		ctx: ctx,
		fs:  fs,
		mp:  mpool.MustNewZero(),
	}
	entries := []*ObjectEntryInfo{{
		ObjectStats: stats,
		CreateTime:  types.BuildTS(1, 0),
	}}

	colErr := assert.AnError
	_, err := reader.scanLogicalTable(ctx, types.BuildTS(200, 0), entries, nil,
		func([]objecttool.ColInfo) error { return colErr },
		nil,
	)
	require.ErrorIs(t, err, colErr)

	rowErr := assert.AnError
	_, err = reader.scanLogicalTable(ctx, types.BuildTS(200, 0), entries, nil,
		func([]objecttool.ColInfo) error { return nil },
		func(string, int, int, []string, []bool) error { return rowErr },
	)
	require.ErrorIs(t, err, rowErr)

	statsAt50, err := reader.scanLogicalTable(ctx, types.BuildTS(50, 0), entries, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 2, statsAt50.PhysicalRows)
	require.Equal(t, 2, statsAt50.VisibleRows)
	require.Zero(t, statsAt50.DeletedRows)
}

func TestScanLogicalTableSkipsMissingDataFile(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewLocalFS(ctx, "local", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)

	reader := &CheckpointReader{
		ctx: ctx,
		fs:  fs,
		mp:  mpool.MustNewZero(),
	}
	stats, err := reader.scanLogicalTable(ctx, types.BuildTS(200, 0), []*ObjectEntryInfo{
		newTestObjectEntryInfo(9, 1, 0),
	}, nil, nil, nil)
	require.ErrorContains(t, err, "is not found")
	require.Zero(t, stats.PhysicalRows)
	require.Zero(t, stats.VisibleRows)
	require.Zero(t, stats.DeletedRows)
}

func writeLogicalTableTestObject(t *testing.T, _ string) (fileservice.FileService, objectio.ObjectStats) {
	t.Helper()
	ctx := context.Background()
	mp := mpool.MustNewZero()
	objectName := objectio.BuildObjectName(&types.Uuid{1}, 1)
	filename := objectName.String()
	fs, err := fileservice.NewFileService(ctx, fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: t.TempDir(),
		Cache:   fileservice.DisabledCacheConfig,
	}, nil)
	require.NoError(t, err)

	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterNormal, filename, fs)
	require.NoError(t, err)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(1), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("alice"), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(2), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("bob"), false, mp))
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	_, err = writer.Write(bat)
	require.NoError(t, err)
	_, err = writer.WriteEnd(ctx, objectio.WriteOptions{
		Type: objectio.WriteTS,
		Val:  time.Unix(100, 0),
	})
	require.NoError(t, err)
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, objectName))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 2))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
	return fs, *stats
}

func TestLogicalTableViewWidthsAndRows(t *testing.T) {
	require.Equal(t, len(logicalTableViewMetaHeaders), (*LogicalTableView)(nil).MetaWidth())
	require.Zero(t, (*LogicalTableView)(nil).DataWidth())

	view := newLogicalTableView()
	require.Equal(t, len(logicalTableViewMetaHeaders), view.MetaWidth())
	require.Zero(t, view.DataWidth())

	view.Headers = append(view.Headers, "id", "name")
	require.Equal(t, 3, view.MetaWidth())
	require.Equal(t, 2, view.DataWidth())
	require.Equal(t, []string{"42", "alice"}, view.DataRow([]string{"obj", "0", "1", "42", "alice"}))
	require.Nil(t, view.DataRow([]string{"obj", "0"}))

	view.Headers = []string{"object", "id"}
	require.Equal(t, 1, view.MetaWidth())
	require.Equal(t, 1, view.DataWidth())
	require.Equal(t, []string{"42"}, view.DataRow([]string{"obj", "42"}))

	view.Headers = nil
	require.Zero(t, view.MetaWidth())
	require.Zero(t, view.DataWidth())
	require.Empty(t, view.DataRow(nil))
}

func newTestObjectEntryInfo(idByte byte, createPhysical int64, deletePhysical int64) *ObjectEntryInfo {
	var objectID objectio.ObjectId
	objectID[0] = idByte
	stats := objectio.NewObjectStats()
	_ = objectio.SetObjectStatsObjectName(stats, objectio.BuildObjectNameWithObjectID(&objectID))
	_ = objectio.SetObjectStatsRowCnt(stats, uint32(idByte))
	_ = objectio.SetObjectStatsBlkCnt(stats, 1)

	entry := &ObjectEntryInfo{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(createPhysical, 0),
	}
	if deletePhysical > 0 {
		entry.DeleteTime = types.BuildTS(deletePhysical, 0)
	}
	return entry
}
