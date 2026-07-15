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
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVecValueToString_SQLLikeFormatting(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	timeVec := vector.NewVec(types.New(types.T_time, 0, 6))
	defer timeVec.Free(mp)
	timeValue, err := types.ParseTime("12:34:56.123456", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(timeVec, timeValue, false, mp))
	require.Equal(t, "12:34:56.123456", vecValueToString(timeVec, 0))

	datetimeVec := vector.NewVec(types.New(types.T_datetime, 0, 6))
	defer datetimeVec.Free(mp)
	datetimeValue, err := types.ParseDatetime("2024-01-02 03:04:05.123456", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(datetimeVec, datetimeValue, false, mp))
	require.Equal(t, "2024-01-02 03:04:05.123456", vecValueToString(datetimeVec, 0))

	timestampVec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
	defer timestampVec.Free(mp)
	timestampValue, err := types.ParseTimestamp(time.Local, "2024-01-02 03:04:05.123456", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(timestampVec, timestampValue, false, mp))
	require.Equal(t, timestampValue.String2(time.Local, 6), vecValueToString(timestampVec, 0))

	decimalVec := vector.NewVec(types.New(types.T_decimal64, 18, 2))
	defer decimalVec.Free(mp)
	decimalValue := types.Decimal64(12345)
	require.NoError(t, vector.AppendFixed(decimalVec, decimalValue, false, mp))
	require.Equal(t, "123.45", vecValueToString(decimalVec, 0))

	uuidVec := vector.NewVec(types.T_uuid.ToType())
	defer uuidVec.Free(mp)
	uuidValue, err := types.ParseUuid("123e4567-e89b-12d3-a456-426614174000")
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(uuidVec, uuidValue, false, mp))
	require.Equal(t, "123e4567-e89b-12d3-a456-426614174000", vecValueToString(uuidVec, 0))

	jsonVec := vector.NewVec(types.T_json.ToType())
	defer jsonVec.Free(mp)
	require.NoError(t, vector.AppendBytes(jsonVec, []byte(`{"a":1}`), false, mp))
	require.Equal(t, `{"a":1}`, vecValueToString(jsonVec, 0))

	nullVec := vector.NewVec(types.T_int64.ToType())
	defer nullVec.Free(mp)
	require.NoError(t, vector.AppendFixed(nullVec, int64(0), true, mp))
	require.Equal(t, "NULL", vecValueToString(nullVec, 0))
}

func TestShouldIncludeIncrementalCheckpointWithoutBase(t *testing.T) {
	zero := types.TS{}
	ts1 := types.BuildTS(1, 0)
	ts2 := types.BuildTS(2, 0)

	assert.True(t, shouldIncludeIncrementalCheckpoint(zero, ts1, zero, ts1, false))
	assert.True(t, shouldIncludeIncrementalCheckpoint(ts1, ts2, zero, ts2, false))
	assert.False(t, shouldIncludeIncrementalCheckpoint(ts1, ts2, zero, ts1, false))

	assert.False(t, shouldIncludeIncrementalCheckpoint(zero, ts1, zero, ts1, true))
	assert.True(t, shouldIncludeIncrementalCheckpoint(ts1, ts2, zero, ts2, true))
}

// TestOpenWithKind verifies the offline checkpoint reader honors WithKind (the
// --local/--s3/--local2 selector) for both DISK and DISK-V2 formats. An empty
// directory has no checkpoint metadata, so Open succeeds with zero entries and
// the reader records the requested on-disk format.
func TestOpenWithKind(t *testing.T) {
	ctx := context.Background()

	for _, kind := range []string{objectio.OfflineKindLocal, objectio.OfflineKindLocal2} {
		r, err := Open(ctx, t.TempDir(), WithKind(kind))
		require.NoErrorf(t, err, "kind=%s", kind)
		require.Equal(t, kind, r.Kind())
		require.Empty(t, r.Entries())
		require.NoError(t, r.Close())
	}

	// default (no WithKind) is the legacy local DISK format
	r, err := Open(ctx, t.TempDir())
	require.NoError(t, err)
	require.Equal(t, objectio.OfflineKindLocal, r.Kind())
	require.NoError(t, r.Close())
}

func TestOpenWithFSOptions(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewLocalFS(ctx, "local", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	mp := mpool.MustNewZero()

	r, err := OpenWithFS(ctx, fs, "/display",
		WithKind(objectio.OfflineKindLocal2),
		WithMPool(mp),
		WithCloseFS(),
	)
	require.NoError(t, err)
	require.Equal(t, objectio.OfflineKindLocal2, r.Kind())
	require.Equal(t, "/display", r.Dir())
	require.Equal(t, fs, r.FS())
	require.Same(t, mp, r.mp)
	require.NoError(t, r.Close())
}

func TestCheckpointReaderBasicAccessorsAndFork(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewLocalFS(ctx, "local", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)

	entry := &checkpoint.CheckpointEntry{}
	reader := &CheckpointReader{
		ctx:     ctx,
		fs:      fs,
		dir:     "/tmp/ckp",
		kind:    objectio.OfflineKindLocal2,
		entries: []*checkpoint.CheckpointEntry{entry},
		mp:      mpool.MustNewZero(),
	}

	require.Equal(t, fs, reader.FS())
	require.Equal(t, "/tmp/ckp", reader.Dir())
	require.Equal(t, objectio.OfflineKindLocal2, reader.Kind())
	require.Equal(t, []*checkpoint.CheckpointEntry{entry}, reader.Entries())

	got, err := reader.GetEntry(0)
	require.NoError(t, err)
	require.Same(t, entry, got)
	_, err = reader.GetEntry(-1)
	require.Error(t, err)
	_, err = reader.GetEntry(1)
	require.Error(t, err)

	fork := reader.Fork(nil)
	require.NotSame(t, reader, fork)
	require.Equal(t, reader.ctx, fork.ctx)
	require.Equal(t, reader.fs, fork.fs)
	require.Equal(t, reader.dir, fork.dir)
	require.Equal(t, reader.kind, fork.kind)
	require.Equal(t, reader.entries, fork.entries)
	require.NotNil(t, fork.mp)

	customCtx := context.WithValue(ctx, struct{}{}, "x")
	fork = reader.Fork(customCtx)
	require.Equal(t, customCtx, fork.ctx)
}

func TestCheckpointReaderInfoSummarizesEntries(t *testing.T) {
	reader := &CheckpointReader{
		dir: "/ckp",
		entries: []*checkpoint.CheckpointEntry{
			checkpoint.NewCheckpointEntry("", types.BuildTS(10, 0), types.BuildTS(20, 0), checkpoint.ET_Global),
			checkpoint.NewCheckpointEntry("", types.BuildTS(21, 0), types.BuildTS(30, 0), checkpoint.ET_Incremental),
			checkpoint.NewCheckpointEntry("", types.BuildTS(1, 0), types.BuildTS(5, 0), checkpoint.ET_Compacted),
			checkpoint.NewCheckpointEntry("", types.TS{}, types.BuildTS(3, 0), checkpoint.ET_Incremental),
		},
	}

	info := reader.Info()
	require.Equal(t, "/ckp", info.Dir)
	require.Equal(t, 4, info.TotalEntries)
	require.Equal(t, 1, info.GlobalCount)
	require.Equal(t, 2, info.IncrCount)
	require.Equal(t, 1, info.CompactCount)
	require.Equal(t, types.BuildTS(1, 0), info.EarliestTS)
	require.Equal(t, types.BuildTS(30, 0), info.LatestTS)
}

func TestValidateSnapshotRequiresCatalogTables(t *testing.T) {
	entry := checkpoint.NewCheckpointEntry("", types.BuildTS(1, 0), types.BuildTS(10, 0), checkpoint.ET_Global)
	reader := &CheckpointReader{
		ctx:     context.Background(),
		entries: []*checkpoint.CheckpointEntry{entry},
	}

	reader.getTablesForTest = func(_ *CheckpointReader, _ *checkpoint.CheckpointEntry) ([]*TableInfo, error) {
		return nil, nil
	}
	require.ErrorContains(t, reader.ValidateSnapshot(context.Background(), types.BuildTS(10, 0)), "no tables")

	reader.getTablesForTest = func(_ *CheckpointReader, _ *checkpoint.CheckpointEntry) ([]*TableInfo, error) {
		return []*TableInfo{{TableID: moTablesID}}, nil
	}
	require.ErrorContains(t, reader.ValidateSnapshot(context.Background(), types.BuildTS(10, 0)), "mo_columns")

	reader.getTablesForTest = func(_ *CheckpointReader, _ *checkpoint.CheckpointEntry) ([]*TableInfo, error) {
		return []*TableInfo{{TableID: moTablesID}, {TableID: moColumnsID}}, nil
	}
	require.NoError(t, reader.ValidateSnapshot(context.Background(), types.BuildTS(10, 0)))
}

func TestCheckpointReaderTestHooksAndRangesToTables(t *testing.T) {
	hookErr := errors.New("hook failed")
	dataStats := testCheckpointObjectStats(t, 1)
	tombStats := testCheckpointObjectStats(t, 2)
	reader := &CheckpointReader{
		ctx: context.Background(),
		getTablesForTest: func(_ *CheckpointReader, _ *checkpoint.CheckpointEntry) ([]*TableInfo, error) {
			return []*TableInfo{{TableID: 7, AccountID: 3}}, nil
		},
		getObjectEntriesForTest: func(_ *CheckpointReader, _ *checkpoint.CheckpointEntry, tableID uint64) ([]*ObjectEntryInfo, []*ObjectEntryInfo, error) {
			require.Equal(t, uint64(7), tableID)
			return []*ObjectEntryInfo{{ObjectStats: dataStats}}, []*ObjectEntryInfo{{ObjectStats: tombStats}}, nil
		},
	}

	tables, err := reader.GetTables(&checkpoint.CheckpointEntry{})
	require.NoError(t, err)
	require.Equal(t, []*TableInfo{{TableID: 7, AccountID: 3}}, tables)

	data, tomb, err := reader.GetObjectEntries(&checkpoint.CheckpointEntry{}, 7)
	require.NoError(t, err)
	require.Len(t, data, 1)
	require.Len(t, tomb, 1)
	require.Equal(t, dataStats.ObjectName().String(), data[0].ObjectStats.ObjectName().String())
	require.Equal(t, tombStats.ObjectName().String(), tomb[0].ObjectStats.ObjectName().String())

	reader.getTablesForTest = func(_ *CheckpointReader, _ *checkpoint.CheckpointEntry) ([]*TableInfo, error) {
		return nil, hookErr
	}
	_, err = reader.GetTables(&checkpoint.CheckpointEntry{})
	require.ErrorIs(t, err, hookErr)

	ranges := []ckputil.TableRange{
		{TableID: uint64(1)<<32 | 10, ObjectType: ckputil.ObjectType_Data, ObjectStats: dataStats},
		{TableID: uint64(1)<<32 | 10, ObjectType: ckputil.ObjectType_Tombstone, ObjectStats: tombStats},
		{TableID: uint64(2)<<32 | 20, ObjectType: ckputil.ObjectType_Data, ObjectStats: dataStats},
	}
	got := reader.rangesToTables(ranges)
	require.Len(t, got, 2)
	require.Equal(t, uint32(1), got[0].AccountID)
	require.Equal(t, uint64(1)<<32|10, got[0].TableID)
	require.Len(t, got[0].DataRanges, 1)
	require.Len(t, got[0].TombRanges, 1)
	require.Equal(t, uint32(2), got[1].AccountID)
}

func TestCheckpointReaderEmptyLocationBranches(t *testing.T) {
	reader := &CheckpointReader{ctx: context.Background()}
	entry := &checkpoint.CheckpointEntry{}

	ranges, err := reader.GetTableRanges(entry)
	require.NoError(t, err)
	require.Nil(t, ranges)

	accounts, err := reader.GetAccounts(entry)
	require.NoError(t, err)
	require.Empty(t, accounts)

	tables, err := reader.GetTablesByAccount(entry, 1)
	require.NoError(t, err)
	require.Empty(t, tables)

	data, tomb, err := reader.GetObjectEntries(entry, 42)
	require.NoError(t, err)
	require.Nil(t, data)
	require.Nil(t, tomb)

	dataByTable, tombByTable, err := reader.GetObjectEntriesForTables(entry, map[uint64]struct{}{42: {}})
	require.NoError(t, err)
	require.Nil(t, dataByTable)
	require.Nil(t, tombByTable)
}

func TestCheckpointReaderReadTableAndRangeData(t *testing.T) {
	ctx := context.Background()
	fs, stats := writeLogicalTableTestObject(t, "range.obj")
	defer fs.Close(ctx)
	reader := &CheckpointReader{
		ctx: ctx,
		fs:  fs,
		mp:  mpool.MustNewZero(),
	}

	rng := ckputil.TableRange{
		TableID:     1,
		ObjectType:  ckputil.ObjectType_Data,
		ObjectStats: stats,
	}
	rng.Start.SetRowOffset(1)
	rng.End.SetRowOffset(1)

	bat, release, err := reader.ReadTableData(ctx, rng)
	require.NoError(t, err)
	require.NotNil(t, release)
	require.Equal(t, 2, bat.RowCount())
	release()

	cols, rows, err := reader.ReadRangeData(&checkpoint.CheckpointEntry{}, rng)
	require.NoError(t, err)
	require.Equal(t, []string{"account_id", "db_id"}, cols)
	require.Equal(t, [][]string{{"2", "bob"}}, rows)
}

func TestCheckpointReaderSmallHelpers(t *testing.T) {
	entries := []fileservice.DirEntry{
		{Name: "dir", IsDir: true},
		{Name: "file", IsDir: false},
		{Name: "file2", IsDir: false},
	}
	require.Equal(t, 1, countDirs(entries))
	require.Equal(t, 2, countFiles(entries))
	require.Equal(t, []string{"account_id", "db_id", "table_id"}, checkpointRangeColumns(3))
	require.Equal(t, []string{"account_id", "db_id", "table_id", "object_type", "id"}, checkpointRangeColumns(5))

	require.False(t, isDataFileNotFound(nil))
	require.True(t, isDataFileNotFound(moerr.NewFileNotFoundNoCtx("missing")))
	require.True(t, isDataFileNotFound(os.ErrNotExist))
	require.False(t, isDataFileNotFound(errors.New("other")))
}

func testCheckpointObjectStats(t *testing.T, idByte byte) objectio.ObjectStats {
	t.Helper()
	var objectID objectio.ObjectId
	objectID[0] = idByte
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, objectio.BuildObjectNameWithObjectID(&objectID)))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, uint32(idByte)))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
	return *stats
}
