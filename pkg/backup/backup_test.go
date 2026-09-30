// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/prashantv/gostub"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	gc "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ModuleName = "Backup"
)

func TestExecBackupRejectsIncompleteCheckpointResponse(t *testing.T) {
	testCases := []struct {
		name      string
		locations []string
		want      string
	}{
		{
			name: "empty response",
			want: "expected backup time and checkpoint location, got 0 field(s)",
		},
		{
			name:      "checkpoint timeout response",
			locations: []string{"2026-06-27 06:01:22"},
			want:      "expected backup time and checkpoint location, got 1 field(s)",
		},
		{
			name:      "empty backup time",
			locations: []string{"", "checkpoint"},
			want:      "backup time is empty",
		},
		{
			name:      "empty checkpoint location",
			locations: []string{"2026-06-27 06:01:22", ""},
			want:      "checkpoint location is empty",
		},
		{
			name:      "malformed checkpoint location",
			locations: []string{"2026-06-27 06:01:22", "Failed"},
			want:      "invalid checkpoint string",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := execBackup(
				context.Background(),
				"",
				nil,
				nil,
				testCase.locations,
				1,
				types.TS{},
				"full",
				nil,
				nil,
				nil,
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}
}

func TestExecBackupKeepsObjectDeletedAfterRestoreTimestamp(t *testing.T) {
	ctx := t.Context()
	src := newBackupMemoryFS(t, "backup-soft-deleted-src")
	dst := newBackupMemoryFS(t, "backup-soft-deleted-dst")

	objectID := objectio.NewObjectid()
	objectName := objectio.BuildObjectNameWithObjectID(&objectID)
	objectStats := objectio.NewObjectStatsWithObjectID(&objectID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsLocation(
		objectStats,
		objectio.BuildLocation(objectName, objectio.NewExtent(0, 0, 1, 1), 1, 0),
	))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(objectStats, 1))
	require.NoError(t, objectio.SetObjectStatsRowCnt(objectStats, 1))
	require.NoError(t, writeFile(ctx, src, objectName.String(), []byte("deleted object")))

	cat := catalog.MockCatalog(nil)
	defer cat.Close()
	dbEntry, err := cat.CreateDBEntry("backup_test", "", "", nil)
	require.NoError(t, err)
	table, err := dbEntry.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
	require.NoError(t, err)

	createTS := types.BuildTS(5, 0)
	checkpointStart := types.BuildTS(15, 0)
	checkpointEnd := types.BuildTS(30, 0)
	entry, err := table.CreateCommittedObject(
		createTS,
		&objectio.CreateObjOpt{Stats: objectStats},
		nil,
	)
	require.NoError(t, err)
	catalog.MockDroppedObjectEntry2List(entry, checkpointEnd)

	checkpointData, err := logtail.BackupCheckpointDataFactory(
		checkpointStart,
		checkpointEnd,
		src,
	)(cat)
	require.NoError(t, err)
	defer checkpointData.Close()
	checkpointLocation, _, err := checkpointData.Sync(ctx, src)
	require.NoError(t, err)

	objects, reader, err := logtail.LoadCheckpointEntriesFromKey(
		ctx,
		"backup-test",
		src,
		checkpointLocation,
		logtail.CheckpointCurrentVersion,
		nil,
		&types.TS{},
	)
	require.NoError(t, err)

	// DropTS is after the restore timestamp. ReWriteCheckpointAndBlockFromKey
	// clears this DeleteTS and retains the non-appendable object, so its physical
	// file must be present in the backup.
	files := selectBackupObjects(objects, checkpointStart, nil, nil, nil)
	require.Contains(t, files, objectName.String())
	_, err = parallelCopyData(ctx, src, dst, files, 1, nil)
	require.NoError(t, err)
	var restoredLive bool
	rewrittenLocation, _, _, err := logtail.ReWriteCheckpointAndBlockFromKey(
		ctx,
		"backup-test",
		src,
		dst,
		checkpointLocation,
		reader,
		logtail.CheckpointCurrentVersion,
		checkpointStart,
	)
	require.NoError(t, err)
	rewrittenReader, err := logtail.GetCheckpointReader(
		ctx,
		"backup-test",
		dst,
		rewrittenLocation,
		logtail.CheckpointCurrentVersion,
	)
	require.NoError(t, err)
	require.NoError(t, rewrittenReader.ForEachRow(
		ctx,
		func(
			_ uint32,
			_, _ uint64,
			_ int8,
			stats objectio.ObjectStats,
			_ types.TS,
			deleteTS types.TS,
			_ types.Rowid,
		) error {
			if stats.ObjectName().String() == objectName.String() && deleteTS.IsEmpty() {
				restoredLive = true
			}
			return nil
		},
	))
	require.True(t, restoredLive)
	_, err = dst.StatFile(ctx, objectName.String())
	require.NoError(t, err)
}

func TestSelectBackupObjectsFromIncrementalCheckpointOmitsHistoricalObject(t *testing.T) {
	for _, appendable := range []bool{false, true} {
		name := "nonappendable"
		if appendable {
			name = "appendable"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			src := newBackupMemoryFS(t, "historical-src")
			dst := newBackupMemoryFS(t, "historical-dst")
			objectID := objectio.NewObjectid()
			objectName := objectio.BuildObjectNameWithObjectID(&objectID)
			stats := objectio.NewObjectStatsWithObjectID(&objectID, appendable, false, false)
			require.NoError(t, objectio.SetObjectStatsLocation(
				stats,
				objectio.BuildLocation(objectName, objectio.NewExtent(0, 0, 1, 1), 1, 0),
			))
			require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
			require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))

			cat := catalog.MockCatalog(nil)
			defer cat.Close()
			db, err := cat.CreateDBEntry("backup_test", "", "", nil)
			require.NoError(t, err)
			table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
			require.NoError(t, err)
			createTS := types.BuildTS(5, 0)
			dropTS := types.BuildTS(10, 0)
			restoreTS := types.BuildTS(20, 1)
			entry, err := table.CreateCommittedObject(createTS, &objectio.CreateObjOpt{Stats: stats}, nil)
			require.NoError(t, err)
			catalog.MockDroppedObjectEntry2List(entry, dropTS)

			checkpointData, err := logtail.IncrementalCheckpointDataFactory(
				types.BuildTS(1, 0), types.BuildTS(20, 0), 0, src,
			)(cat)
			require.NoError(t, err)
			defer checkpointData.Close()
			checkpointLocation, _, err := checkpointData.Sync(ctx, src)
			require.NoError(t, err)
			objects, _, err := logtail.LoadCheckpointEntriesFromKey(
				ctx, "backup-test", src, checkpointLocation,
				logtail.CheckpointCurrentVersion, nil, &types.TS{},
			)
			require.NoError(t, err)

			var creationRows, deletionRows int
			for _, object := range objects {
				if object.Location.Name().String() != objectName.String() {
					continue
				}
				require.Equal(t, table.ID, object.TableID)
				require.Equal(t, ckputil.ObjectType_Data, object.ObjectType)
				require.Equal(t, createTS, object.CrateTS)
				if object.DropTS.IsEmpty() {
					creationRows++
				} else {
					require.Equal(t, dropTS, object.DropTS)
					deletionRows++
				}
			}
			if appendable {
				require.Zero(t, creationRows)
			} else {
				require.Equal(t, 1, creationRows)
			}
			require.Equal(t, 1, deletionRows)

			// The object file is deliberately absent; only checkpoint files may be copied.
			_, err = src.StatFile(ctx, objectName.String())
			require.Error(t, err)
			writeGCMetadata(t, ctx, src, types.BuildTS(1, 0), restoreTS)
			retention, err := loadBackupObjectRetention(ctx, "backup-test", src, restoreTS, nil)
			require.NoError(t, err)
			require.Equal(t, restoreTS, retention.end)
			files := selectBackupObjects(objects, restoreTS, nil, nil, retention)
			require.Contains(t, files, checkpointLocation.Name().String())
			_, err = parallelCopyData(ctx, src, dst, files, 1, nil)
			require.NoError(t, err)
			require.NotContains(t, files, objectName.String())
			for name := range files {
				_, err = dst.StatFile(ctx, name)
				require.NoError(t, err)
			}
		})
	}
}

type backupCheckpointMetaRecorder struct {
	checkpoint.Runner
	files map[string]struct{}
}

func (r *backupCheckpointMetaRecorder) AddCheckpointMetaFile(name string) {
	r.files[name] = struct{}{}
}

func newBackupLocalFS(t *testing.T, name string) fileservice.FileService {
	fs, err := fileservice.NewFileService(t.Context(), fileservice.Config{
		Name: name, Backend: "DISK", DataDir: t.TempDir(), Cache: fileservice.DisabledCacheConfig,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	return fs
}

func TestExecBackupKeepsSnapshotRetainedObjectFromCompactedCheckpoint(t *testing.T) {
	ctx := t.Context()
	src := newBackupLocalFS(t, "snapshot-retained-src")
	dst := newBackupLocalFS(t, "snapshot-retained-dst")
	createTS := types.BuildTS(5, 0)
	snapshotTS := types.BuildTS(7, 0)
	dropTS := types.BuildTS(10, 0)
	checkpointStart := types.BuildTS(1, 0)
	checkpointEnd := types.BuildTS(20, 0)
	restoreTS := types.BuildTS(20, 1)
	backupEnd := types.BuildTS(30, 0)

	objectID := objectio.NewObjectid()
	objectName := objectio.BuildObjectNameWithObjectID(&objectID)
	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer data.Clean(common.DebugAllocator)
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(42), false, common.DebugAllocator))
	data.SetRowCount(1)
	writer, err := ioutil.NewBlockWriterNew(src, objectName, 0, []uint16{0}, false)
	require.NoError(t, err)
	_, err = writer.WriteBatch(data)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	stats := writer.GetObjectStats()
	require.False(t, stats.GetAppendable())

	cat := catalog.MockCatalog(nil)
	defer cat.Close()
	db, err := cat.CreateDBEntry("backup_test", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
	require.NoError(t, err)
	entry, err := table.CreateCommittedObject(createTS, &objectio.CreateObjOpt{Stats: &stats}, nil)
	require.NoError(t, err)
	catalog.MockDroppedObjectEntry2List(entry, dropTS)

	incremental, err := logtail.IncrementalCheckpointDataFactory(
		checkpointStart, checkpointEnd, 0, src,
	)(cat)
	require.NoError(t, err)
	defer incremental.Close()
	incrementalLocation, _, err := incremental.Sync(ctx, src)
	require.NoError(t, err)
	_, incrementalReader, err := logtail.LoadCheckpointEntriesFromKey(
		ctx, "backup-test", src, incrementalLocation, logtail.CheckpointCurrentVersion, nil, &types.TS{},
	)
	require.NoError(t, err)
	incrementalRows, err := incrementalReader.GetCheckpointData(ctx)
	require.NoError(t, err)
	defer incrementalRows.Clean(common.CheckpointAllocator)
	retainedStats := vector.NewVec(ckputil.TableObjectsTypes[ckputil.TableObjectsAttr_ID_Idx])
	defer retainedStats.Free(common.CheckpointAllocator)
	var deletedRows int
	for row := 0; row < incrementalRows.RowCount(); row++ {
		encoded := incrementalRows.Vecs[ckputil.TableObjectsAttr_ID_Idx].GetBytesAt(row)
		rowStats := objectio.ObjectStats(encoded)
		if rowStats.ObjectName().String() != objectName.String() {
			continue
		}
		require.NoError(t, vector.AppendBytes(retainedStats, encoded, false, common.CheckpointAllocator))
		deletedAt := vector.GetFixedAtWithTypeCheck[types.TS](
			incrementalRows.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], row,
		)
		if !deletedAt.IsEmpty() {
			require.Equal(t, dropTS, deletedAt)
			require.True(t, logtail.ObjectIsSnapshotRefers(&rowStats, nil, &createTS, &dropTS, []types.TS{snapshotTS}))
			deletedRows++
		}
	}
	require.Equal(t, 1, deletedRows)
	require.Equal(t, 2, retainedStats.Length())
	filter := bloomfilter.New(int64(retainedStats.Length()), 0.01)
	defer filter.Free()
	filter.Add(retainedStats)

	incrementalEntry := checkpoint.NewCheckpointEntry(
		"", checkpointStart, checkpointEnd, checkpoint.ET_Incremental,
		checkpoint.WithStateEntryOption(checkpoint.ST_Finished),
	)
	incrementalEntry.SetLocation(incrementalLocation, incrementalLocation)
	metaRecorder := &backupCheckpointMetaRecorder{files: make(map[string]struct{})}
	_, _, compactedEntry, compactedRows, err := gc.MergeCheckpoint(
		ctx, "snapshot-retained-test", "backup-test", []*checkpoint.CheckpointEntry{incrementalEntry},
		&filter, &checkpointEnd, metaRecorder, common.CheckpointAllocator, src,
	)
	require.NoError(t, err)
	defer compactedRows.Clean(common.CheckpointAllocator)
	require.Equal(t, checkpoint.ET_Compacted, compactedEntry.GetType())
	require.Len(t, metaRecorder.files, 1)
	compactedObjects, _, err := logtail.LoadCheckpointEntriesFromKey(
		ctx, "backup-test", src, compactedEntry.GetLocation(), compactedEntry.GetVersion(), nil, &types.TS{},
	)
	require.NoError(t, err)
	var retained bool
	for _, object := range compactedObjects {
		if object.Location.Name().String() == objectName.String() && object.DropTS.Equal(&dropTS) {
			retained = true
		}
	}
	require.True(t, retained)

	special, err := logtail.BackupCheckpointDataFactory(restoreTS, backupEnd, src)(cat)
	require.NoError(t, err)
	defer special.Close()
	specialLocation, _, err := special.Sync(ctx, src)
	require.NoError(t, err)
	specialObjects, _, err := logtail.LoadCheckpointEntriesFromKey(
		ctx, "backup-test", src, specialLocation, logtail.CheckpointCurrentVersion, nil, &types.TS{},
	)
	require.NoError(t, err)
	for _, object := range specialObjects {
		require.NotEqual(t, objectName.String(), object.Location.Name().String())
	}
	meta := batch.NewWithSchema(false, checkpoint.CheckpointSchema.Attrs(), checkpoint.CheckpointSchema.Types())
	defer meta.Clean(common.CheckpointAllocator)
	for _, value := range []struct {
		index int
		value types.TS
	}{
		{checkpoint.CheckpointAttr_StartTSIdx, checkpointStart},
		{checkpoint.CheckpointAttr_EndTSIdx, checkpointEnd},
	} {
		require.NoError(t, vector.AppendFixed(meta.Vecs[value.index], value.value, false, common.CheckpointAllocator))
	}
	for _, index := range []int{checkpoint.CheckpointAttr_MetaLocationIdx, checkpoint.CheckpointAttr_AllLocationsIdx} {
		require.NoError(t, vector.AppendBytes(meta.Vecs[index], incrementalLocation, false, common.CheckpointAllocator))
	}
	require.NoError(t, vector.AppendFixed(meta.Vecs[checkpoint.CheckpointAttr_EntryTypeIdx], true, false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(meta.Vecs[checkpoint.CheckpointAttr_VersionIdx], logtail.CheckpointCurrentVersion, false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(meta.Vecs[checkpoint.CheckpointAttr_CheckpointLSNIdx], uint64(0), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(meta.Vecs[checkpoint.CheckpointAttr_TruncateLSNIdx], uint64(0), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendFixed(meta.Vecs[checkpoint.CheckpointAttr_TypeIdx], int8(checkpoint.ET_Incremental), false, common.CheckpointAllocator))
	require.NoError(t, vector.AppendBytes(meta.Vecs[checkpoint.CheckpointAttr_TableIDLocationIdx], nil, false, common.CheckpointAllocator))
	meta.SetRowCount(1)
	metaWriter, err := objectio.NewObjectWriterSpecial(
		objectio.WriterCheckpoint,
		ioutil.EncodeCKPMetadataFullName(checkpointStart, checkpointEnd), src,
	)
	require.NoError(t, err)
	_, err = metaWriter.Write(meta)
	require.NoError(t, err)
	_, err = metaWriter.WriteEnd(ctx)
	require.NoError(t, err)
	compactedMetaName := ioutil.EncodeCompactCKPMetadataFullName(checkpointStart, checkpointEnd)
	_, err = src.StatFile(ctx, compactedMetaName)
	require.NoError(t, err)
	sourceSnapshotEntries, err := checkpoint.ListSnapshotCheckpoint(
		ctx, "backup-test", src, snapshotTS, metaRecorder.files,
	)
	require.NoError(t, err)
	require.NotEmpty(t, sourceSnapshotEntries)
	writeGCMetadata(t, ctx, src, checkpointStart, restoreTS)
	retention, err := loadBackupObjectRetention(ctx, "backup-test", src, restoreTS,
		map[string][]*objectio.BackupObject{compactedEntry.GetLocation().String(): compactedObjects})
	require.NoError(t, err)
	require.Equal(t, checkpointStart, retention.start)
	require.Equal(t, restoreTS, retention.end)
	require.Contains(t, retention.retained, *objectName.Short())

	names := []string{
		"2026-09-30 00:00:00",
		fmt.Sprintf("%s:%d:%s:%s:%s", specialLocation.String(), logtail.CheckpointCurrentVersion,
			backupEnd.ToString(), specialLocation.String(), restoreTS.ToString()),
		fmt.Sprintf("%s:%d", compactedEntry.GetLocation().String(), compactedEntry.GetVersion()),
		fmt.Sprintf("%s:%d", specialLocation.String(), logtail.CheckpointCurrentVersion),
	}
	require.NoError(t, execBackup(ctx, "backup-test", src, dst, names, 1, types.TS{}, "full", nil, nil, nil))
	_, compactedMeta := ioutil.TryDecodeTSRangeFile(compactedMetaName)
	_, err = dst.StatFile(ctx, compactedMeta.GetCKPFullName())
	require.NoError(t, err)
	selected, err := checkpoint.ListSnapshotCheckpoint(ctx, "backup-test", dst, snapshotTS, metaRecorder.files)
	require.NoError(t, err)
	var selectedCompacted bool
	for _, checkpointEntry := range selected {
		if checkpointEntry.GetType() == checkpoint.ET_Compacted &&
			checkpointEntry.GetLocation().Name().String() == compactedEntry.GetLocation().Name().String() {
			selectedCompacted = true
		}
	}
	require.True(t, selectedCompacted)

	reader, err := ioutil.NewObjectReader(
		dst, stats.ObjectLocation(),
		objectio.WithDataCachePolicyOption(fileservice.SkipAllCache),
		objectio.WithMetaCachePolicyOption(fileservice.SkipAllCache),
	)
	require.NoError(t, err)
	readData, release, err := reader.LoadColumns(
		ctx, []uint16{0}, []types.Type{types.T_int64.ToType()}, 0, common.DebugAllocator,
	)
	require.NoError(t, err)
	defer release()
	require.Equal(t, []int64{42}, vector.MustFixedColWithTypeCheck[int64](readData.Vecs[0]))
}

func TestSelectBackupObjectsOnlySkipsObjectsDeletedBeforeRestoreTimestamp(t *testing.T) {
	newObject := func(dropTS types.TS) *objectio.BackupObject {
		objectID := objectio.NewObjectid()
		objectName := objectio.BuildObjectNameWithObjectID(&objectID)
		objectStats := objectio.NewObjectStatsWithObjectID(&objectID, false, false, false)
		require.NoError(t, objectio.SetObjectStatsLocation(
			objectStats,
			objectio.BuildLocation(objectName, objectio.NewExtent(0, 0, 1, 1), 1, 0),
		))
		return &objectio.BackupObject{
			Location: objectStats.ObjectLocation(),
			CrateTS:  types.BuildTS(5, 0),
			DropTS:   dropTS,
			NeedCopy: true,
		}
	}

	before := newObject(types.BuildTS(10, 0))
	at := newObject(types.BuildTS(15, 0))
	after := newObject(types.BuildTS(30, 0))
	files := selectBackupObjects(
		[]*objectio.BackupObject{before, at, after},
		types.BuildTS(15, 0),
		nil,
		nil,
		&backupObjectRetention{start: types.BuildTS(1, 0), end: types.BuildTS(15, 0)},
	)
	require.NotContains(t, files, before.Location.Name().String())
	require.Contains(t, files, at.Location.Name().String())
	require.Contains(t, files, after.Location.Name().String())
}

func TestSelectBackupObjectsOwnerLifecycle(t *testing.T) {
	type row struct {
		tableID    uint64
		objectType int8
		createTS   types.TS
		dropTS     types.TS
		needCopy   bool
	}
	create := types.BuildTS(5, 0)
	drop := types.BuildTS(10, 0)
	restore := types.BuildTS(20, 1)
	dataType := ckputil.ObjectType_Data
	tombstoneType := ckputil.ObjectType_Tombstone
	created := row{tableID: 1, objectType: dataType, createTS: create, needCopy: true}
	deleted := row{tableID: 1, objectType: dataType, createTS: create, dropTS: drop, needCopy: true}

	tests := []struct {
		name       string
		rows       []row
		restoreTS  types.TS
		wantFile   bool
		wantCopy   bool
		dstHave    bool
		globalHave bool
	}{
		{"creation then deletion", []row{created, deleted}, restore, false, false, false, false},
		{"deletion then creation", []row{deleted, created}, restore, false, false, false, false},
		{"different table keeps shared live file", []row{created, deleted, {
			tableID: 2, objectType: dataType, createTS: create, needCopy: true,
		}}, restore, true, true, false, false},
		{"different creation epoch keeps shared live file", []row{created, deleted, {
			tableID: 1, objectType: dataType, createTS: types.BuildTS(15, 0), needCopy: true,
		}}, restore, true, true, false, false},
		{"tombstone owner keeps shared live file", []row{created, deleted, {
			tableID: 1, objectType: tombstoneType, createTS: create, needCopy: true,
		}}, restore, true, true, false, false},
		{"drop before logical restore", []row{created, {
			tableID: 1, objectType: dataType, createTS: create, dropTS: types.BuildTS(20, 0), needCopy: true,
		}}, restore, false, false, false, false},
		{"drop equal logical restore", []row{created, {
			tableID: 1, objectType: dataType, createTS: create, dropTS: restore, needCopy: true,
		}}, restore, true, true, false, false},
		{"drop after logical restore", []row{created, {
			tableID: 1, objectType: dataType, createTS: create, dropTS: types.BuildTS(20, 2), needCopy: true,
		}}, restore, true, true, false, false},
		{"conflicting drops before then equal", []row{created, deleted, {
			tableID: 1, objectType: dataType, createTS: create, dropTS: restore, needCopy: true,
		}}, restore, true, true, false, false},
		{"conflicting drops equal then before", []row{created, {
			tableID: 1, objectType: dataType, createTS: create, dropTS: restore, needCopy: true,
		}, deleted}, restore, true, true, false, false},
		{"conflicting drops before then after", []row{created, deleted, {
			tableID: 1, objectType: dataType, createTS: create, dropTS: types.BuildTS(20, 2), needCopy: true,
		}}, restore, true, true, false, false},
		{"conflicting drops after then before", []row{created, {
			tableID: 1, objectType: dataType, createTS: create, dropTS: types.BuildTS(20, 2), needCopy: true,
		}, deleted}, restore, true, true, false, false},
		{"empty restore keeps lifecycle", []row{created, deleted}, types.TS{}, true, true, false, false},
		{"unowned metadata shares deleted file", []row{created, deleted, {
			createTS: create, needCopy: true,
		}}, restore, true, true, false, false},
		{"unowned creation survives unowned deletion", []row{
			{createTS: create, needCopy: true},
			{createTS: create, dropTS: drop, needCopy: true},
		}, restore, true, true, false, false},
		{"unowned deletion alone follows row boundary", []row{{
			createTS: create, dropTS: drop, needCopy: true,
		}}, restore, false, false, false, false},
		{"retained owners OR copy false then true", []row{
			{tableID: 1, objectType: dataType, createTS: create},
			{tableID: 2, objectType: dataType, createTS: create, needCopy: true},
		}, restore, true, true, false, false},
		{"retained owners OR copy true then false", []row{
			{tableID: 1, objectType: dataType, createTS: create, needCopy: true},
			{tableID: 2, objectType: dataType, createTS: create},
		}, restore, true, true, false, false},
		{"retained owners both no copy", []row{
			{tableID: 1, objectType: dataType, createTS: create},
			{tableID: 2, objectType: dataType, createTS: create},
		}, restore, true, false, false, false},
		{"destination already has shared file", []row{
			{tableID: 1, objectType: dataType, createTS: create},
			{tableID: 2, objectType: dataType, createTS: create, needCopy: true},
		}, restore, true, false, true, false},
		{"global index already has shared file", []row{
			{tableID: 1, objectType: dataType, createTS: create},
			{tableID: 2, objectType: dataType, createTS: create, needCopy: true},
		}, restore, true, false, false, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objectID := objectio.NewObjectid()
			objectName := objectio.BuildObjectNameWithObjectID(&objectID)
			location := objectio.BuildLocation(objectName, objectio.NewExtent(0, 0, 1, 1), 1, 0)
			objects := make([]*objectio.BackupObject, 0, len(test.rows))
			for _, r := range test.rows {
				objects = append(objects, &objectio.BackupObject{
					Location: location, CrateTS: r.createTS, DropTS: r.dropTS,
					NeedCopy: r.needCopy, TableID: r.tableID, ObjectType: r.objectType,
				})
			}
			name := objectName.String()
			var dstHave map[string]bool
			if test.dstHave {
				dstHave = map[string]bool{name: true}
			}
			var globalIndex *GlobalFileIndex
			if test.globalHave {
				globalIndex = NewGlobalFileIndex()
				globalIndex.Add(name)
			}
			files := selectBackupObjects(objects, test.restoreTS, dstHave, globalIndex,
				&backupObjectRetention{start: types.BuildTS(1, 0), end: restore})
			if !test.wantFile {
				require.NotContains(t, files, name)
				return
			}
			require.Len(t, files, 1)
			require.Equal(t, test.wantCopy, files[name].NeedCopy)
		})
	}
	t.Run("distinct names with same owner and creation epoch", func(t *testing.T) {
		oldID := objectio.NewObjectid()
		liveID := objectio.NewObjectid()
		oldName := objectio.BuildObjectNameWithObjectID(&oldID)
		liveName := objectio.BuildObjectNameWithObjectID(&liveID)
		location := func(name objectio.ObjectName) objectio.Location {
			return objectio.BuildLocation(name, objectio.NewExtent(0, 0, 1, 1), 1, 0)
		}
		objects := []*objectio.BackupObject{
			{Location: location(oldName), TableID: 1, ObjectType: dataType, CrateTS: create, NeedCopy: true},
			{Location: location(oldName), TableID: 1, ObjectType: dataType, CrateTS: create, DropTS: drop, NeedCopy: true},
			{Location: location(liveName), TableID: 1, ObjectType: dataType, CrateTS: create, NeedCopy: true},
		}
		files := selectBackupObjects(objects, restore, nil, nil,
			&backupObjectRetention{start: types.BuildTS(1, 0), end: restore})
		require.NotContains(t, files, oldName.String())
		require.Contains(t, files, liveName.String())
		require.True(t, files[liveName.String()].NeedCopy)
		require.Len(t, files, 1)
	})
	t.Run("recent incremental retention requires missing source", func(t *testing.T) {
		objectID := objectio.NewObjectid()
		name := objectio.BuildObjectNameWithObjectID(&objectID)
		location := objectio.BuildLocation(name, objectio.NewExtent(0, 0, 1, 1), 1, 0)
		objects := []*objectio.BackupObject{
			{Location: location, TableID: 1, ObjectType: dataType, CrateTS: create, NeedCopy: true},
			{Location: location, TableID: 1, ObjectType: dataType, CrateTS: create, DropTS: drop, NeedCopy: true},
		}
		retention := &backupObjectRetention{
			start:    types.BuildTS(1, 0),
			end:      restore,
			retained: map[objectio.ObjectNameShort]struct{}{*name.Short(): {}},
		}
		require.Contains(t, selectBackupObjects(objects, restore, nil, nil, nil), name.String())
		files := selectBackupObjects(objects, restore, nil, nil, retention)
		require.Contains(t, files, name.String())
		require.True(t, files[name.String()].NeedCopy)
		src := newBackupMemoryFS(t, "retained-missing-src")
		dst := newBackupMemoryFS(t, "retained-missing-dst")
		_, err := parallelCopyData(t.Context(), src, dst, files, 1, nil)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), err)
	})
}

func TestBackupData(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOptsAndQuickGC(nil)
	db := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer db.Close()
	defer opts.Fs.Close(ctx)

	schema := catalog.MockSchemaAll(13, 3)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 10
	db.BindSchema(schema)
	{
		txn, err := db.DB.StartTxn(nil)
		require.NoError(t, err)
		dbH, err := testutil.CreateDatabase2(ctx, txn, "db")
		require.NoError(t, err)
		_, err = testutil.CreateRelation2(ctx, txn, dbH, schema)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctx))
	}

	totalRows := uint64(schema.Extra.BlockMaxRows * 30)
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	bats := bat.Split(100)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)
	defer pool.Release()

	start := time.Now()
	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(testutil.AppendClosure(t, data, schema.Name, db.DB, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	t.Logf("Append %d rows takes: %s", totalRows, time.Since(start))

	deletedRows := 0
	{
		txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, int(totalRows), false)

		obj := testutil.GetOneBlockMeta(rel)
		id := obj.AsCommonID()
		err := rel.RangeDelete(id, 0, 0, handle.DT_Normal)
		require.NoError(t, err)
		deletedRows = 1
		testutil.CompactBlocks(t, 0, db.DB, "db", schema, false)

		assert.NoError(t, txn.Commit(context.Background()))
	}
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	dir := path.Join(db.Dir, "/local")
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer service.Close(ctx)
	for _, data := range bats {
		txn, rel := db.GetRelation()
		v := testutil.GetSingleSortKeyValue(data, schema, 2)
		filter := handle.NewEQFilter(v)
		err := rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err, v)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	backupTime := time.Now().UTC()
	currTs := types.BuildTS(backupTime.UnixNano(), 0)
	locations := make([]string, 0)
	locations = append(locations, backupTime.Format(time.DateTime))
	location, err := db.ForceCheckpointForBackup(ctx, currTs)
	assert.Nil(t, err)
	_, err = db.BGCheckpointRunner.DisableCheckpoint(ctx)
	assert.NoError(t, err)
	locations = append(locations, location)
	checkpoints := db.BGCheckpointRunner.GetAllCheckpoints()
	files := make(map[string]string, 0)
	for _, candidate := range checkpoints {
		if files[candidate.GetLocation().Name().String()] == "" {
			var loc string
			loc = candidate.GetLocation().String()
			loc += ":"
			loc += fmt.Sprintf("%d", candidate.GetVersion())
			files[candidate.GetLocation().Name().String()] = loc
		}
	}
	for _, location := range files {
		locations = append(locations, location)
	}
	fileList := make([]*taeFile, 0)
	err = execBackup(ctx, "", db.Opts.Fs, service, locations, 1, types.TS{}, "full", &fileList, nil, nil)
	assert.Nil(t, err)
	fileMap := make(map[string]struct{})
	for _, file := range fileList {
		_, ok := fileMap[file.path]
		assert.True(t, !ok)
		fileMap[file.path] = struct{}{}
	}
	db.Opts.Fs = service
	db.Restart(ctx)
	txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, int(totalRows-100)-deletedRows, true)
	assert.NoError(t, txn.Commit(context.Background()))

}

func TestBackupData2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	db := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer db.Close()
	defer opts.Fs.Close(ctx)

	schema := catalog.MockSchemaAll(13, 3)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 10
	db.BindSchema(schema)
	{
		txn, err := db.DB.StartTxn(nil)
		require.NoError(t, err)
		dbH, err := testutil.CreateDatabase2(ctx, txn, "db")
		require.NoError(t, err)
		_, err = testutil.CreateRelation2(ctx, txn, dbH, schema)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctx))
	}

	totalRows := uint64(schema.Extra.BlockMaxRows * 30)
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	bats := bat.Split(100)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)
	defer pool.Release()

	start := time.Now()
	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(testutil.AppendClosure(t, data, schema.Name, db.DB, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	opts = config.WithLongScanAndCKPOpts(nil)
	testutils.WaitExpect(5000, func() bool {
		return db.DiskCleaner.GetCleaner().GetScanWaterMark() != nil
	})
	db.Restart(ctx, opts)
	t.Logf("Append %d rows takes: %s", totalRows, time.Since(start))
	deletedRows := 0
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	dir := path.Join(db.Dir, "/local")
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer service.Close(ctx)
	for _, data := range bats {
		txn, rel := db.GetRelation()
		v := testutil.GetSingleSortKeyValue(data, schema, 2)
		filter := handle.NewEQFilter(v)
		err := rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	backupTime := time.Now().UTC()
	currTs := types.BuildTS(backupTime.UnixNano(), 0)
	locations := make([]string, 0)
	locations = append(locations, backupTime.Format(time.DateTime))
	location, err := db.ForceCheckpointForBackup(ctx, currTs)
	assert.Nil(t, err)
	_, err = db.BGCheckpointRunner.DisableCheckpoint(ctx)
	assert.NoError(t, err)
	locations = append(locations, location)
	compacted := db.BGCheckpointRunner.GetCompacted()
	checkpoints := db.BGCheckpointRunner.GetAllCheckpointsForBackup(compacted)
	files := make(map[string]string, 0)
	for _, candidate := range checkpoints {
		if files[candidate.GetLocation().Name().String()] == "" {
			var loc string
			loc = candidate.GetLocation().String()
			loc += ":"
			loc += fmt.Sprintf("%d", candidate.GetVersion())
			files[candidate.GetLocation().Name().String()] = loc
		}
	}
	for _, location := range files {
		locations = append(locations, location)
	}
	fileList := make([]*taeFile, 0)
	err = execBackup(ctx, "", db.Opts.Fs, service, locations, 1, types.TS{}, "full", &fileList, nil, nil)
	assert.Nil(t, err)
	fileMap := make(map[string]struct{})
	for _, file := range fileList {
		_, ok := fileMap[file.path]
		assert.True(t, !ok)
		fileMap[file.path] = struct{}{}
	}
	db.Opts.Fs = service
	db.Restart(ctx)
	txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, int(totalRows-100)-deletedRows, true)
	assert.NoError(t, txn.Commit(context.Background()))

}

func TestBackupData3(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	db := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer db.Close()
	defer opts.Fs.Close(ctx)

	schema := catalog.MockSchemaAll(13, 3)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 10
	db.BindSchema(schema)

	totalRows := 20
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	db.CreateRelAndAppend2(bat, true)
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	dir := path.Join(db.Dir, "/local")
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer service.Close(ctx)
	backupTime := time.Now().UTC()
	currTs := types.BuildTS(backupTime.UnixNano(), 0)
	locations := make([]string, 0)
	locations = append(locations, backupTime.Format(time.DateTime))
	location, err := db.ForceCheckpointForBackup(ctx, currTs)
	assert.Nil(t, err)
	_, err = db.BGCheckpointRunner.DisableCheckpoint(ctx)
	assert.NoError(t, err)
	locations = append(locations, location)
	compacted := db.BGCheckpointRunner.GetCompacted()
	checkpoints := db.BGCheckpointRunner.GetAllCheckpointsForBackup(compacted)
	files := make(map[string]string, 0)
	for _, candidate := range checkpoints {
		if files[candidate.GetLocation().Name().String()] == "" {
			var loc string
			loc = candidate.GetLocation().String()
			loc += ":"
			loc += fmt.Sprintf("%d", candidate.GetVersion())
			files[candidate.GetLocation().Name().String()] = loc
		}
	}
	for _, location := range files {
		locations = append(locations, location)
	}
	fileList := make([]*taeFile, 0)
	err = execBackup(ctx, "", db.Opts.Fs, service, locations, 1, types.TS{}, "full", &fileList, nil, nil)
	assert.Nil(t, err)
	fileMap := make(map[string]struct{})
	for _, file := range fileList {
		_, ok := fileMap[file.path]
		assert.True(t, !ok)
		fileMap[file.path] = struct{}{}
	}
	db.Opts.Fs = service
	db.Restart(ctx)
	t.Log(db.Catalog.SimplePPString(3))
	txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, int(totalRows), true)
	assert.NoError(t, txn.Commit(context.Background()))
	db.MergeBlocks(true)
	db.ForceGlobalCheckpoint(ctx, db.TxnMgr.Now(), time.Second)
	t.Log(db.Catalog.SimplePPString(3))
	db.Restart(ctx)

}

func TestBackupData4(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	fault.Enable()
	defer fault.Disable()
	fault.AddFaultPoint(ctx, "back up UT", ":::", "echo", 0, "debug", false)
	defer fault.RemoveFaultPoint(ctx, "back up UT")

	opts := config.WithLongScanAndCKPOpts(nil)
	db := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer db.Close()
	defer opts.Fs.Close(ctx)

	schema := catalog.MockSchemaAll(13, 3)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 10
	db.BindSchema(schema)

	totalRows := 20
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	db.CreateRelAndAppend2(bat, true)
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	dir := path.Join(db.Dir, "/local")
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer service.Close(ctx)
	backupTime := time.Now().UTC()
	currTs := types.BuildTS(backupTime.UnixNano(), 0)
	locations := make([]string, 0)
	locations = append(locations, backupTime.Format(time.DateTime))
	location, err := db.ForceCheckpointForBackup(ctx, currTs)
	assert.Nil(t, err)
	_, err = db.BGCheckpointRunner.DisableCheckpoint(ctx)
	assert.NoError(t, err)
	locations = append(locations, location)
	compacted := db.BGCheckpointRunner.GetCompacted()
	checkpoints := db.BGCheckpointRunner.GetAllCheckpointsForBackup(compacted)
	files := make(map[string]string, 0)
	for _, candidate := range checkpoints {
		if files[candidate.GetLocation().Name().String()] == "" {
			var loc string
			loc = candidate.GetLocation().String()
			loc += ":"
			loc += fmt.Sprintf("%d", candidate.GetVersion())
			files[candidate.GetLocation().Name().String()] = loc
		}
	}
	for _, location := range files {
		locations = append(locations, location)
	}
	fileList := make([]*taeFile, 0)
	err = execBackup(ctx, "", db.Opts.Fs, service, locations, 1, types.TS{}, "full", &fileList, nil, nil)
	assert.Nil(t, err)
	fileMap := make(map[string]struct{})
	for _, file := range fileList {
		_, ok := fileMap[file.path]
		assert.True(t, !ok)
		fileMap[file.path] = struct{}{}
	}
	db.Opts.Fs = service
	db.Restart(ctx)
	t.Log(db.Catalog.SimplePPString(3))
	txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, int(totalRows), true)
	assert.NoError(t, txn.Commit(context.Background()))
	db.MergeBlocks(true)
	db.ForceGlobalCheckpoint(ctx, db.TxnMgr.Now(), time.Second)
	t.Log(db.Catalog.SimplePPString(3))
	db.Restart(ctx)

}

func TestBackupData5(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOptsAndQuickGC(nil)
	db := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer db.Close()
	defer opts.Fs.Close(ctx)

	schema := catalog.MockSchemaAll(13, 3)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 10
	db.BindSchema(schema)
	{
		txn, err := db.DB.StartTxn(nil)
		require.NoError(t, err)
		dbH, err := testutil.CreateDatabase2(ctx, txn, "db")
		require.NoError(t, err)
		_, err = testutil.CreateRelation2(ctx, txn, dbH, schema)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctx))
	}

	totalRows := uint64(schema.Extra.BlockMaxRows * 30)
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	bats := bat.Split(100)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)
	defer pool.Release()

	start := time.Now()
	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(testutil.AppendClosure(t, data, schema.Name, db.DB, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	t.Logf("Append %d rows takes: %s", totalRows, time.Since(start))

	deletedRows := 0
	{
		txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, int(totalRows), false)

		obj := testutil.GetOneBlockMeta(rel)
		id := obj.AsCommonID()
		err := rel.RangeDelete(id, 0, 0, handle.DT_Normal)
		require.NoError(t, err)
		deletedRows = 1
		testutil.CompactBlocks(t, 0, db.DB, "db", schema, false)

		assert.NoError(t, txn.Commit(context.Background()))
	}
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	dir := path.Join(db.Dir, "/local")
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer service.Close(ctx)
	for _, data := range bats {
		txn, rel := db.GetRelation()
		v := testutil.GetSingleSortKeyValue(data, schema, 2)
		filter := handle.NewEQFilter(v)
		err := rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	backupTime := time.Now().UTC()
	currTs := types.BuildTS(backupTime.UnixNano(), 0)
	locations := make([]string, 0)
	locations = append(locations, backupTime.Format(time.DateTime))
	location, err := db.ForceCheckpointForBackup(ctx, currTs)
	assert.Nil(t, err)
	_, err = db.BGCheckpointRunner.DisableCheckpoint(ctx)
	assert.NoError(t, err)
	locations = append(locations, location)
	checkpoints := db.BGCheckpointRunner.GetAllCheckpoints()
	files := make(map[string]string, 0)
	for _, candidate := range checkpoints {
		if files[candidate.GetLocation().Name().String()] == "" {
			var loc string
			loc = candidate.GetLocation().String()
			loc += ":"
			loc += fmt.Sprintf("%d", candidate.GetVersion())
			files[candidate.GetLocation().Name().String()] = loc
		}
	}
	for _, location := range files {
		locations = append(locations, location)
	}
	fileList := make([]*taeFile, 0)
	dstObj, err := fileservice.SortedList(db.Opts.Fs.List(ctx, ""))
	assert.NoError(t, err)
	data := []byte(dstObj[0].Name)
	data = append(data, []byte("\n")...)
	service.Write(ctx, fileservice.IOVector{
		FilePath: "file_list",
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len(data)),
				Data:   data,
			},
		},
	})
	err = execBackup(ctx, "", db.Opts.Fs, service, locations, 1, types.TS{}, "full", &fileList, nil, nil)
	assert.Nil(t, err)
	fileMap := make(map[string]struct{})
	for _, file := range fileList {
		_, ok := fileMap[file.path]
		assert.True(t, !ok)
		fileMap[file.path] = struct{}{}
	}
	db.Opts.Fs = service
	db.Restart(ctx)
	txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, int(totalRows-100)-deletedRows, true)
	assert.NoError(t, txn.Commit(context.Background()))

}

func Test_saveTaeFilesList(t *testing.T) {
	type args struct {
		ctx        context.Context
		Fs         fileservice.FileService
		taeFiles   []*taeFile
		backupTime string
	}

	Fs := getTestFs(t, true)
	Fs2 := getTestFs(t, true)
	ts := time.Now().Format(time.DateTime)
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:        context.Background(),
				Fs:         Fs,
				taeFiles:   nil,
				backupTime: "",
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return true
			},
		},
		{
			name: "t2",
			args: args{
				ctx:        context.Background(),
				Fs:         Fs,
				taeFiles:   nil,
				backupTime: ts,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				//check file
				check, err2 := readFileAndCheck(context.Background(), Fs, taeList)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte(""))
				check, err2 = readFileAndCheck(context.Background(), Fs, taeSum)
				assert.NoError(t, err2)
				lines, err2 := fromCsvBytes(check)
				assert.NoError(t, err2)
				assert.Equal(t, lines[0][0], ts)
				assert.Equal(t, lines[0][1], "0")
				return false
			},
		},
		{
			name: "t3",
			args: args{
				ctx:        context.Background(),
				Fs:         nil,
				taeFiles:   nil,
				backupTime: "",
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return true
			},
		},
		{
			name: "t4",
			args: args{
				ctx: context.Background(),
				Fs:  Fs2,
				taeFiles: []*taeFile{
					{
						path:     "t1",
						size:     1,
						checksum: []byte{1},
					},
				},
				backupTime: ts,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				//check file
				check, err2 := readFileAndCheck(context.Background(), Fs2, taeList)
				assert.NoError(t, err2)
				lines, err2 := fromCsvBytes(check)
				assert.NoError(t, err2)
				assert.Equal(t, lines[0][0], "t1")
				assert.Equal(t, lines[0][1], "1")
				assert.Equal(t, lines[0][2], hexStr([]byte{1}))
				check, err2 = readFileAndCheck(context.Background(), Fs2, taeSum)
				assert.NoError(t, err2)
				lines, err2 = fromCsvBytes(check)
				assert.NoError(t, err2)
				assert.Equal(t, lines[0][0], ts)
				assert.Equal(t, lines[0][1], "1")
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, saveTaeFilesList(tt.args.ctx, tt.args.Fs, tt.args.taeFiles, tt.args.backupTime, tt.args.backupTime, ""), fmt.Sprintf("saveTaeFilesList(%v, %v, %v, %v)", tt.args.ctx, tt.args.Fs, tt.args.taeFiles, tt.args.backupTime))
		})
	}
}

func Test_saveMetas(t *testing.T) {
	type args struct {
		ctx context.Context
		cfg *Config
	}

	Fs := getTestFs(t, true)

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx: context.Background(),
				cfg: nil,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return false
			},
		},
		{
			name: "t2",
			args: args{
				ctx: context.Background(),
				cfg: &Config{
					Timestamp:  types.TS{},
					GeneralDir: Fs,
					SharedFs:   nil,
					TaeDir:     nil,
					HAkeeper:   nil,
					Metas: &Metas{
						metas: []*Meta{
							{
								Typ:     TypeVersion,
								Version: "version",
							},
							{
								Typ:       TypeBuildinfo,
								Buildinfo: "build_info",
							},
							{
								Typ:              TypeLaunchconfig,
								LaunchConfigFile: "launch_conf",
							},
							{
								Typ:              TypeLaunchconfig,
								SubTyp:           CnConfig,
								LaunchConfigFile: "launch_cn_conf",
							},
						},
					},
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				check, err2 := readFileAndCheck(context.Background(), Fs, moMeta)
				assert.NoError(t, err2)
				lines, err2 := fromCsvBytes(check)
				assert.NoError(t, err2)
				assert.Equal(t, lines[0][0], "version")
				assert.Equal(t, lines[0][1], "version")
				assert.Equal(t, lines[1][0], "buildinfo")
				assert.Equal(t, lines[1][1], "build_info")
				assert.Equal(t, lines[2][0], "launchconfig")
				assert.Equal(t, lines[2][1], "")
				assert.Equal(t, lines[2][2], "launch_conf")
				assert.Equal(t, lines[3][0], "launchconfig")
				assert.Equal(t, lines[3][1], CnConfig)
				assert.Equal(t, lines[3][2], "launch_cn_conf")
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, saveMetas(tt.args.ctx, tt.args.cfg), fmt.Sprintf("saveMetas(%v, %v)", tt.args.ctx, tt.args.cfg))
		})
	}
}

func Test_backupConfigFile(t *testing.T) {
	type args struct {
		ctx        context.Context
		typ        string
		configPath string
		cfg        *Config
	}

	Fs := getTestFs(t, true)
	file := getTempFile(t, "", "t1", "test_t1")

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:        context.Background(),
				typ:        "",
				configPath: "",
				cfg:        nil,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return true
			},
		},
		{
			name: "t2",
			args: args{
				ctx:        context.Background(),
				typ:        CnConfig,
				configPath: file.Name(),
				cfg: &Config{
					Timestamp:  types.TS{},
					GeneralDir: Fs,
					SharedFs:   nil,
					TaeDir:     nil,
					HAkeeper:   nil,
					Metas:      &Metas{},
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				list, err2 := fileservice.SortedList(Fs.List(context.Background(), configDir))
				assert.NoError(t, err2)
				var configFile string
				for _, entry := range list {
					if entry.IsDir {
						continue
					}
					configFile = entry.Name
					break
				}
				check, err2 := readFileAndCheck(context.Background(), Fs, configDir+"/"+configFile)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte("test_t1"))
				return true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, backupConfigFile(tt.args.ctx, tt.args.typ, tt.args.configPath, tt.args.cfg), fmt.Sprintf("backupConfigFile(%v, %v, %v, %v)", tt.args.ctx, tt.args.typ, tt.args.configPath, tt.args.cfg))
		})
	}
}

var _ logservice.BRHAKeeperClient = new(dumpHakeeper)

const (
	backupData = "backup_data"
)

type dumpHakeeper struct {
}

func (d *dumpHakeeper) Close() error {
	//TODO implement me
	panic("implement me")
}

func (d *dumpHakeeper) GetBackupData(ctx context.Context) ([]byte, error) {
	return []byte(backupData), nil
}

func Test_backupHakeeper(t *testing.T) {
	type args struct {
		ctx    context.Context
		config *Config
	}
	etlFs := getTestFs(t, true)
	taeFs := getTestFs(t, false)

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:    context.Background(),
				config: nil,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return false
			},
		},
		{
			name: "t2",
			args: args{
				ctx: context.Background(),
				config: &Config{
					Timestamp:  types.TS{},
					GeneralDir: nil,
					SharedFs:   nil,
					TaeDir:     nil,
					HAkeeper:   nil,
					Metas:      &Metas{},
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return false
			},
		},
		{
			name: "t3",
			args: args{
				ctx: context.Background(),
				config: &Config{
					Timestamp:  types.TS{},
					GeneralDir: etlFs,
					SharedFs:   nil,
					TaeDir:     etlFs,
					HAkeeper:   &dumpHakeeper{},
					Metas:      &Metas{},
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				check, err2 := readFileAndCheck(context.Background(), etlFs, hakeeperDir+"/"+HakeeperFile)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte(backupData))
				return false
			},
		},
		{
			name: "t4",
			args: args{
				ctx: context.Background(),
				config: &Config{
					Timestamp:  types.TS{},
					GeneralDir: etlFs,
					SharedFs:   nil,
					TaeDir:     taeFs,
					HAkeeper:   &dumpHakeeper{},
					Metas:      &Metas{},
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				check, err2 := readFileAndCheck(context.Background(), taeFs, hakeeperDir+"/"+HakeeperFile)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte(backupData))
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, backupHakeeper(tt.args.ctx, tt.args.config), fmt.Sprintf("backupHakeeper(%v, %v)", tt.args.ctx, tt.args.config))
		})
	}
}

func TestBackup(t *testing.T) {
	type args struct {
		ctx context.Context
		bs  *tree.BackupStart
		cfg *Config
	}

	stubs := gostub.StubFunc(&backupTae, nil)
	defer stubs.Reset()

	tDir := getTempDir(t, "test")
	tf1 := getTempFile(t, "", "t1", "test_t1")

	bs := &tree.BackupStart{
		IsS3:        false,
		Dir:         tDir,
		Parallelism: "10",
	}

	//backup configs
	SaveLaunchConfigPath(CnConfig, []string{tf1.Name()})

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx: nil,
				bs:  bs,
				cfg: nil,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return false
			},
		},
		{
			name: "t2",
			args: args{
				ctx: context.Background(),
				bs:  bs,
				cfg: &Config{
					Timestamp:  types.TS{},
					GeneralDir: nil,
					SharedFs:   nil,
					TaeDir:     nil,
					HAkeeper:   &dumpHakeeper{},
					Metas:      NewMetas(),
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				cfg := i[0].(*Config)
				assert.NoError(t, err)
				assert.NotNil(t, cfg)

				//checkup config files
				list, err2 := fileservice.SortedList(cfg.GeneralDir.List(context.Background(), configDir))
				assert.NoError(t, err2)
				var configFile string
				for _, entry := range list {
					if entry.IsDir {
						continue
					}
					configFile = entry.Name
					break
				}
				check, err2 := readFileAndCheck(context.Background(), cfg.GeneralDir, configDir+"/"+configFile)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte("test_t1"))

				//check hakeeper files
				check, err2 = readFileAndCheck(context.Background(), cfg.TaeDir, hakeeperDir+"/"+HakeeperFile)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte(backupData))

				//check metas
				check, err2 = readFileAndCheck(context.Background(), cfg.GeneralDir, moMeta)
				assert.NoError(t, err2)
				lines, err2 := fromCsvBytes(check)
				assert.NoError(t, err2)
				assert.Equal(t, lines[0][0], "version")
				assert.Equal(t, lines[0][1], Version)
				assert.Equal(t, lines[1][0], "buildinfo")
				assert.Equal(t, lines[1][1], buildInfo())
				assert.Equal(t, lines[2][0], "launchconfig")
				assert.Equal(t, lines[2][1], CnConfig)
				assert.Equal(t, lines[2][2], cfg.Metas.metas[2].LaunchConfigFile)
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime.RunTest(
				"",
				func(rt runtime.Runtime) {
					tt.wantErr(t, Backup(tt.args.ctx, "", tt.args.bs, tt.args.cfg), tt.args.cfg, fmt.Sprintf("Backup(%v, %v, %v)", tt.args.ctx, tt.args.bs, tt.args.cfg))
				},
			)
		})
	}
}
