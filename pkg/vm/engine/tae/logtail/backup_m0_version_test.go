//go:build mo_m0

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

package logtail

import (
	"context"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
)

type m0BackupReadFailure struct {
	fileservice.FileService
	name         string
	metadataOnly bool
}

type m0BackupOutputFailure struct {
	fileservice.FileService
	paths     []string
	failAt    int
	cancel    context.CancelFunc
	cancelAt  int
	deleteErr error
	collide   bool
	panicAt   int
}

func (fs *m0BackupOutputFailure) Write(ctx context.Context, v fileservice.IOVector) error {
	if fs.collide {
		sentinel := []byte("other backup")
		if err := fs.FileService.Write(ctx, fileservice.IOVector{FilePath: v.FilePath, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(sentinel)), Data: sentinel}}}); err != nil {
			return err
		}
		fs.paths = append(fs.paths, v.FilePath)
		return moerr.NewFileAlreadyExistsNoCtx(v.FilePath)
	}
	if err := fs.FileService.Write(ctx, v); err != nil {
		return err
	}
	fs.paths = append(fs.paths, v.FilePath)
	if len(fs.paths) == fs.panicAt {
		panic("injected backup write panic")
	}
	if fs.cancel != nil && len(fs.paths) == fs.cancelAt {
		fs.cancel()
	}
	if len(fs.paths) == fs.failAt {
		return errors.New("injected later backup write failure")
	}
	return nil
}

func (fs *m0BackupOutputFailure) Delete(ctx context.Context, paths ...string) error {
	if fs.deleteErr != nil {
		return fs.deleteErr
	}
	return fs.FileService.Delete(ctx, paths...)
}

func (fs *m0BackupReadFailure) Read(ctx context.Context, v *fileservice.IOVector) error {
	if v.FilePath == fs.name && len(v.Entries) > 0 && (!fs.metadataOnly || v.Entries[0].Offset > 0) {
		return errors.New("injected backup source read failure")
	}
	return fs.FileService.Read(ctx, v)
}

// This fixture exercises real checkpoint/object rewriting with an explicitly
// constructed version-one appendable object. It does not claim that SQL or a
// particular supported backup deployment created the source object.
func TestM0BackupRewritePreservesAppendableObjectVersion(t *testing.T) {
	testBackupRewriteObjectVersion(t, 1)
}

func TestM0BackupRewriteVersionZero(t *testing.T) {
	testBackupRewriteObjectVersion(t, 0)
}

func testBackupRewriteObjectVersion(t *testing.T, sourceVersion uint32) {
	t.Helper()
	ctx := context.Background()
	srcFS, dstFS := testutil.NewSharedFS(), testutil.NewSharedFS()
	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_TS.ToType())
	defer bat.Clean(common.DebugAllocator)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(7), false, common.DebugAllocator))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("key"), false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(9), false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], types.BuildTS(10, 0), false, common.DebugAllocator))
	// A later committed row must not survive the backup snapshot.
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(8), false, common.DebugAllocator))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("later"), false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(10), false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], types.BuildTS(20, 0), false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(11), false, common.DebugAllocator))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("middle"), false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(12), false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], types.BuildTS(11, 0), false, common.DebugAllocator))
	bat.SetRowCount(3)

	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	w, err := ioutil.NewBlockWriterNew(srcFS, name, sourceVersion, []uint16{0, 1, 2, objectio.SEQNUM_COMMITTS}, false)
	require.NoError(t, err)
	w.SetPrimaryKey(1)
	w.SetAppendable()
	_, err = w.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = w.Sync(ctx)
	require.NoError(t, err)
	stats := w.GetObjectStats(objectio.WithAppendable())
	if sourceVersion != 0 {
		segment := name.SegmentId()
		destination := objectio.BuildObjectName(&segment, 777)
		writer, _, readErr := newM0BackupDataWriter(ctx, &m0BackupReadFailure{FileService: srcFS, name: name.String()}, dstFS, destination, &stats)
		require.Nil(t, writer)
		require.ErrorContains(t, readErr, "injected backup source read failure")
		writer, _, readErr = newM0BackupDataWriter(ctx, &m0BackupReadFailure{FileService: srcFS, name: name.String(), metadataOnly: true}, dstFS, destination, &stats)
		require.Nil(t, writer)
		require.ErrorContains(t, readErr, "injected backup source read failure")
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		writer, _, readErr = newM0BackupDataWriter(cancelCtx, srcFS, dstFS, destination, &stats)
		require.Nil(t, writer)
		require.ErrorIs(t, readErr, context.Canceled)
		// An appendable source without a commit timestamp cannot be safely
		// rewritten at a backup snapshot, even when its header has a version.
		invalidID := objectio.NewObjectid()
		invalidName := objectio.BuildObjectNameWithObjectID(&invalidID)
		invalidWriter, invalidErr := ioutil.NewBlockWriterNew(srcFS, invalidName, sourceVersion, []uint16{0, 1, 2}, false)
		require.NoError(t, invalidErr)
		invalidWriter.SetAppendable()
		invalidBatch := batch.NewWithSize(3)
		copy(invalidBatch.Vecs, bat.Vecs[:3]) // borrowed vectors; bat owns them
		invalidBatch.SetRowCount(bat.RowCount())
		_, invalidErr = invalidWriter.WriteBatch(invalidBatch)
		require.NoError(t, invalidErr)
		_, _, invalidErr = invalidWriter.Sync(ctx)
		require.NoError(t, invalidErr)
		invalidStats := invalidWriter.GetObjectStats(objectio.WithAppendable())
		writer, _, invalidErr = newM0BackupDataWriter(ctx, srcFS, dstFS, destination, &invalidStats)
		require.Nil(t, writer)
		require.ErrorContains(t, invalidErr, "no commit timestamp")
	}
	layout := m0read.M0AppendLayout{
		Version: 2, NextColSeqnum: 4, PKSeqnum: 1,
		CompositePK: true, HiddenPK: true, SingleSortKey: true,
		Columns: []m0read.M0AppendColumn{
			{Seqnum: 0, Type: types.T_int64},
			{Seqnum: 1, Type: types.T_varchar, Hidden: true, Primary: true, SortKey: true},
			{Seqnum: 2, Type: types.T_int64, Nullable: true},
			{Seqnum: 3, Type: types.T_varchar, Nullable: true},
		},
	}
	readHeader := func(fs fileservice.FileService, object string) objectio.Header {
		r, e := objectio.NewObjectReaderWithStr(object, fs)
		require.NoError(t, e)
		header, e := r.ReadHeader(ctx, common.DebugAllocator)
		require.NoError(t, e)
		return header
	}
	sourceHeader := readHeader(srcFS, name.String())
	sourceLocation := stats.ObjectLocation()
	sourceMeta, err := objectio.FastLoadObjectMeta(ctx, &sourceLocation, false, srcFS)
	require.NoError(t, err)
	if sourceVersion != 0 {
		absent, err := m0read.ProveM0AbsentColumn(layout, sourceHeader, sourceMeta.MustDataMeta(), 3)
		require.NoError(t, err)
		require.True(t, absent)
	}

	cata := catalog.MockCatalog(nil)
	defer cata.Close()
	db, err := cata.CreateDBEntry("m0_backup", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(3, 1), nil, nil)
	require.NoError(t, err)
	createTS, backupTS, dropTS := types.BuildTS(5, 0), types.BuildTS(15, 0), types.BuildTS(30, 0)
	entry, err := table.CreateCommittedObject(createTS, &objectio.CreateObjOpt{Stats: &stats}, nil)
	require.NoError(t, err)
	catalog.MockDroppedObjectEntry2List(entry, dropTS)
	// A committed tombstone for the first source row must be applied during the
	// rewrite; retaining only the later visible row also tests source row IDs.
	tomb := batch.NewWithSize(3)
	tomb.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tomb.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	tomb.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	defer tomb.Clean(common.DebugAllocator)
	sourceBlockID := stats.ConstructBlockId(0)
	require.NoError(t, vector.AppendFixed(tomb.Vecs[0], types.NewRowid(&sourceBlockID, 0), false, common.DebugAllocator))
	require.NoError(t, vector.AppendBytes(tomb.Vecs[1], []byte("key"), false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(tomb.Vecs[2], types.BuildTS(12, 0), false, common.DebugAllocator))
	tomb.SetRowCount(1)
	tombID := objectio.NewObjectid()
	tombName := objectio.BuildObjectNameWithObjectID(&tombID)
	tombWriter, err := ioutil.NewBlockWriterNew(srcFS, tombName, 0, []uint16{0, 1, objectio.SEQNUM_COMMITTS}, true)
	require.NoError(t, err)
	tombWriter.SetAppendable()
	_, err = tombWriter.WriteBatch(tomb)
	require.NoError(t, err)
	_, _, err = tombWriter.Sync(ctx)
	require.NoError(t, err)
	tombStats := tombWriter.GetObjectStats(objectio.WithAppendable())
	tombEntry, err := table.CreateCommittedObject(createTS, &objectio.CreateObjOpt{Stats: &tombStats, IsTombstone: true}, nil)
	require.NoError(t, err)
	catalog.MockDroppedObjectEntry2List(tombEntry, dropTS)
	collector := NewBaseCollector_V2(backupTS, dropTS, 0, srcFS)
	defer collector.Close()
	require.NoError(t, collector.Collect(cata))
	checkpoint := collector.OrphanData()
	defer checkpoint.Close()
	location, _, err := checkpoint.Sync(ctx, srcFS)
	require.NoError(t, err)
	last, err := GetCheckpointReader(ctx, "m0-backup", srcFS, location, CheckpointCurrentVersion)
	require.NoError(t, err)
	if sourceVersion != 0 {
		failingBase, e := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
		require.NoError(t, e)
		defer failingBase.Close(ctx)
		segment := name.SegmentId()
		legacyName := objectio.BuildObjectName(&segment, uint16(1000)+name.Num()).String()
		keep := []byte("another backup")
		require.NoError(t, failingBase.Write(ctx, fileservice.IOVector{FilePath: legacyName, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(keep)), Data: keep}}}))
		failingDst := &persistThenErrorBackupFileService{FileService: failingBase, failOn: 1}
		_, _, _, e = ReWriteCheckpointAndBlockFromKey(ctx, "m0-backup", srcFS, failingDst, location, last, CheckpointCurrentVersion, backupTS)
		require.ErrorContains(t, e, "injected post-persist write failure")
		require.NotEmpty(t, failingDst.persisted)
		require.NotEqual(t, legacyName, failingDst.persisted)
		_, e = failingBase.StatFile(ctx, failingDst.persisted)
		require.True(t, moerr.IsMoErrCode(e, moerr.ErrFileNotFound), "failed rewrite left an unpublished object")
		kept, e := failingBase.StatFile(ctx, legacyName)
		require.NoError(t, e)
		require.Equal(t, int64(len(keep)), kept.Size)
		// An error after the data output succeeded must also retire that
		// earlier, uniquely owned output.
		for _, failAt := range []int{2, 3} {
			t.Run(fmt.Sprintf("failure_after_%d_writes", failAt), func(t *testing.T) {
				lateBase, e := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
				require.NoError(t, e)
				defer lateBase.Close(ctx)
				tombSegment := tombName.SegmentId()
				legacyTomb := objectio.BuildObjectName(&tombSegment, uint16(1000)+tombName.Num()).String()
				require.NoError(t, lateBase.Write(ctx, fileservice.IOVector{FilePath: legacyTomb, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(keep)), Data: keep}}}))
				lateDst := &m0BackupOutputFailure{FileService: lateBase, failAt: failAt}
				_, _, _, e = ReWriteCheckpointAndBlockFromKey(ctx, "m0-backup", srcFS, lateDst, location, last, CheckpointCurrentVersion, backupTS)
				require.ErrorContains(t, e, "injected later backup write failure")
				require.GreaterOrEqual(t, len(lateDst.paths), failAt)
				for i, path := range lateDst.paths {
					_, e = lateBase.StatFile(ctx, path)
					require.True(t, moerr.IsMoErrCode(e, moerr.ErrFileNotFound), "failed rewrite left output %d", i)
				}
				kept, e := lateBase.StatFile(ctx, legacyTomb)
				require.NoError(t, e)
				require.Equal(t, int64(len(keep)), kept.Size)
			})
		}
		// Cancellation immediately after the first persisted output cannot
		// prevent its cleanup.
		cancelBase, e := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
		require.NoError(t, e)
		defer cancelBase.Close(ctx)
		cancelCtx, stop := context.WithCancel(ctx)
		defer stop()
		cancelDst := &m0BackupOutputFailure{FileService: cancelBase, cancel: stop, cancelAt: 1}
		_, _, _, e = ReWriteCheckpointAndBlockFromKey(cancelCtx, "m0-backup", srcFS, cancelDst, location, last, CheckpointCurrentVersion, backupTS)
		require.ErrorIs(t, e, context.Canceled)
		require.NotEmpty(t, cancelDst.paths)
		_, e = cancelBase.StatFile(ctx, cancelDst.paths[0])
		require.True(t, moerr.IsMoErrCode(e, moerr.ErrFileNotFound), "canceled rewrite left its data output")
		lateCancelBase, e := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
		require.NoError(t, e)
		defer lateCancelBase.Close(ctx)
		lateCancelCtx, lateStop := context.WithCancel(ctx)
		defer lateStop()
		lateCancelDst := &m0BackupOutputFailure{FileService: lateCancelBase, cancel: lateStop, cancelAt: 3}
		_, _, _, e = ReWriteCheckpointAndBlockFromKey(lateCancelCtx, "m0-backup", srcFS, lateCancelDst, location, last, CheckpointCurrentVersion, backupTS)
		require.ErrorIs(t, e, context.Canceled)
		require.GreaterOrEqual(t, len(lateCancelDst.paths), 3)
		for i, path := range lateCancelDst.paths {
			_, e = lateCancelBase.StatFile(ctx, path)
			require.True(t, moerr.IsMoErrCode(e, moerr.ErrFileNotFound), "canceled rewrite left output %d", i)
		}
		// A cleanup failure preserves both the original failure and the exact
		// output identity needed for a later owner-directed cleanup.
		cleanupBase, e := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
		require.NoError(t, e)
		defer cleanupBase.Close(ctx)
		cleanupDst := &m0BackupOutputFailure{FileService: cleanupBase, failAt: 1, deleteErr: errors.New("injected cleanup failure")}
		_, _, _, e = ReWriteCheckpointAndBlockFromKey(ctx, "m0-backup", srcFS, cleanupDst, location, last, CheckpointCurrentVersion, backupTS)
		require.ErrorContains(t, e, "injected later backup write failure")
		require.ErrorContains(t, e, "injected cleanup failure")
		require.Len(t, cleanupDst.paths, 1)
		require.ErrorContains(t, e, cleanupDst.paths[0])
		_, e = cleanupBase.StatFile(ctx, cleanupDst.paths[0])
		require.NoError(t, e)
		// If the target name is already owned, never delete that other file.
		collisionBase, e := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
		require.NoError(t, e)
		defer collisionBase.Close(ctx)
		collisionDst := &m0BackupOutputFailure{FileService: collisionBase, collide: true}
		_, _, _, e = ReWriteCheckpointAndBlockFromKey(ctx, "m0-backup", srcFS, collisionDst, location, last, CheckpointCurrentVersion, backupTS)
		require.True(t, moerr.IsMoErrCode(e, moerr.ErrFileAlreadyExists))
		require.NotEmpty(t, collisionDst.paths)
		for _, path := range collisionDst.paths {
			require.Equal(t, collisionDst.paths[0], path)
		}
		preserved, e := collisionBase.StatFile(ctx, collisionDst.paths[0])
		require.NoError(t, e)
		require.Equal(t, int64(len("other backup")), preserved.Size)
		panicBase, e := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
		require.NoError(t, e)
		defer panicBase.Close(ctx)
		panicDst := &m0BackupOutputFailure{FileService: panicBase, panicAt: 1}
		require.Panics(t, func() {
			_, _, _, _ = ReWriteCheckpointAndBlockFromKey(ctx, "m0-backup", srcFS, panicDst, location, last, CheckpointCurrentVersion, backupTS)
		})
		require.NotEmpty(t, panicDst.paths)
		_, e = panicBase.StatFile(ctx, panicDst.paths[0])
		require.True(t, moerr.IsMoErrCode(e, moerr.ErrFileNotFound), "panicked rewrite left its data output")
		panicCleanupBase, e := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
		require.NoError(t, e)
		defer panicCleanupBase.Close(ctx)
		panicCleanupDst := &m0BackupOutputFailure{FileService: panicCleanupBase, panicAt: 1, deleteErr: errors.New("injected panic cleanup failure")}
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_, _, _, _ = ReWriteCheckpointAndBlockFromKey(ctx, "m0-backup", srcFS, panicCleanupDst, location, last, CheckpointCurrentVersion, backupTS)
		}()
		require.NotNil(t, recovered)
		panicErr, ok := recovered.(error)
		require.True(t, ok)
		require.ErrorContains(t, panicErr, "injected backup write panic")
		require.ErrorContains(t, panicErr, "injected panic cleanup failure")
		require.Len(t, panicCleanupDst.paths, 1)
		require.ErrorContains(t, panicErr, panicCleanupDst.paths[0])

		// A later panic must also settle the data object written by the
		// previous iteration, not just the object whose write panicked.
		for _, cleanupFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("panic_after_two_writes_cleanup_fails_%t", cleanupFails), func(t *testing.T) {
				base, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
				require.NoError(t, err)
				t.Cleanup(func() { base.Close(ctx) })
				dst := &m0BackupOutputFailure{FileService: base, panicAt: 2}
				if cleanupFails {
					dst.deleteErr = errors.New("injected multi-object cleanup failure")
				}
				var got any
				func() {
					defer func() { got = recover() }()
					_, _, _, _ = ReWriteCheckpointAndBlockFromKey(ctx, "m0-backup", srcFS, dst, location, last, CheckpointCurrentVersion, backupTS)
				}()
				require.Len(t, dst.paths, 2)
				if cleanupFails {
					panicErr, ok := got.(error)
					require.True(t, ok, "cleanup failure must retain the panic as an error")
					require.ErrorContains(t, panicErr, "injected backup write panic")
					require.ErrorContains(t, panicErr, "injected multi-object cleanup failure")
					for _, path := range dst.paths {
						require.ErrorContains(t, panicErr, path)
						_, err = base.StatFile(ctx, path)
						require.NoError(t, err, "failed cleanup must retain the object for its next owner")
					}
				} else {
					require.Equal(t, "injected backup write panic", got)
					for _, path := range dst.paths {
						_, err = base.StatFile(ctx, path)
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "panic cleanup left output %s", path)
					}
				}
			})
		}
	}
	rewritten, _, _, err := ReWriteCheckpointAndBlockFromKey(ctx, "m0-backup", srcFS, dstFS, location, last, CheckpointCurrentVersion, backupTS)
	require.NoError(t, err)
	reader, err := GetCheckpointReader(ctx, "m0-backup", dstFS, rewritten, CheckpointCurrentVersion)
	require.NoError(t, err)
	var result objectio.ObjectStats
	err = reader.ForEachRow(ctx, func(_ uint32, _, tid uint64, typ int8, s objectio.ObjectStats, _ types.TS, deleted types.TS, _ types.Rowid) error {
		if tid == table.ID && typ == ckputil.ObjectType_Data && deleted.IsEmpty() && s.ObjectName().String() != name.String() {
			if !result.IsZero() {
				return errors.New("unexpected second rewritten data object")
			}
			result = s
		}
		return nil
	})
	require.NoError(t, err)
	require.False(t, result.IsZero())
	resultHeader := readHeader(dstFS, result.ObjectName().String())
	actualSourceVersion, err := objectio.ObjectSchemaVersion(sourceHeader)
	require.NoError(t, err)
	resultVersion, err := objectio.ObjectSchemaVersion(resultHeader)
	require.NoError(t, err)
	require.Equal(t, sourceVersion, actualSourceVersion)
	require.Equal(t, sourceVersion, resultVersion)
	objectLocation := result.ObjectLocation()
	meta, err := objectio.FastLoadObjectMeta(ctx, &objectLocation, false, dstFS)
	require.NoError(t, err)
	block := meta.MustDataMeta().GetBlockMeta(uint32(objectLocation.ID()))
	require.Equal(t, uint16(2), block.GetMaxSeqnum())
	require.Equal(t, uint8(types.T_varchar), block.MustGetColumn(1).DataType())
	expectedBlockID := result.ConstructBlockId(0)
	require.Equal(t, expectedBlockID, *block.GetBlockID(result.ObjectName()))
	if sourceVersion != 0 {
		metadataID := result.ConstructBlockId(1) // object metadata uses block count as its sequence
		require.Equal(t, metadataID, *meta.MustDataMeta().BlockHeader().BlockID())
		absent, err := m0read.ProveM0AbsentColumn(layout, resultHeader, meta.MustDataMeta(), 3)
		require.NoError(t, err)
		require.True(t, absent)
	}
	contents, _, err := ioutil.LoadOneBlockWithIndex(ctx, dstFS, []uint16{0, 1, 2}, objectLocation, objectio.SchemaData)
	require.NoError(t, err)
	defer contents.Clean(common.DebugAllocator)
	require.Equal(t, 1, contents.RowCount())
	require.Equal(t, int64(11), vector.GetFixedAtWithTypeCheck[int64](contents.Vecs[0], 0))
	require.Equal(t, "middle", contents.Vecs[1].GetStringAt(0))
	require.Equal(t, int64(12), vector.GetFixedAtWithTypeCheck[int64](contents.Vecs[2], 0))
}
