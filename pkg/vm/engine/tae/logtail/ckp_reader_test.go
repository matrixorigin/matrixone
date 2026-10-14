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

package logtail

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/stretchr/testify/require"
)

type trackingDeleteFileService struct {
	fileservice.FileService
	err     error
	deleted []string
	batches [][]string
}

func (fs *trackingDeleteFileService) Delete(
	ctx context.Context,
	files ...string,
) error {
	fs.deleted = append(fs.deleted, files...)
	fs.batches = append(fs.batches, append([]string(nil), files...))
	if fs.err != nil {
		return fs.err
	}
	return fs.FileService.Delete(ctx, files...)
}

func TestDeleteUnpublishedObjectsUsesBoundedBatches(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(),
		defines.SharedFileServiceName,
	)
	require.NoError(t, err)
	trackingFS := &trackingDeleteFileService{FileService: fs}

	files := make([]string, 1001, 1003)
	for i := range files {
		files[i] = fmt.Sprintf("unpublished-%d", i)
	}
	files = append(files, "", files[0])
	count, err := ioutil.DeleteUnpublishedObjects(
		context.Background(), trackingFS, files...)
	require.NoError(t, err)
	require.Equal(t, 1001, count)
	require.Len(t, trackingFS.batches, 2)
	require.Len(t, trackingFS.batches[0], 1000)
	require.Len(t, trackingFS.batches[1], 1)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	count, err = ioutil.DeleteUnpublishedObjects(
		canceledCtx, trackingFS, "canceled-cleanup")
	require.Equal(t, 1, count)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDeleteUnpublishedObjectsIsIdempotent(t *testing.T) {
	trackingFS := &trackingDeleteFileService{
		err: moerr.NewFileNotFoundNoCtx("already-deleted-object"),
	}

	count, err := ioutil.DeleteUnpublishedObjects(
		context.Background(), trackingFS, "already-deleted-object")

	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, []string{"already-deleted-object"}, trackingFS.deleted)
}

func TestConsumeCheckpointWithTableID(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(),
		defines.SharedFileServiceName,
	)
	require.NoError(t, err)

	dataRanges, tombstoneRanges := makeCheckpointObjectRanges(t, proc.Mp(), fs)
	var dataEntries, tombstoneEntries int
	err = consumeCheckpointWithTableID(
		context.Background(),
		func(
			_ context.Context,
			_ fileservice.FileService,
			_ objectio.ObjectEntry,
			isTombstone bool,
		) error {
			if isTombstone {
				tombstoneEntries++
			} else {
				dataEntries++
			}
			return nil
		},
		dataRanges,
		tombstoneRanges,
		1,
		proc.Mp(),
		fs,
	)
	require.NoError(t, err)
	require.Equal(t, 1, dataEntries)
	require.Equal(t, 1, tombstoneEntries)

	meta := ckputil.NewMetaBatch()
	defer meta.Clean(proc.Mp())
	for _, ranges := range [][]ckputil.TableRange{dataRanges, tombstoneRanges} {
		for _, r := range ranges {
			require.NoError(t, r.AppendTo(meta, proc.Mp()))
		}
	}
	writer := ioutil.ConstructWriter(0, ckputil.MetaSeqnums, -1, false, false, fs)
	_, err = writer.WriteBatch(meta)
	require.NoError(t, err)
	_, _, err = writer.Sync(context.Background())
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	location := stats.ObjectLocation()
	dataEntries, tombstoneEntries = 0, 0
	reader := NewCKPReaderWithTableID_V2(CheckpointCurrentVersion, location, 1, proc.Mp(), fs)
	require.NoError(t, reader.ReadMeta(context.Background()))
	err = reader.ConsumeCheckpointWithTableID(
		context.Background(),
		func(_ context.Context, _ fileservice.FileService, _ objectio.ObjectEntry, isTombstone bool) error {
			if isTombstone {
				tombstoneEntries++
			} else {
				dataEntries++
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, dataEntries)
	require.Equal(t, 1, tombstoneEntries)
}

func TestConsumeCheckpointWithTableIDPropagatesIteratorError(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(),
		defines.SharedFileServiceName,
	)
	require.NoError(t, err)

	stats := objectio.NewObjectStats()
	name := objectio.BuildObjectName(&types.Uuid{1}, 0)
	location := objectio.BuildLocation(
		name,
		objectio.NewExtent(0, 0, 1, 1),
		1,
		0,
	)
	require.NoError(t, objectio.SetObjectStatsLocation(stats, location))

	ranges := []ckputil.TableRange{{
		TableID:     1,
		ObjectType:  ckputil.ObjectType_Data,
		ObjectStats: *stats,
	}}

	for _, test := range []struct {
		name            string
		dataRanges      []ckputil.TableRange
		tombstoneRanges []ckputil.TableRange
	}{
		{
			name:       "data",
			dataRanges: ranges,
		},
		{
			name:            "tombstone",
			tombstoneRanges: ranges,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithDeadline(
				context.Background(),
				time.Now().Add(-time.Second),
			)
			defer cancel()

			called := false
			err := consumeCheckpointWithTableID(
				ctx,
				func(
					context.Context,
					fileservice.FileService,
					objectio.ObjectEntry,
					bool,
				) error {
					called = true
					return nil
				},
				test.dataRanges,
				test.tombstoneRanges,
				1,
				proc.Mp(),
				fs,
			)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.False(t, called)
		})
	}
}

func TestSyncTableIDBatchDoesNotClaimHistoryAcrossGap(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(),
		defines.SharedFileServiceName,
	)
	require.NoError(t, err)

	ctx := context.Background()
	previousStart := types.BuildTS(time.Now().UnixNano(), 0)
	previousEnd := types.BuildTS(previousStart.Physical()+time.Second.Nanoseconds(), 0)
	previous, err := MockTableIDBatch(
		ctx,
		previousStart,
		previousEnd,
		64,
		1,
		proc.Mp(),
		fs,
	)
	require.NoError(t, err)

	currentStart := types.BuildTS(previousEnd.Physical()+time.Second.Nanoseconds(), 0)
	currentEnd := types.BuildTS(currentStart.Physical()+time.Second.Nanoseconds(), 0)
	locations, err := SyncTableIDBatch(
		ctx,
		currentStart,
		currentEnd,
		24*time.Hour,
		64,
		objectio.Location{},
		0,
		previous,
		proc.Mp(),
		fs,
	)
	require.NoError(t, err)

	historyStart, historyEnd, known, err := ReadTableIDHistoryRange(
		ctx,
		locations,
		proc.Mp(),
		fs,
	)
	require.NoError(t, err)
	// There is no current checkpoint payload in this merge. Once the previous
	// range is discontinuous, emitting a marker for currentStart-currentEnd
	// would claim history backed by neither input.
	require.False(t, known)
	require.True(t, historyStart.IsEmpty())
	require.True(t, historyEnd.IsEmpty())
}

func TestSyncTableIDBatchValidatesPredecessorInSinglePass(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(),
		defines.SharedFileServiceName,
	)
	require.NoError(t, err)

	ctx := context.Background()
	previousStart := types.BuildTS(time.Now().UnixNano()-time.Hour.Nanoseconds(), 0)
	previousEnd := types.BuildTS(previousStart.Physical()+time.Minute.Nanoseconds(), 0)
	previous, err := MockTableIDBatch(
		ctx,
		previousStart,
		previousEnd,
		64,
		BatchRowCountThreshold+17,
		proc.Mp(),
		fs,
	)
	require.NoError(t, err)

	locations, historyStart, historyEnd, known, err := SyncTableIDBatchWithHistory(
		ctx,
		types.TS{},
		previousEnd.Next(),
		24*time.Hour,
		64,
		objectio.Location{},
		0,
		previous,
		previousEnd,
		proc.Mp(),
		fs,
	)
	require.NoError(t, err)
	require.NotEmpty(t, locations)
	require.True(t, known)
	require.Equal(t, previousStart, historyStart)
	require.Equal(t, previousEnd, historyEnd)
	listFiles := func() []string {
		entries, listErr := fileservice.SortedList(fs.List(ctx, ""))
		require.NoError(t, listErr)
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name)
		}
		return names
	}
	filesBeforeFailure := listFiles()

	missingHistoryEnd := previousEnd.Next()
	invalidGlobalEnd := missingHistoryEnd.Next()
	trackingFS := &trackingDeleteFileService{FileService: fs}
	_, historyStart, historyEnd, known, err = SyncTableIDBatchWithHistory(
		ctx,
		types.TS{},
		invalidGlobalEnd,
		24*time.Hour,
		64,
		objectio.Location{},
		0,
		previous,
		missingHistoryEnd,
		proc.Mp(),
		trackingFS,
	)
	require.ErrorContains(t, err, "table-ID predecessor history is incomplete")
	require.NotEmpty(t, trackingFS.deleted,
		"the test must spill before predecessor validation fails")
	require.Equal(t, filesBeforeFailure, listFiles(),
		"failed table-ID construction must not retain spilled objects")
	require.True(t, known)
	require.Equal(t, previousStart, historyStart)
	require.Equal(t, previousEnd, historyEnd)

	deleteErr := errors.New("injected checkpoint object delete failure")
	failingFS := &trackingDeleteFileService{FileService: fs, err: deleteErr}
	_, _, _, _, err = SyncTableIDBatchWithHistory(
		ctx,
		types.TS{},
		invalidGlobalEnd,
		24*time.Hour,
		64,
		objectio.Location{},
		0,
		previous,
		missingHistoryEnd,
		proc.Mp(),
		failingFS,
	)
	require.ErrorContains(t, err, "table-ID predecessor history is incomplete")
	require.ErrorIs(t, err, deleteErr)
	require.NotEmpty(t, failingFS.deleted)
	// The injected failure deliberately leaves the test objects behind. Remove
	// them through the underlying service so the fixture also proves the exact
	// attempted ownership set is sufficient for cleanup.
	require.NoError(t, fs.Delete(ctx, failingFS.deleted...))
	require.Equal(t, filesBeforeFailure, listFiles())
}

func makeCheckpointObjectRanges(
	t *testing.T,
	mp *mpool.MPool,
	fs fileservice.FileService,
) ([]ckputil.TableRange, []ckputil.TableRange) {
	t.Helper()

	ctx := context.Background()
	data := ckputil.NewObjectListBatch()
	defer data.Clean(mp)

	sinker := ckputil.NewDataSinker(
		mp,
		fs,
		ioutil.WithMemorySizeThreshold(1),
	)
	defer sinker.Close()

	packer := types.NewPacker()
	defer packer.Close()

	for i, vec := range data.Vecs {
		switch i {
		case ckputil.TableObjectsAttr_Accout_Idx:
			require.NoError(t, vector.AppendMultiFixed(vec, uint32(0), false, 2, mp))
		case ckputil.TableObjectsAttr_DB_Idx:
			tableVec := data.Vecs[ckputil.TableObjectsAttr_Table_Idx]
			objectTypeVec := data.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx]
			idVec := data.Vecs[ckputil.TableObjectsAttr_ID_Idx]
			clusterVec := data.Vecs[ckputil.TableObjectsAttr_Cluster_Idx]
			for _, objectType := range []int8{
				ckputil.ObjectType_Data,
				ckputil.ObjectType_Tombstone,
			} {
				var stats objectio.ObjectStats
				name := objectio.MockObjectName()
				require.NoError(t, objectio.SetObjectStatsObjectName(&stats, name))
				require.NoError(t, objectio.SetObjectStatsSize(&stats, 1))

				packer.Reset()
				ckputil.EncodeCluser(
					packer,
					1,
					objectType,
					name.ObjectId(),
					false,
				)

				require.NoError(t, vector.AppendFixed(objectTypeVec, objectType, false, mp))
				require.NoError(t, vector.AppendFixed(vec, uint64(1), false, mp))
				require.NoError(t, vector.AppendFixed(tableVec, uint64(1), false, mp))
				require.NoError(t, vector.AppendBytes(idVec, stats[:], false, mp))
				require.NoError(t, vector.AppendBytes(clusterVec, packer.Bytes(), false, mp))
			}
		case ckputil.TableObjectsAttr_CreateTS_Idx:
			for range 2 {
				require.NoError(t, vector.AppendFixed(vec, types.NextGlobalTsForTest(), false, mp))
			}
		case ckputil.TableObjectsAttr_DeleteTS_Idx:
			for range 2 {
				require.NoError(t, vector.AppendFixed(vec, types.NextGlobalTsForTest(), false, mp))
			}
		}
	}
	data.SetRowCount(2)

	require.NoError(t, sinker.Write(ctx, data))
	require.NoError(t, sinker.Sync(ctx))
	files, inMemory := sinker.GetResult()
	require.Empty(t, inMemory)
	require.NotEmpty(t, files)

	ranges := ckputil.MakeTableRangeBatch()
	defer ranges.Clean(mp)
	require.NoError(t, ckputil.CollectTableRanges(ctx, files, ranges, mp, fs))

	return ckputil.ExportToTableRangesByFilter(
			ranges,
			1,
			ckputil.ObjectType_Data,
		),
		ckputil.ExportToTableRangesByFilter(
			ranges,
			1,
			ckputil.ObjectType_Tombstone,
		)
}

func TestCKPReaderReadMetaEmptyLocation(t *testing.T) {
	for _, version := range []uint32{CheckpointVersion12, CheckpointCurrentVersion} {
		for _, test := range []struct {
			name     string
			location objectio.Location
		}{
			{name: "missing encoding"},
			{
				name: "truncated encoding",
				location: append(
					objectio.Location{1}, make(objectio.Location, objectio.LocationLen-2)...,
				),
			},
			{name: "zero object name", location: make(objectio.Location, objectio.LocationLen)},
		} {
			t.Run(fmt.Sprintf("version-%d/%s", version, test.name), func(t *testing.T) {
				reader := NewCKPReader(version, test.location, nil, nil)
				err := reader.ReadMeta(context.Background())
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
			})
		}
	}
}

func makeCheckpointMetaFixture(
	t testing.TB, ranges []ckputil.TableRange,
) (*fileservice.LocalFS, objectio.Location, *mpool.MPool) {
	t.Helper()
	ctx := context.Background()
	capacity := toml.ByteSize(32 << 20)
	fs, err := fileservice.NewLocalFS2(ctx, defines.SharedFileServiceName, t.TempDir(),
		fileservice.CacheConfig{MemoryCapacity: &capacity}, nil)
	require.NoError(t, err)
	fs.SetAsyncUpdate(false)
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		fs.Close(ctx)
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	bat := ckputil.NewMetaBatch()
	defer bat.Clean(mp)
	for i := range ranges {
		require.NoError(t, ranges[i].AppendTo(bat, mp))
	}
	writer := ioutil.ConstructWriter(0, ckputil.MetaSeqnums, -1, false, false, fs)
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	return fs, stats.ObjectLocation(), mp
}

func checkpointMetaRange(tableID uint64, objectType int8, ordinal int) ckputil.TableRange {
	stats := objectio.NewObjectStats()
	name := objectio.BuildObjectName(&types.Uuid{1, byte(ordinal), byte(ordinal >> 8)}, uint16(ordinal))
	_ = objectio.SetObjectStatsObjectName(stats, name)
	_ = objectio.SetObjectStatsBlkCnt(stats, 1)
	return ckputil.TableRange{
		TableID: tableID, ObjectType: objectType,
		Start: types.BuildTestRowid(int64(ordinal+1), 0),
		End:   types.BuildTestRowid(int64(ordinal+1), 1), ObjectStats: *stats,
	}
}

func TestReadMetaWithTableIDSelectedRanges(t *testing.T) {
	ranges := []ckputil.TableRange{
		checkpointMetaRange(11, ckputil.ObjectType_Data, 0),
		checkpointMetaRange(11, ckputil.ObjectType_Data, 1),
		checkpointMetaRange(11, ckputil.ObjectType_Tombstone, 2),
		checkpointMetaRange(22, ckputil.ObjectType_Data, 3),
		checkpointMetaRange(33, ckputil.ObjectType_Tombstone, 4),
		checkpointMetaRange(33, ckputil.ObjectType_Tombstone, 5),
		checkpointMetaRange(44, ckputil.ObjectType_Data, 6),
		checkpointMetaRange(44, ckputil.ObjectType_Tombstone, 7),
	}
	fs, loc, mp := makeCheckpointMetaFixture(t, ranges)
	ctx := context.Background()
	for _, tid := range []uint64{0, 11, 12, 22, 33, 44, 45} {
		t.Run(fmt.Sprint(tid), func(t *testing.T) {
			var wantData, wantTombstone []ckputil.TableRange
			for _, r := range ranges {
				if r.TableID == tid {
					if r.ObjectType == ckputil.ObjectType_Data {
						wantData = append(wantData, r)
					} else {
						wantTombstone = append(wantTombstone, r)
					}
				}
			}
			for range 2 {
				data, tombstone, err := readMetaWithTableID(ctx, loc, tid, mp, fs)
				require.NoError(t, err)
				require.Equal(t, wantData, data)
				require.Equal(t, wantTombstone, tombstone)
				require.Zero(t, mp.CurrNB())
				fs.FlushCache(ctx)
				// Ranges must own their bytes after both scratch and cache release.
				require.Equal(t, wantData, data)
				require.Equal(t, wantTombstone, tombstone)
				if len(data) > 0 {
					data[0].ObjectStats[0] ^= 0xff
				}
			}
		})
	}
}

type checkpointMetaLease struct {
	fscache.Data
	releases *atomic.Int64
	corrupt  bool
}

func (d *checkpointMetaLease) Bytes() []byte {
	if d.corrupt {
		return nil
	}
	return d.Data.Bytes()
}

func (d *checkpointMetaLease) Release() {
	d.releases.Add(1)
	d.Data.Release()
}

type checkpointMetaReadFS struct {
	fileservice.FileService
	failIDs, failSelected, corruptIDs, corruptSelected bool
	failure                                            error
	leases, releases                                   atomic.Int64
	selectedReads                                      int
	beforeSelectedRead                                 func()
}

func (fs *checkpointMetaReadFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	ids := len(v.Entries) == 1
	selected := len(v.Entries) == len(ckputil.MetaSeqnums)
	if selected {
		fs.selectedReads++
		if fs.beforeSelectedRead != nil {
			fs.beforeSelectedRead()
		}
	}
	if (ids && fs.failIDs) || (selected && fs.failSelected) {
		return fs.failure
	}
	if err := fs.FileService.Read(ctx, v); err != nil {
		return err
	}
	for i := range v.Entries {
		entry := &v.Entries[i]
		corrupt := (ids && fs.corruptIDs) ||
			(selected && fs.corruptSelected && i == len(v.Entries)-1)
		if entry.CachedData != nil && (ids || corrupt) {
			fs.leases.Add(1)
			entry.CachedData = &checkpointMetaLease{
				Data: entry.CachedData, releases: &fs.releases, corrupt: corrupt,
			}
		}
	}
	return nil
}

func TestReadMetaWithTableIDFailureCleanup(t *testing.T) {
	fs, loc, mp := makeCheckpointMetaFixture(t, []ckputil.TableRange{
		checkpointMetaRange(1, ckputil.ObjectType_Data, 1),
		checkpointMetaRange(1, ckputil.ObjectType_Tombstone, 2),
	})
	ctx := context.Background()
	_, _, err := readMetaWithTableID(ctx, loc, 1, mp, fs)
	require.NoError(t, err)
	readErr := errors.New("checkpoint metadata read failure")
	for _, test := range []struct {
		name                                               string
		failIDs, failSelected, corruptIDs, corruptSelected bool
	}{
		{name: "ID read", failIDs: true},
		{name: "selected read", failSelected: true},
		{name: "ID decode", corruptIDs: true},
		{name: "partial materialization", corruptSelected: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			wrapped := &checkpointMetaReadFS{FileService: fs, failure: readErr,
				failIDs: test.failIDs, failSelected: test.failSelected,
				corruptIDs: test.corruptIDs, corruptSelected: test.corruptSelected}
			data, tombstone, err := readMetaWithTableID(ctx, loc, 1, mp, wrapped)
			if test.failIDs || test.failSelected {
				require.ErrorIs(t, err, readErr)
			} else {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			}
			require.Nil(t, data)
			require.Nil(t, tombstone)
			require.Zero(t, mp.CurrNB())
			require.Equal(t, wrapped.leases.Load(), wrapped.releases.Load())
			if test.failSelected || test.corruptSelected {
				require.Positive(t, wrapped.leases.Load())
				require.Equal(t, 1, wrapped.selectedReads)
			}
			// Injected failures must not poison the shared cached representation.
			data, tombstone, err = readMetaWithTableID(ctx, loc, 1, mp, fs)
			require.NoError(t, err)
			require.Len(t, data, 1)
			require.Len(t, tombstone, 1)
			require.Zero(t, mp.CurrNB())
		})
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = readMetaWithTableID(canceled, loc, 1, mp, fs)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, mp.CurrNB())
	t.Run("canceled with ID lease held", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		defer cancel()
		wrapped := &checkpointMetaReadFS{FileService: fs}
		wrapped.beforeSelectedRead = func() {
			require.Greater(t, wrapped.leases.Load(), wrapped.releases.Load())
			cancel()
		}
		data, tombstone, err := readMetaWithTableID(canceled, loc, 1, mp, wrapped)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, data)
		require.Nil(t, tombstone)
		require.Equal(t, 1, wrapped.selectedReads)
		require.Positive(t, wrapped.leases.Load())
		require.Equal(t, wrapped.leases.Load(), wrapped.releases.Load())
		require.Zero(t, mp.CurrNB())
		data, tombstone, err = readMetaWithTableID(ctx, loc, 1, mp, fs)
		require.NoError(t, err)
		require.Len(t, data, 1)
		require.Len(t, tombstone, 1)
	})
	wrapped := &checkpointMetaReadFS{FileService: fs}
	data, tombstone, err := readMetaWithTableID(ctx, loc, 2, mp, wrapped)
	require.NoError(t, err)
	require.Nil(t, data)
	require.Nil(t, tombstone)
	require.Zero(t, wrapped.selectedReads)
	require.Equal(t, wrapped.leases.Load(), wrapped.releases.Load())
}

func BenchmarkReadMetaWithTableID(b *testing.B) {
	for _, rows := range []int{4096, 32768, 131072} {
		b.Run(fmt.Sprint(rows), func(b *testing.B) {
			ranges := make([]ckputil.TableRange, rows)
			for i := range ranges {
				ranges[i] = checkpointMetaRange(uint64(i/4+1), int8(i%4/2+1), i)
			}
			fs, loc, mp := makeCheckpointMetaFixture(b, ranges)
			tid := uint64(rows / 8)
			for _, full := range []bool{true, false} {
				b.Run(fmt.Sprintf("full-batch-%t", full), func(b *testing.B) {
					read := func() {
						if full {
							bat, release, err := readMetaBatch(b.Context(), loc, mp, fs)
							if err != nil {
								b.Fatal(err)
								return
							}
							defer release()
							if len(ckputil.ExportToTableRangesByFilter(bat, tid, ckputil.ObjectType_Data)) != 2 {
								b.Fatal("missing data ranges")
							}
							if len(ckputil.ExportToTableRangesByFilter(bat, tid, ckputil.ObjectType_Tombstone)) != 2 {
								b.Fatal("missing tombstone ranges")
							}
							return
						}
						data, tombstone, err := readMetaWithTableID(b.Context(), loc, tid, mp, fs)
						if err != nil || len(data) != 2 || len(tombstone) != 2 {
							b.Fatalf("ranges=%d/%d err=%v", len(data), len(tombstone), err)
						}
					}
					read()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						read()
					}
					b.StopTimer()
					require.Zero(b, mp.CurrNB())
				})
			}
		})
	}
}
