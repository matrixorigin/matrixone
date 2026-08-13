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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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

	files := make([]string, 1001)
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
	// empty location must be rejected with an error, not panic, on every read path
	for _, version := range []uint32{CheckpointVersion12, CheckpointCurrentVersion} {
		reader := NewCKPReader(version, objectio.Location{}, nil, nil)
		err := reader.ReadMeta(context.Background())
		require.Error(t, err)
	}
}
