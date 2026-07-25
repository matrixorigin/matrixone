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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/stretchr/testify/require"
)

func TestHydrateCNMergeTargetPreservesLineage(t *testing.T) {
	for _, opt := range []objectio.ObjectStatsOptions{
		objectio.WithCNCreated(),
		objectio.WithCNOrigin(),
	} {
		id := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(
			&id, false, true, false,
		)
		opt(stats)
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 17))
		createTS := types.BuildTS(42, 7)

		hydrated, gotCreateTS := hydrateCNMergeTarget(objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  createTS,
		})

		require.Equal(t, *stats, hydrated)
		require.Equal(t, createTS, gotCreateTS)
		require.Equal(t, stats.GetCNCreated(), hydrated.GetCNCreated())
		require.Equal(t, stats.GetCNOrigin(), hydrated.GetCNOrigin())
	}
}

func TestCNMergeRejectsMismatchedTargetMetadata(t *testing.T) {
	_, err := newCNMergeTask(
		context.Background(),
		nil,
		types.TS{},
		-1,
		false,
		[]objectio.ObjectStats{{}},
		nil,
		0,
	)
	require.ErrorContains(t, err, "1 objects, 0 create timestamps")
}

func TestCNMergeDoesNotMarkPureTNOutput(t *testing.T) {
	id := objectio.NewObjectid()
	target := objectio.NewObjectStatsWithObjectID(
		&id, false, true, false,
	)
	createdID := objectio.NewObjectid()
	created := objectio.NewObjectStatsWithObjectID(
		&createdID, false, true, false,
	)
	task := &cnMergeTask{
		targets: []objectio.ObjectStats{*target},
		commitEntry: &api.MergeCommitEntry{
			CreatedObjs: [][]byte{created.Clone().Marshal()},
		},
	}

	task.markCreatedObjectsCNOrigin()

	got := objectio.ObjectStats(task.commitEntry.CreatedObjs[0])
	require.False(t, got.GetCNOrigin())
}

func TestCNMergeMaterializesAndPreservesRowCommitTS(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"cn-merge-commit-ts", fileservice.DisabledCacheConfig, nil,
	)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		fs.Close(ctx)
		require.Zero(t, mp.CurrNB())
	})

	createTSs := []types.TS{
		types.BuildTS(10, 1),
		types.BuildTS(20, 2),
	}
	targets := make([]objectio.ObjectStats, 0, len(createTSs))
	for i := range createTSs {
		targets = append(
			targets,
			writeCNMergeSourceObject(t, ctx, fs, mp, []int32{
				int32(i*2 + 1),
				int32(i*2 + 2),
			}),
		)
	}

	task := newCNMergeTaskForTest(t, ctx, fs, mp, targets, createTSs)
	defer task.Release()
	require.NoError(t, mergesort.DoMergeAndWrite(
		ctx, "cn-merge-commit-ts", -1, task,
	))
	task.markCreatedObjectsCNOrigin()
	require.Len(t, task.GetCommitEntry().CreatedObjs, 1)

	created := objectio.ObjectStats(task.GetCommitEntry().CreatedObjs[0])
	require.True(t, created.GetCNOrigin())
	require.Equal(t, []types.TS{
		createTSs[0], createTSs[0],
		createTSs[1], createTSs[1],
	}, readCNMergeCommitTS(t, ctx, fs, mp, created))

	// A table-policy CN merge can select an object produced by a previous CN
	// merge. Its row timestamps must survive another rewrite unchanged.
	secondTask := newCNMergeTaskForTest(
		t,
		ctx,
		fs,
		mp,
		[]objectio.ObjectStats{created},
		[]types.TS{{}},
	)
	defer secondTask.Release()
	require.NoError(t, mergesort.DoMergeAndWrite(
		ctx, "cn-merge-preserve-commit-ts", -1, secondTask,
	))
	secondTask.markCreatedObjectsCNOrigin()
	require.Len(t, secondTask.GetCommitEntry().CreatedObjs, 1)

	recreated := objectio.ObjectStats(secondTask.GetCommitEntry().CreatedObjs[0])
	require.True(t, recreated.GetCNOrigin())
	require.Equal(t, []types.TS{
		createTSs[0], createTSs[0],
		createTSs[1], createTSs[1],
	}, readCNMergeCommitTS(t, ctx, fs, mp, recreated))
}

func TestCNMergeLegacyTNCommitTSCompatibility(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"cn-merge-legacy-commit-ts", fileservice.DisabledCacheConfig, nil,
	)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		fs.Close(ctx)
		require.Zero(t, mp.CurrNB())
	})

	legacy := writeMergeSourceObject(t, ctx, fs, mp, []int32{1})
	pureTNTask := newCNMergeTaskForTest(
		t, ctx, fs, mp,
		[]objectio.ObjectStats{legacy},
		[]types.TS{{}},
	)
	defer pureTNTask.Release()
	require.NoError(t, mergesort.DoMergeAndWrite(
		ctx, "cn-merge-legacy-tn", -1, pureTNTask,
	))
	pureTNTask.markCreatedObjectsCNOrigin()
	require.Len(t, pureTNTask.GetCommitEntry().CreatedObjs, 1)

	pureTNCreated := objectio.ObjectStats(
		pureTNTask.GetCommitEntry().CreatedObjs[0],
	)
	require.False(t, pureTNCreated.GetCNOrigin())
	pureTNBatch, release := readCNMergeBatch(
		t, ctx, fs, mp, pureTNCreated,
	)
	require.Equal(t, 1, pureTNBatch.RowCount())
	require.True(t, pureTNBatch.Vecs[1].IsNull(0))
	release()

	cnCreateTS := types.BuildTS(30, 3)
	cnCreated := writeCNMergeSourceObject(
		t, ctx, fs, mp, []int32{2},
	)
	mixedTask := newCNMergeTaskForTest(
		t, ctx, fs, mp,
		[]objectio.ObjectStats{legacy, cnCreated},
		[]types.TS{{}, cnCreateTS},
	)
	defer mixedTask.Release()
	require.NoError(t, mergesort.DoMergeAndWrite(
		ctx, "cn-merge-legacy-mixed", -1, mixedTask,
	))
	mixedTask.markCreatedObjectsCNOrigin()
	require.Len(t, mixedTask.GetCommitEntry().CreatedObjs, 1)

	mixedCreated := objectio.ObjectStats(
		mixedTask.GetCommitEntry().CreatedObjs[0],
	)
	require.True(t, mixedCreated.GetCNOrigin())
	mixedBatch, release := readCNMergeBatch(
		t, ctx, fs, mp, mixedCreated,
	)
	defer release()
	require.Equal(t, 2, mixedBatch.RowCount())
	require.True(t, mixedBatch.Vecs[1].IsNull(0))
	require.False(t, mixedBatch.Vecs[1].IsNull(1))
	require.Equal(
		t,
		cnCreateTS,
		vector.GetFixedAtNoTypeCheck[types.TS](mixedBatch.Vecs[1], 1),
	)
}

func readCNMergeCommitTS(
	t *testing.T,
	ctx context.Context,
	fs fileservice.FileService,
	mp *mpool.MPool,
	stats objectio.ObjectStats,
) []types.TS {
	t.Helper()

	got, release := readCNMergeBatch(t, ctx, fs, mp, stats)
	defer release()

	require.False(t, got.Vecs[1].IsConstNull())
	return append(
		[]types.TS(nil),
		vector.MustFixedColWithTypeCheck[types.TS](got.Vecs[1])...,
	)
}

func readCNMergeBatch(
	t *testing.T,
	ctx context.Context,
	fs fileservice.FileService,
	mp *mpool.MPool,
	stats objectio.ObjectStats,
) (*batch.Batch, func()) {
	t.Helper()

	reader := newCNMergeTaskForTest(
		t, ctx, fs, mp, []objectio.ObjectStats{stats}, []types.TS{{}},
	)
	got, _, release, err := reader.LoadNextBatch(ctx, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, release)
	return got, func() {
		release()
		reader.Release()
	}
}

func TestCNMergePreservesMissingLegacyCommitTS(t *testing.T) {
	for _, cnOrigin := range []bool{false, true} {
		t.Run(map[bool]string{false: "TN", true: "CNOrigin"}[cnOrigin], func(t *testing.T) {
			mp := mpool.MustNewZero()
			id := objectio.NewObjectid()
			stats := objectio.NewObjectStatsWithObjectID(
				&id, false, true, false,
			)
			if cnOrigin {
				objectio.WithCNOrigin()(stats)
			}
			task := &cnMergeTask{
				mp:              mp,
				seqnums:         []uint16{0, objectio.SEQNUM_COMMITTS},
				targets:         []objectio.ObjectStats{*stats},
				targetCreateTSs: []types.TS{{}},
			}
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(1), false, mp))
			bat.Vecs[1] = vector.NewConstNull(types.T_TS.ToType(), 1, mp)
			bat.SetRowCount(1)
			defer bat.Clean(mp)

			released := false
			release, err := task.materializeCommitTS(0, bat, func() {
				released = true
			})
			require.NoError(t, err)
			require.NotNil(t, release)
			require.False(t, released)
			require.True(t, bat.Vecs[1].IsConstNull())
			release()
			require.True(t, released)
		})
	}
}

func TestCNMergeRejectsCNCreatedWithoutCreateTS(t *testing.T) {
	mp := mpool.MustNewZero()
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(
		&id, false, true, false,
	)
	objectio.WithCNCreated()(stats)
	task := &cnMergeTask{
		mp:              mp,
		seqnums:         []uint16{0, objectio.SEQNUM_COMMITTS},
		targets:         []objectio.ObjectStats{*stats},
		targetCreateTSs: []types.TS{{}},
	}
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(1), false, mp))
	bat.Vecs[1] = vector.NewConstNull(types.T_TS.ToType(), 1, mp)
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	released := false
	release, err := task.materializeCommitTS(0, bat, func() {
		released = true
	})
	require.ErrorContains(t, err, "has no create timestamp")
	require.Nil(t, release)
	require.True(t, released)
}

func writeCNMergeSourceObject(
	t *testing.T,
	ctx context.Context,
	fs fileservice.FileService,
	mp *mpool.MPool,
	values []int32,
) objectio.ObjectStats {
	return writeMergeSourceObject(
		t, ctx, fs, mp, values, objectio.WithCNCreated(),
	)
}

func writeMergeSourceObject(
	t *testing.T,
	ctx context.Context,
	fs fileservice.FileService,
	mp *mpool.MPool,
	values []int32,
	opts ...objectio.ObjectStatsOptions,
) objectio.ObjectStats {
	t.Helper()

	bat := batch.NewWithSize(1)
	bat.Attrs = []string{"pk"}
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
	}
	bat.SetRowCount(len(values))
	defer bat.Clean(mp)

	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	writer, err := ioutil.NewBlockWriterNew(fs, name, 0, []uint16{0}, false)
	require.NoError(t, err)
	writer.SetPrimaryKey(0)
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	return writer.GetObjectStats(opts...)
}

func newCNMergeTaskForTest(
	t *testing.T,
	ctx context.Context,
	fs fileservice.FileService,
	mp *mpool.MPool,
	targets []objectio.ObjectStats,
	targetCreateTSs []types.TS,
) *cnMergeTask {
	t.Helper()

	blkCnts := make([]int, len(targets))
	blkIters := make([]*objectio.StatsBlkIter, len(targets))
	for i := range targets {
		loc := targets[i].ObjectLocation()
		meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
		require.NoError(t, err)
		blkCnts[i] = int(targets[i].BlkCnt())
		blkIters[i] = objectio.NewStatsBlkIter(
			&targets[i], meta.MustDataMeta(),
		)
	}

	return &cnMergeTask{
		host: &txnTable{
			db:        &txnDatabase{databaseId: 1},
			tableId:   2,
			tableName: "t",
			version:   0,
			comment:   catalog.MO_COMMENT_NO_DEL_HINT,
		},
		snapshot:        types.MaxTs(),
		ds:              &emptyCNMergeDataSource{},
		mp:              mp,
		colattrs:        []string{"pk", objectio.DefaultCommitTS_Attr},
		seqnums:         []uint16{0, objectio.SEQNUM_COMMITTS},
		typs:            []types.Type{types.T_int32.ToType(), types.T_TS.ToType()},
		sortkeyPos:      -1,
		targets:         targets,
		targetCreateTSs: targetCreateTSs,
		fs:              fs,
		blkCnts:         blkCnts,
		blkIters:        blkIters,
		segmentID:       objectio.NewSegmentid(),
	}
}

type emptyCNMergeDataSource struct {
	orderBy []*plan.OrderBySpec
}

func (s *emptyCNMergeDataSource) Next(
	context.Context,
	[]string,
	[]types.Type,
	[]uint16,
	int32,
	any,
	*mpool.MPool,
	*batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {
	return nil, engine.End, nil
}

func (s *emptyCNMergeDataSource) ApplyTombstones(
	_ context.Context,
	_ *objectio.Blockid,
	rows []int64,
	_ engine.TombstoneApplyPolicy,
) ([]int64, error) {
	return rows, nil
}

func (s *emptyCNMergeDataSource) GetTombstones(
	context.Context,
	*objectio.Blockid,
) (objectio.Bitmap, error) {
	return objectio.Bitmap{}, nil
}

func (s *emptyCNMergeDataSource) SetOrderBy(orderBy []*plan.OrderBySpec) {
	s.orderBy = orderBy
}

func (s *emptyCNMergeDataSource) GetOrderBy() []*plan.OrderBySpec {
	return s.orderBy
}

func (*emptyCNMergeDataSource) SetFilterZM(objectio.ZoneMap) {}
func (*emptyCNMergeDataSource) Close()                       {}
func (*emptyCNMergeDataSource) String() string               { return "empty-cn-merge" }
