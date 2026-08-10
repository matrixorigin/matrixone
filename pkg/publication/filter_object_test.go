// Copyright 2024 Matrix Origin
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

package publication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- tombstoneFSinkerWithName ----

func TestTombstoneFSinkerWithName_SyncNilWriter(t *testing.T) {
	s := &tombstoneFSinkerWithName{}
	stats, err := s.Sync(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, stats)
}

func TestTombstoneFSinkerWithName_ResetNilWriter(t *testing.T) {
	s := &tombstoneFSinkerWithName{}
	s.Reset() // should not panic
}

func TestTombstoneFSinkerWithName_Close(t *testing.T) {
	s := &tombstoneFSinkerWithName{}
	err := s.Close()
	assert.NoError(t, err)
}

// ---- newTombstoneFSinkerFactoryWithName ----

func TestNewTombstoneFSinkerFactoryWithName(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	segid := objectio.NewSegmentid()
	objName := objectio.BuildObjectName(segid, 0)
	factory := newTombstoneFSinkerFactoryWithName(objName, objectio.HiddenColumnSelection_None)
	assert.NotNil(t, factory)

	sinker := factory(mp, nil)
	assert.NotNil(t, sinker)

	ts, ok := sinker.(*tombstoneFSinkerWithName)
	assert.True(t, ok)
	assert.Equal(t, objName, ts.objectName)
}

// ---- FilterObject TTL checker ----

func TestFilterObject_TTLExpired(t *testing.T) {
	ttlChecker := func() bool { return false }
	_, err := FilterObject(
		context.Background(),
		make([]byte, objectio.ObjectStatsLen),
		types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, ttlChecker,
	)
	assert.ErrorIs(t, err, ErrSyncProtectionTTLExpired)
}

func TestFilterObject_InvalidStatsLength(t *testing.T) {
	_, err := FilterObject(
		context.Background(),
		[]byte("short"),
		types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid object stats length")
}

// ---- FilterObjectJob TTL expired ----

func TestFilterObjectJob_TTLExpired(t *testing.T) {
	job := NewFilterObjectJob(
		context.Background(),
		nil, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil,
		func() bool { return false },
	)
	job.Execute()
	result := job.WaitDone().(*FilterObjectJobResult)
	assert.ErrorIs(t, result.Err, ErrSyncProtectionTTLExpired)
}

// ---- rewriteTombstoneRowidsBatch with mapping but no RowOffsetMap ----

func TestRewriteTombstoneRowidsBatch_MappingWithoutRowOffsetMap(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	upstreamObjID := types.NewObjectid()
	rid := types.NewRowIDWithObjectIDBlkNumAndRowID(upstreamObjID, 0, 42)

	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(rowidVec, rid, false, mp))

	bat := &batch.Batch{Vecs: []*vector.Vector{rowidVec}}
	bat.SetRowCount(1)

	downstreamObjID := types.NewObjectid()
	var downstreamStats objectio.ObjectStats
	objectio.SetObjectStatsObjectName(&downstreamStats, objectio.BuildObjectNameWithObjectID(&downstreamObjID))

	amap := NewAObjectMap()
	amap.Set(upstreamObjID.String(), &AObjectMapping{
		DownstreamStats: downstreamStats,
		RowOffsetMap:    nil,
	})

	err = rewriteTombstoneRowidsBatch(context.Background(), bat, amap, mp)
	assert.NoError(t, err)

	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](rowidVec)
	rewrittenObjID := rowids[0].BorrowObjectID()
	assert.Equal(t, downstreamObjID.Segment(), rewrittenObjID.Segment())
	assert.Equal(t, uint32(42), rowids[0].GetRowOffset())

	rowidVec.Free(mp)
}

// ---- GetObjectFromUpstreamWithWorker ----

func TestGetObjectFromUpstreamWithWorker_NilExecutor(t *testing.T) {
	_, err := GetObjectFromUpstreamWithWorker(
		context.Background(), nil, "obj1", nil, "acc", "pub",
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "upstream executor is nil")
}

func TestGetObjectFromUpstreamWithWorker_MetaError(t *testing.T) {
	exec := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, moerr.NewInternalErrorNoCtx("connection refused")
		},
	}
	_, err := GetObjectFromUpstreamWithWorker(
		context.Background(), exec, "obj1", nil, "acc", "pub",
	)
	assert.Error(t, err)
}

func TestGetObjectFromUpstreamWithWorker_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	exec := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, ctx.Err()
		},
	}
	_, err := GetObjectFromUpstreamWithWorker(
		ctx, exec, "obj1", nil, "acc", "pub",
	)
	assert.Error(t, err)
}

// ---- filterAppendableObject TTL paths ----

func TestFilterAppendableObject_TTLExpired(t *testing.T) {
	var stats objectio.ObjectStats
	_, err := filterAppendableObject(
		context.Background(), &stats, types.TS{}, nil, nil, false, nil, nil, "", "", nil,
		func() bool { return false },
	)
	assert.ErrorIs(t, err, ErrSyncProtectionTTLExpired)
}

func TestFilterAppendableObject_GetObjectError(t *testing.T) {
	orig := GetObjectFromUpstreamWithWorker
	defer func() { GetObjectFromUpstreamWithWorker = orig }()

	GetObjectFromUpstreamWithWorker = func(
		ctx context.Context, upstreamExecutor SQLExecutor, objectName string,
		getChunkWorker GetChunkWorker, subscriptionAccountName string, pubName string,
	) ([]byte, error) {
		return nil, fmt.Errorf("upstream down")
	}

	var stats objectio.ObjectStats
	_, err := filterAppendableObject(
		context.Background(), &stats, types.TS{}, nil, nil, false, nil, nil, "", "", nil, nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get object from upstream")
}

func TestFilterAppendableObject_TTLExpiredAfterGetObject(t *testing.T) {
	orig := GetObjectFromUpstreamWithWorker
	defer func() { GetObjectFromUpstreamWithWorker = orig }()

	GetObjectFromUpstreamWithWorker = func(
		ctx context.Context, upstreamExecutor SQLExecutor, objectName string,
		getChunkWorker GetChunkWorker, subscriptionAccountName string, pubName string,
	) ([]byte, error) {
		return []byte("data"), nil
	}

	called := false
	ttl := func() bool {
		if !called {
			called = true
			return true // first call passes
		}
		return false // second call fails
	}

	var stats objectio.ObjectStats
	_, err := filterAppendableObject(
		context.Background(), &stats, types.TS{}, nil, nil, false, nil, nil, "", "", nil, ttl,
	)
	assert.ErrorIs(t, err, ErrSyncProtectionTTLExpired)
}

// ---- filterNonAppendableObject TTL paths ----

func TestFilterNonAppendableObject_TTLExpired(t *testing.T) {
	var stats objectio.ObjectStats
	_, err := filterNonAppendableObject(
		context.Background(), &stats, types.TS{}, nil, nil, false, nil, nil, nil, "", "", nil, nil, nil,
		func() bool { return false },
	)
	assert.ErrorIs(t, err, ErrSyncProtectionTTLExpired)
}

func TestFilterNonAppendableObject_GetObjectError(t *testing.T) {
	orig := GetObjectFromUpstreamWithWorker
	defer func() { GetObjectFromUpstreamWithWorker = orig }()

	GetObjectFromUpstreamWithWorker = func(
		ctx context.Context, upstreamExecutor SQLExecutor, objectName string,
		getChunkWorker GetChunkWorker, subscriptionAccountName string, pubName string,
	) ([]byte, error) {
		return nil, fmt.Errorf("network error")
	}

	var stats objectio.ObjectStats
	_, err := filterNonAppendableObject(
		context.Background(), &stats, types.TS{}, nil, nil, false, nil, nil, nil, "", "", nil, nil, nil, nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get object from upstream")
}

func TestFilterNonAppendableObject_TTLExpiredAfterGetObject(t *testing.T) {
	orig := GetObjectFromUpstreamWithWorker
	defer func() { GetObjectFromUpstreamWithWorker = orig }()

	GetObjectFromUpstreamWithWorker = func(
		ctx context.Context, upstreamExecutor SQLExecutor, objectName string,
		getChunkWorker GetChunkWorker, subscriptionAccountName string, pubName string,
	) ([]byte, error) {
		return []byte("data"), nil
	}

	called := false
	ttl := func() bool {
		if !called {
			called = true
			return true
		}
		return false
	}

	var stats objectio.ObjectStats
	_, err := filterNonAppendableObject(
		context.Background(), &stats, types.TS{}, nil, nil, false, nil, nil, nil, "", "", nil, nil, nil, ttl,
	)
	assert.ErrorIs(t, err, ErrSyncProtectionTTLExpired)
}

// ---- getMetaWithRetry ----

func TestGetMetaWithRetry_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := getMetaWithRetry(ctx, nil, "obj", nil, "acc", "pub")
	assert.Error(t, err)
}

func TestGetMetaWithRetry_NonRetryableError(t *testing.T) {
	exec := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, moerr.NewBadDBNoCtx("bad")
		},
	}
	_, err := getMetaWithRetry(context.Background(), exec, "obj", nil, "acc", "pub")
	assert.Error(t, err)
}

// ---- getChunkWithRetry ----

func TestGetChunkWithRetry_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := getChunkWithRetry(ctx, nil, "obj", 1, nil, "acc", "pub")
	assert.Error(t, err)
}

func TestGetChunkWithRetry_NonRetryableError(t *testing.T) {
	exec := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, moerr.NewBadDBNoCtx("bad")
		},
	}
	_, err := getChunkWithRetry(context.Background(), exec, "obj", 1, nil, "acc", "pub")
	assert.Error(t, err)
}

func TestGetChunkWithRetry_AllRetriesFail(t *testing.T) {
	exec := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, moerr.NewInternalErrorNoCtx("transient")
		},
	}
	_, err := getChunkWithRetry(context.Background(), exec, "obj", 1, nil, "acc", "pub")
	assert.Error(t, err)
}

// ---- extractSortKeyFromObject ----

func TestExtractSortKeyFromObject_ContentTooSmall(t *testing.T) {
	var stats objectio.ObjectStats
	// Set extent offset+length > content length to trigger bounds check
	ext := objectio.NewExtent(0, 100, 50, 50)
	require.NoError(t, objectio.SetObjectStatsExtent(&stats, ext))

	_, err := extractSortKeyFromObject(context.Background(), []byte("tiny"), &stats)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "object content too small")
}

// ---- rewriteNonAppendableTombstoneWithSinker ----

func TestRewriteNonAppendableTombstoneWithSinker_ContentTooSmall(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	var stats objectio.ObjectStats
	// Set an extent that exceeds content length
	ext := objectio.NewExtent(0, 100, 50, 50)
	require.NoError(t, objectio.SetObjectStatsExtent(&stats, ext))

	amap := NewAObjectMap()
	_, err = rewriteNonAppendableTombstoneWithSinker(
		context.Background(), []byte("short"), &stats, nil, mp, amap,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "object content too small")
}

type publicationPersistThenErrorFS struct {
	fileservice.FileService
	persisted         string
	failObjectDeletes bool
	objectDeletes     int
}

func (fs *publicationPersistThenErrorFS) Write(
	ctx context.Context,
	vector fileservice.IOVector,
) error {
	if err := fs.FileService.Write(ctx, vector); err != nil {
		return err
	}
	if strings.HasPrefix(vector.FilePath, "gc/unpublished/") {
		return nil
	}
	fs.persisted = vector.FilePath
	return errors.New("injected publication post-persist sync failure")
}

func (fs *publicationPersistThenErrorFS) Delete(
	ctx context.Context,
	paths ...string,
) error {
	if fs.failObjectDeletes {
		for _, path := range paths {
			if !strings.HasPrefix(path, "gc/unpublished/") {
				fs.objectDeletes++
				return errors.New("injected publication object delete failure")
			}
		}
	}
	for _, path := range paths {
		if !strings.HasPrefix(path, "gc/unpublished/") {
			fs.objectDeletes++
		}
	}
	return fs.FileService.Delete(ctx, paths...)
}

func TestRewriteNonAppendableTombstoneSyncErrorHandsOffCleanup(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	sourceFS, err := fileservice.NewMemoryFS(
		"source", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	writer := ioutil.ConstructTombstoneWriter(
		objectio.HiddenColumnSelection_None, sourceFS)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	dataObjectID := objectio.NewObjectid()
	row := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjectID, 0, 0)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], row, false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32(1), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()

	read := &fileservice.IOVector{
		FilePath: stats.ObjectName().String(),
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1}},
	}
	require.NoError(t, sourceFS.Read(ctx, read))
	objectContent := append([]byte(nil), read.Entries[0].Data...)
	read.Release()

	targetBase, err := fileservice.NewMemoryFS(
		"target", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	targetFS := &publicationPersistThenErrorFS{
		FileService:       targetBase,
		failObjectDeletes: true,
	}
	_, err = rewriteNonAppendableTombstoneWithSinker(
		ctx, objectContent, &stats, targetFS, mp, NewAObjectMap())
	require.ErrorContains(t, err, "injected publication post-persist sync failure")
	require.NotEmpty(t, targetFS.persisted)
	require.Equal(t, 1, targetFS.objectDeletes,
		"the caller must attempt exact-name cleanup before handing it off")
	_, err = targetBase.StatFile(ctx, targetFS.persisted)
	require.NoError(t, err, "Sync failed after the object reached storage")

	targetFS.failObjectDeletes = false
	replayed, remaining, err := ioutil.ReplayUnpublishedObjectCleanup(ctx, targetFS)
	require.NoError(t, err)
	require.Equal(t, 1, replayed, "the caller must hand cleanup to a durable owner")
	require.False(t, remaining)
	_, err = targetBase.StatFile(ctx, targetFS.persisted)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

// ---- FilterObject dispatches to appendable vs non-appendable ----

func TestFilterObject_NonAppendable_GetObjectError(t *testing.T) {
	orig := GetObjectFromUpstreamWithWorker
	defer func() { GetObjectFromUpstreamWithWorker = orig }()

	GetObjectFromUpstreamWithWorker = func(
		ctx context.Context, upstreamExecutor SQLExecutor, objectName string,
		getChunkWorker GetChunkWorker, subscriptionAccountName string, pubName string,
	) ([]byte, error) {
		return nil, fmt.Errorf("fail")
	}

	// Build valid stats bytes for a non-appendable object
	var stats objectio.ObjectStats
	// default is non-appendable (appendable=false)
	statsBytes := stats.Marshal()

	_, err := FilterObject(
		context.Background(), statsBytes, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, nil,
	)
	assert.Error(t, err)
}

func TestFilterObject_Appendable_GetObjectError(t *testing.T) {
	orig := GetObjectFromUpstreamWithWorker
	defer func() { GetObjectFromUpstreamWithWorker = orig }()

	GetObjectFromUpstreamWithWorker = func(
		ctx context.Context, upstreamExecutor SQLExecutor, objectName string,
		getChunkWorker GetChunkWorker, subscriptionAccountName string, pubName string,
	) ([]byte, error) {
		return nil, fmt.Errorf("fail")
	}

	// Build valid stats bytes for an appendable object
	id := types.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
	statsBytes := stats.Marshal()

	_, err := FilterObject(
		context.Background(), statsBytes, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, nil,
	)
	assert.Error(t, err)
}

// ---- rewriteTombstoneRowidsBatch: RowOffsetMap path (not covered in filter_object_batch_test.go) ----

func TestRewriteTombstoneRowidsBatch_WithRowOffsetMapRewrite(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	upstreamObjID := types.NewObjectid()
	rid := types.NewRowIDWithObjectIDBlkNumAndRowID(upstreamObjID, 0, 10)

	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(rowidVec, rid, false, mp))

	bat := &batch.Batch{Vecs: []*vector.Vector{rowidVec}}
	bat.SetRowCount(1)

	downstreamObjID := types.NewObjectid()
	var downstreamStats objectio.ObjectStats
	objectio.SetObjectStatsObjectName(&downstreamStats, objectio.BuildObjectNameWithObjectID(&downstreamObjID))

	amap := NewAObjectMap()
	amap.Set(upstreamObjID.String(), &AObjectMapping{
		DownstreamStats: downstreamStats,
		RowOffsetMap:    map[uint32]uint32{10: 99},
	})

	err = rewriteTombstoneRowidsBatch(context.Background(), bat, amap, mp)
	assert.NoError(t, err)

	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](rowidVec)
	assert.Equal(t, uint32(99), rowids[0].GetRowOffset())

	rowidVec.Free(mp)
}
