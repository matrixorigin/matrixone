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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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
