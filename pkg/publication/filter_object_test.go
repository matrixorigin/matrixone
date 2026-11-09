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
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
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
		context.Background(), []byte("short"), &stats, nil, mp, amap, nil,
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

type rewriteFileService struct {
	fileservice.FileService
	beforeWrite func() error
}

func (f *rewriteFileService) Write(ctx context.Context, v fileservice.IOVector) error {
	if err := f.beforeWrite(); err != nil {
		return err
	}
	return f.FileService.Write(ctx, v)
}

func TestRewriteNonAppendableTombstoneWithSinker_WideKeys(t *testing.T) {
	for _, tc := range []struct {
		name      string
		rows      int
		width     int
		remap     bool
		sharedKey bool
	}{
		{"partial", 2000, 32 << 10, false, false},
		{"wide_full", objectio.BlockMaxRows, 16 << 10, true, false},
		{"multi_block", objectio.BlockMaxRows + 1, 2 << 10, true, false},
		{"aliased", objectio.BlockMaxRows, 16 << 10, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			newFS := func(t *testing.T) *fileservice.LocalFS {
				fs, err := fileservice.NewLocalFS(ctx, "SHARED", t.TempDir(), fileservice.DisabledCacheConfig, nil)
				require.NoError(t, err)
				t.Cleanup(func() { fs.Close(ctx) })
				return fs
			}
			sourceFS := newFS(t)
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			attrs, typs := objectio.GetTombstoneSchema(types.T_varchar.ToType(), objectio.HiddenColumnSelection_None)
			objectID := objectio.NewObjectid()
			name := objectio.BuildObjectNameWithObjectID(&objectID)
			source := newTombstoneFSinkerFactoryWithName(name, objectio.HiddenColumnSelection_None)(mp, sourceFS)
			t.Cleanup(func() { require.NoError(t, source.Close()) })
			suffix := strings.Repeat("x", tc.width-8)
			key := func(row int) string {
				if tc.sharedKey {
					row = 0
				}
				return fmt.Sprintf("%08d", row) + suffix
			}
			mapping := NewAObjectMap()
			want := make(map[types.Rowid]int, tc.rows)
			dataObjects := make(map[types.Objectid]int)
			for start := 0; start < tc.rows; start += objectio.BlockMaxRows {
				func() {
					input := batch.NewWithSchema(false, attrs, typs)
					defer input.Clean(mp)
					n := min(objectio.BlockMaxRows, tc.rows-start)
					// Source objects are ordered; remapping reverses both object
					// order and row offsets, including across the block boundary.
					dataID := types.Objectid{byte(1 + start/objectio.BlockMaxRows)}
					targetID := dataID
					if tc.remap {
						targetID[0] = 4 - dataID[0]
						var stats objectio.ObjectStats
						require.NoError(t, objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectNameWithObjectID(&targetID)))
						offsets := make(map[uint32]uint32, n)
						for row := 0; row < n; row++ {
							offsets[uint32(row)] = uint32(n - 1 - row)
						}
						mapping.Set(dataID.String(), &AObjectMapping{DownstreamStats: stats, RowOffsetMap: offsets})
					}
					blockID := objectio.NewBlockidWithObjectID(&dataID, 0)
					require.NoError(t, objectio.ConstructRowidColumnTo(input.Vecs[0], &blockID, 0, uint32(n), mp))
					if tc.sharedKey {
						constant, err := vector.NewConstBytes(typs[1], []byte(key(0)), n, mp)
						require.NoError(t, err)
						defer constant.Free(mp)
						// A real flat on-disk vector can share one varlen value.
						require.NoError(t, input.Vecs[1].UnionBatch(constant, 0, n, nil, mp))
					} else {
						for row := 0; row < n; row++ {
							require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(key(start+row)), false, mp))
						}
					}
					for row := 0; row < n; row++ {
						offset := row
						if tc.remap {
							offset = n - 1 - row
						}
						want[types.NewRowIDWithObjectIDBlkNumAndRowID(targetID, 0, uint32(offset))] = start + row
					}
					dataObjects[targetID] = n
					input.SetRowCount(n)
					require.NoError(t, source.Sink(ctx, input))
				}()
			}
			original, err := source.Sync(ctx)
			require.NoError(t, err)
			require.EqualValues(t, (tc.rows+objectio.BlockMaxRows-1)/objectio.BlockMaxRows, original.BlkCnt())
			file, err := sourceFS.StatFile(ctx, name.String())
			require.NoError(t, err)
			content := fileservice.IOVector{FilePath: name.String(), Entries: []fileservice.IOEntry{{Offset: 0, Size: file.Size}}}
			t.Cleanup(func() { objectio.ReleaseIOVector(&content) })
			require.NoError(t, sourceFS.Read(ctx, &content))
			modes := []string{"success"}
			if tc.sharedKey {
				modes = append(modes, "deny_fetch", "deny_expansion", "cancel", "write_error")
			}
			for _, mode := range modes {
				t.Run(mode, func(t *testing.T) {
					total := int64(64 << 30)
					if mode == "deny_fetch" {
						total = 1
					} else if mode == "deny_expansion" {
						total = 256 << 20
					}
					owner := rscthrottler.NewMemThrottler(t.Name(), 1, rscthrottler.WithConstLimit(total),
						rscthrottler.WithMemorySampleForTesting(total, 0)).(rscthrottler.WorkingSetThrottler)
					jobCtx, cancel := context.WithCancel(context.WithValue(ctx, publicationMemoryKey{}, owner))
					t.Cleanup(cancel)
					fetches, writes := 0, 0
					upstream := GetObjectFromUpstreamWithWorker
					t.Cleanup(func() { GetObjectFromUpstreamWithWorker = upstream })
					GetObjectFromUpstreamWithWorker = func(context.Context, SQLExecutor, string, GetChunkWorker, string, string) ([]byte, error) {
						fetches++
						require.Less(t, owner.WorkingSetHeadroom().Available, total, "reserve before fetching")
						if mode == "cancel" {
							cancel()
						}
						return content.Entries[0].Data, nil
					}
					uploadErr := errors.New("injected write failure")
					targetFS := &rewriteFileService{FileService: newFS(t), beforeWrite: func() error {
						writes++
						require.GreaterOrEqual(t, total-owner.WorkingSetHeadroom().Available,
							int64(tc.rows*tc.width), "materialized output must have an active reservation")
						if mode == "write_error" {
							return uploadErr
						}
						return nil
					}}
					before := mp.CurrNB()
					job := NewFilterObjectJob(jobCtx, original.Marshal(), types.TS{}, nil, true, targetFS, mp,
						nil, nil, "", "", nil, nil, mapping, nil)
					job.Execute()
					result := job.WaitDone().(*FilterObjectJobResult)
					require.Equal(t, before, mp.CurrNB(), "vectors released on every exit")
					require.Equal(t, total, owner.WorkingSetHeadroom().Available, "all promises released")
					if mode != "success" {
						require.Error(t, result.Err)
						require.Equal(t, objectio.ObjectStats{}, result.DownstreamStats)
						if mode == "deny_fetch" {
							var capacity *rscthrottler.WorkingSetCapacityError
							require.ErrorAs(t, result.Err, &capacity)
						} else if mode == "deny_expansion" {
							require.True(t, moerr.IsMoErrCode(result.Err, moerr.ErrInternal))
						} else if mode == "cancel" {
							require.ErrorIs(t, result.Err, context.Canceled)
						} else {
							require.ErrorIs(t, result.Err, uploadErr)
						}
						if mode == "deny_fetch" {
							require.Zero(t, fetches)
						} else {
							require.Equal(t, 1, fetches)
						}
						if mode != "write_error" {
							require.Zero(t, writes)
						}
						return
					}
					require.NoError(t, result.Err)
					require.Equal(t, 1, writes, "the fixed object is committed once")
					rewritten := result.DownstreamStats
					require.EqualValues(t, tc.rows, rewritten.Rows())
					require.True(t, rewritten.GetCNCreated())
					require.Equal(t, name, rewritten.ObjectName())
					seen := make(map[types.Rowid]bool, tc.rows)
					locations := make(map[int]objectio.Location)
					var previous types.Rowid
					objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
						func() {
							loc := objectio.Location(info.MetaLoc[:])
							locations[int(loc.ID())] = loc
							reader, err := ioutil.NewObjectReader(targetFS, loc)
							require.NoError(t, err)
							got, release, err := reader.LoadColumns(ctx, []uint16{0, 1}, typs, loc.ID(), mp)
							require.NoError(t, err)
							defer release()
							require.EqualValues(t, loc.Rows(), got.RowCount(), "locations match real block rows")
							for row, id := range vector.MustFixedColNoTypeCheck[types.Rowid](got.Vecs[0]) {
								expected, ok := want[id]
								require.True(t, ok)
								require.False(t, seen[id])
								if len(seen) > 0 {
									require.True(t, previous.LT(&id), "global RowID order after remapping")
								}
								seen[id], previous = true, id
								require.Equal(t, key(expected), string(got.Vecs[1].GetBytesAt(row)))
							}
						}()
						return true
					}, rewritten)
					require.Len(t, seen, tc.rows)
					for id, expected := range dataObjects {
						next := true
						deleted := 0
						_, _, _, err := ioutil.CheckTombstoneFile(ctx, id[:], func() (*objectio.ObjectStats, error) {
							if !next {
								return nil, nil
							}
							next = false
							return &rewritten, nil
						}, func(_ *objectio.ObjectStats, block int) (bool, error) {
							blockID := objectio.NewBlockidWithObjectID(&id, 0)
							mask, err := ioutil.FillBlockDeleteMask(ctx, &types.TS{}, &blockID, locations[block], targetFS, true)
							if err != nil {
								return false, err
							}
							defer mask.Release()
							deleted += mask.Count()
							return true, nil
						}, targetFS)
						require.NoError(t, err)
						require.Equal(t, expected, deleted, "real tombstone reader finds all remapped deletes")
					}
				})
			}
		})
	}
}
