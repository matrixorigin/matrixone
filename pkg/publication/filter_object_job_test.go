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

package publication

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewGetMetaJob(t *testing.T) {
	ctx := context.Background()
	job := NewGetMetaJob(ctx, nil, "obj1", "acc1", "pub1")
	require.NotNil(t, job)
	assert.Equal(t, int8(1), job.GetType())
}

func TestNewGetChunkJob(t *testing.T) {
	ctx := context.Background()
	job := NewGetChunkJob(ctx, nil, "obj1", 5, "acc1", "pub1")
	require.NotNil(t, job)
	assert.Equal(t, "obj1", job.GetObjectName())
	assert.Equal(t, int64(5), job.GetChunkIndex())
	assert.Equal(t, int8(2), job.GetType())
}

func TestNewWriteObjectJob(t *testing.T) {
	ctx := context.Background()
	content := []byte("test content")
	job := NewWriteObjectJob(ctx, nil, "obj1", content, nil, nil)
	require.NotNil(t, job)
	assert.Equal(t, "obj1", job.GetObjectName())
	assert.Equal(t, int64(len(content)), job.GetObjectSize())
	assert.Equal(t, JobTypeWriteObject, job.GetType())
}

func TestNewFilterObjectJob(t *testing.T) {
	ctx := context.Background()
	statsBytes := []byte("stats")
	ts := types.BuildTS(100, 0)
	job := NewFilterObjectJob(
		ctx, statsBytes, ts, nil, false, nil, nil, nil, nil,
		"acc1", "pub1", nil, nil, nil, nil,
	)
	require.NotNil(t, job)
	assert.Equal(t, JobTypeFilterObject, job.GetType())
}

func TestGetOrCreateChunkSemaphore(t *testing.T) {
	sem := getOrCreateChunkSemaphore()
	assert.NotNil(t, sem)
	// Should return the same instance
	assert.Equal(t, sem, getOrCreateChunkSemaphore())
}

func TestAObjectMap(t *testing.T) {
	m := NewAObjectMap()
	assert.NotNil(t, m)

	mapping := &AObjectMapping{DBName: "db1", TableName: "t1"}
	m.Set("key1", mapping)

	got, ok := m.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "db1", got.DBName)

	_, ok = m.Get("nonexistent")
	assert.False(t, ok)

	m.Delete("key1")
	_, ok = m.Get("key1")
	assert.False(t, ok)
}

// --- Additional coverage tests ---

func TestGetMetaJob_WaitDoneReceivesResult(t *testing.T) {
	ctx := context.Background()
	job := NewGetMetaJob(ctx, nil, "obj1", "acc1", "pub1")
	// Manually send a result to the channel to exercise WaitDone
	expected := &GetMetaJobResult{
		TotalSize:   1024,
		TotalChunks: 4,
		ChunkIndex:  0,
		IsComplete:  true,
	}
	job.result <- expected
	raw := job.WaitDone()
	got, ok := raw.(*GetMetaJobResult)
	require.True(t, ok)
	assert.Equal(t, int64(1024), got.TotalSize)
	assert.Equal(t, int64(4), got.TotalChunks)
	assert.True(t, got.IsComplete)
	assert.NoError(t, got.Err)
}

func TestGetMetaJob_WaitDoneReceivesError(t *testing.T) {
	ctx := context.Background()
	job := NewGetMetaJob(ctx, nil, "obj1", "acc1", "pub1")
	expected := &GetMetaJobResult{
		Err: assert.AnError,
	}
	job.result <- expected
	raw := job.WaitDone()
	got := raw.(*GetMetaJobResult)
	assert.Error(t, got.Err)
}

func TestGetChunkJob_WaitDoneReceivesResult(t *testing.T) {
	ctx := context.Background()
	job := NewGetChunkJob(ctx, nil, "obj2", 3, "acc1", "pub1")
	expected := &GetChunkJobResult{
		ChunkData:  []byte("chunk data"),
		ChunkIndex: 3,
	}
	job.result <- expected
	raw := job.WaitDone()
	got, ok := raw.(*GetChunkJobResult)
	require.True(t, ok)
	assert.Equal(t, []byte("chunk data"), got.ChunkData)
	assert.Equal(t, int64(3), got.ChunkIndex)
	assert.NoError(t, got.Err)
}

func TestGetChunkJob_WaitDoneReceivesError(t *testing.T) {
	ctx := context.Background()
	job := NewGetChunkJob(ctx, nil, "obj2", 1, "acc1", "pub1")
	expected := &GetChunkJobResult{Err: assert.AnError, ChunkIndex: 1}
	job.result <- expected
	raw := job.WaitDone()
	got := raw.(*GetChunkJobResult)
	assert.Error(t, got.Err)
	assert.Equal(t, int64(1), got.ChunkIndex)
}

func TestWriteObjectJob_WaitDoneReceivesResult(t *testing.T) {
	ctx := context.Background()
	job := NewWriteObjectJob(ctx, nil, "obj3", []byte("data"), nil, nil)
	expected := &WriteObjectJobResult{}
	job.result <- expected
	raw := job.WaitDone()
	got, ok := raw.(*WriteObjectJobResult)
	require.True(t, ok)
	assert.NoError(t, got.Err)
}

func TestWriteObjectJob_WaitDoneReceivesError(t *testing.T) {
	ctx := context.Background()
	job := NewWriteObjectJob(ctx, nil, "obj3", nil, nil, nil)
	expected := &WriteObjectJobResult{Err: assert.AnError}
	job.result <- expected
	raw := job.WaitDone()
	got := raw.(*WriteObjectJobResult)
	assert.Error(t, got.Err)
}

func TestWriteObjectJob_NilContent(t *testing.T) {
	ctx := context.Background()
	job := NewWriteObjectJob(ctx, nil, "obj-nil", nil, nil, nil)
	require.NotNil(t, job)
	assert.Equal(t, "obj-nil", job.GetObjectName())
	assert.Equal(t, int64(0), job.GetObjectSize())
}

func TestFilterObjectJob_WaitDoneReceivesResult(t *testing.T) {
	ctx := context.Background()
	ts := types.BuildTS(200, 0)
	job := NewFilterObjectJob(ctx, nil, ts, nil, false, nil, nil, nil, nil, "acc1", "pub1", nil, nil, nil, nil)
	expected := &FilterObjectJobResult{HasMappingUpdate: true}
	job.result <- expected
	raw := job.WaitDone()
	got, ok := raw.(*FilterObjectJobResult)
	require.True(t, ok)
	assert.True(t, got.HasMappingUpdate)
	assert.NoError(t, got.Err)
}

func TestFilterObjectJob_WaitDoneReceivesError(t *testing.T) {
	ctx := context.Background()
	ts := types.BuildTS(200, 0)
	job := NewFilterObjectJob(ctx, nil, ts, nil, true, nil, nil, nil, nil, "acc1", "pub1", nil, nil, nil, nil)
	expected := &FilterObjectJobResult{Err: assert.AnError}
	job.result <- expected
	raw := job.WaitDone()
	got := raw.(*FilterObjectJobResult)
	assert.Error(t, got.Err)
}

func TestFilterObjectJob_ExecuteWithTTLExpired(t *testing.T) {
	ctx := context.Background()
	ts := types.BuildTS(100, 0)
	// TTL checker that returns false (expired)
	ttlChecker := func() bool { return false }
	job := NewFilterObjectJob(ctx, nil, ts, nil, false, nil, nil, nil, nil, "acc1", "pub1", nil, nil, nil, ttlChecker)
	job.Execute()
	raw := job.WaitDone()
	got := raw.(*FilterObjectJobResult)
	assert.ErrorIs(t, got.Err, ErrSyncProtectionTTLExpired)
}

func TestNewGetMetaJob_NilExecutor(t *testing.T) {
	job := NewGetMetaJob(context.Background(), nil, "", "", "")
	require.NotNil(t, job)
	assert.Equal(t, JobTypeGetMeta, job.GetType())
}

func TestNewGetChunkJob_NilExecutor(t *testing.T) {
	job := NewGetChunkJob(context.Background(), nil, "", 0, "", "")
	require.NotNil(t, job)
	assert.Equal(t, JobTypeGetChunk, job.GetType())
	assert.Equal(t, "", job.GetObjectName())
	assert.Equal(t, int64(0), job.GetChunkIndex())
}

func TestNewFilterObjectJob_AllNilParams(t *testing.T) {
	job := NewFilterObjectJob(context.Background(), nil, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, nil)
	require.NotNil(t, job)
	assert.Equal(t, JobTypeFilterObject, job.GetType())
}

func TestJobTypeConstants(t *testing.T) {
	assert.Equal(t, int8(1), JobTypeGetMeta)
	assert.Equal(t, int8(2), JobTypeGetChunk)
	assert.Equal(t, int8(3), JobTypeFilterObject)
	assert.Equal(t, int8(4), JobTypeWriteObject)
}

// ---- GetMetaJob WaitDone via manual channel ----

func TestGetMetaJob_WaitDone(t *testing.T) {
	job := NewGetMetaJob(context.Background(), nil, "obj1", "acc1", "pub1")
	go func() {
		job.result <- &GetMetaJobResult{
			MetadataData: []byte("test-data"),
			TotalSize:    100,
			ChunkIndex:   0,
			TotalChunks:  1,
			IsComplete:   true,
		}
	}()
	result := job.WaitDone().(*GetMetaJobResult)
	assert.Nil(t, result.Err)
	assert.Equal(t, int64(100), result.TotalSize)
	assert.True(t, result.IsComplete)
}

func TestGetMetaJob_WaitDoneError(t *testing.T) {
	job := NewGetMetaJob(context.Background(), nil, "obj1", "acc1", "pub1")
	go func() {
		job.result <- &GetMetaJobResult{Err: moerr.NewInternalErrorNoCtx("test error")}
	}()
	result := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, result.Err)
}

func TestGetMetaJob_ExecuteWithCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	job := NewGetMetaJob(ctx, nil, "obj1", "acc1", "pub1")
	job.Execute()
	result := job.WaitDone().(*GetMetaJobResult)
	assert.Error(t, result.Err)
}

// ---- GetChunkJob WaitDone via manual channel ----

func TestGetChunkJob_WaitDone(t *testing.T) {
	job := NewGetChunkJob(context.Background(), nil, "obj1", 1, "acc1", "pub1")
	go func() {
		job.result <- &GetChunkJobResult{
			ChunkData:  []byte("chunk-data"),
			ChunkIndex: 1,
		}
	}()
	result := job.WaitDone().(*GetChunkJobResult)
	assert.Nil(t, result.Err)
	assert.Equal(t, int64(1), result.ChunkIndex)
}

func TestGetChunkJob_ExecuteWithCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	job := NewGetChunkJob(ctx, nil, "obj1", 0, "acc1", "pub1")
	job.Execute()
	result := job.WaitDone().(*GetChunkJobResult)
	assert.Error(t, result.Err)
}

// ---- WriteObjectJob tests ----

func TestWriteObjectJob_WaitDone(t *testing.T) {
	job := NewWriteObjectJob(context.Background(), nil, "test.obj", nil, nil, nil)
	go func() {
		job.result <- &WriteObjectJobResult{}
	}()
	result := job.WaitDone().(*WriteObjectJobResult)
	assert.Nil(t, result.Err)
}

func TestWriteObjectJob_GetObjectSize(t *testing.T) {
	job := NewWriteObjectJob(context.Background(), nil, "test.obj", []byte("hello"), nil, nil)
	assert.Equal(t, int64(5), job.GetObjectSize())
}

func TestWriteObjectJob_GetObjectSizeEmpty(t *testing.T) {
	job := NewWriteObjectJob(context.Background(), nil, "test.obj", nil, nil, nil)
	assert.Equal(t, int64(0), job.GetObjectSize())
}

func TestWriteObjectJob_GetObjectName(t *testing.T) {
	job := NewWriteObjectJob(context.Background(), nil, "test.obj", nil, nil, nil)
	assert.Equal(t, "test.obj", job.GetObjectName())
}

// ---- FilterObjectJob tests ----

func TestFilterObjectJob_WaitDone(t *testing.T) {
	job := NewFilterObjectJob(context.Background(), nil, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, nil)
	go func() {
		job.result <- &FilterObjectJobResult{Err: moerr.NewInternalErrorNoCtx("test")}
	}()
	result := job.WaitDone().(*FilterObjectJobResult)
	assert.Error(t, result.Err)
}

func TestFilterObjectJob_TTLExpired_ViaExecute(t *testing.T) {
	ttlChecker := func() bool { return false }
	job := NewFilterObjectJob(context.Background(), nil, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, ttlChecker)
	job.Execute()
	result := job.WaitDone().(*FilterObjectJobResult)
	assert.ErrorIs(t, result.Err, ErrSyncProtectionTTLExpired)
}

func TestFilterObjectJob_GetType(t *testing.T) {
	job := NewFilterObjectJob(context.Background(), nil, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, nil)
	assert.Equal(t, JobTypeFilterObject, job.GetType())
}
