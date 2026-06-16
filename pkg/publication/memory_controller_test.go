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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestMemoryController(t *testing.T, maxBytes int64) (*MemoryController, *mpool.MPool) {
	mp, err := mpool.NewMPool("test_mc", 0, mpool.NoFixed)
	require.NoError(t, err)
	mc := NewMemoryController(mp, maxBytes)
	return mc, mp
}

func TestMemoryType_String(t *testing.T) {
	assert.Equal(t, "object_content", MemoryTypeObjectContent.String())
	assert.Equal(t, "decompress_buffer", MemoryTypeDecompressBuffer.String())
	assert.Equal(t, "sort_index", MemoryTypeSortIndex.String())
	assert.Equal(t, "row_offset_map", MemoryTypeRowOffsetMap.String())
	assert.Equal(t, "unknown", MemoryType(99).String())
}

func TestNewMemoryController(t *testing.T) {
	mc, _ := newTestMemoryController(t, 1024*1024)
	assert.NotNil(t, mc)
	stats := mc.GetStats()
	assert.Equal(t, int64(1024*1024), stats.MaxBytes)
	assert.Equal(t, int64(0), stats.TotalBytes)
}

func TestMemoryController_AllocAndFree(t *testing.T) {
	mc, _ := newTestMemoryController(t, 1024*1024*1024)
	ctx := context.Background()

	data, err := mc.Alloc(ctx, 1024, MemoryTypeObjectContent)
	require.NoError(t, err)
	assert.Len(t, data, 1024)

	stats := mc.GetStats()
	assert.Equal(t, int64(1024), stats.ObjectContentBytes)
	assert.Equal(t, int64(1024), stats.TotalBytes)

	mc.Free(data, MemoryTypeObjectContent)
	stats = mc.GetStats()
	assert.Equal(t, int64(0), stats.ObjectContentBytes)
	assert.Equal(t, int64(0), stats.TotalBytes)
}

func TestMemoryController_AllocAllTypes(t *testing.T) {
	mc, _ := newTestMemoryController(t, 1024*1024*1024)
	ctx := context.Background()

	types := []MemoryType{
		MemoryTypeObjectContent,
		MemoryTypeDecompressBuffer,
		MemoryTypeSortIndex,
		MemoryTypeRowOffsetMap,
	}

	allocs := make([][]byte, len(types))
	for i, mt := range types {
		data, err := mc.Alloc(ctx, 256, mt)
		require.NoError(t, err)
		allocs[i] = data
	}

	stats := mc.GetStats()
	assert.Equal(t, int64(256), stats.ObjectContentBytes)
	assert.Equal(t, int64(256), stats.DecompressBufferBytes)
	assert.Equal(t, int64(256), stats.SortIndexBytes)
	assert.Equal(t, int64(256), stats.RowOffsetMapBytes)
	assert.Equal(t, int64(1024), stats.TotalBytes)

	for i, mt := range types {
		mc.Free(allocs[i], mt)
	}
	stats = mc.GetStats()
	assert.Equal(t, int64(0), stats.TotalBytes)
}

func TestMemoryController_FreeNil(t *testing.T) {
	mc, _ := newTestMemoryController(t, 1024*1024)
	// Should not panic
	mc.Free(nil, MemoryTypeObjectContent)
}

func TestMemoryController_AllocExceedsLimit(t *testing.T) {
	mc, _ := newTestMemoryController(t, 100)
	ctx := context.Background()

	// Allocation exceeding limit should still succeed (it only logs a warning)
	data, err := mc.Alloc(ctx, 200, MemoryTypeObjectContent)
	require.NoError(t, err)
	assert.Len(t, data, 200)
	mc.Free(data, MemoryTypeObjectContent)
}

func TestMemoryController_AllocCancelled(t *testing.T) {
	mc, _ := newTestMemoryController(t, 1024*1024*1024)
	ctx, cancel := context.WithCancel(context.Background())

	// Fill the semaphore (capacity 10)
	largeSize := 11 * 1024 * 1024 // > 10MB, triggers semaphore
	for i := 0; i < 10; i++ {
		data, err := mc.Alloc(ctx, largeSize, MemoryTypeObjectContent)
		require.NoError(t, err)
		defer mc.Free(data, MemoryTypeObjectContent)
	}

	// Cancel context, then try to allocate - should fail
	cancel()
	_, err := mc.Alloc(ctx, largeSize, MemoryTypeObjectContent)
	assert.Error(t, err)
}

// ============================================================================
// Object Pool Tests
// ============================================================================

func TestAcquireReleaseGetChunkJob(t *testing.T) {
	job := AcquireGetChunkJob()
	assert.NotNil(t, job)
	ReleaseGetChunkJob(job)
	// Release nil should not panic
	ReleaseGetChunkJob(nil)
}

func TestAcquireReleaseGetChunkJobResult(t *testing.T) {
	res := AcquireGetChunkJobResult()
	assert.NotNil(t, res)
	ReleaseGetChunkJobResult(res)
	ReleaseGetChunkJobResult(nil)
}

func TestAcquireReleaseWriteObjectJob(t *testing.T) {
	job := AcquireWriteObjectJob()
	assert.NotNil(t, job)
	ReleaseWriteObjectJob(job)
	ReleaseWriteObjectJob(nil)
}

func TestAcquireReleaseWriteObjectJobResult(t *testing.T) {
	res := AcquireWriteObjectJobResult()
	assert.NotNil(t, res)
	ReleaseWriteObjectJobResult(res)
	ReleaseWriteObjectJobResult(nil)
}

func TestAcquireReleaseGetMetaJob(t *testing.T) {
	job := AcquireGetMetaJob()
	assert.NotNil(t, job)
	ReleaseGetMetaJob(job)
	ReleaseGetMetaJob(nil)
}

func TestAcquireReleaseGetMetaJobResult(t *testing.T) {
	res := AcquireGetMetaJobResult()
	assert.NotNil(t, res)
	ReleaseGetMetaJobResult(res)
	ReleaseGetMetaJobResult(nil)
}

func TestAcquireReleaseFilterObjectJob(t *testing.T) {
	job := AcquireFilterObjectJob()
	assert.NotNil(t, job)
	ReleaseFilterObjectJob(job)
	ReleaseFilterObjectJob(nil)
}

func TestAcquireReleaseFilterObjectJobResult(t *testing.T) {
	res := AcquireFilterObjectJobResult()
	assert.NotNil(t, res)
	ReleaseFilterObjectJobResult(res)
	ReleaseFilterObjectJobResult(nil)
}

func TestAcquireReleaseAObjectMapping(t *testing.T) {
	mapping := AcquireAObjectMapping()
	assert.NotNil(t, mapping)
	ReleaseAObjectMapping(mapping)
	ReleaseAObjectMapping(nil)
}

// ============================================================================
// Global Memory Controller Tests
// ============================================================================

func TestGetGlobalMemoryController_Nil(t *testing.T) {
	// Save and restore
	old := GetGlobalMemoryController()
	SetGlobalMemoryController(nil)
	assert.Nil(t, GetGlobalMemoryController())
	SetGlobalMemoryController(old)
}

func TestSetGlobalMemoryController(t *testing.T) {
	mc, _ := newTestMemoryController(t, 1024)
	old := GetGlobalMemoryController()
	SetGlobalMemoryController(mc)
	assert.Equal(t, mc, GetGlobalMemoryController())
	SetGlobalMemoryController(old)
}
