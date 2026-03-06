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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

// MemoryType represents different types of memory allocations in CCPR
type MemoryType int

const (
	MemoryTypeObjectContent MemoryType = iota
	MemoryTypeDecompressBuffer
	MemoryTypeSortIndex
	MemoryTypeRowOffsetMap
)

func (t MemoryType) String() string {
	switch t {
	case MemoryTypeObjectContent:
		return "object_content"
	case MemoryTypeDecompressBuffer:
		return "decompress_buffer"
	case MemoryTypeSortIndex:
		return "sort_index"
	case MemoryTypeRowOffsetMap:
		return "row_offset_map"
	default:
		return "unknown"
	}
}

// MemoryController manages memory allocation for CCPR operations
// It provides memory tracking, limits, and metrics collection
type MemoryController struct {
	mp       *mpool.MPool
	maxBytes int64

	// Current memory usage by type
	objectContentBytes    atomic.Int64
	decompressBufferBytes atomic.Int64
	sortIndexBytes        atomic.Int64
	rowOffsetMapBytes     atomic.Int64
	totalBytes            atomic.Int64

	// Semaphore for limiting concurrent large allocations
	largeSem chan struct{}

	mu sync.RWMutex
}

// NewMemoryController creates a new MemoryController
func NewMemoryController(mp *mpool.MPool, maxBytes int64) *MemoryController {
	mc := &MemoryController{
		mp:       mp,
		maxBytes: maxBytes,
		largeSem: make(chan struct{}, 10), // Allow up to 10 concurrent large allocations
	}

	// Set memory limit gauge
	v2.CCPRMemoryLimitGauge.Set(float64(maxBytes))

	return mc
}

// Alloc allocates memory from the pool with tracking
func (mc *MemoryController) Alloc(ctx context.Context, size int, memType MemoryType) ([]byte, error) {
	startTime := time.Now()

	// For large allocations, acquire semaphore
	isLarge := size > 10*1024*1024 // > 10MB
	if isLarge {
		select {
		case mc.largeSem <- struct{}{}:
			// acquired
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Check if allocation would exceed limit
	currentTotal := mc.totalBytes.Load()
	if mc.maxBytes > 0 && currentTotal+int64(size) > mc.maxBytes {
		waitDuration := time.Since(startTime)
		v2.CCPRMemoryWaitDurationHistogram.Observe(waitDuration.Seconds())
		logutil.Warn("ccpr-memory-controller allocation would exceed limit",
			zap.Int64("current", currentTotal),
			zap.Int("requested", size),
			zap.Int64("limit", mc.maxBytes),
		)
	}

	// Allocate from mpool (offHeap=false for normal allocations)
	data, err := mc.mp.Alloc(size, false)
	if err != nil {
		if isLarge {
			<-mc.largeSem
		}
		return nil, err
	}

	// Update metrics
	mc.updateAllocMetrics(size, memType)

	return data, nil
}

// Free releases memory back to the pool with tracking
func (mc *MemoryController) Free(data []byte, memType MemoryType) {
	if data == nil {
		return
	}

	size := len(data)

	// Free to mpool
	mc.mp.Free(data)

	// Update metrics
	mc.updateFreeMetrics(size, memType)

	// Release semaphore if this was a large allocation
	if size > 10*1024*1024 {
		select {
		case <-mc.largeSem:
		default:
		}
	}
}

// updateAllocMetrics updates allocation metrics
func (mc *MemoryController) updateAllocMetrics(size int, memType MemoryType) {
	sizeInt64 := int64(size)

	// Update type-specific counter
	switch memType {
	case MemoryTypeObjectContent:
		mc.objectContentBytes.Add(sizeInt64)
		v2.CCPRMemoryObjectContentGauge.Add(float64(size))
		v2.CCPRMemoryAllocObjectContentCounter.Inc()
		v2.CCPRMemoryAllocBytesObjectContentCounter.Add(float64(size))
	case MemoryTypeDecompressBuffer:
		mc.decompressBufferBytes.Add(sizeInt64)
		v2.CCPRMemoryDecompressBufferGauge.Add(float64(size))
		v2.CCPRMemoryAllocDecompressBufferCounter.Inc()
		v2.CCPRMemoryAllocBytesDecompressBufferCounter.Add(float64(size))
	case MemoryTypeSortIndex:
		mc.sortIndexBytes.Add(sizeInt64)
		v2.CCPRMemorySortIndexGauge.Add(float64(size))
		v2.CCPRMemoryAllocSortIndexCounter.Inc()
		v2.CCPRMemoryAllocBytesSortIndexCounter.Add(float64(size))
	case MemoryTypeRowOffsetMap:
		mc.rowOffsetMapBytes.Add(sizeInt64)
		v2.CCPRMemoryRowOffsetMapGauge.Add(float64(size))
		v2.CCPRMemoryAllocRowOffsetMapCounter.Inc()
		v2.CCPRMemoryAllocBytesRowOffsetMapCounter.Add(float64(size))
	}

	// Update total
	mc.totalBytes.Add(sizeInt64)
	v2.CCPRMemoryTotalGauge.Add(float64(size))

	// Record allocation size histogram
	v2.CCPRMemoryAllocSizeHistogram.WithLabelValues(memType.String()).Observe(float64(size))
}

// updateFreeMetrics updates free metrics
func (mc *MemoryController) updateFreeMetrics(size int, memType MemoryType) {
	sizeInt64 := int64(size)

	// Update type-specific counter
	switch memType {
	case MemoryTypeObjectContent:
		mc.objectContentBytes.Add(-sizeInt64)
		v2.CCPRMemoryObjectContentGauge.Sub(float64(size))
		v2.CCPRMemoryFreeObjectContentCounter.Inc()
	case MemoryTypeDecompressBuffer:
		mc.decompressBufferBytes.Add(-sizeInt64)
		v2.CCPRMemoryDecompressBufferGauge.Sub(float64(size))
		v2.CCPRMemoryFreeDecompressBufferCounter.Inc()
	case MemoryTypeSortIndex:
		mc.sortIndexBytes.Add(-sizeInt64)
		v2.CCPRMemorySortIndexGauge.Sub(float64(size))
		v2.CCPRMemoryFreeSortIndexCounter.Inc()
	case MemoryTypeRowOffsetMap:
		mc.rowOffsetMapBytes.Add(-sizeInt64)
		v2.CCPRMemoryRowOffsetMapGauge.Sub(float64(size))
		v2.CCPRMemoryFreeRowOffsetMapCounter.Inc()
	}

	// Update total
	mc.totalBytes.Add(-sizeInt64)
	v2.CCPRMemoryTotalGauge.Sub(float64(size))
}

// GetStats returns current memory statistics
func (mc *MemoryController) GetStats() MemoryStats {
	return MemoryStats{
		ObjectContentBytes:    mc.objectContentBytes.Load(),
		DecompressBufferBytes: mc.decompressBufferBytes.Load(),
		SortIndexBytes:        mc.sortIndexBytes.Load(),
		RowOffsetMapBytes:     mc.rowOffsetMapBytes.Load(),
		TotalBytes:            mc.totalBytes.Load(),
		MaxBytes:              mc.maxBytes,
	}
}

// MemoryStats holds memory usage statistics
type MemoryStats struct {
	ObjectContentBytes    int64
	DecompressBufferBytes int64
	SortIndexBytes        int64
	RowOffsetMapBytes     int64
	TotalBytes            int64
	MaxBytes              int64
}

// ============================================================================
// Object Pools for Job and Result objects
// ============================================================================

var (
	// GetChunkJob pool
	getChunkJobPool = sync.Pool{
		New: func() interface{} {
			v2.CCPRPoolGetChunkJobMissCounter.Inc()
			return &GetChunkJob{
				result: make(chan *GetChunkJobResult, 1),
			}
		},
	}

	// GetChunkJobResult pool
	getChunkJobResultPool = sync.Pool{
		New: func() interface{} {
			return &GetChunkJobResult{}
		},
	}

	// WriteObjectJob pool
	writeObjectJobPool = sync.Pool{
		New: func() interface{} {
			v2.CCPRPoolWriteObjectJobMissCounter.Inc()
			return &WriteObjectJob{
				result: make(chan *WriteObjectJobResult, 1),
			}
		},
	}

	// WriteObjectJobResult pool
	writeObjectJobResultPool = sync.Pool{
		New: func() interface{} {
			return &WriteObjectJobResult{}
		},
	}

	// GetMetaJob pool
	getMetaJobPool = sync.Pool{
		New: func() interface{} {
			return &GetMetaJob{
				result: make(chan *GetMetaJobResult, 1),
			}
		},
	}

	// GetMetaJobResult pool
	getMetaJobResultPool = sync.Pool{
		New: func() interface{} {
			return &GetMetaJobResult{}
		},
	}

	// FilterObjectJob pool
	filterObjectJobPool = sync.Pool{
		New: func() interface{} {
			v2.CCPRPoolFilterObjectJobMissCounter.Inc()
			return &FilterObjectJob{
				result: make(chan *FilterObjectJobResult, 1),
			}
		},
	}

	// FilterObjectJobResult pool
	filterObjectJobResultPool = sync.Pool{
		New: func() interface{} {
			return &FilterObjectJobResult{}
		},
	}

	// AObjectMapping pool
	aObjectMappingPool = sync.Pool{
		New: func() interface{} {
			v2.CCPRPoolAObjectMappingMissCounter.Inc()
			return &AObjectMapping{}
		},
	}
)

// AcquireGetChunkJob gets a GetChunkJob from the pool
func AcquireGetChunkJob() *GetChunkJob {
	job := getChunkJobPool.Get().(*GetChunkJob)
	v2.CCPRPoolGetChunkJobHitCounter.Inc()
	return job
}

// ReleaseGetChunkJob returns a GetChunkJob to the pool
func ReleaseGetChunkJob(job *GetChunkJob) {
	if job == nil {
		return
	}
	// Clear state
	job.ctx = nil
	job.upstreamExecutor = nil
	job.objectName = ""
	job.chunkIndex = 0
	job.subscriptionAccountName = ""
	job.pubName = ""
	// Drain channel if needed
	select {
	case <-job.result:
	default:
	}
	getChunkJobPool.Put(job)
}

// AcquireGetChunkJobResult gets a GetChunkJobResult from the pool
func AcquireGetChunkJobResult() *GetChunkJobResult {
	return getChunkJobResultPool.Get().(*GetChunkJobResult)
}

// ReleaseGetChunkJobResult returns a GetChunkJobResult to the pool
func ReleaseGetChunkJobResult(res *GetChunkJobResult) {
	if res == nil {
		return
	}
	res.ChunkData = nil
	res.ChunkIndex = 0
	res.Err = nil
	getChunkJobResultPool.Put(res)
}

// AcquireWriteObjectJob gets a WriteObjectJob from the pool
func AcquireWriteObjectJob() *WriteObjectJob {
	job := writeObjectJobPool.Get().(*WriteObjectJob)
	v2.CCPRPoolWriteObjectJobHitCounter.Inc()
	return job
}

// ReleaseWriteObjectJob returns a WriteObjectJob to the pool
func ReleaseWriteObjectJob(job *WriteObjectJob) {
	if job == nil {
		return
	}
	job.ctx = nil
	job.localFS = nil
	job.objectName = ""
	job.objectContent = nil
	job.ccprCache = nil
	job.txnID = nil
	select {
	case <-job.result:
	default:
	}
	writeObjectJobPool.Put(job)
}

// AcquireWriteObjectJobResult gets a WriteObjectJobResult from the pool
func AcquireWriteObjectJobResult() *WriteObjectJobResult {
	return writeObjectJobResultPool.Get().(*WriteObjectJobResult)
}

// ReleaseWriteObjectJobResult returns a WriteObjectJobResult to the pool
func ReleaseWriteObjectJobResult(res *WriteObjectJobResult) {
	if res == nil {
		return
	}
	res.Err = nil
	writeObjectJobResultPool.Put(res)
}

// AcquireGetMetaJob gets a GetMetaJob from the pool
func AcquireGetMetaJob() *GetMetaJob {
	return getMetaJobPool.Get().(*GetMetaJob)
}

// ReleaseGetMetaJob returns a GetMetaJob to the pool
func ReleaseGetMetaJob(job *GetMetaJob) {
	if job == nil {
		return
	}
	job.ctx = nil
	job.upstreamExecutor = nil
	job.objectName = ""
	job.subscriptionAccountName = ""
	job.pubName = ""
	select {
	case <-job.result:
	default:
	}
	getMetaJobPool.Put(job)
}

// AcquireGetMetaJobResult gets a GetMetaJobResult from the pool
func AcquireGetMetaJobResult() *GetMetaJobResult {
	return getMetaJobResultPool.Get().(*GetMetaJobResult)
}

// ReleaseGetMetaJobResult returns a GetMetaJobResult to the pool
func ReleaseGetMetaJobResult(res *GetMetaJobResult) {
	if res == nil {
		return
	}
	res.MetadataData = nil
	res.TotalSize = 0
	res.ChunkIndex = 0
	res.TotalChunks = 0
	res.IsComplete = false
	res.Err = nil
	getMetaJobResultPool.Put(res)
}

// AcquireFilterObjectJob gets a FilterObjectJob from the pool
func AcquireFilterObjectJob() *FilterObjectJob {
	job := filterObjectJobPool.Get().(*FilterObjectJob)
	v2.CCPRPoolFilterObjectJobHitCounter.Inc()
	return job
}

// ReleaseFilterObjectJob returns a FilterObjectJob to the pool
func ReleaseFilterObjectJob(job *FilterObjectJob) {
	if job == nil {
		return
	}
	job.ctx = nil
	job.objectStatsBytes = nil
	job.snapshotTS = types.TS{}
	job.upstreamExecutor = nil
	job.isTombstone = false
	job.localFS = nil
	job.mp = nil
	job.getChunkWorker = nil
	job.writeObjectWorker = nil
	job.subscriptionAccountName = ""
	job.pubName = ""
	job.ccprCache = nil
	job.txnID = nil
	job.aobjectMap = nil
	job.ttlChecker = nil
	select {
	case <-job.result:
	default:
	}
	filterObjectJobPool.Put(job)
}

// AcquireFilterObjectJobResult gets a FilterObjectJobResult from the pool
func AcquireFilterObjectJobResult() *FilterObjectJobResult {
	return filterObjectJobResultPool.Get().(*FilterObjectJobResult)
}

// ReleaseFilterObjectJobResult returns a FilterObjectJobResult to the pool
func ReleaseFilterObjectJobResult(res *FilterObjectJobResult) {
	if res == nil {
		return
	}
	res.Err = nil
	res.HasMappingUpdate = false
	res.UpstreamAObjUUID = nil
	res.PreviousStats = objectio.ObjectStats{}
	res.CurrentStats = objectio.ObjectStats{}
	res.DownstreamStats = objectio.ObjectStats{}
	res.RowOffsetMap = nil
	filterObjectJobResultPool.Put(res)
}

// AcquireAObjectMapping gets an AObjectMapping from the pool
func AcquireAObjectMapping() *AObjectMapping {
	mapping := aObjectMappingPool.Get().(*AObjectMapping)
	v2.CCPRPoolAObjectMappingHitCounter.Inc()
	return mapping
}

// ReleaseAObjectMapping returns an AObjectMapping to the pool
func ReleaseAObjectMapping(mapping *AObjectMapping) {
	if mapping == nil {
		return
	}
	mapping.DownstreamStats = objectio.ObjectStats{}
	mapping.IsTombstone = false
	mapping.DBName = ""
	mapping.TableName = ""
	mapping.RowOffsetMap = nil
	aObjectMappingPool.Put(mapping)
}

// ============================================================================
// Global Memory Controller Instance
// ============================================================================

var (
	globalMemoryController     *MemoryController
	globalMemoryControllerOnce sync.Once
	globalMemoryControllerMu   sync.RWMutex
)

// GetGlobalMemoryController returns the global memory controller instance
func GetGlobalMemoryController() *MemoryController {
	globalMemoryControllerMu.RLock()
	mc := globalMemoryController
	globalMemoryControllerMu.RUnlock()
	if mc != nil {
		return mc
	}
	return nil
}

// SetGlobalMemoryController sets the global memory controller instance
func SetGlobalMemoryController(mc *MemoryController) {
	globalMemoryControllerMu.Lock()
	globalMemoryController = mc
	globalMemoryControllerMu.Unlock()
}

// InitGlobalMemoryController initializes the global memory controller
func InitGlobalMemoryController(mp *mpool.MPool, maxBytes int64) *MemoryController {
	globalMemoryControllerOnce.Do(func() {
		globalMemoryController = NewMemoryController(mp, maxBytes)
	})
	return globalMemoryController
}
