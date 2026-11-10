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
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"

	"go.uber.org/zap"
)

// ErrSyncProtectionTTLExpired is returned when sync protection TTL has expired
var ErrSyncProtectionTTLExpired = moerr.NewInternalErrorNoCtx("sync protection TTL expired")

// CCPRTxnCacheWriter is an interface for writing objects to CCPR transaction cache
// This interface is implemented by disttae.CCPRTxnCache
type CCPRTxnCacheWriter interface {
	// WriteObject checks if an object needs to be written and registers it in the cache.
	// Does NOT write the file - caller should write the file when isNewFile=true.
	// Returns isNewFile (true if file needs to be written) and any error.
	WriteObject(ctx context.Context, objectName string, txnID []byte) (isNewFile bool, err error)
	// OnFileWritten is called after the file has been successfully written to fileservice.
	OnFileWritten(objectName string)
}

// FilterObjectResult holds the result of FilterObject
type FilterObjectResult struct {
	HasMappingUpdate bool
	UpstreamAObjUUID *objectio.ObjectId
	PreviousStats    objectio.ObjectStats
	CurrentStats     objectio.ObjectStats
	// DownstreamStats holds the stats for non-appendable objects that were written to fileservice
	DownstreamStats objectio.ObjectStats
	// RowOffsetMap maps original rowoffset to new rowoffset after sorting
	// Key: original rowoffset, Value: new rowoffset
	RowOffsetMap map[uint32]uint32
}

// FilterObject filters an object based on snapshot TS
// Input: object stats (as bytes), snapshot TS, and whether it's an aobj (checked from object stats)
// For aobj: gets object file, converts to batch, filters by snapshot TS, creates new object
// The mapping between new UUID and upstream aobj is stored in ccprCache.aobjectMap
// For nobj: writes directly to fileservice with new UUID
// ccprCache: optional CCPR transaction cache for atomic write and registration (can be nil)
// txnID: transaction ID for CCPR cache registration (can be nil)
// aobjectMap: mapping from upstream aobj to downstream object stats (used for tombstone rowid rewriting)
// ttlChecker: optional function to check if sync protection TTL has expired (can be nil)
func FilterObject(
	ctx context.Context,
	objectStatsBytes []byte,
	snapshotTS types.TS,
	upstreamExecutor SQLExecutor,
	isTombstone bool,
	localFS fileservice.FileService,
	mp *mpool.MPool,
	getChunkWorker GetChunkWorker,
	writeObjectWorker WriteObjectWorker,
	subscriptionAccountName string,
	pubName string,
	ccprCache CCPRTxnCacheWriter,
	txnID []byte,
	aobjectMap *AObjectMap,
	ttlChecker TTLChecker,
) (*FilterObjectResult, error) {
	// Check TTL before processing
	if ttlChecker != nil && !ttlChecker() {
		return nil, ErrSyncProtectionTTLExpired
	}

	if len(objectStatsBytes) != objectio.ObjectStatsLen {
		return nil, moerr.NewInternalErrorf(ctx, "invalid object stats length: expected %d, got %d", objectio.ObjectStatsLen, len(objectStatsBytes))
	}

	// Parse ObjectStats from bytes
	var stats objectio.ObjectStats
	stats.UnMarshal(objectStatsBytes)

	var minimum int64
	for {
		result, err := func() (*FilterObjectResult, error) {
			if ttlChecker != nil && !ttlChecker() {
				return nil, ErrSyncProtectionTTLExpired
			}
			memory, release, err := reservePublicationObject(ctx, &stats, minimum, isTombstone && !stats.GetAppendable() && aobjectMap != nil)
			if err != nil {
				return nil, err
			}
			defer release()
			ctx := context.WithValue(ctx, publicationFetchMemoryKey{}, memory)
			// Check if it's an appendable object
			isAObj := stats.GetAppendable()
			if isAObj {
				// Handle appendable object
				return filterAppendableObject(ctx, &stats, snapshotTS, upstreamExecutor, localFS, isTombstone, mp, getChunkWorker, subscriptionAccountName, pubName, aobjectMap, ttlChecker)
			} else {
				// Handle non-appendable object - write directly to fileservice with new UUID
				// For tombstone, need to convert to batch and rewrite rowids using aobjectMap
				newStats, err := filterNonAppendableObject(ctx, &stats, snapshotTS, upstreamExecutor, localFS, isTombstone, mp, getChunkWorker, writeObjectWorker, subscriptionAccountName, pubName, ccprCache, txnID, aobjectMap, ttlChecker)
				if err != nil {
					return nil, err
				}
				// Return the new downstream stats with new object name
				return &FilterObjectResult{
					DownstreamStats: newStats,
				}, nil
			}
		}()
		var growth *objectMemoryGrowth
		if !errors.As(err, &growth) {
			return result, err
		}
		minimum = growth.bytes
	}
}

// filterAppendableObject handles appendable objects
// Gets object file from upstream, converts to batch, filters by snapshot TS, creates new object
// Returns the mapping update info for storage in ccprCache.aobjectMap
// For tombstone objects, rewrites delete rowids using aobjectMap
func filterAppendableObject(
	ctx context.Context,
	stats *objectio.ObjectStats,
	snapshotTS types.TS,
	upstreamExecutor SQLExecutor,
	localFS fileservice.FileService,
	isTombstone bool,
	mp *mpool.MPool,
	getChunkWorker GetChunkWorker,
	subscriptionAccountName string,
	pubName string,
	aobjectMap *AObjectMap,
	ttlChecker TTLChecker,
) (*FilterObjectResult, error) {
	// Check TTL before processing
	if ttlChecker != nil && !ttlChecker() {
		return nil, ErrSyncProtectionTTLExpired
	}

	// Get object name from stats (upstream aobj UUID)
	upstreamAObjUUID := stats.ObjectName().ObjectId()

	// Get object file from upstream using GETOBJECT
	objectContent, err := GetObjectFromUpstreamWithWorker(ctx, upstreamExecutor, stats.ObjectName().String(), getChunkWorker, subscriptionAccountName, pubName)
	if err != nil {
		return nil, errutil.Wrap(err, "failed to get object from upstream")
	}

	// Check TTL after getting object
	if ttlChecker != nil && !ttlChecker() {
		return nil, ErrSyncProtectionTTLExpired
	}

	// Extract sortkey from original object metadata
	sortKeySeqnum, err := extractSortKeyFromObject(ctx, objectContent, stats)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to extract sortkey from object: %v", err)
	}

	// Convert object file to batch
	bat, err := convertObjectToBatch(ctx, objectContent, stats, snapshotTS, localFS, mp)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to convert object to batch: %v", err)
	}
	defer bat.Close()

	// Filter batch by snapshot TS
	filteredBat, err := filterBatchBySnapshotTS(ctx, bat, snapshotTS, mp)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to filter batch by snapshot TS: %v", err)
	}
	defer filteredBat.Close()

	// For tombstone objects, rewrite delete rowids using aobjectMap
	if isTombstone && aobjectMap != nil {
		if err := rewriteTombstoneRowids(ctx, filteredBat, aobjectMap, mp); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to rewrite tombstone rowids: %v", err)
		}
	}

	// Sort batch by primary key, remove commit TS column, write to file, and record ObjectStats
	// This is data object (not tombstone), so use SchemaData
	// Use new object name for appendable objects (keepOriginalName=false)
	objStats, rowOffsetMap, err := createObjectFromBatch(ctx, filteredBat, stats, snapshotTS, isTombstone, localFS, mp, sortKeySeqnum, false)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to create object from batch: %v", err)
	}

	// Return mapping update info for storage in ccprCache.aobjectMap
	return &FilterObjectResult{
		HasMappingUpdate: true,
		UpstreamAObjUUID: upstreamAObjUUID,
		CurrentStats:     objStats, // New current stats
		RowOffsetMap:     rowOffsetMap,
	}, nil
}

// AObjectMapping represents a mapping from upstream aobj to downstream object stats
type AObjectMapping struct {
	DownstreamStats objectio.ObjectStats
	IsTombstone     bool
	DBName          string
	TableName       string
	// RowOffsetMap maps original rowoffset to new rowoffset after sorting
	// Key: original rowoffset, Value: new rowoffset
	RowOffsetMap map[uint32]uint32
}

// AObjectMap stores the mapping from upstream aobj to downstream object stats
// Key: upstreamID (string), Value: *AObjectMapping
// This map is used to track appendable object transformations during CCPR sync
type AObjectMap struct {
	mu sync.RWMutex
	m  map[string]*AObjectMapping
}

// NewAObjectMap creates a new AObjectMap instance
func NewAObjectMap() *AObjectMap {
	return &AObjectMap{m: make(map[string]*AObjectMapping)}
}

// Get retrieves the mapping for an upstream aobj
func (am *AObjectMap) Get(upstreamID string) (*AObjectMapping, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	mapping, exists := am.m[upstreamID]
	return mapping, exists
}

// Set stores or updates the mapping for an upstream aobj
func (am *AObjectMap) Set(upstreamID string, mapping *AObjectMapping) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.m[upstreamID] = mapping
}

// Delete removes the mapping for an upstream aobj
func (am *AObjectMap) Delete(upstreamID string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	delete(am.m, upstreamID)
}

// Range iterates over all entries in the map, calling f for each.
// The callback receives a copy of each key-value pair.
func (am *AObjectMap) Range(f func(upstreamID string, mapping *AObjectMapping)) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	for k, v := range am.m {
		f(k, v)
	}
}

// rewriteTombstoneRowids rewrites delete rowids in tombstone batch using aobjectMap
// For each rowid, extract the object ID and check if it exists in aobjectMap
// If found, replace the segment ID in rowid with the downstream object's segment ID
func rewriteTombstoneRowids(
	ctx context.Context,
	bat *containers.Batch,
	aobjectMap *AObjectMap,
	mp *mpool.MPool,
) error {
	if bat == nil || bat.Length() == 0 || aobjectMap == nil {
		return nil
	}

	// Tombstone schema: first column is delete rowid (TombstoneAttr_Rowid_Attr)
	// The rowid contains the object ID of the data object being deleted
	rowidVec := bat.Vecs[0]
	if rowidVec == nil || rowidVec.Length() == 0 {
		return nil
	}

	// Verify the column type is Rowid
	if rowidVec.GetType().Oid != types.T_Rowid {
		return moerr.NewInternalErrorf(ctx, "first column of tombstone should be rowid, got %s", rowidVec.GetType().String())
	}

	// Get rowid values from the vector
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](rowidVec.GetDownstreamVector())

	// Iterate through each rowid and rewrite if mapping exists
	for i := range rowids {
		// Extract object ID from rowid
		upstreamObjID := rowids[i].BorrowObjectID()
		upstreamIDStr := upstreamObjID.String()

		// Check if this object ID has a mapping in aobjectMap
		if mapping, exists := aobjectMap.Get(upstreamIDStr); exists {
			// Get downstream object ID from mapping
			downstreamObjID := mapping.DownstreamStats.ObjectName().ObjectId()

			// Replace the segment ID in rowid with downstream object's segment ID
			// Rowid structure: [SegmentID (16 bytes)][ObjOffset (2 bytes)][BlkOffset (2 bytes)][RowOffset (4 bytes)]
			// We need to replace the first 18 bytes (SegmentID + ObjOffset) with downstream object ID
			rowids[i].SetSegment(types.Segmentid(*downstreamObjID.Segment()))
			rowids[i].SetObjOffset(downstreamObjID.Offset())

			// Rewrite row offset using RowOffsetMap if available
			// The RowOffsetMap maps original rowoffset to new rowoffset after sorting
			if mapping.RowOffsetMap != nil {
				oldRowOffset := rowids[i].GetRowOffset()
				if newRowOffset, ok := mapping.RowOffsetMap[oldRowOffset]; ok {
					rowids[i].SetRowOffset(newRowOffset)
				}
			}
		} else {
			logutil.Warn("ccpr-tombstone: skipping rowid rewrite for unmapped appendable object",
				zap.String("upstreamID", upstreamIDStr),
			)
		}
	}

	// Re-sort tombstone batch by rowid after rewriting
	// Tombstones must be sorted by rowid (TombstonePrimaryKeyIdx = 0) as required by flush
	n := rowidVec.Length()
	sortedIdx := make([]int64, n)
	for i := 0; i < n; i++ {
		sortedIdx[i] = int64(i)
	}
	sort.Sort(false, false, false, sortedIdx, rowidVec.GetDownstreamVector())

	// Shuffle all vectors in the batch using the sorted index
	for i := 0; i < len(bat.Vecs); i++ {
		if err := bat.Vecs[i].GetDownstreamVector().Shuffle(sortedIdx, mp); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to shuffle tombstone column %d: %v", i, err)
		}
	}

	return nil
}

// filterNonAppendableObject handles non-appendable objects
// For data objects: writes directly to fileservice with the original object name
// For tombstone objects: reads blocks one by one, rewrites rowids using aobjectMap,
// writes to a sorted sinker with the original object name
// Returns the ObjectStats (original for data, new for tombstone)
func filterNonAppendableObject(
	ctx context.Context,
	stats *objectio.ObjectStats,
	snapshotTS types.TS,
	upstreamExecutor SQLExecutor,
	localFS fileservice.FileService,
	isTombstone bool,
	mp *mpool.MPool,
	getChunkWorker GetChunkWorker,
	writeObjectWorker WriteObjectWorker,
	subscriptionAccountName string,
	pubName string,
	ccprCache CCPRTxnCacheWriter,
	txnID []byte,
	aobjectMap *AObjectMap,
	ttlChecker TTLChecker,
) (objectio.ObjectStats, error) {
	if ttlChecker != nil && !ttlChecker() {
		return objectio.ObjectStats{}, ErrSyncProtectionTTLExpired
	}
	memory, _ := ctx.Value(publicationFetchMemoryKey{}).(*rscthrottler.InputLease)
	// Get upstream object name from stats
	upstreamObjectName := stats.ObjectName().String()

	objectContent, err := GetObjectFromUpstreamWithWorker(ctx, upstreamExecutor, upstreamObjectName, getChunkWorker, subscriptionAccountName, pubName)
	if err != nil {
		return objectio.ObjectStats{}, errutil.Wrap(err, "failed to get object from upstream")
	}

	// Check TTL after getting object
	if ttlChecker != nil && !ttlChecker() {
		return objectio.ObjectStats{}, ErrSyncProtectionTTLExpired
	}

	// Tombstone RowIDs may change order across source blocks after remapping.
	if isTombstone && aobjectMap != nil {
		objStats, err := rewriteNonAppendableTombstoneWithSinker(
			ctx, objectContent, stats, localFS, mp, aobjectMap, memory,
		)
		if err != nil {
			return objectio.ObjectStats{}, err
		}
		return objStats, nil
	}

	// For data objects (or tombstone without aobjectMap), write directly to fileservice
	writeJob := NewWriteObjectJob(ctx, localFS, upstreamObjectName, objectContent, ccprCache, txnID)
	if writeObjectWorker != nil {
		if err := writeObjectWorker.SubmitWriteObject(writeJob); err != nil {
			return objectio.ObjectStats{}, err
		}
	} else {
		writeJob.Execute()
	}
	writeResult := writeJob.WaitDone().(*WriteObjectJobResult)
	if writeResult.Err != nil {
		return objectio.ObjectStats{}, writeResult.Err
	}

	// Return original stats (no need to create new stats since we use the same object name)
	return *stats, nil
}

// GetObjectFromUpstreamWithWorker gets object file from upstream using GETOBJECT SQL with worker pool
var GetObjectFromUpstreamWithWorker = func(
	ctx context.Context,
	upstreamExecutor SQLExecutor,
	objectName string,
	getChunkWorker GetChunkWorker,
	subscriptionAccountName string,
	pubName string,
) ([]byte, error) {
	return fetchObjectChunks(ctx, upstreamExecutor, objectName, getChunkWorker, subscriptionAccountName, pubName, GetChunkSize)
}

// chunkSize follows the GETOBJECT protocol in frontend/get_object.go. Keeping
// geometry explicit also permits small tests of the same multi-chunk path.
func fetchObjectChunks(ctx context.Context, upstreamExecutor SQLExecutor, objectName string, getChunkWorker GetChunkWorker, subscriptionAccountName, pubName string, chunkSize int64) ([]byte, error) {
	if upstreamExecutor == nil {
		return nil, moerr.NewInternalError(ctx, "upstream executor is nil")
	}
	meta, err := getMetaWithRetry(ctx, upstreamExecutor, objectName, getChunkWorker, subscriptionAccountName, pubName)
	if err != nil {
		return nil, err
	}
	expectedChunks, err := objectChunkCount(meta.TotalSize, chunkSize)
	if err != nil {
		return nil, err
	}
	if meta.TotalChunks != expectedChunks {
		return nil, moerr.NewInternalErrorNoCtx("inconsistent GETOBJECT chunk count")
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	memory, ok := ctx.Value(publicationFetchMemoryKey{}).(*rscthrottler.InputLease)
	if !ok || memory == nil {
		return nil, moerr.NewInternalErrorNoCtx("publication fetch requires an object reservation")
	}
	peak, err := publicationBufferBytes(meta.TotalSize)
	if err != nil {
		return nil, err
	}
	if err := growPublicationObject(ctx, memory, peak); err != nil {
		return nil, err
	}

	objectContent := make([]byte, int(meta.TotalSize))
	limit := min(GetGetChunkWorkerThread(), cap(getOrCreateChunkSemaphore()))
	if getChunkWorker == nil {
		limit = 1
	}
	limit = int(min(int64(max(1, limit)), meta.TotalChunks))
	completed := make(chan *GetChunkJobResult, limit)
	ctx, cancel := context.WithCancel(ctx)
	pending := 0
	defer func() {
		cancel()
		// Every accepted job publishes after query/semaphore cleanup, even
		// when submission fails or the worker stops. Keep the owner until then.
		for pending > 0 {
			<-completed
			pending--
		}
	}()
	next := int64(1)
	submit := func() error {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		job := NewGetChunkJob(ctx, upstreamExecutor, objectName, next, subscriptionAccountName, pubName)
		job.result = completed
		if getChunkWorker != nil {
			if err := getChunkWorker.SubmitGetChunk(job); err != nil {
				return err
			}
		} else {
			job.Execute()
		}
		pending++
		next++
		return nil
	}
	for pending < limit && next <= meta.TotalChunks {
		if err := submit(); err != nil {
			return nil, err
		}
	}
	for pending > 0 {
		res := <-completed
		pending--
		data := res.ChunkData
		if res.Err != nil {
			if !(DefaultClassifier{}).IsRetryable(res.Err) {
				return nil, res.Err
			}
			data, err = getChunkWithRetry(ctx, upstreamExecutor, objectName, res.ChunkIndex, getChunkWorker, subscriptionAccountName, pubName)
			if err != nil {
				return nil, err
			}
		}
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		if res.ChunkIndex < 1 || res.ChunkIndex > meta.TotalChunks {
			return nil, moerr.NewInternalErrorNoCtx("invalid GETOBJECT chunk index")
		}
		start := (res.ChunkIndex - 1) * chunkSize
		end := start + min(chunkSize, meta.TotalSize-start)
		if int64(len(data)) != end-start {
			return nil, moerr.NewInternalErrorNoCtxf("invalid GETOBJECT chunk %d length %d", res.ChunkIndex, len(data))
		}
		copy(objectContent[start:end], data)
		res.ChunkData = nil
		if next <= meta.TotalChunks {
			if err := submit(); err != nil {
				return nil, err
			}
		}
	}
	return objectContent, nil
}

// ObjectStats stores file size as uint32. Validate before allocating or
// multiplying offsets; an empty file still has one protocol data chunk.

// getMetaWithRetry gets object metadata with retry logic
func getMetaWithRetry(
	ctx context.Context,
	upstreamExecutor SQLExecutor,
	objectName string,
	getChunkWorker GetChunkWorker,
	subscriptionAccountName string,
	pubName string,
) (*GetMetaJobResult, error) {
	const maxRetries = 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		metaJob := NewGetMetaJob(ctx, upstreamExecutor, objectName, subscriptionAccountName, pubName)
		if getChunkWorker != nil {
			if err := getChunkWorker.SubmitGetChunk(metaJob); err != nil {
				return nil, err
			}
		} else {
			metaJob.Execute()
		}
		metaResult := metaJob.WaitDone().(*GetMetaJobResult)
		if metaResult.Err == nil {
			return metaResult, nil
		}

		lastErr = metaResult.Err
		if !(DefaultClassifier{}).IsRetryable(lastErr) {
			return nil, lastErr
		}
	}

	return nil, moerr.NewInternalErrorf(ctx, "failed to get object metadata after %d retries: %v", maxRetries, lastErr)
}

// getChunkWithRetry gets a single chunk with retry logic
func getChunkWithRetry(
	ctx context.Context,
	upstreamExecutor SQLExecutor,
	objectName string,
	chunkIndex int64,
	getChunkWorker GetChunkWorker,
	subscriptionAccountName string,
	pubName string,
) ([]byte, error) {
	const maxRetries = 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		chunkJob := NewGetChunkJob(ctx, upstreamExecutor, objectName, chunkIndex, subscriptionAccountName, pubName)
		if getChunkWorker != nil {
			if err := getChunkWorker.SubmitGetChunk(chunkJob); err != nil {
				return nil, err
			}
		} else {
			chunkJob.Execute()
		}
		chunkResult := chunkJob.WaitDone().(*GetChunkJobResult)
		if chunkResult.Err == nil {
			return chunkResult.ChunkData, nil
		}

		lastErr = chunkResult.Err
		if !(DefaultClassifier{}).IsRetryable(lastErr) {
			return nil, lastErr
		}
	}

	return nil, moerr.NewInternalErrorf(ctx, "failed to get chunk %d after %d retries: %v", chunkIndex, maxRetries, lastErr)
}

// extractSortKeyFromObject extracts sortkey seqnum from object metadata
func extractSortKeyFromObject(
	ctx context.Context,
	objectContent []byte,
	stats *objectio.ObjectStats,
) (uint16, error) {
	// Read object meta from objectContent bytes
	metaExtent := stats.Extent()
	if int(metaExtent.Offset()+metaExtent.Length()) > len(objectContent) {
		return 0, moerr.NewInternalErrorf(ctx, "object content too small for meta extent")
	}
	metaBytes := objectContent[metaExtent.Offset() : metaExtent.Offset()+metaExtent.Length()]

	// Check if meta needs decompression
	var decompressedMetaBytes []byte
	var decompressedBuf fscache.Data
	if metaExtent.Alg() == compress.None {
		decompressedMetaBytes = metaBytes
	} else {
		// Allocate buffer for decompressed data
		allocator := fileservice.DefaultCacheDataAllocator()
		decompressedBuf = allocator.AllocateCacheDataWithHint(ctx, int(metaExtent.OriginSize()), malloc.NoClear)
		bs, err := compress.Decompress(metaBytes, decompressedBuf.Bytes(), compress.Lz4)
		if err != nil {
			if decompressedBuf != nil {
				decompressedBuf.Release()
			}
			return 0, moerr.NewInternalErrorf(ctx, "failed to decompress meta data: %v", err)
		}
		decompressedMetaBytes = decompressedBuf.Bytes()[:len(bs)]
		// Clone the data to ensure meta doesn't hold reference to buffer
		decompressedMetaBytes = append([]byte(nil), decompressedMetaBytes...)
		if decompressedBuf != nil {
			decompressedBuf.Release()
		}
	}

	meta := objectio.MustObjectMeta(decompressedMetaBytes)
	dataMeta := meta.MustGetMeta(objectio.SchemaData)

	// Get sortkey seqnum from block header
	sortKeySeqnum := dataMeta.BlockHeader().SortKey()
	return sortKeySeqnum, nil
}

// Rewrite and globally sort under one admitted job slot. A single file sinker
// owns the fixed-name object through the final Sync; source blocks never Sync it.
func rewriteNonAppendableTombstoneWithSinker(
	ctx context.Context,
	objectContent []byte,
	stats *objectio.ObjectStats,
	localFS fileservice.FileService,
	mp *mpool.MPool,
	aobjectMap *AObjectMap,
	memory *rscthrottler.InputLease,
) (objectio.ObjectStats, error) {
	if err := context.Cause(ctx); err != nil {
		return objectio.ObjectStats{}, err
	}
	// Step 1: Parse object metadata
	metaExtent := stats.Extent()
	if int(metaExtent.Offset()+metaExtent.Length()) > len(objectContent) {
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "object content too small for meta extent")
	}
	if memory == nil {
		return objectio.ObjectStats{}, moerr.NewInternalErrorNoCtx("publication rewrite requires a memory slot")
	}
	metaBytes := objectContent[metaExtent.Offset() : metaExtent.Offset()+metaExtent.Length()]

	// Decompress meta if needed
	var decompressedMetaBytes []byte
	var decompressedMetaBuf fscache.Data
	if metaExtent.Alg() == compress.None {
		decompressedMetaBytes = metaBytes
	} else {
		allocator := fileservice.DefaultCacheDataAllocator()
		decompressedMetaBuf = allocator.AllocateCacheDataWithHint(ctx, int(metaExtent.OriginSize()), malloc.NoClear)
		bs, err := compress.Decompress(metaBytes, decompressedMetaBuf.Bytes(), compress.Lz4)
		if err != nil {
			if decompressedMetaBuf != nil {
				decompressedMetaBuf.Release()
			}
			return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to decompress meta data: %v", err)
		}
		decompressedMetaBytes = decompressedMetaBuf.Bytes()[:len(bs)]
		decompressedMetaBytes = append([]byte(nil), decompressedMetaBytes...)
		if decompressedMetaBuf != nil {
			decompressedMetaBuf.Release()
		}
	}

	meta := objectio.MustObjectMeta(decompressedMetaBytes)
	dataMeta := meta.MustGetMeta(objectio.SchemaData)
	blkCnt := dataMeta.BlockCount()
	if blkCnt == 0 {
		return objectio.ObjectStats{}, nil
	}

	// CN-created tombstones retain RowID and PK; commit timestamps are not
	// needed after the upstream non-appendable object is visible in the snapshot.
	firstBlkMeta := dataMeta.GetBlockMeta(0)
	maxSeqnum := firstBlkMeta.GetMaxSeqnum()
	cols := []uint16{objectio.TombstoneAttr_Rowid_SeqNum, objectio.TombstoneAttr_PK_SeqNum}
	typs := []types.Type{types.T_Rowid.ToType(), types.T(firstBlkMeta.ColumnMeta(objectio.TombstoneAttr_PK_SeqNum).DataType()).ToType()}
	// Serialized constants can be tiny while their flat vectors still need
	// rows*TypeSize backing. Admit both before decoding any source vectors.
	payload := int64(len(objectContent)) + int64(metaExtent.OriginSize())
	for block := uint32(0); block < blkCnt; block++ {
		meta := dataMeta.GetBlockMeta(block)
		if meta.GetRows() > objectio.BlockMaxRows {
			return objectio.ObjectStats{}, moerr.NewInternalErrorNoCtx("publication source block exceeds row limit")
		}
		for i, col := range cols {
			payload += int64(meta.ColumnMeta(col).Location().OriginSize()) + int64(meta.GetRows())*int64(typs[i].TypeSize())
		}
	}
	peak, err := ioutil.IOEnvelope([][]types.Type{typs}, 1, payload)
	if err != nil {
		return objectio.ObjectStats{}, err
	}
	if err := growPublicationObject(ctx, memory, peak); err != nil {
		return objectio.ObjectStats{}, err
	}
	var inputs []*batch.Batch
	defer func() {
		for _, input := range inputs {
			if input != nil {
				input.Clean(mp)
			}
		}
	}()
	allocator := fileservice.DefaultCacheDataAllocator()
	for blkIdx := uint32(0); blkIdx < blkCnt; blkIdx++ {
		if err := context.Cause(ctx); err != nil {
			return objectio.ObjectStats{}, err
		}
		blkBat, err := readSingleBlockToBatch(ctx, objectContent, dataMeta, blkIdx, cols, typs, maxSeqnum, allocator, mp)
		if err != nil {
			return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to read block %d: %v", blkIdx, err)
		}

		inputs = append(inputs, blkBat)
		if err := rewriteTombstoneRowidsBatch(ctx, blkBat, aobjectMap, mp); err != nil {
			return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to rewrite tombstone rowids for block %d: %v", blkIdx, err)
		}
		if err := mergeutil.SortColumnsByIndex(blkBat.Vecs, objectio.TombstonePrimaryKeyIdx, mp); err != nil {
			return objectio.ObjectStats{}, err
		}
	}
	// Before merge/encoding, charge expanded constant and alias payloads, not
	// just the serialized source size. Growth never waits behind these inputs.
	payload = int64(len(objectContent)) + int64(metaExtent.OriginSize())
	for _, input := range inputs {
		for _, vec := range input.Vecs {
			payload += vec.WindowSize(0, input.RowCount())
		}
	}
	peak, err = ioutil.IOEnvelope([][]types.Type{typs}, 1, payload)
	if err != nil {
		return objectio.ObjectStats{}, err
	}
	if err := growPublicationObject(ctx, memory, peak); err != nil {
		return objectio.ObjectStats{}, err
	}
	file := newTombstoneFSinkerFactoryWithName(stats.ObjectName(), objectio.HiddenColumnSelection_None)(mp, localFS)
	defer file.Close()
	attrs, _ := objectio.GetTombstoneSchema(typs[1], objectio.HiddenColumnSelection_None)
	buffer := batch.NewWithSchema(false, attrs, typs)
	defer buffer.Clean(mp)
	// The admitted complete-job peak covers each full output block. The
	// ordinary byte target would create invalid partial intermediate blocks.
	_, err = mergeutil.MergeSortBatches(inputs, objectio.TombstonePrimaryKeyIdx, buffer,
		func(data *batch.Batch) (*batch.Batch, error) {
			if err := context.Cause(ctx); err != nil {
				return data, err
			}
			if err := file.Sink(ctx, data); err != nil {
				return data, err
			}
			data.CleanOnlyData()
			return data, nil
		}, mp, func(i int) { inputs[i].Clean(mp); inputs[i] = nil }, peak)
	if err != nil {
		return objectio.ObjectStats{}, err
	}
	result, err := file.Sync(ctx)
	if err != nil {
		return objectio.ObjectStats{}, err
	}
	if result == nil {
		return objectio.ObjectStats{}, moerr.NewInternalErrorNoCtx("empty publication tombstone object")
	}
	return *result, nil
}

// newTombstoneFSinkerFactoryWithName creates a FileSinkerFactory for tombstone
// that uses a specified object name instead of generating a new one
func newTombstoneFSinkerFactoryWithName(
	objectName objectio.ObjectName,
	hidden objectio.HiddenColumnSelection,
) ioutil.FileSinkerFactory {
	return func(mp *mpool.MPool, fs fileservice.FileService) ioutil.FileSinker {
		return &tombstoneFSinkerWithName{
			objectName:      objectName,
			hiddenSelection: hidden,
			mp:              mp,
			fs:              fs,
		}
	}
}

// tombstoneFSinkerWithName is a FileSinker for tombstone that uses a specified object name
type tombstoneFSinkerWithName struct {
	writer          *ioutil.BlockWriter
	objectName      objectio.ObjectName
	hiddenSelection objectio.HiddenColumnSelection
	mp              *mpool.MPool
	fs              fileservice.FileService
}

func (s *tombstoneFSinkerWithName) Sink(ctx context.Context, b *batch.Batch) error {
	if s.writer == nil {
		// Create tombstone writer with specified object name
		seqnums := objectio.GetTombstoneSeqnums(s.hiddenSelection)
		segid := s.objectName.SegmentId()
		s.writer = ioutil.ConstructWriterWithSegmentID(
			&segid,
			s.objectName.Num(),
			0, // version
			seqnums,
			objectio.TombstonePrimaryKeyIdx, // sortkeyPos
			true,                            // sortkeyIsPK
			true,                            // isTombstone
			s.fs,
			nil, // arena
		)
	}
	_, err := s.writer.WriteBatch(b)
	return err
}

func (s *tombstoneFSinkerWithName) Sync(ctx context.Context) (*objectio.ObjectStats, error) {
	if s.writer == nil {
		return nil, nil
	}
	if _, _, err := s.writer.Sync(ctx); err != nil {
		return nil, err
	}
	ss := s.writer.GetObjectStats(objectio.WithSorted(), objectio.WithCNCreated())
	s.writer = nil
	return &ss, nil
}

func (s *tombstoneFSinkerWithName) Reset() {
	if s.writer != nil {
		s.writer = nil
	}
}

func (s *tombstoneFSinkerWithName) Close() error {
	s.writer = nil
	return nil
}
