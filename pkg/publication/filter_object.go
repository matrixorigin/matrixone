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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

// ErrSyncProtectionTTLExpired is returned when sync protection TTL has expired
var ErrSyncProtectionTTLExpired = moerr.NewInternalErrorNoCtx("sync protection TTL expired")

const publicationTombstoneCleanupTimeout = time.Minute

// CCPRTxnCacheWriter is an interface for writing objects to CCPR transaction cache
// This interface is implemented by disttae.CCPRTxnCache
type CCPRTxnCacheWriter interface {
	// WriteObject checks if an object needs to be written and registers it in the cache.
	// Does NOT write the file - caller should write the file when isNewFile=true.
	// Returns isNewFile (true if file needs to be written) and any error.
	WriteObject(ctx context.Context, objectName string, txnID []byte) (isNewFile bool, err error)
	// WriteNewObject registers a caller-generated unique object without a
	// remote existence probe. It fails if the name is already in flight.
	WriteNewObject(
		ctx context.Context,
		object ioutil.UnpublishedObject,
		txnID []byte,
	) error
	// OnFileWritten is called after the file has been successfully written to fileservice.
	OnFileWritten(objectName string)
}

// CCPRObjectCleanupOwner supplies the durable catalog and sync-protection
// identity for one table's attempt-unique publication outputs.
type CCPRObjectCleanupOwner struct {
	DBID                  uint64
	TableID               uint64
	TNShardID             uint64
	SyncProtectionJobID   string
	SyncProtectionValidTS func() int64
}

func (o *CCPRObjectCleanupOwner) intent(
	ctx context.Context,
	file string,
	isTombstone bool,
) (ioutil.UnpublishedObject, error) {
	if o == nil || o.DBID == 0 || o.TableID == 0 || o.TNShardID == 0 ||
		o.SyncProtectionJobID == "" || o.SyncProtectionValidTS == nil {
		return ioutil.UnpublishedObject{}, moerr.NewInternalError(
			ctx, "CCPR durable cleanup owner is not configured")
	}
	validTS := o.SyncProtectionValidTS()
	if validTS <= time.Now().UnixNano() {
		return ioutil.UnpublishedObject{}, moerr.NewInternalError(
			ctx, "CCPR sync protection is no longer valid")
	}
	return ioutil.UnpublishedObject{
		File:                  file,
		DBID:                  o.DBID,
		TableID:               o.TableID,
		IsTombstone:           isTombstone,
		TNShardID:             o.TNShardID,
		SyncProtectionJobID:   o.SyncProtectionJobID,
		SyncProtectionValidTS: validTS,
	}, nil
}

// FilterObjectResult holds the result of FilterObject
type FilterObjectResult struct {
	HasMappingUpdate bool
	UpstreamAObjUUID *objectio.ObjectId
	PreviousStats    objectio.ObjectStats
	CurrentStats     objectio.ObjectStats
	// DownstreamStats holds the stats for non-appendable objects that were written to fileservice
	DownstreamStats objectio.ObjectStats
	// DownstreamStatsList contains every output of a bounded multi-spill
	// non-appendable rewrite. Data-object copies still contain exactly one item.
	DownstreamStatsList []objectio.ObjectStats
	// RowOffsetMap maps original rowoffset to new rowoffset after sorting
	// Key: original rowoffset, Value: new rowoffset
	RowOffsetMap map[uint32]uint32
}

// FilterObject filters an object based on snapshot TS
// Input: object stats (as bytes), snapshot TS, and whether it's an aobj (checked from object stats)
// For aobj: gets object file, converts to batch, filters by snapshot TS, creates new object
// The mapping between new UUID and upstream aobj is stored in ccprCache.aobjectMap
// For nobj: writes directly to fileservice with new UUID
// ccprCache and txnID are required for non-appendable tombstone rewrites,
// because they install rollback ownership before each output object write.
// Other filter paths do not use them.
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
	cleanupOwners ...*CCPRObjectCleanupOwner,
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

	// Check if it's an appendable object
	isAObj := stats.GetAppendable()
	var cleanupOwner *CCPRObjectCleanupOwner
	if len(cleanupOwners) != 0 {
		cleanupOwner = cleanupOwners[0]
	}
	if isAObj {
		// Handle appendable object
		return filterAppendableObject(ctx, &stats, snapshotTS, upstreamExecutor, localFS, isTombstone, mp, getChunkWorker, subscriptionAccountName, pubName, aobjectMap, ttlChecker)
	} else {
		// Handle non-appendable object - write directly to fileservice with new UUID
		// For tombstone, need to convert to batch and rewrite rowids using aobjectMap
		newStats, err := filterNonAppendableObject(ctx, &stats, snapshotTS, upstreamExecutor, localFS, isTombstone, mp, getChunkWorker, writeObjectWorker, subscriptionAccountName, pubName, ccprCache, txnID, cleanupOwner, aobjectMap, ttlChecker)
		if err != nil {
			return nil, err
		}
		result := &FilterObjectResult{DownstreamStatsList: newStats}
		if len(newStats) != 0 {
			result.DownstreamStats = newStats[0]
		}
		return result, nil
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
		return nil, moerr.NewInternalErrorf(ctx, "failed to get object from upstream: %v", err)
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
	filteredBat, originalRowOffsets, err := filterBatchBySnapshotTS(ctx, bat, snapshotTS, mp)
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
	objStats, rowOffsetMap, err := createObjectFromBatch(ctx, filteredBat, originalRowOffsets, stats, snapshotTS, isTombstone, localFS, mp, sortKeySeqnum, false)
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

// AObjectMapping retains the downstream object identities needed to rewrite or
// retire an upstream object in a later publication iteration.
type AObjectMapping struct {
	DownstreamStats objectio.ObjectStats
	// A non-nil pointer selects one-to-many identity ownership; an empty slice
	// records that the rewrite intentionally produced no downstream object.
	DownstreamObjectIDs *[]objectio.ObjectId
	IsTombstone         bool
	DBName              string
	TableName           string
	// RowOffsetMap maps original rowoffset to new rowoffset after sorting
	// Key: original rowoffset, Value: new rowoffset
	RowOffsetMap map[uint32]uint32
}

// AObjectMap stores mappings from upstream objects to downstream objects.
// Key: upstreamID (string), Value: *AObjectMapping
// This map is persisted with the publication iteration state.
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
// and writes to a sorted sinker with attempt-unique object names.
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
	cleanupOwner *CCPRObjectCleanupOwner,
	aobjectMap *AObjectMap,
	ttlChecker TTLChecker,
) ([]objectio.ObjectStats, error) {
	// Check TTL before processing
	if ttlChecker != nil && !ttlChecker() {
		return nil, ErrSyncProtectionTTLExpired
	}

	// Get upstream object name from stats
	upstreamObjectName := stats.ObjectName().String()

	// Get object file from upstream
	objectContent, err := GetObjectFromUpstreamWithWorker(ctx, upstreamExecutor, upstreamObjectName, getChunkWorker, subscriptionAccountName, pubName)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to get object from upstream: %v", err)
	}

	// Check TTL after getting object
	if ttlChecker != nil && !ttlChecker() {
		return nil, ErrSyncProtectionTTLExpired
	}

	// For tombstone objects, read blocks one by one and rewrite rowids using aobjectMap
	if isTombstone && aobjectMap != nil {
		objStats, err := rewriteNonAppendableTombstoneWithSinker(
			ctx, objectContent, stats, localFS, mp, aobjectMap,
			ccprCache, txnID, cleanupOwner,
		)
		if err != nil {
			return nil, err
		}
		return objStats, nil
	}

	// For data objects (or tombstone without aobjectMap), write directly to fileservice
	writeJob := NewWriteObjectJob(ctx, localFS, upstreamObjectName, objectContent, ccprCache, txnID)
	if writeObjectWorker != nil {
		if err := writeObjectWorker.SubmitWriteObject(writeJob); err != nil {
			return nil, err
		}
	} else {
		writeJob.Execute()
	}
	writeResult := writeJob.WaitDone().(*WriteObjectJobResult)
	if writeResult.Err != nil {
		return nil, writeResult.Err
	}

	// Return original stats (no need to create new stats since we use the same object name)
	return []objectio.ObjectStats{*stats}, nil
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
	if upstreamExecutor == nil {
		return nil, moerr.NewInternalError(ctx, "upstream executor is nil")
	}

	// First, get offset 0 to get metadata (total_chunks, total_size, etc.)
	// GETOBJECT returns: data, total_size, chunk_index, total_chunks, is_complete
	// offset 0 returns metadata with data = nil
	metaResult, err := getMetaWithRetry(ctx, upstreamExecutor, objectName, getChunkWorker, subscriptionAccountName, pubName)
	if err != nil {
		return nil, err
	}

	totalChunks := metaResult.TotalChunks
	chunkCtx, cancelChunks := context.WithCancel(ctx)
	var acceptedChunks acceptedJobBatch[*GetChunkJob]
	defer func() {
		// Cancel first so an error or partial admission cannot leave an accepted
		// sibling blocked in I/O, then join before the shared executor can close.
		cancelChunks()
		acceptedChunks.join()
	}()

	// Fetch data chunks starting from chunk 1
	// chunk 0 is metadata, chunks 1 to totalChunks are data chunks
	allChunks := make([][]byte, totalChunks)

	// Submit all chunk jobs to worker pool
	for i := int64(1); i <= totalChunks; i++ {
		chunkJob := NewGetChunkJob(chunkCtx, upstreamExecutor, objectName, i, subscriptionAccountName, pubName)
		if getChunkWorker != nil {
			if err := getChunkWorker.SubmitGetChunk(chunkJob); err != nil {
				return nil, err
			}
		} else {
			chunkJob.Execute()
		}
		acceptedChunks.add(chunkJob)
	}

	// Wait for all chunk jobs to complete, retry failed chunks
	for i := int64(0); i < totalChunks; i++ {
		chunkResult := acceptedChunks.waitNext().(*GetChunkJobResult)
		if chunkResult.Err != nil {
			// Retry failed chunk
			chunkData, err := getChunkWithRetry(ctx, upstreamExecutor, objectName, i+1, getChunkWorker, subscriptionAccountName, pubName)
			if err != nil {
				return nil, err
			}
			allChunks[i] = chunkData
		} else {
			allChunks[i] = chunkResult.ChunkData
		}
	}

	// Combine all chunks
	totalLen := 0
	for _, chunk := range allChunks {
		totalLen += len(chunk)
	}
	objectContent := make([]byte, 0, totalLen)
	for _, chunk := range allChunks {
		objectContent = append(objectContent, chunk...)
	}

	return objectContent, nil
}

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

// rewriteNonAppendableTombstoneWithSinker reads tombstone blocks one by one,
// rewrites rowids using aobjectMap, and writes to a sorted sinker with unique output names.
// This avoids loading the entire object into memory at once.
func rewriteNonAppendableTombstoneWithSinker(
	ctx context.Context,
	objectContent []byte,
	stats *objectio.ObjectStats,
	localFS fileservice.FileService,
	mp *mpool.MPool,
	aobjectMap *AObjectMap,
	ccprCache CCPRTxnCacheWriter,
	txnID []byte,
	cleanupOwner *CCPRObjectCleanupOwner,
) ([]objectio.ObjectStats, error) {
	// Step 1: Parse object metadata
	metaExtent := stats.Extent()
	if int(metaExtent.Offset()+metaExtent.Length()) > len(objectContent) {
		return nil, moerr.NewInternalErrorf(ctx, "object content too small for meta extent")
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
			return nil, moerr.NewInternalErrorf(ctx, "failed to decompress meta data: %v", err)
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
		return nil, nil
	}

	// Step 2: Get column information from first block meta
	firstBlkMeta := dataMeta.GetBlockMeta(0)
	maxSeqnum := firstBlkMeta.GetMaxSeqnum()

	// Prepare columns and types (excluding commit TS for non-appendable)
	cols := make([]uint16, 0, maxSeqnum+1)
	typs := make([]types.Type, 0, maxSeqnum+1)
	for seqnum := uint16(0); seqnum <= maxSeqnum; seqnum++ {
		colMeta := firstBlkMeta.ColumnMeta(seqnum)
		if colMeta.DataType() == 0 {
			continue
		}
		cols = append(cols, seqnum)
		typ := types.T(colMeta.DataType()).ToType()
		typs = append(typs, typ)
	}

	// Get PK type from second column (TombstoneAttr_PK_Idx = 1)
	pkType := typs[objectio.TombstoneAttr_PK_Idx]

	// Every spill gets an attempt-unique name and is registered with the CCPR
	// transaction before Sync can persist it. This is both the generation fence
	// and the rollback owner; reusing the upstream name lets a stale cleanup
	// delete a later successful retry.
	if ccprCache == nil || len(txnID) == 0 {
		return nil, moerr.NewInternalErrorNoCtx(
			"CCPR transaction cleanup owner is required for tombstone rewrite")
	}
	sinkerFactory := newCCPRTombstoneFileSinkerFactory(
		ccprCache, txnID, cleanupOwner,
		objectio.HiddenColumnSelection_None)
	attrs, attrTypes := objectio.GetTombstoneSchema(pkType, objectio.HiddenColumnSelection_None)

	sinker := ioutil.NewSinker(
		objectio.TombstonePrimaryKeyIdx,
		attrs,
		attrTypes,
		sinkerFactory,
		mp,
		localFS,
		ioutil.WithTailSizeCap(0), // Force all data to be written to object
	)
	published := false
	defer func() {
		if !published {
			cleanupPublicationTombstoneSinker(ctx, sinker)
		}
		_ = sinker.Close()
	}()

	// Step 4: Read blocks one by one and write to sinker
	allocator := fileservice.DefaultCacheDataAllocator()

	for blkIdx := uint32(0); blkIdx < blkCnt; blkIdx++ {
		// Read single block
		blkBat, err := readSingleBlockToBatch(ctx, objectContent, dataMeta, blkIdx, cols, typs, maxSeqnum, allocator, mp)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to read block %d: %v", blkIdx, err)
		}

		// Rewrite tombstone rowids using aobjectMap
		if err := rewriteTombstoneRowidsBatch(ctx, blkBat, aobjectMap, mp); err != nil {
			blkBat.Clean(mp)
			return nil, moerr.NewInternalErrorf(ctx, "failed to rewrite tombstone rowids for block %d: %v", blkIdx, err)
		}

		// Write to sinker
		if err := sinker.Write(ctx, blkBat); err != nil {
			blkBat.Clean(mp)
			return nil, moerr.NewInternalErrorf(ctx, "failed to write block %d to sinker: %v", blkIdx, err)
		}

		blkBat.Clean(mp)
	}

	// Step 5: Sync sinker and get result
	if err := sinker.Sync(ctx); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to sync sinker: %v", err)
	}

	persisted, _ := sinker.GetResult()
	if len(persisted) == 0 {
		return nil, moerr.NewInternalErrorf(ctx, "no object was created by sinker")
	}

	published = true
	return persisted, nil
}

func cleanupPublicationTombstoneSinker(
	ctx context.Context,
	sinker *ioutil.Sinker,
) {
	// CCPRTxnCache registered every unique name before its write. A best-effort
	// direct delete shortens retention; transaction rollback remains the owner
	// if the delete fails or Sync returned an ambiguous result.
	cleanupCtx, cancel := context.WithTimeoutCause(
		context.WithoutCancel(ctx),
		publicationTombstoneCleanupTimeout,
		moerr.CauseCleanUpUselessFiles,
	)
	defer cancel()
	_, _ = sinker.DeletePersisted(cleanupCtx)
}

func newCCPRTombstoneFileSinkerFactory(
	cache CCPRTxnCacheWriter,
	txnID []byte,
	cleanupOwner *CCPRObjectCleanupOwner,
	hidden objectio.HiddenColumnSelection,
) ioutil.FileSinkerFactory {
	return func(mp *mpool.MPool, fs fileservice.FileService) ioutil.FileSinker {
		return &ccprTombstoneFileSinker{
			inner: ioutil.NewTombstoneFSinkerImpl(hidden, mp, fs),
			cache: cache,
			txnID: append([]byte(nil), txnID...),
			owner: cleanupOwner,
		}
	}
}

type ccprTombstoneFileSinker struct {
	inner *ioutil.FSinkerImpl
	cache CCPRTxnCacheWriter
	txnID []byte
	owner *CCPRObjectCleanupOwner
}

func (s *ccprTombstoneFileSinker) Sink(
	ctx context.Context,
	bat *batch.Batch,
) error {
	return s.inner.Sink(ctx, bat)
}

func (s *ccprTombstoneFileSinker) Sync(
	ctx context.Context,
) (*objectio.ObjectStats, error) {
	if s.cache == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"CCPR tombstone cleanup owner is not configured")
	}
	name := s.inner.ActiveObjectName()
	if name == "" {
		return nil, moerr.NewInternalErrorNoCtx(
			"CCPR tombstone sinker has no active object")
	}
	intent, err := s.owner.intent(ctx, name, true)
	if err != nil {
		return nil, err
	}
	if err := s.cache.WriteNewObject(ctx, intent, s.txnID); err != nil {
		return nil, err
	}
	stats, err := s.inner.Sync(ctx)
	if err == nil {
		s.cache.OnFileWritten(name)
	}
	return stats, err
}

func (s *ccprTombstoneFileSinker) Reset() {
	s.inner.Reset()
}

func (s *ccprTombstoneFileSinker) ActiveObjectName() string {
	return s.inner.ActiveObjectName()
}

func (s *ccprTombstoneFileSinker) Close() error {
	return s.inner.Close()
}
