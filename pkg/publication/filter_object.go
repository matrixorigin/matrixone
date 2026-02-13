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
	"fmt"
	"math"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

// Job types
const (
	JobTypeGetMeta      int8 = 1
	JobTypeGetChunk     int8 = 2
	JobTypeFilterObject int8 = 3
	JobTypeWriteObject  int8 = 4
)

const (
	// getChunkMaxMemory is the maximum memory for concurrent GetChunkJob operations (1GB)
	GetChunkMaxMemory = 3 * 1024 * 1024 * 1024
	// getChunkSize is the size of each chunk (100MB)
	GetChunkSize = 100 * 1024 * 1024
	// getChunkMaxConcurrent is the maximum concurrent chunk reads (1GB / 100MB = 10)
	getChunkMaxConcurrent = GetChunkMaxMemory / GetChunkSize
)

// getChunkSemaphore limits concurrent memory usage for GetChunkJob (1GB max)
var getChunkSemaphore = make(chan struct{}, getChunkMaxConcurrent)

// Job is an interface for async jobs
type Job interface {
	Execute()
	WaitDone() any
	GetType() int8
}

// GetMetaJobResult holds the result of GetMetaJob
type GetMetaJobResult struct {
	MetadataData []byte
	TotalSize    int64
	ChunkIndex   int64
	TotalChunks  int64
	IsComplete   bool
	Err          error
}

// GetMetaJob is a job for getting metadata (chunk 0)
type GetMetaJob struct {
	ctx                     context.Context
	upstreamExecutor        SQLExecutor
	objectName              string
	subscriptionAccountName string
	pubName                 string
	result                  chan *GetMetaJobResult
}

// NewGetMetaJob creates a new GetMetaJob
func NewGetMetaJob(ctx context.Context, upstreamExecutor SQLExecutor, objectName string, subscriptionAccountName string, pubName string) *GetMetaJob {
	return &GetMetaJob{
		ctx:                     ctx,
		upstreamExecutor:        upstreamExecutor,
		objectName:              objectName,
		subscriptionAccountName: subscriptionAccountName,
		pubName:                 pubName,
		result:                  make(chan *GetMetaJobResult, 1),
	}
}

// Execute runs the GetMetaJob
func (j *GetMetaJob) Execute() {
	res := &GetMetaJobResult{}
	getChunk0SQL := PublicationSQLBuilder.GetObjectSQL(j.subscriptionAccountName, j.pubName, j.objectName, 0)

	result, cancel, err := j.upstreamExecutor.ExecSQL(j.ctx, nil, InvalidAccountID, getChunk0SQL, false, true, time.Second*10)
	if err != nil {
		res.Err = moerr.NewInternalErrorf(j.ctx, "failed to execute GETOBJECT query for offset 0: %v", err)
		j.result <- res
		return
	}
	if !result.Next() {
		result.Close()
		cancel()
		res.Err = moerr.NewInternalErrorf(j.ctx, "no object content returned for %s", j.objectName)
		j.result <- res
		return
	}

	if err := result.Scan(&res.MetadataData, &res.TotalSize, &res.ChunkIndex, &res.TotalChunks, &res.IsComplete); err != nil {
		result.Close()
		cancel()
		res.Err = moerr.NewInternalErrorf(j.ctx, "failed to scan offset 0: %v", err)
		j.result <- res
		return
	}
	result.Close()
	cancel()

	if res.TotalChunks <= 0 {
		res.Err = moerr.NewInternalErrorf(j.ctx, "invalid total_chunks: %d", res.TotalChunks)
		j.result <- res
		return
	}

	j.result <- res
}

// WaitDone waits for the job to complete and returns the result
func (j *GetMetaJob) WaitDone() any {
	return <-j.result
}

// GetType returns the job type
func (j *GetMetaJob) GetType() int8 {
	return JobTypeGetMeta
}

// GetChunkJobResult holds the result of GetChunkJob
type GetChunkJobResult struct {
	ChunkData  []byte
	ChunkIndex int64
	Err        error
}

// GetChunkJob is a job for getting a single chunk
type GetChunkJob struct {
	ctx                     context.Context
	upstreamExecutor        SQLExecutor
	objectName              string
	chunkIndex              int64
	subscriptionAccountName string
	pubName                 string
	result                  chan *GetChunkJobResult
}

// NewGetChunkJob creates a new GetChunkJob
func NewGetChunkJob(ctx context.Context, upstreamExecutor SQLExecutor, objectName string, chunkIndex int64, subscriptionAccountName string, pubName string) *GetChunkJob {
	return &GetChunkJob{
		ctx:                     ctx,
		upstreamExecutor:        upstreamExecutor,
		objectName:              objectName,
		chunkIndex:              chunkIndex,
		subscriptionAccountName: subscriptionAccountName,
		pubName:                 pubName,
		result:                  make(chan *GetChunkJobResult, 1),
	}
}

// Execute runs the GetChunkJob
func (j *GetChunkJob) Execute() {
	res := &GetChunkJobResult{ChunkIndex: j.chunkIndex}

	// Acquire semaphore for memory control (blocks if 1GB limit reached)
	select {
	case getChunkSemaphore <- struct{}{}:
		// acquired
	case <-j.ctx.Done():
		res.Err = j.ctx.Err()
		j.result <- res
		return
	}
	defer func() { <-getChunkSemaphore }()

	getChunkSQL := PublicationSQLBuilder.GetObjectSQL(j.subscriptionAccountName, j.pubName, j.objectName, j.chunkIndex)
	result, cancel, err := j.upstreamExecutor.ExecSQL(j.ctx, nil, InvalidAccountID, getChunkSQL, false, true, time.Minute)
	if err != nil {
		res.Err = moerr.NewInternalErrorf(j.ctx, "failed to execute GETOBJECT query for offset %d: %v, sql: %v", j.chunkIndex, err, getChunkSQL)
		j.result <- res
		return
	}
	if result.Next() {
		var chunkData []byte
		var totalSizeChk int64
		var chunkIndexChk int64
		var totalChunksChk int64
		var isCompleteChk bool
		if err := result.Scan(&chunkData, &totalSizeChk, &chunkIndexChk, &totalChunksChk, &isCompleteChk); err != nil {
			result.Close()
			res.Err = moerr.NewInternalErrorf(j.ctx, "failed to scan offset %d: %v", j.chunkIndex, err)
			j.result <- res
			return
		}
		res.ChunkData = chunkData
	} else {
		result.Close()
		cancel()
		res.Err = moerr.NewInternalErrorf(j.ctx, "no chunk content returned for chunk %d of %s", j.chunkIndex, j.objectName)
		j.result <- res
		return
	}
	result.Close()
	cancel()

	j.result <- res
}

// WaitDone waits for the job to complete and returns the result
func (j *GetChunkJob) WaitDone() any {
	return <-j.result
}

// GetType returns the job type
func (j *GetChunkJob) GetType() int8 {
	return JobTypeGetChunk
}

// GetObjectName returns the object name
func (j *GetChunkJob) GetObjectName() string {
	return j.objectName
}

// GetChunkIndex returns the chunk index
func (j *GetChunkJob) GetChunkIndex() int64 {
	return j.chunkIndex
}

// WriteObjectJobResult holds the result of WriteObjectJob
type WriteObjectJobResult struct {
	Err error
}

// WriteObjectJob is a job for writing object to fileservice
type WriteObjectJob struct {
	ctx           context.Context
	localFS       fileservice.FileService
	objectName    string
	objectContent []byte
	ccprCache     CCPRTxnCacheWriter
	txnID         []byte
	result        chan *WriteObjectJobResult
}

// NewWriteObjectJob creates a new WriteObjectJob
func NewWriteObjectJob(
	ctx context.Context,
	localFS fileservice.FileService,
	objectName string,
	objectContent []byte,
	ccprCache CCPRTxnCacheWriter,
	txnID []byte,
) *WriteObjectJob {
	return &WriteObjectJob{
		ctx:           ctx,
		localFS:       localFS,
		objectName:    objectName,
		objectContent: objectContent,
		ccprCache:     ccprCache,
		txnID:         txnID,
		result:        make(chan *WriteObjectJobResult, 1),
	}
}

// Execute runs the WriteObjectJob
func (j *WriteObjectJob) Execute() {
	res := &WriteObjectJobResult{}

	t0 := time.Now()
	t1 := time.Now()
	// Use CCPRTxnCache.WriteObject if cache is available, otherwise write directly
	if j.ccprCache != nil && len(j.txnID) > 0 {
		// Check if file needs to be written and register in cache
		isNewFile, err := j.ccprCache.WriteObject(j.ctx, j.objectName, j.txnID)
		if err != nil {
			res.Err = err
			j.result <- res
			return
		}
		isNewDuration := time.Since(t1)
		t1 = time.Now()
		if isNewFile {
			// File needs to be written - do it outside the cache lock
			err = j.localFS.Write(j.ctx, fileservice.IOVector{
				FilePath: j.objectName,
				Entries: []fileservice.IOEntry{
					{
						Offset: 0,
						Size:   int64(len(j.objectContent)),
						Data:   j.objectContent,
					},
				},
			})
			if err != nil {
				res.Err = moerr.NewInternalErrorf(j.ctx, "failed to write object to fileservice: %v", err)
				j.result <- res
				return
			}
			// Notify cache that file has been written
			j.ccprCache.OnFileWritten(j.objectName)
		}
		totalDuration := time.Since(t0)
		writeDuration := time.Since(t1)
		if totalDuration > time.Second*30 {
			logutil.Infof("ccpr-worker write object duration is too long, total duration: %v, is new file duration: %v, write duration: %v", totalDuration, isNewDuration, writeDuration)
		}
	} else {
		// Fallback: Write to local fileservice with original object name
		err := j.localFS.Write(j.ctx, fileservice.IOVector{
			FilePath: j.objectName,
			Entries: []fileservice.IOEntry{
				{
					Offset: 0,
					Size:   int64(len(j.objectContent)),
					Data:   j.objectContent,
				},
			},
		})
		if err != nil {
			// Check if the error is due to file already exists
			if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
				// File already exists, this is ok
			} else {
				res.Err = moerr.NewInternalErrorf(j.ctx, "failed to write object to fileservice: %v", err)
				j.result <- res
				return
			}
		}
	}

	j.result <- res
}

// WaitDone waits for the job to complete and returns the result
func (j *WriteObjectJob) WaitDone() any {
	return <-j.result
}

// GetType returns the job type
func (j *WriteObjectJob) GetType() int8 {
	return JobTypeWriteObject
}

// GetObjectName returns the object name for WriteObjectJobInfo interface
func (j *WriteObjectJob) GetObjectName() string {
	return j.objectName
}

// GetObjectSize returns the object content size for WriteObjectJobInfo interface
func (j *WriteObjectJob) GetObjectSize() int64 {
	return int64(len(j.objectContent))
}

// FilterObjectJobResult holds the result of FilterObjectJob
type FilterObjectJobResult struct {
	Err              error
	HasMappingUpdate bool
	UpstreamAObjUUID *objectio.ObjectId
	PreviousStats    objectio.ObjectStats
	CurrentStats     objectio.ObjectStats
	// DownstreamStats holds the stats for non-appendable objects that were written to fileservice
	DownstreamStats objectio.ObjectStats
}

// TTLChecker is a function type for checking if sync protection TTL has expired
// Returns true if TTL has expired and the job should abort
type TTLChecker func() bool

// FilterObjectJob is a job for filtering an object
type FilterObjectJob struct {
	ctx                     context.Context
	objectStatsBytes        []byte
	snapshotTS              types.TS
	upstreamExecutor        SQLExecutor
	isTombstone             bool
	localFS                 fileservice.FileService
	mp                      *mpool.MPool
	getChunkWorker          GetChunkWorker
	writeObjectWorker       WriteObjectWorker
	subscriptionAccountName string
	pubName                 string
	ccprCache               CCPRTxnCacheWriter
	txnID                   []byte
	aobjectMap              AObjectMap // Used for tombstone rowid rewriting
	ttlChecker              TTLChecker // TTL expiration checker
	result                  chan *FilterObjectJobResult
}

// NewFilterObjectJob creates a new FilterObjectJob
func NewFilterObjectJob(
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
	aobjectMap AObjectMap,
	ttlChecker TTLChecker,
) *FilterObjectJob {
	return &FilterObjectJob{
		ctx:                     ctx,
		objectStatsBytes:        objectStatsBytes,
		snapshotTS:              snapshotTS,
		upstreamExecutor:        upstreamExecutor,
		isTombstone:             isTombstone,
		localFS:                 localFS,
		mp:                      mp,
		getChunkWorker:          getChunkWorker,
		writeObjectWorker:       writeObjectWorker,
		subscriptionAccountName: subscriptionAccountName,
		pubName:                 pubName,
		ccprCache:               ccprCache,
		txnID:                   txnID,
		aobjectMap:              aobjectMap,
		ttlChecker:              ttlChecker,
		result:                  make(chan *FilterObjectJobResult, 1),
	}
}

// ErrSyncProtectionTTLExpired is returned when sync protection TTL has expired
var ErrSyncProtectionTTLExpired = moerr.NewInternalErrorNoCtx("sync protection TTL expired")

// Execute runs the FilterObjectJob
func (j *FilterObjectJob) Execute() {
	res := &FilterObjectJobResult{}

	// Check TTL before starting
	if j.ttlChecker != nil && j.ttlChecker() {
		res.Err = ErrSyncProtectionTTLExpired
		j.result <- res
		return
	}

	filterResult, err := FilterObject(
		j.ctx,
		j.objectStatsBytes,
		j.snapshotTS,
		j.upstreamExecutor,
		j.isTombstone,
		j.localFS,
		j.mp,
		j.getChunkWorker,
		j.writeObjectWorker,
		j.subscriptionAccountName,
		j.pubName,
		j.ccprCache,
		j.txnID,
		j.aobjectMap,
		j.ttlChecker,
	)
	res.Err = err
	if filterResult != nil {
		res.HasMappingUpdate = filterResult.HasMappingUpdate
		res.UpstreamAObjUUID = filterResult.UpstreamAObjUUID
		res.PreviousStats = filterResult.PreviousStats
		res.CurrentStats = filterResult.CurrentStats
		res.DownstreamStats = filterResult.DownstreamStats
	}
	j.result <- res
}

// WaitDone waits for the job to complete and returns the result
func (j *FilterObjectJob) WaitDone() any {
	return <-j.result
}

// GetType returns the job type
func (j *FilterObjectJob) GetType() int8 {
	return JobTypeFilterObject
}

// FilterObjectResult holds the result of FilterObject
type FilterObjectResult struct {
	HasMappingUpdate bool
	UpstreamAObjUUID *objectio.ObjectId
	PreviousStats    objectio.ObjectStats
	CurrentStats     objectio.ObjectStats
	// DownstreamStats holds the stats for non-appendable objects that were written to fileservice
	DownstreamStats objectio.ObjectStats
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
	aobjectMap AObjectMap,
	ttlChecker TTLChecker,
) (*FilterObjectResult, error) {
	// Check TTL before processing
	if ttlChecker != nil && ttlChecker() {
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
	if isAObj {
		// Handle appendable object
		return filterAppendableObject(ctx, &stats, snapshotTS, upstreamExecutor, localFS, isTombstone, mp, getChunkWorker, subscriptionAccountName, pubName, aobjectMap, ttlChecker)
	} else {
		// Handle non-appendable object - write directly to fileservice with new UUID
		newStats, err := filterNonAppendableObject(ctx, &stats, upstreamExecutor, localFS, getChunkWorker, writeObjectWorker, subscriptionAccountName, pubName, ccprCache, txnID, ttlChecker)
		if err != nil {
			return nil, err
		}
		// Return the new downstream stats with new object name
		return &FilterObjectResult{
			DownstreamStats: newStats,
		}, nil
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
	aobjectMap AObjectMap,
	ttlChecker TTLChecker,
) (*FilterObjectResult, error) {
	// Check TTL before processing
	if ttlChecker != nil && ttlChecker() {
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
	if ttlChecker != nil && ttlChecker() {
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
	objStats, err := createObjectFromBatch(ctx, filteredBat, stats, snapshotTS, isTombstone, localFS, mp, sortKeySeqnum)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to create object from batch: %v", err)
	}

	// Return mapping update info for storage in ccprCache.aobjectMap
	return &FilterObjectResult{
		HasMappingUpdate: true,
		UpstreamAObjUUID: upstreamAObjUUID,
		CurrentStats:     objStats, // New current stats
	}, nil
}

// AObjectMapping represents a mapping from upstream aobj to downstream object stats
type AObjectMapping struct {
	DownstreamStats objectio.ObjectStats
	IsTombstone     bool
	DBName          string
	TableName       string
}

// AObjectMap stores the mapping from upstream aobj to downstream object stats
// Key: upstreamID (string), Value: *AObjectMapping
// This map is used to track appendable object transformations during CCPR sync
type AObjectMap map[string]*AObjectMapping

// NewAObjectMap creates a new AObjectMap instance
func NewAObjectMap() AObjectMap {
	return make(AObjectMap)
}

// Get retrieves the mapping for an upstream aobj
func (m AObjectMap) Get(upstreamID string) (*AObjectMapping, bool) {
	mapping, exists := m[upstreamID]
	return mapping, exists
}

// Set stores or updates the mapping for an upstream aobj
func (m AObjectMap) Set(upstreamID string, mapping *AObjectMapping) {
	m[upstreamID] = mapping
}

// Delete removes the mapping for an upstream aobj
func (m AObjectMap) Delete(upstreamID string) {
	delete(m, upstreamID)
}

// rewriteTombstoneRowids rewrites delete rowids in tombstone batch using aobjectMap
// For each rowid, extract the object ID and check if it exists in aobjectMap
// If found, replace the segment ID in rowid with the downstream object's segment ID
func rewriteTombstoneRowids(
	ctx context.Context,
	bat *containers.Batch,
	aobjectMap AObjectMap,
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
		}
	}

	return nil
}

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

// filterNonAppendableObject handles non-appendable objects
// Writes directly to fileservice with the original object name
// Returns the original ObjectStats
func filterNonAppendableObject(
	ctx context.Context,
	stats *objectio.ObjectStats,
	upstreamExecutor SQLExecutor,
	localFS fileservice.FileService,
	getChunkWorker GetChunkWorker,
	writeObjectWorker WriteObjectWorker,
	subscriptionAccountName string,
	pubName string,
	ccprCache CCPRTxnCacheWriter,
	txnID []byte,
	ttlChecker TTLChecker,
) (objectio.ObjectStats, error) {
	// Check TTL before processing
	if ttlChecker != nil && ttlChecker() {
		return objectio.ObjectStats{}, ErrSyncProtectionTTLExpired
	}

	// Get upstream object name from stats
	upstreamObjectName := stats.ObjectName().String()

	// Get object file from upstream
	objectContent, err := GetObjectFromUpstreamWithWorker(ctx, upstreamExecutor, upstreamObjectName, getChunkWorker, subscriptionAccountName, pubName)
	if err != nil {
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to get object from upstream: %v", err)
	}

	// Check TTL after getting object
	if ttlChecker != nil && ttlChecker() {
		return objectio.ObjectStats{}, ErrSyncProtectionTTLExpired
	}

	// Use WriteObjectJob to write to fileservice via worker pool
	writeJob := NewWriteObjectJob(ctx, localFS, upstreamObjectName, objectContent, ccprCache, txnID)
	if writeObjectWorker != nil {
		writeObjectWorker.SubmitWriteObject(writeJob)
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
	if upstreamExecutor == nil {
		return nil, moerr.NewInternalError(ctx, "upstream executor is nil")
	}

	// First, get offset 0 to get metadata (total_chunks, total_size, etc.)
	// GETOBJECT returns: data, total_size, chunk_index, total_chunks, is_complete
	// offset 0 returns metadata with data = nil
	metaJob := NewGetMetaJob(ctx, upstreamExecutor, objectName, subscriptionAccountName, pubName)
	if getChunkWorker != nil {
		getChunkWorker.SubmitGetChunk(metaJob)
	} else {
		metaJob.Execute()
	}
	metaResult := metaJob.WaitDone().(*GetMetaJobResult)
	if metaResult.Err != nil {
		return nil, metaResult.Err
	}

	totalChunks := metaResult.TotalChunks

	// Fetch data chunks starting from chunk 1
	// chunk 0 is metadata, chunks 1 to totalChunks are data chunks
	allChunks := make([][]byte, totalChunks)

	// Submit all chunk jobs to worker pool
	chunkJobs := make([]*GetChunkJob, totalChunks)
	for i := int64(1); i <= totalChunks; i++ {
		chunkJob := NewGetChunkJob(ctx, upstreamExecutor, objectName, i, subscriptionAccountName, pubName)
		chunkJobs[i-1] = chunkJob
		if getChunkWorker != nil {
			getChunkWorker.SubmitGetChunk(chunkJob)
		} else {
			chunkJob.Execute()
		}
	}

	// Wait for all chunk jobs to complete
	for i := int64(0); i < totalChunks; i++ {
		chunkResult := chunkJobs[i].WaitDone().(*GetChunkJobResult)
		if chunkResult.Err != nil {
			return nil, chunkResult.Err
		}
		// Store chunk at index i since chunkJobs is 0-indexed
		allChunks[i] = chunkResult.ChunkData
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

// convertObjectToBatch converts object file content to batch directly from memory
// This function is specifically for appendable objects (aobj)
// Steps:
// 1. Read object meta from objectContent bytes using stats.Extent()
// 2. Get column information from block meta
// 3. Read column data directly from objectContent bytes using column extents
// 4. Decode and create vectors
// 5. Create batch with columns
func convertObjectToBatch(
	ctx context.Context,
	objectContent []byte,
	stats *objectio.ObjectStats,
	snapshotTS types.TS,
	localFS fileservice.FileService,
	mp *mpool.MPool,
) (*containers.Batch, error) {
	// Step 1: Read object meta from objectContent bytes
	metaExtent := stats.Extent()
	if int(metaExtent.Offset()+metaExtent.Length()) > len(objectContent) {
		return nil, moerr.NewInternalErrorf(ctx, "object content too small for meta extent")
	}
	metaBytes := objectContent[metaExtent.Offset() : metaExtent.Offset()+metaExtent.Length()]

	// Check if meta needs decompression (same as ReadExtent does)
	var decompressedMetaBytes []byte
	var decompressedMetaBuf fscache.Data
	if metaExtent.Alg() == compress.None {
		decompressedMetaBytes = metaBytes
	} else {
		// Allocate buffer for decompressed data
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
		// Clone the data to ensure meta doesn't hold reference to buffer
		decompressedMetaBytes = append([]byte(nil), decompressedMetaBytes...)
		if decompressedMetaBuf != nil {
			decompressedMetaBuf.Release()
		}
	}

	meta := objectio.MustObjectMeta(decompressedMetaBytes)

	dataMeta := meta.MustGetMeta(objectio.SchemaData)
	blkCnt := dataMeta.BlockCount()
	if blkCnt == 0 {
		return containers.NewBatch(), nil
	}

	// Step 2: Get column information from first block meta
	blkMeta := dataMeta.GetBlockMeta(0)
	maxSeqnum := blkMeta.GetMaxSeqnum()

	// Step 3: Prepare columns and types
	// For appendable objects, we need to include commit TS column
	cols := make([]uint16, 0, maxSeqnum+2)
	typs := make([]types.Type, 0, maxSeqnum+2)

	// Add data columns
	for seqnum := uint16(0); seqnum <= maxSeqnum; seqnum++ {
		colMeta := blkMeta.ColumnMeta(seqnum)
		if colMeta.DataType() == 0 {
			continue // Skip invalid columns
		}
		cols = append(cols, seqnum)
		typ := types.T(colMeta.DataType()).ToType()
		typs = append(typs, typ)
	}

	// Add commit TS column for appendable objects
	cols = append(cols, objectio.SEQNUM_COMMITTS)
	typs = append(typs, objectio.TSType)

	// Step 4: Read column data directly from objectContent bytes
	vecs := make([]containers.Vector, 0, len(cols))
	allocator := fileservice.DefaultCacheDataAllocator()

	for i, seqnum := range cols {
		var colMeta objectio.ColumnMeta
		var ext objectio.Extent

		// Handle special columns (commit TS)
		if seqnum >= objectio.SEQNUM_UPPER {
			if seqnum == objectio.SEQNUM_COMMITTS {
				metaColCnt := blkMeta.GetMetaColumnCount()
				colMeta = blkMeta.ColumnMeta(metaColCnt - 1)
			} else {
				return nil, moerr.NewInternalErrorf(ctx, "unsupported special column: %d", seqnum)
			}
		} else {
			// Normal column
			if seqnum > maxSeqnum || blkMeta.ColumnMeta(seqnum).DataType() == 0 {
				// Generate null vector for missing columns
				length := int(blkMeta.GetRows())
				nullVec := containers.MakeVector(typs[i], mp)
				for j := 0; j < length; j++ {
					nullVec.Append(nil, false)
				}
				vecs = append(vecs, nullVec)
				continue
			}
			colMeta = blkMeta.ColumnMeta(seqnum)
		}

		ext = colMeta.Location()

		// Read column data from objectContent bytes
		if int(ext.Offset()+ext.Length()) > len(objectContent) {
			return nil, moerr.NewInternalErrorf(ctx, "object content too small for column extent at seqnum %d", seqnum)
		}
		colData := objectContent[ext.Offset() : ext.Offset()+ext.Length()]

		// Decompress if needed
		var decompressedData []byte
		var decompressedBuf fscache.Data
		if ext.Alg() == compress.None {
			decompressedData = colData
		} else {
			// Allocate buffer for decompressed data
			decompressedBuf = allocator.AllocateCacheDataWithHint(ctx, int(ext.OriginSize()), malloc.NoClear)
			bs, err := compress.Decompress(colData, decompressedBuf.Bytes(), compress.Lz4)
			if err != nil {
				if decompressedBuf != nil {
					decompressedBuf.Release()
				}
				return nil, moerr.NewInternalErrorf(ctx, "failed to decompress column data: %v", err)
			}
			decompressedData = decompressedBuf.Bytes()[:len(bs)]
			// Clone the data to ensure decoded vector doesn't hold reference to buffer
			decompressedData = append([]byte(nil), decompressedData...)
			// Release buffer immediately after cloning
			if decompressedBuf != nil {
				decompressedBuf.Release()
			}
		}

		// Decode to vector.Vector
		obj, err := objectio.Decode(decompressedData)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to decode column data: %v", err)
		}
		vec := obj.(*vector.Vector)

		// Convert to containers.Vector
		tnVec := containers.ToTNVector(vec, mp)
		vecs = append(vecs, tnVec)
	}

	// Step 5: Create batch with columns
	bat := containers.NewBatch()
	for i, vec := range vecs {
		var attr string
		if cols[i] == objectio.SEQNUM_COMMITTS {
			attr = objectio.TombstoneAttr_CommitTs_Attr
		} else {
			attr = fmt.Sprintf("tmp_%d", i)
		}
		bat.AddVector(attr, vec)
	}

	return bat, nil
}

// filterBatchBySnapshotTS filters batch rows by snapshot TS
// For appendable objects, rows with commit TS >snapshot TS should be filtered out
func filterBatchBySnapshotTS(
	ctx context.Context,
	bat *containers.Batch,
	snapshotTS types.TS,
	mp *mpool.MPool,
) (*containers.Batch, error) {
	if bat == nil {
		return nil, nil
	}

	// Find the commit TS column
	commitTSVec := bat.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr)
	if commitTSVec == nil {
		return nil, moerr.NewInternalErrorf(ctx, "commit TS column not found in batch")
	}

	// Verify the column type is TS
	if commitTSVec.GetType().Oid != types.T_TS {
		return nil, moerr.NewInternalErrorf(ctx, "commit TS column type mismatch: expected TS, got %s", commitTSVec.GetType().String())
	}

	// Get commit TS values
	commitTSs := vector.MustFixedColWithTypeCheck[types.TS](commitTSVec.GetDownstreamVector())

	// Build bitmap of rows to delete (commit TS < snapshot TS)
	deletes := roaring.New()
	for i, ts := range commitTSs {
		if ts.GT(&snapshotTS) {
			deletes.Add(uint32(i))
		}
	}

	// If no rows to delete, return original batch
	if deletes.IsEmpty() {
		return bat, nil
	}

	// Compact all vectors to remove deleted rows
	for _, vec := range bat.Vecs {
		vec.Compact(deletes)
	}

	return bat, nil
}

// createObjectFromBatch sorts batch by primary key, removes commit TS column,
// writes to object file, and returns objectio.ObjectStats
// isTombstone: true for tombstone objects, false for data objects
// sortKeySeqnum: the seqnum of the sortkey column in the original object
func createObjectFromBatch(
	ctx context.Context,
	bat *containers.Batch,
	originalStats *objectio.ObjectStats,
	snapshotTS types.TS,
	isTombstone bool,
	localFS fileservice.FileService,
	mp *mpool.MPool,
	sortKeySeqnum uint16,
) (objectio.ObjectStats, error) {
	if bat == nil || bat.Length() == 0 {
		return objectio.ObjectStats{}, nil
	}

	// Step 1: Convert to CN batch for sorting
	cnBat := containers.ToCNBatch(bat)
	defer cnBat.Clean(mp)

	// Step 2: Sort by primary key (first column, seqnum 0)
	// Primary key is typically the first column
	if len(cnBat.Vecs) == 0 {
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "batch has no columns")
	}
	pkIdx := 0 // Primary key is the first column
	sortedIdx := make([]int64, cnBat.Vecs[0].Length())
	for i := 0; i < len(sortedIdx); i++ {
		sortedIdx[i] = int64(i)
	}
	sort.Sort(false, false, true, sortedIdx, cnBat.Vecs[pkIdx])
	for i := 0; i < len(cnBat.Vecs); i++ {
		if err := cnBat.Vecs[i].Shuffle(sortedIdx, mp); err != nil {
			return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to shuffle vector: %v", err)
		}
	}

	// Step 3: Remove commit TS column
	// Find commit TS column index
	commitTSIdx := -1
	for i, attr := range cnBat.Attrs {
		if attr == objectio.TombstoneAttr_CommitTs_Attr {
			commitTSIdx = i
			break
		}
	}
	if commitTSIdx == -1 {
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "commit TS column not found")
	}

	// Create new batch without commit TS column
	newBat := &batch.Batch{
		Vecs:  make([]*vector.Vector, 0, len(cnBat.Vecs)-1),
		Attrs: make([]string, 0, len(cnBat.Attrs)-1),
	}
	for i, vec := range cnBat.Vecs {
		if i != commitTSIdx {
			newBat.Attrs = append(newBat.Attrs, cnBat.Attrs[i])
			newBat.Vecs = append(newBat.Vecs, vec)
		}
	}
	newBat.SetRowCount(cnBat.Vecs[0].Length())

	// Step 4: Write to object file
	// Get seqnums from original stats to determine column seqnums
	// For appendable objects, we need to exclude commit TS seqnum
	seqnums := make([]uint16, 0, len(newBat.Vecs))
	for i := uint16(0); i < uint16(len(newBat.Vecs)); i++ {
		seqnums = append(seqnums, i)
	}

	// Map sortkey seqnum to position in new batch
	// Since commit TS is removed but data columns keep their original positions,
	// the sortkey position is the same as its seqnum (assuming sortkey is a data column, not commit TS)
	sortKeyPos := 0
	if sortKeySeqnum != math.MaxUint16 {
		// Convert seqnum to position in new batch
		sortKeyPos = int(sortKeySeqnum)
		// If sortkey position is invalid (out of range), fallback to 0
		if sortKeyPos >= len(newBat.Vecs) {
			sortKeyPos = 0
		}
	}
	// If sortKeySeqnum is math.MaxUint16, it means no sortkey was set, use 0 as default

	// Create block writer - use data schema for data objects, tombstone schema for tombstone objects
	var writer *ioutil.BlockWriter
	if isTombstone {
		// Use tombstone schema
		writer = ioutil.ConstructWriter(
			0, // version
			seqnums,
			sortKeyPos, // sortkeyPos from original object metadata
			true,       // sortkeyIsPK
			true,       // isTombstone
			localFS,
		)
	} else {
		// Use data schema
		writer = ioutil.ConstructWriter(
			0, // version
			seqnums,
			sortKeyPos, // sortkeyPos from original object metadata
			true,       // sortkeyIsPK
			false,      // isTombstone
			localFS,
		)
	}

	// Write batch to appropriate schema
	// WriteBatch will use isTombstone flag to write to correct schema (SchemaData or SchemaTombstone)
	// and build objMetaBuilder and update zonemap
	_, err := writer.WriteBatch(newBat)
	if err != nil {
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to write batch: %v", err)
	}

	// Sync writer to flush data
	// Sync will call WriteObjectMeta which sets colmeta (or tombstonesColmeta for tombstone),
	// and then DescribeObject which uses colmeta[sortKeySeqnum] to set zonemap
	_, _, err = writer.Sync(ctx)
	if err != nil {
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to sync writer: %v", err)
	}

	// Step 5: Get and return objectio.ObjectStats
	objStats := writer.GetObjectStats(objectio.WithSorted(), objectio.WithCNCreated())
	return objStats, nil
}

// submitObjectsAsInsert submits objects as INSERT operation
func submitObjectsAsInsert(ctx context.Context, taskID string, txn client.TxnOperator, cnEngine engine.Engine, tombstoneInsertStats []*ObjectWithTableInfo, dataInsertStats []*ObjectWithTableInfo, mp *mpool.MPool) error {
	if len(tombstoneInsertStats) == 0 && len(dataInsertStats) == 0 {
		return nil
	}

	if cnEngine == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}

	// Group objects by (dbName, tableName)
	type tableKey struct {
		dbName    string
		tableName string
	}
	tombstoneByTable := make(map[tableKey][]objectio.ObjectStats)
	dataByTable := make(map[tableKey][]objectio.ObjectStats)

	for _, obj := range tombstoneInsertStats {
		key := tableKey{dbName: obj.DBName, tableName: obj.TableName}
		tombstoneByTable[key] = append(tombstoneByTable[key], obj.Stats)
	}
	for _, obj := range dataInsertStats {
		key := tableKey{dbName: obj.DBName, tableName: obj.TableName}
		dataByTable[key] = append(dataByTable[key], obj.Stats)
	}

	// Process each table separately
	for key, tombstoneStats := range tombstoneByTable {
		if len(tombstoneStats) == 0 {
			continue
		}

		// Get database using transaction from iteration context
		db, err := cnEngine.Database(ctx, key.dbName, txn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", key.dbName, err)
		}

		// Get relation using transaction from iteration context
		rel, err := db.Relation(ctx, key.tableName, nil)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", key.dbName, key.tableName, err)
		}

		// Get table definition to check for fake pk
		tableDef := rel.GetTableDef(ctx)
		hasFakePK := false
		if tableDef != nil && tableDef.Pkey != nil {
			hasFakePK = catalog.IsFakePkName(tableDef.Pkey.PkeyColName)
		}

		// Update ObjectStats flags before submitting
		for i := range tombstoneStats {
			updateObjectStatsFlags(&tombstoneStats[i], true, hasFakePK) // isTombstone = true
		}

		// Collect object abbreviations for logging
		var createObjs []string
		for _, stats := range tombstoneStats {
			createObjs = append(createObjs, stats.ObjectName().ObjectId().ShortStringEx())
		}
		if len(createObjs) > 0 {
			logutil.Info("ccpr-iteration objectsubmit",
				zap.String("task_id", taskID),
				zap.String("database", key.dbName),
				zap.String("table", key.tableName),
				zap.String("operation", "create"),
				zap.Strings("objects", createObjs),
			)
		}

		// Create batch with ObjectStats for deletion
		deleteBat := batch.NewWithSize(1)
		deleteBat.SetAttributes([]string{catalog.ObjectMeta_ObjectStats})

		// ObjectStats column (T_binary)
		statsVec := vector.NewVec(types.T_binary.ToType())
		deleteBat.Vecs[0] = statsVec

		// Append ObjectStats to the batch using Marshal()
		for _, stats := range tombstoneStats {
			statsBytes := stats.Marshal()
			if err := vector.AppendBytes(statsVec, statsBytes, false, mp); err != nil {
				deleteBat.Clean(mp)
				return moerr.NewInternalErrorf(ctx, "failed to append tombstone object stats: %v", err)
			}
		}

		deleteBat.SetRowCount(len(tombstoneStats))

		// Delete through relation
		if err := rel.Delete(ctx, deleteBat, ""); err != nil {
			deleteBat.Clean(mp)
			return moerr.NewInternalErrorf(ctx, "failed to delete tombstone objects: %v", err)
		}
		deleteBat.Clean(mp)
	}

	// Handle regular data objects: use the original Write logic
	for key, dataStats := range dataByTable {
		if len(dataStats) == 0 {
			continue
		}

		// Get database using transaction from iteration context
		db, err := cnEngine.Database(ctx, key.dbName, txn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", key.dbName, err)
		}

		// Get relation using transaction from iteration context
		rel, err := db.Relation(ctx, key.tableName, nil)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", key.dbName, key.tableName, err)
		}

		// Get table definition to check for fake pk
		tableDef := rel.GetTableDef(ctx)
		hasFakePK := false
		if tableDef != nil && tableDef.Pkey != nil {
			hasFakePK = catalog.IsFakePkName(tableDef.Pkey.PkeyColName)
		}

		// Update ObjectStats flags before submitting
		for i := range dataStats {
			updateObjectStatsFlags(&dataStats[i], false, hasFakePK) // isTombstone = false
		}

		// Collect object abbreviations for logging
		var createObjs []string
		for _, stats := range dataStats {
			createObjs = append(createObjs, stats.ObjectName().ObjectId().ShortStringEx())
		}
		if len(createObjs) > 0 {
			logutil.Info("ccpr-iteration objectsubmit",
				zap.String("task_id", taskID),
				zap.String("database", key.dbName),
				zap.String("table", key.tableName),
				zap.String("operation", "create"),
				zap.Strings("objects", createObjs),
			)
		}

		// Create batch with ObjectStats using the same structure as s3util
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})

		// First column: BlockInfo (T_text)
		blockInfoVec := vector.NewVec(types.T_text.ToType())
		bat.Vecs[0] = blockInfoVec

		// Second column: ObjectStats (T_binary)
		statsVec := vector.NewVec(types.T_binary.ToType())
		bat.Vecs[1] = statsVec

		// Use ExpandObjectStatsToBatch to properly expand ObjectStats to batch
		// This handles the correct mapping between blocks and their parent objects
		if err := colexec.ExpandObjectStatsToBatch(
			mp,
			false, // isTombstone = false for INSERT
			bat,
			true, // isCNCreated = true
			dataStats...,
		); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to expand object stats to batch: %v", err)
		}

		// Write through relation
		if err := rel.Write(ctx, bat); err != nil {
			bat.Clean(mp)
			return moerr.NewInternalErrorf(ctx, "failed to write objects: %v", err)
		}
		bat.Clean(mp)
	}

	return nil
}

// submitObjectsAsDelete submits objects as DELETE operation
// It uses SoftDeleteObject to soft delete objects by setting their deleteat timestamp
func submitObjectsAsDelete(
	ctx context.Context,
	taskID string,
	txn client.TxnOperator,
	cnEngine engine.Engine,
	statsList []*ObjectWithTableInfo,
	mp *mpool.MPool,
) error {
	if len(statsList) == 0 {
		return nil
	}

	if cnEngine == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}

	// Group objects by (dbName, tableName)
	type tableKey struct {
		dbName    string
		tableName string
	}
	statsByTable := make(map[tableKey][]*ObjectWithTableInfo)

	for _, obj := range statsList {
		key := tableKey{dbName: obj.DBName, tableName: obj.TableName}
		statsByTable[key] = append(statsByTable[key], obj)
	}

	// Process each table separately
	for key, tableStats := range statsByTable {
		if len(tableStats) == 0 {
			continue
		}

		// Get database using transaction from iteration context
		db, err := cnEngine.Database(ctx, key.dbName, txn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", key.dbName, err)
		}

		// Get relation using transaction from iteration context
		rel, err := db.Relation(ctx, key.tableName, nil)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", key.dbName, key.tableName, err)
		}

		// Get table definition to check for fake pk
		tableDef := rel.GetTableDef(ctx)
		hasFakePK := false
		if tableDef != nil && tableDef.Pkey != nil {
			hasFakePK = catalog.IsFakePkName(tableDef.Pkey.PkeyColName)
		}

		// Update ObjectStats flags before submitting
		for i := range tableStats {
			updateObjectStatsFlags(&tableStats[i].Stats, tableStats[i].IsTombstone, hasFakePK)
		}

		// Try to use SoftDeleteObject if available (for disttae txnTable or txnTableDelegate)
		// Otherwise fall back to the old Delete method
		// Check if it's a txnTableDelegate first
		if delegate, ok := rel.(interface {
			SoftDeleteObject(ctx context.Context, objID *objectio.ObjectId, isTombstone bool) error
		}); ok {
			// Use SoftDeleteObject for each object
			// The deleteat will be set to the transaction's commit timestamp
			var deleteObjs []string
			for _, obj := range tableStats {
				objID := obj.Stats.ObjectName().ObjectId()
				deleteObjs = append(deleteObjs, objID.ShortStringEx())

				// objID is already *objectio.ObjectId, so we pass it directly
				if err := delegate.SoftDeleteObject(ctx, objID, obj.IsTombstone); err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to soft delete object %s: %v", objID.ShortStringEx(), err)
				}
			}
			if len(deleteObjs) > 0 {
				logutil.Info("ccpr-iteration objectsubmit",
					zap.String("task_id", taskID),
					zap.String("database", key.dbName),
					zap.String("table", key.tableName),
					zap.String("operation", "delete"),
					zap.Strings("objects", deleteObjs),
				)
			}
		} else {
			return moerr.NewInternalErrorf(ctx, "failed to use SoftDeleteObject for relation %s.%s", key.dbName, key.tableName)
		}
	}
	return nil
}

func GetObjectListMap(ctx context.Context, iterationCtx *IterationContext, cnEngine engine.Engine) (map[objectio.ObjectId]*ObjectWithTableInfo, error) {

	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	objectListResult, cancel, err := GetObjectListFromSnapshotDiff(ctxWithTimeout, iterationCtx)
	if err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to get object list from snapshot diff: %v", err)
		return nil, err
	}
	defer cancel()
	defer func() {
		if objectListResult != nil {
			objectListResult.Close()
		}
	}()
	// Map to deduplicate objects by ObjectId
	// Key: ObjectId, Value: object info
	objectMap := make(map[objectio.ObjectId]*ObjectWithTableInfo)

	if objectListResult != nil {
		// Check for errors during iteration
		if err = objectListResult.Err(); err != nil {
			err = moerr.NewInternalErrorf(ctx, "error reading object list result: %v", err)
			return nil, err
		}

		objectCount := 0
		// Iterate through object list
		for objectListResult.Next() {
			objectCount++
			// Read columns: db name, table name, object stats, create at, delete at, is tombstone
			var dbName, tableName string
			var statsBytes []byte
			var createAt, deleteAt types.TS
			var isTombstone bool

			if err = objectListResult.Scan(&dbName, &tableName, &statsBytes, &createAt, &deleteAt, &isTombstone); err != nil {
				err = moerr.NewInternalErrorf(ctx, "failed to scan object list result: %v", err)
				return nil, err
			}

			// Parse ObjectStats from bytes
			var stats objectio.ObjectStats
			stats.UnMarshal(statsBytes)

			// Get ObjectId from stats
			objID := *stats.ObjectName().ObjectId()
			delete := !deleteAt.IsEmpty()

			// Check if this object already exists in map
			if _, exists := objectMap[objID]; exists {
				// If there are two records, one without delete and one with delete, use delete to override
				if delete {
					// New record is delete, override existing record
					objectMap[objID] = &ObjectWithTableInfo{
						Stats:       stats,
						IsTombstone: isTombstone,
						Delete:      true,
						DBName:      dbName,
						TableName:   tableName,
					}
				}
			} else {
				// New object, add to map
				objectMap[objID] = &ObjectWithTableInfo{
					Stats:       stats,
					IsTombstone: isTombstone,
					Delete:      delete,
					DBName:      dbName,
					TableName:   tableName,
				}
			}

		}

	}

	return objectMap, nil
}

func ApplyObjects(
	ctx context.Context,
	taskID string,
	accountID uint32,
	indexTableMappings map[string]string,
	objectMap map[objectio.ObjectId]*ObjectWithTableInfo,
	upstreamExecutor SQLExecutor,
	localExecutor SQLExecutor,
	currentTS types.TS,
	txn client.TxnOperator,
	cnEngine engine.Engine,
	mp *mpool.MPool,
	fs fileservice.FileService,
	filterObjectWorker FilterObjectWorker,
	getChunkWorker GetChunkWorker,
	writeObjectWorker WriteObjectWorker,
	subscriptionAccountName string,
	pubName string,
	ccprCache CCPRTxnCacheWriter,
	aobjectMap AObjectMap,
	ttlChecker TTLChecker,
) (err error) {
	// Check TTL before starting
	if ttlChecker != nil && ttlChecker() {
		return ErrSyncProtectionTTLExpired
	}

	var collectedTombstoneDeleteStats []*ObjectWithTableInfo
	var collectedTombstoneInsertStats []*ObjectWithTableInfo
	var collectedDataDeleteStats []*ObjectWithTableInfo
	var collectedDataInsertStats []*ObjectWithTableInfo

	// Get txnID from txn operator for CCPR cache
	var txnID []byte
	if txn != nil {
		txnID = txn.Txn().ID
	}

	// Separate data objects and tombstone objects
	var dataObjects []*ObjectWithTableInfo
	var tombstoneObjects []*ObjectWithTableInfo
	for _, info := range objectMap {
		// Apply index table name mapping
		if indexTableMappings != nil {
			if downstreamName, exists := indexTableMappings[info.TableName]; exists {
				info.TableName = downstreamName
			}
		}
		if info.IsTombstone {
			tombstoneObjects = append(tombstoneObjects, info)
		} else {
			dataObjects = append(dataObjects, info)
		}
	}

	// Phase 1: Submit and process all DATA objects first (without aobjectMap for tombstone rewriting)
	// This ensures aobjectMap is populated with data object mappings before tombstone processing
	for _, info := range dataObjects {
		if !info.Delete {
			statsBytes := info.Stats.Marshal()
			// Data objects don't need aobjectMap for rewriting, pass nil
			filterJob := NewFilterObjectJob(ctx, statsBytes, currentTS, upstreamExecutor, false, fs, mp, getChunkWorker, writeObjectWorker, subscriptionAccountName, pubName, ccprCache, txnID, nil, ttlChecker)
			if filterObjectWorker != nil {
				filterObjectWorker.SubmitFilterObject(filterJob)
			} else {
				filterJob.Execute()
			}
			info.FilterJob = filterJob
		}
	}

	// Phase 2: Wait for all DATA filter jobs to complete and update aobjectMap
	for _, info := range dataObjects {
		if info.Stats.GetAppendable() {
			upstreamObjID := info.Stats.ObjectName().ObjectId()
			upstreamIDStr := upstreamObjID.String()

			if info.Delete {
				// Query existing mapping from aobjectMap
				if aobjectMap != nil {
					if existingMapping, exists := aobjectMap.Get(upstreamIDStr); exists {
						// Add existing downstream object to delete stats
						collectedDataDeleteStats = append(collectedDataDeleteStats, &ObjectWithTableInfo{
							Stats:       existingMapping.DownstreamStats,
							DBName:      existingMapping.DBName,
							TableName:   existingMapping.TableName,
							IsTombstone: false,
							Delete:      true,
						})
						// Delete the mapping from aobjectMap
						aobjectMap.Delete(upstreamIDStr)
					}
				}
			} else {
				filterResult := info.FilterJob.WaitDone().(*FilterObjectJobResult)
				if filterResult.Err != nil {
					err = moerr.NewInternalErrorf(ctx, "failed to filter data object: %v", filterResult.Err)
					return
				}
				// Query existing mapping from aobjectMap and delete old downstream object
				if aobjectMap != nil {
					if existingMapping, exists := aobjectMap.Get(upstreamIDStr); exists {
						collectedDataDeleteStats = append(collectedDataDeleteStats, &ObjectWithTableInfo{
							Stats:       existingMapping.DownstreamStats,
							DBName:      existingMapping.DBName,
							TableName:   existingMapping.TableName,
							IsTombstone: false,
							Delete:      true,
						})
					}
				}
				// Insert/update new mapping to aobjectMap
				if filterResult.HasMappingUpdate && filterResult.CurrentStats.ObjectName() != nil && aobjectMap != nil {
					aobjectMap.Set(upstreamIDStr, &AObjectMapping{
						DownstreamStats: filterResult.CurrentStats,
						IsTombstone:     false,
						DBName:          info.DBName,
						TableName:       info.TableName,
					})
					// Add new downstream object to insert stats
					collectedDataInsertStats = append(collectedDataInsertStats, &ObjectWithTableInfo{
						Stats:       filterResult.CurrentStats,
						DBName:      info.DBName,
						TableName:   info.TableName,
						IsTombstone: false,
						Delete:      false,
					})
				}
			}
		} else {
			// Handle non-appendable data objects
			if info.Delete {
				collectedDataDeleteStats = append(collectedDataDeleteStats, &ObjectWithTableInfo{
					Stats:       info.Stats,
					DBName:      info.DBName,
					TableName:   info.TableName,
					IsTombstone: false,
					Delete:      true,
				})
			} else {
				filterResult := info.FilterJob.WaitDone().(*FilterObjectJobResult)
				if filterResult.Err != nil {
					err = moerr.NewInternalErrorf(ctx, "failed to filter data object: %v", filterResult.Err)
					return
				}
				if !filterResult.DownstreamStats.IsZero() {
					collectedDataInsertStats = append(collectedDataInsertStats, &ObjectWithTableInfo{
						Stats:       filterResult.DownstreamStats,
						DBName:      info.DBName,
						TableName:   info.TableName,
						IsTombstone: false,
						Delete:      false,
					})
				}
			}
		}
	}

	// Phase 3: Now submit all TOMBSTONE objects (with aobjectMap for rowid rewriting)
	// At this point, aobjectMap contains all data object mappings
	for _, info := range tombstoneObjects {
		if !info.Delete {
			statsBytes := info.Stats.Marshal()
			// Tombstone objects need aobjectMap for rowid rewriting
			filterJob := NewFilterObjectJob(ctx, statsBytes, currentTS, upstreamExecutor, true, fs, mp, getChunkWorker, writeObjectWorker, subscriptionAccountName, pubName, ccprCache, txnID, aobjectMap, ttlChecker)
			if filterObjectWorker != nil {
				filterObjectWorker.SubmitFilterObject(filterJob)
			} else {
				filterJob.Execute()
			}
			info.FilterJob = filterJob
		}
	}

	// Phase 4: Wait for all TOMBSTONE filter jobs to complete
	for _, info := range tombstoneObjects {
		if info.Stats.GetAppendable() {
			upstreamObjID := info.Stats.ObjectName().ObjectId()
			upstreamIDStr := upstreamObjID.String()

			if info.Delete {
				// Query existing mapping from aobjectMap
				if aobjectMap != nil {
					if existingMapping, exists := aobjectMap.Get(upstreamIDStr); exists {
						collectedTombstoneDeleteStats = append(collectedTombstoneDeleteStats, &ObjectWithTableInfo{
							Stats:       existingMapping.DownstreamStats,
							DBName:      existingMapping.DBName,
							TableName:   existingMapping.TableName,
							IsTombstone: true,
							Delete:      true,
						})
						// Delete the mapping from aobjectMap
						aobjectMap.Delete(upstreamIDStr)
					}
				}
			} else {
				filterResult := info.FilterJob.WaitDone().(*FilterObjectJobResult)
				if filterResult.Err != nil {
					err = moerr.NewInternalErrorf(ctx, "failed to filter tombstone object: %v", filterResult.Err)
					return
				}
				// Query existing mapping from aobjectMap and delete old downstream object
				if aobjectMap != nil {
					if existingMapping, exists := aobjectMap.Get(upstreamIDStr); exists {
						collectedTombstoneDeleteStats = append(collectedTombstoneDeleteStats, &ObjectWithTableInfo{
							Stats:       existingMapping.DownstreamStats,
							DBName:      existingMapping.DBName,
							TableName:   existingMapping.TableName,
							IsTombstone: true,
							Delete:      true,
						})
					}
				}
				// Insert/update new mapping to aobjectMap
				if filterResult.HasMappingUpdate && filterResult.CurrentStats.ObjectName() != nil && aobjectMap != nil {
					aobjectMap.Set(upstreamIDStr, &AObjectMapping{
						DownstreamStats: filterResult.CurrentStats,
						IsTombstone:     true,
						DBName:          info.DBName,
						TableName:       info.TableName,
					})
					collectedTombstoneInsertStats = append(collectedTombstoneInsertStats, &ObjectWithTableInfo{
						Stats:       filterResult.CurrentStats,
						DBName:      info.DBName,
						TableName:   info.TableName,
						IsTombstone: true,
						Delete:      false,
					})
				}
			}
		} else {
			// Handle non-appendable tombstone objects
			if info.Delete {
				collectedTombstoneDeleteStats = append(collectedTombstoneDeleteStats, &ObjectWithTableInfo{
					Stats:       info.Stats,
					DBName:      info.DBName,
					TableName:   info.TableName,
					IsTombstone: true,
					Delete:      true,
				})
			} else {
				filterResult := info.FilterJob.WaitDone().(*FilterObjectJobResult)
				if filterResult.Err != nil {
					err = moerr.NewInternalErrorf(ctx, "failed to filter tombstone object: %v", filterResult.Err)
					return
				}
				if !filterResult.DownstreamStats.IsZero() {
					collectedTombstoneInsertStats = append(collectedTombstoneInsertStats, &ObjectWithTableInfo{
						Stats:       filterResult.DownstreamStats,
						DBName:      info.DBName,
						TableName:   info.TableName,
						IsTombstone: true,
						Delete:      false,
					})
				}
			}
		}
	}

	// Submit all collected objects to TN in order: tombstone delete -> tombstone insert -> data delete -> data insert
	// Use downstream account ID from iterationCtx.SrcInfo
	// Set PkCheckByTN to SkipAllDedup to completely skip all deduplication checks in TN
	downstreamCtx := context.WithValue(ctx, defines.TenantIDKey{}, accountID)
	downstreamCtx = context.WithValue(downstreamCtx, defines.PkCheckByTN{}, int8(cmd_util.SkipAllDedup))

	// 1. Submit tombstone delete objects (soft delete)
	if len(collectedTombstoneDeleteStats) > 0 {
		if err = submitObjectsAsDelete(downstreamCtx, taskID, txn, cnEngine, collectedTombstoneDeleteStats, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit tombstone delete objects: %v", err)
			return
		}
	}

	// 2. Submit tombstone insert objects
	if len(collectedTombstoneInsertStats) > 0 {
		if err = submitObjectsAsInsert(downstreamCtx, taskID, txn, cnEngine, collectedTombstoneInsertStats, nil, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit tombstone insert objects: %v", err)
			return
		}
	}

	// 3. Submit data delete objects (soft delete)
	if len(collectedDataDeleteStats) > 0 {
		if err = submitObjectsAsDelete(downstreamCtx, taskID, txn, cnEngine, collectedDataDeleteStats, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit data delete objects: %v", err)
			return
		}
	}

	// 4. Submit data insert objects
	if len(collectedDataInsertStats) > 0 {
		if err = submitObjectsAsInsert(downstreamCtx, taskID, txn, cnEngine, nil, collectedDataInsertStats, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit data insert objects: %v", err)
			return
		}
	}
	return
}
