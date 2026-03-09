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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// Job types
const (
	JobTypeGetMeta      int8 = 1
	JobTypeGetChunk     int8 = 2
	JobTypeFilterObject int8 = 3
	JobTypeWriteObject  int8 = 4
)

var (
	// getChunkSemaphore limits concurrent memory usage for GetChunkJob
	// Initialized lazily based on configuration
	getChunkSemaphore     chan struct{}
	getChunkSemaphoreOnce sync.Once
)

// getOrCreateChunkSemaphore returns the chunk semaphore, creating it if necessary
func getOrCreateChunkSemaphore() chan struct{} {
	getChunkSemaphoreOnce.Do(func() {
		maxConcurrent := GetGlobalConfig().GetChunkMaxConcurrent()
		if maxConcurrent <= 0 {
			maxConcurrent = 30 // fallback default
		}
		getChunkSemaphore = make(chan struct{}, maxConcurrent)
	})
	return getChunkSemaphore
}

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

// Execute runs the GetMetaJob with retry logic
func (j *GetMetaJob) Execute() {
	const maxRetries = 3
	res := &GetMetaJobResult{}
	getChunk0SQL := PublicationSQLBuilder.GetObjectSQL(j.subscriptionAccountName, j.pubName, j.objectName, 0)

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-j.ctx.Done():
			res.Err = j.ctx.Err()
			j.result <- res
			return
		default:
		}

		result, cancel, err := j.upstreamExecutor.ExecSQL(j.ctx, nil, InvalidAccountID, getChunk0SQL, false, true, time.Second*10)
		if err != nil {
			lastErr = err
			if !(DefaultClassifier{}).IsRetryable(err) {
				res.Err = moerr.NewInternalErrorf(j.ctx, "failed to execute GETOBJECT query for offset 0: %v", err)
				j.result <- res
				return
			}
			continue
		}

		if !result.Next() {
			if err := result.Err(); err != nil {
				result.Close()
				cancel()
				lastErr = err
				if !(DefaultClassifier{}).IsRetryable(err) {
					res.Err = moerr.NewInternalErrorf(j.ctx, "failed to read metadata for %s: %v", j.objectName, err)
					j.result <- res
					return
				}
				continue
			}
			result.Close()
			cancel()
			res.Err = moerr.NewInternalErrorf(j.ctx, "no object content returned for %s", j.objectName)
			j.result <- res
			return
		}

		if err := result.Scan(&res.MetadataData, &res.TotalSize, &res.ChunkIndex, &res.TotalChunks, &res.IsComplete); err != nil {
			result.Close()
			cancel()
			lastErr = err
			if !(DefaultClassifier{}).IsRetryable(err) {
				res.Err = moerr.NewInternalErrorf(j.ctx, "failed to scan offset 0: %v", err)
				j.result <- res
				return
			}
			continue
		}
		result.Close()
		cancel()

		if res.TotalChunks <= 0 {
			res.Err = moerr.NewInternalErrorf(j.ctx, "invalid total_chunks: %d", res.TotalChunks)
			j.result <- res
			return
		}

		// Success
		j.result <- res
		return
	}

	// All retries failed
	res.Err = moerr.NewInternalErrorf(j.ctx, "failed to get metadata for %s after %d retries: %v", j.objectName, maxRetries, lastErr)
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

// Execute runs the GetChunkJob with retry logic
func (j *GetChunkJob) Execute() {
	const maxRetries = 3
	res := &GetChunkJobResult{ChunkIndex: j.chunkIndex}

	// Acquire semaphore for memory control (blocks if max concurrent limit reached)
	sem := getOrCreateChunkSemaphore()
	select {
	case sem <- struct{}{}:
		// acquired
	case <-j.ctx.Done():
		res.Err = j.ctx.Err()
		j.result <- res
		return
	}
	defer func() { <-sem }()

	getChunkSQL := PublicationSQLBuilder.GetObjectSQL(j.subscriptionAccountName, j.pubName, j.objectName, j.chunkIndex)

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-j.ctx.Done():
			res.Err = j.ctx.Err()
			j.result <- res
			return
		default:
		}

		result, cancel, err := j.upstreamExecutor.ExecSQL(j.ctx, nil, InvalidAccountID, getChunkSQL, false, true, time.Minute)
		if err != nil {
			lastErr = err
			if !(DefaultClassifier{}).IsRetryable(err) {
				res.Err = moerr.NewInternalErrorf(j.ctx, "failed to execute GETOBJECT query for offset %d: %v, sql: %v", j.chunkIndex, err, getChunkSQL)
				j.result <- res
				return
			}
			continue
		}

		if result.Next() {
			var chunkData []byte
			var totalSizeChk int64
			var chunkIndexChk int64
			var totalChunksChk int64
			var isCompleteChk bool
			if err := result.Scan(&chunkData, &totalSizeChk, &chunkIndexChk, &totalChunksChk, &isCompleteChk); err != nil {
				result.Close()
				cancel()
				lastErr = err
				if !(DefaultClassifier{}).IsRetryable(err) {
					res.Err = moerr.NewInternalErrorf(j.ctx, "failed to scan offset %d: %v", j.chunkIndex, err)
					j.result <- res
					return
				}
				continue
			}
			res.ChunkData = chunkData
			result.Close()
			cancel()
			// Success
			j.result <- res
			return
		} else {
			if err := result.Err(); err != nil {
				result.Close()
				cancel()
				lastErr = err
				if !(DefaultClassifier{}).IsRetryable(err) {
					res.Err = moerr.NewInternalErrorf(j.ctx, "failed to read chunk %d of %s: %v", j.chunkIndex, j.objectName, err)
					j.result <- res
					return
				}
				continue
			}
			result.Close()
			cancel()
			// No data returned and no error - this is a real "no content" error, don't retry
			res.Err = moerr.NewInternalErrorf(j.ctx, "no chunk content returned for chunk %d of %s", j.chunkIndex, j.objectName)
			j.result <- res
			return
		}
	}

	// All retries failed
	res.Err = moerr.NewInternalErrorf(j.ctx, "failed to get chunk %d of %s after %d retries: %v", j.chunkIndex, j.objectName, maxRetries, lastErr)
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
	// RowOffsetMap maps original rowoffset to new rowoffset after sorting
	// Key: original rowoffset, Value: new rowoffset
	RowOffsetMap map[uint32]uint32
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
		res.RowOffsetMap = filterResult.RowOffsetMap
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
