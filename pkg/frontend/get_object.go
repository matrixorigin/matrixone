// Copyright 2025 Matrix Origin
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

package frontend

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/publication"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func init() {
	// Periodically log chunkSemaphore statistics
	go func() {
		for {
			time.Sleep(10 * time.Second)
			waiting := atomic.LoadInt64(&chunkSemaphoreWaiting)
			holding := atomic.LoadInt64(&chunkSemaphoreHolding)
			finished := atomic.LoadInt64(&chunkSemaphoreFinished)
			logutil.Infof("[chunkSemaphore] STATS: waiting=%d, holding=%d, finished=%d, max=%d", waiting, holding, finished, getObjectMaxConcurrent)
		}
	}()
}

const (
	// getObjectChunkSize is the size of each chunk for GetObject (100MB)
	getObjectChunkSize = publication.GetChunkSize
	// getObjectMaxMemory is the maximum memory for concurrent GetObject operations (5GB)
	getObjectMaxMemory = publication.GetChunkMaxMemory
	// getObjectMaxConcurrent is the maximum concurrent chunk reads (5GB / 100MB = 50)
	getObjectMaxConcurrent = getObjectMaxMemory / getObjectChunkSize
)

// chunkBufferPool is a pool for reusing 100MB buffers in GetObject
var chunkBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, getObjectChunkSize)
		return &buf
	},
}

// chunkSemaphore limits concurrent memory usage for GetObject (5GB max)
var chunkSemaphore = make(chan struct{}, getObjectMaxConcurrent)

// Counters for tracking semaphore usage (only counts goroutines currently waiting or holding)
var (
	chunkSemaphoreWaiting  int64 // goroutines currently waiting for semaphore
	chunkSemaphoreHolding  int64 // goroutines currently holding semaphore
	chunkSemaphoreFinished int64 // total finished requests
)

// getGoroutineID extracts goroutine ID from runtime stack
func getGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Stack format: "goroutine 123 [running]:\n..."
	idField := bytes.Fields(buf[:n])[1]
	id, _ := strconv.ParseInt(string(idField), 10, 64)
	return id
}

// GetObjectPermissionChecker is the function to check publication permission for GetObject
// This is exported as a variable to allow stubbing in tests
var GetObjectPermissionChecker = func(ctx context.Context, ses *Session) error {
	return checkPublicationPermissionForGetObject(ctx, ses)
}

// GetObjectFSProvider is the function to get fileservice for GetObject
// This is exported as a variable to allow stubbing in tests
var GetObjectFSProvider = func(ses *Session) (fileservice.FileService, error) {
	eng := getPu(ses.GetService()).StorageEngine
	var de *disttae.Engine
	var ok bool
	if de, ok = eng.(*disttae.Engine); !ok {
		var entireEngine *engine.EntireEngine
		if entireEngine, ok = eng.(*engine.EntireEngine); ok {
			de, ok = entireEngine.Engine.(*disttae.Engine)
		}
		if !ok {
			return nil, moerr.NewInternalErrorNoCtx("failed to get disttae engine")
		}
	}

	fs := de.FS()
	if fs == nil {
		return nil, moerr.NewInternalErrorNoCtx("fileservice is not available")
	}
	return fs, nil
}

// GetObjectDataReader is the function to read object data from fileservice
// This is exported as a variable to allow stubbing in tests
var GetObjectDataReader = func(ctx context.Context, ses *Session, objectName string, offset int64, size int64) ([]byte, error) {
	return readObjectFromFS(ctx, ses, objectName, offset, size)
}

// readObjectFromFS reads the object file from fileservice and returns its content as []byte
func readObjectFromFS(ctx context.Context, ses *Session, objectName string, offset int64, size int64) ([]byte, error) {
	eng := getPu(ses.GetService()).StorageEngine
	return ReadObjectFromEngine(ctx, eng, objectName, offset, size)
}

// ReadObjectFromEngine reads the object file from engine's fileservice and returns its content as []byte
// offset: 读取偏移，>=0
// size: 读取大小，必须 > 0 且 <= 100MB (getObjectChunkSize)
// This is a version that doesn't require Session
func ReadObjectFromEngine(ctx context.Context, eng engine.Engine, objectName string, offset int64, size int64) ([]byte, error) {
	if eng == nil {
		return nil, moerr.NewInternalError(ctx, "engine is not available")
	}

	// Validate size: must be positive and within chunk size limit
	if size <= 0 {
		return nil, moerr.NewInternalError(ctx, "size must be positive")
	}
	if size > getObjectChunkSize {
		return nil, moerr.NewInternalError(ctx, "size exceeds maximum chunk size (100MB)")
	}

	var de *disttae.Engine
	var ok bool
	if de, ok = eng.(*disttae.Engine); !ok {
		var entireEngine *engine.EntireEngine
		if entireEngine, ok = eng.(*engine.EntireEngine); ok {
			de, ok = entireEngine.Engine.(*disttae.Engine)
		}
		if !ok {
			return nil, moerr.NewInternalError(ctx, "failed to get disttae engine")
		}
	}

	fs := de.FS()
	if fs == nil {
		return nil, moerr.NewInternalError(ctx, "fileservice is not available")
	}

	atomic.AddInt64(&chunkSemaphoreWaiting, 1)
	atomic.LoadInt64(&chunkSemaphoreHolding)

	select {
	case chunkSemaphore <- struct{}{}:
		// acquired - remove from waiting, add to holding
		atomic.AddInt64(&chunkSemaphoreWaiting, -1)
		atomic.AddInt64(&chunkSemaphoreHolding, 1)
		atomic.LoadInt64(&chunkSemaphoreWaiting)
	case <-ctx.Done():
		atomic.AddInt64(&chunkSemaphoreWaiting, -1)
		return nil, ctx.Err()
	}
	defer func() {
		<-chunkSemaphore
		atomic.AddInt64(&chunkSemaphoreHolding, -1)
		atomic.AddInt64(&chunkSemaphoreFinished, 1)
	}()

	// Get buffer from pool for reuse
	bufPtr := chunkBufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer chunkBufferPool.Put(bufPtr)

	// Use pre-allocated buffer in IOEntry.Data to avoid fileservice internal allocation
	entry := fileservice.IOEntry{
		Offset: offset,
		Size:   size,
		Data:   buf[:size],
	}

	err := fs.Read(ctx, &fileservice.IOVector{
		FilePath: objectName,
		Entries:  []fileservice.IOEntry{entry},
	})
	if err != nil {
		return nil, err
	}

	// Copy result to a new slice (buffer will be returned to pool)
	result := make([]byte, size)
	copy(result, entry.Data[:size])

	return result, nil
}

func handleGetObject(
	ctx context.Context,
	ses *Session,
	stmt *tree.GetObject,
) error {
	var (
		mrs      = ses.GetMysqlResultSet()
		showCols []*MysqlColumn
	)

	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()

	// Create columns: data, total_size, chunk_index, total_chunks, is_complete
	colData := new(MysqlColumn)
	colData.SetName("data")
	colData.SetColumnType(defines.MYSQL_TYPE_BLOB)
	showCols = append(showCols, colData)

	colTotalSize := new(MysqlColumn)
	colTotalSize.SetName("total_size")
	colTotalSize.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	showCols = append(showCols, colTotalSize)

	colChunkIndex := new(MysqlColumn)
	colChunkIndex.SetName("chunk_index")
	colChunkIndex.SetColumnType(defines.MYSQL_TYPE_LONG)
	showCols = append(showCols, colChunkIndex)

	colTotalChunks := new(MysqlColumn)
	colTotalChunks.SetName("total_chunks")
	colTotalChunks.SetColumnType(defines.MYSQL_TYPE_LONG)
	showCols = append(showCols, colTotalChunks)

	colIsComplete := new(MysqlColumn)
	colIsComplete.SetName("is_complete")
	colIsComplete.SetColumnType(defines.MYSQL_TYPE_TINY)
	showCols = append(showCols, colIsComplete)

	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	// Read object from fileservice
	objectName := stmt.ObjectName.String()
	chunkIndex := stmt.ChunkIndex

	// Check publication permission
	// For GET OBJECT, we check if the account has permission to access any publication
	// since objectName doesn't contain database/table information
	if err := GetObjectPermissionChecker(ctx, ses); err != nil {
		return err
	}

	// Get fileservice
	fs, err := GetObjectFSProvider(ses)
	if err != nil {
		return err
	}

	// Get file size
	dirEntry, err := fs.StatFile(ctx, objectName)
	if err != nil {
		return err
	}
	fileSize := dirEntry.Size

	// Calculate total data chunks (chunk 0 is metadata, chunks 1+ are data)
	var totalChunks int64
	if fileSize <= getObjectChunkSize {
		totalChunks = 1
	} else {
		totalChunks = (fileSize + getObjectChunkSize - 1) / getObjectChunkSize // 向上取整
	}

	// Validate chunk index
	if chunkIndex < 0 {
		return moerr.NewInvalidInput(ctx, "invalid chunk_index: must be >= 0")
	}
	// chunk 0 is metadata, chunks 1 to totalChunks are data chunks
	if chunkIndex > totalChunks {
		return moerr.NewInvalidInput(ctx, fmt.Sprintf("invalid chunk_index: %d, file has only %d data chunks (chunk 0 is metadata)", chunkIndex, totalChunks))
	}

	var data []byte
	var isComplete bool

	if chunkIndex == 0 {
		// Metadata only request - return nil data with metadata information
		data = nil
		isComplete = false

	} else {
		// Data chunk request (chunkIndex >= 1)
		// Calculate offset: chunk 1 starts at offset 0, chunk 2 at getObjectChunkSize, etc.
		offset := (chunkIndex - 1) * getObjectChunkSize
		size := int64(getObjectChunkSize)
		if chunkIndex == totalChunks {
			// Last chunk may be smaller
			size = fileSize - offset
		}

		// Read the chunk data
		data, err = GetObjectDataReader(ctx, ses, objectName, offset, size)
		if err != nil {
			return err
		}

		isComplete = (chunkIndex == totalChunks)
	}

	// Add row with the result
	row := make([]any, 5)
	row[0] = data
	row[1] = fileSize
	row[2] = chunkIndex
	row[3] = totalChunks
	row[4] = isComplete
	mrs.AddRow(row)

	// Save query result if needed
	return trySaveQueryResult(ctx, ses, mrs)
}
