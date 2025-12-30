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
	"context"
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

// readObjectFromFS reads the object file from fileservice and returns its content as []byte
func readObjectFromFS(ctx context.Context, ses *Session, objectName string, offset int64, size int64) ([]byte, error) {
	eng := getPu(ses.GetService()).StorageEngine
	return ReadObjectFromEngine(ctx, eng, objectName, offset, size)
}

// ReadObjectFromEngine reads the object file from engine's fileservice and returns its content as []byte
// offset: 读取偏移，>=0
// size: 读取大小，-1 表示读到末尾
// This is a version that doesn't require Session
func ReadObjectFromEngine(ctx context.Context, eng engine.Engine, objectName string, offset int64, size int64) ([]byte, error) {
	if eng == nil {
		return nil, moerr.NewInternalError(ctx, "engine is not available")
	}

	var de *disttae.Engine
	var ok bool
	if de, ok = eng.(*disttae.Engine); !ok {
		if entireEngine, ok := eng.(*engine.EntireEngine); ok {
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

	var r io.ReadCloser
	err := fs.Read(ctx, &fileservice.IOVector{
		FilePath: objectName,
		Entries: []fileservice.IOEntry{
			{
				Offset:            offset,
				Size:              size,
				ReadCloserForRead: &r,
			},
		},
	})
	if err != nil {
		// If ReadCloser was set even on error, close it to release resources
		if r != nil {
			r.Close()
		}
		return nil, err
	}
	// Ensure ReadCloser is closed to release memory allocated by fileservice
	// The ReadCloser now properly manages memory lifecycle (see io_entry.go fix)
	defer r.Close()

	content, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return content, nil
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
	if err := checkPublicationPermissionForGetObject(ctx, ses); err != nil {
		return err
	}

	// Get fileservice
	eng := getPu(ses.GetService()).StorageEngine
	var de *disttae.Engine
	var ok bool
	if de, ok = eng.(*disttae.Engine); !ok {
		if entireEngine, ok := eng.(*engine.EntireEngine); ok {
			de, ok = entireEngine.Engine.(*disttae.Engine)
		}
		if !ok {
			return moerr.NewInternalError(ctx, "failed to get disttae engine")
		}
	}

	fs := de.FS()
	if fs == nil {
		return moerr.NewInternalError(ctx, "fileservice is not available")
	}

	// Get file size
	dirEntry, err := fs.StatFile(ctx, objectName)
	if err != nil {
		return err
	}
	fileSize := dirEntry.Size

	// Calculate total data chunks (chunk 0 is metadata, chunks 1+ are data)
	const chunkSize = 1 * 1024 * 1024 // 1MB
	var totalChunks int64
	if fileSize <= chunkSize {
		totalChunks = 1
	} else {
		totalChunks = (fileSize + chunkSize - 1) / chunkSize // 向上取整
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

		// Print detailed chunk information if there are multiple chunks
		if totalChunks > 1 {
			chunkDetails := make([]zap.Field, 0, totalChunks+1)
			chunkDetails = append(chunkDetails, zap.String("object", objectName))
		}
	} else {
		// Data chunk request (chunkIndex >= 1)
		// Calculate offset: chunk 1 starts at offset 0, chunk 2 at chunkSize, etc.
		offset := (chunkIndex - 1) * chunkSize
		size := int64(chunkSize)
		if chunkIndex == totalChunks {
			// Last chunk may be smaller
			size = fileSize - offset
		}

		// Read the chunk data
		data, err = readObjectFromFS(ctx, ses, objectName, offset, size)
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
