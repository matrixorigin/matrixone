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
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

// readObjectFromFS reads the object file from fileservice and returns its content as []byte
func readObjectFromFS(ctx context.Context, ses *Session, objectName string) ([]byte, error) {
	eng := getPu(ses.GetService()).StorageEngine
	return ReadObjectFromEngine(ctx, eng, objectName)
}

// ReadObjectFromEngine reads the object file from engine's fileservice and returns its content as []byte
// This is a version that doesn't require Session
func ReadObjectFromEngine(ctx context.Context, eng engine.Engine, objectName string) ([]byte, error) {
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
				Offset:            0,
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	})
	if err != nil {
		return nil, err
	}
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

	// Create column: data
	col := new(MysqlColumn)
	col.SetName("data")
	col.SetColumnType(defines.MYSQL_TYPE_BLOB)
	showCols = append(showCols, col)

	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	// Read object from fileservice
	objectName := stmt.ObjectName.String()
	content, err := readObjectFromFS(ctx, ses, objectName)
	if err != nil {
		return err
	}

	// Add one row with the file content
	row := make([]any, 1)
	row[0] = content
	mrs.AddRow(row)

	// Save query result if needed
	return trySaveQueryResult(ctx, ses, mrs)
}
