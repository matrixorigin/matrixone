// Copyright 2024 Matrix Origin
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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func handleCheckSnapshotFlushed(ses *Session, execCtx *ExecCtx, stmt *tree.CheckSnapshotFlushed) error {
	return doCheckSnapshotFlushed(execCtx.reqCtx, ses, stmt)
}

func doCheckSnapshotFlushed(ctx context.Context, ses *Session, stmt *tree.CheckSnapshotFlushed) error {
	var mrs = ses.GetMysqlResultSet()
	ses.ClearAllMysqlResultSet()

	// Create column
	col := new(MysqlColumn)
	col.SetColumnType(defines.MYSQL_TYPE_BOOL)
	col.SetName("result")
	mrs.AddColumn(col)

	// Get snapshot by name and retrieve ts
	snapshotName := string(stmt.Name)
	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	record, err := getSnapshotByName(ctx, bh, snapshotName)
	if err != nil {
		// If snapshot not found, return false
		result := false
		row := []interface{}{result}
		mrs.AddRow(row)
		ses.SetMysqlResultSet(mrs)
		return nil
	}

	// Print snapshot ts using logutil
	if record == nil {
		return moerr.NewInternalError(ctx, "snapshot not found")
	}
	// Get fileservice from session
	eng := getPu(ses.GetService()).StorageEngine
	if eng == nil {
		return moerr.NewInternalError(ctx, "engine is not available")
	}

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

	// Mock result: always return true for now
	result, err := CheckSnapshotFlushed(ctx, types.BuildTS(record.ts, 0), fs)
	if err != nil {
		return err
	}
	row := []interface{}{result}
	mrs.AddRow(row)

	ses.SetMysqlResultSet(mrs)
	return nil
}

// CheckSnapshotFlushed checks if a snapshot's timestamp is less than or equal to the checkpoint watermark
// This is an exported function that can be used without Session
func CheckSnapshotFlushed(ctx context.Context, snapshotTS types.TS, fs fileservice.FileService) (bool, error) {
	// Scan checkpoint directory and find max end ts
	metaFiles, err := ckputil.ListCKPMetaFiles(ctx, fs)
	if err != nil {
		return false, err
	}

	if len(metaFiles) == 0 {
		return false, nil
	}

	// Sort by end ts to find the maximum
	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].GetEnd().LT(metaFiles[j].GetEnd())
	})

	maxFile := metaFiles[len(metaFiles)-1]
	maxEndTS := maxFile.GetEnd()
	return maxEndTS.GE(&snapshotTS), nil
}
