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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"go.uber.org/zap"
)

// getSnapshotByNameFunc is a function variable for getSnapshotByName
// This allows mocking in unit tests
var getSnapshotByNameFunc = getSnapshotByName

// checkSnapshotFlushedFunc is a function variable for CheckSnapshotFlushed
// This allows mocking in unit tests
var checkSnapshotFlushedFunc = CheckSnapshotFlushed

// getFileServiceFunc is a function variable for getting fileservice from disttae.Engine
// This allows mocking in unit tests
var getFileServiceFunc = func(de *disttae.Engine) fileservice.FileService {
	return de.FS()
}

// getAccountFromPublicationFunc is a function variable for getAccountFromPublication
// This allows mocking in unit tests
var getAccountFromPublicationFunc = getAccountFromPublication

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
	accountName := string(stmt.AccountName)
	publicationName := string(stmt.PublicationName)

	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	// Check publication permission using getAccountFromPublication
	if accountName != "" && publicationName != "" {
		currentAccount := ses.GetTenantInfo().GetTenant()
		accountId, _, err := getAccountFromPublicationFunc(ctx, bh, accountName, publicationName, currentAccount)
		if err != nil {
			return err
		}

		// Use the authorized account context for snapshot query
		ctx = defines.AttachAccountId(ctx, uint32(accountId))

		logutil.Info("check_snapshot_flushed using authorized account via publication",
			zap.String("snapshot_name", snapshotName),
			zap.String("account_name", accountName),
			zap.String("publication_name", publicationName),
			zap.Uint64("account_id", accountId),
		)
	} else {
		return moerr.NewInternalError(ctx, "publication account name and publication name are required for CHECK SNAPSHOT FLUSHED")
	}

	record, err := getSnapshotByNameFunc(ctx, bh, snapshotName)
	if err != nil {
		return err
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
		var entireEngine *engine.EntireEngine
		if entireEngine, ok = eng.(*engine.EntireEngine); ok {
			de, ok = entireEngine.Engine.(*disttae.Engine)
		}
		if !ok {
			return moerr.NewInternalError(ctx, "failed to get disttae engine")
		}
	}

	fs := getFileServiceFunc(de)
	if fs == nil {
		return moerr.NewInternalError(ctx, "fileservice is not available")
	}

	txn := ses.GetTxnHandler().GetTxn()
	txn = txn.CloneSnapshotOp(timestamp.Timestamp{
		PhysicalTime: record.ts,
		LogicalTime:  0,
	})
	// Mock result: always return true for now
	result, err := checkSnapshotFlushedFunc(ctx, txn, types.BuildTS(record.ts, 0), de, record, fs)
	if err != nil {
		return err
	}
	row := []interface{}{result}
	mrs.AddRow(row)

	ses.SetMysqlResultSet(mrs)
	return nil
}

// GetSnapshotRecordByName gets snapshot record by name using executor
// This is an exported function that can be used without Session
func GetSnapshotRecordByName(ctx context.Context, sqlExecutor executor.SQLExecutor, txnOp client.TxnOperator, snapshotName string) (*snapshotRecord, error) {
	if err := inputNameIsInvalid(ctx, snapshotName); err != nil {
		return nil, err
	}

	// Query all snapshot fields: snapshot_id, sname, ts, level, account_name, database_name, table_name, obj_id
	querySQL := fmt.Sprintf(`select snapshot_id, sname, ts, level, account_name, database_name, table_name, obj_id from mo_catalog.mo_snapshots where sname = '%s' order by snapshot_id limit 1;`, snapshotName)
	opts := executor.Options{}.WithDisableIncrStatement().WithTxn(txnOp)
	queryResult, err := sqlExecutor.Exec(ctx, querySQL, opts)
	if err != nil {
		return nil, err
	}
	defer queryResult.Close()

	var record snapshotRecord
	var found bool
	queryResult.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 && len(cols) >= 8 {
			// Column 0: snapshot_id (UUID type, convert to string)
			if cols[0].Length() > 0 {
				record.snapshotId = vector.GetFixedAtWithTypeCheck[types.Uuid](cols[0], 0).String()
			}
			if cols[1].Length() > 0 {
				record.snapshotName = cols[1].GetStringAt(0)
			}
			if cols[2].Length() > 0 {
				record.ts = vector.GetFixedAtWithTypeCheck[int64](cols[2], 0)
			}
			if cols[3].Length() > 0 {
				record.level = cols[3].GetStringAt(0)
			}
			if cols[4].Length() > 0 {
				record.accountName = cols[4].GetStringAt(0)
			}
			if cols[5].Length() > 0 {
				record.databaseName = cols[5].GetStringAt(0)
			}
			if cols[6].Length() > 0 {
				record.tableName = cols[6].GetStringAt(0)
			}
			if cols[7].Length() > 0 {
				record.objId = vector.GetFixedAtWithTypeCheck[uint64](cols[7], 0)
			}
			found = true
		}
		return true
	})

	if !found {
		return nil, moerr.NewInternalErrorf(ctx, "snapshot %s does not exist", snapshotName)
	}

	return &record, nil
}

// GetSnapshotTS gets the timestamp from a snapshot record
// This is an exported function to access the unexported ts field
func GetSnapshotTS(record *snapshotRecord) int64 {
	return record.ts
}

// CheckSnapshotFlushed checks if a snapshot's timestamp is less than or equal to the checkpoint watermark
// This is an exported function that can be used without Session
func CheckSnapshotFlushed(ctx context.Context, txn client.TxnOperator, snapshotTS types.TS, engine *disttae.Engine, record *snapshotRecord, fs fileservice.FileService) (bool, error) {
	switch record.level {
	case "account":
		dbs, err := engine.Databases(ctx, txn)
		if err != nil {
			return false, err
		}
		for _, dbName := range dbs {
			flushed, err := checkDBFlushTS(ctx, txn, dbName, engine, record)
			if err != nil {
				return false, err
			}
			if !flushed {
				return false, nil
			}
		}
		return true, nil
	case "database":
		flushed, err := checkDBFlushTS(ctx, txn, record.databaseName, engine, record)
		if err != nil {
			return false, err
		}
		if !flushed {
			return false, nil
		}
		return true, nil
	case "table":
		db, err := engine.Database(ctx, record.databaseName, txn)
		if err != nil {
			return false, err
		}
		flushed, err := checkTableFlushTS(ctx, db, record.tableName, record)
		if err != nil {
			return false, err
		}
		return flushed, nil
	default:
		return false, moerr.NewInternalError(ctx, fmt.Sprintf("invalid snapshot level: %s", record.level))
	}
}

func checkDBFlushTS(
	ctx context.Context,
	txn client.TxnOperator,
	dbName string,
	engine *disttae.Engine,
	record *snapshotRecord,
) (flushed bool, err error) {
	db, err := engine.Database(ctx, dbName, txn)
	if err != nil {
		return false, err
	}
	tbls, err := db.Relations(ctx)
	if err != nil {
		return false, err
	}
	for _, tblName := range tbls {
		if catalog.IsHiddenTable(tblName) {
			continue
		}
		flushed, err := checkTableFlushTS(ctx, db, tblName, record)
		if err != nil {
			return false, err
		}
		if !flushed {
			return false, nil
		}
	}
	return true, nil
}

func checkTableFlushTS(
	ctx context.Context,
	db engine.Database,
	tableName string,
	record *snapshotRecord,
) (flushed bool, err error) {
	snapshotTS := types.BuildTS(record.ts, 0)
	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return false, err
	}
	def := rel.GetTableDef(ctx)
	tableNames := make(map[string]struct{})
	tableNames[tableName] = struct{}{}
	for _, index := range def.Indexes {
		tableNames[index.IndexTableName] = struct{}{}
	}
	for tbl := range tableNames {
		rel2, err := db.Relation(ctx, tbl, nil)
		if err != nil {
			return false, err
		}
		flushTS, err := rel2.GetFlushTS(ctx)
		if err != nil {
			return false, err
		}
		if flushTS.LT(&snapshotTS) {
			logutil.Info(
				"ccpr check flush ts failed",
				zap.String("snapshot", snapshotTS.String()),
				zap.String("flush ts", flushTS.String()),
				zap.String("table", tbl),
			)
			return false, nil
		}
	}
	return true, nil
}
