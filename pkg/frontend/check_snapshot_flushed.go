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
var checkSnapshotFlushedFunc = func(ctx context.Context, txn client.TxnOperator, snapshotTs int64, engine *disttae.Engine, level, databaseName, tableName string) (bool, error) {
	return CheckSnapshotFlushed(ctx, txn, snapshotTs, engine, level, databaseName, tableName)
}

// getFileServiceFunc is a function variable for getting fileservice from disttae.Engine
// This allows mocking in unit tests
var getFileServiceFunc = func(de *disttae.Engine) fileservice.FileService {
	return de.FS()
}

// getAccountFromPublicationFunc is a function variable for getAccountFromPublication
// This allows mocking in unit tests
var getAccountFromPublicationFunc = getAccountFromPublication

// handleInternalCheckSnapshotFlushed handles the internal command for check snapshot flushed
// It checks publication permission first and then checks if snapshot is flushed
func handleInternalCheckSnapshotFlushed(ses FeSession, execCtx *ExecCtx, cmd *InternalCmdCheckSnapshotFlushed) error {
	ctx := execCtx.reqCtx
	session := ses.(*Session)

	var mrs MysqlResultSet
	session.ClearAllMysqlResultSet()

	// Create column
	col := new(MysqlColumn)
	col.SetColumnType(defines.MYSQL_TYPE_BOOL)
	col.SetName("result")
	mrs.AddColumn(col)

	snapshotName := cmd.snapshotName
	subscriptionAccountName := cmd.subscriptionAccountName
	publicationName := cmd.publicationName

	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	// Get current account name
	currentAccount := ses.GetTenantInfo().GetTenant()

	// Step 1: Check permission via publication and get authorized account
	accountId, _, err := getAccountFromPublicationFunc(ctx, bh, subscriptionAccountName, publicationName, currentAccount)
	if err != nil {
		return err
	}

	// Use the authorized account context for snapshot query
	ctx = defines.AttachAccountId(ctx, uint32(accountId))

	logutil.Info("internal_check_snapshot_flushed using authorized account via publication",
		zap.String("snapshot_name", snapshotName),
		zap.String("subscription_account_name", subscriptionAccountName),
		zap.String("publication_name", publicationName),
		zap.Uint64("account_id", accountId),
	)

	// Step 2: Get snapshot record
	record, err := getSnapshotByNameFunc(ctx, bh, snapshotName)
	if err != nil {
		return err
	}

	if record == nil {
		return moerr.NewInternalError(ctx, "snapshot not found")
	}

	// Step 3: Get fileservice from session
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

	result, err := checkSnapshotFlushedFunc(ctx, txn, record.ts, de, record.level, record.databaseName, record.tableName)
	if err != nil {
		return err
	}
	row := []interface{}{result}
	mrs.AddRow(row)

	ses.SetMysqlResultSet(&mrs)
	return nil
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

	// Check publication permission using getAccountFromPublicationFunc
	if accountName == "" || publicationName == "" {
		return moerr.NewInternalError(ctx, "publication account name and publication name are required for CHECK SNAPSHOT FLUSHED")
	}

	// Get current account name
	currentAccount := ses.GetTenantInfo().GetTenant()

	// Step 1: Check permission via publication and get authorized account
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

	// Step 2: Get snapshot record
	record, err := getSnapshotByNameFunc(ctx, bh, snapshotName)
	if err != nil {
		return err
	}

	if record == nil {
		return moerr.NewInternalError(ctx, "snapshot not found")
	}

	// Step 3: Get fileservice from session
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

	result, err := checkSnapshotFlushedFunc(ctx, txn, record.ts, de, record.level, record.databaseName, record.tableName)
	if err != nil {
		return err
	}
	row := []interface{}{result}
	mrs.AddRow(row)

	ses.SetMysqlResultSet(mrs)
	return nil
}

// SnapshotInfo represents the exported snapshot information
// This can be used by external packages without exposing internal snapshotRecord
type SnapshotInfo struct {
	SnapshotId   string
	SnapshotName string
	Ts           int64
	Level        string
	AccountName  string
	DatabaseName string
	TableName    string
	ObjId        uint64
}

// GetSnapshotInfoByName gets snapshot info by name using executor
// This is an exported function that can be used without Session
func GetSnapshotInfoByName(ctx context.Context, sqlExecutor executor.SQLExecutor, txnOp client.TxnOperator, snapshotName string) (*SnapshotInfo, error) {
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

	var info SnapshotInfo
	var found bool
	queryResult.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 && len(cols) >= 8 {
			// Column 0: snapshot_id (UUID type, convert to string)
			if cols[0].Length() > 0 {
				info.SnapshotId = vector.GetFixedAtWithTypeCheck[types.Uuid](cols[0], 0).String()
			}
			if cols[1].Length() > 0 {
				info.SnapshotName = cols[1].GetStringAt(0)
			}
			if cols[2].Length() > 0 {
				info.Ts = vector.GetFixedAtWithTypeCheck[int64](cols[2], 0)
			}
			if cols[3].Length() > 0 {
				info.Level = cols[3].GetStringAt(0)
			}
			if cols[4].Length() > 0 {
				info.AccountName = cols[4].GetStringAt(0)
			}
			if cols[5].Length() > 0 {
				info.DatabaseName = cols[5].GetStringAt(0)
			}
			if cols[6].Length() > 0 {
				info.TableName = cols[6].GetStringAt(0)
			}
			if cols[7].Length() > 0 {
				info.ObjId = vector.GetFixedAtWithTypeCheck[uint64](cols[7], 0)
			}
			found = true
		}
		return true
	})

	if !found {
		return nil, moerr.NewInternalErrorf(ctx, "snapshot %s does not exist", snapshotName)
	}

	return &info, nil
}

// CheckSnapshotFlushed checks if a snapshot's timestamp is less than or equal to the checkpoint watermark
// This is an exported function that can be used without Session
// Parameters:
//   - level: snapshot level ("account", "database", "table")
//   - databaseName: database name (required for "database" and "table" levels)
//   - tableName: table name (required for "table" level)
//   - snapshotTs: snapshot timestamp
func CheckSnapshotFlushed(ctx context.Context, txn client.TxnOperator, snapshotTs int64, engine *disttae.Engine, level, databaseName, tableName string) (bool, error) {
	switch level {
	case "account":
		dbs, err := engine.Databases(ctx, txn)
		if err != nil {
			return false, err
		}
		for _, dbName := range dbs {
			flushed, err := checkDBFlushTS(ctx, txn, dbName, engine, snapshotTs)
			if err != nil {
				return false, err
			}
			if !flushed {
				return false, nil
			}
		}
		return true, nil
	case "database":
		flushed, err := checkDBFlushTS(ctx, txn, databaseName, engine, snapshotTs)
		if err != nil {
			return false, err
		}
		if !flushed {
			return false, nil
		}
		return true, nil
	case "table":
		db, err := engine.Database(ctx, databaseName, txn)
		if err != nil {
			return false, err
		}
		flushed, err := checkTableFlushTS(ctx, db, tableName, snapshotTs)
		if err != nil {
			return false, err
		}
		return flushed, nil
	default:
		return false, moerr.NewInternalError(ctx, fmt.Sprintf("invalid snapshot level: %s", level))
	}
}

func checkDBFlushTS(
	ctx context.Context,
	txn client.TxnOperator,
	dbName string,
	engine *disttae.Engine,
	snapshotTs int64,
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
		flushed, err := checkTableFlushTS(ctx, db, tblName, snapshotTs)
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
	snapshotTs int64,
) (flushed bool, err error) {
	snapshotTS := types.BuildTS(snapshotTs, 0)
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
