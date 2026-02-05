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

package test

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/publication"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"go.uber.org/zap"
)

// UpstreamSQLHelper handles special SQL statements (CREATE/DROP SNAPSHOT, OBJECTLIST, GET OBJECT, GETDDL)
// by routing them through frontend processing logic without requiring a Session
type UpstreamSQLHelper struct {
	txnOp        client.TxnOperator
	txnClient    client.TxnClient // Optional: if provided, used to create new transactions
	engine       engine.Engine
	accountID    uint32
	executor     executor.SQLExecutor
	createdTxnOp client.TxnOperator // Track if we created a new txn (to commit at end)
}

// SetTxnOp sets the transaction operator (called after StartTxn)
// This clears the createdTxnOp flag since the txn is now managed externally
func (h *UpstreamSQLHelper) SetTxnOp(txnOp client.TxnOperator) {
	h.txnOp = txnOp
	h.createdTxnOp = nil // Clear created flag since txn is now managed externally
}

// NewUpstreamSQLHelper creates a new UpstreamSQLHelper
// txnClient is optional - if provided, it will be used to create new transactions when needed
func NewUpstreamSQLHelper(
	txnOp client.TxnOperator,
	engine engine.Engine,
	accountID uint32,
	executor executor.SQLExecutor,
	txnClient client.TxnClient, // Optional: if nil, will try to get from engine
) *UpstreamSQLHelper {
	return &UpstreamSQLHelper{
		txnOp:     txnOp,
		txnClient: txnClient,
		engine:    engine,
		accountID: accountID,
		executor:  executor,
	}
}

// Internal command prefixes
const (
	cmdGetSnapshotTsPrefix        = "__++__internal_get_snapshot_ts"
	cmdGetDatabasesPrefix         = "__++__internal_get_databases"
	cmdGetDdlPrefix               = "__++__internal_get_ddl"
	cmdGetMoIndexesPrefix         = "__++__internal_get_mo_indexes"
	cmdObjectListPrefix           = "__++__internal_object_list"
	cmdGetObjectPrefix            = "__++__internal_get_object"
	cmdCheckSnapshotFlushedPrefix = "__++__internal_check_snapshot_flushed"
)

// placeholderToEmpty converts the "-" placeholder back to empty string
// This is the inverse of escapeOrPlaceholder in sql_builder.go
func placeholderToEmpty(s string) string {
	if s == "-" {
		return ""
	}
	return s
}

// HandleSpecialSQL checks if the SQL is a special statement and handles it directly
// Returns (handled, result, error) where handled indicates if the statement was handled
// If a new transaction was created (because there was no txn initially), it will be committed on success or rolled back on error
func (h *UpstreamSQLHelper) HandleSpecialSQL(
	ctx context.Context,
	query string,
) (bool, *publication.Result, error) {
	// Check for internal commands BEFORE parsing (they are not valid SQL)
	lowerQuery := strings.ToLower(query)
	if strings.HasPrefix(lowerQuery, cmdGetSnapshotTsPrefix) {
		return h.handleGetSnapshotTsCmd(ctx, query)
	}
	if strings.HasPrefix(lowerQuery, cmdGetDatabasesPrefix) {
		return h.handleGetDatabasesCmd(ctx, query)
	}
	if strings.HasPrefix(lowerQuery, cmdGetDdlPrefix) {
		return h.handleGetDdlCmd(ctx, query)
	}
	if strings.HasPrefix(lowerQuery, cmdGetMoIndexesPrefix) {
		return h.handleGetMoIndexesCmd(ctx, query)
	}
	if strings.HasPrefix(lowerQuery, cmdObjectListPrefix) {
		return h.handleObjectListCmd(ctx, query)
	}
	if strings.HasPrefix(lowerQuery, cmdGetObjectPrefix) {
		return h.handleGetObjectCmd(ctx, query)
	}
	if strings.HasPrefix(lowerQuery, cmdCheckSnapshotFlushedPrefix) {
		return h.handleCheckSnapshotFlushedCmd(ctx, query)
	}

	// Parse SQL to check if it's a special statement
	stmts, err := parsers.Parse(ctx, dialect.MYSQL, query, 0)
	if err != nil || len(stmts) == 0 {
		return false, nil, nil // Not a special statement or parse error, let normal executor handle
	}

	stmt := stmts[0]
	defer func() {
		for _, s := range stmts {
			s.Free()
		}
	}()

	// Track if we need to commit/rollback the transaction at the end
	// Save the original txnOp to check if we created a new one
	originalTxnOp := h.txnOp
	var result *publication.Result
	var handleErr error

	// Use defer to ensure transaction is always committed/rolled back if we created it
	defer func() {
		// If we created a new transaction (originalTxnOp was nil but now we have one), commit/rollback it
		if originalTxnOp == nil && h.createdTxnOp != nil {
			createdTxn := h.createdTxnOp
			if handleErr != nil {
				// Rollback on error
				if rollbackErr := createdTxn.Rollback(ctx); rollbackErr != nil {
					logutil.Error("UpstreamSQLHelper: failed to rollback created transaction",
						zap.Error(rollbackErr),
					)
				}
			} else {
				// Commit on success
				if commitErr := createdTxn.Commit(ctx); commitErr != nil {
					logutil.Error("UpstreamSQLHelper: failed to commit created transaction",
						zap.Error(commitErr),
					)
					// Update handleErr so caller knows about commit failure
					handleErr = commitErr
				}
			}
			// Clear the created txn references only if txnOp still points to the created one
			if h.txnOp == createdTxn {
				h.txnOp = nil
			}
			h.createdTxnOp = nil
		}
	}()

	// Route special statements through frontend layer processing
	switch s := stmt.(type) {
	case *tree.CreateSnapShot:
		logutil.Info("UpstreamSQLHelper: routing CREATE SNAPSHOT to frontend",
			zap.String("sql", query),
		)
		handleErr = h.handleCreateSnapshotDirectly(ctx, s)
		if handleErr != nil {
			return true, nil, handleErr
		}
		// CREATE SNAPSHOT doesn't return rows
		// Create empty result using internal executor's convertExecutorResult equivalent
		emptyResult := executor.Result{}
		result = h.convertExecutorResult(emptyResult)

	case *tree.DropSnapShot:
		logutil.Info("UpstreamSQLHelper: routing DROP SNAPSHOT to frontend",
			zap.String("sql", query),
		)
		handleErr = h.handleDropSnapshotDirectly(ctx, s)
		if handleErr != nil {
			return true, nil, handleErr
		}
		// DROP SNAPSHOT doesn't return rows
		emptyResult := executor.Result{}
		result = h.convertExecutorResult(emptyResult)

	case *tree.ObjectList:
		logutil.Info("UpstreamSQLHelper: routing OBJECTLIST to frontend",
			zap.String("sql", query),
		)
		result, handleErr = h.handleObjectListDirectly(ctx, s)
		if handleErr != nil {
			return true, nil, handleErr
		}

	case *tree.GetObject:
		logutil.Info("UpstreamSQLHelper: routing GET OBJECT to frontend",
			zap.String("sql", query),
		)
		result, handleErr = h.handleGetObjectDirectly(ctx, s)
		if handleErr != nil {
			return true, nil, handleErr
		}

	case *tree.CheckSnapshotFlushed:
		logutil.Info("UpstreamSQLHelper: routing CHECK SNAPSHOT FLUSHED to frontend",
			zap.String("sql", query),
		)
		result, handleErr = h.handleCheckSnapshotFlushedDirectly(ctx, s)
		if handleErr != nil {
			return true, nil, handleErr
		}

	default:
		return false, nil, nil // Not a special statement
	}

	return true, result, handleErr
}

// ensureTxnOp ensures txnOp exists, creating one if necessary
// Returns the txnOp and a boolean indicating if a new transaction was created
func (h *UpstreamSQLHelper) ensureTxnOp(ctx context.Context) (client.TxnOperator, error) {
	if h.txnOp != nil {
		return h.txnOp, nil
	}

	if h.engine == nil {
		return nil, moerr.NewInternalError(ctx, "engine is required to create transaction")
	}

	// Get txnClient - prefer the one passed in, otherwise try to get from engine
	txnClient := h.txnClient
	if txnClient == nil {
		var err error
		txnClient, err = h.getTxnClientFromEngine()
		if err != nil {
			return nil, err
		}
	}

	// Get latest logtail applied time as snapshot timestamp
	snapshotTS := h.engine.LatestLogtailAppliedTime()

	// Create new txn operator
	txnOp, err := txnClient.New(ctx, snapshotTS)
	if err != nil {
		return nil, err
	}

	// Initialize engine with the new txn
	if err := h.engine.New(ctx, txnOp); err != nil {
		return nil, err
	}

	// Store the created txnOp and mark it as created by us
	h.txnOp = txnOp
	h.createdTxnOp = txnOp
	return txnOp, nil
}

// getTxnClientFromEngine gets txnClient from engine using reflection (fallback when txnClient is not provided)
func (h *UpstreamSQLHelper) getTxnClientFromEngine() (client.TxnClient, error) {
	// Get disttae.Engine to access txnClient
	var de *disttae.Engine
	var ok bool
	if de, ok = h.engine.(*disttae.Engine); !ok {
		var entireEngine *engine.EntireEngine
		if entireEngine, ok = h.engine.(*engine.EntireEngine); ok {
			de, ok = entireEngine.Engine.(*disttae.Engine)
		}
		if !ok {
			return nil, moerr.NewInternalError(context.Background(), "failed to get disttae engine to create transaction")
		}
	}

	// Use reflection to access private cli field
	engineValue := reflect.ValueOf(de).Elem()
	cliField := engineValue.FieldByName("cli")
	if !cliField.IsValid() || cliField.IsNil() {
		return nil, moerr.NewInternalError(context.Background(), "txnClient is not available in engine")
	}

	return cliField.Interface().(client.TxnClient), nil
}

// handleCreateSnapshotDirectly handles CREATE SNAPSHOT by directly calling frontend logic
func (h *UpstreamSQLHelper) handleCreateSnapshotDirectly(
	ctx context.Context,
	stmt *tree.CreateSnapShot,
) error {
	if h.engine == nil {
		return moerr.NewInternalError(ctx, "engine is required for CREATE SNAPSHOT")
	}

	txnOp, err := h.ensureTxnOp(ctx)
	if err != nil {
		return err
	}

	snapshotName := string(stmt.Name)
	snapshotLevel := stmt.Object.SLevel.Level

	// Check if snapshot already exists
	checkSQL := fmt.Sprintf(`select snapshot_id from mo_catalog.mo_snapshots where sname = "%s" order by snapshot_id;`, snapshotName)
	opts := executor.Options{}.WithDisableIncrStatement().WithTxn(txnOp)
	checkResult, err := h.executor.Exec(ctx, checkSQL, opts)
	if err != nil {
		return err
	}
	defer checkResult.Close()

	var snapshotExists bool
	checkResult.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 {
			snapshotExists = true
		}
		return true
	})

	if snapshotExists {
		if !stmt.IfNotExists {
			return moerr.NewInternalErrorf(ctx, "snapshot %s already exists", snapshotName)
		}
		return nil
	}

	// Generate snapshot ID
	newUUID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	snapshotId := newUUID.String()

	// Increase transaction physical timestamp
	snapshotTS, err := h.tryToIncreaseTxnPhysicalTS(ctx)
	if err != nil {
		return err
	}

	// Get database name, table name and objId according to snapshot level
	var sql string
	var objId uint64
	var databaseName, tableName, accountName string

	switch snapshotLevel {
	case tree.SNAPSHOTLEVELCLUSTER:
		sql = fmt.Sprintf(`insert into mo_catalog.mo_snapshots(
			snapshot_id,
			sname,
			ts,
			level,
			account_name,
			database_name,
			table_name,
			obj_id ) values ('%s', '%s', %d, '%s', '%s', '%s', '%s', %d);`,
			snapshotId, snapshotName, snapshotTS, snapshotLevel.String(), "", "", "", uint64(math.MaxUint64))

	case tree.SNAPSHOTLEVELACCOUNT:
		accountName = string(stmt.Object.ObjName)
		// Use system account context to query mo_account table
		systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
		// If account name is empty, use current account ID
		if len(accountName) == 0 {
			objId = uint64(h.accountID)
			// Get account name from account ID
			getAccountNameSQL := fmt.Sprintf(`select account_name from mo_catalog.mo_account where account_id = %d;`, h.accountID)
			accountResult, err := h.executor.Exec(systemCtx, getAccountNameSQL, opts)
			if err != nil {
				return err
			}
			defer accountResult.Close()
			accountResult.ReadRows(func(rows int, cols []*vector.Vector) bool {
				if rows > 0 && len(cols) > 0 {
					accountName = cols[0].GetStringAt(0)
				}
				return true
			})
		} else {
			// Check account exists and get account ID
			getAccountSQL := fmt.Sprintf(`select account_id from mo_catalog.mo_account where account_name = "%s" order by account_id;`, accountName)
			accountResult, err := h.executor.Exec(systemCtx, getAccountSQL, opts)
			if err != nil {
				return err
			}
			defer accountResult.Close()

			var found bool
			accountResult.ReadRows(func(rows int, cols []*vector.Vector) bool {
				if rows > 0 && len(cols) > 0 {
					objId = vector.GetFixedAtWithTypeCheck[uint64](cols[0], 0)
					found = true
				}
				return true
			})
			if !found {
				return moerr.NewInternalErrorf(ctx, "account %s does not exist", accountName)
			}
		}

		sql = fmt.Sprintf(`insert into mo_catalog.mo_snapshots(
			snapshot_id,
			sname,
			ts,
			level,
			account_name,
			database_name,
			table_name,
			obj_id ) values ('%s', '%s', %d, '%s', '%s', '%s', '%s', %d);`,
			snapshotId, snapshotName, snapshotTS, snapshotLevel.String(), accountName, "", "", objId)

	case tree.SNAPSHOTLEVELDATABASE:
		databaseName = string(stmt.Object.ObjName)
		if len(databaseName) == 0 {
			return moerr.NewInternalError(ctx, "database name is required for database level snapshot")
		}

		// Check if it's a system database that cannot be snapshotted
		skipDbs := []string{"mysql", "system", "system_metrics", "mo_task", "mo_debug", "information_schema", "mo_catalog"}
		if slices.Contains(skipDbs, databaseName) {
			return moerr.NewInternalErrorf(ctx, "can not create snapshot for system database %s", databaseName)
		}

		// Get database ID
		getDatabaseSQL := fmt.Sprintf(`select dat_id from mo_catalog.mo_database where datname = "%s";`, databaseName)
		dbResult, err := h.executor.Exec(ctx, getDatabaseSQL, opts)
		if err != nil {
			return err
		}
		defer dbResult.Close()

		var found bool
		dbResult.ReadRows(func(rows int, cols []*vector.Vector) bool {
			if rows > 0 && len(cols) > 0 {
				objId = vector.GetFixedAtWithTypeCheck[uint64](cols[0], 0)
				found = true
			}
			return true
		})
		if !found {
			return moerr.NewInternalErrorf(ctx, "database %s does not exist", databaseName)
		}

		// Temporarily set account_name to empty string for database level snapshot
		sql = fmt.Sprintf(`insert into mo_catalog.mo_snapshots(
			snapshot_id,
			sname,
			ts,
			level,
			account_name,
			database_name,
			table_name,
			obj_id ) values ('%s', '%s', %d, '%s', '%s', '%s', '%s', %d);`,
			snapshotId, snapshotName, snapshotTS, snapshotLevel.String(), "", databaseName, "", objId)

	case tree.SNAPSHOTLEVELTABLE:
		objectName := string(stmt.Object.ObjName)
		objects := strings.Split(objectName, ".")
		if len(objects) != 2 {
			return moerr.NewInternalErrorf(ctx, "invalid table name %s", objectName)
		}
		databaseName = objects[0]
		tableName = objects[1]

		// Get table ID
		getTableSQL := fmt.Sprintf(`select rel_id from mo_catalog.mo_tables where account_id = %d and relname = '%s' and reldatabase = '%s';`,
			h.accountID, tableName, databaseName)
		tableResult, err := h.executor.Exec(ctx, getTableSQL, opts)
		if err != nil {
			return err
		}
		defer tableResult.Close()

		var found bool
		tableResult.ReadRows(func(rows int, cols []*vector.Vector) bool {
			if rows > 0 && len(cols) > 0 {
				objId = vector.GetFixedAtWithTypeCheck[uint64](cols[0], 0)
				found = true
			}
			return true
		})
		if !found {
			return moerr.NewInternalErrorf(ctx, "table %s.%s does not exist", databaseName, tableName)
		}

		sql = fmt.Sprintf(`insert into mo_catalog.mo_snapshots(
			snapshot_id,
			sname,
			ts,
			level,
			account_name,
			database_name,
			table_name,
			obj_id ) values ('%s', '%s', %d, '%s', '%s', '%s', '%s', %d);`,
			snapshotId, snapshotName, snapshotTS, snapshotLevel.String(), "", databaseName, tableName, objId)
	default:
		return moerr.NewNotSupportedNoCtxf("snapshot level %s not supported in internal executor", snapshotLevel.String())
	}

	// Execute INSERT statement
	logutil.Info("UpstreamSQLHelper: executing CREATE SNAPSHOT SQL", zap.String("sql", sql))
	_, err = h.executor.Exec(ctx, sql, opts)
	return err
}

// handleDropSnapshotDirectly handles DROP SNAPSHOT by directly calling frontend logic
func (h *UpstreamSQLHelper) handleDropSnapshotDirectly(
	ctx context.Context,
	stmt *tree.DropSnapShot,
) error {
	txnOp, err := h.ensureTxnOp(ctx)
	if err != nil {
		return err
	}

	snapshotName := string(stmt.Name)
	sql := fmt.Sprintf(`delete from mo_catalog.mo_snapshots where sname = '%s' order by snapshot_id;`, snapshotName)

	opts := executor.Options{}.WithDisableIncrStatement().WithTxn(txnOp)
	logutil.Info("UpstreamSQLHelper: executing DROP SNAPSHOT SQL", zap.String("sql", sql))
	_, err = h.executor.Exec(ctx, sql, opts)
	return err
}

// handleObjectListDirectly handles OBJECTLIST by directly calling frontend logic
func (h *UpstreamSQLHelper) handleObjectListDirectly(
	ctx context.Context,
	stmt *tree.ObjectList,
) (*publication.Result, error) {
	if h.engine == nil {
		return nil, moerr.NewInternalError(ctx, "engine is required for OBJECTLIST")
	}

	txnOp, err := h.ensureTxnOp(ctx)
	if err != nil {
		return nil, err
	}

	// Get database name and table name
	dbname := string(stmt.Database)
	tablename := string(stmt.Table)

	// Get mpool
	mp := mpool.MustNewZero()

	// Resolve snapshot using executor
	resolveSnapshot := func(ctx context.Context, snapshotName string) (*timestamp.Timestamp, error) {
		return frontend.ResolveSnapshotWithSnapshotNameWithoutSession(ctx, snapshotName, h.executor, txnOp)
	}

	// Get current timestamp from txn
	getCurrentTS := func() types.TS {
		return types.TimestampToTS(txnOp.SnapshotTS())
	}

	// Process object list using core function
	resultBatch, err := frontend.ProcessObjectList(ctx, stmt, h.engine, txnOp, mp, resolveSnapshot, getCurrentTS, dbname, tablename)
	if err != nil {
		return nil, err
	}

	// Convert batch to result
	return h.convertExecutorResult(executor.Result{
		Batches: []*batch.Batch{resultBatch},
		Mp:      mp,
	}), nil
}

// handleGetObjectDirectly handles GET OBJECT by directly calling frontend logic
func (h *UpstreamSQLHelper) handleGetObjectDirectly(
	ctx context.Context,
	stmt *tree.GetObject,
) (*publication.Result, error) {
	if h.engine == nil {
		return nil, moerr.NewInternalError(ctx, "engine is required for GET OBJECT")
	}

	objectName := stmt.ObjectName.String()
	chunkIndex := stmt.ChunkIndex

	// Get fileservice from engine
	fs, err := h.getFileserviceFromEngine()
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to get fileservice: %v", err)
	}

	// Get file size
	dirEntry, err := fs.StatFile(ctx, objectName)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to stat file: %v", err)
	}
	fileSize := dirEntry.Size

	// Calculate total chunks
	const chunkSize = 1 * 1024 // 1KB (matching frontend/get_object.go)
	var totalChunks int64
	if fileSize <= chunkSize {
		totalChunks = 1
	} else {
		totalChunks = (fileSize + chunkSize - 1) / chunkSize // 向上取整
	}

	// Validate chunk index
	if chunkIndex < -1 {
		return nil, moerr.NewInvalidInput(ctx, "invalid chunk_index: must be >= -1")
	}
	if chunkIndex > totalChunks {
		return nil, moerr.NewInvalidInput(ctx, fmt.Sprintf("invalid chunk_index: %d, file has only %d chunks", chunkIndex, totalChunks))
	}

	var data []byte
	var isComplete bool

	if chunkIndex == 0 {
		// Metadata only request
		data = nil
		isComplete = false
	} else {
		// Data chunk request
		offset := (chunkIndex - 1) * chunkSize
		size := int64(chunkSize)
		if chunkIndex == totalChunks {
			// Last chunk may be smaller
			size = fileSize - offset
		}

		// Read object chunk from engine using frontend function
		content, err := frontend.ReadObjectFromEngine(ctx, h.engine, objectName, offset, size)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to read object chunk: %v", err)
		}
		data = content
		isComplete = (chunkIndex == totalChunks)
	}

	// Create a batch with 5 columns: data, total_size, chunk_index, total_chunks, is_complete
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"data", "total_size", "chunk_index", "total_chunks", "is_complete"})

	// Column 0: data (BLOB)
	bat.Vecs[0] = vector.NewVec(types.T_blob.ToType())
	if data != nil {
		err = vector.AppendBytes(bat.Vecs[0], data, false, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, err
		}
	} else {
		err = vector.AppendBytes(bat.Vecs[0], nil, true, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, err
		}
	}

	// Column 1: total_size (LONGLONG)
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixed(bat.Vecs[1], fileSize, false, mp)
	if err != nil {
		bat.Clean(mp)
		return nil, err
	}

	// Column 2: chunk_index (LONG) - use int64 to match getObjectFromUpstream expectations
	bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixed(bat.Vecs[2], chunkIndex, false, mp)
	if err != nil {
		bat.Clean(mp)
		return nil, err
	}

	// Column 3: total_chunks (LONG) - use int64 to match getObjectFromUpstream expectations
	bat.Vecs[3] = vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixed(bat.Vecs[3], totalChunks, false, mp)
	if err != nil {
		bat.Clean(mp)
		return nil, err
	}

	// Column 4: is_complete (bool) - use bool to match getObjectFromUpstream expectations
	bat.Vecs[4] = vector.NewVec(types.T_bool.ToType())
	err = vector.AppendFixed(bat.Vecs[4], isComplete, false, mp)
	if err != nil {
		bat.Clean(mp)
		return nil, err
	}

	bat.SetRowCount(1)

	return h.convertExecutorResult(executor.Result{
		Batches: []*batch.Batch{bat},
		Mp:      mp,
	}), nil
}

// handleCheckSnapshotFlushedDirectly handles CHECK SNAPSHOT FLUSHED by directly calling frontend logic
func (h *UpstreamSQLHelper) handleCheckSnapshotFlushedDirectly(
	ctx context.Context,
	stmt *tree.CheckSnapshotFlushed,
) (*publication.Result, error) {
	if h.engine == nil {
		return nil, moerr.NewInternalError(ctx, "engine is required for CHECK SNAPSHOT FLUSHED")
	}

	txnOp, err := h.ensureTxnOp(ctx)
	if err != nil {
		return nil, err
	}

	// Get snapshot info by name
	snapshotInfo, err := frontend.GetSnapshotInfoByName(ctx, h.executor, txnOp, string(stmt.Name))
	if err != nil {
		// If snapshot not found, return false
		mp := mpool.MustNewZero()
		bat := batch.New([]string{"result"})
		bat.Vecs[0] = vector.NewVec(types.T_bool.ToType())
		err = vector.AppendFixed(bat.Vecs[0], false, false, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, err
		}
		bat.SetRowCount(1)
		return h.convertExecutorResult(executor.Result{
			Batches: []*batch.Batch{bat},
			Mp:      mp,
		}), nil
	}

	if snapshotInfo == nil {
		return nil, moerr.NewInternalError(ctx, "snapshot not found")
	}

	// Get disttae.Engine from engine
	var de *disttae.Engine
	var ok bool
	if de, ok = h.engine.(*disttae.Engine); !ok {
		var entireEngine *engine.EntireEngine
		if entireEngine, ok = h.engine.(*engine.EntireEngine); ok {
			de, ok = entireEngine.Engine.(*disttae.Engine)
		}
		if !ok {
			return nil, moerr.NewInternalError(ctx, "failed to get disttae engine")
		}
	}

	// Create txn with snapshot timestamp
	txn := txnOp.CloneSnapshotOp(timestamp.Timestamp{
		PhysicalTime: snapshotInfo.Ts,
		LogicalTime:  0,
	})

	// Call frontend.CheckSnapshotFlushed with all required parameters
	result, err := frontend.CheckSnapshotFlushed(ctx, txn, snapshotInfo.Ts, de, snapshotInfo.Level, snapshotInfo.DatabaseName, snapshotInfo.TableName)
	if err != nil {
		return nil, err
	}

	// Create a batch with one column containing the result
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"result"})
	bat.Vecs[0] = vector.NewVec(types.T_bool.ToType())
	err = vector.AppendFixed(bat.Vecs[0], result, false, mp)
	if err != nil {
		bat.Clean(mp)
		return nil, err
	}
	bat.SetRowCount(1)

	return h.convertExecutorResult(executor.Result{
		Batches: []*batch.Batch{bat},
		Mp:      mp,
	}), nil
}

// getFileserviceFromEngine gets fileservice from engine (similar to ReadObjectFromEngine)
func (h *UpstreamSQLHelper) getFileserviceFromEngine() (fileservice.FileService, error) {
	if h.engine == nil {
		return nil, moerr.NewInternalError(context.Background(), "engine is not available")
	}

	var de *disttae.Engine
	var ok bool
	if de, ok = h.engine.(*disttae.Engine); !ok {
		var entireEngine *engine.EntireEngine
		if entireEngine, ok = h.engine.(*engine.EntireEngine); ok {
			de, ok = entireEngine.Engine.(*disttae.Engine)
		}
		if !ok {
			return nil, moerr.NewInternalError(context.Background(), "failed to get disttae engine")
		}
	}

	fs := de.FS()
	if fs == nil {
		return nil, moerr.NewInternalError(context.Background(), "fileservice is not available")
	}

	return fs, nil
}

// tryToIncreaseTxnPhysicalTS increases the transaction physical timestamp
func (h *UpstreamSQLHelper) tryToIncreaseTxnPhysicalTS(ctx context.Context) (int64, error) {
	txnOp, err := h.ensureTxnOp(ctx)
	if err != nil {
		return 0, err
	}

	curTxnPhysicalTS := txnOp.SnapshotTS().PhysicalTime

	if ctx.Value(defines.TenantIDKey{}) == nil {
		return curTxnPhysicalTS, nil
	}

	// A slight increase added to the physical to make sure
	// the updated ts is greater than the old txn timestamp (physical + logic)
	curTxnPhysicalTS += int64(time.Microsecond)
	err = txnOp.UpdateSnapshot(ctx, timestamp.Timestamp{
		PhysicalTime: curTxnPhysicalTS,
	})
	if err != nil {
		return 0, err
	}

	return txnOp.SnapshotTS().PhysicalTime, nil
}

// handleGetDdlCmd handles the internal __++__internal_get_ddl command
// Format: __++__internal_get_ddl <snapshotName> <subscriptionAccountName> <publicationName> <level> <dbName> <tableName>
func (h *UpstreamSQLHelper) handleGetDdlCmd(
	ctx context.Context,
	query string,
) (bool, *publication.Result, error) {
	// Parse the command parameters
	params := strings.TrimSpace(query[len(cmdGetDdlPrefix):])
	parts := strings.Fields(params)
	if len(parts) != 6 {
		return true, nil, moerr.NewInternalError(ctx, "invalid get_ddl command format, expected: __++__internal_get_ddl <snapshotName> <subscriptionAccountName> <publicationName> <level> <dbName> <tableName>")
	}
	snapshotName := parts[0]
	// subscriptionAccountName (parts[1]) and publicationName (parts[2]) are not used in test helper since we don't check publication permissions
	level := parts[3]
	databaseName := placeholderToEmpty(parts[4])
	tableName := placeholderToEmpty(parts[5])

	logutil.Info("UpstreamSQLHelper: handling internal get_ddl command",
		zap.String("snapshotName", snapshotName),
		zap.String("level", level),
		zap.String("databaseName", databaseName),
		zap.String("tableName", tableName),
	)

	if h.engine == nil {
		return true, nil, moerr.NewInternalError(ctx, "engine is required for internal get_ddl")
	}

	// Ensure we have a transaction
	txnOp, err := h.ensureTxnOp(ctx)
	if err != nil {
		return true, nil, err
	}

	// Step 1: Get snapshot info to get the timestamp
	snapshotInfo, err := frontend.GetSnapshotInfoByName(ctx, h.executor, txnOp, snapshotName)
	if err != nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "failed to get snapshot info: %v", err)
	}
	if snapshotInfo == nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "snapshot %s does not exist", snapshotName)
	}

	// Step 2: Get snapshot timestamp
	snapshotTs := snapshotInfo.Ts

	// Step 4: Compute DDL batch
	mp := mpool.MustNewZero()
	resultBatch, err := frontend.ComputeDdlBatchWithSnapshot(ctx, databaseName, tableName, h.engine, mp, txnOp, snapshotTs)
	if err != nil {
		return true, nil, err
	}

	// Convert batch to result
	return true, h.convertExecutorResult(executor.Result{
		Batches: []*batch.Batch{resultBatch},
		Mp:      mp,
	}), nil
}

// convertExecutorResult converts executor.Result to publication.Result
func (h *UpstreamSQLHelper) convertExecutorResult(execResult executor.Result) *publication.Result {
	return publication.NewResultFromExecutorResult(execResult)
}

// handleGetSnapshotTsCmd handles the internal __++__internal_get_snapshot_ts command
// Format: __++__internal_get_snapshot_ts <snapshotName> <accountName> <publicationName>
func (h *UpstreamSQLHelper) handleGetSnapshotTsCmd(
	ctx context.Context,
	query string,
) (bool, *publication.Result, error) {
	// Parse the command parameters
	params := strings.TrimSpace(query[len(cmdGetSnapshotTsPrefix):])
	parts := strings.Fields(params)
	if len(parts) != 3 {
		return true, nil, moerr.NewInternalError(ctx, "invalid get_snapshot_ts command format, expected: __++__internal_get_snapshot_ts <snapshotName> <accountName> <publicationName>")
	}
	snapshotName := parts[0]
	// accountName and publicationName are not used in test helper since we don't check publication permissions

	logutil.Info("UpstreamSQLHelper: handling internal get_snapshot_ts command",
		zap.String("snapshotName", snapshotName),
	)

	// Ensure we have a transaction
	txnOp, err := h.ensureTxnOp(ctx)
	if err != nil {
		return true, nil, err
	}

	// Query mo_snapshots for the timestamp
	sql := fmt.Sprintf("SELECT ts FROM mo_catalog.mo_snapshots WHERE sname = '%s'", snapshotName)

	// Create context with account ID
	queryCtx := context.WithValue(ctx, defines.TenantIDKey{}, h.accountID)

	// Execute using internal executor
	execResult, err := h.executor.Exec(queryCtx, sql, executor.Options{}.WithTxn(txnOp))
	if err != nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "failed to query snapshot ts: %v", err)
	}

	return true, h.convertExecutorResult(execResult), nil
}

// handleGetDatabasesCmd handles the internal __++__internal_get_databases command
// Format: __++__internal_get_databases <snapshotName> <accountName> <publicationName> <level> <dbName> <tableName>
func (h *UpstreamSQLHelper) handleGetDatabasesCmd(
	ctx context.Context,
	query string,
) (bool, *publication.Result, error) {
	// Parse the command parameters
	params := strings.TrimSpace(query[len(cmdGetDatabasesPrefix):])
	parts := strings.Fields(params)
	if len(parts) != 6 {
		return true, nil, moerr.NewInternalError(ctx, "invalid get_databases command format, expected: __++__internal_get_databases <snapshotName> <accountName> <publicationName> <level> <dbName> <tableName>")
	}
	snapshotName := parts[0]
	// accountName and publicationName are not used in test helper since we don't check publication permissions
	level := parts[3]
	dbName := placeholderToEmpty(parts[4])
	// tableName := placeholderToEmpty(parts[5]) // not used for database query

	logutil.Info("UpstreamSQLHelper: handling internal get_databases command",
		zap.String("snapshotName", snapshotName),
		zap.String("level", level),
		zap.String("dbName", dbName),
	)

	// If table level, return empty result (no databases to create/drop)
	if level == publication.SyncLevelTable {
		return true, h.convertExecutorResult(executor.Result{}), nil
	}

	// Ensure we have a transaction
	txnOp, err := h.ensureTxnOp(ctx)
	if err != nil {
		return true, nil, err
	}

	// Get snapshot info
	snapshotInfo, err := frontend.GetSnapshotInfoByName(ctx, h.executor, txnOp, snapshotName)
	if err != nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "failed to get snapshot info: %v", err)
	}
	snapshotTs := snapshotInfo.Ts

	// Create context with account ID
	queryCtx := context.WithValue(ctx, defines.TenantIDKey{}, h.accountID)

	var sql string
	if level == publication.SyncLevelDatabase {
		// Database level: only check/return the specific database if it exists
		sql = fmt.Sprintf("SELECT datname FROM mo_catalog.mo_database{MO_TS = %d} WHERE account_id = %d AND datname = '%s'",
			snapshotTs, h.accountID, dbName)
	} else {
		// Account level: return all databases for the account
		sql = fmt.Sprintf("SELECT datname FROM mo_catalog.mo_database{MO_TS = %d} WHERE account_id = %d", snapshotTs, h.accountID)
	}

	// Execute using internal executor
	execResult, err := h.executor.Exec(queryCtx, sql, executor.Options{}.WithTxn(txnOp))
	if err != nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "failed to query databases: %v", err)
	}

	return true, h.convertExecutorResult(execResult), nil
}

// handleGetMoIndexesCmd handles the internal __++__internal_get_mo_indexes command
// Format: __++__internal_get_mo_indexes <tableId> <subscriptionAccountName> <publicationName> <snapshotName>
func (h *UpstreamSQLHelper) handleGetMoIndexesCmd(
	ctx context.Context,
	query string,
) (bool, *publication.Result, error) {
	// Parse the command parameters
	params := strings.TrimSpace(query[len(cmdGetMoIndexesPrefix):])
	parts := strings.Fields(params)
	if len(parts) != 4 {
		return true, nil, moerr.NewInternalError(ctx, "invalid get_mo_indexes command format, expected: __++__internal_get_mo_indexes <tableId> <subscriptionAccountName> <publicationName> <snapshotName>")
	}

	// Parse tableId
	var tableId uint64
	_, err := fmt.Sscanf(parts[0], "%d", &tableId)
	if err != nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "invalid tableId format: %v", err)
	}
	snapshotName := parts[3]

	logutil.Info("UpstreamSQLHelper: handling internal get_mo_indexes command",
		zap.Uint64("tableId", tableId),
		zap.String("snapshotName", snapshotName),
	)

	// Ensure we have a transaction
	txnOp, err := h.ensureTxnOp(ctx)
	if err != nil {
		return true, nil, err
	}

	// Get snapshot info
	snapshotInfo, err := frontend.GetSnapshotInfoByName(ctx, h.executor, txnOp, snapshotName)
	if err != nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "failed to get snapshot info: %v", err)
	}
	snapshotTs := snapshotInfo.Ts

	// Query mo_indexes using the snapshot timestamp
	sql := fmt.Sprintf("SELECT table_id, name, algo_table_type, index_table_name FROM mo_catalog.mo_indexes{MO_TS = %d} WHERE table_id = %d", snapshotTs, tableId)

	// Create context with account ID
	queryCtx := context.WithValue(ctx, defines.TenantIDKey{}, h.accountID)

	// Execute using internal executor
	execResult, err := h.executor.Exec(queryCtx, sql, executor.Options{}.WithTxn(txnOp))
	if err != nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "failed to query mo_indexes: %v", err)
	}

	return true, h.convertExecutorResult(execResult), nil
}

// handleObjectListCmd handles the internal __++__internal_object_list command
// Format: __++__internal_object_list <snapshotName> <againstSnapshotName> <subscriptionAccountName> <publicationName>
func (h *UpstreamSQLHelper) handleObjectListCmd(
	ctx context.Context,
	query string,
) (bool, *publication.Result, error) {
	// Parse the command parameters
	params := strings.TrimSpace(query[len(cmdObjectListPrefix):])
	parts := strings.Fields(params)
	if len(parts) != 4 {
		return true, nil, moerr.NewInternalError(ctx, "invalid object_list command format, expected: __++__internal_object_list <snapshotName> <againstSnapshotName> <subscriptionAccountName> <publicationName>")
	}
	snapshotName := parts[0]
	againstSnapshotName := parts[1]

	logutil.Info("UpstreamSQLHelper: handling internal object_list command",
		zap.String("snapshotName", snapshotName),
		zap.String("againstSnapshotName", againstSnapshotName),
	)

	if h.engine == nil {
		return true, nil, moerr.NewInternalError(ctx, "engine is required for internal object_list")
	}

	// Ensure we have a transaction
	txnOp, err := h.ensureTxnOp(ctx)
	if err != nil {
		return true, nil, err
	}

	// Get snapshot info to get scope (database name, table name, level)
	snapshotInfo, err := frontend.GetSnapshotInfoByName(ctx, h.executor, txnOp, snapshotName)
	if err != nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "failed to get snapshot info: %v", err)
	}

	// Determine dbName and tableName based on snapshot level
	var dbname, tablename string
	switch snapshotInfo.Level {
	case "table":
		dbname = snapshotInfo.DatabaseName
		tablename = snapshotInfo.TableName
	case "database":
		dbname = snapshotInfo.DatabaseName
		tablename = ""
	case "account", "cluster":
		dbname = ""
		tablename = ""
	default:
		return true, nil, moerr.NewInternalErrorf(ctx, "unsupported snapshot level: %s", snapshotInfo.Level)
	}

	// Build tree.ObjectList statement for ProcessObjectList
	stmt := &tree.ObjectList{
		Database: tree.Identifier(dbname),
		Table:    tree.Identifier(tablename),
		Snapshot: tree.Identifier(snapshotName),
	}
	if againstSnapshotName != "" && againstSnapshotName != "_" {
		againstName := tree.Identifier(againstSnapshotName)
		stmt.AgainstSnapshot = &againstName
	}

	// Get mpool
	mp := mpool.MustNewZero()

	// Resolve snapshot using executor
	resolveSnapshot := func(ctx context.Context, snapshotName string) (*timestamp.Timestamp, error) {
		return frontend.ResolveSnapshotWithSnapshotNameWithoutSession(ctx, snapshotName, h.executor, txnOp)
	}

	// Get current timestamp from txn
	getCurrentTS := func() types.TS {
		return types.TimestampToTS(txnOp.SnapshotTS())
	}

	// Process object list using core function
	resultBatch, err := frontend.ProcessObjectList(ctx, stmt, h.engine, txnOp, mp, resolveSnapshot, getCurrentTS, dbname, tablename)
	if err != nil {
		return true, nil, err
	}

	// Convert batch to result
	return true, h.convertExecutorResult(executor.Result{
		Batches: []*batch.Batch{resultBatch},
		Mp:      mp,
	}), nil
}

// handleGetObjectCmd handles the internal __++__internal_get_object command
// Format: __++__internal_get_object <subscriptionAccountName> <publicationName> <objectName> <chunkIndex>
func (h *UpstreamSQLHelper) handleGetObjectCmd(
	ctx context.Context,
	query string,
) (bool, *publication.Result, error) {
	// Parse the command parameters
	params := strings.TrimSpace(query[len(cmdGetObjectPrefix):])
	parts := strings.Fields(params)
	if len(parts) != 4 {
		return true, nil, moerr.NewInternalError(ctx, "invalid get_object command format, expected: __++__internal_get_object <subscriptionAccountName> <publicationName> <objectName> <chunkIndex>")
	}
	objectName := parts[2]
	var chunkIndex int64
	_, err := fmt.Sscanf(parts[3], "%d", &chunkIndex)
	if err != nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "invalid chunkIndex format: %v", err)
	}

	logutil.Info("UpstreamSQLHelper: handling internal get_object command",
		zap.String("objectName", objectName),
		zap.Int64("chunkIndex", chunkIndex),
	)

	if h.engine == nil {
		return true, nil, moerr.NewInternalError(ctx, "engine is required for internal get_object")
	}

	// Get fileservice from engine
	fs, err := h.getFileserviceFromEngine()
	if err != nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "failed to get fileservice: %v", err)
	}

	// Get file size
	dirEntry, err := fs.StatFile(ctx, objectName)
	if err != nil {
		return true, nil, moerr.NewInternalErrorf(ctx, "failed to stat file: %v", err)
	}
	fileSize := dirEntry.Size

	// Calculate total chunks (matching frontend/get_object.go chunk size)
	const chunkSize = publication.GetChunkSize // 100MB
	var totalChunks int64
	if fileSize <= chunkSize {
		totalChunks = 1
	} else {
		totalChunks = (fileSize + chunkSize - 1) / chunkSize
	}

	// Validate chunk index
	if chunkIndex < 0 {
		return true, nil, moerr.NewInvalidInput(ctx, "invalid chunk_index: must be >= 0")
	}
	if chunkIndex > totalChunks {
		return true, nil, moerr.NewInvalidInput(ctx, fmt.Sprintf("invalid chunk_index: %d, file has only %d data chunks (chunk 0 is metadata)", chunkIndex, totalChunks))
	}

	var data []byte
	var isComplete bool

	if chunkIndex == 0 {
		// Metadata only request
		data = nil
		isComplete = false
	} else {
		// Data chunk request
		offset := (chunkIndex - 1) * chunkSize
		size := int64(chunkSize)
		if chunkIndex == totalChunks {
			// Last chunk may be smaller
			size = fileSize - offset
		}

		// Read object chunk from engine using frontend function
		content, err := frontend.ReadObjectFromEngine(ctx, h.engine, objectName, offset, size)
		if err != nil {
			return true, nil, moerr.NewInternalErrorf(ctx, "failed to read object chunk: %v", err)
		}
		data = content
		isComplete = (chunkIndex == totalChunks)
	}

	// Create a batch with 5 columns: data, total_size, chunk_index, total_chunks, is_complete
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"data", "total_size", "chunk_index", "total_chunks", "is_complete"})

	// Column 0: data (BLOB)
	bat.Vecs[0] = vector.NewVec(types.T_blob.ToType())
	if data != nil {
		err = vector.AppendBytes(bat.Vecs[0], data, false, mp)
		if err != nil {
			bat.Clean(mp)
			return true, nil, err
		}
	} else {
		err = vector.AppendBytes(bat.Vecs[0], nil, true, mp)
		if err != nil {
			bat.Clean(mp)
			return true, nil, err
		}
	}

	// Column 1: total_size (LONGLONG)
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixed(bat.Vecs[1], fileSize, false, mp)
	if err != nil {
		bat.Clean(mp)
		return true, nil, err
	}

	// Column 2: chunk_index (LONG)
	bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixed(bat.Vecs[2], chunkIndex, false, mp)
	if err != nil {
		bat.Clean(mp)
		return true, nil, err
	}

	// Column 3: total_chunks (LONG)
	bat.Vecs[3] = vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixed(bat.Vecs[3], totalChunks, false, mp)
	if err != nil {
		bat.Clean(mp)
		return true, nil, err
	}

	// Column 4: is_complete (bool)
	bat.Vecs[4] = vector.NewVec(types.T_bool.ToType())
	err = vector.AppendFixed(bat.Vecs[4], isComplete, false, mp)
	if err != nil {
		bat.Clean(mp)
		return true, nil, err
	}

	bat.SetRowCount(1)

	return true, h.convertExecutorResult(executor.Result{
		Batches: []*batch.Batch{bat},
		Mp:      mp,
	}), nil
}

// handleCheckSnapshotFlushedCmd handles the internal __++__internal_check_snapshot_flushed command
// Format: __++__internal_check_snapshot_flushed <snapshotName> <subscriptionAccountName> <publicationName>
func (h *UpstreamSQLHelper) handleCheckSnapshotFlushedCmd(
	ctx context.Context,
	query string,
) (bool, *publication.Result, error) {
	// Parse the command parameters
	params := strings.TrimSpace(query[len(cmdCheckSnapshotFlushedPrefix):])
	parts := strings.Fields(params)
	if len(parts) != 3 {
		return true, nil, moerr.NewInternalError(ctx, "invalid check_snapshot_flushed command format, expected: __++__internal_check_snapshot_flushed <snapshotName> <subscriptionAccountName> <publicationName>")
	}
	snapshotName := parts[0]

	logutil.Info("UpstreamSQLHelper: handling internal check_snapshot_flushed command",
		zap.String("snapshotName", snapshotName),
	)

	if h.engine == nil {
		return true, nil, moerr.NewInternalError(ctx, "engine is required for internal check_snapshot_flushed")
	}

	// Ensure we have a transaction
	txnOp, err := h.ensureTxnOp(ctx)
	if err != nil {
		return true, nil, err
	}

	// Get snapshot info by name
	snapshotInfo, err := frontend.GetSnapshotInfoByName(ctx, h.executor, txnOp, snapshotName)
	if err != nil {
		// If snapshot not found, return false
		mp := mpool.MustNewZero()
		bat := batch.New([]string{"result"})
		bat.Vecs[0] = vector.NewVec(types.T_bool.ToType())
		err = vector.AppendFixed(bat.Vecs[0], false, false, mp)
		if err != nil {
			bat.Clean(mp)
			return true, nil, err
		}
		bat.SetRowCount(1)
		return true, h.convertExecutorResult(executor.Result{
			Batches: []*batch.Batch{bat},
			Mp:      mp,
		}), nil
	}

	if snapshotInfo == nil {
		return true, nil, moerr.NewInternalError(ctx, "snapshot not found")
	}

	// Get disttae.Engine from engine
	var de *disttae.Engine
	var ok bool
	if de, ok = h.engine.(*disttae.Engine); !ok {
		var entireEngine *engine.EntireEngine
		if entireEngine, ok = h.engine.(*engine.EntireEngine); ok {
			de, ok = entireEngine.Engine.(*disttae.Engine)
		}
		if !ok {
			return true, nil, moerr.NewInternalError(ctx, "failed to get disttae engine")
		}
	}

	// Create txn with snapshot timestamp
	txn := txnOp.CloneSnapshotOp(timestamp.Timestamp{
		PhysicalTime: snapshotInfo.Ts,
		LogicalTime:  0,
	})

	// Call frontend.CheckSnapshotFlushed with all required parameters
	result, err := frontend.CheckSnapshotFlushed(ctx, txn, snapshotInfo.Ts, de, snapshotInfo.Level, snapshotInfo.DatabaseName, snapshotInfo.TableName)
	if err != nil {
		return true, nil, err
	}

	// Create a batch with one column containing the result
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"result"})
	bat.Vecs[0] = vector.NewVec(types.T_bool.ToType())
	err = vector.AppendFixed(bat.Vecs[0], result, false, mp)
	if err != nil {
		bat.Clean(mp)
		return true, nil, err
	}
	bat.SetRowCount(1)

	return true, h.convertExecutorResult(executor.Result{
		Batches: []*batch.Batch{bat},
		Mp:      mp,
	}), nil
}
