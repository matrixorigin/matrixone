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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/publication"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

// UpstreamSQLHelper handles special SQL statements (CREATE/DROP SNAPSHOT, OBJECTLIST, GET OBJECT, GETDDL)
// by routing them through frontend processing logic without requiring a Session
type UpstreamSQLHelper struct {
	txnOp     client.TxnOperator
	engine    engine.Engine
	accountID uint32
	executor  executor.SQLExecutor
}

// SetTxnOp sets the transaction operator (called after StartTxn)
func (h *UpstreamSQLHelper) SetTxnOp(txnOp client.TxnOperator) {
	h.txnOp = txnOp
}

// NewUpstreamSQLHelper creates a new UpstreamSQLHelper
func NewUpstreamSQLHelper(
	txnOp client.TxnOperator,
	engine engine.Engine,
	accountID uint32,
	executor executor.SQLExecutor,
) *UpstreamSQLHelper {
	return &UpstreamSQLHelper{
		txnOp:     txnOp,
		engine:    engine,
		accountID: accountID,
		executor:  executor,
	}
}

// HandleSpecialSQL checks if the SQL is a special statement and handles it directly
// Returns (handled, result, error) where handled indicates if the statement was handled
func (h *UpstreamSQLHelper) HandleSpecialSQL(
	ctx context.Context,
	query string,
) (bool, *publication.Result, error) {
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

	// Route special statements through frontend layer processing
	switch s := stmt.(type) {
	case *tree.CreateSnapShot:
		logutil.Info("UpstreamSQLHelper: routing CREATE SNAPSHOT to frontend",
			zap.String("sql", query),
		)
		err := h.handleCreateSnapshotDirectly(ctx, s)
		if err != nil {
			return true, nil, err
		}
		// CREATE SNAPSHOT doesn't return rows
		// Create empty result using internal executor's convertExecutorResult equivalent
		emptyResult := executor.Result{}
		return true, h.convertExecutorResult(emptyResult), nil

	case *tree.DropSnapShot:
		logutil.Info("UpstreamSQLHelper: routing DROP SNAPSHOT to frontend",
			zap.String("sql", query),
		)
		err := h.handleDropSnapshotDirectly(ctx, s)
		if err != nil {
			return true, nil, err
		}
		// DROP SNAPSHOT doesn't return rows
		emptyResult := executor.Result{}
		return true, h.convertExecutorResult(emptyResult), nil

	case *tree.ObjectList:
		logutil.Info("UpstreamSQLHelper: routing OBJECTLIST to frontend",
			zap.String("sql", query),
		)
		result, err := h.handleObjectListDirectly(ctx, s)
		if err != nil {
			return true, nil, err
		}
		return true, result, nil

	case *tree.GetObject:
		logutil.Info("UpstreamSQLHelper: routing GET OBJECT to frontend",
			zap.String("sql", query),
		)
		result, err := h.handleGetObjectDirectly(ctx, s)
		if err != nil {
			return true, nil, err
		}
		return true, result, nil

	case *tree.GetDdl:
		logutil.Info("UpstreamSQLHelper: routing GETDDL to frontend",
			zap.String("sql", query),
		)
		result, err := h.handleGetDdlDirectly(ctx, s)
		if err != nil {
			return true, nil, err
		}
		return true, result, nil
	}

	return false, nil, nil // Not a special statement
}

// handleCreateSnapshotDirectly handles CREATE SNAPSHOT by directly calling frontend logic
func (h *UpstreamSQLHelper) handleCreateSnapshotDirectly(
	ctx context.Context,
	stmt *tree.CreateSnapShot,
) error {
	if h.txnOp == nil {
		return moerr.NewInternalError(ctx, "transaction is required for CREATE SNAPSHOT")
	}
	if h.engine == nil {
		return moerr.NewInternalError(ctx, "engine is required for CREATE SNAPSHOT")
	}

	snapshotName := string(stmt.Name)
	snapshotLevel := stmt.Object.SLevel.Level

	// Check if snapshot already exists
	checkSQL := fmt.Sprintf(`select snapshot_id from mo_catalog.mo_snapshots where sname = "%s" order by snapshot_id;`, snapshotName)
	opts := executor.Options{}.WithDisableIncrStatement().WithTxn(h.txnOp)
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
	var databaseName, tableName string

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
	if h.txnOp == nil {
		return moerr.NewInternalError(ctx, "transaction is required for DROP SNAPSHOT")
	}

	snapshotName := string(stmt.Name)
	sql := fmt.Sprintf(`delete from mo_catalog.mo_snapshots where sname = '%s' order by snapshot_id;`, snapshotName)

	opts := executor.Options{}.WithDisableIncrStatement().WithTxn(h.txnOp)
	logutil.Info("UpstreamSQLHelper: executing DROP SNAPSHOT SQL", zap.String("sql", sql))
	_, err := h.executor.Exec(ctx, sql, opts)
	return err
}

// handleObjectListDirectly handles OBJECTLIST by directly calling frontend logic
func (h *UpstreamSQLHelper) handleObjectListDirectly(
	ctx context.Context,
	stmt *tree.ObjectList,
) (*publication.Result, error) {
	if h.txnOp == nil {
		return nil, moerr.NewInternalError(ctx, "transaction is required for OBJECTLIST")
	}
	if h.engine == nil {
		return nil, moerr.NewInternalError(ctx, "engine is required for OBJECTLIST")
	}

	// Get database name and table name
	dbname := string(stmt.Database)
	tablename := string(stmt.Table)

	// Get mpool
	mp := mpool.MustNewZero()

	// Resolve snapshot using executor
	resolveSnapshot := func(ctx context.Context, snapshotName string) (*timestamp.Timestamp, error) {
		return frontend.ResolveSnapshotWithSnapshotNameWithoutSession(ctx, snapshotName, h.executor, h.txnOp)
	}

	// Get current timestamp from txn
	getCurrentTS := func() types.TS {
		return types.TimestampToTS(h.txnOp.SnapshotTS())
	}

	// Process object list using core function
	resultBatch, err := frontend.ProcessObjectList(ctx, stmt, h.engine, h.txnOp, mp, resolveSnapshot, getCurrentTS, dbname, tablename)
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

	// Read object from engine using frontend function
	content, err := frontend.ReadObjectFromEngine(ctx, h.engine, objectName)
	if err != nil {
		return nil, err
	}

	// Create a batch with one column containing the content
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"data"})
	bat.Vecs[0] = vector.NewVec(types.T_blob.ToType())
	err = vector.AppendBytes(bat.Vecs[0], content, false, mp)
	if err != nil {
		bat.Clean(mp)
		return nil, err
	}

	return h.convertExecutorResult(executor.Result{
		Batches: []*batch.Batch{bat},
		Mp:      mp,
	}), nil
}

// tryToIncreaseTxnPhysicalTS increases the transaction physical timestamp
func (h *UpstreamSQLHelper) tryToIncreaseTxnPhysicalTS(ctx context.Context) (int64, error) {
	if h.txnOp == nil {
		return 0, moerr.NewInternalError(ctx, "transaction is nil")
	}

	curTxnPhysicalTS := h.txnOp.SnapshotTS().PhysicalTime

	if ctx.Value(defines.TenantIDKey{}) == nil {
		return curTxnPhysicalTS, nil
	}

	// A slight increase added to the physical to make sure
	// the updated ts is greater than the old txn timestamp (physical + logic)
	curTxnPhysicalTS += int64(time.Microsecond)
	err := h.txnOp.UpdateSnapshot(ctx, timestamp.Timestamp{
		PhysicalTime: curTxnPhysicalTS,
	})
	if err != nil {
		return 0, err
	}

	return h.txnOp.SnapshotTS().PhysicalTime, nil
}

// handleGetDdlDirectly handles GETDDL by directly calling frontend logic
func (h *UpstreamSQLHelper) handleGetDdlDirectly(
	ctx context.Context,
	stmt *tree.GetDdl,
) (*publication.Result, error) {
	if h.txnOp == nil {
		return nil, moerr.NewInternalError(ctx, "transaction is required for GETDDL")
	}
	if h.engine == nil {
		return nil, moerr.NewInternalError(ctx, "engine is required for GETDDL")
	}

	// Get database name and table name
	var databaseName string
	var tableName string
	if stmt.Database != nil {
		databaseName = string(*stmt.Database)
	}
	if stmt.Table != nil {
		tableName = string(*stmt.Table)
	}

	// Get mpool
	mp := mpool.MustNewZero()

	// Resolve snapshot if provided
	var snapshot *plan2.Snapshot
	if stmt.Snapshot != nil {
		snapshotName := string(*stmt.Snapshot)
		ts, err := frontend.ResolveSnapshotWithSnapshotNameWithoutSession(ctx, snapshotName, h.executor, h.txnOp)
		if err != nil {
			return nil, err
		}
		if ts != nil {
			// Create snapshot with timestamp and account ID
			snapshot = &plan2.Snapshot{
				TS: ts,
				Tenant: &plan2.SnapshotTenant{
					TenantID: h.accountID,
				},
			}
		}
	}

	// Call GetDdlBatchWithoutSession
	resultBatch, err := frontend.GetDdlBatchWithoutSession(ctx, databaseName, tableName, h.engine, h.txnOp, mp, snapshot)
	if err != nil {
		return nil, err
	}

	// Convert batch to result
	return h.convertExecutorResult(executor.Result{
		Batches: []*batch.Batch{resultBatch},
		Mp:      mp,
	}), nil
}

// convertExecutorResult converts executor.Result to publication.Result
func (h *UpstreamSQLHelper) convertExecutorResult(execResult executor.Result) *publication.Result {
	return publication.NewResultFromExecutorResult(execResult)
}
