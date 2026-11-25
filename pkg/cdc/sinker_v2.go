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

package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

// mysqlSinker2 is an improved CDC MySQL sink with clear architecture and robust error handling.
//
// Architecture:
//
//	Producer (Reader) -> Command Channel -> Consumer Goroutine -> Executor -> Database
//
// Key Improvements over mysqlSinker:
// 1. Structured commands (not raw []byte) - type-safe and self-documenting
// 2. Explicit transaction state machine - clear state at any time
// 3. No panic in error handling - proper nil checking everywhere
// 4. Clean separation of concerns - SQLBuilder, Executor, Coordinator
// 5. Guaranteed resource cleanup - no transaction leaks
//
// Error Handling:
// - Errors are stored atomically and checked before each command
// - Once error occurs, all subsequent commands are skipped until ClearError()
// - Producer polls Error() to detect async failures
// - ClearError() must be called before sending recovery commands (e.g. ROLLBACK)
//
// Transaction Lifecycle:
//
//	IDLE --SendBegin--> ACTIVE --SendCommit--> COMMITTED --cleanup--> IDLE
//	                    ACTIVE --SendRollback--> ROLLED_BACK --cleanup--> IDLE
type mysqlSinker2 struct {
	// Core components
	executor *Executor
	builder  *CDCStatementBuilder
	// Serialize consumer commands with producer-side ownership release and
	// lifecycle cleanup. An error can become visible before a command finishes
	// unwinding, so observing Error() is not an executor completion barrier.
	executorMu sync.Mutex

	// Task identification
	accountId uint64
	taskId    string
	dbTblInfo *DbTableInfo
	// Non-zero only for stable-epoch tasks. It makes watermark validation use
	// the same generation-first ordering as the async writer.
	watermarkGeneration uint64

	// Dependencies
	watermarkUpdater *CDCWatermarkUpdater
	ar               *ActiveRoutine
	progressTracker  *ProgressTracker

	// Command channel for async communication
	cmdCh chan *Command
	// Channel closed during shutdown to unblock senders/listeners
	closeCh chan struct{}
	// consumerDone closes whenever the sole command consumer exits. Senders
	// must observe it independently of Close: context or control cancellation
	// can stop Run before the reader reaches its deferred sinker cleanup.
	consumerDone     chan struct{}
	consumerDoneOnce sync.Once

	// Error state - atomic access, no panic risk
	err atomic.Pointer[error]

	// Transaction state machine
	txnState atomic.Int32 // 0=IDLE, 1=ACTIVE, 2=COMMITTED, 3=ROLLED_BACK

	// Lifecycle management
	closeMutex sync.RWMutex
	closed     bool
	closeOnce  sync.Once
	senderWG   sync.WaitGroup

	// Consumer goroutine wait group
	wg sync.WaitGroup
}

// Transaction states
const (
	v2TxnStateIdle       int32 = 0
	v2TxnStateActive     int32 = 1
	v2TxnStateCommitted  int32 = 2
	v2TxnStateRolledBack int32 = 3
)

var (
	errMysqlSinkerPaused          = moerr.NewInternalErrorNoCtx("CDC sinker paused")
	errMysqlSinkerCancelled       = moerr.NewInternalErrorNoCtx("CDC sinker cancelled")
	errMysqlSinkerConsumerStopped = moerr.NewInternalErrorNoCtx("CDC sinker consumer stopped")
)

// Compile-time check that mysqlSinker2 implements Sinker interface
var _ Sinker = (*mysqlSinker2)(nil)

// CreateMysqlSinker2 creates a new MySQL sinker with v2 architecture
// This is the main entry point for creating CDC sink, replacing the old CreateMysqlSinker
// Defined as var for test mocking
var CreateMysqlSinker2 = func(
	ctx context.Context,
	sinkUri UriInfo,
	accountId uint64,
	taskId string,
	dbTblInfo *DbTableInfo,
	watermarkUpdater *CDCWatermarkUpdater,
	tableDef *plan.TableDef,
	retryTimes int,
	retryDuration time.Duration,
	ar *ActiveRoutine,
	maxSqlLength uint64,
	sendSqlTimeout string,
) (Sinker, error) {
	return createMysqlSinker2(
		ctx, sinkUri, accountId, taskId, dbTblInfo, watermarkUpdater, tableDef,
		retryTimes, retryDuration, ar, maxSqlLength, sendSqlTimeout, nil,
	)
}

func createMysqlSinker2(
	ctx context.Context,
	sinkUri UriInfo,
	accountId uint64,
	taskId string,
	dbTblInfo *DbTableInfo,
	watermarkUpdater *CDCWatermarkUpdater,
	tableDef *plan.TableDef,
	retryTimes int,
	retryDuration time.Duration,
	ar *ActiveRoutine,
	maxSqlLength uint64,
	sendSqlTimeout string,
	ownerFence *OwnerFence,
) (_ Sinker, resultErr error) {
	defer func() { resultErr = classifyCDCTargetSQLError(resultErr) }()
	// 1. Determine if we need to record transactions for debugging
	var doRecord bool
	if tableDef != nil {
		doRecord, _ = objectio.CDCRecordTxnInjected(tableDef.DbName, tableDef.Name)
	}

	// 2. Create Executor (replaces Sink in old architecture)
	executor, err := NewExecutor(
		ctx,
		sinkUri.User, sinkUri.Password,
		sinkUri.Ip, sinkUri.Port,
		retryTimes, retryDuration,
		sendSqlTimeout,
		doRecord,
	)
	if err != nil {
		return nil, err
	}

	// 3. Execute DDL initialization (same as old version). Stable tasks pass
	// the table-callback lifecycle context so pause/cancel can interrupt target
	// lock acquisition and initialization SQL.
	var targetOwnerFence func(context.Context) error
	var targetWaitCheck func(context.Context) error
	if ownerFence != nil {
		targetWaitCheck = func(waitCtx context.Context) error {
			if err := waitCtx.Err(); err != nil {
				return err
			}
			if ar != nil {
				select {
				case <-ar.Pause:
					return moerr.NewInternalError(waitCtx, "task paused while waiting for target ownership")
				case <-ar.Cancel:
					return moerr.NewInternalError(waitCtx, "task cancelled while waiting for target ownership")
				default:
				}
			}
			return nil
		}
		targetOwnerFence = ownerFence.Check
		lockIdentity := fmt.Sprintf(
			"%d\x00%s\x00%s\x00%s",
			accountId,
			taskId,
			dbTblInfo.SinkDbName,
			dbTblInfo.SinkTblName,
		)
		if err = executor.AcquireTargetLock(
			ctx, lockIdentity, targetOwnerFence, targetWaitCheck,
		); err != nil {
			executor.Close()
			return nil, err
		}
	}
	if sinkUri.SinkTyp == CDCSinkType_MO &&
		(dbTblInfo.targetIdentityAck != nil || dbTblInfo.TargetIdentity != "") {
		probeTx, beginErr := executor.targetLockConn.BeginTx(ctx, nil)
		if beginErr != nil {
			executor.Close()
			return nil, beginErr
		}
		_, probeErr := probeTx.ExecContext(ctx, "CALL mo_cdc_target_guard_capability()")
		rollbackErr := probeTx.Rollback()
		if probeErr != nil {
			executor.Close()
			return nil, probeErr
		}
		if rollbackErr != nil {
			executor.Close()
			return nil, rollbackErr
		}
	} else if sinkUri.SinkTyp == CDCSinkType_MySQL &&
		(dbTblInfo.targetIdentityAck != nil || dbTblInfo.TargetIdentity != "") {
		if err = checkMySQLTargetIdentityCapability(ctx, executor.targetLockConn,
			dbTblInfo.SinkDbName, dbTblInfo.SinkTblName,
			dbTblInfo.TargetPreIdentity == absentCDCTargetIdentity); err != nil {
			executor.Close()
			return nil, err
		}
	}

	// Helper function to add padding
	addPadding := func(sql string) []byte {
		padding := strings.Repeat(" ", v2SQLBufReserved)
		return []byte(padding + sql)
	}

	// Only a first admission that observed an absent target may create its
	// database. Resume must not mutate a replacement before identity checking.
	if dbTblInfo.TargetPreIdentity == "" && dbTblInfo.TargetIdentity == "" ||
		dbTblInfo.TargetPreIdentity == absentCDCTargetIdentity {
		createDbSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteSQLIdentifier(dbTblInfo.SinkDbName))
		err = executor.ExecSQL(ctx, ar, addPadding(createDbSQL), false)
		if err != nil {
			executor.Close()
			return nil, err
		}
	}

	// USE DATABASE
	useDbSQL := fmt.Sprintf("USE %s", quoteSQLIdentifier(dbTblInfo.SinkDbName))
	err = executor.ExecSQL(ctx, ar, addPadding(useDbSQL), false)
	if err != nil {
		executor.Close()
		return nil, err
	}

	// A durable generation acknowledgement owns target state. A detector hint
	// must not drop a target that already belongs to this generation.
	if dbTblInfo.targetReady {
		if dbTblInfo.TargetIdentity != "" {
			guardTx, beginErr := executor.targetLockConn.BeginTx(ctx, nil)
			if beginErr != nil {
				executor.Close()
				return nil, beginErr
			}
			actual, guardErr := guardedCDCTargetIdentity(ctx, guardTx, sinkUri.SinkTyp, dbTblInfo.SinkDbName, dbTblInfo.SinkTblName)
			if guardErr == nil && actual != dbTblInfo.TargetIdentity {
				guardErr = moerr.NewInternalErrorf(ctx, "CDC target %s.%s was replaced; explicit rebuild is required", dbTblInfo.SinkDbName, dbTblInfo.SinkTblName)
			}
			if guardErr == nil {
				guardErr = verifyCDCTargetTableWithQuery(ctx, guardTx, dbTblInfo, tableDef, sinkUri.SinkTyp)
			}
			rollbackErr := guardTx.Rollback()
			if guardErr != nil {
				executor.Close()
				return nil, guardErr
			}
			if rollbackErr != nil {
				executor.Close()
				return nil, rollbackErr
			}
		} else {
			// The guarded metadata reads - including the generation
			// signature call - require an explicit transaction on the lock
			// connection, never an autocommit pool.
			verifyTx, beginErr := executor.targetLockConn.BeginTx(ctx, nil)
			if beginErr != nil {
				executor.Close()
				return nil, beginErr
			}
			err = verifyCDCTargetTableWithQuery(ctx, verifyTx, dbTblInfo, tableDef, sinkUri.SinkTyp)
			verifyRollbackErr := verifyTx.Rollback()
			if err != nil {
				executor.Close()
				return nil, err
			}
			if verifyRollbackErr != nil {
				executor.Close()
				return nil, verifyRollbackErr
			}
		}
	}

	// CREATE TABLE
	var createSql string
	if tableDef != nil {
		newTableDef := *tableDef
		newTableDef.DbName = dbTblInfo.SinkDbName
		newTableDef.Name = dbTblInfo.SinkTblName
		newTableDef.Fkeys = nil
		newTableDef.Partition = nil

		if newTableDef.TableType == catalog.SystemClusterRel {
			executor.Close()
			return nil, moerr.NewInternalErrorNoCtx("cluster table is not supported")
		}
		if newTableDef.TableType == catalog.SystemExternalRel {
			executor.Close()
			return nil, moerr.NewInternalErrorNoCtx("external table is not supported")
		}

		createSql, _, err = plan2.ConstructCreateTableSQL(nil, &newTableDef, nil, true, nil)
		if err != nil {
			executor.Close()
			return nil, err
		}
		if dbTblInfo.TargetPreIdentity != absentCDCTargetIdentity {
			createSql = strings.Replace(createSql, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
		}
	}

	if !dbTblInfo.targetReady {
		if dbTblInfo.TargetPreIdentity == "" || dbTblInfo.TargetPreIdentity == absentCDCTargetIdentity {
			err = executor.ExecSQL(ctx, ar, addPadding(createSql), false)
			if err != nil {
				executor.Close()
				return nil, err
			}
		}
		if ownerFence != nil && dbTblInfo.targetIdentityAck == nil {
			verifyTx, beginErr := executor.targetLockConn.BeginTx(ctx, nil)
			if beginErr != nil {
				executor.Close()
				return nil, beginErr
			}
			err = verifyCDCTargetTableWithQuery(ctx, verifyTx, dbTblInfo, tableDef, sinkUri.SinkTyp)
			verifyRollbackErr := verifyTx.Rollback()
			if err != nil {
				executor.Close()
				return nil, err
			}
			if verifyRollbackErr != nil {
				executor.Close()
				return nil, verifyRollbackErr
			}
		}
		if dbTblInfo.targetIdentityAck != nil {
			guardTx, beginErr := executor.targetLockConn.BeginTx(ctx, nil)
			if beginErr != nil {
				executor.Close()
				return nil, beginErr
			}
			actual, guardErr := guardedCDCTargetIdentity(ctx, guardTx, sinkUri.SinkTyp, dbTblInfo.SinkDbName, dbTblInfo.SinkTblName)
			if guardErr == nil && dbTblInfo.TargetPreIdentity != absentCDCTargetIdentity && actual != dbTblInfo.TargetPreIdentity {
				guardErr = moerr.NewInternalErrorf(ctx, "CDC target %s.%s changed before first acknowledgement; explicit recovery is required", dbTblInfo.SinkDbName, dbTblInfo.SinkTblName)
			}
			if guardErr == nil {
				guardErr = verifyCDCTargetTableWithQuery(ctx, guardTx, dbTblInfo, tableDef, sinkUri.SinkTyp)
			}
			if guardErr == nil {
				guardErr = dbTblInfo.targetIdentityAck(ctx, actual)
			}
			if guardErr == nil {
				guardErr = guardTx.Commit()
			} else {
				_ = guardTx.Rollback()
			}
			if guardErr != nil {
				executor.Close()
				return nil, guardErr
			}
			dbTblInfo.TargetIdentity = actual
		}
	}
	if targetOwnerFence != nil {
		if err = executor.ReleaseTargetLock(); err != nil {
			executor.Close()
			return nil, err
		}
	}

	// 4. Create SQL Statement Builder
	builderContract := dbTblInfo.GenerationContract
	if builderContract == nil {
		// Legacy unverified creation may proceed without a target observation
		// only when the projection provably uses no generated source slot. An
		// unobserved generated slot has no value-copy proof and fails closed
		// before any write or acknowledgement, never silently omitted.
		if CDCSourceHasGeneratedColumn(tableDef.Cols) {
			executor.Close()
			return nil, moerr.NewInternalErrorf(ctx,
				"CDC target %s.%s has no observed generation contract while the source defines generated columns: explicit target verification or a generated-free mapping is required",
				dbTblInfo.SinkDbName, dbTblInfo.SinkTblName)
		}
		builderContract = CDCOmitGeneratedContractFor(tableDef)
	}
	builder, err := NewCDCStatementBuilder(
		dbTblInfo.SinkDbName,
		dbTblInfo.SinkTblName,
		tableDef,
		maxSqlLength,
		sinkUri.SinkTyp == CDCSinkType_MO,
		builderContract,
	)
	if err != nil {
		executor.Close()
		return nil, err
	}

	// 5. Create mysqlSinker2
	sinker := NewMysqlSinker2(
		executor,
		accountId,
		taskId,
		dbTblInfo,
		watermarkUpdater,
		builder,
		ar,
	)
	if ownerFence != nil {
		sinker.watermarkGeneration = dbTblInfo.SourceTblId
	}
	sinker.dbTblInfo.TargetSinkType = sinkUri.SinkTyp

	// Note: Run() will be started by the caller (e.g., cdc_executor.go)
	// This maintains compatibility with the old mysqlSinker pattern

	logutil.Info(
		"cdc.mysql_sinker2.create_success",
		zap.String("db", dbTblInfo.SinkDbName),
		zap.String("table", dbTblInfo.SinkTblName),
		zap.String("task-id", taskId),
		zap.Uint64("account-id", accountId),
	)

	return sinker, nil
}

// CDCGeneratedExprFirstOrdinal returns the visibility ordinal of the first
// source-generated column, or -1 when the mapping is entirely generated
// free.
func CDCGeneratedExprFirstOrdinal(visible []*plan.ColDef) int32 {
	first := int32(-1)
	for i, col := range visible {
		if col != nil && col.GetGeneratedCol() != nil {
			first = int32(i)
			break
		}
	}
	// int32(-1) may also encode "no generated column"; the loop keeps the
	// first found ordinal below.
	for i, col := range visible {
		if col != nil && col.GetGeneratedCol() != nil {
			return int32(i)
		}
	}
	return first
}

// CopyDecodeSupported pins the typed conversions the row extractor and SQL
// renderer provably complete for a replicated source value written into an
// ordinary writable target column. Generated slots outside this set are
// fenced at admission with their own error instead of failing mid-stream.
func CopyDecodeSupported(t types.T) bool {
	switch t {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128,
		types.T_bool,
		types.T_char, types.T_varchar, types.T_text,
		types.T_date, types.T_datetime, types.T_timestamp:
		return true
	}
	return false
}

// CDCOmitGeneratedContractFor builds the committed-admission-style contract
// that omits every source-generated column and copies every ordinary column.
// External fixtures and tests use it to pass the exact contract shape the
// admission verifier captures for a proven generated target.
func CDCOmitGeneratedContractFor(tableDef *plan.TableDef) *CDCTargetGenerationContract {
	contract := &CDCTargetGenerationContract{}
	if tableDef == nil {
		return contract
	}
	for _, col := range tableDef.Cols {
		if col == nil {
			continue
		}
		if _, internal := catalog.InternalColumns[col.Name]; internal || col.Hidden {
			continue
		}
		contract.Copy = append(contract.Copy, col.GetGeneratedCol() == nil)
	}
	return contract
}

// classifyCDCTargetGeneration builds the per-visible-column W/G contract from
// observed target metadata, inside the caller's admission verify transaction.
// Ordinary writable target columns always copy the source materialized value
// (including a source-generated slot: merge-base parity). A target generated
// column admits omission only for a known MO generated target whose typed
// bound expression, names/ordinals, types, nullability and STORED/VIRTUAL kind
// are proven compatible on both ends; everything else fails closed before any
// DML or acknowledgement, with the bounded alternatives stated in the error.
func classifyCDCTargetGeneration(
	ctx context.Context,
	queryer interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	},
	info *DbTableInfo,
	sinkType string,
	visible []*plan.ColDef,
) (*CDCTargetGenerationContract, error) {
	contract := &CDCTargetGenerationContract{
		Copy:         make([]bool, len(visible)),
		TargetClass:  make([]byte, len(visible)),
		TargetNames:  make([]string, len(visible)),
		TargetDigest: make([]uint64, len(visible)),
	}
	// The only same-admission provenance that truly establishes the current
	// schema is the guarded identity observation of an ABSENT target
	// (TargetPreIdentity == absentCDCTargetIdentity): this admission observed
	// no table and this sink materialized the schema itself. Every other
	// empty-preid path must fall through to the guarded metadata read: a
	// durable resume passes targetReady with the recorded identity and skips
	// CREATE entirely, and legacy CREATE IF NOT EXISTS is not an exclusive
	// creation. A resumed or attached target is thereby proven ordinary, and
	// a generated or unknown target column fails closed before the mapping
	// Conditional routing governs the guarded generation authority: every
	// source-generated mapping reads the strict typed guarded signature in
	// the caller's already-held verify transaction and keeps per-DML
	// revalidation; every generation-free mapping proves ordinary positively
	// through the object-privileged guarded SHOW metadata (live catalog
	// attributes, never a historical view template) at zero per-DML cost.
	// Denied, absent, unknown, incomplete, ambiguous or generated metadata
	// fails closed before the first acknowledgement.
	if sinkType == CDCSinkType_MO {
		// Conditional routing before the first acknowledgement: a
		// GENERATION-FREE source needs only positive ordinary proof, and the
		// guarded SHOW metadata - object-privileged (#30304), rendered from
		// the live catalog attributes rather than any historical view
		// template - provides it through the same already-held verify
		// transaction with unchanged grants. Every source-generated mapping
		// keeps the strict typed guarded signature and its per-DML
		// revalidation. Denied, absent, unknown, incomplete, ambiguous or
		// generated SHOW states fail closed before the first acknowledgement.
		if !CDCSourceHasGeneratedColumn(visible) {
			return classifyShowColumnsOrdinary(ctx, queryer, info, visible, contract)
		}
		rows, err := queryer.QueryContext(ctx, fmt.Sprintf(
			"CALL mo_cdc_target_generation_signature('%s', '%s')",
			escapeSQLString(info.SinkDbName), escapeSQLString(info.SinkTblName)))
		if err != nil {
			return nil, retryableCDCTargetMetadataError(err)
		}
		// Bounded duplicate detection: a second row for one ordinal is
		// malformed metadata and fails closed, never last-write-wins.
		reported := make([]bool, len(visible))
		for rows.Next() {
			var ordinal int64
			var name, class string
			var notNull, digest int64
			if err = rows.Scan(&ordinal, &name, &class, &notNull, &digest); err != nil {
				break
			}
			if ordinal < 0 || int64(int(ordinal)) != ordinal || int(ordinal) >= len(visible) {
				err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s reported generation ordinal %d outside the column layout",
					info.SinkDbName, info.SinkTblName, ordinal)
				break
			}
			if reported[ordinal] {
				err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s reported duplicate generation metadata for column %d",
					info.SinkDbName, info.SinkTblName, ordinal+1)
				break
			}
			reported[ordinal] = true
			switch class {
			case string(CDCColumnOrdinary), string(CDCColumnGeneratedStored), string(CDCColumnGeneratedVirt):
			default:
				err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s reported an unknown generation class %q",
					info.SinkDbName, info.SinkTblName, class)
			}
			if err == nil && name != visible[ordinal].GetOriginCaseName() {
				err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s column %d generation metadata name %q differs from the admission verify",
					info.SinkDbName, info.SinkTblName, ordinal+1, name)
			}
			if err != nil {
				break
			}
			contract.TargetClass[ordinal] = class[0]
			contract.TargetDigest[ordinal] = uint64(digest)
		}
		if err == nil {
			err = rows.Err()
		}
		closeErr := rows.Close() //nolint:sqlclosecheck // Checked below before the per-column decisions.
		if err != nil {
			return nil, retryableCDCTargetMetadataError(err)
		}
		if closeErr != nil {
			return nil, retryableCDCTargetMetadataError(closeErr)
		}
	} else {
		rows, err := queryer.QueryContext(ctx,
			"SELECT column_name, extra FROM information_schema.columns "+
				"WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
			info.SinkDbName, info.SinkTblName)
		if err != nil {
			return nil, retryableCDCTargetMetadataError(err)
		}
		ordinal := 0
		for rows.Next() {
			var layoutName string
			var extra sql.NullString
			if err = rows.Scan(&layoutName, &extra); err != nil {
				break
			}
			if ordinal >= len(visible) {
				ordinal++
				continue
			}
			contract.TargetNames[ordinal] = layoutName
			contract.TargetClass[ordinal] = CDCColumnOrdinary
			if !extra.Valid {
				err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s column %d has unavailable generation metadata",
					info.SinkDbName, info.SinkTblName, ordinal+1)
				break
			}
			// MySQL EXTRA must be decoded precisely: only VIRTUAL GENERATED or
			// STORED GENERATED are computed by the table. DEFAULT_GENERATED
			// marks an ordinary writable expression-default column and stays
			// copyable; an ordinary empty EXTRA stays copyable too.
			upper := strings.ToUpper(extra.String)
			switch {
			case strings.Contains(upper, "VIRTUAL GENERATED"):
				contract.TargetClass[ordinal] = CDCColumnGeneratedVirt
			case strings.Contains(upper, "STORED GENERATED"):
				contract.TargetClass[ordinal] = CDCColumnGeneratedStored
			}
			ordinal++
		}
		if err == nil {
			err = rows.Err()
		}
		closeErr := rows.Close() //nolint:sqlclosecheck // Checked below before the per-column decisions.
		if err != nil {
			return nil, retryableCDCTargetMetadataError(err)
		}
		if closeErr != nil {
			return nil, retryableCDCTargetMetadataError(closeErr)
		}
		if ordinal != len(visible) {
			return nil, moerr.NewInternalErrorf(ctx, "CDC target %s.%s has %d generation metadata rows; expected %d",
				info.SinkDbName, info.SinkTblName, ordinal, len(visible))
		}
	}
	for i := range visible {
		if contract.TargetClass[i] == 0 && contract.TargetDigest[i] == 0 {
			return nil, moerr.NewInternalErrorf(ctx, "CDC target %s.%s column %d has no generation metadata; the target schema is incomplete or denied",
				info.SinkDbName, info.SinkTblName, i+1)
		}
	}
	firstGenerated := CDCGeneratedExprFirstOrdinal(visible)
	for i, col := range visible {
		gen := col.GetGeneratedCol()
		if contract.TargetClass[i] == CDCColumnOrdinary {
			if gen == nil {
				contract.Copy[i] = true
				continue
			}
			if !CopyDecodeSupported(types.T(col.Typ.Id)) {
				return nil, moerr.NewInternalErrorf(ctx,
					"CDC target %s.%s column %s is writable, but its generated payload type cannot be decoded for replication; pre-create an ordinary column of a supported type",
					info.SinkDbName, info.SinkTblName, col.GetOriginCaseName())
			}
			contract.Copy[i] = true
			continue
		}
		if gen == nil {
			return nil, moerr.NewInternalErrorf(ctx,
				"CDC target %s.%s column %s is generated while the source column is not; a generated target column cannot receive a replicated value: pre-create an ordinary writable column instead",
				info.SinkDbName, info.SinkTblName, col.GetOriginCaseName())
		}
		if sinkType != CDCSinkType_MO {
			return nil, moerr.NewNotSupportedf(ctx,
				"CDC target %s.%s column %s is generated on this backend; generated recomputation is not supported until typed equivalence is proven: pre-create an ordinary writable column instead",
				info.SinkDbName, info.SinkTblName, col.GetOriginCaseName())
		}
		srcStored := gen.GetIsStored()
		dstStored := contract.TargetClass[i] == CDCColumnGeneratedStored
		if srcStored != dstStored {
			return nil, moerr.NewInternalErrorf(ctx,
				"CDC target %s.%s column %s generation kind differs from the source: pre-create an ordinary writable column instead",
				info.SinkDbName, info.SinkTblName, col.GetOriginCaseName())
		}
		if !CDCGeneratedExprSafe(gen, firstGenerated, types.T(col.Typ.Id)) {
			return nil, moerr.NewNotSupportedf(ctx,
				"CDC target %s.%s column %s has a session-context dependent generated expression that cannot be reproduced at the target: pre-create an ordinary writable column of the same type instead [%s]",
				info.SinkDbName, info.SinkTblName, col.GetOriginCaseName(),
				CDCGeneratedExprStructure(gen.GetExpr(), 0))
		}
		// The source side must render a complete canonical image before the
		// equality test: a truncated or unhandled node fails closed instead
		// of aliasing into the captured target digest.
		sourceDigest, sourceComplete := CDCColumnTypedDigestFromColDef(col, int32(i))
		if !sourceComplete {
			return nil, moerr.NewInternalErrorf(ctx,
				"CDC target %s.%s column %s generated expression cannot be canonicalized: the source bound tree is truncated or contains an unhandled node: pre-create an ordinary writable column instead",
				info.SinkDbName, info.SinkTblName, col.GetOriginCaseName())
		}
		if sourceDigest != contract.TargetDigest[i] {
			return nil, moerr.NewInternalErrorf(ctx,
				"CDC target %s.%s column %s generated expression differs from the source: pre-create an ordinary writable column instead",
				info.SinkDbName, info.SinkTblName, col.GetOriginCaseName())
		}
		contract.Copy[i] = false
	}
	for i, copy := range contract.Copy {
		if !copy {
			contract.GeneratedOrdinals = append(contract.GeneratedOrdinals, int32(i))
		}
	}
	// Projection-sensitive covers every schema-dependent mapping, not only
	// admitted omissions: a copied source-generated slot (W) drifts the same
	// way when the target layout changes, and mixed maps depend on ordinary
	// operand columns too. Only mappings with no source-generated and no
	// target-generated column keep the plain per-transaction identity cost.
	contract.ProjectionSensitive = false
	for i, col := range visible {
		if col.GetGeneratedCol() != nil || contract.TargetClass[i] != CDCColumnOrdinary {
			contract.ProjectionSensitive = true
			break
		}
	}
	return contract, nil
}

// revalidateCDCTargetGeneration is the per-DML transaction fence: it re-reads
// the target's guarded generation signature on the caller's sinker
// transaction (the exact call family an INSERT-only role may invoke) and
// compares the captured contract. It runs after the target identity guard has
// taken the same catalog name locks as DDL and advanced the snapshot.
func revalidateCDCTargetGeneration(ctx context.Context, tx *sql.Tx, info *DbTableInfo) error {
	if tx == nil || info == nil || info.GenerationContract == nil || !info.GenerationContract.ProjectionSensitive {
		return nil
	}
	if info.TargetSinkType != CDCSinkType_MO {
		// Backend-appropriate evidence for a copied ordinary layout: inside
		// the same sinker transaction the catalog layout is re-read and must
		// hold the admitted names and generation classes exactly. Duplicates,
		// shorts, drifts or unavailable metadata all fail closed before any
		// write of this batch.
		rows, qErr := tx.QueryContext(ctx,
			"SELECT column_name, extra FROM information_schema.columns "+
				"WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
			info.SinkDbName, info.SinkTblName)
		if qErr != nil {
			return moerr.NewInternalErrorf(ctx, "CDC target %s.%s generation layout revalidation failed: %v",
				info.SinkDbName, info.SinkTblName, qErr)
		}
		width := len(info.GenerationContract.Copy)
		seen := make([]bool, width)
		columns := 0
		var err error
		for rows.Next() {
			var layoutName string
			var extra sql.NullString
			if err = rows.Scan(&layoutName, &extra); err != nil {
				break
			}
			if columns >= width || layoutName != info.GenerationContract.TargetNames[columns] {
				err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s reported an unexpected column layout at position %d",
					info.SinkDbName, info.SinkTblName, columns+1)
				break
			}
			if !extra.Valid {
				err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s column %d has unavailable generation metadata",
					info.SinkDbName, info.SinkTblName, columns+1)
				break
			}
			upperLayout := strings.ToUpper(extra.String)
			class := CDCColumnOrdinary
			switch {
			case strings.Contains(upperLayout, "VIRTUAL GENERATED"):
				class = CDCColumnGeneratedVirt
			case strings.Contains(upperLayout, "STORED GENERATED"):
				class = CDCColumnGeneratedStored
			}
			if class != info.GenerationContract.TargetClass[columns] {
				err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s column %s changed generation class",
					info.SinkDbName, info.SinkTblName, layoutName)
				break
			}
			seen[columns] = true
			columns++
		}
		if err == nil {
			err = rows.Err()
		}
		closeErr := rows.Close() //nolint:sqlclosecheck // Checked below before the mismatch fence.
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "CDC target %s.%s generation layout revalidation failed: %v",
				info.SinkDbName, info.SinkTblName, err)
		}
		for i := 0; i < width; i++ {
			if !seen[i] {
				return moerr.NewInternalErrorf(ctx,
					"CDC target %s.%s generation layout revalidation is incomplete: %d of %d columns reported",
					info.SinkDbName, info.SinkTblName, columns, width)
			}
		}
		return nil
	}
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(
		"CALL mo_cdc_target_generation_signature('%s', '%s')",
		escapeSQLString(info.SinkDbName), escapeSQLString(info.SinkTblName)))
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "CDC target %s.%s generation signature revalidation failed: %v",
			info.SinkDbName, info.SinkTblName, err)
	}
	// Fixed-width, bounded state: no ordinal-driven growth. Every malformed
	// input fails closed - short, duplicate, out-of-range, empty or unknown
	// metadata never reaches a padded match.
	width := len(info.GenerationContract.Copy)
	class := make([]byte, width)
	digest := make([]uint64, width)
	seen := make([]bool, width)
	columns := 0
	for rows.Next() {
		var ordinal int64
		var name, classText string
		var notNull, digestValue int64
		if err = rows.Scan(&ordinal, &name, &classText, &notNull, &digestValue); err != nil {
			break
		}
		if ordinal < 0 || int64(int(ordinal)) != ordinal || int(ordinal) >= width || len(classText) != 1 {
			err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s reported malformed generation metadata (ordinal %d, class %q)",
				info.SinkDbName, info.SinkTblName, ordinal, classText)
			break
		}
		if seen[ordinal] {
			err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s reported duplicate generation metadata for column %d",
				info.SinkDbName, info.SinkTblName, ordinal+1)
			break
		}
		seen[ordinal] = true
		class[ordinal] = classText[0]
		digest[ordinal] = uint64(digestValue)
		columns++
	}
	if err == nil {
		err = rows.Err()
	}
	closeErr := rows.Close() //nolint:sqlclosecheck // Checked below before the mismatch error.
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "CDC target %s.%s generation signature revalidation failed: %v",
			info.SinkDbName, info.SinkTblName, err)
	}
	if columns != width {
		return moerr.NewInternalErrorf(ctx,
			"CDC target %s.%s generation signature revalidation is incomplete: %d of %d columns reported",
			info.SinkDbName, info.SinkTblName, columns, width)
	}
	if !CDCContractMatchesSignature(info.GenerationContract, class, digest) {
		return moerr.NewInternalErrorf(ctx,
			"CDC target %s.%s changed schema without being replaced; explicit recovery is required before any further DML",
			info.SinkDbName, info.SinkTblName)
	}
	return nil
}

func verifyCDCTargetTable(
	ctx context.Context,
	executor *Executor,
	info *DbTableInfo,
	tableDef *plan.TableDef,
	sinkType string,
) error {
	if executor.targetLockConn == nil {
		return moerr.NewInternalError(ctx, "CDC target verification requires the target lock")
	}
	return verifyCDCTargetTableWithQuery(ctx, executor.targetLockConn, info, tableDef, sinkType)
}

func verifyCDCTargetTableWithQuery(
	ctx context.Context,
	queryer interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	},
	info *DbTableInfo,
	tableDef *plan.TableDef,
	sinkType string,
) error {
	if tableDef == nil || tableDef.Pkey == nil || len(tableDef.Pkey.Names) == 0 {
		return moerr.NewInternalError(ctx, "CDC target verification requires source table metadata and a primary key")
	}
	visible := make([]*plan.ColDef, 0, len(tableDef.Cols))
	origin := make(map[string]string, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		if col == nil {
			return moerr.NewInternalError(ctx, "CDC source has a nil column")
		}
		if _, internal := catalog.InternalColumns[col.Name]; internal || col.Hidden {
			continue
		}
		visible = append(visible, col)
		origin[col.Name] = col.GetOriginCaseName()
	}
	rows, err := queryer.QueryContext(ctx,
		"SELECT column_name, column_type, collation_name, numeric_scale FROM information_schema.columns "+
			"WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
		info.SinkDbName, info.SinkTblName)
	if err != nil {
		return retryableCDCTargetMetadataError(err)
	}
	columnCount := 0
	for rows.Next() {
		var name, typ string
		var collation sql.NullString
		var numericScale sql.NullInt64
		if err = rows.Scan(&name, &typ, &collation, &numericScale); err != nil {
			break
		}
		if columnCount >= len(visible) || name != visible[columnCount].GetOriginCaseName() ||
			!cdcTargetColumnTypeMatches(visible[columnCount].Typ, typ, sinkType, numericScale) {
			err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s column %d differs from source generation %d",
				info.SinkDbName, info.SinkTblName, columnCount+1, info.SourceTblId)
			break
		}
		if expected := expectedCDCTargetCollation(visible[columnCount].Typ); expected != "" &&
			(!collation.Valid || !strings.EqualFold(collation.String, expected)) {
			err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s column %d collation differs from source generation %d",
				info.SinkDbName, info.SinkTblName, columnCount+1, info.SourceTblId)
			break
		}
		columnCount++
	}
	if err == nil {
		err = retryableCDCTargetMetadataError(rows.Err())
	}
	closeErr := rows.Close() //nolint:sqlclosecheck // Check the close error before the next metadata query.
	if err != nil {
		return err
	}
	if closeErr != nil {
		return retryableCDCTargetMetadataError(closeErr)
	}
	if columnCount != len(visible) {
		return moerr.NewInternalErrorf(ctx, "CDC target %s.%s has %d columns; expected %d",
			info.SinkDbName, info.SinkTblName, columnCount, len(visible))
	}
	contract, err := classifyCDCTargetGeneration(ctx, queryer, info, sinkType, visible)
	if err != nil {
		return err
	}
	info.GenerationContract = contract

	indexRows, err := queryer.QueryContext(ctx,
		"SELECT index_name, non_unique, seq_in_index, column_name, sub_part FROM information_schema.statistics "+
			"WHERE table_schema = ? AND table_name = ? ORDER BY index_name, seq_in_index",
		info.SinkDbName, info.SinkTblName)
	if err != nil {
		return retryableCDCTargetMetadataError(err)
	}
	actualPrimary := []string(nil)
	actualUnique := make(map[string][]string)
	for indexRows.Next() {
		var name, column string
		var nonUnique, sequence int64
		var prefix sql.NullInt64
		if err = indexRows.Scan(&name, &nonUnique, &sequence, &column, &prefix); err != nil {
			break
		}
		if nonUnique != 0 {
			continue
		}
		if prefix.Valid {
			err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s has a prefix unique key", info.SinkDbName, info.SinkTblName)
			break
		}
		if strings.EqualFold(name, "PRIMARY") {
			if sequence != int64(len(actualPrimary)+1) {
				err = moerr.NewInternalError(ctx, "CDC target primary key order is invalid")
				break
			}
			actualPrimary = append(actualPrimary, column)
		} else {
			parts := actualUnique[name]
			if sequence != int64(len(parts)+1) {
				err = moerr.NewInternalError(ctx, "CDC target unique key order is invalid")
				break
			}
			actualUnique[name] = append(parts, column)
		}
	}
	if err == nil {
		err = retryableCDCTargetMetadataError(indexRows.Err())
	}
	closeErr = indexRows.Close() //nolint:sqlclosecheck // Check the close error before accepting the target.
	if err != nil {
		return err
	}
	if closeErr != nil {
		return retryableCDCTargetMetadataError(closeErr)
	}
	expectedPrimary := make([]string, 0, len(tableDef.Pkey.Names))
	for _, name := range tableDef.Pkey.Names {
		expectedPrimary = append(expectedPrimary, origin[name])
	}
	if strings.Join(actualPrimary, "\x00") != strings.Join(expectedPrimary, "\x00") {
		return moerr.NewInternalErrorf(ctx, "CDC target %s.%s primary key differs from source generation %d",
			info.SinkDbName, info.SinkTblName, info.SourceTblId)
	}
	expectedUnique := make(map[string]int)
	for _, index := range tableDef.Indexes {
		if index == nil || !index.Unique {
			continue
		}
		parts := make([]string, 0, len(index.Parts))
		for _, name := range index.Parts {
			if catalog.IsAlias(name) {
				continue
			}
			parts = append(parts, origin[name])
		}
		expectedUnique[strings.Join(parts, "\x00")]++
	}
	for _, parts := range actualUnique {
		key := strings.Join(parts, "\x00")
		if expectedUnique[key] == 0 {
			return moerr.NewInternalErrorf(ctx, "CDC target %s.%s has an unexpected unique key",
				info.SinkDbName, info.SinkTblName)
		}
		expectedUnique[key]--
	}
	for _, missing := range expectedUnique {
		if missing != 0 {
			return moerr.NewInternalErrorf(ctx, "CDC target %s.%s is missing a source unique key",
				info.SinkDbName, info.SinkTblName)
		}
	}
	return nil
}

func retryableCDCTargetMetadataError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return err
	}
	return NewRetryableSnapshotEpochError(err)
}

// VerifyOwnedTarget checks a completed historical generation without issuing
// target DDL. The target lock excludes a concurrent claimant's reset.
func VerifyOwnedTarget(
	ctx context.Context, sinkUri UriInfo, accountID uint64, taskID string,
	info *DbTableInfo, tableDef *plan.TableDef, fence *OwnerFence, timeout string,
) (resultErr error) {
	defer func() { resultErr = classifyCDCTargetSQLError(resultErr) }()
	if fence == nil {
		return moerr.NewInternalError(ctx, "CDC target verification requires an owner fence")
	}
	executor, err := NewExecutor(ctx, sinkUri.User, sinkUri.Password, sinkUri.Ip, sinkUri.Port,
		CDCDefaultRetryTimes, CDCDefaultRetryDuration, timeout, false)
	if err != nil {
		return err
	}
	defer executor.Close()
	identity := fmt.Sprintf("%d\x00%s\x00%s\x00%s", accountID, taskID, info.SinkDbName, info.SinkTblName)
	if err = executor.AcquireTargetLock(ctx, identity, fence.Check, fence.Check); err != nil {
		return err
	}
	if info.TargetIdentity != "" {
		guardTx, beginErr := executor.targetLockConn.BeginTx(ctx, nil)
		if beginErr != nil {
			return beginErr
		}
		actual, guardErr := guardedCDCTargetIdentity(ctx, guardTx, sinkUri.SinkTyp, info.SinkDbName, info.SinkTblName)
		if guardErr == nil && actual != info.TargetIdentity {
			guardErr = moerr.NewInternalErrorf(ctx, "CDC target %s.%s was replaced; explicit rebuild is required", info.SinkDbName, info.SinkTblName)
		}
		if guardErr == nil {
			guardErr = verifyCDCTargetTableWithQuery(ctx, guardTx, info, tableDef, sinkUri.SinkTyp)
		}
		rollbackErr := guardTx.Rollback()
		if guardErr != nil {
			return guardErr
		}
		if rollbackErr != nil {
			return rollbackErr
		}
	} else {
		verifyTx, beginErr := executor.targetLockConn.BeginTx(ctx, nil)
		if beginErr != nil {
			return beginErr
		}
		err = verifyCDCTargetTableWithQuery(ctx, verifyTx, info, tableDef, sinkUri.SinkTyp)
		verifyRollbackErr := verifyTx.Rollback()
		if err != nil {
			return err
		}
		if verifyRollbackErr != nil {
			return verifyRollbackErr
		}
	}
	if err = fence.Check(ctx); err != nil {
		return err
	}
	return executor.ReleaseTargetLock()
}

var (
	cdcIntegerDisplayWidth = regexp.MustCompile(`(tinyint|smallint|mediumint|bigint|int)(unsigned)?\([0-9]+\)`)
	cdcScalarZeroWidth     = regexp.MustCompile(`(float|double|date|datetime|timestamp|time|bool|json|uuid|datalink)\(0\)`)
)

func normalizeCDCTargetColumnType(raw string) string {
	if at := strings.IndexByte(raw, '('); at >= 0 && strings.ContainsAny(raw[at:], "'\"") {
		// ENUM/SET values are data. Folding spaces or aliases inside quoted
		// elements could admit a different target domain.
		return strings.ToLower(strings.TrimSpace(raw[:at])) + raw[at:]
	}
	typ := strings.ToLower(strings.Join(strings.Fields(raw), ""))
	if strings.HasPrefix(typ, "integer") {
		typ = "int" + strings.TrimPrefix(typ, "integer")
	}
	typ = cdcScalarZeroWidth.ReplaceAllString(typ, "$1")
	if typ == "year(4)" {
		return "year"
	}
	if typ == "boolean" {
		return "bool"
	}
	return cdcIntegerDisplayWidth.ReplaceAllString(typ, "$1$2")
}

func cdcTargetColumnTypeMatches(source plan.Type, target, sinkType string, numericScale sql.NullInt64) bool {
	want := normalizeCDCTargetColumnType(plan2.FormatColType(source))
	got := normalizeCDCTargetColumnType(target)
	if sinkType == CDCSinkType_MO && source.Width > 0 &&
		(types.T(source.Id) == types.T_float32 || types.T(source.Id) == types.T_float64) {
		// Legacy MO COLUMN_TYPE prints FLOAT(M) and DOUBLE(M) without D. The
		// independent NUMERIC_SCALE field distinguishes omitted D from an
		// explicit scale, including D=0.
		name := "float"
		if types.T(source.Id) == types.T_float64 {
			name = "double"
		}
		if got != want && got != fmt.Sprintf("%s(%d)", name, source.Width) {
			return false
		}
		return source.Scale < 0 && !numericScale.Valid ||
			source.Scale >= 0 && numericScale.Valid && numericScale.Int64 == int64(source.Scale)
	}
	// A current-generation MO target renders its physical-BOOL COLUMN_TYPE as
	// the raw alias tinyint(1) (issue #31749), while a native MO integer
	// target never renders a display width: types.FormatSQLColumnType ignores
	// integer widths. The exact raw token therefore identifies a physical
	// BOOL target alone, and it must be gated before the normalized want==got
	// comparison, which strips the display width and would admit every
	// tinyint source as a false positive. Only a native BOOL source may sink
	// into the alias; legacy MO 'boolean'/'bool' spellings keep the normalized
	// path below, and MySQL keeps its display-width-tolerant final arm.
	if sinkType == CDCSinkType_MO && strings.EqualFold(strings.TrimSpace(target), "tinyint(1)") {
		return types.T(source.Id) == types.T_bool
	}
	if want == got {
		return true
	}
	return types.T(source.Id) == types.T_bool && sinkType == CDCSinkType_MySQL &&
		strings.EqualFold(strings.TrimSpace(target), "tinyint(1)")
}

func expectedCDCTargetCollation(typ plan.Type) string {
	switch types.T(typ.Id) {
	case types.T_char, types.T_varchar, types.T_text:
		switch typ.Charset {
		case uint32(types.CharsetLegacy), uint32(types.CharsetUTF8MB4Bin):
			return "utf8mb4_bin"
		case uint32(types.CharsetUTF8):
			return "utf8mb4_general_ci"
		case uint32(types.CharsetBinary):
			return "binary"
		}
		return "unsupported source collation"
	}
	return ""
}

// NewMysqlSinker2 creates a new improved MySQL sinker
func NewMysqlSinker2(
	executor *Executor,
	accountId uint64,
	taskId string,
	dbTblInfo *DbTableInfo,
	watermarkUpdater *CDCWatermarkUpdater,
	builder *CDCStatementBuilder,
	ar *ActiveRoutine,
) *mysqlSinker2 {
	s := &mysqlSinker2{
		executor:         executor,
		builder:          builder,
		accountId:        accountId,
		taskId:           taskId,
		dbTblInfo:        dbTblInfo,
		watermarkUpdater: watermarkUpdater,
		ar:               ar,
		cmdCh:            make(chan *Command), // Unbuffered for backpressure
		closeCh:          make(chan struct{}),
		consumerDone:     make(chan struct{}),
		closed:           false,
	}

	// Initialize transaction state
	s.txnState.Store(v2TxnStateIdle)

	return s
}

// AttachProgressTracker attaches a progress tracker for observability updates.
func (s *mysqlSinker2) AttachProgressTracker(pt *ProgressTracker) {
	s.progressTracker = pt
}

// Run is the consumer goroutine that processes commands from the channel
//
// Process Flow:
// 1. Read command from channel
// 2. Check error state (skip if error exists)
// 3. Validate command
// 4. Execute command via appropriate handler
// 5. Set error state if execution fails
// 6. Update transaction state
//
// The goroutine exits when:
// - Command channel is closed (normal shutdown)
// - Context is cancelled
// - ActiveRoutine signals pause/cancel
func (s *mysqlSinker2) Run(ctx context.Context, ar *ActiveRoutine) {
	logutil.Info("cdc.mysql_sinker2.run_start",
		zap.String("table", s.dbTblInfo.String()))
	defer s.consumerDoneOnce.Do(func() { close(s.consumerDone) })

	// Check if already closed before incrementing wait group
	// This prevents data race with Close() calling wg.Wait()
	s.closeMutex.RLock()
	if s.closed {
		s.closeMutex.RUnlock()
		logutil.Info("cdc.mysql_sinker2.run_already_closed",
			zap.String("table", s.dbTblInfo.String()))
		return
	}
	s.wg.Add(1)
	s.closeMutex.RUnlock()
	defer s.wg.Done()

	defer func() {
		logutil.Info("cdc.mysql_sinker2.run_end",
			zap.String("table", s.dbTblInfo.String()))
	}()

	for {
		select {
		case <-ctx.Done():
			s.setErrorIfNil(context.Cause(ctx))
			return
		case <-s.closeCh:
			return
		case <-ar.Pause:
			s.setErrorIfNil(errMysqlSinkerPaused)
			return
		case <-ar.Cancel:
			s.setErrorIfNil(errMysqlSinkerCancelled)
			return
		case cmd, ok := <-s.cmdCh:
			if !ok {
				// Channel closed, exit
				return
			}

			// Process the command
			s.processCommand(ctx, cmd)
		}
	}
}

// processCommand processes a single command
func (s *mysqlSinker2) processCommand(ctx context.Context, cmd *Command) {
	s.executorMu.Lock()
	defer s.executorMu.Unlock()

	// Check error state - skip processing if error exists
	if err := s.Error(); err != nil {
		logutil.Debug("cdc.mysql_sinker2.command_skipped_due_to_error",
			zap.String("table", s.dbTblInfo.String()),
			zap.String("command", cmd.String()),
			zap.Error(err))
		// Clean up batch data in skipped commands to prevent memory leaks
		cmd.Close()
		return
	}

	// Validate command
	if err := cmd.Validate(); err != nil {
		logutil.Error("cdc.mysql_sinker2.invalid_command",
			zap.String("table", s.dbTblInfo.String()),
			zap.String("command", cmd.String()),
			zap.Error(err))
		cmd.Close()
		s.SetError(err)
		return
	}

	// Dispatch to appropriate handler
	var err error
	switch cmd.Type {
	case CmdBegin:
		logutil.Debug("cdc.mysql_sinker2.processing_begin",
			zap.String("table", s.dbTblInfo.String()),
			zap.Int32("current-state", s.txnState.Load()))
		err = s.handleBegin(ctx)
	case CmdCommit:
		err = s.handleCommit(ctx)
	case CmdRollback:
		err = s.handleRollback(ctx)
	case CmdInsertBatch:
		err = s.handleInsertBatch(ctx, cmd)
	case CmdInsertDeleteBatch:
		err = s.handleInsertDeleteBatch(ctx, cmd)
	case CmdFlush:
		err = s.handleFlush(ctx, cmd)
	case CmdDummy:
		// Dummy command - no operation, just for synchronization
		return
	default:
		err = moerr.NewInternalError(ctx, "unknown command type")
	}

	// Set error if handler failed
	if err != nil {
		logutil.Error("cdc.mysql_sinker2.command_failed",
			zap.String("table", s.dbTblInfo.String()),
			zap.String("command", cmd.String()),
			zap.Error(err))
		s.SetError(err)
	}
}

// handleBegin handles BEGIN transaction command
func (s *mysqlSinker2) handleBegin(ctx context.Context) (resultErr error) {
	defer func() { resultErr = classifyCDCTargetSQLError(resultErr) }()
	// Check current state
	currentState := s.txnState.Load()
	if currentState != v2TxnStateIdle {
		// If already active, this is idempotent - just return success
		// This can happen if BEGIN was called multiple times
		if currentState == v2TxnStateActive {
			logutil.Debug("cdc.mysql_sinker2.begin_already_active",
				zap.String("table", s.dbTblInfo.String()))
			v2.CdcSinkerTransactionCounter.WithLabelValues("begin", "skipped").Inc()
			return nil
		}
		// Other states (COMMITTED, ROLLED_BACK) - reset to IDLE and continue
		// This is idempotent behavior - log at Debug level
		logutil.Debug("cdc.mysql_sinker2.begin_not_idle",
			zap.String("table", s.dbTblInfo.String()),
			zap.Int32("current-state", currentState),
			zap.Int32("expected-state", v2TxnStateIdle))
		// Reset to IDLE and continue with BEGIN
		s.txnState.Store(v2TxnStateIdle)
		// Fall through to execute BEGIN
	}

	// Begin transaction
	if err := s.executor.BeginTx(ctx); err != nil {
		v2.CdcSinkerTransactionCounter.WithLabelValues("begin", "error").Inc()
		logutil.Error("cdc.mysql_sinker2.begin_tx_failed",
			zap.String("table", s.dbTblInfo.String()),
			zap.Error(err))
		return err
	}
	if s.dbTblInfo.TargetIdentity != "" {
		actual, err := guardedCDCTargetIdentity(ctx, s.executor.tx,
			s.dbTblInfo.TargetSinkType, s.dbTblInfo.SinkDbName, s.dbTblInfo.SinkTblName)
		if err == nil && actual != s.dbTblInfo.TargetIdentity {
			err = moerr.NewInternalErrorf(ctx, "CDC target %s.%s was replaced; explicit rebuild is required", s.dbTblInfo.SinkDbName, s.dbTblInfo.SinkTblName)
		}
		if err != nil {
			_ = s.executor.RollbackTx(ctx)
			return err
		}
	}
	// Projection-sensitive mappings revalidate the captured generation
	// contract on this same DML transaction, after the identity guard has
	// taken the same catalog name locks as DDL and advanced its snapshot,
	// and strictly before any row SQL is dispatched.
	if err := revalidateCDCTargetGeneration(ctx, s.executor.tx, s.dbTblInfo); err != nil {
		v2.CdcSinkerTransactionCounter.WithLabelValues("revalidate", "error").Inc()
		logutil.Error("cdc.mysql_sinker2.generation_contract_fence",
			zap.String("table", s.dbTblInfo.String()),
			zap.Error(err))
		_ = s.executor.RollbackTx(ctx)
		return err
	}

	// Update state
	s.txnState.Store(v2TxnStateActive)

	// Record metrics
	v2.CdcSinkerTransactionCounter.WithLabelValues("begin", "success").Inc()

	// logutil.Debug("cdc.mysql_sinker2.begin_txn",
	// 	zap.String("table", s.dbTblInfo.String()),
	// 	zap.Int32("new-state", s.txnState.Load()))

	return nil
}

// handleCommit handles COMMIT transaction command
func (s *mysqlSinker2) handleCommit(ctx context.Context) error {
	// Check current state
	currentState := s.txnState.Load()
	if currentState != v2TxnStateActive {
		// Not an error - idempotent behavior
		// Log at Debug level to avoid noise, metrics still recorded for observability
		logutil.Debug("cdc.mysql_sinker2.commit_no_active_txn",
			zap.String("table", s.dbTblInfo.String()),
			zap.Int32("current-state", currentState),
			zap.Int32("expected-state", v2TxnStateActive))
		// Record as skipped (no active transaction to commit)
		v2.CdcSinkerTransactionCounter.WithLabelValues("commit", "skipped").Inc()
		return nil
	}

	// Commit transaction with timing
	start := time.Now()
	if err := s.executor.CommitTx(ctx); err != nil {
		// Even on error, transition to ROLLED_BACK (tx is cleaned up in executor)
		s.txnState.Store(v2TxnStateRolledBack)

		// Record metrics
		v2.CdcSinkerTransactionCounter.WithLabelValues("commit", "error").Inc()

		logutil.Error("cdc.mysql_sinker2.commit_failed",
			zap.String("table", s.dbTblInfo.String()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return err
	}

	// Update state
	s.txnState.Store(v2TxnStateCommitted)

	// Transition back to IDLE
	s.txnState.Store(v2TxnStateIdle)

	// Record metrics
	v2.CdcSinkerTransactionCounter.WithLabelValues("commit", "success").Inc()

	// logutil.Debug("cdc.mysql_sinker2.commit_success",
	// 	zap.String("task-id", s.taskId),
	// 	zap.String("table", s.dbTblInfo.String()),
	// 	zap.Duration("commit-duration", time.Since(start)),
	// 	zap.Int32("final-state", s.txnState.Load()))

	return nil
}

// handleRollback handles ROLLBACK transaction command
func (s *mysqlSinker2) handleRollback(ctx context.Context) error {
	// Check current state
	currentState := s.txnState.Load()
	if currentState != v2TxnStateActive {
		// Not an error - idempotent behavior
		// But we should still record metrics for observability
		logutil.Debug("cdc.mysql_sinker2.rollback_no_active_txn",
			zap.String("table", s.dbTblInfo.String()),
			zap.Int32("state", currentState))
		// Record as skipped (no active transaction to rollback)
		v2.CdcSinkerTransactionCounter.WithLabelValues("rollback", "skipped").Inc()
		return nil
	}

	// Rollback transaction
	if err := s.executor.RollbackTx(ctx); err != nil {
		// Even on error, transition to ROLLED_BACK (tx is cleaned up in executor)
		s.txnState.Store(v2TxnStateRolledBack)
		s.txnState.Store(v2TxnStateIdle)

		// Record metrics
		v2.CdcSinkerTransactionCounter.WithLabelValues("rollback", "error").Inc()

		return err
	}

	// Update state
	s.txnState.Store(v2TxnStateRolledBack)

	// Transition back to IDLE
	s.txnState.Store(v2TxnStateIdle)

	// Record metrics
	v2.CdcSinkerTransactionCounter.WithLabelValues("rollback", "success").Inc()

	logutil.Debug("cdc.mysql_sinker2.rollback_txn",
		zap.String("task-id", s.taskId),
		zap.String("table", s.dbTblInfo.String()))

	return nil
}

func (s *mysqlSinker2) recordSQLSuccess(sqlType string, duration time.Duration) {
	v2.CdcSinkerSQLCounter.WithLabelValues(sqlType, "success").Inc()
	v2.CdcSinkerSQLDuration.WithLabelValues(sqlType).Observe(duration.Seconds())
	if s.progressTracker != nil {
		s.progressTracker.RecordSQL(1)
	}
}

func (s *mysqlSinker2) recordSQLFailure(sqlType string, duration time.Duration) {
	v2.CdcSinkerSQLCounter.WithLabelValues(sqlType, "error").Inc()
	v2.CdcSinkerSQLDuration.WithLabelValues(sqlType).Observe(duration.Seconds())
}

// handleInsertBatch handles INSERT batch command (snapshot data)
func (s *mysqlSinker2) handleInsertBatch(ctx context.Context, cmd *Command) error {
	// Ensure snapshot batch is cleaned up after processing
	defer cmd.Close()

	// start := time.Now()
	rows := 0
	if cmd.InsertBatch != nil {
		rows = cmd.InsertBatch.RowCount()
	}

	// Build INSERT SQL statements
	sqls, err := s.builder.BuildInsertSQL(ctx, cmd.InsertBatch, cmd.Meta.FromTs, cmd.Meta.ToTs)
	if err != nil {
		logutil.Error("cdc.mysql_sinker2.build_insert_sql_failed",
			zap.String("table", s.dbTblInfo.String()),
			zap.Int("rows", rows),
			zap.Error(err))
		return err
	}

	// logutil.Debug("cdc.mysql_sinker2.insert_batch_start",
	// 	zap.String("task-id", s.taskId),
	// 	zap.String("table", s.dbTblInfo.String()),
	// 	zap.Int("rows", rows),
	// 	zap.Int("sql-count", len(sqls)),
	// 	zap.String("from-ts", cmd.Meta.FromTs.ToString()),
	// 	zap.String("to-ts", cmd.Meta.ToTs.ToString()))

	// Execute each SQL statement
	for i, sql := range sqls {
		sqlStart := time.Now()
		err := s.executor.ExecSQL(ctx, s.ar, sql, true)
		duration := time.Since(sqlStart)
		if err != nil {
			s.recordSQLFailure("insert", duration)
			logutil.Error("cdc.mysql_sinker2.exec_insert_sql_failed",
				zap.String("table", s.dbTblInfo.String()),
				zap.Int("sql-index", i),
				zap.Int("total-sqls", len(sqls)),
				zap.Duration("duration", duration),
				zap.Error(err))
			return err
		}

		s.recordSQLSuccess("insert", duration)

		if duration > time.Second {
			logutil.Warn("cdc.mysql_sinker2.exec_insert_sql_slow",
				zap.String("table", s.dbTblInfo.String()),
				zap.Int("sql-index", i),
				zap.Duration("duration", duration))
		}
	}

	// logutil.Debug("cdc.mysql_sinker2.insert_batch_complete",
	// 	zap.String("task-id", s.taskId),
	// 	zap.String("table", s.dbTblInfo.String()),
	// 	zap.Int("rows", rows),
	// 	zap.Int("sql-count", len(sqls)),
	// 	zap.String("from-ts", cmd.Meta.FromTs.ToString()),
	// 	zap.String("to-ts", cmd.Meta.ToTs.ToString()),
	// 	zap.Duration("total-duration", time.Since(start)))

	return nil
}

// handleInsertDeleteBatch handles INSERT/DELETE batch command (tail data)
func (s *mysqlSinker2) handleInsertDeleteBatch(ctx context.Context, cmd *Command) error {
	// Ensure atomic batches are cleaned up on all paths (including error returns)
	defer cmd.Close()

	start := time.Now()
	insertRows := 0
	deleteRows := 0
	insertTotalRows := 0
	deleteTotalRows := 0
	insertDuplicates := 0
	deleteDuplicates := 0

	if cmd.InsertAtmBatch != nil {
		insertRows = cmd.InsertAtmBatch.RowCount()
		insertTotalRows = cmd.InsertAtmBatch.TotalRows()
		insertDuplicates = cmd.InsertAtmBatch.DuplicateRows()
	}
	if cmd.DeleteAtmBatch != nil {
		deleteRows = cmd.DeleteAtmBatch.RowCount()
		deleteTotalRows = cmd.DeleteAtmBatch.TotalRows()
		deleteDuplicates = cmd.DeleteAtmBatch.DuplicateRows()
	}

	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		startFields := []zap.Field{
			zap.String("table", s.dbTblInfo.String()),
			zap.Int("insert-rows", insertRows),
			zap.Int("delete-rows", deleteRows),
			zap.String("from-ts", cmd.Meta.FromTs.ToString()),
			zap.String("to-ts", cmd.Meta.ToTs.ToString()),
		}
		if insertTotalRows != insertRows || insertDuplicates > 0 {
			startFields = append(startFields,
				zap.Int("insert-total-rows", insertTotalRows),
				zap.Int("insert-duplicates", insertDuplicates),
			)
		}
		if deleteTotalRows != deleteRows || deleteDuplicates > 0 {
			startFields = append(startFields,
				zap.Int("delete-total-rows", deleteTotalRows),
				zap.Int("delete-duplicates", deleteDuplicates),
			)
		}
		logutil.Debug("cdc.mysql_sinker2.insert_delete_batch_start", startFields...)
	}

	// A logical change can delete and insert the same primary key (for example,
	// a partition-key update). Apply deletes first so the replacement remains
	// visible at the sink after the transaction is replayed.
	if cmd.DeleteAtmBatch != nil && cmd.DeleteAtmBatch.RowCount() > 0 {
		// Record metrics for delete operation
		deleteRows := cmd.DeleteAtmBatch.RowCount()
		deleteBytes := uint64(deleteRows * 100) // Rough estimate: 100 bytes per row
		if s.progressTracker != nil {
			tableLabel := s.progressTracker.TableKey()
			v2.CdcRowsProcessedCounter.WithLabelValues("delete", tableLabel).Add(float64(deleteRows))
			v2.CdcBytesProcessedCounter.WithLabelValues("delete", tableLabel).Add(float64(deleteBytes))
		}

		sqls, err := s.builder.BuildDeleteSQL(ctx, cmd.DeleteAtmBatch, cmd.Meta.FromTs, cmd.Meta.ToTs)
		if err != nil {
			logutil.Error("cdc.mysql_sinker2.build_delete_sql_failed",
				zap.String("table", s.dbTblInfo.String()),
				zap.Int("rows", deleteRows),
				zap.Error(err))
			return err
		}

		for i, sql := range sqls {
			sqlStart := time.Now()
			err := s.executor.ExecSQL(ctx, s.ar, sql, true)
			duration := time.Since(sqlStart)
			if err != nil {
				s.recordSQLFailure("delete", duration)
				logutil.Error("cdc.mysql_sinker2.exec_delete_sql_failed",
					zap.String("table", s.dbTblInfo.String()),
					zap.Int("sql-index", i),
					zap.Int("total-sqls", len(sqls)),
					zap.Duration("duration", duration),
					zap.Error(err))
				return err
			}

			s.recordSQLSuccess("delete", duration)

			if duration > time.Second {
				logutil.Warn("cdc.mysql_sinker2.exec_delete_sql_slow",
					zap.String("table", s.dbTblInfo.String()),
					zap.Int("sql-index", i),
					zap.Duration("duration", duration))
			}
		}
	}

	// Build and execute INSERT SQL after deletes. A successful insert is the
	// final state for updates and partition moves that share a commit timestamp.
	if cmd.InsertAtmBatch != nil && cmd.InsertAtmBatch.RowCount() > 0 {
		// Record metrics for insert operation
		insertRows := cmd.InsertAtmBatch.RowCount()
		insertBytes := uint64(insertRows * 100) // Rough estimate: 100 bytes per row
		if s.progressTracker != nil {
			tableLabel := s.progressTracker.TableKey()
			v2.CdcRowsProcessedCounter.WithLabelValues("insert", tableLabel).Add(float64(insertRows))
			v2.CdcBytesProcessedCounter.WithLabelValues("insert", tableLabel).Add(float64(insertBytes))
		}

		sqls, err := s.builder.buildAtomicInsertSQL(ctx, cmd.InsertAtmBatch, cmd.Meta.FromTs, cmd.Meta.ToTs)
		if err != nil {
			logutil.Error("cdc.mysql_sinker2.build_insert_sql_failed",
				zap.String("table", s.dbTblInfo.String()),
				zap.Int("rows", insertRows),
				zap.Error(err))
			return err
		}

		for i, sql := range sqls {
			sqlStart := time.Now()
			err := s.executor.ExecSQL(ctx, s.ar, sql, true)
			duration := time.Since(sqlStart)
			if err != nil {
				s.recordSQLFailure("insert", duration)
				logutil.Error("cdc.mysql_sinker2.exec_insert_sql_failed",
					zap.String("table", s.dbTblInfo.String()),
					zap.Int("sql-index", i),
					zap.Int("total-sqls", len(sqls)),
					zap.Duration("duration", duration),
					zap.Error(err))
				return err
			}

			s.recordSQLSuccess("insert", duration)

			if duration > time.Second {
				logutil.Warn("cdc.mysql_sinker2.exec_insert_sql_slow",
					zap.String("table", s.dbTblInfo.String()),
					zap.Int("sql-index", i),
					zap.Duration("duration", duration))
			}
		}
	}

	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		completeFields := []zap.Field{
			zap.String("table", s.dbTblInfo.String()),
			zap.Int("insert-rows", insertRows),
			zap.Int("delete-rows", deleteRows),
			zap.Duration("total-duration", time.Since(start)),
		}
		if insertTotalRows != insertRows || insertDuplicates > 0 {
			completeFields = append(completeFields,
				zap.Int("insert-total-rows", insertTotalRows),
				zap.Int("insert-duplicates", insertDuplicates),
			)
		}
		if deleteTotalRows != deleteRows || deleteDuplicates > 0 {
			completeFields = append(completeFields,
				zap.Int("delete-total-rows", deleteTotalRows),
				zap.Int("delete-duplicates", deleteDuplicates),
			)
		}
		logutil.Debug("cdc.mysql_sinker2.insert_delete_batch_complete", completeFields...)
	}

	return nil
}

// handleFlush handles FLUSH command (sends any buffered SQL)
func (s *mysqlSinker2) handleFlush(ctx context.Context, cmd *Command) error {
	// In the new design, we don't buffer SQL at the sinker level
	// SQL is constructed and sent immediately in handleInsertBatch/handleInsertDeleteBatch
	// So FLUSH is essentially a no-op

	logutil.Debug("cdc.mysql_sinker2.flush",
		zap.String("table", s.dbTblInfo.String()),
		zap.Bool("noMoreData", cmd.Meta.NoMoreData))

	return nil
}

// Sink processes data and queues SQL execution commands
//
// This method is called by the producer (reader) to sink data.
// It validates watermark and queues appropriate commands for the consumer goroutine.
func (s *mysqlSinker2) Sink(ctx context.Context, data *DecoderOutput) {
	// Ensure batch data is freed if we return before transferring ownership to a Command.
	// Once ownership transfers, we nil out data's batch fields to prevent double-free.
	defer data.Close()

	// Check if sinker is closed
	s.closeMutex.RLock()
	if s.closed {
		s.closeMutex.RUnlock()
		return
	}
	s.closeMutex.RUnlock()

	// Validate watermark (ensure we're not processing old data)
	key := WatermarkKey{
		AccountId: s.accountId,
		TaskId:    s.taskId,
		DBName:    s.dbTblInfo.SourceDbName,
		TableName: s.dbTblInfo.SourceTblName,
	}

	watermark, watermarkGeneration, err := s.watermarkUpdater.GetFromCacheWithGeneration(ctx, &key)
	if err != nil {
		logutil.Error("cdc.mysql_sinker2.get_watermark_failed",
			zap.String("table", s.dbTblInfo.String()),
			zap.String("key", key.String()),
			zap.Error(err))
		s.SetError(err)
		return
	}

	if isWatermarkAheadOfOutput(
		watermark, watermarkGeneration, data.toTs, s.watermarkGeneration) {
		logutil.Error("cdc.mysql_sinker2.unexpected_watermark",
			zap.String("table", s.dbTblInfo.String()),
			zap.String("toTs", data.toTs.ToString()),
			zap.String("watermark", watermark.ToString()),
			zap.Uint64("source-table-id", s.watermarkGeneration),
			zap.Uint64("watermark-source-table-id", watermarkGeneration))
		err := moerr.NewInternalError(ctx, "unexpected watermark")
		s.SetError(err)
		return
	}

	// Handle based on data type
	if data.noMoreData {
		// Flush any remaining data
		cmd := NewFlushCommand(true, data.fromTs, data.toTs)
		s.sendCommand(cmd)
		return
	}

	// Queue data command — transfer ownership of batch data to the Command.
	// Nil out data's fields so the deferred data.Close() won't double-free.
	var cmd *Command
	if data.outputTyp == OutputTypeSnapshot {
		cmd = NewInsertBatchCommand(data.checkpointBat, data.mp, data.fromTs, data.toTs)
		cmd.snapshotPermit = data.snapshotPermit
		data.checkpointBat = nil
		data.mp = nil
		data.snapshotPermit = nil
	} else if data.outputTyp == OutputTypeTail {
		cmd = NewInsertDeleteBatchCommand(data.insertAtmBatch, data.deleteAtmBatch, data.fromTs, data.toTs)
		data.insertAtmBatch = nil
		data.deleteAtmBatch = nil
	} else {
		logutil.Error("cdc.mysql_sinker2.unknown_output_type",
			zap.String("table", s.dbTblInfo.String()),
			zap.String("outputType", data.outputTyp.String()))
		return
	}

	s.sendCommand(cmd)
}

func isWatermarkAheadOfOutput(
	watermark types.TS,
	watermarkGeneration uint64,
	outputTS types.TS,
	outputGeneration uint64,
) bool {
	return watermarkGeneration > outputGeneration ||
		(watermarkGeneration == outputGeneration && outputTS.LT(&watermark))
}

// sendCommand sends a command to the consumer goroutine
func (s *mysqlSinker2) sendCommand(cmd *Command) {
	s.closeMutex.RLock()
	if s.closed {
		s.closeMutex.RUnlock()
		// Clean up batch data in dropped commands
		cmd.Close()
		return
	}
	cmdCh := s.cmdCh
	closeCh := s.closeCh
	consumerDone := s.consumerDone
	s.senderWG.Add(1)
	s.closeMutex.RUnlock()

	defer s.senderWG.Done()

	select {
	case cmdCh <- cmd:
	case <-closeCh:
		// Clean up batch data in dropped commands
		cmd.Close()
		return
	case <-consumerDone:
		// Run can stop on context/pause/cancel before Close is reached by the
		// producer. Republish a terminal error here because rollback deliberately
		// clears an earlier error before sending its recovery commands. Otherwise
		// a command dropped after consumer exit could be mistaken for success.
		s.setErrorIfNil(errMysqlSinkerConsumerStopped)
		cmd.Close()
		return
	}
}

// SendBegin queues a BEGIN transaction command
func (s *mysqlSinker2) SendBegin() {
	s.sendCommand(NewBeginCommand())
}

// SendCommit queues a COMMIT transaction command
func (s *mysqlSinker2) SendCommit() {
	s.sendCommand(NewCommitCommand())
}

// SendRollback queues a ROLLBACK transaction command
func (s *mysqlSinker2) SendRollback() {
	s.sendCommand(NewRollbackCommand())
}

// SendDummy queues a dummy command for synchronization
//
// The dummy command forces the consumer to process all previous commands
// and allows the producer to check for errors via Error()
func (s *mysqlSinker2) SendDummy() {
	s.sendCommand(NewDummyCommand())
}

// releaseTargetOwnership ends the current effect interval after the consumer
// has synchronously completed COMMIT and the producer has accepted the related
// watermark. The next BEGIN reacquires and revalidates the same target lock.
func (s *mysqlSinker2) releaseTargetOwnership() error {
	s.executorMu.Lock()
	defer s.executorMu.Unlock()
	return s.executor.ReleaseTargetLock()
}

// Error returns the current error state
//
// Thread-safe and panic-free (unlike old implementation)
func (s *mysqlSinker2) Error() error {
	errPtr := s.err.Load()
	if errPtr == nil {
		return nil
	}
	return *errPtr
}

// SetError sets the error state
//
// Preserves classified backend/control causes; normalizes unknown errors.
func (s *mysqlSinker2) SetError(err error) {
	if err == nil {
		s.err.Store(nil)
		return
	}
	err = normalizeMysqlSinkerError(err)
	s.err.Store(&err)
}

func normalizeMysqlSinkerError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*moerr.Error); ok {
		return err
	}
	// Stream recovery and control handling depend on the original typed cause.
	if _, classified := ClassifyRetryableError(err); classified {
		return err
	}
	return moerr.ConvertGoError(context.Background(), err)
}

func (s *mysqlSinker2) setErrorIfNil(err error) {
	if err == nil {
		return
	}
	err = normalizeMysqlSinkerError(err)
	s.err.CompareAndSwap(nil, &err)
}

// ClearError clears the error state
//
// Must be called before sending recovery commands (e.g. ROLLBACK after error)
func (s *mysqlSinker2) ClearError() {
	s.err.Store(nil)
}

// Reset resets the sinker state
//
// Unlike the old implementation, this properly cleans up active transactions:
// 1. Rollback any active transaction
// 2. Clear error state
// 3. Reset transaction state to IDLE
func (s *mysqlSinker2) Reset() {
	s.executorMu.Lock()
	defer s.executorMu.Unlock()

	// If there's an active transaction, roll it back
	if s.txnState.Load() == v2TxnStateActive {
		if err := s.executor.RollbackTx(context.Background()); err != nil {
			logutil.Error("cdc.mysql_sinker2.reset_rollback_failed",
				zap.String("table", s.dbTblInfo.String()),
				zap.Error(err))
		}
	}

	// Reset state
	s.txnState.Store(v2TxnStateIdle)
	s.ClearError()

	logutil.Info("cdc.mysql_sinker2.reset",
		zap.String("table", s.dbTblInfo.String()))
}

// Close stops the consumer goroutine and releases resources
//
// Safe to call multiple times (idempotent)
func (s *mysqlSinker2) Close() {
	s.closeOnce.Do(func() {
		s.closeMutex.Lock()
		if s.closed {
			s.closeMutex.Unlock()
			return
		}
		s.closed = true
		close(s.closeCh)
		s.closeMutex.Unlock()

		s.senderWG.Wait()

		// Close command channel (stops consumer goroutine)
		close(s.cmdCh)

		// Wait for consumer goroutine to exit
		s.wg.Wait()

		// Close executor (rolls back any active transaction). The consumer is
		// stopped, but a producer may still be releasing target ownership.
		s.executorMu.Lock()
		if s.executor != nil {
			if err := s.executor.Close(); err != nil {
				logutil.Error("cdc.mysql_sinker2.close_executor_failed",
					zap.String("table", s.dbTblInfo.String()),
					zap.Error(err))
			}
		}
		s.executorMu.Unlock()

		logutil.Info("cdc.mysql_sinker2.closed",
			zap.String("table", s.dbTblInfo.String()))
	})
}

// GetTxnState returns the current transaction state (for testing/debugging)
func (s *mysqlSinker2) GetTxnState() int32 {
	return s.txnState.Load()
}
