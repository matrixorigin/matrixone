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
	"fmt"
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
) (Sinker, error) {
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

	// Helper function to add padding
	addPadding := func(sql string) []byte {
		padding := strings.Repeat(" ", v2SQLBufReserved)
		return []byte(padding + sql)
	}

	// CREATE DATABASE
	createDbSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbTblInfo.SinkDbName)
	err = executor.ExecSQL(ctx, ar, addPadding(createDbSQL), false)
	if err != nil {
		executor.Close()
		return nil, err
	}

	// USE DATABASE
	useDbSQL := fmt.Sprintf("USE `%s`", dbTblInfo.SinkDbName)
	err = executor.ExecSQL(ctx, ar, addPadding(useDbSQL), false)
	if err != nil {
		executor.Close()
		return nil, err
	}

	// DROP TABLE if table ID changed (truncate scenario)
	if dbTblInfo.IdChanged {
		dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", dbTblInfo.SinkTblName)
		err = executor.ExecSQL(ctx, ar, addPadding(dropTableSQL), false)
		if err != nil {
			executor.Close()
			return nil, err
		}
		dbTblInfo.IdChanged = false
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
		createSql = strings.Replace(createSql, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
	}

	err = executor.ExecSQL(ctx, ar, addPadding(createSql), false)
	if err != nil {
		executor.Close()
		return nil, err
	}
	if targetOwnerFence != nil {
		if err = executor.ReleaseTargetLock(); err != nil {
			executor.Close()
			return nil, err
		}
	}

	// 4. Create SQL Statement Builder
	builder, err := NewCDCStatementBuilder(
		dbTblInfo.SinkDbName,
		dbTblInfo.SinkTblName,
		tableDef,
		maxSqlLength,
		sinkUri.SinkTyp == CDCSinkType_MO,
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
func (s *mysqlSinker2) handleBegin(ctx context.Context) error {
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
// Converts non-moerr.Error to moerr.Error for consistency
func (s *mysqlSinker2) SetError(err error) {
	if err == nil {
		s.err.Store(nil)
		return
	}
	err = normalizeMysqlSinkerError(err)
	s.err.Store(&err)
}

func normalizeMysqlSinkerError(err error) error {
	// Preserve typed owner-fence wrappers: stream lifecycle and retry policy
	// depend on their identity, and converting them to a plain moerr would turn
	// supersession into shared table failure metadata.
	if _, ok := err.(*moerr.Error); !ok &&
		!IsOwnerFenceLostError(err) && !IsRetryableOwnerFenceError(err) &&
		!IsRetryableTargetLockError(err) && !IsRetryableConnectionError(err) {
		err = moerr.ConvertGoError(context.Background(), err)
	}
	return err
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

		// Close executor (rolls back any active transaction)
		if s.executor != nil {
			if err := s.executor.Close(); err != nil {
				logutil.Error("cdc.mysql_sinker2.close_executor_failed",
					zap.String("table", s.dbTblInfo.String()),
					zap.Error(err))
			}
		}

		logutil.Info("cdc.mysql_sinker2.closed",
			zap.String("table", s.dbTblInfo.String()))
	})
}

// GetTxnState returns the current transaction state (for testing/debugging)
func (s *mysqlSinker2) GetTxnState() int32 {
	return s.txnState.Load()
}
