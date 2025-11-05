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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/mysql"
	"go.uber.org/zap"
)

// Constants for SQL execution
const (
	// v2SQLBufReserved reserves 5 bytes at the head of SQL buffer for mysql driver optimization
	// The mysql driver can reuse the buffer if the first 5 bytes are reserved
	v2SQLBufReserved = 5

	// v2SQLPrintLen limits the length of SQL printed in logs for readability
	v2SQLPrintLen = 200

	// v2FakeSql is a placeholder SQL used with mysql.ReuseQueryBuf
	// The actual SQL is passed via the named argument
	v2FakeSql = "fakeSql"
)

// Executor manages database connection, transaction lifecycle, and SQL execution.
//
// Key Features:
// - Nil-safe transaction operations (idempotent Begin/Commit/Rollback)
// - Automatic retry with exponential backoff
// - Clear transaction state management
// - No transaction leaks (always cleans up on commit/rollback)
//
// Transaction Guarantees:
// - BeginTx() can only be called when no active transaction exists
// - CommitTx()/RollbackTx() are idempotent (safe to call even if tx is nil)
// - After Commit/Rollback, transaction is always set to nil
type Executor struct {
	conn *sql.DB
	tx   *sql.Tx

	// Connection info (for reconnection)
	user, password string
	ip             string
	port           int
	timeout        string

	// Retry configuration
	retryTimes    int           // -1 for infinite retry
	retryDuration time.Duration // Max total retry duration

	// Debug transaction recording
	debugTxnRecorder struct {
		doRecord bool
		txnSQL   []string
		sqlBytes int
	}
}

// NewExecutor creates a new Executor with database connection
func NewExecutor(
	user, password string,
	ip string,
	port int,
	retryTimes int,
	retryDuration time.Duration,
	timeout string,
	doRecord bool,
) (*Executor, error) {
	e := &Executor{
		user:          user,
		password:      password,
		ip:            ip,
		port:          port,
		retryTimes:    retryTimes,
		retryDuration: retryDuration,
		timeout:       timeout,
	}
	e.debugTxnRecorder.doRecord = doRecord

	if err := e.Connect(); err != nil {
		return nil, err
	}

	return e, nil
}

// Connect establishes a database connection
func (e *Executor) Connect() error {
	conn, err := OpenDbConn(e.user, e.password, e.ip, e.port, e.timeout)
	if err != nil {
		return err
	}
	e.conn = conn
	return nil
}

// BeginTx starts a new transaction
//
// Returns error if:
// - A transaction is already active
// - Database connection failed
func (e *Executor) BeginTx(ctx context.Context) error {
	if e.tx != nil {
		return moerr.NewInternalError(ctx, "transaction already active")
	}

	e.resetRecordedTxn()

	tx, err := e.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	e.tx = tx
	return nil
}

// CommitTx commits the current transaction
//
// Idempotent: Returns nil if no active transaction exists
// Always sets e.tx to nil after commit (successful or not)
func (e *Executor) CommitTx(ctx context.Context) error {
	if e.tx == nil {
		return nil // Idempotent
	}

	err := e.tx.Commit()
	e.tx = nil // Always clear, even on error
	e.resetRecordedTxn()
	return err
}

// RollbackTx rolls back the current transaction
//
// Idempotent: Returns nil if no active transaction exists
// Always sets e.tx to nil after rollback (successful or not)
func (e *Executor) RollbackTx(ctx context.Context) error {
	if e.tx == nil {
		return nil // Idempotent
	}

	err := e.tx.Rollback()
	e.tx = nil // Always clear, even on error
	e.resetRecordedTxn()
	return err
}

// ExecSQL executes a SQL statement
//
// If a transaction is active, executes within the transaction.
// Otherwise, executes as a standalone statement.
//
// Parameters:
// - sqlBuf: SQL statement with 5-byte header (for mysql driver reuse)
// - needRetry: Whether to retry on failure
// - ar: ActiveRoutine for cancellation support
func (e *Executor) ExecSQL(
	ctx context.Context,
	ar *ActiveRoutine,
	sqlBuf []byte,
	needRetry bool,
) error {
	if len(sqlBuf) <= v2SQLBufReserved {
		return moerr.NewInternalError(ctx, "SQL buffer too short")
	}

	reuseQueryArg := sql.NamedArg{
		Name:  mysql.ReuseQueryBuf,
		Value: sqlBuf,
	}

	e.recordTxnSQL(sqlBuf)

	execFunc := func() error {
		var err error
		if e.tx != nil {
			_, err = e.tx.Exec(v2FakeSql, reuseQueryArg)
		} else {
			_, err = e.conn.Exec(v2FakeSql, reuseQueryArg)
		}

		if err != nil {
			e.logFailedSQL(err, sqlBuf)
		}
		return err
	}

	if !needRetry {
		return execFunc()
	}

	return e.retryWithBackoff(ctx, ar, execFunc)
}

// HasActiveTx returns true if there's an active transaction
func (e *Executor) HasActiveTx() bool {
	return e.tx != nil
}

// Close closes the database connection and rolls back any active transaction
func (e *Executor) Close() error {
	// Rollback any active transaction
	if e.tx != nil {
		_ = e.tx.Rollback()
		e.tx = nil
	}

	// Close connection
	if e.conn != nil {
		err := e.conn.Close()
		e.conn = nil
		return err
	}

	e.debugTxnRecorder.txnSQL = nil
	return nil
}

// retryWithBackoff retries a function with exponential backoff
func (e *Executor) retryWithBackoff(
	ctx context.Context,
	ar *ActiveRoutine,
	fn func() error,
) error {
	shouldContinue := func(attempt int, startTime time.Time) bool {
		// retryTimes == -1 means retry forever
		if e.retryTimes == -1 {
			return time.Since(startTime) < e.retryDuration
		}
		// Always execute at least once (when attempt == 0)
		// Then retry up to retryTimes more times
		return attempt <= e.retryTimes && time.Since(startTime) < e.retryDuration
	}

	backoff := time.Second
	startTime := time.Now()

	for attempt := 0; shouldContinue(attempt, startTime); attempt++ {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ar.Pause:
			return moerr.NewInternalError(ctx, "task paused")
		case <-ar.Cancel:
			return moerr.NewInternalError(ctx, "task cancelled")
		default:
		}

		// Execute function
		start := time.Now()
		err := fn()
		v2.CdcSendSqlDurationHistogram.Observe(time.Since(start).Seconds())

		// Success
		if err == nil {
			return nil
		}

		// Log retry attempt
		logutil.Error(
			"CDC-Executor-RetryFailed",
			zap.Int("attempt", attempt+1),
			zap.Error(err),
		)
		v2.CdcMysqlSinkErrorCounter.Inc()

		// Wait with backoff
		time.Sleep(backoff)
		backoff = min(backoff*2, 30*time.Second) // Cap at 30 seconds
	}

	return moerr.NewInternalError(ctx, "retry limit exceeded")
}

// recordTxnSQL records SQL for debugging
func (e *Executor) recordTxnSQL(sqlBuf []byte) {
	if !e.debugTxnRecorder.doRecord {
		return
	}

	e.debugTxnRecorder.sqlBytes += len(sqlBuf)
	e.debugTxnRecorder.txnSQL = append(
		e.debugTxnRecorder.txnSQL,
		string(sqlBuf[v2SQLBufReserved:]),
	)
}

// resetRecordedTxn resets the recorded transaction SQL
func (e *Executor) resetRecordedTxn() {
	if !e.debugTxnRecorder.doRecord {
		return
	}
	e.debugTxnRecorder.sqlBytes = 0
	e.debugTxnRecorder.txnSQL = e.debugTxnRecorder.txnSQL[:0]
}

// logFailedSQL logs failed SQL statement (truncated for readability)
func (e *Executor) logFailedSQL(err error, sqlBuf []byte) {
	maxLen := min(len(sqlBuf), v2SQLPrintLen+v2SQLBufReserved)
	logutil.Error(
		"CDC-Executor-SQLFailed",
		zap.Error(err),
		zap.String("sql", string(sqlBuf[v2SQLBufReserved:maxLen])),
	)

	// Log full transaction if recording is enabled
	if e.debugTxnRecorder.doRecord && len(e.debugTxnRecorder.txnSQL) > 0 {
		logutil.Error(
			"CDC-Executor-TransactionHistory",
			zap.Strings("txnSQL", e.debugTxnRecorder.txnSQL),
		)
	}
}

// GetMaxAllowedPacket queries the database for max_allowed_packet setting
func (e *Executor) GetMaxAllowedPacket() (uint64, error) {
	var maxPacket uint64
	err := e.conn.QueryRow("SELECT @@max_allowed_packet").Scan(&maxPacket)
	if err != nil {
		return 0, err
	}
	return maxPacket, nil
}

// ExecSimpleSQL executes a simple SQL statement outside of transaction
// Used for initialization (CREATE DATABASE, USE db, etc.)
func (e *Executor) ExecSimpleSQL(ctx context.Context, sql string) error {
	if e.tx != nil {
		return moerr.NewInternalError(ctx, "cannot execute simple SQL while transaction is active")
	}

	_, err := e.conn.ExecContext(ctx, sql)
	return err
}
