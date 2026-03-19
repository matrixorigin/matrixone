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
	"database/sql/driver"
	"errors"
	"math"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc/retry"
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

	defaultCircuitBreakerFailures = 5
	defaultCircuitBreakerCooldown = 30 * time.Second
)

type circuitBreaker struct {
	sinkLabel    string
	maxFailures  int
	coolDown     time.Duration
	failureCount int
	open         bool
	openedAt     time.Time
	mu           sync.Mutex
}

func newCircuitBreaker(sink string, maxFailures int, coolDown time.Duration) *circuitBreaker {
	if maxFailures <= 0 {
		maxFailures = defaultCircuitBreakerFailures
	}
	if coolDown <= 0 {
		coolDown = defaultCircuitBreakerCooldown
	}
	cb := &circuitBreaker{
		sinkLabel:   sink,
		maxFailures: maxFailures,
		coolDown:    coolDown,
	}
	v2.CdcSinkerCircuitStateGauge.WithLabelValues(sink).Set(0)
	return cb
}

func (cb *circuitBreaker) IsOpen() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if !cb.open {
		return false
	}

	if time.Since(cb.openedAt) >= cb.coolDown {
		cb.open = false
		cb.failureCount = 0
		cb.openedAt = time.Time{}
		v2.CdcSinkerCircuitStateGauge.WithLabelValues(cb.sinkLabel).Set(0)
		logutil.Info("cdc.executor.retry_circuit_half_open",
			zap.String("sink", cb.sinkLabel))
		return false
	}
	return true
}

func (cb *circuitBreaker) OnFailure() (opened bool, justOpened bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	if cb.failureCount >= cb.maxFailures {
		if !cb.open {
			cb.open = true
			cb.openedAt = time.Now()
			v2.CdcSinkerCircuitStateGauge.WithLabelValues(cb.sinkLabel).Set(1)
			return true, true
		}
		cb.openedAt = time.Now()
		return true, false
	}
	return cb.open, false
}

func (cb *circuitBreaker) OnSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount = 0
	if cb.open {
		cb.open = false
		cb.openedAt = time.Time{}
		v2.CdcSinkerCircuitStateGauge.WithLabelValues(cb.sinkLabel).Set(0)
		logutil.Info("cdc.executor.retry_circuit_closed",
			zap.String("sink", cb.sinkLabel))
	}
}

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

	retryPolicy     retry.Policy
	retryClassifier retry.ErrorClassifier

	sinkLabel      string
	circuitBreaker *circuitBreaker

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

	e.initRetryPolicy()

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

	if err := e.ensureConnection(ctx); err != nil {
		return err
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

	if err := e.ensureConnection(ctx); err != nil {
		return err
	}

	reuseQueryArg := sql.NamedArg{
		Name:  mysql.ReuseQueryBuf,
		Value: sqlBuf,
	}

	e.recordTxnSQL(sqlBuf)

	execFunc := func() error {
		if err := e.ensureConnection(ctx); err != nil {
			return err
		}

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

	start := time.Now()
	if !needRetry {
		err := execFunc()
		v2.CdcSendSqlDurationHistogram.Observe(time.Since(start).Seconds())
		return err
	}

	return e.execWithRetry(ctx, ar, execFunc)
}

func (e *Executor) execWithRetry(
	ctx context.Context,
	ar *ActiveRoutine,
	fn func() error,
) error {
	sinkLabel := e.sinkLabel
	if sinkLabel == "" {
		sinkLabel = "mysql"
	}

	if e.circuitBreaker != nil && e.circuitBreaker.IsOpen() {
		logutil.Warn(
			"cdc.executor.retry_circuit_blocked",
			zap.String("sink", sinkLabel),
		)
		v2.CdcSinkerRetryCounter.WithLabelValues(sinkLabel, "circuit_open", "blocked").Inc()
		return moerr.NewInternalError(ctx, "sinker circuit breaker open")
	}

	policy := e.retryPolicy
	policy.MaxAttempts = e.calculateMaxAttempts()
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	policy.Classifier = e.retryClassifier

	start := time.Now()
	attempt := 0
	var lastErr error

	err := policy.Do(ctx, func() error {
		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if ar != nil {
			select {
			case <-ar.Pause:
				return moerr.NewInternalError(ctx, "task paused")
			case <-ar.Cancel:
				return moerr.NewInternalError(ctx, "task cancelled")
			default:
			}
		}

		if e.retryDuration > 0 && attempt > 1 && time.Since(start) >= e.retryDuration {
			return retry.ErrNonRetryable
		}

		begin := time.Now()
		err := fn()
		v2.CdcSendSqlDurationHistogram.Observe(time.Since(begin).Seconds())
		if err == nil {
			if e.circuitBreaker != nil {
				e.circuitBreaker.OnSuccess()
			}
			lastErr = nil
			return nil
		}

		reason := classifyRetryReason(err)
		lastErr = err

		logutil.Error(
			"cdc.executor.retry_failed",
			zap.Int("attempt", attempt),
			zap.Error(err),
			zap.String("reason", reason),
		)
		v2.CdcMysqlSinkErrorCounter.Inc()
		v2.CdcSinkerRetryCounter.WithLabelValues(sinkLabel, reason, "failed").Inc()

		if e.circuitBreaker != nil {
			if opened, justOpened := e.circuitBreaker.OnFailure(); opened {
				if justOpened {
					logutil.Warn("cdc.executor.retry_circuit_opened",
						zap.String("sink", sinkLabel),
						zap.Int("attempt", attempt),
						zap.String("reason", reason),
					)
				}
				v2.CdcSinkerRetryCounter.WithLabelValues(sinkLabel, reason, "circuit_open").Inc()
				return retry.ErrCircuitOpen
			}
		}

		return err
	})

	if err == nil {
		if attempt > 1 && lastErr != nil {
			reason := classifyRetryReason(lastErr)
			logutil.Info(
				"cdc.executor.retry_success",
				zap.Int("attempts", attempt),
				zap.Duration("total-duration", time.Since(start)),
				zap.String("reason", reason),
			)
			v2.CdcSinkerRetryCounter.WithLabelValues(sinkLabel, reason, "success").Inc()
		}
		return nil
	}

	if errors.Is(err, retry.ErrCircuitOpen) {
		return moerr.NewInternalError(ctx, "sinker circuit breaker open")
	}

	if errors.Is(err, retry.ErrNonRetryable) {
		reason := "duration_limit"
		if lastErr != nil {
			reason = classifyRetryReason(lastErr)
		}
		logutil.Error(
			"cdc.executor.retry_exhausted",
			zap.Int("attempts", attempt),
			zap.Duration("total-duration", time.Since(start)),
			zap.String("reason", reason),
			zap.Error(lastErr),
		)
		v2.CdcSinkerRetryCounter.WithLabelValues(sinkLabel, reason, "exhausted").Inc()
		return moerr.NewInternalError(ctx, "retry limit exceeded")
	}

	if lastErr != nil {
		return lastErr
	}

	return err
}

func (e *Executor) initRetryPolicy() {
	classifier := retry.MultiClassifier{
		retry.DefaultClassifier{},
		retry.MySQLErrorClassifier{},
	}

	e.retryClassifier = classifier
	e.retryPolicy = retry.Policy{
		MaxAttempts: e.calculateMaxAttempts(),
		Backoff: retry.ExponentialBackoff{
			Base:   200 * time.Millisecond,
			Factor: 2,
			Max:    30 * time.Second,
			Jitter: 200 * time.Millisecond,
		},
		Classifier: classifier,
	}
	if e.sinkLabel == "" {
		e.sinkLabel = "mysql"
	}
	e.circuitBreaker = newCircuitBreaker(e.sinkLabel, defaultCircuitBreakerFailures, defaultCircuitBreakerCooldown)
}

func (e *Executor) calculateMaxAttempts() int {
	if e.retryTimes < 0 {
		return math.MaxInt32
	}
	attempts := e.retryTimes + 1
	if attempts < 1 {
		attempts = 1
	}
	return attempts
}

func classifyRetryReason(err error) string {
	if err == nil {
		return "unknown"
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline"
	}
	if errors.Is(err, driver.ErrBadConn) {
		return "bad_conn"
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return "conn_reset"
	}
	if errors.Is(err, syscall.EPIPE) {
		return "broken_pipe"
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "net_timeout"
		}
		type temporary interface {
			Temporary() bool
		}
		if tmp, ok := netErr.(temporary); ok && tmp.Temporary() {
			return "net_temporary"
		}
		return "net_error"
	}

	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return "mysql_" + strconv.FormatUint(uint64(mysqlErr.Number), 10)
	}

	return "unknown"
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
// ensureConnection makes sure executor has an active DB connection.
func (e *Executor) ensureConnection(ctx context.Context) error {
	if e.conn != nil {
		return nil
	}

	if err := e.Connect(); err != nil {
		return err
	}

	logutil.Info("cdc.executor.reconnected",
		zap.String("ip", e.ip),
		zap.Int("port", e.port))
	return nil
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
		"cdc.executor.sql_failed",
		zap.Error(err),
		zap.String("sql", string(sqlBuf[v2SQLBufReserved:maxLen])),
	)

	// Log full transaction if recording is enabled
	if e.debugTxnRecorder.doRecord && len(e.debugTxnRecorder.txnSQL) > 0 {
		logutil.Error(
			"cdc.executor.transaction_history",
			zap.Strings("txn_sql", e.debugTxnRecorder.txnSQL),
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
