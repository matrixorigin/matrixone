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

package publication

import (
	"context"
	sql "database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/cdc/retry"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/mysql"
	"go.uber.org/zap"
)

// getParameterUnitWrapper is a wrapper function to get ParameterUnit from cnUUID
// Similar to getGlobalPuWrapper in pkg/frontend/cdc_util.go
// This can be set by the caller to provide a way to get ParameterUnit
var getParameterUnitWrapper func(cnUUID string) *config.ParameterUnit

// SetGetParameterUnitWrapper sets the wrapper function to get ParameterUnit from cnUUID
// This should be called during initialization to provide a way to get ParameterUnit
func SetGetParameterUnitWrapper(fn func(cnUUID string) *config.ParameterUnit) {
	getParameterUnitWrapper = fn
}

// Result wraps sql.Rows or InternalResult to provide query result access
type Result struct {
	rows           *sql.Rows
	internalResult *InternalResult
}

// Close closes the result rows
func (r *Result) Close() error {
	if r.rows != nil {
		return r.rows.Close()
	}
	if r.internalResult != nil {
		return r.internalResult.Close()
	}
	return nil
}

// Next moves to the next row
func (r *Result) Next() bool {
	if r.rows != nil {
		return r.rows.Next()
	}
	if r.internalResult != nil {
		return r.internalResult.Next()
	}
	return false
}

// Scan scans the current row into the provided destinations
func (r *Result) Scan(dest ...interface{}) error {
	if r.rows != nil {
		// When using sql.Rows, we need to handle types.TS specially
		// because the SQL driver doesn't know how to scan directly into types.TS
		// We'll scan into temporary []byte buffers and then unmarshal
		tsIndices := make([]int, 0)     // Track which indices are TS fields
		tsBuffers := make([]*[]byte, 0) // Store pointers to buffers
		scanDest := make([]interface{}, len(dest))

		for i, d := range dest {
			if _, ok := d.(*types.TS); ok {
				// Create a temporary buffer for this TS field
				buf := make([]byte, types.TxnTsSize)
				tsBuffers = append(tsBuffers, &buf)
				tsIndices = append(tsIndices, i)
				scanDest[i] = &buf
			} else {
				scanDest[i] = d
			}
		}

		// Scan into the destinations (which may include temporary buffers)
		if err := r.rows.Scan(scanDest...); err != nil {
			return err
		}

		// Unmarshal the TS buffers into the actual TS destinations
		for bufIdx, destIdx := range tsIndices {
			if tsDest, ok := dest[destIdx].(*types.TS); ok {
				if err := tsDest.Unmarshal(*tsBuffers[bufIdx]); err != nil {
					return moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to unmarshal TS at column %d: %v", destIdx, err))
				}
			}
		}

		return nil
	}
	if r.internalResult != nil {
		return r.internalResult.Scan(dest...)
	}
	return moerr.NewInternalErrorNoCtx("result is nil")
}

// Columns returns the column names
func (r *Result) Columns() ([]string, error) {
	if r.rows != nil {
		return r.rows.Columns()
	}
	if r.internalResult != nil {
		return r.internalResult.Columns()
	}
	return nil, moerr.NewInternalErrorNoCtx("result is nil")
}

// Err returns any error encountered during iteration
func (r *Result) Err() error {
	if r.rows != nil {
		return r.rows.Err()
	}
	if r.internalResult != nil {
		return r.internalResult.Err()
	}
	return nil
}

type SQLExecutor interface {
	Close() error
	Connect() error
	StartTxn(ctx context.Context) error
	EndTxn(ctx context.Context, commit bool) error
	ExecSQL(ctx context.Context, query string) (*Result, error)
	ExecSQLWithOptions(ctx context.Context, ar *ActiveRoutine, query string, needRetry bool) (*Result, error)
	HasActiveTx() bool
}

var _ SQLExecutor = (*UpstreamExecutor)(nil)

// UpstreamExecutor manages database connection, transaction lifecycle, and SQL execution
// for upstream MatrixOne cluster operations.
type UpstreamExecutor struct {
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
}

// circuitBreaker implements circuit breaker pattern for upstream connections
type circuitBreaker struct {
	sinkLabel    string
	maxFailures  int
	coolDown     time.Duration
	failureCount int
	open         bool
	openedAt     time.Time
	mu           sync.Mutex
}

const (
	defaultCircuitBreakerFailures = 5
	defaultCircuitBreakerCooldown = 30 * time.Second
)

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
		logutil.Info("publication.executor.retry_circuit_half_open",
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
		logutil.Info("publication.executor.retry_circuit_closed",
			zap.String("sink", cb.sinkLabel))
	}
}

// NewUpstreamExecutor creates a new UpstreamExecutor with database connection
func NewUpstreamExecutor(
	user, password string,
	ip string,
	port int,
	retryTimes int,
	retryDuration time.Duration,
	timeout string,
) (*UpstreamExecutor, error) {
	e := &UpstreamExecutor{
		user:          user,
		password:      password,
		ip:            ip,
		port:          port,
		retryTimes:    retryTimes,
		retryDuration: retryDuration,
		timeout:       timeout,
		sinkLabel:     "upstream",
	}

	if err := e.Connect(); err != nil {
		return nil, err
	}

	e.initRetryPolicy()

	return e, nil
}

// Connect establishes a database connection
func (e *UpstreamExecutor) Connect() error {
	conn, err := openDbConn(e.user, e.password, e.ip, e.port, e.timeout)
	if err != nil {
		return err
	}
	e.conn = conn
	return nil
}

// UpstreamConnConfig represents parsed upstream connection configuration
type UpstreamConnConfig struct {
	User     string
	Password string
	Host     string
	Port     int
	Timeout  string
}

// tryDecryptPassword attempts to decrypt password if it appears to be encrypted
// Returns decrypted password if successful, original password otherwise
func tryDecryptPassword(ctx context.Context, encryptedPassword string, executor SQLExecutor, cnUUID string) string {
	// Check if password looks like encrypted (hex string, typically longer than 32 chars for AES CFB)
	if len(encryptedPassword) < 32 {
		return encryptedPassword // Too short to be encrypted
	}

	// Try to decode as hex to check if it's valid hex
	_, err := hex.DecodeString(encryptedPassword)
	if err != nil {
		return encryptedPassword // Not valid hex, assume plaintext
	}

	// Try to initialize AES key and decrypt
	if executor != nil && cnUUID != "" {
		// Initialize AES key if not already initialized
		if len(cdc.AesKey) == 0 {
			if err := initAesKeyForPublication(ctx, executor, cnUUID); err != nil {
				logutil.Warn("failed to initialize AES key for password decryption, using password as-is",
					zap.Error(err))
				return encryptedPassword
			}
		}

		// Try to decrypt
		decrypted, err := cdc.AesCFBDecode(ctx, encryptedPassword)
		if err != nil {
			logutil.Warn("failed to decrypt password, using password as-is",
				zap.Error(err))
			return encryptedPassword
		}
		return decrypted
	}

	return encryptedPassword
}

// initAesKeyForPublication initializes AES key for password decryption
// Similar to initAesKeyBySqlExecutor in pkg/frontend/cdc_exector.go
// Uses cnUUID to get ParameterUnit via getParameterUnitWrapper (similar to CDC's getGlobalPuWrapper)
func initAesKeyForPublication(ctx context.Context, executor SQLExecutor, cnUUID string) error {
	if len(cdc.AesKey) > 0 {
		return nil // Already initialized
	}

	// Query the data key from mo_data_key table
	querySQL := cdc.CDCSQLBuilder.GetDataKeySQL(uint64(catalog.System_Account), cdc.InitKeyId)
	systemCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	result, err := executor.ExecSQL(systemCtx, querySQL)
	if err != nil {
		return err
	}
	defer result.Close()

	var encryptedKey string
	if result.Next() {
		if err := result.Scan(&encryptedKey); err != nil {
			return err
		}
	} else {
		return moerr.NewInternalError(ctx, "no data key found")
	}

	// Get KeyEncryptionKey using getParameterUnitWrapper (similar to CDC)
	// First try getParameterUnitWrapper if available
	var pu *config.ParameterUnit
	if getParameterUnitWrapper != nil {
		pu = getParameterUnitWrapper(cnUUID)
	}

	// Fallback to context if wrapper is not available or returned nil
	if pu == nil {
		puValue := ctx.Value(config.ParameterUnitKey)
		if puValue != nil {
			if puPtr, ok := puValue.(*config.ParameterUnit); ok && puPtr != nil {
				pu = puPtr
			}
		}
	}

	if pu == nil || pu.SV == nil {
		return moerr.NewInternalError(ctx, "ParameterUnit not available")
	}

	// Decrypt the data key using KeyEncryptionKey
	cdc.AesKey, err = cdc.AesCFBDecodeWithKey(
		ctx,
		encryptedKey,
		[]byte(pu.SV.KeyEncryptionKey),
	)
	return err
}

// ParseUpstreamConn parses upstream connection string
// Format: mysql://account#user:password@host:port (unified format, account may be empty)
// If password appears to be encrypted (hex string), it will be decrypted if ctx and executor are provided
// KeyEncryptionKey will be read from context (similar to CDC)
func ParseUpstreamConn(connStr string) (*UpstreamConnConfig, error) {
	return ParseUpstreamConnWithDecrypt(context.Background(), connStr, nil, "")
}

// ParseUpstreamConnWithDecrypt parses upstream connection string with optional password decryption
// Format: mysql://account#user:password@host:port (account may be empty, i.e., mysql://#user:password@host:port)
// If executor and cnUUID are provided, encrypted passwords will be decrypted using KeyEncryptionKey
// Similar to CDC's implementation using getGlobalPuWrapper
func ParseUpstreamConnWithDecrypt(ctx context.Context, connStr string, executor SQLExecutor, cnUUID string) (*UpstreamConnConfig, error) {
	if connStr == "" {
		return nil, moerr.NewInternalErrorNoCtx("upstream connection string is empty")
	}

	// Must start with mysql://
	if !strings.HasPrefix(connStr, "mysql://") {
		return nil, moerr.NewInternalErrorNoCtx("invalid connection string format, expected mysql://account#user:password@host:port")
	}

	connStr = strings.TrimPrefix(connStr, "mysql://")

	// Split by @ to separate user:password and host:port
	parts := strings.Split(connStr, "@")
	if len(parts) != 2 {
		return nil, moerr.NewInternalErrorNoCtx("invalid connection string format, expected mysql://account#user:password@host:port")
	}

	// Parse user:password part (must contain account# prefix)
	userPass := strings.Split(parts[0], ":")
	if len(userPass) < 2 {
		return nil, moerr.NewInternalErrorNoCtx("invalid user:password format, expected account#user:password")
	}

	// Handle account#user format (account may be empty)
	userPart := userPass[0]
	if !strings.Contains(userPart, "#") {
		return nil, moerr.NewInternalErrorNoCtx("invalid format, expected account#user prefix")
	}

	idx := strings.Index(userPart, "#")
	if idx < 0 || idx == len(userPart)-1 {
		return nil, moerr.NewInternalErrorNoCtx("invalid format, user cannot be empty after account#")
	}

	// Skip account part, only use user (account may be empty string)
	user := userPart[idx+1:]
	if user == "" {
		return nil, moerr.NewInternalErrorNoCtx("invalid format, user cannot be empty")
	}

	// Join remaining parts as password (in case password contains ':')
	password := strings.Join(userPass[1:], ":")
	if password == "" {
		return nil, moerr.NewInternalErrorNoCtx("invalid format, password cannot be empty")
	}

	// Parse host:port part
	addrPart := parts[1]
	// Remove any query parameters after /
	slashIdx := strings.Index(addrPart, "/")
	if slashIdx != -1 {
		addrPart = addrPart[:slashIdx]
	}

	hostPortParts := strings.Split(addrPart, ":")
	if len(hostPortParts) != 2 {
		return nil, moerr.NewInternalErrorNoCtx("invalid host:port format")
	}

	host := hostPortParts[0]
	if host == "" {
		return nil, moerr.NewInternalErrorNoCtx("invalid format, host cannot be empty")
	}

	port, err := strconv.Atoi(hostPortParts[1])
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid port: %v", err))
	}

	// Try to decrypt password if it appears to be encrypted
	decryptedPassword := tryDecryptPassword(ctx, password, executor, cnUUID)

	// Default timeout
	timeout := "10s"

	return &UpstreamConnConfig{
		User:     user,
		Password: decryptedPassword,
		Host:     host,
		Port:     port,
		Timeout:  timeout,
	}, nil
}

// openDbConn opens a database connection (similar to cdc.OpenDbConn)
func openDbConn(user, password string, ip string, port int, timeout string) (*sql.DB, error) {
	logutil.Info("publication.executor.open_db_conn", zap.String("timeout", timeout))
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?readTimeout=%s&timeout=%s&writeTimeout=%s&multiStatements=true",
		user, password, ip, port, timeout, timeout, timeout)

	var db *sql.DB
	var err error
	for i := 0; i < 3; i++ {
		if db, err = tryConn(dsn); err == nil {
			return db, nil
		}
		time.Sleep(time.Second)
	}
	logutil.Error("publication.executor.open_db_conn_failed", zap.Error(err))
	return nil, err
}

func tryConn(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql-mo", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	time.Sleep(time.Millisecond * 100)

	// ping opens the connection
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// StartTxn starts a new transaction (implements UpstreamExecutor interface)
func (e *UpstreamExecutor) StartTxn(ctx context.Context) error {
	if e.tx != nil {
		return moerr.NewInternalError(ctx, "transaction already active")
	}

	if err := e.ensureConnection(ctx); err != nil {
		return err
	}

	tx, err := e.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	e.tx = tx
	return nil
}

// EndTxn ends the current transaction (implements UpstreamExecutor interface)
func (e *UpstreamExecutor) EndTxn(ctx context.Context, commit bool) error {
	if e.tx == nil {
		return nil // Idempotent
	}

	var err error
	if commit {
		err = e.tx.Commit()
	} else {
		err = e.tx.Rollback()
	}
	e.tx = nil // Always clear, even on error
	return err
}

// ExecSQL executes a SQL statement and returns the result (implements UpstreamExecutor interface)
// If a transaction is active, executes within the transaction.
// Otherwise, executes as a standalone statement.
// By default, retry is enabled. Use ExecSQLWithOptions for more control.
func (e *UpstreamExecutor) ExecSQL(ctx context.Context, query string) (*Result, error) {
	return e.ExecSQLWithOptions(ctx, nil, query, true)
}

// ExecSQLWithOptions executes a SQL statement with additional options
// If a transaction is active, executes within the transaction.
// Otherwise, executes as a standalone statement.
func (e *UpstreamExecutor) ExecSQLWithOptions(
	ctx context.Context,
	ar *ActiveRoutine,
	query string,
	needRetry bool,
) (*Result, error) {
	if err := e.ensureConnection(ctx); err != nil {
		return nil, err
	}

	execFunc := func() (*Result, error) {
		if err := e.ensureConnection(ctx); err != nil {
			return nil, err
		}

		var rows *sql.Rows
		var err error
		if e.tx != nil {
			rows, err = e.tx.QueryContext(ctx, query)
		} else {
			rows, err = e.conn.QueryContext(ctx, query)
		}

		if err != nil {
			e.logFailedSQL(err, query)
			return nil, err
		}

		return &Result{rows: rows}, nil
	}

	if !needRetry {
		return execFunc()
	}

	return e.execWithRetry(ctx, ar, execFunc)
}

// execWithRetry executes a function with retry logic
func (e *UpstreamExecutor) execWithRetry(
	ctx context.Context,
	ar *ActiveRoutine,
	fn func() (*Result, error),
) (*Result, error) {
	sinkLabel := e.sinkLabel
	if sinkLabel == "" {
		sinkLabel = "upstream"
	}

	if e.circuitBreaker != nil && e.circuitBreaker.IsOpen() {
		logutil.Warn(
			"publication.executor.retry_circuit_blocked",
			zap.String("sink", sinkLabel),
		)
		return nil, moerr.NewInternalError(ctx, "upstream circuit breaker open")
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
	var lastResult *Result

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
		result, err := fn()
		_ = begin // TODO: add metrics if needed
		if err == nil {
			if e.circuitBreaker != nil {
				e.circuitBreaker.OnSuccess()
			}
			lastErr = nil
			lastResult = result
			return nil
		}

		reason := classifyRetryReason(err)
		lastErr = err

		logutil.Error(
			"publication.executor.retry_failed",
			zap.Int("attempt", attempt),
			zap.Error(err),
			zap.String("reason", reason),
		)

		if e.circuitBreaker != nil {
			if opened, justOpened := e.circuitBreaker.OnFailure(); opened {
				if justOpened {
					logutil.Warn("publication.executor.retry_circuit_opened",
						zap.String("sink", sinkLabel),
						zap.Int("attempt", attempt),
						zap.String("reason", reason),
					)
				}
				return retry.ErrCircuitOpen
			}
		}

		return err
	})

	if err == nil {
		if attempt > 1 && lastErr != nil {
			reason := classifyRetryReason(lastErr)
			logutil.Info(
				"publication.executor.retry_success",
				zap.Int("attempts", attempt),
				zap.Duration("total-duration", time.Since(start)),
				zap.String("reason", reason),
			)
		}
		return lastResult, nil
	}

	if errors.Is(err, retry.ErrCircuitOpen) {
		return nil, moerr.NewInternalError(ctx, "upstream circuit breaker open")
	}

	if errors.Is(err, retry.ErrNonRetryable) {
		reason := "duration_limit"
		if lastErr != nil {
			reason = classifyRetryReason(lastErr)
		}
		logutil.Error(
			"publication.executor.retry_exhausted",
			zap.Int("attempts", attempt),
			zap.Duration("total-duration", time.Since(start)),
			zap.String("reason", reason),
			zap.Error(lastErr),
		)
		return nil, moerr.NewInternalError(ctx, "retry limit exceeded")
	}

	if lastErr != nil {
		return nil, lastErr
	}

	return nil, err
}

func (e *UpstreamExecutor) initRetryPolicy() {
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
		e.sinkLabel = "upstream"
	}
	e.circuitBreaker = newCircuitBreaker(e.sinkLabel, defaultCircuitBreakerFailures, defaultCircuitBreakerCooldown)
}

func (e *UpstreamExecutor) calculateMaxAttempts() int {
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
func (e *UpstreamExecutor) HasActiveTx() bool {
	return e.tx != nil
}

// Close closes the database connection and rolls back any active transaction
func (e *UpstreamExecutor) Close() error {
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

	return nil
}

// ensureConnection makes sure executor has an active DB connection
func (e *UpstreamExecutor) ensureConnection(ctx context.Context) error {
	if e.conn != nil {
		return nil
	}

	if err := e.Connect(); err != nil {
		return err
	}

	logutil.Info("publication.executor.reconnected",
		zap.String("ip", e.ip),
		zap.Int("port", e.port))
	return nil
}

// logFailedSQL logs failed SQL statement
func (e *UpstreamExecutor) logFailedSQL(err error, query string) {
	const maxSQLPrintLen = 200
	sqlToLog := query
	if len(sqlToLog) > maxSQLPrintLen {
		sqlToLog = sqlToLog[:maxSQLPrintLen] + "..."
	}
	logutil.Error(
		"publication.executor.sql_failed",
		zap.Error(err),
		zap.String("sql", sqlToLog),
	)
}

// ActiveRoutine represents an active routine that can be paused or cancelled
type ActiveRoutine struct {
	sync.Mutex
	Pause  chan struct{}
	Cancel chan struct{}
}

// NewActiveRoutine creates a new ActiveRoutine
func NewActiveRoutine() *ActiveRoutine {
	return &ActiveRoutine{
		Pause:  make(chan struct{}),
		Cancel: make(chan struct{}),
	}
}

// ClosePause closes the pause channel
func (ar *ActiveRoutine) ClosePause() {
	ar.Lock()
	defer ar.Unlock()
	close(ar.Pause)
}

// CloseCancel closes the cancel channel
func (ar *ActiveRoutine) CloseCancel() {
	ar.Lock()
	defer ar.Unlock()
	close(ar.Cancel)
}
