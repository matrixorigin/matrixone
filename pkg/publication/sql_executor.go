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
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// getParameterUnitWrapper is a wrapper function to get ParameterUnit from cnUUID
// Similar to getGlobalPuWrapper in pkg/frontend/cdc_util.go
// This can be set by the caller to provide a way to get ParameterUnit
var (
	getParameterUnitWrapper   func(cnUUID string) *config.ParameterUnit
	getParameterUnitWrapperMu sync.RWMutex
)

// SetGetParameterUnitWrapper sets the wrapper function to get ParameterUnit from cnUUID
// This should be called during initialization to provide a way to get ParameterUnit
func SetGetParameterUnitWrapper(fn func(cnUUID string) *config.ParameterUnit) {
	getParameterUnitWrapperMu.Lock()
	defer getParameterUnitWrapperMu.Unlock()
	getParameterUnitWrapper = fn
}

// MockResultScanner is an interface for testing purposes
type MockResultScanner interface {
	Close() error
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
}

// Result wraps sql.Rows or InternalResult to provide query result access
type Result struct {
	rows           *sql.Rows
	internalResult *InternalResult
	mockResult     MockResultScanner // For testing only
}

// Close closes the result rows
func (r *Result) Close() error {
	if r.mockResult != nil {
		return r.mockResult.Close()
	}
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
	if r.mockResult != nil {
		return r.mockResult.Next()
	}
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
	if r.mockResult != nil {
		return r.mockResult.Scan(dest...)
	}
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
				buf := *tsBuffers[bufIdx]
				// Handle NULL values: if buffer is nil, empty, or has insufficient length,
				// set TS to empty value instead of unmarshaling
				if buf == nil || len(buf) < types.TxnTsSize {
					*tsDest = types.TS{}
				} else {
					if err := tsDest.Unmarshal(buf); err != nil {
						return moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to unmarshal TS at column %d: %v", destIdx, err))
					}
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

// Err returns any error encountered during iteration
func (r *Result) Err() error {
	if r.mockResult != nil {
		return r.mockResult.Err()
	}
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
	EndTxn(ctx context.Context, commit bool) error
	// ExecSQL executes a SQL statement with options
	// useTxn: if true, execute within a transaction (must have active transaction for InternalSQLExecutor, not allowed for UpstreamExecutor)
	// if false, execute as autocommit (will create and commit transaction automatically for InternalSQLExecutor)
	ExecSQL(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool) (*Result, error)
}

var _ SQLExecutor = (*UpstreamExecutor)(nil)

// UpstreamExecutor manages database connection, transaction lifecycle, and SQL execution
// for upstream MatrixOne cluster operations.
type UpstreamExecutor struct {
	conn *sql.DB
	tx   *sql.Tx

	// Connection info (for reconnection)
	account, user, password string
	ip                      string
	port                    int
	timeout                 string

	// Retry configuration
	retryTimes    int           // -1 for infinite retry
	retryDuration time.Duration // Max total retry duration

	retryPolicy     Policy
	retryClassifier ErrorClassifier
}

// NewUpstreamExecutor creates a new UpstreamExecutor with database connection
func NewUpstreamExecutor(
	account, user, password string,
	ip string,
	port int,
	retryTimes int,
	retryDuration time.Duration,
	timeout string,
	classifier ErrorClassifier,
) (*UpstreamExecutor, error) {
	// Validate that user is not empty
	if user == "" {
		return nil, moerr.NewInternalErrorNoCtx("user cannot be empty when creating upstream executor")
	}
	// If account is provided, ensure it's not empty (empty account means standard MySQL format)
	// If account is not empty but user is empty, that's invalid
	if account != "" && user == "" {
		return nil, moerr.NewInternalErrorNoCtx("user cannot be empty when account is provided")
	}

	e := &UpstreamExecutor{
		account:       account,
		user:          user,
		password:      password,
		ip:            ip,
		port:          port,
		retryTimes:    retryTimes,
		retryDuration: retryDuration,
		timeout:       timeout,
	}

	if err := e.Connect(); err != nil {
		return nil, err
	}

	e.initRetryPolicy(classifier)

	return e, nil
}

// Connect establishes a database connection
func (e *UpstreamExecutor) Connect() error {
	conn, err := openDbConn(e.account, e.user, e.password, e.ip, e.port, e.timeout)
	if err != nil {
		return err
	}
	e.conn = conn
	return nil
}

// UpstreamConnConfig represents parsed upstream connection configuration
type UpstreamConnConfig struct {
	Account  string // Account name (may be empty)
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
	result, err := executor.ExecSQL(systemCtx, nil, querySQL, false, false)
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
	getParameterUnitWrapperMu.RLock()
	wrapper := getParameterUnitWrapper
	getParameterUnitWrapperMu.RUnlock()
	if wrapper != nil {
		pu = wrapper(cnUUID)
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
// Format: mysql://account#user:password@host:port or mysql://user:password@host:port
// If account is provided, uses account#user format for MatrixOne authentication (in DSN)
// If account is not provided (standard MySQL format), account will be empty and uses user format
// If executor and cnUUID are provided, encrypted passwords will be decrypted using KeyEncryptionKey
// Similar to CDC's implementation using getGlobalPuWrapper
func ParseUpstreamConnWithDecrypt(ctx context.Context, connStr string, executor SQLExecutor, cnUUID string) (*UpstreamConnConfig, error) {
	if connStr == "" {
		return nil, moerr.NewInternalErrorNoCtx("upstream connection string is empty")
	}

	// Must start with mysql://
	if !strings.HasPrefix(connStr, "mysql://") {
		return nil, moerr.NewInternalErrorNoCtx("invalid connection string format, expected mysql://account#user:password@host:port or mysql://user:password@host:port")
	}

	connStr = strings.TrimPrefix(connStr, "mysql://")

	// Split by @ to separate user:password and host:port
	parts := strings.Split(connStr, "@")
	if len(parts) != 2 {
		return nil, moerr.NewInternalErrorNoCtx("invalid connection string format, expected mysql://account#user:password@host:port or mysql://user:password@host:port")
	}

	// Parse user:password part
	userPass := strings.Split(parts[0], ":")
	if len(userPass) < 2 {
		return nil, moerr.NewInternalErrorNoCtx("invalid user:password format, expected account#user:password or user:password")
	}

	// Handle account#user format or standard user format
	userPart := userPass[0]
	var account string
	var user string

	if strings.Contains(userPart, "#") {
		// Format: account#user
		idx := strings.Index(userPart, "#")
		if idx < 0 || idx == len(userPart)-1 {
			return nil, moerr.NewInternalErrorNoCtx("invalid format, user cannot be empty after account#")
		}
		account = userPart[:idx]
		user = userPart[idx+1:]
		if user == "" {
			return nil, moerr.NewInternalErrorNoCtx("invalid format, user cannot be empty")
		}
	} else {
		// Format: user (standard MySQL format, no account)
		account = ""
		user = userPart
		if user == "" {
			return nil, moerr.NewInternalErrorNoCtx("invalid format, user cannot be empty")
		}
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

	// Log parsed connection config for debugging
	logutil.Info("publication.executor.parse_upstream_conn",
		zap.String("account", account),
		zap.String("user", user),
		zap.String("host", host),
		zap.Int("port", port),
		zap.Bool("password_encrypted", len(decryptedPassword) != len(password)))

	return &UpstreamConnConfig{
		Account:  account,
		User:     user,
		Password: decryptedPassword,
		Host:     host,
		Port:     port,
		Timeout:  timeout,
	}, nil
}

// openDbConn opens a database connection (similar to cdc.OpenDbConn)
// If account is provided, uses account#user format for MatrixOne authentication
func openDbConn(account, user, password string, ip string, port int, timeout string) (*sql.DB, error) {
	logutil.Info("publication.executor.open_db_conn",
		zap.String("account", account),
		zap.String("user", user),
		zap.String("timeout", timeout),
		zap.String("host", ip),
		zap.Int("port", port))
	// Build user string: if account is provided, use account#user format (MatrixOne uses # as separator)
	userStr := user
	if account != "" {
		if user == "" {
			logutil.Error("publication.executor.open_db_conn_invalid",
				zap.String("error", "account is provided but user is empty"),
				zap.String("account", account))
			return nil, moerr.NewInternalErrorNoCtx("account is provided but user is empty, cannot build connection string")
		}
		userStr = fmt.Sprintf("%s#%s", account, user)
	} else if user == "" {
		logutil.Error("publication.executor.open_db_conn_invalid",
			zap.String("error", "both account and user are empty"))
		return nil, moerr.NewInternalErrorNoCtx("user cannot be empty")
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?readTimeout=%s&timeout=%s&writeTimeout=%s&multiStatements=true",
		userStr, password, ip, port, timeout, timeout, timeout)
	logutil.Info("publication.executor.open_db_conn_dsn", zap.String("dsn_user", userStr), zap.String("host", ip), zap.Int("port", port))

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

// ExecSQL executes a SQL statement with options
// useTxn: must be false for UpstreamExecutor (transaction not supported, will error if true)
// UpstreamExecutor always uses connection-level autocommit (no explicit transactions)
func (e *UpstreamExecutor) ExecSQL(
	ctx context.Context,
	ar *ActiveRoutine,
	query string,
	useTxn bool,
	needRetry bool,
) (*Result, error) {
	// UpstreamExecutor does not support explicit transactions
	if useTxn {
		return nil, moerr.NewInternalError(ctx, "UpstreamExecutor does not support transactions. Use useTxn=false")
	}
	if err := e.ensureConnection(ctx); err != nil {
		return nil, err
	}

	execFunc := func() (*Result, error) {
		if err := e.ensureConnection(ctx); err != nil {
			return nil, err
		}

		// UpstreamExecutor always uses connection (autocommit), not transaction
		var rows *sql.Rows
		var err error
		rows, err = e.conn.QueryContext(ctx, query)
		if err != nil {
			e.logFailedSQL(err, query)
			return nil, err
		}
		err = rows.Err()
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
			return ErrNonRetryable
		}

		begin := time.Now()
		result, err := fn()
		_ = begin // TODO: add metrics if needed
		if err == nil {
			lastErr = nil
			lastResult = result
			return nil
		}

		lastErr = err

		logutil.Error(
			"publication.executor.retry_failed",
			zap.Int("attempt", attempt),
			zap.Error(err),
		)

		return err
	})

	if err == nil {
		if attempt > 1 && lastErr != nil {
			logutil.Info(
				"publication.executor.retry_success",
				zap.Int("attempts", attempt),
				zap.Duration("total-duration", time.Since(start)),
				zap.Error(lastErr),
			)
		}
		return lastResult, nil
	}

	if errors.Is(err, ErrNonRetryable) {
		logutil.Error(
			"publication.executor.retry_exhausted",
			zap.Int("attempts", attempt),
			zap.Duration("total-duration", time.Since(start)),
			zap.Error(lastErr),
		)
		return nil, moerr.NewInternalError(ctx, "retry limit exceeded")
	}

	if lastErr != nil {
		return nil, lastErr
	}

	return nil, err
}

func (e *UpstreamExecutor) initRetryPolicy(classifier ErrorClassifier) {
	e.retryClassifier = classifier
	e.retryPolicy = Policy{
		MaxAttempts: e.calculateMaxAttempts(),
		Backoff: ExponentialBackoff{
			Base:   200 * time.Millisecond,
			Factor: 2,
			Max:    30 * time.Second,
			Jitter: 200 * time.Millisecond,
		},
		Classifier: classifier,
	}
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
