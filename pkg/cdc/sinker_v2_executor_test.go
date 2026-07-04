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
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeMySQLServer struct {
	listener net.Listener
	queries  chan string
	errs     chan error
	wg       sync.WaitGroup
}

func startFakeMySQLServer(t *testing.T) *fakeMySQLServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := &fakeMySQLServer{
		listener: listener,
		queries:  make(chan string, 4),
		errs:     make(chan error, 1),
	}
	server.wg.Add(1)
	go server.serve()

	t.Cleanup(func() {
		_ = listener.Close()
		server.wg.Wait()
	})

	return server
}

func (s *fakeMySQLServer) addr(t *testing.T) (string, int) {
	t.Helper()

	host, portStr, err := net.SplitHostPort(s.listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return host, port
}

func (s *fakeMySQLServer) serve() {
	defer s.wg.Done()

	conn, err := s.listener.Accept()
	if err != nil {
		if !errorsIsNetClosed(err) {
			s.reportErr(err)
		}
		return
	}
	defer conn.Close()

	if err := writeMySQLPacket(conn, 0, mysqlHandshakePayload()); err != nil {
		s.reportErr(err)
		return
	}
	if _, _, err := readMySQLPacket(conn); err != nil {
		s.reportErr(err)
		return
	}
	if err := writeMySQLOK(conn, 2); err != nil {
		s.reportErr(err)
		return
	}

	for {
		_, payload, err := readMySQLPacket(conn)
		if err != nil {
			if err != io.EOF && !errorsIsNetClosed(err) {
				s.reportErr(err)
			}
			return
		}
		if len(payload) == 0 {
			continue
		}

		switch payload[0] {
		case 0x01: // COM_QUIT
			return
		case 0x03: // COM_QUERY
			s.queries <- string(payload[1:])
			if err := writeMySQLOK(conn, 1); err != nil {
				s.reportErr(err)
				return
			}
		case 0x0e: // COM_PING
			if err := writeMySQLOK(conn, 1); err != nil {
				s.reportErr(err)
				return
			}
		default:
			s.reportErr(io.ErrUnexpectedEOF)
			return
		}
	}
}

func (s *fakeMySQLServer) reportErr(err error) {
	select {
	case s.errs <- err:
	default:
	}
}

func errorsIsNetClosed(err error) bool {
	return errors.Is(err, net.ErrClosed) || err.Error() == "use of closed network connection"
}

func mysqlHandshakePayload() []byte {
	const (
		clientLongPassword    uint32 = 1 << 0
		clientLongFlag        uint32 = 1 << 2
		clientProtocol41      uint32 = 1 << 9
		clientTransactions    uint32 = 1 << 13
		clientSecureConn      uint32 = 1 << 15
		clientMultiStatements uint32 = 1 << 16
		clientPluginAuth      uint32 = 1 << 19
	)

	caps := clientLongPassword | clientLongFlag | clientProtocol41 |
		clientTransactions | clientSecureConn | clientMultiStatements | clientPluginAuth
	authData := []byte("12345678abcdefghijklmnop")

	payload := []byte{0x0a}
	payload = append(payload, []byte("5.7.0-cdc-test")...)
	payload = append(payload, 0x00)
	payload = binary.LittleEndian.AppendUint32(payload, 1)
	payload = append(payload, authData[:8]...)
	payload = append(payload, 0x00)
	payload = binary.LittleEndian.AppendUint16(payload, uint16(caps))
	payload = append(payload, 0x21)
	payload = binary.LittleEndian.AppendUint16(payload, 0x0002)
	payload = binary.LittleEndian.AppendUint16(payload, uint16(caps>>16))
	payload = append(payload, 21)
	payload = append(payload, make([]byte, 10)...)
	payload = append(payload, authData[8:21]...)
	payload = append(payload, 0x00)
	payload = append(payload, []byte("mysql_native_password")...)
	payload = append(payload, 0x00)
	return payload
}

func readMySQLPacket(conn net.Conn) (byte, []byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, err
	}

	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, nil, err
	}
	return header[3], payload, nil
}

func writeMySQLPacket(conn net.Conn, sequence byte, payload []byte) error {
	header := []byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), sequence}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func writeMySQLOK(conn net.Conn, sequence byte) error {
	return writeMySQLPacket(conn, sequence, []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00})
}

func TestExecutor_BeginTx(t *testing.T) {
	t.Run("SuccessfulBegin", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		mock.ExpectBegin()

		ctx := context.Background()
		err = executor.BeginTx(ctx)

		assert.NoError(t, err)
		assert.NotNil(t, executor.tx, "Transaction should be active")
		assert.True(t, executor.HasActiveTx())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("BeginTxFails", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		expectedErr := sqlmock.ErrCancelled
		mock.ExpectBegin().WillReturnError(expectedErr)

		ctx := context.Background()
		err = executor.BeginTx(ctx)

		assert.Error(t, err)
		assert.Nil(t, executor.tx, "Transaction should not be active on failure")
		assert.False(t, executor.HasActiveTx())
	})

	t.Run("BeginTxWhenAlreadyActive", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start first transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Try to start second transaction
		err = executor.BeginTx(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already active")
		assert.NotNil(t, executor.tx, "First transaction should still be active")
	})
}

func TestExecutor_CommitTx(t *testing.T) {
	t.Run("SuccessfulCommit", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Commit transaction
		mock.ExpectCommit()
		err = executor.CommitTx(ctx)

		assert.NoError(t, err)
		assert.Nil(t, executor.tx, "Transaction should be nil after commit")
		assert.False(t, executor.HasActiveTx())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("CommitTxFails", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Commit fails
		expectedErr := sqlmock.ErrCancelled
		mock.ExpectCommit().WillReturnError(expectedErr)
		err = executor.CommitTx(ctx)

		assert.Error(t, err)
		assert.Nil(t, executor.tx, "Transaction should be nil even after failed commit")
		assert.False(t, executor.HasActiveTx())
	})

	t.Run("CommitTxWhenNoTransaction_Idempotent", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
			tx:   nil, // No active transaction
		}

		ctx := context.Background()
		err = executor.CommitTx(ctx)

		// Should not error - idempotent behavior
		assert.NoError(t, err)
		assert.Nil(t, executor.tx)
	})
}

func TestExecutor_RollbackTx(t *testing.T) {
	t.Run("SuccessfulRollback", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Rollback transaction
		mock.ExpectRollback()
		err = executor.RollbackTx(ctx)

		assert.NoError(t, err)
		assert.Nil(t, executor.tx, "Transaction should be nil after rollback")
		assert.False(t, executor.HasActiveTx())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("RollbackTxFails", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Rollback fails
		expectedErr := sqlmock.ErrCancelled
		mock.ExpectRollback().WillReturnError(expectedErr)
		err = executor.RollbackTx(ctx)

		assert.Error(t, err)
		assert.Nil(t, executor.tx, "Transaction should be nil even after failed rollback")
		assert.False(t, executor.HasActiveTx())
	})

	t.Run("RollbackTxWhenNoTransaction_Idempotent", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
			tx:   nil, // No active transaction
		}

		ctx := context.Background()
		err = executor.RollbackTx(ctx)

		// Should not error - idempotent behavior
		assert.NoError(t, err)
		assert.Nil(t, executor.tx)
	})
}

func TestExecutor_execWithRetry_RetryableError(t *testing.T) {
	executor := &Executor{
		retryTimes:    2,
		retryDuration: 5 * time.Second,
	}
	executor.initRetryPolicy()

	attempts := 0
	err := executor.execWithRetry(context.Background(), nil, func() error {
		attempts++
		if attempts < 3 {
			return driver.ErrBadConn
		}
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestExecutor_execWithRetry_NonRetryableError(t *testing.T) {
	executor := &Executor{
		retryTimes:    5,
		retryDuration: time.Second,
	}
	executor.initRetryPolicy()

	attempts := 0
	expectedErr := moerr.NewInternalErrorNoCtx("permanent failure")

	err := executor.execWithRetry(context.Background(), nil, func() error {
		attempts++
		return expectedErr
	})

	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, 1, attempts)
}

func TestExecutor_execWithRetry_DurationLimit(t *testing.T) {
	executor := &Executor{
		retryTimes:    -1,
		retryDuration: 10 * time.Millisecond,
	}
	executor.initRetryPolicy()

	attempts := 0
	err := executor.execWithRetry(context.Background(), nil, func() error {
		attempts++
		time.Sleep(5 * time.Millisecond)
		return driver.ErrBadConn
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "retry limit exceeded")
	require.GreaterOrEqual(t, attempts, 1)
}

func TestExecutor_execWithRetry_CircuitBreakerOpens(t *testing.T) {
	executor := &Executor{
		retryTimes:    5,
		retryDuration: time.Second,
		sinkLabel:     "mysql",
	}
	executor.initRetryPolicy()
	executor.circuitBreaker.maxFailures = 1
	executor.circuitBreaker.coolDown = 50 * time.Millisecond

	v2.CdcSinkerRetryCounter.Reset()
	v2.CdcSinkerCircuitStateGauge.Reset()

	attempts := 0
	err := executor.execWithRetry(context.Background(), nil, func() error {
		attempts++
		return driver.ErrBadConn
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "circuit breaker open")
	require.Equal(t, 1, attempts)

	require.True(t, executor.circuitBreaker.open)

	// Circuit should block immediate retries
	err = executor.execWithRetry(context.Background(), nil, func() error {
		t.Helper()
		t.Fatalf("operation should not execute when circuit is open")
		return nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "circuit breaker open")

	// After cooldown, circuit should half-open and allow attempts
	time.Sleep(60 * time.Millisecond)
	require.False(t, executor.circuitBreaker.IsOpen())
	executor.circuitBreaker.maxFailures = 2
	attempts = 0
	err = executor.execWithRetry(context.Background(), nil, func() error {
		attempts++
		if attempts < 2 {
			return driver.ErrBadConn
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.False(t, executor.circuitBreaker.IsOpen())
}

func TestExecutor_ExecSQL(t *testing.T) {
	t.Run("ExecWithinTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Execute SQL within transaction
		sqlBuf := []byte("     INSERT INTO test VALUES (1)")
		mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(1, 1))

		ar := &ActiveRoutine{
			Pause:  make(chan struct{}),
			Cancel: make(chan struct{}),
		}
		err = executor.ExecSQL(ctx, ar, sqlBuf, false)

		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ExecWithoutTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
			tx:   nil, // No transaction
		}

		// Execute SQL without transaction
		sqlBuf := []byte("     CREATE DATABASE test")
		mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(0, 0))

		ctx := context.Background()
		ar := &ActiveRoutine{
			Pause:  make(chan struct{}),
			Cancel: make(chan struct{}),
		}
		err = executor.ExecSQL(ctx, ar, sqlBuf, false)

		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ExecSQLTooShort", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// SQL buffer too short (less than 5 bytes)
		sqlBuf := []byte("ABC")

		ctx := context.Background()
		ar := &ActiveRoutine{
			Pause:  make(chan struct{}),
			Cancel: make(chan struct{}),
		}
		err = executor.ExecSQL(ctx, ar, sqlBuf, false)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too short")
	})

	t.Run("ExecSQLReestablishesConnectionWhenNil", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		stub := gostub.Stub(&OpenDbConn, func(user, password, ip string, port int, timeout string) (*sql.DB, error) {
			return db, nil
		})
		defer stub.Reset()

		executor := &Executor{
			user:          "user",
			password:      "pass",
			ip:            "127.0.0.1",
			port:          3306,
			timeout:       "5s",
			retryTimes:    0,
			retryDuration: 0,
			conn:          nil,
			tx:            nil,
		}

		sqlBuf := []byte("     INSERT INTO test VALUES (1)")
		mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(1, 1))

		ctx := context.Background()
		ar := &ActiveRoutine{
			Pause:  make(chan struct{}),
			Cancel: make(chan struct{}),
		}

		err = executor.ExecSQL(ctx, ar, sqlBuf, false)

		assert.NoError(t, err)
		assert.NotNil(t, executor.conn)
		assert.NoError(t, mock.ExpectationsWereMet())

		_ = executor.Close()
	})
}

func TestExecutor_ExecSQLAfterTryConnUsesReuseQueryBuf(t *testing.T) {
	server := startFakeMySQLServer(t)
	host, port := server.addr(t)

	cfg, err := makeMysqlConfig("user", "password", host, port, "5s")
	require.NoError(t, err)
	cfg.MaxAllowedPacket = 64 << 20

	db, err := tryConn(cfg)
	require.NoError(t, err)

	executor := &Executor{conn: db}
	defer func() {
		require.NoError(t, executor.Close())
	}()

	sqlBuf := append(make([]byte, v2SQLBufReserved), []byte("CREATE DATABASE cdc_regression")...)
	err = executor.ExecSQL(context.Background(), nil, sqlBuf, false)
	require.NoError(t, err)

	select {
	case query := <-server.queries:
		require.Equal(t, "CREATE DATABASE cdc_regression", query)
	case err := <-server.errs:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for COM_QUERY")
	}
}

func TestExecutor_Close(t *testing.T) {
	t.Run("CloseWithActiveTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Close should rollback active transaction
		mock.ExpectRollback()
		mock.ExpectClose()

		err = executor.Close()

		assert.NoError(t, err)
		assert.Nil(t, executor.tx)
		assert.Nil(t, executor.conn)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("CloseWithoutTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		executor := &Executor{
			conn: db,
			tx:   nil,
		}

		mock.ExpectClose()

		err = executor.Close()

		assert.NoError(t, err)
		assert.Nil(t, executor.conn)
	})

	t.Run("CloseWhenAlreadyClosed_Idempotent", func(t *testing.T) {
		executor := &Executor{
			conn: nil,
			tx:   nil,
		}

		err := executor.Close()

		// Should not error - idempotent
		assert.NoError(t, err)
	})
}

func TestExecutor_TransactionLifecycle(t *testing.T) {
	t.Run("CompleteTransactionLifecycle", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		ctx := context.Background()
		ar := &ActiveRoutine{
			Pause:  make(chan struct{}),
			Cancel: make(chan struct{}),
		}

		// 1. No transaction initially
		assert.False(t, executor.HasActiveTx())

		// 2. Begin transaction
		mock.ExpectBegin()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)
		assert.True(t, executor.HasActiveTx())

		// 3. Execute SQL
		sqlBuf := []byte("     INSERT INTO test VALUES (1)")
		mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(1, 1))
		err = executor.ExecSQL(ctx, ar, sqlBuf, false)
		require.NoError(t, err)
		assert.True(t, executor.HasActiveTx(), "Transaction should still be active")

		// 4. Commit transaction
		mock.ExpectCommit()
		err = executor.CommitTx(ctx)
		require.NoError(t, err)
		assert.False(t, executor.HasActiveTx())

		// 5. Can start new transaction after commit
		mock.ExpectBegin()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)
		assert.True(t, executor.HasActiveTx())

		// 6. Rollback new transaction
		mock.ExpectRollback()
		err = executor.RollbackTx(ctx)
		require.NoError(t, err)
		assert.False(t, executor.HasActiveTx())

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestExecutor_IdempotentOperations(t *testing.T) {
	t.Run("MultipleCommitsWithoutTransaction", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
			tx:   nil,
		}

		ctx := context.Background()

		// Multiple commits should all succeed (idempotent)
		err = executor.CommitTx(ctx)
		assert.NoError(t, err)

		err = executor.CommitTx(ctx)
		assert.NoError(t, err)

		err = executor.CommitTx(ctx)
		assert.NoError(t, err)
	})

	t.Run("MultipleRollbacksWithoutTransaction", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
			tx:   nil,
		}

		ctx := context.Background()

		// Multiple rollbacks should all succeed (idempotent)
		err = executor.RollbackTx(ctx)
		assert.NoError(t, err)

		err = executor.RollbackTx(ctx)
		assert.NoError(t, err)

		err = executor.RollbackTx(ctx)
		assert.NoError(t, err)
	})
}

func TestExecutor_TransactionCleanupOnError(t *testing.T) {
	t.Run("CommitFailure_ClearsTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)
		assert.NotNil(t, executor.tx)

		// Commit fails
		mock.ExpectCommit().WillReturnError(sqlmock.ErrCancelled)
		err = executor.CommitTx(ctx)

		assert.Error(t, err)
		// Critical: Transaction should be cleared even on failure
		assert.Nil(t, executor.tx, "Transaction must be cleared on commit failure")
		assert.False(t, executor.HasActiveTx())

		// Should be able to start new transaction after failed commit
		mock.ExpectBegin()
		err = executor.BeginTx(ctx)
		assert.NoError(t, err)
	})

	t.Run("RollbackFailure_ClearsTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)
		assert.NotNil(t, executor.tx)

		// Rollback fails
		mock.ExpectRollback().WillReturnError(sqlmock.ErrCancelled)
		err = executor.RollbackTx(ctx)

		assert.Error(t, err)
		// Critical: Transaction should be cleared even on failure
		assert.Nil(t, executor.tx, "Transaction must be cleared on rollback failure")
		assert.False(t, executor.HasActiveTx())

		// Should be able to start new transaction after failed rollback
		mock.ExpectBegin()
		err = executor.BeginTx(ctx)
		assert.NoError(t, err)
	})
}

func TestExecutor_RecordTxnSQL_Cap(t *testing.T) {
	executor := &Executor{}
	executor.debugTxnRecorder.doRecord = true

	// Fill up to the cap
	for i := 0; i < maxDebugTxnSQLEntries; i++ {
		sqlBuf := make([]byte, v2SQLBufReserved+10)
		copy(sqlBuf[v2SQLBufReserved:], []byte("SELECT 1;"))
		executor.recordTxnSQL(sqlBuf)
	}
	assert.Equal(t, maxDebugTxnSQLEntries, len(executor.debugTxnRecorder.txnSQL))

	// One more should be dropped
	sqlBuf := make([]byte, v2SQLBufReserved+20)
	copy(sqlBuf[v2SQLBufReserved:], []byte("SELECT overflow;"))
	executor.recordTxnSQL(sqlBuf)
	assert.Equal(t, maxDebugTxnSQLEntries, len(executor.debugTxnRecorder.txnSQL))

	t.Run("RecordDisabled", func(t *testing.T) {
		e := &Executor{}
		e.debugTxnRecorder.doRecord = false
		sqlBuf := make([]byte, v2SQLBufReserved+10)
		copy(sqlBuf[v2SQLBufReserved:], []byte("SELECT 1;"))
		e.recordTxnSQL(sqlBuf)
		assert.Equal(t, 0, len(e.debugTxnRecorder.txnSQL))
	})
}
