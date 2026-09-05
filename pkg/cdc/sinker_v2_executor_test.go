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
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
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

// blockingTargetLockConnector gives target-lock tests a deterministic barrier:
// the first query does not return until its context is cancelled. Later queries
// model best-effort RELEASE_LOCK cleanup on the same pinned session.
type blockingTargetLockConnector struct {
	started   chan struct{}
	deadline  chan time.Duration
	queryOnce sync.Once
	queries   atomic.Int32
	block     bool
}

func (c *blockingTargetLockConnector) Connect(context.Context) (driver.Conn, error) {
	return &blockingTargetLockConn{connector: c}, nil
}

func (c *blockingTargetLockConnector) Driver() driver.Driver {
	return blockingTargetLockDriver{}
}

type blockingTargetLockDriver struct{}

func (blockingTargetLockDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("blockingTargetLockConnector must be used with sql.OpenDB")
}

type blockingTargetLockConn struct {
	connector *blockingTargetLockConnector
}

func (*blockingTargetLockConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (*blockingTargetLockConn) Close() error { return nil }

func (*blockingTargetLockConn) Begin() (driver.Tx, error) { return nil, driver.ErrSkip }

func (c *blockingTargetLockConn) QueryContext(
	ctx context.Context,
	_ string,
	_ []driver.NamedValue,
) (driver.Rows, error) {
	if c.connector.queries.Add(1) == 1 {
		deadline, ok := ctx.Deadline()
		if !ok {
			return nil, errors.New("target-lock query context has no deadline")
		}
		c.connector.deadline <- time.Until(deadline)
		c.connector.queryOnce.Do(func() { close(c.connector.started) })
		if c.connector.block {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, errors.New("target-lock probe failed")
	}
	return &singleValueRows{value: int64(0)}, nil
}

type singleValueRows struct {
	read  bool
	value driver.Value
}

func (*singleValueRows) Columns() []string { return []string{"value"} }

func (*singleValueRows) Close() error { return nil }

func (r *singleValueRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	dest[0] = r.value
	return nil
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

func TestExecutorTargetOwnershipLock(t *testing.T) {
	t.Run("connection checkout failure remains retryable", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		mock.ExpectClose()
		require.NoError(t, db.Close())
		executor := &Executor{conn: db}

		err = executor.AcquireTargetLock(
			context.Background(),
			"account/task/db/table",
			func(context.Context) error { return nil },
			nil,
		)
		require.Error(t, err)
		require.True(t, IsRetryableConnectionError(err))
	})

	t.Run("releases completed effect and reacquires for the next transaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		executor := &Executor{conn: db}

		mock.ExpectQuery("SELECT GET_LOCK").
			WithArgs(sqlmock.AnyArg(), targetLockPollSeconds).
			WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(1))
		checks := 0
		require.NoError(t, executor.AcquireTargetLock(
			context.Background(),
			"account/task/db/table",
			func(context.Context) error { checks++; return nil },
			nil,
		))
		require.Equal(t, 1, checks)
		require.NotNil(t, executor.targetLockConn)

		mock.ExpectBegin()
		require.NoError(t, executor.BeginTx(context.Background()))
		mock.ExpectCommit()
		require.NoError(t, executor.CommitTx(context.Background()))

		mock.ExpectQuery("SELECT RELEASE_LOCK").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
		require.NoError(t, executor.ReleaseTargetLock())
		require.Nil(t, executor.targetLockConn)

		mock.ExpectQuery("SELECT GET_LOCK").
			WithArgs(sqlmock.AnyArg(), targetLockPollSeconds).
			WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(1))
		mock.ExpectBegin()
		require.NoError(t, executor.BeginTx(context.Background()))
		mock.ExpectRollback()
		mock.ExpectQuery("SELECT RELEASE_LOCK").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
		require.NoError(t, executor.RollbackTx(context.Background()))
		require.Equal(t, 2, checks)

		mock.ExpectClose()
		require.NoError(t, executor.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("stale waiter releases lock before target work", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		executor := &Executor{conn: db}

		mock.ExpectQuery("SELECT GET_LOCK").
			WithArgs(sqlmock.AnyArg(), targetLockPollSeconds).
			WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(1))
		mock.ExpectQuery("SELECT RELEASE_LOCK").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
		mock.ExpectClose()
		err = executor.AcquireTargetLock(
			context.Background(),
			"account/task/db/table",
			func(ctx context.Context) error {
				return moerr.NewInvalidTask(ctx, "old-owner", 1)
			},
			nil,
		)
		require.Error(t, err)
		require.Nil(t, executor.targetLockConn)
		require.NoError(t, executor.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("cancelled waiter stops without remote validation", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		executor := &Executor{conn: db}

		mock.ExpectQuery("SELECT GET_LOCK").
			WithArgs(sqlmock.AnyArg(), targetLockPollSeconds).
			WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(0))
		mock.ExpectQuery("SELECT RELEASE_LOCK").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(0))
		mock.ExpectClose()
		waitChecks := 0
		ownerChecks := 0
		err = executor.AcquireTargetLock(
			context.Background(),
			"account/task/db/table",
			func(context.Context) error { ownerChecks++; return nil },
			func(ctx context.Context) error {
				waitChecks++
				if waitChecks == 2 {
					return context.Canceled
				}
				return nil
			},
		)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 2, waitChecks)
		require.Zero(t, ownerChecks)
		require.Nil(t, executor.targetLockConn)
		require.NoError(t, executor.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("cancel after successful poll releases before remote validation", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		executor := &Executor{conn: db}

		mock.ExpectQuery("SELECT GET_LOCK").
			WithArgs(sqlmock.AnyArg(), targetLockPollSeconds).
			WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(1))
		mock.ExpectQuery("SELECT RELEASE_LOCK").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
		mock.ExpectClose()
		waitChecks := 0
		ownerChecks := 0
		err = executor.AcquireTargetLock(
			context.Background(),
			"account/task/db/table",
			func(context.Context) error { ownerChecks++; return nil },
			func(context.Context) error {
				waitChecks++
				if waitChecks == 2 {
					return context.Canceled
				}
				return nil
			},
		)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 2, waitChecks)
		require.Zero(t, ownerChecks)
		require.Nil(t, executor.targetLockConn)
		require.NoError(t, executor.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("caller cancellation interrupts an in-flight poll", func(t *testing.T) {
		connector := &blockingTargetLockConnector{
			started:  make(chan struct{}),
			deadline: make(chan time.Duration, 1),
			block:    true,
		}
		db := sql.OpenDB(connector)
		executor := &Executor{conn: db}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- executor.AcquireTargetLock(
				ctx,
				"account/task/db/table",
				func(context.Context) error { return nil },
				nil,
			)
		}()

		<-connector.started
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("target-lock query did not observe caller cancellation")
		}
		require.Nil(t, executor.targetLockConn)
		require.NoError(t, executor.Close())
	})

	t.Run("each poll has a hard deadline independent of SQL configuration", func(t *testing.T) {
		connector := &blockingTargetLockConnector{
			started:  make(chan struct{}),
			deadline: make(chan time.Duration, 1),
		}
		db := sql.OpenDB(connector)
		executor := &Executor{conn: db}
		err := executor.AcquireTargetLock(
			context.Background(),
			"account/task/db/table",
			func(context.Context) error { return nil },
			nil,
		)
		require.Error(t, err)
		require.True(t, IsRetryableTargetLockError(err))
		remaining := <-connector.deadline
		require.Positive(t, remaining)
		require.LessOrEqual(t, remaining, targetLockPollQueryTimeout)
		require.Nil(t, executor.targetLockConn)
		require.NoError(t, executor.Close())
	})

	for _, tc := range []struct {
		name  string
		value any
	}{
		{name: "NULL response exits retryably", value: nil},
		{name: "unexpected response exits retryably", value: int64(2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			executor := &Executor{conn: db}
			mock.ExpectQuery("SELECT GET_LOCK").
				WithArgs(sqlmock.AnyArg(), targetLockPollSeconds).
				WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(tc.value))
			mock.ExpectQuery("SELECT RELEASE_LOCK").
				WithArgs(sqlmock.AnyArg()).
				WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(0))
			mock.ExpectClose()

			err = executor.AcquireTargetLock(
				context.Background(),
				"account/task/db/table",
				func(context.Context) error { return nil },
				nil,
			)
			require.Error(t, err)
			require.True(t, IsRetryableTargetLockError(err))
			require.Nil(t, executor.targetLockConn)
			require.NoError(t, executor.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}

	t.Run("ambiguous release discards the pinned session", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		executor := &Executor{conn: db}

		mock.ExpectQuery("SELECT GET_LOCK").
			WithArgs(sqlmock.AnyArg(), targetLockPollSeconds).
			WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(1))
		require.NoError(t, executor.AcquireTargetLock(
			context.Background(), "account/task/db/table",
			func(context.Context) error { return nil }, nil,
		))
		releaseErr := errors.New("release response lost")
		mock.ExpectQuery("SELECT RELEASE_LOCK").
			WithArgs(sqlmock.AnyArg()).
			WillReturnError(releaseErr)
		require.ErrorIs(t, executor.ReleaseTargetLock(), releaseErr)
		require.Nil(t, executor.targetLockConn)

		require.NoError(t, executor.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestExecutorTargetOwnerValidationCallRate(t *testing.T) {
	const effects = 100
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	executor := &Executor{conn: db}
	ownerChecks := 0
	for i := 0; i < effects; i++ {
		mock.ExpectQuery("SELECT GET_LOCK").
			WithArgs(sqlmock.AnyArg(), targetLockPollSeconds).
			WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(1))
		require.NoError(t, executor.AcquireTargetLock(
			context.Background(), fmt.Sprintf("account/task/db/table-%d", i),
			func(context.Context) error { ownerChecks++; return nil }, nil,
		))
		mock.ExpectQuery("SELECT RELEASE_LOCK").
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
		require.NoError(t, executor.ReleaseTargetLock())
	}
	require.Equal(t, effects, ownerChecks,
		"steady-state effect count must equal read-only claim validation count")
	mock.ExpectClose()
	require.NoError(t, executor.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTargetOwnershipSerializesBlockedOldCommitAndTakeover(t *testing.T) {
	oldDB, oldMock, err := sqlmock.New()
	require.NoError(t, err)
	oldExec := &Executor{conn: oldDB}
	newDB, newMock, err := sqlmock.New()
	require.NoError(t, err)
	newExec := &Executor{conn: newDB}

	oldMock.ExpectQuery("SELECT GET_LOCK").
		WithArgs(sqlmock.AnyArg(), targetLockPollSeconds).
		WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(1))
	require.NoError(t, oldExec.AcquireTargetLock(
		context.Background(), "same-task-table", func(context.Context) error { return nil }, nil))
	oldMock.ExpectBegin()
	require.NoError(t, oldExec.BeginTx(context.Background()))
	oldMock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, oldExec.ExecSQL(
		context.Background(), nil, []byte("     REPLACE old-generation"), false))

	// The replacement first observes the target lock as busy. Its second poll
	// is released only after the old COMMIT has terminated and the old pinned
	// session has released ownership.
	newMock.ExpectQuery("SELECT GET_LOCK").
		WithArgs(sqlmock.AnyArg(), targetLockPollSeconds).
		WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(0))
	newMock.ExpectQuery("SELECT GET_LOCK").
		WithArgs(sqlmock.AnyArg(), targetLockPollSeconds).
		WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(1))
	newWaiting := make(chan struct{})
	oldReleased := make(chan struct{})
	var newWaitChecks atomic.Int32
	var newFenceCalls atomic.Int32
	newAcquireDone := make(chan error, 1)
	go func() {
		newAcquireDone <- newExec.AcquireTargetLock(
			context.Background(),
			"same-task-table",
			func(context.Context) error {
				newFenceCalls.Add(1)
				return nil
			},
			func(context.Context) error {
				if newWaitChecks.Add(1) == 2 {
					close(newWaiting)
					<-oldReleased
				}
				return nil
			},
		)
	}()
	<-newWaiting

	target := map[string]struct{}{}
	oldMock.ExpectCommit()
	require.NoError(t, oldExec.CommitTx(context.Background()))
	target["retired-key"] = struct{}{}
	oldMock.ExpectQuery("SELECT RELEASE_LOCK").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
	require.NoError(t, oldExec.ReleaseTargetLock())
	close(oldReleased)
	require.NoError(t, <-newAcquireDone)

	// The replacement is necessarily last at the target boundary, so its
	// stable-epoch replay/reset determines the exact final key set.
	newMock.ExpectBegin()
	require.NoError(t, newExec.BeginTx(context.Background()))
	newMock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, newExec.ExecSQL(
		context.Background(), nil, []byte("     REPLACE replacement-generation"), false))
	newMock.ExpectCommit()
	require.NoError(t, newExec.CommitTx(context.Background()))
	target = map[string]struct{}{"replacement-key": {}}
	newMock.ExpectQuery("SELECT RELEASE_LOCK").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
	require.NoError(t, newExec.ReleaseTargetLock())
	oldMock.ExpectClose()
	require.NoError(t, oldExec.Close())
	newMock.ExpectClose()
	require.NoError(t, newExec.Close())

	require.Equal(t, map[string]struct{}{"replacement-key": {}}, target)
	require.Equal(t, int32(1), newFenceCalls.Load(),
		"lock polling must not multiply remote owner validations")
	require.NoError(t, oldMock.ExpectationsWereMet())
	require.NoError(t, newMock.ExpectationsWereMet())
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
	// Keep the closed-to-open assertion independent of scheduler pauses.
	executor.circuitBreaker.coolDown = time.Hour

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
	executor.circuitBreaker.mu.Lock()
	executor.circuitBreaker.openedAt = time.Now().Add(-executor.circuitBreaker.coolDown)
	executor.circuitBreaker.mu.Unlock()
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

		stub := gostub.Stub(&OpenDbConn, func(_ context.Context, user, password, ip string, port int, timeout string) (*sql.DB, error) {
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

	db, err := tryConn(context.Background(), cfg)
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

	t.Run("TotalBytes", func(t *testing.T) {
		e := &Executor{}
		e.debugTxnRecorder.doRecord = true
		e.debugTxnRecorder.sqlBytes = maxDebugTxnSQLBytes - 2

		sqlBuf := append(make([]byte, v2SQLBufReserved), []byte("OK")...)
		e.recordTxnSQL(sqlBuf)
		assert.Equal(t, maxDebugTxnSQLBytes, e.debugTxnRecorder.sqlBytes)
		assert.Equal(t, []string{"OK"}, e.debugTxnRecorder.txnSQL)

		e.recordTxnSQL(append(make([]byte, v2SQLBufReserved), byte('X')))
		assert.Equal(t, maxDebugTxnSQLBytes, e.debugTxnRecorder.sqlBytes)
		assert.Len(t, e.debugTxnRecorder.txnSQL, 1)
	})
}
