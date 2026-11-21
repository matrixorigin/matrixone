// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db_holder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/stretchr/testify/require"
)

func TestWriteMergeFileConnectionCancellation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cancel bool
	}{
		{name: "caller cancellation", cancel: true},
		{name: "internal timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				SyncTestWriteRowRecords(t, func(t *testing.T) {
					oldBackoff, oldProvider := DBConnErrCount, GetOrInitDBConn
					DBConnErrCount = NewReConnectionBackOff(time.Hour, 1)
					DBConnErrCount.Count()
					defer func() {
						DBConnErrCount, GetOrInitDBConn = oldBackoff, oldProvider
					}()
					db, _, err := sqlmock.New()
					require.NoError(t, err)
					defer db.Close()
					db.SetMaxOpenConns(1)
					occupied, err := db.Conn(t.Context())
					require.NoError(t, err)
					defer occupied.Close()
					GetOrInitDBConn = func(bool, bool) (*sql.DB, error) { return db, nil }

					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					timeout := time.Minute
					if !tc.cancel {
						timeout = 200 * time.Millisecond
					}
					result := make(chan error, 1)
					go func() {
						result <- WriteMergeFile(ctx, &table.Table{}, []string{"first"}, func() ([]string, error) {
							return nil, errors.New("unexpected read")
						}, timeout)
					}()
					// Wait until database/sql parks the acquisition on the occupied pool.
					synctest.Wait()
					require.EqualValues(t, 1, db.Stats().WaitCount)

					if tc.cancel {
						cancel()
						require.ErrorIs(t, <-result, context.Canceled)
						require.True(t, DBConnErrCount.Check(), "caller cancellation must not trigger reconnect")
						DBConnErrCount.Count()
						require.False(t, DBConnErrCount.Check(), "caller cancellation must not reset prior failures")
					} else {
						require.ErrorIs(t, <-result, context.DeadlineExceeded)
						require.False(t, DBConnErrCount.Check(), "internal timeout must trigger reconnect")
					}
				})
			})
		})
	}
}

// packetQueryConnector supplies controlled driver results without sqlmock's
// nonstandard cancellation sentinel or timing races.
type packetQueryConnector struct {
	query func(context.Context) error
}

func (c packetQueryConnector) Connect(context.Context) (driver.Conn, error) {
	return packetQueryConn{query: c.query}, nil
}
func (c packetQueryConnector) Driver() driver.Driver { return packetQueryDriver{} }

type packetQueryDriver struct{}

func (packetQueryDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("unexpected Open")
}

type packetQueryConn struct {
	query func(context.Context) error
}

func (packetQueryConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (packetQueryConn) Close() error              { return nil }
func (packetQueryConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected Begin") }
func (c packetQueryConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return nil, c.query(ctx)
}

func TestWriteMergeFilePacketQueryFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cancel     bool
		timeout    bool
		deadline   bool
		otherError bool
		countError bool
	}{
		{name: "caller cancellation", cancel: true},
		{name: "internal timeout", timeout: true, countError: true},
		{name: "caller deadline", deadline: true},
		{name: "internal timeout then caller deadline", timeout: true, deadline: true, countError: true},
		{name: "internal timeout then caller cancellation", timeout: true, cancel: true, countError: true},
		{name: "server error", otherError: true, countError: true},
		{name: "server error concurrent caller cancellation", cancel: true, otherError: true, countError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				SyncTestWriteRowRecords(t, func(t *testing.T) {
					oldBackoff, oldProvider := DBConnErrCount, GetOrInitDBConn
					DBConnErrCount = NewReConnectionBackOff(time.Hour, 0)
					defer func() {
						DBConnErrCount, GetOrInitDBConn = oldBackoff, oldProvider
					}()
					ctx, cancel := context.WithCancel(t.Context())
					if tc.deadline {
						cancel()
						ctx, cancel = context.WithTimeout(t.Context(), 150*time.Millisecond)
					}
					defer cancel()
					failure := errors.New("packet query failed")
					db := sql.OpenDB(packetQueryConnector{query: func(queryCtx context.Context) error {
						if tc.timeout {
							<-queryCtx.Done()
							if tc.cancel {
								cancel()
							}
							if tc.deadline {
								<-ctx.Done()
							}
							return queryCtx.Err()
						}
						if tc.deadline {
							<-queryCtx.Done()
							return queryCtx.Err()
						}
						if tc.cancel {
							cancel()
						}
						if tc.otherError {
							return failure
						}
						return queryCtx.Err()
					}})
					defer db.Close()
					GetOrInitDBConn = func(bool, bool) (*sql.DB, error) { return db, nil }
					timeout := time.Minute
					if tc.timeout {
						timeout = 50 * time.Millisecond
					}

					err := WriteMergeFile(ctx, &table.Table{}, []string{"first"}, func() ([]string, error) {
						return nil, errors.New("unexpected read")
					}, timeout)
					require.Error(t, err)
					switch {
					case tc.otherError:
						require.ErrorIs(t, err, failure)
					case tc.timeout || tc.deadline:
						require.ErrorIs(t, err, context.DeadlineExceeded)
					default:
						require.ErrorIs(t, err, context.Canceled)
					}
					require.Equal(t, !tc.countError, DBConnErrCount.Check())
				})
			})
		})
	}
}

func TestWriteMergeFileProviderErrorDuringCancellation(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		oldBackoff, oldProvider := DBConnErrCount, GetOrInitDBConn
		DBConnErrCount = NewReConnectionBackOff(time.Hour, 0)
		defer func() { DBConnErrCount, GetOrInitDBConn = oldBackoff, oldProvider }()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		failure := errors.New("connection provider failed")
		GetOrInitDBConn = func(bool, bool) (*sql.DB, error) {
			cancel()
			return nil, failure
		}
		err := WriteMergeFile(ctx, &table.Table{}, []string{"first"}, nil, time.Minute)
		require.ErrorIs(t, err, failure)
		require.False(t, DBConnErrCount.Check())
	})
}

func TestWriteMergeFileCallerCancellationWithSharedCause(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancellation"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				SyncTestWriteRowRecords(t, func(t *testing.T) {
					oldBackoff, oldProvider := DBConnErrCount, GetOrInitDBConn
					DBConnErrCount = NewReConnectionBackOff(time.Hour, 0)
					defer func() { DBConnErrCount, GetOrInitDBConn = oldBackoff, oldProvider }()
					var ctx context.Context
					var stop func()
					var cancel context.CancelCauseFunc
					if deadline {
						ctx, stop = context.WithTimeoutCause(t.Context(), time.Second, moerr.CauseWriteRowRecords)
					} else {
						ctx, cancel = context.WithCancelCause(t.Context())
						stop = func() { cancel(nil) }
					}
					defer stop()
					db := sql.OpenDB(packetQueryConnector{query: func(queryCtx context.Context) error {
						if deadline {
							<-queryCtx.Done()
						} else {
							cancel(moerr.CauseWriteRowRecords)
						}
						return queryCtx.Err()
					}})
					defer db.Close()
					GetOrInitDBConn = func(bool, bool) (*sql.DB, error) { return db, nil }

					err := WriteMergeFile(ctx, &table.Table{}, []string{"first"}, nil, time.Minute)
					if deadline {
						require.ErrorIs(t, err, context.DeadlineExceeded)
					} else {
						require.ErrorIs(t, err, context.Canceled)
					}
					require.True(t, DBConnErrCount.Check(), "caller cause must not be mistaken for the child timeout")
				})
			})
		})
	}
}
