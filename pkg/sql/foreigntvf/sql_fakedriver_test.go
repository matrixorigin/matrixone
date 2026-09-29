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

package foreigntvf

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"testing"

	gosql "database/sql"

	"github.com/stretchr/testify/require"
)

// A minimal in-process database/sql driver so connectSQL / SqlConn.Query /
// encodeRowsCSV run end-to-end in unit tests without any server. The DSN
// selects the scripted result: "rows" streams two rows (one NULL), and
// "midstream-error" fails after the first row.
type fakeDriver struct{}

type fakeDrvConn struct{ dsn string }
type fakeStmt struct{ dsn string }

type fakeRows struct {
	dsn  string
	next int
}

func (fakeDriver) Open(dsn string) (driver.Conn, error) {
	if dsn == "refuse" {
		return nil, errors.New("fake refuses to connect")
	}
	return &fakeDrvConn{dsn: dsn}, nil
}

func (c *fakeDrvConn) Prepare(q string) (driver.Stmt, error) { return &fakeStmt{dsn: c.dsn}, nil }
func (c *fakeDrvConn) Close() error                          { return nil }
func (c *fakeDrvConn) Begin() (driver.Tx, error)             { return nil, driver.ErrSkip }

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return 0 }
func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	return driver.ResultNoRows, nil
}
func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &fakeRows{dsn: s.dsn}, nil
}

func (r *fakeRows) Columns() []string { return []string{"id", "name"} }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	r.next++
	switch {
	case r.next == 1:
		dest[0], dest[1] = int64(1), []byte("al\"i,ce")
		return nil
	case r.next == 2 && r.dsn == "midstream-error":
		return errors.New("connection lost mid-stream")
	case r.next == 2:
		dest[0], dest[1] = int64(2), nil // SQL NULL
		return nil
	default:
		return io.EOF
	}
}

var registerFakeOnce sync.Once

func useFakeDriver(t *testing.T) {
	registerFakeOnce.Do(func() { gosql.Register("foreigntvf-fake", fakeDriver{}) })
	if _, ok := driverAliases["fake"]; !ok {
		driverAliases["fake"] = "foreigntvf-fake"
	}
	t.Cleanup(func() { delete(driverAliases, "fake") })
}

func TestConnectSQLAndQueryFakeDriver(t *testing.T) {
	useFakeDriver(t)
	ctx := context.Background()

	conn, err := connectSQL(ctx, `{"driver":"fake","dsn":"rows"}`)
	require.NoError(t, err)
	require.Equal(t, KindSQL, conn.Kind())

	stream, err := conn.Query(ctx, "select whatever")
	require.NoError(t, err)
	data, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	// row 1: quote and comma escaped per the MySQL dialect; row 2: NULL as \N
	require.Equal(t, "\"1\",\"al\\\"i,ce\"\n\"2\",\\N\n", string(data))
	require.NoError(t, conn.Close())
}

func TestSQLQueryMidStreamErrorAttributed(t *testing.T) {
	useFakeDriver(t)
	ctx := context.Background()
	conn, err := connectSQL(ctx, `{"driver":"fake","dsn":"midstream-error"}`)
	require.NoError(t, err)
	defer conn.Close()

	stream, err := conn.Query(ctx, "select whatever")
	require.NoError(t, err)
	_, err = io.ReadAll(stream)
	require.Error(t, err)
	// the raw driver error must arrive wrapped with sql_tvf attribution
	require.ErrorContains(t, err, "sql_tvf: reading foreign result")
	require.ErrorContains(t, err, "connection lost mid-stream")
	_ = stream.Close()
}

// TestSQLQueryEarlyReaderClose proves the encoder goroutine terminates when
// the consumer abandons the stream before EOF (operator reset/free path).
func TestSQLQueryEarlyReaderClose(t *testing.T) {
	useFakeDriver(t)
	ctx := context.Background()
	conn, err := connectSQL(ctx, `{"driver":"fake","dsn":"rows"}`)
	require.NoError(t, err)
	defer conn.Close()

	stream, err := conn.Query(ctx, "select whatever")
	require.NoError(t, err)
	// read one byte, then walk away
	one := make([]byte, 1)
	_, err = stream.Read(one)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	// a second Close is fine
	require.NoError(t, stream.Close())
}

func TestConnectSQLDialFailure(t *testing.T) {
	useFakeDriver(t)
	_, err := connectSQL(context.Background(), `{"driver":"fake","dsn":"refuse"}`)
	require.ErrorContains(t, err, "cannot connect")
}

// TestResolveOrConnectFakeDriver drives the full connect-or-reuse cycle
// through the public API: fresh connect, cached reuse, and ByHandle lookup.
func TestResolveOrConnectFakeDriver(t *testing.T) {
	useFakeDriver(t)
	ctx := context.Background()
	cache := newFakeConnCache()
	cfg := `{"driver":"fake","dsn":"rows"}`

	c1, h1, err := ResolveOrConnect(ctx, cache, KindSQL, cfg)
	require.NoError(t, err)
	require.Equal(t, MakeHandle(KindSQL, cfg), h1)

	// same config -> same handle, same connection (no reconnect)
	c2, h2, err := ResolveOrConnect(ctx, cache, KindSQL, cfg)
	require.NoError(t, err)
	require.Equal(t, h1, h2)
	require.Same(t, c1, c2)

	// explicit handle lookup
	c3, err := ByHandle(ctx, cache, h1)
	require.NoError(t, err)
	require.Same(t, c1, c3)

	// unknown handle
	_, err = ByHandle(ctx, cache, "sql:deadbeef")
	require.ErrorContains(t, err, "not found or disconnected")

	// remove + close, then the handle is gone
	removed, ok := cache.RemoveForeignConn(h1)
	require.True(t, ok)
	require.NoError(t, removed.Close())
	_, err = ByHandle(ctx, cache, h1)
	require.Error(t, err)
}

func TestConnectUnknownKind(t *testing.T) {
	_, err := Connect(context.Background(), Kind("nosuch"), "{}")
	require.ErrorContains(t, err, "unknown connection kind")
	require.Error(t, ValidateConfig(context.Background(), Kind("nosuch"), "{}"))
}

func TestValidateConfigBothKinds(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, ValidateConfig(ctx, KindSQL, `{"driver":"mysql","dsn":"x"}`))
	require.Error(t, ValidateConfig(ctx, KindSQL, `{"driver":"mysql"}`))
	require.NoError(t, ValidateConfig(ctx, KindESQL, `{"addresses":["http://h"]}`))
	require.Error(t, ValidateConfig(ctx, KindESQL, `{}`))
	require.NoError(t, validateSQLConfig(ctx, `{"driver":"postgres","dsn":"x"}`))
}
