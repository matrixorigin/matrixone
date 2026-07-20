// Copyright 2021 - 2026 Matrix Origin
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

package issues

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

// TestIssue25753PreparedNumericProtocolLifecycle exercises the real
// COM_STMT_PREPARE / COM_STMT_EXECUTE / COM_STMT_CLOSE path. In particular,
// interpolateParams=false prevents the client from replacing placeholders in
// a text query, and all executions below reuse the same server-side statement.
func TestIssue25753PreparedNumericProtocolLifecycle(t *testing.T) {
	require.NoError(t, embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			port := cn.GetServiceConfig().CN.Frontend.Port
			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port)
			db, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			defer db.Close()

			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			stmt, err := conn.PrepareContext(ctx, "select cast((? + ?) + 1 as decimal(30, 0)) as result")
			require.NoError(t, err)
			stmtClosed := false
			defer func() {
				if !stmtClosed {
					_ = stmt.Close()
				}
			}()

			assertValue := func(left, right any, expected string) {
				t.Helper()
				rows, queryErr := stmt.QueryContext(ctx, left, right)
				require.NoError(t, queryErr)
				defer rows.Close()

				columnTypes, typeErr := rows.ColumnTypes()
				require.NoError(t, typeErr)
				require.Len(t, columnTypes, 1)
				require.Equal(t, "DECIMAL", columnTypes[0].DatabaseTypeName())
				precision, scale, ok := columnTypes[0].DecimalSize()
				require.True(t, ok)
				require.Equal(t, int64(30), precision)
				require.Equal(t, int64(0), scale)

				require.True(t, rows.Next())
				var actual string
				require.NoError(t, rows.Scan(&actual))
				require.Equal(t, expected, actual)
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}

			// Every call uses a new-params-bound flag and a different client
			// transport type. The prepared decimal computation domain must stay
			// stable, including for integers above the exact float64 range.
			assertValue(int64(9007199254740993), int64(0), "9007199254740994")
			assertValue(uint64(9007199254740993), uint64(0), "9007199254740994")
			assertValue("9007199254740993", "0", "9007199254740994")
			assertValue(int64(-9007199254740993), int64(0), "-9007199254740992")

			rows, err := stmt.QueryContext(ctx, nil, int64(0))
			require.NoError(t, err)
			defer rows.Close()
			require.True(t, rows.Next())
			var nullResult sql.NullString
			require.NoError(t, rows.Scan(&nullResult))
			require.False(t, nullResult.Valid)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())

			// A conversion failure must have a stable server error code and must
			// not poison the cached statement or its parameter state.
			var firstCode uint16
			for range 2 {
				var ignored string
				err = stmt.QueryRowContext(ctx, "not-a-number", int64(0)).Scan(&ignored)
				require.Error(t, err)
				var mysqlErr *mysqlDriver.MySQLError
				require.True(t, errors.As(err, &mysqlErr), "expected MySQL protocol error, got %T: %v", err, err)
				require.NotZero(t, mysqlErr.Number)
				if firstCode == 0 {
					firstCode = mysqlErr.Number
				} else {
					require.Equal(t, firstCode, mysqlErr.Number)
				}
			}

			assertValue(int64(9007199254740993), int64(0), "9007199254740994")

			// The driver's argument-count check is populated from the parameter
			// count in COM_STMT_PREPARE_OK.
			err = stmt.QueryRowContext(ctx, int64(1)).Scan(new(string))
			require.ErrorContains(t, err, "expected 2 arguments")

			require.NoError(t, stmt.Close())
			stmtClosed = true
			err = stmt.QueryRowContext(ctx, int64(1), int64(0)).Scan(new(string))
			require.ErrorContains(t, err, "statement is closed")
		},
	))
}
