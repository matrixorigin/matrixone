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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

// TestIssue27635PreparedBitNull exercises the server-prepared path used by
// Connector/J. database/sql prepares once and go-sql-driver/mysql sends every
// execution through COM_STMT_EXECUTE, including its NULL bitmap and cached
// parameter-type metadata.
func TestIssue27635PreparedBitNull(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		defer db.Close()

		const dbName = "issue_27635_prepared_bit_null"
		execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+dbName)
		}()
		execSQLRequire(t, ctx, db, "create database "+dbName)
		execSQLRequire(t, ctx, db,
			"create table "+dbName+".t(id int primary key, b1 bit(1), b8 bit(8), b64 bit(64))")

		stmt, err := db.PrepareContext(ctx,
			"insert into "+dbName+".t(id, b1, b8, b64) values (?, ?, ?, ?)")
		require.NoError(t, err)
		defer stmt.Close()

		// First execution: all BIT parameters are NULL in COM_STMT_EXECUTE's bitmap.
		_, err = stmt.ExecContext(ctx, 1, nil, nil, nil)
		require.NoError(t, err)
		// Rebind all widths to non-NULL values on the same server statement.
		_, err = stmt.ExecContext(ctx, 2, true, int64(0xa5), uint64(1)<<63)
		require.NoError(t, err)
		// Reuse the statement once more; NULL must not retain the preceding values.
		_, err = stmt.ExecContext(ctx, 3, nil, nil, nil)
		require.NoError(t, err)

		rows, err := db.QueryContext(ctx,
			"select id, b1 is null, b8 is null, b64 is null, hex(b1), hex(b8), hex(b64) "+
				"from "+dbName+".t order by id")
		require.NoError(t, err)
		defer rows.Close()

		type expectedRow struct {
			id               int
			b1Null, b8Null   int
			b64Null          int
			h1, h8, h64      string
			hexValuesAreNull bool
		}
		expected := []expectedRow{
			{id: 1, b1Null: 1, b8Null: 1, b64Null: 1, hexValuesAreNull: true},
			{id: 2, h1: "1", h8: "A5", h64: "8000000000000000"},
			{id: 3, b1Null: 1, b8Null: 1, b64Null: 1, hexValuesAreNull: true},
		}
		for _, want := range expected {
			require.True(t, rows.Next())
			var id, b1Null, b8Null, b64Null int
			var h1, h8, h64 sql.NullString
			require.NoError(t, rows.Scan(&id, &b1Null, &b8Null, &b64Null, &h1, &h8, &h64))
			require.Equal(t, want.id, id)
			require.Equal(t, want.b1Null, b1Null)
			require.Equal(t, want.b8Null, b8Null)
			require.Equal(t, want.b64Null, b64Null)
			if want.hexValuesAreNull {
				require.False(t, h1.Valid)
				require.False(t, h8.Valid)
				require.False(t, h64.Valid)
			} else {
				require.Equal(t, want.h1, h1.String)
				require.Equal(t, want.h8, h8.String)
				require.Equal(t, want.h64, h64.String)
			}
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})
}
