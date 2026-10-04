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
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

// TestIssue26873BinaryPreparedEnumAndYearCoveringIndex verifies the public
// COM_STMT_PREPARE / COM_STMT_EXECUTE path. interpolateParams=false keeps the
// driver on the binary prepared-statement protocol instead of interpolating a
// text query.
func TestIssue26873BinaryPreparedEnumAndYearCoveringIndex(t *testing.T) {
	embed.RunBaseClusterTests(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			defer cancel()

			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			port := cn.GetServiceConfig().CN.Frontend.Port
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
			require.NoError(t, err)
			defer db.Close()

			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			dbName := testutils.GetDatabaseName(t)
			mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
			mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
			}()

			cases := []struct {
				name       string
				create     string
				insert     string
				table      string
				index      string
				column     string
				literal    string
				parameter  any
				expectedID int
			}{
				{
					name:   "enum",
					create: "create table enum_index (id int primary key, e enum('red','green','blue'), payload varchar(20), key idx_e(e))",
					insert: "insert into enum_index values (1,'red','one'),(2,'green','two'),(3,'blue','three'),(4,null,'null')",
					table:  "enum_index", index: "idx_e", column: "e", literal: "'red'", parameter: "red", expectedID: 1,
				},
				{
					name:   "year",
					create: "create table year_index (id int primary key, y year, payload varchar(20), key idx_y(y))",
					insert: "insert into year_index values (1,2026,'one'),(2,2025,'two'),(3,2024,'three'),(4,null,'null')",
					table:  "year_index", index: "idx_y", column: "y", literal: "2026", parameter: int64(2026), expectedID: 1,
				},
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					mustExec(t, ctx, conn, tc.create)
					mustExec(t, ctx, conn, tc.insert)

					forceStmt, err := conn.PrepareContext(ctx, fmt.Sprintf(
						"select id from %s force index(%s) where %s=?", tc.table, tc.index, tc.column,
					))
					require.NoError(t, err)
					defer forceStmt.Close()

					ignoreStmt, err := conn.PrepareContext(ctx, fmt.Sprintf(
						"select id from %s ignore index(%s) where %s=?", tc.table, tc.index, tc.column,
					))
					require.NoError(t, err)
					defer ignoreStmt.Close()

					backfillStmt, err := conn.PrepareContext(ctx, fmt.Sprintf(
						"select id from %s force index(%s) where %s=? and payload='one'", tc.table, tc.index, tc.column,
					))
					require.NoError(t, err)
					defer backfillStmt.Close()

					var literalID, scannedID, backfillID, forcedID int
					require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
						"select id from %s force index(%s) where %s=%s", tc.table, tc.index, tc.column, tc.literal,
					)).Scan(&literalID))
					require.NoError(t, ignoreStmt.QueryRowContext(ctx, tc.parameter).Scan(&scannedID))
					require.NoError(t, backfillStmt.QueryRowContext(ctx, tc.parameter).Scan(&backfillID))
					require.NoError(t, forceStmt.QueryRowContext(ctx, tc.parameter).Scan(&forcedID))
					require.Equal(t, tc.expectedID, literalID)
					require.Equal(t, tc.expectedID, forcedID)
					require.Equal(t, scannedID, forcedID)
					require.Equal(t, backfillID, forcedID)

					var ignoredID int
					err = forceStmt.QueryRowContext(ctx, nil).Scan(&ignoredID)
					require.ErrorIs(t, err, sql.ErrNoRows)
				})
			}
		},
	)
}
