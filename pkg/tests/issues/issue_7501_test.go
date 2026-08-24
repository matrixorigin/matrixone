// Copyright 2026 Matrix Origin
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
)

func TestIssue7501PreparedDMLReturning(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()

		const dbName = "issue_7501_prepared"
		execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+dbName)
		}()
		execSQLRequire(t, ctx, db, "create database "+dbName)
		execSQLRequire(t, ctx, db, "create table "+dbName+".t(id int primary key, v varchar(20))")

		assertPreparedRow := func(query string, args []any, wantID int, wantValue string) {
			t.Helper()
			stmt, err := db.PrepareContext(ctx, query)
			require.NoError(t, err)
			defer stmt.Close()

			rows, err := stmt.QueryContext(ctx, args...)
			require.NoError(t, err)
			defer rows.Close()
			require.True(t, rows.Next())
			var id int
			var value string
			require.NoError(t, rows.Scan(&id, &value))
			require.Equal(t, wantID, id)
			require.Equal(t, wantValue, value)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}

		assertPreparedRow(
			"insert into "+dbName+".t values (?, ?) returning id, v",
			[]any{1, "inserted"}, 1, "inserted",
		)
		assertPreparedRow(
			"update "+dbName+".t set v = ? where id = ? returning id, v",
			[]any{"updated", 1}, 1, "updated",
		)
		assertPreparedRow(
			"delete from "+dbName+".t where id = ? returning id, v",
			[]any{1}, 1, "updated",
		)
		assertPreparedRow(
			"insert into "+dbName+".t values (?, ?) returning id, ?",
			[]any{2, "stored", "projected"}, 2, "projected",
		)
	})
}
