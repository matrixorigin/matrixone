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

	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue26866BinaryPreparedNthValueOffsetValidation(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
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

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()

		mustExec(t, ctx, conn, "create table t (id int primary key, grp int)")
		mustExec(t, ctx, conn, "insert into t values (1,1),(2,1),(3,1),(4,2),(5,2)")
		stmt, err := conn.PrepareContext(ctx, `select id, nth_value(id, ?) over (
			partition by grp order by id
			rows between unbounded preceding and unbounded following
		) from t order by grp, id`)
		require.NoError(t, err)
		defer stmt.Close()

		assertRows := func(offset any, expected []int64) {
			t.Helper()
			rows, err := stmt.QueryContext(ctx, offset)
			require.NoError(t, err)
			defer rows.Close()
			var actual []int64
			for rows.Next() {
				var id, nth int64
				require.NoError(t, rows.Scan(&id, &nth))
				actual = append(actual, id, nth)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, expected, actual)
		}

		assertRows(int64(2), []int64{1, 2, 2, 2, 3, 2, 4, 5, 5, 5})
		for _, test := range []struct {
			name  string
			value any
		}{
			{name: "zero", value: int64(0)},
			{name: "negative", value: int64(-1)},
			{name: "null", value: nil},
			{name: "float", value: float64(2.5)},
			{name: "string", value: "2"},
		} {
			t.Run(test.name, func(t *testing.T) {
				_, err := stmt.ExecContext(ctx, test.value)
				require.Error(t, err)
				var mysqlErr *mysql.MySQLError
				require.ErrorAs(t, err, &mysqlErr)
				require.Equal(t, uint16(1210), mysqlErr.Number)
				require.Contains(t, mysqlErr.Message, "Incorrect arguments to nth_value")
			})
		}

		// Rejected executions must leave the prepared handle reusable.
		assertRows(int64(1), []int64{1, 1, 2, 1, 3, 1, 4, 4, 5, 4})
	})
}
