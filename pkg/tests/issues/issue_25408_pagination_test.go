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
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

func TestIssue25408PreparedPaginationParameters(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		const dbName = "issue_25408_pagination"
		execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+dbName)
		}()
		execSQLRequire(t, ctx, db, "create database "+dbName)
		execSQLRequire(t, ctx, db, "create table "+dbName+".page(id int)")
		execSQLRequire(t, ctx, db, "insert into "+dbName+".page values (1),(2),(3)")

		assertRows := func(t *testing.T, query string, want ...int) {
			t.Helper()
			rows, queryErr := db.QueryContext(ctx, query)
			require.NoError(t, queryErr)
			defer rows.Close()
			var actual []int
			for rows.Next() {
				var value int
				require.NoError(t, rows.Scan(&value))
				actual = append(actual, value)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, actual)
		}
		assertMySQLError := func(t *testing.T, err error, number uint16) {
			t.Helper()
			require.Error(t, err)
			mysqlErr, ok := err.(*mysqlDriver.MySQLError)
			require.True(t, ok)
			require.Equal(t, number, mysqlErr.Number)
		}

		t.Run("SQL PREPARE reuse", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "prepare issue25408_page from 'select id from "+dbName+".page order by id limit ? offset ?'")
			defer execSQLMaybe(t, context.Background(), db, "deallocate prepare issue25408_page")

			execSQLRequire(t, ctx, db, "set @lim=2,@off=1")
			assertRows(t, "execute issue25408_page using @lim,@off", 2, 3)
			execSQLRequire(t, ctx, db, "set @lim=null,@off=1")
			assertRows(t, "execute issue25408_page using @lim,@off")
			execSQLRequire(t, ctx, db, "set @lim=2,@off=null")
			assertRows(t, "execute issue25408_page using @lim,@off", 1, 2)
			execSQLRequire(t, ctx, db, "set @lim=true,@off=0")
			assertRows(t, "execute issue25408_page using @lim,@off", 1)

			execSQLRequire(t, ctx, db, "set @lim='1',@off=0")
			_, err = db.ExecContext(ctx, "execute issue25408_page using @lim,@off")
			assertMySQLError(t, err, 1210)
			execSQLRequire(t, ctx, db, "set @lim=-1,@off=0")
			_, err = db.ExecContext(ctx, "execute issue25408_page using @lim,@off")
			assertMySQLError(t, err, 1690)
		})

		for index, paginationSQL := range []string{
			"select id from " + dbName + ".page order by id limit ? offset ?",
			"select id from " + dbName + ".page order by id limit ?, ?",
		} {
			t.Run(fmt.Sprintf("SQL PREPARE error priority %d", index), func(t *testing.T) {
				name := fmt.Sprintf("issue25408_priority_%d", index)
				execSQLRequire(t, ctx, db, "prepare "+name+" from '"+paginationSQL+"'")
				defer execSQLMaybe(t, context.Background(), db, "deallocate prepare "+name)
				execSQLRequire(t, ctx, db, "set @first='1',@second=-1")
				_, executeErr := db.ExecContext(ctx, "execute "+name+" using @first,@second")
				assertMySQLError(t, executeErr, 1210)
			})
		}

		t.Run("COM_STMT and CTAS", func(t *testing.T) {
			stmt, prepareErr := db.PrepareContext(ctx,
				"select id from "+dbName+".page order by id limit ? offset ?")
			require.NoError(t, prepareErr)
			defer stmt.Close()

			func() {
				rows, queryErr := stmt.QueryContext(ctx, int64(1), int64(1))
				require.NoError(t, queryErr)
				defer rows.Close()
				require.True(t, rows.Next())
				var id int
				require.NoError(t, rows.Scan(&id))
				require.Equal(t, 2, id)
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}()
			_, err = stmt.ExecContext(ctx, "1", int64(0))
			assertMySQLError(t, err, 1210)
			_, err = stmt.ExecContext(ctx, int64(-1), int64(0))
			assertMySQLError(t, err, 1690)

			ctas, prepareErr := db.PrepareContext(ctx,
				"create table "+dbName+".bad_page as select 1 limit ?")
			require.NoError(t, prepareErr)
			defer ctas.Close()
			_, err = ctas.ExecContext(ctx, "1")
			assertMySQLError(t, err, 1210)
		})

		for index, paginationSQL := range []string{
			"select id from " + dbName + ".page order by id limit ? offset ?",
			"select id from " + dbName + ".page order by id limit ?, ?",
		} {
			t.Run(fmt.Sprintf("COM_STMT error priority %d", index), func(t *testing.T) {
				stmt, prepareErr := db.PrepareContext(ctx, paginationSQL)
				require.NoError(t, prepareErr)
				defer stmt.Close()
				_, executeErr := stmt.ExecContext(ctx, "1", int64(-1))
				assertMySQLError(t, executeErr, 1210)
			})
		}
	})
}
