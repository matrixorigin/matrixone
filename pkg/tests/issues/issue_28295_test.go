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

	mysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue28295RowConstructorScalarSubquery(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer admin.Close()
		conn, err := admin.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		database := testutils.GetDatabaseName(t)
		_, err = conn.ExecContext(ctx, "create database `"+database+"`")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists `"+database+"`")
		}()
		_, err = conn.ExecContext(ctx, "use `"+database+"`")
		require.NoError(t, err)

		_, err = conn.ExecContext(ctx, "create table scalar_rows(id int primary key, a int, b int)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into scalar_rows values (1,1,5),(2,1,null),(3,2,8),(4,2,10)")
		require.NoError(t, err)

		assertBool := func(query string, want sql.NullBool) {
			t.Helper()
			var got sql.NullBool
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&got))
			require.Equal(t, want, got)
		}
		assertBool("select (1,5) = (select a,b from scalar_rows where id=1)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (1,4) <> (select a,b from scalar_rows where id=1)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (1,4) < (select a,b from scalar_rows where id=1)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (2,8) <= (select a,b from scalar_rows where id=3)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (select a,b from scalar_rows where id=4) > (2,8)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (2,10) >= (select a,b from scalar_rows where id=4)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (1,5) = (select a,b from scalar_rows where id=99)", sql.NullBool{})
		assertBool("select (1,5) = (select a,b from scalar_rows where id=2)", sql.NullBool{})
		assertBool("select (0,5) = (select a,b from scalar_rows where id=2)", sql.NullBool{Valid: true})
		assertBool("select (1,null) <=> (select a,b from scalar_rows where id=2)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (1,5) <=> (select a,b from scalar_rows where id=99)", sql.NullBool{Valid: true})
		assertBool("select (1,'5') = (select a,b from scalar_rows where id=1)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (1,(select 5)) = (select a,b from scalar_rows where id=1)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (select a,b from scalar_rows where id=1) = ((select 1),5)", sql.NullBool{Bool: true, Valid: true})

		var correlatedCount int
		err = conn.QueryRowContext(ctx, `select count(*) from scalar_rows outer_row
			where (outer_row.a, outer_row.b) =
				(select inner_row.a, inner_row.b from scalar_rows inner_row where inner_row.id = outer_row.id)`).Scan(&correlatedCount)
		require.NoError(t, err)
		require.Equal(t, 3, correlatedCount)

		_, err = conn.ExecContext(ctx, "create table scalar_outer(k int primary key)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into scalar_outer values (1),(2)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "create table scalar_inner(k int, v int)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into scalar_inner values (1,10)")
		require.NoError(t, err)
		assertBool(`select (1,10) =
			(select count(*),sum(v) from scalar_inner i where i.k=o.k)
			from scalar_outer o where o.k=2`, sql.NullBool{Valid: true})
		assertBool(`select (0,null) <=>
			(select count(*),sum(v) from scalar_inner i where i.k=o.k)
			from scalar_outer o where o.k=2`, sql.NullBool{Bool: true, Valid: true})
		assertBool(`select (1,1) =
			(select o.k,i.k from scalar_inner i where i.k=o.k)
			from scalar_outer o where o.k=1`, sql.NullBool{Bool: true, Valid: true})
		assertBool(`select (1,1) =
			(select i.k,o.k from scalar_inner i where i.k=o.k)
			from scalar_outer o where o.k=1`, sql.NullBool{Bool: true, Valid: true})
		assertBool(`select (1,7) =
			(select o.k,7 from scalar_inner i where i.k=o.k)
			from scalar_outer o where o.k=1`, sql.NullBool{Bool: true, Valid: true})
		assertBool(`select (2,7) =
			(select o.k,7 from scalar_inner i where i.k=o.k)
			from scalar_outer o where o.k=2`, sql.NullBool{})
		assertBool(`select (1,7) =
			(select o.k,7 from scalar_inner i where i.k=o.k limit 1)
			from scalar_outer o where o.k=1`, sql.NullBool{Bool: true, Valid: true})
		assertBool(`select (1,7) =
			(select distinct o.k,7 from scalar_inner i where i.k=o.k)
			from scalar_outer o where o.k=1`, sql.NullBool{Bool: true, Valid: true})

		assertBool(`select (0,null) <=>
			(select count(*),sum(v) from scalar_inner i where i.k=o.k having count(*)=0)
			from scalar_outer o where o.k=2`, sql.NullBool{Bool: true, Valid: true})
		ifNullValues := func() []int {
			rows, queryErr := conn.QueryContext(ctx, `select ifnull(
				(select min(i.v) from scalar_inner i where i.k=o.k), 0)
				from scalar_outer o order by o.k`)
			require.NoError(t, queryErr)
			defer func() { require.NoError(t, rows.Close()) }()
			columnTypes, queryErr := rows.ColumnTypes()
			require.NoError(t, queryErr)
			require.Len(t, columnTypes, 1)
			nullable, ok := columnTypes[0].Nullable()
			require.True(t, ok)
			require.False(t, nullable)
			var values []int
			for rows.Next() {
				var value int
				require.NoError(t, rows.Scan(&value))
				values = append(values, value)
			}
			require.NoError(t, rows.Err())
			return values
		}()
		require.Equal(t, []int{10, 0}, ifNullValues)
		err = conn.QueryRowContext(ctx, `select count(*) from scalar_outer o
			where (1,10) =
				(select count(*),sum(v) from scalar_inner i where i.k<o.k)`).Scan(&correlatedCount)
		require.NoError(t, err)
		require.Equal(t, 1, correlatedCount)

		queryErr := drainQueryError(ctx, conn,
			"select (1,5) = (select a,b from scalar_rows where id > 0)")
		assertSubqueryCardinalityError(t, queryErr)

		_, err = conn.ExecContext(ctx, "set @issue28295_a = 1, @issue28295_b = 5")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx,
			"prepare issue28295_text from 'select (?,?) = (select a,b from scalar_rows where id=1)'")
		require.NoError(t, err)
		defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare issue28295_text") }()
		assertBool("execute issue28295_text using @issue28295_a, @issue28295_b", sql.NullBool{Bool: true, Valid: true})

		prepared, err := conn.PrepareContext(ctx,
			"select (?,?) = (select a,b from scalar_rows where id=1)")
		require.NoError(t, err)
		defer prepared.Close()
		var preparedResult sql.NullBool
		require.NoError(t, prepared.QueryRowContext(ctx, int64(1), int64(5)).Scan(&preparedResult))
		require.Equal(t, sql.NullBool{Bool: true, Valid: true}, preparedResult)
	})
}

func drainQueryError(ctx context.Context, conn *sql.Conn, query string) error {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}

func assertSubqueryCardinalityError(t *testing.T, err error) {
	t.Helper()
	require.ErrorContains(t, err, "Subquery returns more than 1 row")
	var mysqlErr *mysql.MySQLError
	require.ErrorAs(t, err, &mysqlErr)
	require.Equal(t, uint16(1242), mysqlErr.Number)
	require.Equal(t, "21000", string(mysqlErr.SQLState[:]))
}
