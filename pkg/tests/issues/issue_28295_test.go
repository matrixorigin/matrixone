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
		assertBool("select (1,2) = (select 1,2 limit 0)", sql.NullBool{})
		assertBool("select (1,2) = (select 1,2 limit 1 offset 1)", sql.NullBool{})
		assertBool("select (1,2) = (select 1,2 limit 1 offset 0)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (1,2) <=> (select 1,2 limit 0)", sql.NullBool{Valid: true})
		_, err = conn.ExecContext(ctx, "create table scalar_decimal(x decimal(10,2) not null, y int not null)")
		require.NoError(t, err)
		assertBool("select (1.001,1) = (select x,y from scalar_decimal)", sql.NullBool{})
		assertBool("select (1.001,1) <> (select x,y from scalar_decimal)", sql.NullBool{})
		assertBool("select (select x,y from scalar_decimal) = (1.001,1)", sql.NullBool{})
		assertBool("select 1.001 = (select x from scalar_decimal)", sql.NullBool{})
		assertBool("select (1.001,1) <=> (select x,y from scalar_decimal)", sql.NullBool{Valid: true})
		_, err = conn.ExecContext(ctx, "insert into scalar_decimal values (1.00,1)")
		require.NoError(t, err)
		assertBool("select (1.001,1) = (select x,y from scalar_decimal)", sql.NullBool{Valid: true})
		assertBool("select (1.001,1) <> (select x,y from scalar_decimal)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (select x,y from scalar_decimal) <> (1.001,1)", sql.NullBool{Bool: true, Valid: true})
		assertBool("select (1.001,1) = (select x,y from scalar_decimal where y=2)", sql.NullBool{})
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
		for _, tc := range []struct {
			condition string
			want      int
		}{
			{"y=2", 0}, // AVG over no rows is NULL, not a passing comparison.
			{"y=1", 1}, // The matching AVG is 1.00, so only outer key 2 passes.
		} {
			err = conn.QueryRowContext(ctx, `select count(*) from scalar_outer o
			where cast(o.k as decimal(10,2)) >
			(select avg(x) from scalar_decimal where `+tc.condition+`)`).Scan(&correlatedCount)
			require.NoError(t, err)
			require.Equal(t, tc.want, correlatedCount)
		}
		_, err = conn.ExecContext(ctx, "create table scalar_inner(k int, v int)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into scalar_inner values (1,10)")
		require.NoError(t, err)
		err = conn.QueryRowContext(ctx, `select count(*) from scalar_outer o
			where (1.001,1) <> (select i.x,i.y from scalar_decimal i where i.y=o.k)`).Scan(&correlatedCount)
		require.NoError(t, err)
		require.Equal(t, 1, correlatedCount, "an unmatched scalar row must not pass WHERE <>")
		assertBool(`select (1.001,0) <> (select min(i.x),count(*) from scalar_decimal i
			where i.y<o.k) from scalar_outer o where o.k=1`, sql.NullBool{})
		assertBool(`select (1.001,0) <> (select min(i.x),count(*) from scalar_decimal i
			where i.y<o.k) from scalar_outer o where o.k=2`, sql.NullBool{Bool: true, Valid: true})
		assertBool(`select 1.001 <> (select min(i.x) from scalar_decimal i
			where i.y<o.k) from scalar_outer o where o.k=1`, sql.NullBool{})
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
		_, err = conn.ExecContext(ctx, "create table having_outer(k int)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into having_outer values (0),(1),(2)")
		require.NoError(t, err)
		assertHavingRows := func(query string, oracle func(int) string) {
			t.Helper()
			rows, queryErr := conn.QueryContext(ctx, query)
			require.NoError(t, queryErr, query)
			var got []sql.NullBool
			for rows.Next() {
				var value sql.NullBool
				require.NoError(t, rows.Scan(&value), query)
				got = append(got, value)
			}
			require.NoError(t, rows.Err(), query)
			require.NoError(t, rows.Close())
			var want []sql.NullBool
			for key := 0; key <= 2; key++ {
				var value sql.NullBool
				require.NoError(t, conn.QueryRowContext(ctx, oracle(key)).Scan(&value))
				want = append(want, value)
			}
			require.Equal(t, want, got, query)
		}
		assertHavingRows(`select (0,null) <=> (select count(*),sum(i.v)
			from scalar_inner i where i.k=o.k having o.k=1)
			from having_outer o order by o.k`, func(key int) string {
			return fmt.Sprintf(`select (0,null) <=> (select count(*),sum(i.v)
				from scalar_inner i where i.k=%d having %d=1)`, key, key)
		})
		for _, op := range []string{"=", "<>", "<", "<=", ">", ">=", "<=>"} {
			assertHavingRows(fmt.Sprintf(`select (0,null) %s (select count(*),sum(i.v)
				from scalar_inner i where i.k=o.k having count(*)<=o.k)
				from having_outer o order by o.k`, op), func(key int) string {
				return fmt.Sprintf(`select (0,null) %s (select count(*),sum(i.v)
					from scalar_inner i where i.k=%d having count(*)<=%d)`, op, key, key)
			})
		}
		assertHavingRows(`select (null,null) <=> (select count(*),sum(i.v)
			from scalar_inner i where i.k=1 having count(*)<>1 and count(*)=o.k)
			from having_outer o order by o.k`, func(key int) string {
			return fmt.Sprintf(`select (null,null) <=> (select count(*),sum(i.v)
				from scalar_inner i where i.k=1 having count(*)<>1 and count(*)=%d)`, key)
		})
		assertHavingRows(`select (null,null) <=> (select count(*),sum(i.v)
			from scalar_inner i where i.k=1 having o.k=1)
			from having_outer o order by o.k`, func(key int) string {
			return fmt.Sprintf(`select (null,null) <=> (select count(*),sum(i.v)
				from scalar_inner i where i.k=1 having %d=1)`, key)
		})
		assertBool(`select (select count(*),sum(i.v) from scalar_inner i
			where i.k=o.k having o.k=1) <=> (null,null)
			from having_outer o where o.k=0`, sql.NullBool{Bool: true, Valid: true})
		_, err = conn.ExecContext(ctx, "insert into having_outer values (null),(1)")
		require.NoError(t, err)
		assertBool(`select (null,null) <=> (select count(*),sum(i.v)
			from scalar_inner i where i.k=o.k having count(*)=o.k)
			from having_outer o where o.k is null`, sql.NullBool{Bool: true, Valid: true})
		var repeatedMatches int
		require.NoError(t, conn.QueryRowContext(ctx, `select count(*) from having_outer o
			where (1,10) <=> (select count(*),sum(i.v) from scalar_inner i
				where i.k=o.k having count(*)=o.k)`).Scan(&repeatedMatches))
		require.Equal(t, 2, repeatedMatches)
		preparedHaving, err := conn.PrepareContext(ctx, `select (0,null) <=>
			(select count(*),sum(i.v) from scalar_inner i where i.k=o.k
				having count(*)<=? and o.k>=0)
			from having_outer o where o.k=? limit 1`)
		require.NoError(t, err)
		for _, tc := range []struct {
			threshold int
			key       int
			want      bool
		}{{0, 0, true}, {0, 1, false}, {0, 2, true}, {-1, 0, false}, {0, 0, true}} {
			var got sql.NullBool
			require.NoError(t, preparedHaving.QueryRowContext(ctx, tc.threshold, tc.key).Scan(&got))
			require.Equal(t, sql.NullBool{Bool: tc.want, Valid: true}, got)
		}
		require.NoError(t, preparedHaving.Close())
		require.ErrorContains(t, drainQueryError(ctx, conn, `select (0,null) <=>
			(select count(*),sum(i.v) from scalar_inner i where i.k<o.k having count(*)=o.k)
			from having_outer o`), "not yet implemented")
		require.ErrorContains(t, drainQueryError(ctx, conn, `select (0,null) <=>
			(select count(*),sum(i.v) from scalar_inner i where i.k=o.k
				group by i.k having count(*)=o.k)
			from having_outer o`), "not yet implemented")
		require.ErrorContains(t, drainQueryError(ctx, conn, `select (0,null) <=>
			(select count(*),sum(i.v) from scalar_inner i where i.k=o.k
				having count(*)<=o.k and (select max(v) from scalar_inner where k=1)>0)
			from having_outer o`), "not yet implemented")
		assertBool("select (1,10) <=> (select count(*),sum(i.v) from scalar_inner i where i.k=1)",
			sql.NullBool{Bool: true, Valid: true})
		_, err = conn.ExecContext(ctx, "create sequence row_scalar_guard_seq")
		require.NoError(t, err)
		require.ErrorContains(t, drainQueryError(ctx, conn,
			"select (nextval('row_scalar_guard_seq'),0) < (select 1,1)"),
			"volatile row ordering comparison")
		require.ErrorContains(t, drainQueryError(ctx, conn, `select (0,null) <=>
			(select count(*),sum(i.v) from scalar_inner i where i.k=o.k+100
			 having nextval('row_scalar_guard_seq')=1)
			from scalar_outer o where o.k=1`), "volatile correlated scalar HAVING")
		var firstSequenceValue int64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select nextval('row_scalar_guard_seq')").Scan(&firstSequenceValue))
		require.Equal(t, int64(1), firstSequenceValue)
		var ordinaryScalarCount int64
		require.NoError(t, conn.QueryRowContext(ctx, `select (select count(*) from scalar_inner i
			where i.k=o.k having nextval('row_scalar_guard_seq')=2)
			from scalar_outer o where o.k=1`).Scan(&ordinaryScalarCount))
		require.Equal(t, int64(1), ordinaryScalarCount)
		ifNullValues := func() []int {
			rows, queryErr := conn.QueryContext(ctx, `select ifnull(
				(select min(i.v) from scalar_inner i where i.k=o.k), 0),
				ifnull(cast((select min(i.v) from scalar_inner i where i.k=o.k) as bigint), 0)
				from scalar_outer o order by o.k`)
			require.NoError(t, queryErr)
			defer func() { require.NoError(t, rows.Close()) }()
			columnTypes, queryErr := rows.ColumnTypes()
			require.NoError(t, queryErr)
			require.Len(t, columnTypes, 2)
			for _, columnType := range columnTypes {
				nullable, ok := columnType.Nullable()
				require.True(t, ok)
				require.False(t, nullable)
			}
			var values []int
			for rows.Next() {
				var value, castValue int
				require.NoError(t, rows.Scan(&value, &castValue))
				require.Equal(t, value, castValue)
				values = append(values, value)
			}
			require.NoError(t, rows.Err())
			return values
		}()
		require.Equal(t, []int{10, 0}, ifNullValues)
		_, err = conn.ExecContext(ctx, "create table ifnull_outer(k int primary key)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into ifnull_outer values (1),(2),(3)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "create table ifnull_inner(k int, v int)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into ifnull_inner values (1,10),(1,20),(2,null)")
		require.NoError(t, err)
		ifNullSelected := func() []int {
			rows, queryErr := conn.QueryContext(ctx, `select o.k,
				ifnull((select min(i.v) from ifnull_inner i where i.k<o.k),0)
				from ifnull_outer o order by o.k`)
			require.NoError(t, queryErr)
			defer func() { require.NoError(t, rows.Close()) }()
			var values []int
			for rows.Next() {
				var key, value int
				require.NoError(t, rows.Scan(&key, &value))
				values = append(values, value)
			}
			require.NoError(t, rows.Err())
			return values
		}()
		require.Equal(t, []int{0, 10, 10}, ifNullSelected)
		for _, test := range []struct {
			query string
			want  int
		}{
			{"select (select ifnull((select 2),0)) + ifnull((select 3),0)", 5},
			{"select ifnull((select 3),0) + (select ifnull((select 2),0))", 5},
			{"select (select ifnull((select 2),0)) + (select ifnull((select 3),0))", 5},
			{"select x + ifnull((select 3),0) from (select ifnull((select 2),0) x) d", 5},
			{"select ifnull((select 2),0) + ifnull((select 3),0)", 5},
		} {
			var got int
			require.NoError(t, conn.QueryRowContext(ctx, test.query).Scan(&got), test.query)
			require.Equal(t, test.want, got, test.query)
		}
		var ifNullStrings string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select concat((select ifnull((select 'left'),'')),ifnull((select 'right'),''))").Scan(&ifNullStrings))
		require.Equal(t, "leftright", ifNullStrings)
		rawMin := func() []sql.NullInt64 {
			rows, queryErr := conn.QueryContext(ctx, `select
				(select min(i.v) from ifnull_inner i where i.k<o.k)
				from ifnull_outer o order by o.k`)
			require.NoError(t, queryErr)
			defer func() { require.NoError(t, rows.Close()) }()
			var values []sql.NullInt64
			for rows.Next() {
				var value sql.NullInt64
				require.NoError(t, rows.Scan(&value))
				values = append(values, value)
			}
			require.NoError(t, rows.Err())
			return values
		}()
		require.Equal(t, []sql.NullInt64{{}, {Int64: 10, Valid: true}, {Int64: 10, Valid: true}}, rawMin)
		err = conn.QueryRowContext(ctx, `select count(*) from scalar_outer o
			where (1,10) =
				(select count(*),sum(v) from scalar_inner i where i.k<o.k)`).Scan(&correlatedCount)
		require.NoError(t, err)
		require.Equal(t, 1, correlatedCount)
		// The non-equality aggregate fallback must discard the synthetic LEFT JOIN row.
		assertBool(`select (0,null) <=> (select count(1),sum(i.v)
			from scalar_inner i where i.k<o.k) from scalar_outer o where o.k=1`,
			sql.NullBool{Bool: true, Valid: true})
		assertBool(`select (1,10) <=> (select count(1),sum(i.v)
			from scalar_inner i where i.k<o.k) from scalar_outer o where o.k=2`,
			sql.NullBool{Bool: true, Valid: true})
		var emptyCount int
		require.NoError(t, conn.QueryRowContext(ctx, `select (select count(1)
			from scalar_inner i where i.k<o.k) from scalar_outer o where o.k=1`).Scan(&emptyCount))
		require.Zero(t, emptyCount)
		for _, query := range []string{
			`select (0,null) <=> (select count(*),sum(i.v)
				from scalar_inner i where i.k<o.k limit 0) from scalar_outer o where o.k=1`,
			`select (0,null) <=> (select sum(coalesce(i.v,0)),sum(i.v)
				from scalar_inner i where i.k<o.k) from scalar_outer o where o.k=1`,
			`select (o.k,(select v from scalar_inner where k=1)) =
				(select count(*),sum(i.v) from scalar_inner i where i.k<o.k)
				from scalar_outer o where o.k=1`,
			`select (0,null) <=> (select count(*),sum(i.v) from scalar_inner i where i.k<o.k),
				(0,null) <=> (select count(*),sum(i.v) from scalar_inner i where i.k<o.k)
				from scalar_outer o where o.k=1`,
		} {
			require.ErrorContains(t, drainQueryError(ctx, conn, query), "not yet implemented")
		}
		assertBool(`select (o.k,(select 10)) =
			(select count(*),sum(i.v) from scalar_inner i where i.k<o.k)
			from scalar_outer o where o.k=2`, sql.NullBool{Valid: true})
		require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&emptyCount))
		require.Equal(t, 1, emptyCount)
		_, err = conn.ExecContext(ctx, "insert into scalar_inner values (1,null)")
		require.NoError(t, err)
		assertBool(`select (1,2) <=> (select count(distinct 1),sum(1)
			from scalar_inner i where i.k<o.k) from scalar_outer o where o.k=2`,
			sql.NullBool{Bool: true, Valid: true})
		assertBool(`select (0,null) <=> (select count(distinct 1),sum(1)
			from scalar_inner i where i.k<o.k) from scalar_outer o where o.k=1`,
			sql.NullBool{Bool: true, Valid: true})
		assertBool(`select (1,0,null) <=> (select count(*),count(i.v),sum(i.v)
			from scalar_inner i where i.k<o.k and i.v is null)
			from scalar_outer o where o.k=2`, sql.NullBool{Bool: true, Valid: true})
		_, err = conn.ExecContext(ctx, "insert into scalar_inner values (1,20),(1,10)")
		require.NoError(t, err)
		var concat sql.NullString
		require.NoError(t, conn.QueryRowContext(ctx, `select
			(select group_concat(i.v order by i.v desc separator '~')
			 from scalar_inner i where i.k<o.k) from scalar_outer o where o.k=1`).Scan(&concat))
		require.Equal(t, sql.NullString{}, concat)
		require.NoError(t, conn.QueryRowContext(ctx, `select
			(select group_concat(distinct i.v order by i.v desc separator '~')
			 from scalar_inner i where i.k<o.k) from scalar_outer o where o.k=2`).Scan(&concat))
		require.Equal(t, sql.NullString{String: "20~10", Valid: true}, concat)
		require.NoError(t, conn.QueryRowContext(ctx, `select
			(select group_concat(1 order by i.v desc separator '~')
			 from scalar_inner i where i.k<o.k) from scalar_outer o where o.k=2`).Scan(&concat))
		require.Equal(t, sql.NullString{String: "1~1~1~1", Valid: true}, concat)
		_, err = conn.ExecContext(ctx, "create table scalar_outer_dup(k int)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into scalar_outer_dup values (2),(2)")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, `select count(*) from scalar_outer_dup o
			where (4,40) <=> (select count(1),sum(i.v) from scalar_inner i where i.k<o.k)`).Scan(&emptyCount))
		require.Equal(t, 2, emptyCount)

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
