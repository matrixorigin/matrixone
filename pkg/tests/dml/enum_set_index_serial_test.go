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

package dml

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIndexBackfillPreservesPublicSerial(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		database := strings.ToLower(testutils.GetDatabaseName(t))
		quoted := "`" + strings.ReplaceAll(database, "`", "``") + "`"
		exec := func(query string) sql.Result {
			t.Helper()
			result, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
			return result
		}
		count := func(query string) int {
			t.Helper()
			var n int
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&n), query)
			return n
		}
		exec("create database " + quoted)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanup, "drop database "+quoted)
			require.NoError(t, err)
		}()
		exec("use " + quoted)
		exec("create table t(id int primary key, e enum('new','paid'), s set('a','b'))")
		exec("insert into t values (1,'paid','a'),(2,'new','b')")

		checkPublicSerial := func() {
			t.Helper()
			var enumValue, setValue string
			var enumEqual, setEqual, fullEqual int
			err := conn.QueryRowContext(ctx, `select
				serial_extract(serial(e),0 as varchar(20)),
				serial_extract(serial(s),0 as varchar(20)),
				serial(e)=serial('paid'), serial(s)=serial('a'),
				serial_full(e,s)=serial_full('paid','a') from t where id=1`).
				Scan(&enumValue, &setValue, &enumEqual, &setEqual, &fullEqual)
			require.NoError(t, err)
			require.Equal(t, "paid", enumValue)
			require.Equal(t, "a", setValue)
			require.Equal(t, 1, enumEqual)
			require.Equal(t, 1, setEqual)
			require.Equal(t, 1, fullEqual)
		}
		checkPublicSerial()
		exec("alter table t add key ix(e,s)")
		checkPublicSerial()

		result := exec("delete from t where id=2")
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, affected)
		checkPublicSerial()

		exec("create table dup(id int primary key, e enum('new','paid'))")
		exec("insert into dup values (1,'paid'),(2,'paid')")
		_, err = conn.ExecContext(ctx, "alter table dup add unique key ux(e)")
		var duplicate *mysql.MySQLError
		require.ErrorAs(t, err, &duplicate)
		require.EqualValues(t, 1062, duplicate.Number)
		require.Zero(t, count(`select count(*) from mo_catalog.mo_indexes i
join mo_catalog.mo_tables t on i.table_id=t.rel_id
where t.reldatabase=database() and t.relname='dup' and i.name='ux'`))
		checkPublicSerial()

		exec("create table dup_prefix(id int primary key, e enum('new','paid'), v varchar(8))")
		exec("insert into dup_prefix values (1,'paid','abcd'),(2,'paid','abef')")
		_, err = conn.ExecContext(ctx, "alter table dup_prefix add unique key ux(e,v(2))")
		require.ErrorAs(t, err, &duplicate)
		require.EqualValues(t, 1062, duplicate.Number)
		exec("create table dup_null(id int primary key, e enum('new','paid'), v varchar(8))")
		exec("insert into dup_null values (1,'paid',null),(2,'paid',null)")
		exec("alter table dup_null add unique key ux(e,v)")
		checkPublicSerial()

		// Legacy writers and index backfill disagree on prefixes of the
		// special column itself. Reject those definitions before any DDL write.
		exec("create table prefix_guard(id int primary key, e enum('blue','black'), s set('a','b'), v varchar(8))")
		exec("insert into prefix_guard values (1,'blue','a','apple'),(2,'black','b','ant')")
		hiddenBefore := count("select count(*) from mo_catalog.mo_tables where reldatabase=database() and relname like '__mo_index_%'")
		for _, query := range []string{
			"create table bad_inline(id int primary key, e enum('blue','black'), v int, unique key ux(e(1),v))",
			"create table bad_before(key ix(e(1)), id int primary key, e enum('blue','black'))",
			"create table bad_empty(id int primary key, e enum(''), key ix(e(1)))",
			"create table bad_set(id int primary key, s set('a','b'), key ix(s(1)))",
			"alter table prefix_guard add unique key ux(e(1),v)",
			"create unique index ux on prefix_guard(e(1),v)",
			"create index sx on prefix_guard(s(1))",
		} {
			_, err = conn.ExecContext(ctx, query)
			require.ErrorContains(t, err, "index prefix on ENUM or SET", query)
			require.Equal(t, 2, count("select count(*) from prefix_guard use index()"))
			require.Equal(t, hiddenBefore, count("select count(*) from mo_catalog.mo_tables where reldatabase=database() and relname like '__mo_index_%'"))
		}
		require.Zero(t, count(`select count(*) from mo_catalog.mo_tables
where reldatabase=database() and relname in ('bad_inline','bad_before','bad_empty','bad_set')`))
		// Existing (0) spelling has always meant the complete key.
		exec("alter table prefix_guard add key ex(e(0))")

		// A prefix on an ordinary sibling column remains supported.
		exec("alter table prefix_guard add unique key ux(e,v(1))")
		_, err = conn.ExecContext(ctx, "insert into prefix_guard values (3,'blue','a','apex')")
		require.ErrorAs(t, err, &duplicate)
		require.EqualValues(t, 1062, duplicate.Number)
		exec("insert into prefix_guard values (4,'blue','a',null),(5,'blue','b',null)")
		exec("delete from prefix_guard where id=1")
		exec("insert into prefix_guard values (3,'blue','a','apex')")
		var prefixIndex string
		err = conn.QueryRowContext(ctx, `select distinct i.index_table_name
from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id
where t.reldatabase=database() and t.relname='prefix_guard' and i.name='ux'`).Scan(&prefixIndex)
		require.NoError(t, err)
		prefixIndex = "`" + strings.ReplaceAll(prefixIndex, "`", "``") + "`"
		require.Zero(t, count("select count(*) from "+prefixIndex+" where __mo_index_pri_col=1"))
		require.Equal(t, 1, count("select count(*) from "+prefixIndex+" where __mo_index_pri_col=3"))
		checkPublicSerial()

		// An empty SET member and an empty bitmap have the same display value.
		// Updating either the SET or an unrelated column must keep the old bitmap.
		exec("create table update_set(id int primary key, s set('','a'), n int, key ix(s))")
		exec("insert into update_set values (1,1,0),(2,1,0)")
		result = exec("update update_set set n=9 where id=1")
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, affected)
		var bitmap, n int
		require.NoError(t, conn.QueryRowContext(ctx, "select s+0,n from update_set where id=1").Scan(&bitmap, &n))
		require.Equal(t, 1, bitmap)
		require.Equal(t, 9, n)
		result = exec("update update_set set s=2 where id=2")
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, affected)
		require.NoError(t, conn.QueryRowContext(ctx, "select s+0 from update_set where id=2").Scan(&bitmap))
		require.Equal(t, 2, bitmap)

		// Multi-target UPDATE takes the legacy planner route.
		exec("create table update_aux(id int primary key, n int)")
		exec("insert into update_aux values (1,0)")
		result = exec("update update_set a join update_aux b on a.id=b.id set a.n=8,b.n=7 where a.id=1")
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 2, affected)
		require.NoError(t, conn.QueryRowContext(ctx, "select s+0,n from update_set where id=1").Scan(&bitmap, &n))
		require.Equal(t, 1, bitmap)
		require.Equal(t, 8, n)

		// Older catalogs can contain an ENUM in a table-level composite PK.
		// MASTER stores that PK in both its key and owner during backfill.
		for _, tc := range []struct {
			name, ddl, values string
		}{
			{"master_special", "primary key(e,id), e enum('a','b'), id int, v varchar(8)", "('a',1,'one'),('b',2,'two')"},
			{"master_regular", "id int, n int, v varchar(8), primary key(id,n)", "(1,1,'one'),(2,2,'two')"},
		} {
			exec("create table " + tc.name + " (" + tc.ddl + ")")
			exec("insert into " + tc.name + " values " + tc.values)
			exec("create index mi using master on " + tc.name + "(v)")
			var indexName string
			require.NoError(t, conn.QueryRowContext(ctx, `select distinct i.index_table_name
from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id
where t.reldatabase=? and t.relname=? and i.name='mi'`, database, tc.name).Scan(&indexName))
			indexName = "`" + strings.ReplaceAll(indexName, "`", "``") + "`"
			ownerJoin := "select count(*) from " + tc.name + " b join " + indexName + " i on b.__mo_cpkey_col=i.__mo_index_pri_col"
			require.Equal(t, 2, count(ownerJoin), tc.name)
			result := exec("delete from " + tc.name + " where id=1")
			affected, err := result.RowsAffected()
			require.NoError(t, err)
			require.EqualValues(t, 1, affected, tc.name)
			require.Equal(t, 1, count("select count(*) from "+indexName), tc.name)
			require.Equal(t, 1, count(ownerJoin), tc.name)
		}
		checkPublicSerial()
	})
}
