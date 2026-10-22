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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestCTEEvaluationIdentity(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists cte_evaluation_identity")
		for _, q := range []string{"create database cte_evaluation_identity", "use cte_evaluation_identity",
			"create table t(k int)", "insert into t values(1),(2),(2),(null)",
			"create table special(e enum('b','a'),s set('b','a'))", "insert into special values('b','b'),('a','a')"} {
			execSQLRequire(t, ctx, db, q)
		}
		for _, tc := range []struct {
			name, query string
			want        int
		}{
			{"rand", "with r as(select rand()x) select count(*) from r a join r b on a.x=b.x", 1},
			{"uuid", "with r as(select uuid()x) select count(*) from r a join r b on a.x=b.x", 1},
			{"volatile unnest argument", `with r as(select value from unnest(concat('["',uuid(),'"]'))u) select count(*) from r a join r b on a.value=b.value`, 1},
			{"union", "with r as(select 1 k,rand()x union all select 2,rand()) select count(*) from r a join r b on a.k=b.k and a.x=b.x", 2},
			{"table bag and null", "with r as(select k,uuid()x from t) select count(*) from r a join r b on a.k<=>b.k and a.x=b.x", 4},
			{"special types", "with r as(select e,s,uuid()x from special) select count(*) from r a join r b on a.e=b.e and a.s=b.s and a.x=b.x", 2},
			{"special numeric", "with r as(select e,uuid()x from special) select sum(a.e+0) from r a join r b on a.x=b.x", 3},
			{"grouped", "with r as(select k,rand()x from t group by k) select count(*) from r a join r b on a.k<=>b.k and a.x=b.x", 3},
			{"recursive", "with recursive r(n,x) as(select 1,rand() union all select n+1,x from r where n<3) select count(*) from r a join r b on a.n=b.n and a.x=b.x", 3},
			{"recursive offset", "with recursive r(n,x) as(select 1,uuid() union all select n+1,x from r where n<5 limit 2 offset 1) select count(*) from r a join r b on a.n=b.n and a.x=b.x", 2},
			{"recursive distinct", "with recursive r(n,x) as(select 1,uuid() union distinct select n,x from r) select count(*) from r a join r b on a.x=b.x", 1},
			{"recursive predecessor", "with recursive r as(select uuid()x), seq(n,x) as(select 1,x from r union all select n+1,r.x from seq join r on seq.x=r.x where n<3) select count(*) from seq", 3},
			{"transitive outer", "with r as(select uuid()x),s as(select x from r) select count(*) from s a join s b on a.x=b.x join r c on a.x=c.x", 1},
			{"transitive inner", "with s as(with r as(select uuid()x) select x from r) select count(*) from s a join s b on a.x=b.x", 1},
			{"shadowed declarations", "with r as(select uuid()x) select count(*) from r a join (with r as(select uuid()x) select x from r) b on a.x<>b.x", 1},
			{"three readers", "with r as(select uuid()x) select count(*) from r a join r b on a.x=b.x join r c on b.x=c.x", 1},
			{"early consumers", "with r as(select k,uuid()x from t where k=1) select count(*) from (select x from r limit 1)a join (select x from r limit 1)b on a.x=b.x", 1},
			{"exists", "with r as(select uuid()x from t) select count(*) from r a where exists(select 1 from r b where b.x=a.x)", 4},
			{"empty", "with r as(select uuid()x from t where false) select count(*) from r a join r b on a.x=b.x", 0},
			{"statement stable control", "with r as(select now()x) select count(*) from r a join r b on a.x=b.x", 1},
			{"unused invalid", "with unused as(select missing_column from t) select 1", 1},
			{"unused recursive", "with recursive unused(n) as(select 1 union all select n+1 from unused) select 7", 7},
			{"correlated local", "select sum(n) from (select k,(with r as(select t.k,uuid()x) select count(*) from r a join r b on a.x=b.x and a.k<=>b.k) n from t)q", 4},
			{"correlated invocation generations", "select count(distinct x) from (select (with r as(select t.k k,uuid()x) select min(a.x) from r a join r b on a.x=b.x and a.k<=>b.k) x from t)q", 4},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var got int
				require.NoError(t, db.QueryRowContext(ctx, tc.query).Scan(&got), tc.query)
				require.Equal(t, tc.want, got, tc.query)
			})
		}
		for _, tc := range []struct {
			name, query string
			want        []int
		}{
			{"late recursive producer", "with recursive b as(select rand()x),r(n) as(select 1 union all select n+1 from r,b where n<3 and b.x>=0) select count(*),sum(n) from r", []int{3, 6}},
			{"late second member", "with recursive b as(select rand()x),r(n,p) as(select 1,1 union all select n+1,p from r where n=1 union all select n+2,null from r,b where n=1 and b.x>=0) select count(*),sum(n),count(p) from r", []int{3, 6, 2}},
			{"late UUID identity", "with recursive b as(select uuid()x),r(n,x) as(select 1,cast(null as varchar(36)) union all select n+1,b.x from r,b where n<3) select count(*),sum(a.n),count(a.x),count(distinct a.x) from r a join r c on a.n=c.n and a.x<=>c.x", []int{3, 6, 2, 1}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got := make([]int, len(tc.want))
				columns := make([]any, len(got))
				for i := range got {
					columns[i] = &got[i]
				}
				require.NoError(t, db.QueryRowContext(ctx, tc.query).Scan(columns...), tc.query)
				require.Equal(t, tc.want, got, tc.query)
			})
		}
		for _, tc := range []struct{ name, query string }{
			{"stable recursive sibling programs", "select k,(with recursive r(n) as(select (select t.k) union all select n+1 from r where n<3) select (select min(n) from r)+(select max(n) from r)) v from t order by k"},
			{"stable CHAR recursive siblings", "select k,(with recursive r(n,s) as(select (select t.k),cast('a' as char(8)) union all select n+1,concat(s,'') from r where n<3) select (select min(n) from r)+(select max(n) from r)) v from t order by k"},
			{"stable series siblings", "select k,(with r as(select result k from generate_series(1,3)g) select (select min(k) from r where k>=t.k)+(select max(k) from r)) v from t order by k"},
			{"stable constant rows control", "select k,(with r as(select 1 k union all select 2 union all select 3) select (select min(k) from r where k>=t.k)+(select max(k) from r)) v from t order by k"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := db.QueryContext(ctx, tc.query)
				require.NoError(t, err)
				require.Equal(t, []string{"NULL:NULL", "1:4", "2:5", "2:5"}, readWindowNullRows(t, rows))
			})
		}
		t.Run("unsupported lifetime captures", func(t *testing.T) {
			for _, q := range []string{
				"with r as(select uuid()x) select (select max(x) from r group by t.k) from t",
				"with r as(select result k from generate_series(cast(rand()*10 as bigint),10)g) select (select max(k) from r group by t.k) from t",
				"select (with recursive r(n,x) as(select 1,cast(null as varchar(36)) union all select n+1,uuid() from r where n<t.k) select (select max(x) from r)) from t",
				"select (with recursive base as(select uuid()x),r(n,x) as(select 1,x from base union all select n+1,base.x from r join base on r.x=base.x where n<t.k) select count(*) from r) from t",
			} {
				var unused int
				err := db.QueryRowContext(ctx, q).Scan(&unused)
				require.ErrorContains(t, err, "shared CTE capture across")
				var sqlErr *mysql.MySQLError
				require.ErrorAs(t, err, &sqlErr)
				require.EqualValues(t, 20102, sqlErr.Number)
			}
		})
		t.Run("cached reader locking", func(t *testing.T) {
			for _, tc := range []struct{ query, wantError string }{
				{"with recursive r(n)as(select 1 union all select n+1 from r where n<3) select a.n from(select n from r)a join r b on a.n=b.n for update", "FOR UPDATE on a recursive CTE"},
				{"with r as(select k,uuid()x from t) select a.k from(select k from r)a join r b on a.k=b.k for update", "FOR UPDATE on a materialized CTE"},
			} {
				var unused int
				err := db.QueryRowContext(ctx, tc.query).Scan(&unused)
				require.ErrorContains(t, err, tc.wantError)
			}
		})
		t.Run("visible schema", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "with r(key_col,identity_col)as(select k,uuid()from t) select a.* from r a join r b on a.identity_col=b.identity_col")
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			require.Equal(t, []string{"key_col", "identity_col"}, columns)
			got := readWindowNullRows(t, rows)
			require.Len(t, got, 4, "private transport columns must not enter star output")
		})
		t.Run("hint cannot disable identity", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "set optimizer_hints='sharedComputation=1'")
			defer execSQLRequire(t, ctx, db, "set optimizer_hints=''")
			var n int
			require.NoError(t, db.QueryRowContext(ctx, "with r as(select uuid()x) select count(*) from r a join r b on a.x=b.x").Scan(&n))
			require.Equal(t, 1, n)
		})
		t.Run("prepared generation", func(t *testing.T) {
			stmt, err := db.PrepareContext(ctx, "with r as(select uuid()x) select a.x,b.x from r a join r b on a.x=b.x where ?")
			require.NoError(t, err)
			defer stmt.Close()
			var first string
			for _, enabled := range []bool{true, false, true} {
				var a, b string
				err := stmt.QueryRowContext(ctx, enabled).Scan(&a, &b)
				if !enabled {
					require.ErrorIs(t, err, sql.ErrNoRows)
					_, err = db.ExecContext(ctx, "select missing_column")
					require.Error(t, err)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, a, b)
				if first == "" {
					first = a
				} else {
					require.NotEqual(t, first, a, "prepared executions must not reuse materialized rows")
				}
			}
		})
		t.Run("prepared stable series", func(t *testing.T) {
			stmt, err := db.PrepareContext(ctx, "select t.k,(with r as(select result k from generate_series(?,?,1)g) select (select min(k) from r where k>=t.k)+(select max(k) from r)) v from t order by t.k")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				start, end int
				want       []string
			}{
				{1, 3, []string{"NULL:NULL", "1:4", "2:5", "2:5"}},
				{3, 1, []string{"NULL:NULL", "1:NULL", "2:NULL", "2:NULL"}},
				{2, 4, []string{"NULL:NULL", "1:6", "2:6", "2:6"}},
			} {
				rows, err := stmt.QueryContext(ctx, tc.start, tc.end)
				require.NoError(t, err)
				require.Equal(t, tc.want, readWindowNullRows(t, rows))
			}
		})
		t.Run("prepared recursive SQL mode", func(t *testing.T) {
			var mode string
			require.NoError(t, db.QueryRowContext(ctx, "select @@sql_mode").Scan(&mode))
			defer execSQLRequire(t, ctx, db, "set sql_mode='"+mode+"'")
			execSQLRequire(t, ctx, db, "set sql_mode='STRICT_TRANS_TABLES'")
			stmt, err := db.PrepareContext(ctx, "select (with recursive r(n,s)as(select (select t.k),cast('a' as char(2)) union all select n+1,concat(s,?) from r where n<3) select (select sum(n) from r)+(select max(length(s)) from r)) from t where k=1")
			require.NoError(t, err)
			defer stmt.Close()
			var got int
			err = stmt.QueryRowContext(ctx, "x").Scan(&got)
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr)
			require.EqualValues(t, 1406, sqlErr.Number)
			execSQLRequire(t, ctx, db, "set sql_mode=''")
			require.NoError(t, stmt.QueryRowContext(ctx, "x").Scan(&got))
			require.Equal(t, 8, got)
		})
	})
}
