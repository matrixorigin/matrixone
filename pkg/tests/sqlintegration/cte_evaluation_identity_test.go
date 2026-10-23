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
			"create table dst(x varchar(100))",
			"create table special(e enum('b','a'),s set('b','a'))", "insert into special values('b','b'),('a','a')"} {
			execSQLRequire(t, ctx, db, q)
		}
		t.Run("single reference UNION demand", func(t *testing.T) {
			for _, tc := range []struct{ name, query string }{
				{"derived control", `select x from (select uuid() x union all select extractvalue('<a/>','[')) r limit 1`},
				{"CTE", `with r(x) as(select uuid() union all select extractvalue('<a/>','[')) select x from r limit 1`},
				{"chain", `with r(x) as(select uuid() union all select extractvalue('<a/>','[')),s as(select x from r) select x from s limit 1`},
				{"nested", `with r as(with s(x) as(select uuid() union all select extractvalue('<a/>','[')) select x from s) select x from r limit 1`},
				{"offset", `with r(x) as(select 'skip' union all select uuid() union all select extractvalue('<a/>','[')) select x from r limit 1 offset 1`},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var got string
					require.NoError(t, db.QueryRowContext(ctx, tc.query).Scan(&got), tc.query)
					require.Len(t, got, 36, tc.query)
				})
			}
			query := `with r(x) as(select uuid() union all select extractvalue('<a/>','[')) select x from r`
			var got string
			require.ErrorIs(t, db.QueryRowContext(ctx, query+" limit 0").Scan(&got), sql.ErrNoRows)
			for _, suffix := range []string{"", " order by x limit 1"} {
				var sqlErr *mysql.MySQLError
				require.ErrorAs(t, db.QueryRowContext(ctx, query+suffix).Scan(&got), &sqlErr)
				require.EqualValues(t, 20105, sqlErr.Number)
			}
			stmt, err := db.PrepareContext(ctx, query+" limit ?")
			require.NoError(t, err)
			defer stmt.Close()
			for _, limit := range []int{1, 2, 1} {
				err = func() error {
					rows, err := stmt.QueryContext(ctx, limit)
					if err != nil {
						return err
					}
					defer rows.Close()
					count := 0
					for rows.Next() {
						require.NoError(t, rows.Scan(&got))
						require.Len(t, got, 36)
						count++
					}
					if limit == 1 {
						require.Equal(t, 1, count)
					}
					return rows.Err()
				}()
				if limit == 1 {
					require.NoError(t, err)
				} else {
					var sqlErr *mysql.MySQLError
					require.ErrorAs(t, err, &sqlErr)
					require.EqualValues(t, 20105, sqlErr.Number)
				}
			}
		})
		t.Run("DML UNION demand", func(t *testing.T) {
			const source = `with r(x) as(select uuid() union all select extractvalue('<a/>','[')) select x from r`
			assertCount := func(t *testing.T, want int) {
				t.Helper()
				var count, valid int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*),count(case when length(x)=36 then 1 end) from dst").Scan(&count, &valid))
				require.Equal(t, want, count)
				require.Equal(t, want, valid)
			}
			for _, tc := range []struct {
				name, query string
				want        int64
			}{
				{"derived control", `insert into dst select x from(select uuid()x union all select extractvalue('<a/>','['))r limit 1`, 1},
				{"insert", "insert into dst " + source + " limit 1", 1},
				{"replace", "replace into dst " + source + " limit 1", 1},
				{"zero", "insert into dst " + source + " limit 0", 0},
				{"chain", `insert into dst with r(x) as(select uuid() union all select extractvalue('<a/>','[')),s as(select x from r) select x from s limit 1`, 1},
				{"offset", `insert into dst with r(x) as(select 'skip' union all select uuid() union all select extractvalue('<a/>','[')) select x from r limit 1 offset 1`, 1},
				{"shared identity", `insert into dst with r as(select uuid()x) select a.x from r a join r b on a.x=b.x`, 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					execSQLRequire(t, ctx, db, "delete from dst")
					res, err := db.ExecContext(ctx, tc.query)
					require.NoError(t, err)
					changed, err := res.RowsAffected()
					require.NoError(t, err)
					require.Equal(t, tc.want, changed)
					assertCount(t, int(tc.want))
				})
			}
			for _, suffix := range []string{"", " limit 2", " order by x limit 1"} {
				execSQLRequire(t, ctx, db, "delete from dst")
				_, err := db.ExecContext(ctx, "insert into dst "+source+suffix)
				var sqlErr *mysql.MySQLError
				require.ErrorAs(t, err, &sqlErr)
				require.EqualValues(t, 20105, sqlErr.Number)
				assertCount(t, 0)
			}
			stmt, err := db.PrepareContext(ctx, "insert into dst "+source+" limit ?")
			require.NoError(t, err)
			defer stmt.Close()
			for _, limit := range []int{1, 2, 1} {
				execSQLRequire(t, ctx, db, "delete from dst")
				_, err := stmt.ExecContext(ctx, limit)
				if limit == 1 {
					require.NoError(t, err)
					assertCount(t, 1)
				} else {
					var sqlErr *mysql.MySQLError
					require.ErrorAs(t, err, &sqlErr)
					require.EqualValues(t, 20105, sqlErr.Number)
					assertCount(t, 0)
				}
			}
			t.Run("returning", func(t *testing.T) {
				execSQLRequire(t, ctx, db, "delete from dst")
				var value string
				require.NoError(t, db.QueryRowContext(ctx, "insert into dst "+source+" limit 1 returning x").Scan(&value))
				require.Len(t, value, 36)
				assertCount(t, 1)
			})
			t.Run("CTAS", func(t *testing.T) {
				execSQLRequire(t, ctx, db, "create table ctas_dst as "+source+" limit 1")
				var count, width int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*),min(length(x)) from ctas_dst").Scan(&count, &width))
				require.Equal(t, 1, count)
				require.Equal(t, 36, width)
			})
		})
		for _, tc := range []struct {
			name, query string
			want        int
		}{
			{"single projected identity", "with r as(select uuid()x) select count(*) from (select x a,x b from r)q where a=b", 1},
			{"rand", "with r as(select rand()x) select count(*) from r a join r b on a.x=b.x", 1},
			{"uuid", "with r as(select uuid()x) select count(*) from r a join r b on a.x=b.x", 1},
			{"volatile charset argument", "with r as(select convert(uuid() using utf8mb4)x) select count(*) from r a join r b on a.x=b.x", 1},
			{"volatile unnest argument", `with r as(select value from unnest(concat('["',uuid(),'"]'))u) select count(*) from r a join r b on a.value=b.value`, 1},
			{"volatile JSON_TABLE argument", `with r as(select x from json_table(concat('["',uuid(),'"]'),'$[*]' columns(x varchar(36) path '$'))j) select count(*) from r a join r b on a.x=b.x`, 1},
			{"union", "with r as(select 1 k,rand()x union all select 2,rand()) select count(*) from r a join r b on a.k=b.k and a.x=b.x", 2},
			{"table bag and null", "with r as(select k,uuid()x from t) select count(*) from r a join r b on a.k<=>b.k and a.x=b.x", 4},
			{"special types", "with r as(select e,s,uuid()x from special) select count(*) from r a join r b on a.e=b.e and a.s=b.s and a.x=b.x", 2},
			{"single ENUM numeric", "with r as(select e,uuid()x from special) select sum(e+0) from r", 3},
			{"single SET numeric", "with r as(select s,uuid()x from special) select sum(s+0) from r", 3},
			{"special numeric", "with r as(select e,uuid()x from special) select sum(a.e+0) from r a join r b on a.x=b.x", 3},
			{"grouped", "with r as(select k,rand()x from t group by k) select count(*) from r a join r b on a.k<=>b.k and a.x=b.x", 3},
			{"recursive", "with recursive r(n,x) as(select 1,rand() union all select n+1,x from r where n<3) select count(*) from r a join r b on a.n=b.n and a.x=b.x", 3},
			{"recursive offset", "with recursive r(n,x) as(select 1,uuid() union all select n+1,x from r where n<5 limit 2 offset 1) select count(*) from r a join r b on a.n=b.n and a.x=b.x", 2},
			{"recursive distinct", "with recursive r(n,x) as(select 1,uuid() union distinct select n,x from r) select count(*) from r a join r b on a.x=b.x", 1},
			{"recursive predecessor", "with recursive r as(select uuid()x), seq(n,x) as(select 1,x from r union all select n+1,r.x from seq join r on seq.x=r.x where n<3) select count(*) from seq", 3},
			{"transitive outer", "with r as(select uuid()x),s as(select x from r) select count(*) from s a join s b on a.x=b.x join r c on a.x=c.x", 1},
			{"transitive inner", "with s as(with r as(select uuid()x) select x from r) select count(*) from s a join s b on a.x=b.x", 1},
			{"shadowed declarations", "with r as(select uuid()x) select count(*) from r a join (with r as(select uuid()x) select x from r) b on a.x<>b.x", 1},
			{"compact earlier step", "with single as(select uuid()x),shared as(select uuid()x) select count(*) from single,shared a,shared b where a.x=b.x", 1},
			{"compact later step", "with shared as(select uuid()x),single as(select uuid()x) select count(*) from shared a,shared b,single where a.x=b.x", 1},
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
			{"stable JSON_TABLE siblings", `select k,(with r as(select x from json_table('[1,2,3]','$[*]' columns(x int path '$'))j) select (select min(x) from r where x>=t.k)+(select max(x) from r)) v from t order by k`},
			{"stable constant rows control", "select k,(with r as(select 1 k union all select 2 union all select 3) select (select min(k) from r where k>=t.k)+(select max(k) from r)) v from t order by k"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := db.QueryContext(ctx, tc.query)
				require.NoError(t, err)
				require.Equal(t, []string{"NULL:NULL", "1:4", "2:5", "2:5"}, readWindowNullRows(t, rows))
			})
		}
		t.Run("stable scalar captures", func(t *testing.T) {
			var zone, mode string
			require.NoError(t, db.QueryRowContext(ctx, "select @@time_zone,@@block_encryption_mode").Scan(&zone, &mode))
			defer execSQLRequire(t, ctx, db, "set time_zone='"+zone+"',block_encryption_mode='"+mode+"'")
			execSQLRequire(t, ctx, db, "set time_zone='+00:00',block_encryption_mode='aes-128-ecb'")
			for _, tc := range []struct{ name, expr, want string }{
				{"datediff", "datediff('2026-01-03','2026-01-01')", "2"},
				{"convert using", "convert('abc' using utf8mb4)", "abc"},
				{"extractvalue", "extractvalue('<a>2</a>','/a')", "2"},
				{"updatexml", "updatexml('<a>1</a>','/a','<a>2</a>')", "<a>2</a>"},
				{"unix timestamp argument", "unix_timestamp('1970-01-01 00:00:02')", "2"},
				{"unix timestamp datetime", "unix_timestamp(cast('1970-01-01 00:00:02' as datetime))", "2"},
				{"uncompress", "hex(uncompress(compress('ab')))", "6162"},
				{"uncompressed length", "uncompressed_length(compress('ab'))", "2"},
				{"AES", "hex(aes_decrypt(aes_encrypt('ab','k'),'k'))", "6162"},
				{"database", "database()", "cte_evaluation_identity"},
				{"user", "cast(user()=user() as int)", "1"},
				{"found rows", "cast(found_rows()=found_rows() as int)", "1"},
				{"row count", "cast(row_count()=row_count() as int)", "1"},
				{"last insert ID", "cast(last_insert_id()=last_insert_id() as int)", "1"},
				{"last query ID", "cast(last_query_id()=last_query_id() as int)", "1"},
				{"indexed query ID", "cast(last_query_id(-1)=last_query_id(-1) as int)", "1"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					query := "with r as(select " + tc.expr + " x) select t.k,(select max(x) from r group by t.k) from t order by t.k"
					rows, err := db.QueryContext(ctx, query)
					require.NoError(t, err)
					require.Equal(t, []string{"NULL:" + tc.want, "1:" + tc.want, "2:" + tc.want, "2:" + tc.want}, readWindowNullRows(t, rows))
				})
			}
		})
		t.Run("stable JSON_TABLE captures", func(t *testing.T) {
			for _, tc := range []struct{ name, source, want string }{
				{"text", "'[1,2,3]'", "3"},
				{"JSON", "cast('[1,2,3]' as json)", "3"},
				{"NULL", "null", "NULL"},
				{"empty", "'[]'", "NULL"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					query := "with r as(select x from json_table(" + tc.source + ",'$[*]' columns(x int path '$'))j) select t.k,(select max(x) from r group by t.k) from t order by t.k"
					rows, err := db.QueryContext(ctx, query)
					require.NoError(t, err)
					require.Equal(t, []string{"NULL:" + tc.want, "1:" + tc.want, "2:" + tc.want, "2:" + tc.want}, readWindowNullRows(t, rows))
				})
			}
		})
		t.Run("JSON_TABLE diagnostics", func(t *testing.T) {
			for _, query := range []string{
				`with r as(select x from json_table('[1.234]','$[*]' columns(x decimal(4,1) path '$'))j) select t.k,(select max(x) from r group by t.k) from(select 1 k union all select 2)t order by t.k`,
				`select t.k,(select max(x) from json_table('[1.234]','$[*]' columns(x decimal(4,1) path '$'))j group by t.k) from(select 1 k union all select 2)t order by t.k`,
			} {
				rows, err := db.QueryContext(ctx, query)
				require.NoError(t, err)
				require.Equal(t, []string{"1:1.2", "2:1.2"}, readWindowNullRows(t, rows))
				var warnings int
				require.NoError(t, db.QueryRowContext(ctx, "show count(*) warnings").Scan(&warnings))
				require.Equal(t, 2, warnings, "CTE capture retains per-invocation diagnostics")
			}
		})
		t.Run("prepared JSON_TABLE input", func(t *testing.T) {
			stmt, err := db.PrepareContext(ctx, "with r as(select x from json_table(?,'$[*]' columns(x int path '$'))j) select t.k,(select max(x) from r group by t.k) from t order by t.k")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct{ input, want string }{{"[1,2,3]", "3"}, {"invalid", ""}, {"[]", "NULL"}, {"[4]", "4"}} {
				rows, err := stmt.QueryContext(ctx, tc.input)
				if tc.want == "" {
					require.Error(t, err)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, []string{"NULL:" + tc.want, "1:" + tc.want, "2:" + tc.want, "2:" + tc.want}, readWindowNullRows(t, rows))
			}
		})
		t.Run("prepared session readers", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create table session_identity(id bigint auto_increment primary key)")
			stmt, err := db.PrepareContext(ctx, "with r as(select found_rows()f,row_count()a,last_insert_id()i) select t.k,(select max(f) from r group by t.k),(select max(a) from r group by t.k),(select max(i) from r group by t.k) from t order by t.k")
			require.NoError(t, err)
			defer stmt.Close()
			// Seed after preparation. SELECT and INSERT exercise the real session
			// publication paths instead of setting mock process fields.
			rows, err := db.QueryContext(ctx, "select k from t limit 2")
			require.NoError(t, err)
			require.Len(t, readWindowNullRows(t, rows), 2)
			for _, tc := range []struct{ seed, suffix string }{
				{"insert into session_identity values(null)", ":2:1:1"},
				{"insert into session_identity values(null),(null)", ":4:2:2"},
				{"select missing_column", ":4:-1:2"},
			} {
				if tc.seed == "select missing_column" {
					_, err := db.ExecContext(ctx, tc.seed)
					require.Error(t, err)
				} else {
					execSQLRequire(t, ctx, db, tc.seed)
				}
				rows, err := stmt.QueryContext(ctx)
				require.NoError(t, err)
				require.Equal(t, []string{"NULL" + tc.suffix, "1" + tc.suffix, "2" + tc.suffix, "2" + tc.suffix}, readWindowNullRows(t, rows))
			}
		})
		t.Run("scalar diagnostics", func(t *testing.T) {
			rows, err := db.QueryContext(ctx, "with r as(select extractvalue('<a>','/a')x) select t.k,(select max(x) from r group by t.k) from(select 1 k union all select 2)t order by t.k")
			require.NoError(t, err)
			require.Equal(t, []string{"1:NULL", "2:NULL"}, readWindowNullRows(t, rows))
			var warnings int
			require.NoError(t, db.QueryRowContext(ctx, "show count(*) warnings").Scan(&warnings))
			require.Equal(t, 2, warnings, "retain the verified base's per-invocation diagnostics")
		})
		t.Run("unsupported lifetime captures", func(t *testing.T) {
			for _, q := range []string{
				`with r as(select x from json_table(cast('file:///missing' as datalink),'$[*]' columns(x int path '$'))j) select (select max(x) from r group by t.k) from t`,
				`with r as(select x from json_table(load_file('file:///missing'),'$[*]' columns(x int path '$'))j) select (select max(x) from r group by t.k) from t`,
				`with r as(select x from json_table(concat('["',uuid(),'"]'),'$[*]' columns(x varchar(36) path '$'))j) select (select max(x) from r group by t.k) from t`,
				"with r as(select uuid()x) select (select max(x) from r group by t.k) from t",
				"with r as(select result k from generate_series(cast(rand()*10 as bigint),10)g) select (select max(k) from r group by t.k) from t",
				"with r as(select convert(uuid() using utf8mb4)x) select (select max(x) from r group by t.k) from t",
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
		t.Run("prepared scalar execution context", func(t *testing.T) {
			var zone, mode string
			require.NoError(t, db.QueryRowContext(ctx, "select @@time_zone,@@block_encryption_mode").Scan(&zone, &mode))
			defer execSQLRequire(t, ctx, db, "set time_zone='"+zone+"',block_encryption_mode='"+mode+"',timestamp=0")
			stmt, err := db.PrepareContext(ctx, "with r as(select cast(unix_timestamp(?) as bigint)x,hex(aes_encrypt('a','k','0123456789abcdef'))y,unix_timestamp()z) select t.k,(select max(x) from r group by t.k),(select max(y) from r group by t.k),(select max(z) from r group by t.k) from t order by t.k")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				zone, mode      string
				timestamp, want int
			}{
				{"+00:00", "aes-128-cbc", 1234, 86400},
				{"+08:00", "aes-256-cbc", 5678, 57600},
				{"+00:00", "aes-128-cbc", 1234, 86400},
			} {
				execSQLRequire(t, ctx, db, fmt.Sprintf("set time_zone='%s',block_encryption_mode='%s',timestamp=%d", tc.zone, tc.mode, tc.timestamp))
				var encrypted string
				require.NoError(t, db.QueryRowContext(ctx, "select hex(aes_encrypt('a','k','0123456789abcdef'))").Scan(&encrypted))
				rows, err := stmt.QueryContext(ctx, "1970-01-02 00:00:00")
				require.NoError(t, err)
				suffix := fmt.Sprintf(":%d:%s:%d", tc.want, encrypted, tc.timestamp)
				require.Equal(t, []string{"NULL" + suffix, "1" + suffix, "2" + suffix, "2" + suffix}, readWindowNullRows(t, rows))
			}
		})
		t.Run("prepared XML error recovery", func(t *testing.T) {
			stmt, err := db.PrepareContext(ctx, "with r as(select extractvalue(?,?)x) select t.k,(select max(x) from r group by t.k) from t order by t.k")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct{ path, want string }{{"/a", "2"}, {"[", ""}, {"/a", "3"}} {
				if tc.want == "" {
					var key, value sql.NullString
					err := stmt.QueryRowContext(ctx, "<a></a>", tc.path).Scan(&key, &value)
					require.ErrorContains(t, err, "XPATH")
					continue
				}
				rows, err := stmt.QueryContext(ctx, "<a>"+tc.want+"</a>", tc.path)
				require.NoError(t, err)
				require.Equal(t, []string{"NULL:" + tc.want, "1:" + tc.want, "2:" + tc.want, "2:" + tc.want}, readWindowNullRows(t, rows))
			}
		})
	})
}
