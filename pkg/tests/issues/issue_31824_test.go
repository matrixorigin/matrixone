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
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue31824RepeatedUpdate(t *testing.T) {
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
		name := strings.ToLower(testutils.GetDatabaseName(t))
		_, err = conn.ExecContext(ctx, "create database `"+name+"`")
		require.NoError(t, err)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			if _, e := db.ExecContext(cleanup, "drop database if exists `"+name+"`"); e != nil {
				t.Errorf("cleanup: %v", e)
			}
		}()
		_, err = conn.ExecContext(ctx, "use `"+name+"`")
		require.NoError(t, err)
		reset := func(t *testing.T) {
			for _, q := range []string{
				"drop table if exists t", "drop table if exists u",
				"create table t(id int primary key,k int,v int not null,unique key uk_k(k))",
				"create table u(id int primary key,v int not null)",
				"insert into t values (1,10,1000),(2,20,2000),(3,30,3000)",
				"insert into u values (1,100)",
			} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
		}
		read := func(t *testing.T, q string, cols int) []string {
			rows, e := conn.QueryContext(ctx, q)
			require.NoError(t, e, q)
			defer rows.Close()
			var out []string
			for rows.Next() {
				vals := make([]int, cols)
				args := make([]any, cols)
				for i := range vals {
					args[i] = &vals[i]
				}
				require.NoError(t, rows.Scan(args...))
				parts := make([]string, cols)
				for i, v := range vals {
					parts[i] = fmt.Sprint(v)
				}
				out = append(out, strings.Join(parts, ","))
			}
			require.NoError(t, rows.Err())
			return out
		}
		t.Run("single_alias_control", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 set x.v=y.v+1")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,10,2001", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("distinct_targets_control", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update t x join u on x.id=u.id set x.v=111,u.v=222")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1,10,111", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
			require.Equal(t, []string{"1,222"}, read(t, "select id,v from u", 2))
		})
		t.Run("no_secondary_index", func(t *testing.T) {
			for _, q := range []string{"drop table if exists plain_t", "create table plain_t(id int primary key,v int)", "insert into plain_t values(1,10),(2,20)"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			res, e := conn.ExecContext(ctx, "update plain_t x join plain_t y on x.id=1 and y.id=2 set x.v=11,y.v=21")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1,11", "2,21"}, read(t, "select id,v from plain_t order by id", 2))
		})
		t.Run("no_declared_primary_key", func(t *testing.T) {
			for _, q := range []string{"create table no_pk(v int)", "insert into no_pk values(1),(1)"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			res, e := conn.ExecContext(ctx, "update no_pk x join no_pk y on x.v=y.v set x.v=2,y.v=3")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 4, n)
			require.Equal(t, []string{"2"}, read(t, "select count(*) from no_pk where v=3", 1))
		})
		t.Run("absent_repeated_role", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update t x left join t y on y.id=999 set x.v=x.v+1,y.v=999 where x.id=1")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,10,1001", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("generated_default_is_not_a_second_target", func(t *testing.T) {
			for _, q := range []string{"drop table if exists gen_t", "create table gen_t(id int primary key,a int,g int generated always as (a*2) stored)", "insert into gen_t(id,a) values(1,3),(2,5)"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			res, e := conn.ExecContext(ctx, "update gen_t x join gen_t y on x.id=1 and y.id=2 set x.g=default,y.a=7")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,3,6", "2,7,14"}, read(t, "select id,a,g from gen_t order by id", 3))
		})
		t.Run("two_alias_swap", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 set x.k=20,y.k=10")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1,20,1000", "2,10,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
			require.Equal(t, []string{"1,20,1000", "2,10,2000"}, read(t, "select id,k,v from t force index(uk_k) where k in (10,20) order by id", 3))
		})
		t.Run("correlated_rhs", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 set x.k=(select u.v from u where u.id=x.id),y.k=10")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1,100,1000", "2,10,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("same_row_two_roles", func(t *testing.T) {
			reset(t)
			_, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=1 set x.k=40,y.k=50")
			require.NoError(t, e)
			require.Equal(t, []string{"1,50,1000", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("same_row_multiple_columns", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=1 set x.k=40,x.v=111,y.k=50,y.v=222")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1,50,222", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("three_alias_same_row", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=1 join t z on z.id=1 set x.k=40,y.k=50,z.k=60")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 3, n)
			require.Equal(t, []string{"1,60,1000", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("fanout_keeps_one_nullable_tuple", func(t *testing.T) {
			reset(t)
			_, e := conn.ExecContext(ctx, "insert into u values(2,200)")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 join u on u.id in (1,2) set x.k=case when u.id=1 then null else 33 end,x.v=case when u.id=1 then 222 else 333 end,y.v=y.v+1")
			require.NoError(t, e)
			require.Equal(t, []string{"1"}, read(t, "select count(*) from t where id=1 and ((k is null and v=222) or (k=33 and v=333))", 1))
			require.Equal(t, []string{"2,20,2001"}, read(t, "select id,k,v from t where id=2", 3))
		})
		t.Run("on_update_keeps_old_key", func(t *testing.T) {
			reset(t)
			_, e := conn.ExecContext(ctx, "alter table t add column ts timestamp default '2000-01-01 00:00:00' on update current_timestamp")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 set x.k=40,y.v=222")
			require.NoError(t, e)
			require.Equal(t, []string{"2"}, read(t, "select count(*) from t where id in (1,2) and ts>'2000-01-01 00:00:00'", 1))
			require.Equal(t, []string{"1,40,1000"}, read(t, "select id,k,v from t force index(uk_k) where k=40", 3))
			require.Empty(t, read(t, "select id,k from t force index(uk_k) where k=10", 2))
		})
		t.Run("two_alias_duplicate", func(t *testing.T) {
			reset(t)
			_, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 set x.k=40,y.k=40")
			var duplicate *mysql.MySQLError
			require.ErrorAs(t, e, &duplicate)
			require.EqualValues(t, 1062, duplicate.Number)
			require.Equal(t, []string{"1,10,1000", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
			require.Empty(t, read(t, "select id,k from t force index(uk_k) where k=40 order by id", 2))
		})
		t.Run("ignore_same_key_one_wins", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=2 set x.k=40,y.k=40")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,40,1000", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("ignore_key_vacancy_order", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=2 set x.k=20,y.k=40")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,10,1000", "2,40,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
			reset(t)
			res, e = conn.ExecContext(ctx, "update ignore t x join t y on x.id=2 and y.id=1 set x.k=40,y.k=20")
			require.NoError(t, e)
			n, e = res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1,20,1000", "2,40,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("ignore_all_rejected_preserves_rows", func(t *testing.T) {
			reset(t)
			rowids := func() []string {
				rows, e := conn.QueryContext(ctx, "select __mo_rowid from t order by id")
				require.NoError(t, e)
				defer rows.Close()
				var ids []string
				for rows.Next() {
					var raw []byte
					require.NoError(t, rows.Scan(&raw))
					ids = append(ids, string(raw))
				}
				require.NoError(t, rows.Err())
				return ids
			}
			before := rowids()
			res, e := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=2 set x.k=30,y.k=30")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 0, n)
			require.Equal(t, before, rowids())
			require.Equal(t, []string{"1,10,1000", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("ignore_null_keys", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=2 set x.k=null,y.k=null")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"2"}, read(t, "select count(*) from t where k is null", 1))
		})
		t.Run("ignore_mixed_target", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=2 join u on u.id=x.id set x.k=40,y.k=40,u.v=u.v+1")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1,40,1000", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
			require.Equal(t, []string{"1,101"}, read(t, "select id,v from u order by id", 2))
		})
		t.Run("ignore_independent_unique", func(t *testing.T) {
			reset(t)
			for _, q := range []string{"insert into u values (2,200)", "alter table u add unique key uk_u_v(v)"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			res, e := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=2 join u on u.id=x.id set x.k=40,y.k=40,u.v=200")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,40,1000", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
			require.Equal(t, []string{"1,100", "2,200"}, read(t, "select id,v from u order by id", 2))
			require.Equal(t, []string{"2,200"}, read(t, "select id,v from u force index(uk_u_v) where v=200", 2))
		})
		t.Run("ignore_independent_generated_unique", func(t *testing.T) {
			reset(t)
			for _, q := range []string{"insert into u values (2,200)", "alter table u add column g int generated always as (v*2) stored", "alter table u add unique key uk_u_g(g)"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			res, e := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=2 join u on u.id=x.id set x.k=40,y.k=40,u.v=200")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,100,200", "2,200,400"}, read(t, "select id,v,g from u order by id", 3))
			require.Equal(t, []string{"2,200,400"}, read(t, "select id,v,g from u force index(uk_u_g) where g=400", 3))
		})
		t.Run("auto_increment_key_boundary", func(t *testing.T) {
			_, e := conn.ExecContext(ctx, "drop table if exists auto_t")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "create table auto_t(id int auto_increment primary key, v int)")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "insert into auto_t(id,v) values (1,10),(2,20)")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "update auto_t x join auto_t y on x.id=1 and y.id=2 set x.id=4,y.v=30")
			require.ErrorContains(t, e, "auto-increment column")
			require.Equal(t, []string{"1,10", "2,20"}, read(t, "select id,v from auto_t order by id", 2))
			res, e := conn.ExecContext(ctx, "update auto_t x join auto_t y on x.id=1 and y.id=2 set x.v=11,y.v=21")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1,11", "2,21"}, read(t, "select id,v from auto_t order by id", 2))
			_, e = conn.ExecContext(ctx, "drop table auto_t")
			require.NoError(t, e)
		})
		t.Run("independent_check_ignore", func(t *testing.T) {
			reset(t)
			_, e := conn.ExecContext(ctx, "drop table u")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "create table u(id int primary key,v int not null,constraint ck_u check (v<=150))")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "insert into u values(1,100)")
			require.NoError(t, e)
			res, e := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=2 join u on u.id=x.id set x.k=40,y.k=40,u.v=200")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,100"}, read(t, "select id,v from u", 2))
		})
		t.Run("window_assignment", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 set x.v=row_number() over (order by y.id), y.v=222")
			require.ErrorContains(t, e, "window function")
			require.Nil(t, res)
			require.Equal(t, []string{"1,10,1000", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("native_window_assignment", func(t *testing.T) {
			reset(t)
			_, e := conn.ExecContext(ctx, "set session sql_mode = 'MATRIXONE_NATIVE'")
			require.NoError(t, e)
			defer func() { _, e := conn.ExecContext(ctx, "set session sql_mode = default"); require.NoError(t, e) }()
			res, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 set x.v=row_number() over (order by y.id), y.v=222")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1,10,1", "2,20,222", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("top_level_aggregate_is_rejected", func(t *testing.T) {
			reset(t)
			_, e := conn.ExecContext(ctx, "set session sql_mode = ''")
			require.NoError(t, e)
			defer func() { _, e := conn.ExecContext(ctx, "set session sql_mode = default"); require.NoError(t, e) }()
			res, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 set x.v=sum(y.v), y.v=222")
			require.ErrorContains(t, e, "top-level aggregate")
			require.Nil(t, res)
			require.Equal(t, []string{"1,10,1000", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("ignore_multiple_unique_keys_are_atomic", func(t *testing.T) {
			for _, q := range []string{
				"create table cross_rows(id int primary key,u int,v int,payload int,unique key uk_u(u),unique key uk_v(v))",
				"insert into cross_rows values (1,10,100,0),(2,20,200,0),(3,30,300,0)",
			} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			result, e := conn.ExecContext(ctx, "update ignore cross_rows x join cross_rows y on x.id in (1,2) and y.id=x.id set x.u=500,x.v=if(x.id=1,300,400),y.payload=y.payload+1")
			require.NoError(t, e)
			n, e := result.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 3, n)
			require.Equal(t, []string{"1,10,100,1", "2,500,400,1", "3,30,300,0"}, read(t, "select id,u,v,payload from cross_rows order by id", 4))
		})
		t.Run("ignore_composite_key_uses_accepted_image", func(t *testing.T) {
			for _, q := range []string{"create table comp_rows(id int primary key,u int,v int,unique key uv(u,v))", "insert into comp_rows values(1,1,1),(2,2,2),(3,3,3)"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			res, e := conn.ExecContext(ctx, "update ignore comp_rows x join comp_rows y on x.id=1 and y.id=1 set x.u=2,y.v=2")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,2,1", "2,2,2", "3,3,3"}, read(t, "select id,u,v from comp_rows order by id", 3))
			_, e = conn.ExecContext(ctx, "update comp_rows set u=1,v=1 where id=1")
			require.NoError(t, e)
			res, e = conn.ExecContext(ctx, "update ignore comp_rows y join comp_rows x on y.id=1 and x.id=1 set x.u=2,y.v=2")
			require.NoError(t, e)
			n, e = res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,1,2", "2,2,2", "3,3,3"}, read(t, "select id,u,v from comp_rows order by id", 3))
			_, e = conn.ExecContext(ctx, "begin")
			require.NoError(t, e)
			defer conn.ExecContext(context.Background(), "rollback")
			_, e = conn.ExecContext(ctx, "update comp_rows set u=1,v=1 where id=1")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "update comp_rows set u=4,v=4 where id=2")
			require.NoError(t, e)
			res, e = conn.ExecContext(ctx, "update ignore comp_rows x join comp_rows y on x.id=1 and y.id=1 set x.u=4,y.v=4")
			require.NoError(t, e)
			n, e = res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,4,1", "2,4,4", "3,3,3"}, read(t, "select id,u,v from comp_rows order by id", 3))
			_, e = conn.ExecContext(ctx, "rollback")
			require.NoError(t, e)
		})
		t.Run("ignore_primary_key_conflict_preserves_sibling", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=1 set x.id=2,y.v=111")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,10,111", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("generated_and_check_constraints", func(t *testing.T) {
			for _, q := range []string{
				"create table generated_t(id int primary key,a int,v int,g int generated always as (a*2) stored,unique key uk_g(g))",
				"insert into generated_t(id,a,v) values(1,10,100),(2,20,200),(3,250,300)",
				"create table checked_t(id int primary key,a int,b int,constraint ck check(a<=b))",
				"insert into checked_t values(1,0,0)",
			} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			res, e := conn.ExecContext(ctx, "update ignore generated_t x join generated_t y on x.id=1 and y.id=1 set x.a=250,y.v=600")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,10,600,20"}, read(t, "select id,a,v,g from generated_t force index(uk_g) where g=20", 4))
			res, e = conn.ExecContext(ctx, "update ignore checked_t x join checked_t y on x.id=1 and y.id=1 set x.a=1,y.b=1")
			require.NoError(t, e)
			n, e = res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,0,1"}, read(t, "select id,a,b from checked_t", 3))
			_, e = conn.ExecContext(ctx, "update checked_t set a=0,b=0")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "update checked_t x join checked_t y on x.id=1 and y.id=1 set x.a=1,y.b=1")
			require.Error(t, e)
			require.Equal(t, []string{"1,0,0"}, read(t, "select id,a,b from checked_t", 3))
		})
		t.Run("generated_primary_key_with_sync_fulltext", func(t *testing.T) {
			for _, q := range []string{
				"set experimental_fulltext_index=1",
				"create table fulltext_t(a bigint,id bigint generated always as(a*2) stored primary key,body text,v int,fulltext fti(body))",
				"insert into fulltext_t(a,body,v) values(1,'apple fruit',0),(10,'pear other',0)",
			} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			defer func() { _, _ = conn.ExecContext(context.Background(), "set experimental_fulltext_index=0") }()
			query := "select id,a,v from fulltext_t where match(body) against('apple')"
			require.Equal(t, []string{"2,1,0"}, read(t, query, 3))
			for _, keyword := range []string{"", "ignore "} {
				_, e := conn.ExecContext(ctx, "update "+keyword+"fulltext_t x join fulltext_t y on x.id=2 and y.id=2 set x.a=2,y.v=3")
				require.ErrorContains(t, e, "synchronous full-text/vector index")
				require.Equal(t, []string{"2,1,0"}, read(t, query, 3))
			}
			_, e := conn.ExecContext(ctx, "update fulltext_t x join fulltext_t y on x.id=2 and y.id=2 set x.v=2,y.v=3")
			require.NoError(t, e)
			require.Equal(t, []string{"2,1,3"}, read(t, query, 3))

			for _, q := range []string{
				"create table generated_pk_t(a bigint,id bigint generated always as(a*2) stored primary key,k int unique,v int)",
				"insert into generated_pk_t(a,k,v) values(1,10,0),(10,20,0)",
				"update generated_pk_t x join generated_pk_t y on x.id=2 and y.id=2 set x.a=2,y.v=3",
			} {
				_, e = conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			require.Equal(t, []string{"4,2,3"}, read(t, "select id,a,v from generated_pk_t force index(k) where k=10", 3))
		})
		t.Run("foreign_key_boundary", func(t *testing.T) {
			for _, q := range []string{
				"create table parent_t(id int primary key,k int unique)",
				"create table child_t(id int primary key,pid int,v int,constraint fk_child foreign key(pid) references parent_t(k) on update restrict)",
				"insert into parent_t values(1,1)", "insert into child_t values(1,1,0)",
			} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			_, e := conn.ExecContext(ctx, "update ignore child_t x join child_t y on x.id=1 and y.id=1 set x.pid=999,y.v=7")
			require.ErrorContains(t, e, "foreign-key")
			require.Equal(t, []string{"1,1,0"}, read(t, "select id,pid,v from child_t", 3))
			_, e = conn.ExecContext(ctx, "update child_t x join child_t y on x.id=1 and y.id=1 set x.v=7,y.v=8")
			require.NoError(t, e)
			require.Equal(t, []string{"1,1,8"}, read(t, "select id,pid,v from child_t", 3))
			for _, q := range []string{"set foreign_key_checks=0", "prepare fk_stmt from 'update child_t x join child_t y on x.id=1 and y.id=1 set x.pid=999,y.v=9'", "execute fk_stmt", "update child_t set pid=1 where id=1", "set foreign_key_checks=1"} {
				_, e = conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			_, e = conn.ExecContext(ctx, "execute fk_stmt")
			require.ErrorContains(t, e, "foreign-key")
			require.Equal(t, []string{"1,1,9"}, read(t, "select id,pid,v from child_t", 3))
			_, e = conn.ExecContext(ctx, "deallocate prepare fk_stmt")
			require.NoError(t, e)
			for _, q := range []string{"set foreign_key_checks=0", "update child_t set pid=999 where id=1", "set foreign_key_checks=1", "update child_t x join child_t y on x.id=1 and y.id=1 set x.v=10,y.v=11"} {
				_, e = conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			require.Equal(t, []string{"1,999,11"}, read(t, "select id,pid,v from child_t", 3))
			for _, q := range []string{
				"set foreign_key_checks=0",
				"create table self_t(id int primary key,pid int,v int,foreign key(pid) references self_t(id))",
				"insert into self_t values(1,999,0)",
				"set foreign_key_checks=1",
				"update self_t x join self_t y on x.id=1 and y.id=1 set x.v=1,y.v=2",
			} {
				_, e = conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			require.Equal(t, []string{"1,999,2"}, read(t, "select id,pid,v from self_t", 3))
		})
		t.Run("partitioned_target_boundary", func(t *testing.T) {
			for _, q := range []string{
				"create table part_t(id int primary key,v int,key iv(v)) partition by range(id) (partition p0 values less than(10),partition p1 values less than(100))",
				"insert into part_t values(1,100),(11,1100)",
			} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			_, e := conn.ExecContext(ctx, "update part_t x join part_t y on x.id=1 and y.id=11 set x.v=101,y.v=1101")
			require.ErrorContains(t, e, "partitioned table")
			require.Equal(t, []string{"1,100", "11,1100"}, read(t, "select id,v from part_t order by id", 2))
			reset(t)
			_, e = conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 join part_t p on p.id=1 set x.v=p.v,y.v=p.v+1")
			require.NoError(t, e)
			require.Equal(t, []string{"1,10,100", "2,20,101", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
		})
		t.Run("composite_primary_key", func(t *testing.T) {
			for _, q := range []string{"create table cp(a int,b int,k int unique,v int,primary key(a,b))", "insert into cp values(1,1,10,100),(2,2,20,200),(3,3,30,300)"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			res, e := conn.ExecContext(ctx, "update ignore cp x join cp y on x.a=1 and x.b=1 and y.a=1 and y.b=1 set x.a=2,y.b=2")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"2,1,10,100", "2,2,20,200", "3,3,30,300"}, read(t, "select a,b,k,v from cp order by a,b", 4))
			require.Equal(t, []string{"2,1,10"}, read(t, "select a,b,k from cp force index(k) where k=10", 3))
		})
		t.Run("enum_set_index_storage", func(t *testing.T) {
			for _, q := range []string{"create table special_t(id int primary key,e enum('x','y'),s set('','a'),v int,key ix(e),key iset(s))", "insert into special_t values(1,'x',1,0),(2,'y',2,0)"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			indexes := make([]string, 0, 2)
			for _, index := range []string{"ix", "iset"} {
				var internal string
				require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='special_t') and name=? limit 1", name, index).Scan(&internal))
				require.Regexp(t, `^__mo_index_secondary_[a-z0-9-]+$`, internal)
				indexes = append(indexes, internal)
			}
			check := func() {
				require.Equal(t, []string{"1"}, read(t, "select s+0 from special_t where id=1", 1))
				for _, index := range indexes {
					require.Equal(t, []string{"1"}, read(t, "select count(*) from `"+index+"` where __mo_index_pri_col=1", 1))
				}
			}
			for _, q := range []string{"update special_t x join special_t y on x.id=1 and y.id=2 set x.e='y',y.v=y.v+1", "update special_t x join special_t y on x.id=1 and y.id=2 set x.e='x',y.v=y.v+1", "update special_t x join special_t y on x.id=1 and y.id=2 set x.s=x.s,y.v=y.v+1"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
				check()
			}
		})
		t.Run("prefix_unique_index", func(t *testing.T) {
			for _, q := range []string{"create table prefix_t(id int primary key,u varchar(30),v int,unique key uk(u(3)))", "insert into prefix_t values(1,'aaaa',10),(2,'bbbb',20)"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			var index string
			require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='prefix_t') and name='uk' limit 1", name).Scan(&index))
			require.Regexp(t, `^__mo_index_unique_[a-z0-9-]+$`, index)
			res, e := conn.ExecContext(ctx, "update ignore prefix_t x join prefix_t y on x.id=2 and y.id=2 set x.u='aaab',y.v=30")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1"}, read(t, "select count(*) from prefix_t where id=2 and u='bbbb' and v=30", 1))
			require.Equal(t, []string{"1"}, read(t, "select count(*) from `"+index+"` where __mo_index_pri_col=2 and __mo_index_idx_col='bbb'", 1))
			_, e = conn.ExecContext(ctx, "update prefix_t x join prefix_t y on x.id=2 and y.id=2 set x.u='aaab',y.v=31")
			var duplicate *mysql.MySQLError
			require.ErrorAs(t, e, &duplicate)
			require.EqualValues(t, 1062, duplicate.Number)
			require.Equal(t, []string{"1"}, read(t, "select count(*) from prefix_t where id=2 and u='bbbb' and v=30", 1))
			res, e = conn.ExecContext(ctx, "update prefix_t x join prefix_t y on x.id=2 and y.id=2 set x.u='cccc',y.v=31")
			require.NoError(t, e)
			n, e = res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1"}, read(t, "select count(*) from `"+index+"` where __mo_index_pri_col=2 and __mo_index_idx_col='ccc'", 1))
			require.Equal(t, []string{"0"}, read(t, "select count(*) from `"+index+"` where __mo_index_pri_col=2 and __mo_index_idx_col='bbb'", 1))
		})
		t.Run("not_null_checked_per_accepted_role", func(t *testing.T) {
			for _, q := range []string{"create table notnull_t(id int primary key,k int unique,v int not null,w int)", "insert into notnull_t values(1,10,10,100),(2,20,20,200)"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			for _, keyword := range []string{"", "ignore "} {
				_, e := conn.ExecContext(ctx, "update "+keyword+"notnull_t x join notnull_t y on x.id=1 and y.id=1 set x.v=if(x.w=100,null,2),y.v=3")
				require.ErrorContains(t, e, "Column 'v' cannot be null")
				require.Equal(t, []string{"1,10,10,100", "2,20,20,200"}, read(t, "select id,k,v,w from notnull_t order by id", 4))
			}
			res, e := conn.ExecContext(ctx, "update ignore notnull_t x join notnull_t y on x.id=1 and y.id=1 set x.k=20,x.v=if(x.w=100,null,2),y.v=30")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1, n)
			require.Equal(t, []string{"1,10,30,100", "2,20,20,200"}, read(t, "select id,k,v,w from notnull_t order by id", 4))
		})
		t.Run("strict_temporal_checks_and_prepared_mode", func(t *testing.T) {
			for _, q := range []string{"set session sql_mode=''", "create table temporal_t(id int primary key,d date,v date,payload int)", "insert into temporal_t values(1,'2001-01-01','0000-00-00',0)", "set session sql_mode=default"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			defer func() { _, _ = conn.ExecContext(context.Background(), "set session sql_mode=default") }()
			stmt, e := conn.PrepareContext(ctx, "update temporal_t x join temporal_t y on x.id=1 and y.id=1 set x.d=x.v,y.d='2003-01-01',y.v='2003-01-01'")
			require.NoError(t, e)
			defer stmt.Close()
			_, e = stmt.ExecContext(ctx)
			var invalid *mysql.MySQLError
			require.ErrorAs(t, e, &invalid)
			require.EqualValues(t, 1366, invalid.Number)
			var initialDate, oldDate string
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(d as char),cast(v as char) from temporal_t where id=1").Scan(&initialDate, &oldDate))
			require.Equal(t, "2001-01-01", initialDate)
			require.Equal(t, "0000-00-00", oldDate)
			_, e = conn.ExecContext(ctx, "set session sql_mode=''")
			require.NoError(t, e)
			_, e = stmt.ExecContext(ctx)
			require.NoError(t, e)
			require.Equal(t, []string{"1"}, read(t, "select count(*) from temporal_t where d='2003-01-01' and v='2003-01-01'", 1))
			_, e = conn.ExecContext(ctx, "update temporal_t set d='0000-00-00'")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "set session sql_mode=default")
			require.NoError(t, e)
			_, e = conn.ExecContext(ctx, "update temporal_t x join temporal_t y on x.id=1 and y.id=1 set x.payload=1,y.d='2003-01-01'")
			require.ErrorAs(t, e, &invalid)
			require.EqualValues(t, 1366, invalid.Number)
			var payload int
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(d as char),payload from temporal_t where id=1").Scan(&oldDate, &payload))
			require.Equal(t, "0000-00-00", oldDate)
			require.Zero(t, payload)
		})
		t.Run("auto_increment_non_primary_boundary", func(t *testing.T) {
			for _, q := range []string{"create table auto_other(id int primary key,a int auto_increment unique,v int)", "insert into auto_other(id,a,v) values(1,100,1),(2,200,2)"} {
				_, e := conn.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			_, e := conn.ExecContext(ctx, "update ignore auto_other x join auto_other y on x.id=1 and y.id=2 set x.a=if(x.v=1,null,2),y.v=3")
			require.ErrorContains(t, e, "auto-increment column")
			require.Equal(t, []string{"1,100,1", "2,200,2"}, read(t, "select id,a,v from auto_other order by id", 3))
			res, e := conn.ExecContext(ctx, "update auto_other x join auto_other y on x.id=1 and y.id=2 set x.v=10,y.v=20")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1,100,10", "2,200,20"}, read(t, "select id,a,v from auto_other order by id", 3))
		})
		t.Run("mixed_target_success", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 join u on u.id=x.id set x.k=20,y.k=10,u.v=u.v+1")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 3, n)
			require.Equal(t, []string{"1,20,1000", "2,10,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
			require.Equal(t, []string{"1,101"}, read(t, "select id,v from u order by id", 2))
		})
		t.Run("mixed_target_duplicate_rolls_back_u", func(t *testing.T) {
			reset(t)
			_, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 join u on u.id=x.id set x.k=40,y.k=40,u.v=u.v+1")
			require.Error(t, e)
			require.Equal(t, []string{"1,10,1000", "2,20,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
			require.Equal(t, []string{"1,100"}, read(t, "select id,v from u order by id", 2))
		})
		t.Run("mixed_outer_absence", func(t *testing.T) {
			reset(t)
			res, e := conn.ExecContext(ctx, "update t x join t y on x.id=1 and y.id=2 left join u on u.id=999 set x.k=20,y.k=10,u.v=u.v+1")
			require.NoError(t, e)
			n, e := res.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 2, n)
			require.Equal(t, []string{"1,20,1000", "2,10,2000", "3,30,3000"}, read(t, "select id,k,v from t order by id", 3))
			require.Equal(t, []string{"1,100"}, read(t, "select id,v from u order by id", 2))
		})
	})
}
