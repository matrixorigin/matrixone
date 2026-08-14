// Copyright 2021-2024 Matrix Origin
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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUpdateIgnoreRepeatedAliasesAdvanceGreedily(t *testing.T) {
	RunBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		exec := func(statement string, args ...any) sql.Result {
			result, execErr := conn.ExecContext(ctx, statement, args...)
			require.NoError(t, execErr, statement)
			return result
		}
		queryInts := func(statement string, count int) []int64 {
			row := conn.QueryRowContext(ctx, statement)
			values := make([]int64, count)
			dest := make([]any, count)
			for i := range values {
				dest[i] = &values[i]
			}
			require.NoError(t, row.Scan(dest...), statement)
			return values
		}
		assertAffected := func(result sql.Result, expected int64) {
			actual, rowsErr := result.RowsAffected()
			require.NoError(t, rowsErr)
			require.Equal(t, expected, actual)
		}

		exec("drop database if exists update_ignore_greedy_test")
		exec("create database update_ignore_greedy_test")
		defer conn.ExecContext(context.Background(), "drop database if exists update_ignore_greedy_test")
		exec("use update_ignore_greedy_test")

		exec("create table unique_t (id int primary key, u int not null, v int not null, x int not null, unique key uk_uv(u,v))")
		exec("insert into unique_t values (1,1,1,0),(2,2,2,0)")
		result := exec("update ignore unique_t a join unique_t b on a.id=b.id join unique_t c on b.id=c.id set a.u=2,b.v=2,c.x=9 where a.id=1")
		assertAffected(result, 2)
		require.Equal(t, []int64{2, 1, 9}, queryInts("select u,v,x from unique_t where id=1", 3))

		exec("truncate table unique_t")
		exec("insert into unique_t values (1,1,1,0),(2,2,2,0)")
		result = exec("update ignore unique_t a join unique_t b on a.id=b.id join unique_t c on b.id=c.id set a.u=2,b.v=2,b.x=7,c.v=3 where a.id=1")
		assertAffected(result, 2)
		require.Equal(t, []int64{2, 3, 0}, queryInts("select u,v,x from unique_t where id=1", 3))

		exec("truncate table unique_t")
		exec("insert into unique_t values (1,1,1,0),(2,2,2,0)")
		result = exec("update ignore unique_t a join unique_t b on a.id=b.id join unique_t c on b.id=c.id set a.u=?,b.v=?,c.x=? where a.id=1", 2, 2, 9)
		assertAffected(result, 2)
		require.Equal(t, []int64{2, 1, 9}, queryInts("select u,v,x from unique_t where id=1", 3))

		exec("truncate table unique_t")
		exec("insert into unique_t values (1,1,1,0),(2,2,2,0)")
		result = exec("update ignore unique_t a join unique_t b on a.id=b.id join unique_t c on b.id=c.id set c.x=9,b.v=2,a.u=2 where a.id=1")
		assertAffected(result, 2)
		require.Equal(t, []int64{2, 1, 9}, queryInts("select u,v,x from unique_t where id=1", 3),
			"SET order must not change repeated-alias evaluation order")

		exec("truncate table unique_t")
		exec("insert into unique_t values (1,1,1,0),(2,2,2,0)")
		result = exec("update ignore unique_t a join unique_t b on a.id=b.id set a.x=2,b.x=3 where a.id=1")
		assertAffected(result, 2)
		require.Equal(t, []int64{3}, queryInts("select x from unique_t where id=1", 1),
			"the later table alias must win when aliases assign the same column")

		exec("create table unique4_t (id int primary key, u int not null, v int not null, w int not null, x int not null, unique key uk_uvw(u,v,w))")
		exec("insert into unique4_t values (1,1,1,1,0),(2,2,2,2,0)")
		result = exec("update ignore unique4_t a join unique4_t b on a.id=b.id join unique4_t c on b.id=c.id join unique4_t d on c.id=d.id set a.u=2,b.v=2,c.w=2,d.x=9 where a.id=1")
		assertAffected(result, 3)
		require.Equal(t, []int64{2, 2, 1, 9}, queryInts("select u,v,w,x from unique4_t where id=1", 4))

		exec("create table check_t (id int primary key, x int, y int, q int, check (x < y))")
		exec("insert into check_t values (1,1,10,0)")
		result = exec("update ignore check_t a join check_t b on a.id=b.id join check_t c on b.id=c.id set a.x=5,b.y=4,b.q=7,c.q=9")
		assertAffected(result, 2)
		require.Equal(t, []int64{5, 10, 9}, queryInts("select x,y,q from check_t", 3))

		exec("create table generated_check_t (id int primary key, x int, y int, q int, g int generated always as (x+y) stored, check (g < 20))")
		exec("insert into generated_check_t(id,x,y,q) values (1,1,10,0)")
		result = exec("update ignore generated_check_t a join generated_check_t b on a.id=b.id join generated_check_t c on b.id=c.id set a.x=5,b.y=20,b.q=7,c.q=9")
		assertAffected(result, 2)
		require.Equal(t, []int64{5, 10, 9, 15}, queryInts("select x,y,q,g from generated_check_t", 4))

		exec("create table parent_t (a int, b int, primary key(a,b))")
		exec("insert into parent_t values (1,1),(2,1),(1,2)")
		exec("create table child_t (id int primary key, fa int, fb int, v int, foreign key(fa,fb) references parent_t(a,b))")
		exec("insert into child_t values (1,1,1,0)")
		result = exec("update ignore child_t a join child_t b on a.id=b.id join child_t c on b.id=c.id set a.fa=2,b.fb=2,c.v=9")
		assertAffected(result, 2)
		require.Equal(t, []int64{2, 1, 9}, queryInts("select fa,fb,v from child_t", 3))

		exec("create table restrict_parent_t (id int primary key, k int unique, x int)")
		exec("create table restrict_child_t (id int primary key, fk int, foreign key(fk) references restrict_parent_t(k) on update restrict)")
		exec("insert into restrict_parent_t values (1,1,0)")
		exec("insert into restrict_child_t values (1,1)")
		result = exec("update ignore restrict_parent_t a join restrict_parent_t b on a.id=b.id set a.k=2,b.x=9 where a.id=1")
		assertAffected(result, 1)
		require.Equal(t, []int64{1, 9}, queryInts("select k,x from restrict_parent_t", 2))

		exec("update restrict_parent_t set x=0 where id=1")
		result = exec("update ignore restrict_parent_t a join restrict_parent_t b on a.id=b.id set a.k=?,b.x=? where a.id=1", 2, 9)
		assertAffected(result, 1)
		require.Equal(t, []int64{1, 9}, queryInts("select k,x from restrict_parent_t", 2),
			"binary COM_STMT must filter the rejected parent-key alias and keep its sibling")

		exec("create table no_action_parent_t (id int primary key, k int unique, x int)")
		exec("create table no_action_child_t (id int primary key, fk int, foreign key(fk) references no_action_parent_t(k) on update no action)")
		exec("insert into no_action_parent_t values (1,1,0)")
		exec("insert into no_action_child_t values (1,1)")
		result = exec("update ignore no_action_parent_t a join no_action_parent_t b on a.id=b.id set a.k=2,b.x=9 where a.id=1")
		assertAffected(result, 1)
		require.Equal(t, []int64{1, 9}, queryInts("select k,x from no_action_parent_t", 2))
	})
}
