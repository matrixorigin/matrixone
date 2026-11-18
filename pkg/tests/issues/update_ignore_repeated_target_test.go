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
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

type updateIgnoreRepeatedTargetConn struct {
	t   *testing.T
	ctx context.Context
	cn  *sql.Conn
}

func (c *updateIgnoreRepeatedTargetConn) exec(statement string, args ...any) sql.Result {
	c.t.Helper()
	result, err := c.cn.ExecContext(c.ctx, statement, args...)
	require.NoError(c.t, err, statement)
	return result
}

func (c *updateIgnoreRepeatedTargetConn) resetTable(name, definition, values string) {
	c.t.Helper()
	c.exec("drop table if exists " + name)
	c.exec("create table " + name + " " + definition)
	if values != "" {
		c.exec("insert into " + name + " values " + values)
	}
}

func (c *updateIgnoreRepeatedTargetConn) affected(result sql.Result) int64 {
	c.t.Helper()
	affected, err := result.RowsAffected()
	require.NoError(c.t, err)
	return affected
}

func (c *updateIgnoreRepeatedTargetConn) intRows(statement string, width int) [][]int64 {
	c.t.Helper()
	rows, err := c.cn.QueryContext(c.ctx, statement)
	require.NoError(c.t, err, statement)
	defer rows.Close()
	var result [][]int64
	for rows.Next() {
		values := make([]int64, width)
		dest := make([]any, width)
		for i := range values {
			dest[i] = &values[i]
		}
		require.NoError(c.t, rows.Scan(dest...), statement)
		result = append(result, values)
	}
	require.NoError(c.t, rows.Err(), statement)
	return result
}

func (c *updateIgnoreRepeatedTargetConn) stringRows(statement string) []string {
	c.t.Helper()
	rows, err := c.cn.QueryContext(c.ctx, statement)
	require.NoError(c.t, err, statement)
	defer rows.Close()
	var result []string
	for rows.Next() {
		var value string
		require.NoError(c.t, rows.Scan(&value), statement)
		result = append(result, value)
	}
	require.NoError(c.t, rows.Err(), statement)
	return result
}

func TestUpdateIgnoreRepeatedPhysicalTargetUniqueArbitration(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		dbName := testutils.GetDatabaseName(t)
		exec(fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, cleanupErr := db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
			require.NoError(t, cleanupErr)
		}()
		exec(fmt.Sprintf("use `%s`", dbName))

		newConn := func(t *testing.T) *updateIgnoreRepeatedTargetConn {
			return &updateIgnoreRepeatedTargetConn{t: t, ctx: ctx, cn: conn}
		}
		intTable := "(id int primary key, k int unique, v int not null)"

		t.Run("whole tuple selection on fanout", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("fanout", "(id int primary key,k int,v int)", "(1,10,100),(2,20,200),(3,30,300)")
			c.exec(`update fanout x join fanout y on x.id=1 and y.id in (2,3)
                set x.k=case when y.id=2 then null else 33 end,
                    x.v=case when y.id=2 then 222 else null end,y.v=y.v+1`)
			rows := c.intRows("select id,coalesce(k,-1),coalesce(v,-1) from fanout order by id", 3)
			require.Len(t, rows, 3)
			require.Contains(t, [][]int64{{1, -1, 222}, {1, 33, -1}}, rows[0], "one complete joined tuple must be used")
			require.Equal(t, [][]int64{{2, 20, 201}, {3, 30, 301}}, rows[1:])
		})

		t.Run("different aliases keep their own matched contributions", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("roles", "(id int primary key,grp int,a int,b int)", "(1,1,0,0),(2,1,0,0),(3,1,0,0)")
			c.exec(`update roles x join roles y on x.grp=y.grp and x.id<>y.id
                set x.a=1000+y.id,y.b=2000+x.id where x.id in (1,2) and y.id in (1,2)`)
			require.Equal(t, [][]int64{{1, 1002, 2002}, {2, 1001, 2001}, {3, 0, 0}},
				c.intRows("select id,a,b from roles order by id", 3))
		})

		t.Run("rejected earlier alias releases every proposed key", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("two_keys", "(id int primary key,u int unique,v int unique)", "(1,10,100),(2,20,200),(3,30,300)")
			c.exec(`update ignore two_keys x join two_keys y on x.id=1 and y.id=2
                set x.u=500,x.v=300,y.u=500,y.v=600`)
			expected := [][]int64{{1, 10, 100}, {2, 500, 600}, {3, 30, 300}}
			require.Equal(t, expected, c.intRows("select id,u,v from two_keys order by id", 3))
			require.Equal(t, expected, c.intRows("select id,u,v from two_keys force index(u) order by id", 3))
			require.Equal(t, expected, c.intRows("select id,u,v from two_keys force index(v) order by id", 3))
		})

		t.Run("later key rejection does not discard another row", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("cross_rows", "(id int primary key,u int unique,v int unique,payload int)", "(1,10,100,0),(2,20,200,0),(3,30,300,0)")
			c.exec(`update ignore cross_rows x join cross_rows y on x.id in (1,2) and y.id=x.id
                set x.u=500,x.v=if(x.id=1,300,400),y.payload=y.payload+1`)
			expected := [][]int64{{1, 10, 100, 1}, {2, 500, 400, 1}, {3, 30, 300, 0}}
			require.Equal(t, expected, c.intRows("select id,u,v,payload from cross_rows order by id", 4))
			require.Equal(t, expected, c.intRows("select id,u,v,payload from cross_rows force index(u) order by id", 4))
			require.Equal(t, expected, c.intRows("select id,u,v,payload from cross_rows force index(v) order by id", 4))
		})

		t.Run("generated unique key checks merged candidate", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("gen_unique", "(id int primary key,u int,v int,gv int generated always as (u+v) stored,unique key uk_gv(gv))", "")
			c.exec("insert into gen_unique(id,u,v) values (1,1,1),(2,2,2)")
			c.exec("update ignore gen_unique a join gen_unique b on a.id=b.id set a.u=2,b.v=2 where a.id=1")
			require.Equal(t, [][]int64{{1, 2, 1, 3}, {2, 2, 2, 4}}, c.intRows("select id,u,v,gv from gen_unique order by id", 4))
			require.Equal(t, [][]int64{{1, 3}, {2, 4}}, c.intRows("select id,gv from gen_unique force index(uk_gv) order by id", 2))
		})

		t.Run("CHECK uses latest accepted alias image", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("checked", "(id int primary key,u int,v int,check(u+v<150))", "(1,1,1)")
			c.exec("update ignore checked a join checked b on a.id=b.id set a.u=100,b.v=100")
			require.Equal(t, [][]int64{{1, 100, 1}}, c.intRows("select id,u,v from checked", 3))

			c.resetTable("checked", "(id int primary key,u int,v int,check(u+v<150))", "(1,100,1)")
			c.exec("update ignore checked a join checked b on a.id=b.id set a.u=1,b.v=100")
			require.Equal(t, [][]int64{{1, 1, 100}}, c.intRows("select id,u,v from checked", 3))
		})

		t.Run("FK restrict observes alias transition", func(t *testing.T) {
			c := newConn(t)
			c.exec("create table fk_parent_r(id int primary key,keyval int unique)")
			c.exec("create table fk_child_r(id int primary key,pid int,foreign key(pid) references fk_parent_r(keyval) on update restrict)")
			c.exec("insert into fk_parent_r values (1,1)")
			c.exec("insert into fk_child_r values (10,1)")
			_, err := conn.ExecContext(ctx, "update fk_parent_r x join fk_parent_r y on x.id=1 and y.id=1 set x.keyval=4,y.keyval=1")
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &mysqlErr)
			require.Equal(t, uint16(1217), mysqlErr.Number) // MatrixOne uses ER_ROW_IS_REFERENCED
			require.Equal(t, [][]int64{{1, 1}}, c.intRows("select id,keyval from fk_parent_r", 2))
			require.Equal(t, [][]int64{{10, 1}}, c.intRows("select id,pid from fk_child_r", 2))
			c.exec("update fk_parent_r x join fk_parent_r y on x.id=1 and y.id=1 set x.keyval=1,y.keyval=1")
			c.exec("update fk_parent_r x join fk_parent_r y on x.id=2 and y.id=2 set x.keyval=4,y.keyval=1")
			c.exec("delete from fk_child_r")
			c.exec("update fk_parent_r x join fk_parent_r y on x.id=1 and y.id=1 set x.keyval=4,y.keyval=1")
			require.Equal(t, [][]int64{{1, 1}}, c.intRows("select id,keyval from fk_parent_r", 2))
		})

		t.Run("FK cascade overlap fails before writes", func(t *testing.T) {
			c := newConn(t)
			c.exec("create table fk_parent_c(id int primary key,keyval int unique)")
			c.exec("create table fk_child_c(rid int primary key,code int unique,pid int,v int,foreign key(pid) references fk_parent_c(keyval) on update cascade)")
			c.exec("insert into fk_parent_c values (1,1),(2,2)")
			c.exec("insert into fk_child_c values (10,10,1,0)")
			// Combining explicit child writes with a cascade is not yet supported.
			// The planner must reject this shape before either writer changes data.
			_, err := conn.ExecContext(ctx, `update fk_parent_c p1 join fk_child_c x on p1.id=1 and x.rid=10 join fk_child_c y on y.rid=10
                set p1.keyval=4,x.code=11,y.v=1`)
			require.ErrorContains(t, err, "overlapping update paths")
			require.Equal(t, [][]int64{{1, 1}, {2, 2}}, c.intRows("select id,keyval from fk_parent_c order by id", 2))
			require.Equal(t, [][]int64{{10, 10, 1, 0}}, c.intRows("select rid,code,pid,v from fk_child_c", 4))
			require.Equal(t, [][]int64{{10, 10, 1, 0}}, c.intRows("select rid,code,pid,v from fk_child_c force index(code)", 4))
		})

		t.Run("first target wins cross-row same new key", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,20,200)")
			result := c.exec(`update ignore t x join t y on x.id=1 and y.id=2
				set x.k=30,y.k=30`)
			require.Equal(t, int64(1), c.affected(result))
			expected := [][]int64{{1, 30, 100}, {2, 20, 200}}
			require.Equal(t, expected, c.intRows("select id,k,v from t order by id", 3))
			require.Equal(t, [][]int64{{1, 30}, {2, 20}},
				c.intRows("select id,k from t force index(k) order by id", 2))
		})

		t.Run("target order controls winner not SET order", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,20,200)")
			result := c.exec(`update ignore t y join t x on x.id=1 and y.id=2
				set x.k=30,y.k=30`)
			require.Equal(t, int64(1), c.affected(result))
			require.Equal(t, [][]int64{{1, 10, 100}, {2, 30, 200}},
				c.intRows("select id,k,v from t order by id", 3))

			c.resetTable("t", intTable, "(1,10,100),(2,20,200)")
			result = c.exec(`update ignore t x join t y on x.id=1 and y.id=2
				set y.k=30,x.k=30`)
			require.Equal(t, int64(1), c.affected(result))
			require.Equal(t, [][]int64{{1, 30, 100}, {2, 20, 200}},
				c.intRows("select id,k,v from t order by id", 3))
		})

		t.Run("three targets only first advances", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,20,200),(3,30,300)")
			result := c.exec(`update ignore t x join t y on x.id=1 and y.id=2
				join t z on y.id=2 and z.id=3 set x.k=40,y.k=40,z.k=40`)
			require.Equal(t, int64(1), c.affected(result))
			require.Equal(t, [][]int64{{1, 40, 100}, {2, 20, 200}, {3, 30, 300}},
				c.intRows("select id,k,v from t order by id", 3))
		})

		t.Run("rejected candidate does not block later non-key alias", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,20,200)")
			result := c.exec(`update ignore t x join t y on x.id=1 and y.id=2
				join t z on y.id=z.id set x.k=30,y.k=30,z.v=9`)
			require.Equal(t, int64(2), c.affected(result))
			require.Equal(t, [][]int64{{1, 30, 100}, {2, 20, 9}},
				c.intRows("select id,k,v from t order by id", 3))
		})

		t.Run("same row no-op key keeps non-key assignment", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,20,200)")
			result := c.exec(`update ignore t x join t y on x.id=y.id and x.id=1
				set x.k=30,y.k=30,y.v=9`)
			require.Equal(t, int64(2), c.affected(result))
			require.Equal(t, [][]int64{{1, 30, 9}, {2, 20, 200}},
				c.intRows("select id,k,v from t order by id", 3))
		})

		t.Run("released key remains available to later alias", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,20,200)")
			result := c.exec(`update ignore t a join t b on a.id=2 and b.id=1
				set a.k=30,b.k=20`)
			require.Equal(t, int64(2), c.affected(result))
			require.Equal(t, [][]int64{{1, 20, 100}, {2, 30, 200}},
				c.intRows("select id,k,v from t order by id", 3))
		})

		t.Run("multiple unique indexes reject candidate atomically", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", "(id int primary key,k1 int,k2 int,v int,unique key uk1(k1),unique key uk2(k2))",
				"(1,10,100,1000),(2,20,200,2000)")
			result := c.exec(`update ignore t x join t y on x.id=1 and y.id=2
				set x.k1=40,x.k2=400,y.k1=41,y.k2=400`)
			require.Equal(t, int64(1), c.affected(result))
			require.Equal(t, [][]int64{{1, 40, 400, 1000}, {2, 20, 200, 2000}},
				c.intRows("select id,k1,k2,v from t order by id", 4))
		})

		t.Run("composite unique conflict and non-conflicting control", func(t *testing.T) {
			c := newConn(t)
			definition := "(id int primary key,g int,k int,v int,unique key uk_gk(g,k))"
			c.resetTable("t", definition, "(1,1,10,100),(2,1,20,200)")
			result := c.exec(`update ignore t x join t y on x.id=1 and y.id=2
				set x.k=30,y.k=30`)
			require.Equal(t, int64(1), c.affected(result))
			require.Equal(t, [][]int64{{1, 1, 30, 100}, {2, 1, 20, 200}},
				c.intRows("select id,g,k,v from t order by id", 4))

			c.resetTable("t", definition, "(1,1,10,100),(2,1,20,200)")
			result = c.exec(`update ignore t x join t y on x.id=1 and y.id=2
				set x.k=30,y.g=2,y.k=30`)
			require.Equal(t, int64(2), c.affected(result))
			require.Equal(t, [][]int64{{1, 1, 30, 100}, {2, 2, 30, 200}},
				c.intRows("select id,g,k,v from t order by id", 4))
		})

		t.Run("NULL unique keys do not conflict", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", "(id int primary key,k int unique,v int not null)",
				"(1,10,100),(2,20,200)")
			result := c.exec(`update ignore t x join t y on x.id=1 and y.id=2
				set x.k=NULL,y.k=NULL,x.v=101,y.v=201`)
			require.Equal(t, int64(2), c.affected(result))
			require.Equal(t, [][]int64{{1, -1, 101}, {2, -1, 201}},
				c.intRows("select id,coalesce(k,-1),v from t order by id", 3))
		})

		t.Run("prefix unique conflict and non-conflicting control", func(t *testing.T) {
			c := newConn(t)
			definition := "(id int primary key,k varchar(16),v int,unique key uk_k(k(4)))"
			c.resetTable("t", definition, "(1,'aa10',100),(2,'bb10',200)")
			result := c.exec(`update ignore t x join t y on x.id=1 and y.id=2
				set x.k='ccXX1',y.k='ccXX2'`)
			require.Equal(t, int64(1), c.affected(result))
			require.Equal(t, []string{"ccXX1", "bb10"},
				c.stringRows("select k from t order by id"))

			c.resetTable("t", definition, "(1,'aa10',100),(2,'bb10',200)")
			result = c.exec(`update ignore t x join t y on x.id=1 and y.id=2
				set x.k='dd001',y.k='ee001'`)
			require.Equal(t, int64(2), c.affected(result))
			require.Equal(t, []string{"dd001", "ee001"},
				c.stringRows("select k from t order by id"))
		})

		t.Run("prepared statement retry keeps arbitration", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,20,200)")
			stmt, err := conn.PrepareContext(ctx,
				"update ignore t x join t y on x.id=1 and y.id=2 set x.k=?,y.k=?")
			require.NoError(t, err)
			defer stmt.Close()
			for range 3 {
				c.exec("update t set k=10,v=100 where id=1")
				c.exec("update t set k=20,v=200 where id=2")
				result, err := stmt.ExecContext(ctx, 30, 30)
				require.NoError(t, err)
				affected, err := result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, int64(1), affected)
				require.Equal(t, [][]int64{{1, 30, 100}, {2, 20, 200}},
					c.intRows("select id,k,v from t order by id", 3))
			}
		})

		t.Run("transaction workspace keys and rollback", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,20,200),(3,30,300)")
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				_, _ = conn.ExecContext(cleanupCtx, "rollback")
			}()
			original := [][]int64{{1, 10, 100}, {2, 20, 200}, {3, 30, 300}}
			updated := [][]int64{{1, 10, 100}, {2, 30, 222}, {3, 40, 300}}
			for _, commit := range []bool{false, true} {
				c.exec("begin")
				c.exec("update t set k=40 where id=3")
				result := c.exec(`update ignore t x join t y on x.id=1 and y.id=2
					set x.k=40,x.v=111,y.k=30,y.v=222`)
				require.Equal(t, int64(1), c.affected(result))
				for _, query := range []string{
					"select id,k,v from t order by id",
					"select id,k,v from t force index(k) order by id",
				} {
					require.Equal(t, updated, c.intRows(query, 3), query)
				}
				if commit {
					c.exec("commit")
				} else {
					c.exec("rollback")
				}
				expected := original
				if commit {
					expected = updated
				}
				for _, query := range []string{
					"select id,k,v from t order by id",
					"select id,k,v from t force index(k) order by id",
				} {
					require.Equal(t, expected, c.intRows(query, 3), query)
				}
			}
		})

		t.Run("single target control is unchanged", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,30,200)")
			c.exec("update ignore t set k=30 where id=1")
			require.Equal(t, [][]int64{{1, 10, 100}, {2, 30, 200}},
				c.intRows("select id,k,v from t order by id", 3))
		})

		t.Run("large cross-row collisions preserve every unique key", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "")
			var values strings.Builder
			for i := 1; i <= 1000; i++ {
				if i > 1 {
					values.WriteByte(',')
				}
				fmt.Fprintf(&values, "(%d,%d,0)", i, i)
			}
			c.exec("insert into t values " + values.String())
			result := c.exec(`update ignore t x join t y on x.id+1=y.id and x.id%2=1
				set x.k=10000+x.id,y.k=10000+y.id-1`)
			require.Equal(t, int64(500), c.affected(result),
				"exactly one alias in each conflicting pair must advance")
			for _, statement := range []string{
				"select count(*),count(distinct k) from t",
				"select count(*),count(distinct k) from t force index(k)",
			} {
				rows := c.intRows(statement, 2)
				require.Equal(t, [][]int64{{1000, 1000}}, rows, statement)
			}
			rows := c.intRows("select id,k from t order by id", 2)
			require.Len(t, rows, 1000)
			for i, row := range rows {
				id := int64(i + 1)
				expectedKey := id
				if id%2 == 1 {
					expectedKey = 10000 + id
				}
				require.Equal(t, []int64{id, expectedKey}, row)
			}
		})
	})
}
