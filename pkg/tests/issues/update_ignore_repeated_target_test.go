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

func (c *updateIgnoreRepeatedTargetConn) warnings() []string {
	c.t.Helper()
	rows, err := c.cn.QueryContext(c.ctx, "show warnings")
	require.NoError(c.t, err)
	defer rows.Close()
	var result []string
	for rows.Next() {
		var level, message string
		var code int
		require.NoError(c.t, rows.Scan(&level, &code, &message))
		require.Equal(c.t, "Warning", level)
		result = append(result, fmt.Sprintf("%d %s", code, message))
	}
	require.NoError(c.t, rows.Err())
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

		t.Run("collation diagnostics only include rejected candidates", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("u", "(id int primary key, s varchar(30) collate utf8mb4_unicode_ci, n int, unique key uk_named(s))",
				"(1,'Alpha',7),(2,NULL,9),(3,'Beta',8)")
			result := c.exec("update ignore u a join u b on a.id=1 and b.id=3 set a.s='Alpha',b.s='ALPHA'")
			require.Equal(t, int64(0), c.affected(result))
			require.Equal(t, []string{"1062 Duplicate entry 'ALPHA' for key 'uk_named'"}, c.warnings())
			for _, query := range []string{
				"select s from u where id in (1,3) order by id",
				"select s from u force index(uk_named) where id in (1,3) order by id",
			} {
				require.Equal(t, []string{"Alpha", "Beta"}, c.stringRows(query))
			}
		})

		t.Run("two real conflicts produce two warnings", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,20,200),(3,30,300)")
			result := c.exec("update ignore t a join t b on a.id=1 and b.id in(2,3) set a.k=10,b.k=10")
			require.Equal(t, int64(0), c.affected(result))
			require.Equal(t, []string{
				"1062 Duplicate entry '10' for key 'k'",
				"1062 Duplicate entry '10' for key 'k'",
			}, c.warnings())
			want := [][]int64{{1, 10, 100}, {2, 20, 200}, {3, 30, 300}}
			require.Equal(t, want, c.intRows("select id,k,v from t order by id", 3))
			require.Equal(t, want, c.intRows("select id,k,v from t force index(k) order by id", 3))
		})

		for _, ownerID := range []int{1, 2} {
			for _, tc := range []struct {
				name                          string
				lastKey, finalKey, finalValue int
				affected                      int64
				warnings                      []string
			}{
				{"snapshot key now belongs to another row", 10, 30, 100, 2, []string{"1062 Duplicate entry '10' for key 'k'"}},
				{"distinct third key has no stale warning", 40, 40, 999, 3, nil},
				{"current-key no-op keeps payload", 30, 30, 999, 3, nil},
			} {
				t.Run(fmt.Sprintf("%s owner%d", tc.name, ownerID), func(t *testing.T) {
					c := newConn(t)
					otherID := 3 - ownerID
					c.resetTable("t", intTable, fmt.Sprintf("(%d,10,100),(%d,20,200)", ownerID, otherID))
					result := c.exec(fmt.Sprintf(`update ignore t x join t y on x.id=%d and y.id=%d
						join t z on z.id=x.id set x.k=30,y.k=10,z.k=%d,z.v=999`, ownerID, otherID, tc.lastKey))
					require.Equal(t, tc.affected, c.affected(result))
					require.Equal(t, tc.warnings, c.warnings())
					want := [][]int64{{int64(ownerID), int64(tc.finalKey), int64(tc.finalValue)}, {int64(otherID), 10, 200}}
					require.ElementsMatch(t, want, c.intRows("select id,k,v from t", 3))
					require.ElementsMatch(t, want, c.intRows("select id,k,v from t force index(k)", 3))
					require.Equal(t, [][]int64{{2, 2}}, c.intRows("select count(*),count(distinct k) from t", 2))
				})
			}
		}

		t.Run("CHECK rejection retains accepted reservation", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", "(id int primary key,k int unique,u int,v int,check(u+v<150))", "(1,10,1,1),(2,20,1,1)")
			result := c.exec(`update ignore t x join t y on x.id=1 and y.id in(1,2)
				set x.k=30,x.u=100,y.k=if(y.id=1,40,30),y.v=if(y.id=1,100,y.v)`)
			require.Equal(t, int64(1), c.affected(result))
			require.ElementsMatch(t, []string{"3819 Check constraint 't_chk_1' is violated", "1062 Duplicate entry '30' for key 'k'"}, c.warnings())
			require.Equal(t, [][]int64{{1, 30, 100, 1}, {2, 20, 1, 1}}, c.intRows("select id,k,u,v from t order by id", 4))
			require.Equal(t, [][]int64{{1, 30}, {2, 20}}, c.intRows("select id,k from t force index(k) order by id", 2))
		})

		t.Run("singleton table keeps its independent UNIQUE check", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,20,200)")
			c.resetTable("single_target", intTable, "(1,10,100),(2,20,200)")
			result := c.exec(`update ignore t x join t y on x.id=1 and y.id=2
				join single_target z on z.id=2 set x.k=30,y.k=40,z.k=10`)
			require.Equal(t, int64(2), c.affected(result))
			require.Equal(t, []string{"1062 Duplicate entry '10' for key 'k'"}, c.warnings())
			require.Equal(t, [][]int64{{1, 30, 100}, {2, 40, 200}}, c.intRows("select id,k,v from t order by id", 3))
			require.Equal(t, [][]int64{{1, 10, 100}, {2, 20, 200}}, c.intRows("select id,k,v from single_target order by id", 3))
		})

		t.Run("repeated PK update guard remains atomic", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("t", intTable, "(1,10,100),(2,20,200)")
			_, err := c.cn.ExecContext(ctx, "update ignore t a join t b on a.id=b.id and a.id=1 set a.id=2,b.v=9")
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr)
			require.Equal(t, uint16(1706), sqlErr.Number)
			require.Equal(t, [][]int64{{1, 10, 100}, {2, 20, 200}}, c.intRows("select id,k,v from t order by id", 3))
		})

		for _, chain := range []bool{false, true} {
			t.Run(fmt.Sprintf("generated PK guard chain=%t", chain), func(t *testing.T) {
				c := newConn(t)
				definition := "(a int,n int,g int generated always as(a*2) stored,primary key(g))"
				firstKey, secondKey := int64(2), int64(4)
				if chain {
					definition = "(a int,n int,m int generated always as(a*2) stored,g int generated always as(m+1) stored,primary key(g))"
					firstKey, secondKey = 3, 5
				}
				c.resetTable("genpk", definition, "")
				c.exec("insert into genpk(a,n) values(1,10),(2,20)")
				original := [][]int64{{1, 10, firstKey}, {2, 20, secondKey}}
				for _, keyword := range []string{"update", "update ignore"} {
					_, err := c.cn.ExecContext(ctx, fmt.Sprintf("%s genpk x join genpk y on x.g=y.g and x.g=%d set x.a=2,y.n=9", keyword, firstKey))
					var sqlErr *mysql.MySQLError
					require.ErrorAs(t, err, &sqlErr)
					require.Equal(t, uint16(1706), sqlErr.Number)
					require.Equal(t, original, c.intRows("select a,n,g from genpk order by g", 3))
				}
				c.exec(fmt.Sprintf("update ignore genpk x join genpk y on x.g=y.g and x.g=%d set x.n=11,y.n=9", firstKey))
				require.Equal(t, []string(nil), c.warnings())
				require.Equal(t, [][]int64{{1, 9, firstKey}, {2, 20, secondKey}}, c.intRows("select a,n,g from genpk order by g", 3))
				c.exec(fmt.Sprintf("update ignore genpk x join genpk y on x.g=y.g and x.g=%d set x.a=3", firstKey))
				newKey := int64(6)
				if chain {
					newKey = 7
				}
				require.Equal(t, [][]int64{{3, 9, newKey}}, c.intRows(fmt.Sprintf("select a,n,g from genpk where g=%d", newKey), 3))
				if chain {
					c.exec(fmt.Sprintf("update ignore genpk x join genpk y on x.g=y.g and x.g=%d set x.a=4,y.m=DEFAULT", newKey))
					require.Equal(t, [][]int64{{4, 9, 9}}, c.intRows("select a,n,g from genpk where g=9", 3))
				}
			})
		}

		t.Run("generated unique key rejects conflicting alias", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("g", "(id int primary key, u int, v int, x int, gv int generated always as (u+v) stored, unique key uk_gv(gv))", "")
			c.exec("insert into g (id,u,v,x) values (1,1,1,0),(2,2,2,0)")
			result := c.exec("update ignore g a join g b on a.id=b.id set a.u=2,b.v=2 where a.id=1")
			require.Equal(t, int64(1), c.affected(result))
			require.Equal(t, [][]int64{{1, 2, 1, 0, 3}, {2, 2, 2, 0, 4}},
				c.intRows("select id,u,v,x,gv from g order by id", 5))
		})

		t.Run("generated unique key allows non-conflicting aliases", func(t *testing.T) {
			c := newConn(t)
			c.resetTable("g", "(id int primary key, u int, v int, x int, gv int generated always as (u+v) stored, unique key uk_gv(gv))", "")
			c.exec("insert into g (id,u,v,x) values (1,1,1,0),(2,3,3,0)")
			result := c.exec("update ignore g a join g b on a.id=b.id set a.u=2,b.v=2 where a.id=1")
			require.Equal(t, int64(2), c.affected(result))
			require.Equal(t, [][]int64{{1, 2, 2, 0, 4}, {2, 3, 3, 0, 6}},
				c.intRows("select id,u,v,x,gv from g order by id", 5))
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
