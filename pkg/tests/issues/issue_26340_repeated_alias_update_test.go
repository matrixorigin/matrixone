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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestRepeatedAliasUpdateUsesOneConstraintSafePhysicalRow(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()

		t.Run("final check and generated check", func(t *testing.T) {
			mustExec(t, ctx, conn, `create table check_row (
				id int primary key, x int, y int, q int, constraint ck_xy check (x < y))`)
			mustExec(t, ctx, conn, "insert into check_row values (1,1,10,0)")
			mustExec(t, ctx, conn, `update ignore check_row a join check_row b on a.id=b.id
				set a.x=5, b.y=4, b.q=9 where a.id=1`)
			var x, y, q int
			require.NoError(t, conn.QueryRowContext(ctx,
				"select x,y,q from check_row where id=1").Scan(&x, &y, &q))
			require.Equal(t, []int{5, 10, 0}, []int{x, y, q})

			mustExec(t, ctx, conn, `create table generated_check_row (
				id int primary key, x int, y int, q int,
				g int generated always as (x+y), constraint ck_g check (g < 10))`)
			mustExec(t, ctx, conn, "insert into generated_check_row(id,x,y,q) values (1,1,1,0)")
			mustExec(t, ctx, conn, `update ignore generated_check_row a
				join generated_check_row b on a.id=b.id
				set a.x=5, b.y=6, b.q=9 where a.id=1`)
			var g int
			require.NoError(t, conn.QueryRowContext(ctx,
				"select x,y,q,g from generated_check_row where id=1").Scan(&x, &y, &q, &g))
			require.Equal(t, []int{5, 1, 0, 6}, []int{x, y, q, g})
		})

		t.Run("final composite child foreign key", func(t *testing.T) {
			mustExec(t, ctx, conn, `create table fk_parent (
				pa int, pb int, primary key(pa,pb))`)
			mustExec(t, ctx, conn, `insert into fk_parent values (1,1),(2,1),(1,2)`)
			mustExec(t, ctx, conn, `create table fk_child (
				id int primary key, fa int, fb int, v int,
				constraint fk_pair foreign key(fa,fb) references fk_parent(pa,pb))`)
			mustExec(t, ctx, conn, "insert into fk_child values (1,1,1,0)")
			mustExec(t, ctx, conn, `update ignore fk_child a join fk_child b on a.id=b.id
				set a.fa=2, b.fb=2, b.v=9 where a.id=1`)
			var fa, fb, v int
			require.NoError(t, conn.QueryRowContext(ctx,
				"select fa,fb,v from fk_child where id=1").Scan(&fa, &fb, &v))
			require.Equal(t, []int{2, 1, 0}, []int{fa, fb, v})
		})

		for _, action := range []struct {
			name   string
			clause string
			wantFA any
			wantFB any
		}{
			{name: "cascade", clause: "cascade", wantFA: int64(2), wantFB: int64(2)},
			{name: "set null", clause: "set null", wantFA: nil, wantFB: nil},
		} {
			for _, ignore := range []bool{false, true} {
				name := action.name + " regular"
				keyword := "update"
				if ignore {
					name = action.name + " ignore"
					keyword = "update ignore"
				}
				t.Run(name, func(t *testing.T) {
					mustExec(t, ctx, conn, "drop table if exists action_child")
					mustExec(t, ctx, conn, "drop table if exists action_parent")
					mustExec(t, ctx, conn, `create table action_parent (
						id int primary key, pa int, pb int, unique key uk_pair(pa,pb))`)
					mustExec(t, ctx, conn, fmt.Sprintf(`create table action_child (
						id int primary key, fa int, fb int,
						constraint fk_action foreign key(fa,fb) references action_parent(pa,pb)
						on update %s)`, action.clause))
					mustExec(t, ctx, conn, "insert into action_parent values (1,1,1)")
					mustExec(t, ctx, conn, "insert into action_child values (1,1,1)")
					mustExec(t, ctx, conn, fmt.Sprintf(`%s action_parent a
						join action_parent b on a.id=b.id
						set a.pa=2, b.pb=2 where a.id=1`, keyword))
					var pa, pb int
					require.NoError(t, conn.QueryRowContext(ctx,
						"select pa,pb from action_parent where id=1").Scan(&pa, &pb))
					require.Equal(t, []int{2, 2}, []int{pa, pb})
					var childFA, childFB sql.NullInt64
					require.NoError(t, conn.QueryRowContext(ctx,
						"select fa,fb from action_child where id=1").Scan(&childFA, &childFB))
					if action.wantFA == nil {
						require.False(t, childFA.Valid)
						require.False(t, childFB.Valid)
					} else {
						require.Equal(t, action.wantFA, childFA.Int64)
						require.Equal(t, action.wantFB, childFB.Int64)
					}
				})
			}
		}

		t.Run("unmodified enum and set use their alias old image", func(t *testing.T) {
			mustExec(t, ctx, conn, `create table enum_row (
				id int primary key, e enum('a','b'), s set('a','b'), x int, y int)`)
			mustExec(t, ctx, conn, "insert into enum_row values (1,'b','a,b',0,0)")
			mustExec(t, ctx, conn, `update enum_row a join enum_row b on a.id=b.id
				set a.x=1, b.y=2 where a.id=1`)
			var enumValue, setValue string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select e,s from enum_row where id=1").Scan(&enumValue, &setValue))
			require.Equal(t, "b", enumValue)
			require.Equal(t, "a,b", setValue)
		})

		t.Run("binary prepared unique fallback", func(t *testing.T) {
			mustExec(t, ctx, conn, `create table prepared_unique_row (
				id int primary key, u int unique, x int)`)
			stmt, err := conn.PrepareContext(ctx, `update ignore prepared_unique_row a
				join prepared_unique_row b on a.id=b.id
				set a.u=?, b.x=? where a.id=1`)
			require.NoError(t, err)
			defer stmt.Close()
			for _, conflict := range []any{int64(2), "2", []byte("2")} {
				mustExec(t, ctx, conn, "truncate table prepared_unique_row")
				mustExec(t, ctx, conn,
					"insert into prepared_unique_row values (1,1,0),(2,2,0)")
				_, err = stmt.ExecContext(ctx, conflict, 9)
				require.NoError(t, err, "parameter type %T", conflict)
				var u, x int
				require.NoError(t, conn.QueryRowContext(ctx,
					"select u,x from prepared_unique_row where id=1").Scan(&u, &x))
				require.Equal(t, []int{1, 9}, []int{u, x}, "parameter type %T", conflict)
				var duplicates int
				require.NoError(t, conn.QueryRowContext(ctx,
					"select count(*) from prepared_unique_row where u=2").Scan(&duplicates))
				require.Equal(t, 1, duplicates)
			}
		})
	})
}
