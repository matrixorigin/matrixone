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

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue31824SelfJoinMultiTargetUpdateUnique(t *testing.T) {
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

		originalRows := []string{"1,1,10,100", "2,1,20,200"}
		conflictTests := []struct {
			name           string
			uniqueDef      string
			indexName      string
			indexPredicate string
			update         string
		}{
			{
				name:           "single column existing key through second alias",
				uniqueDef:      "unique key uk_k(k)",
				indexName:      "uk_k",
				indexPredicate: "k=10",
				update: `update t as x join t as y on x.id=1 and y.id=2
					set x.v=x.v+1, y.k=10`,
			},
			{
				name:           "single column existing key through first alias",
				uniqueDef:      "unique key uk_k(k)",
				indexName:      "uk_k",
				indexPredicate: "k=20",
				update: `update t as y join t as x on x.id=1 and y.id=2
					set y.v=y.v+1, x.k=20`,
			},
			{
				name:           "single column same new key",
				uniqueDef:      "unique key uk_k(k)",
				indexName:      "uk_k",
				indexPredicate: "k=30",
				update: `update t as x join t as y on x.id=1 and y.id=2
					set x.k=30, y.k=30`,
			},
			{
				name:           "single column same new key reversed assignments",
				uniqueDef:      "unique key uk_k(k)",
				indexName:      "uk_k",
				indexPredicate: "k=30",
				update: `update t as y join t as x on x.id=1 and y.id=2
					set y.k=30, x.k=30`,
			},
			{
				name:           "composite existing key through second alias",
				uniqueDef:      "unique key uk_gk(g,k)",
				indexName:      "uk_gk",
				indexPredicate: "g=1 and k=10",
				update: `update t as x join t as y on x.id=1 and y.id=2
					set x.v=x.v+1, y.k=10`,
			},
			{
				name:           "composite existing key through first alias",
				uniqueDef:      "unique key uk_gk(g,k)",
				indexName:      "uk_gk",
				indexPredicate: "g=1 and k=20",
				update: `update t as y join t as x on x.id=1 and y.id=2
					set y.v=y.v+1, x.k=20`,
			},
			{
				name:           "composite same new key",
				uniqueDef:      "unique key uk_gk(g,k)",
				indexName:      "uk_gk",
				indexPredicate: "g=1 and k=30",
				update: `update t as x join t as y on x.id=1 and y.id=2
					set x.k=30, y.k=30`,
			},
			{
				name:           "composite same new key reversed assignments",
				uniqueDef:      "unique key uk_gk(g,k)",
				indexName:      "uk_gk",
				indexPredicate: "g=1 and k=30",
				update: `update t as y join t as x on x.id=1 and y.id=2
					set y.k=30, x.k=30`,
			},
		}

		for _, tt := range conflictTests {
			t.Run(tt.name, func(t *testing.T) {
				createIssue31824Table(t, ctx, conn, tt.uniqueDef)
				_, err := conn.ExecContext(ctx, tt.update)
				requireIssue31824Duplicate(t, err)

				require.Equal(t, originalRows, queryIssue31824Rows(t, ctx, conn,
					fmt.Sprintf("select id,g,k,v from t ignore index(%s) order by id", tt.indexName)))
				var wantIndexed []string
				switch tt.indexPredicate {
				case "k=10", "g=1 and k=10":
					wantIndexed = originalRows[:1]
				case "k=20", "g=1 and k=20":
					wantIndexed = originalRows[1:]
				}
				require.Equal(t, wantIndexed, queryIssue31824Rows(t, ctx, conn,
					fmt.Sprintf("select id,g,k,v from t force index(%s) where %s order by id",
						tt.indexName, tt.indexPredicate)))

				// A rejected statement must leave the connection usable for a retry.
				mustExec(t, ctx, conn, "update t set v=v+10 where id=2")
				require.Equal(t, []string{"1,1,10,100", "2,1,20,210"},
					queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
			})
		}

		successTests := []struct {
			name      string
			uniqueDef string
			update    string
			want      []string
		}{
			{
				name:      "single column non conflicting keys",
				uniqueDef: "unique key uk_k(k)",
				update: `update t as x join t as y on x.id=1 and y.id=2
					set x.k=30, y.k=40, x.v=x.v+1, y.v=y.v+1`,
				want: []string{"1,1,30,101", "2,1,40,201"},
			},
			{
				name:      "single column null keys",
				uniqueDef: "unique key uk_k(k)",
				update: `update t as x join t as y on x.id=1 and y.id=2
					set x.k=null, y.k=null`,
				want: []string{"1,1,NULL,100", "2,1,NULL,200"},
			},
			{
				name:      "composite non conflicting keys",
				uniqueDef: "unique key uk_gk(g,k)",
				update: `update t as x join t as y on x.id=1 and y.id=2
					set x.k=30, y.k=40, x.v=x.v+1, y.v=y.v+1`,
				want: []string{"1,1,30,101", "2,1,40,201"},
			},
			{
				name:      "composite null keys",
				uniqueDef: "unique key uk_gk(g,k)",
				update: `update t as x join t as y on x.id=1 and y.id=2
					set x.k=null, y.k=null`,
				want: []string{"1,1,NULL,100", "2,1,NULL,200"},
			},
		}
		for _, tt := range successTests {
			t.Run(tt.name, func(t *testing.T) {
				createIssue31824Table(t, ctx, conn, tt.uniqueDef)
				mustExec(t, ctx, conn, tt.update)
				require.Equal(t, tt.want,
					queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
			})
		}

		t.Run("one target matches multiple rows", func(t *testing.T) {
			createIssue31824Table(t, ctx, conn, "unique key uk_k(k)")
			mustExec(t, ctx, conn, "insert into t values (3,1,30,300)")
			_, err := conn.ExecContext(ctx, `update t as x join t as y on x.id=1 and y.id in (2,3)
				set x.v=x.v+1, y.k=40`)
			requireIssue31824Duplicate(t, err)
			require.Equal(t, []string{"1,1,10,100", "2,1,20,200", "3,1,30,300"},
				queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
			require.Empty(t, queryIssue31824Rows(t, ctx, conn,
				"select id,g,k,v from t force index(uk_k) where k=40 order by id"))

			mustExec(t, ctx, conn, `update t as x join t as y on x.id=1 and y.id in (2,3)
				set x.v=x.v+1, y.k=y.k+100`)
			require.Equal(t, []string{"1,1,10,101", "2,1,120,200", "3,1,130,300"},
				queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
		})

		t.Run("conflict rolls back another physical target", func(t *testing.T) {
			createIssue31824Table(t, ctx, conn, "unique key uk_k(k)")
			mustExec(t, ctx, conn, "create table aux (id int primary key, v int not null)")
			mustExec(t, ctx, conn, "insert into aux values (1,500)")
			_, err := conn.ExecContext(ctx, `update t as x join t as y on x.id=1 and y.id=2
				join aux as z on z.id=x.id set x.v=x.v+1, y.k=10, z.v=z.v+1`)
			requireIssue31824Duplicate(t, err)
			require.Equal(t, originalRows,
				queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
			var auxValue int
			require.NoError(t, conn.QueryRowContext(ctx, "select v from aux where id=1").Scan(&auxValue))
			require.Equal(t, 500, auxValue)
		})

		t.Run("same physical row self exemption", func(t *testing.T) {
			createIssue31824Table(t, ctx, conn, "unique key uk_k(k)")
			mustExec(t, ctx, conn, `update t as x join t as y on x.id=y.id and x.id=1
				set x.k=30, y.v=201`)
			require.Equal(t, []string{"1,1,30,201", "2,1,20,200"},
				queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
			mustExec(t, ctx, conn, `update t as x join t as y on x.id=y.id and x.id=1
				set x.k=x.k, y.v=202`)
			require.Equal(t, []string{"1,1,30,202", "2,1,20,200"},
				queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
			mustExec(t, ctx, conn, `update t as x join t as y on x.id=y.id and x.id=1
				set x.k=40, y.k=40`)
			require.Equal(t, []string{"1,1,40,202", "2,1,20,200"},
				queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
		})

		t.Run("key swap preserves uniqueness", func(t *testing.T) {
			createIssue31824Table(t, ctx, conn, "unique key uk_k(k)")
			mustExec(t, ctx, conn, `update t as x join t as y on x.id=1 and y.id=2
				set x.k=20, y.k=10`)
			require.Equal(t, []string{"1,1,20,100", "2,1,10,200"},
				queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
			requireIssue31824UniqueRowCount(t, ctx, conn, 2)
		})

		t.Run("three aliases non conflicting and conflicting", func(t *testing.T) {
			createIssue31824Table(t, ctx, conn, "unique key uk_k(k)")
			mustExec(t, ctx, conn, "insert into t values (3,1,30,300)")
			mustExec(t, ctx, conn, `update t as x join t as y on x.id=1 and y.id=2
				join t as z on z.id=3 set x.k=40, y.k=50, z.k=60`)
			require.Equal(t, []string{"1,1,40,100", "2,1,50,200", "3,1,60,300"},
				queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))

			createIssue31824Table(t, ctx, conn, "unique key uk_k(k)")
			mustExec(t, ctx, conn, "insert into t values (3,1,30,300)")
			_, err := conn.ExecContext(ctx, `update t as x join t as y on x.id=1 and y.id=2
				join t as z on z.id=3 set x.k=40, y.k=40, z.v=301`)
			requireIssue31824Duplicate(t, err)
			require.Equal(t, []string{"1,1,10,100", "2,1,20,200", "3,1,30,300"},
				queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
		})

		for _, tt := range []struct {
			name   string
			update string
			want   []string
		}{
			{
				name: "first of two unique indexes",
				update: `update t as x join t as y on x.id=1 and y.id=2
					set x.k=30, y.k=30, x.v=300, y.v=400`,
			},
			{
				name: "second of two unique indexes same new key",
				update: `update t as x join t as y on x.id=1 and y.id=2
					set x.k=30, y.k=40, x.v=300, y.v=300`,
			},
			{
				name: "second of two unique indexes moves an existing key",
				update: `update t as x join t as y on x.id=1 and y.id=2
					set x.k=30, y.k=40, x.v=200, y.v=300`,
				want: []string{"1,1,30,200", "2,1,40,300"},
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				createIssue31824Table(t, ctx, conn, "unique key uk_k(k), unique key uk_v(v)")
				if tt.want != nil {
					mustExec(t, ctx, conn, tt.update)
					require.Equal(t, tt.want,
						queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
					return
				}
				_, err := conn.ExecContext(ctx, tt.update)
				requireIssue31824Duplicate(t, err)
				require.Equal(t, originalRows,
					queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
			})
		}
		t.Run("two unique indexes non conflicting", func(t *testing.T) {
			createIssue31824Table(t, ctx, conn, "unique key uk_k(k), unique key uk_v(v)")
			mustExec(t, ctx, conn, `update t as x join t as y on x.id=1 and y.id=2
				set x.k=30, y.k=40, x.v=300, y.v=400`)
			require.Equal(t, []string{"1,1,30,300", "2,1,40,400"},
				queryIssue31824Rows(t, ctx, conn, "select id,g,k,v from t order by id"))
		})

		t.Run("prefix unique rejects same new prefix", func(t *testing.T) {
			createIssue31824VarcharTable(t, ctx, conn, "", "unique key uk_k(k(2))")
			_, err := conn.ExecContext(ctx, `update t as x join t as y on x.id=1 and y.id=2
				set x.k='cc1', y.k='cc2'`)
			requireIssue31824Duplicate(t, err)
			require.Equal(t, []string{"1,1,aa1,100", "2,1,bb1,200"},
				queryIssue31824VarcharRows(t, ctx, conn, "select id,g,k,v from t order by id"))
		})

		t.Run("prefix unique allows different prefixes", func(t *testing.T) {
			createIssue31824VarcharTable(t, ctx, conn, "", "unique key uk_k(k(2))")
			mustExec(t, ctx, conn, `update t as x join t as y on x.id=1 and y.id=2
				set x.k='cc1', y.k='dd1'`)
			require.Equal(t, []string{"1,1,cc1,100", "2,1,dd1,200"},
				queryIssue31824VarcharRows(t, ctx, conn, "select id,g,k,v from t order by id"))
		})

		t.Run("update ignore existing conflict preserves uniqueness", func(t *testing.T) {
			createIssue31824Table(t, ctx, conn, "unique key uk_k(k)")
			mustExec(t, ctx, conn, `update ignore t as x join t as y on x.id=1 and y.id=2
				set x.v=x.v+1, y.k=10`)
			requireIssue31824UniqueRowCount(t, ctx, conn, 2)
		})
	})
}

func createIssue31824Table(t *testing.T, ctx context.Context, conn *sql.Conn, uniqueDef string) {
	t.Helper()
	mustExec(t, ctx, conn, "drop table if exists t")
	mustExec(t, ctx, conn, fmt.Sprintf(`create table t (
		id int primary key, g int not null, k int, v int not null, %s)`, uniqueDef))
	mustExec(t, ctx, conn, "insert into t values (1,1,10,100),(2,1,20,200)")
}

func createIssue31824VarcharTable(
	t *testing.T, ctx context.Context, conn *sql.Conn, collation, uniqueDef string,
) {
	t.Helper()
	mustExec(t, ctx, conn, "drop table if exists t")
	mustExec(t, ctx, conn, fmt.Sprintf(`create table t (
		id int primary key, g int not null, k varchar(20) %s, v int not null, %s)`,
		collation, uniqueDef))
	mustExec(t, ctx, conn, "insert into t values (1,1,'aa1',100),(2,1,'bb1',200)")
}

func queryIssue31824VarcharRows(
	t *testing.T, ctx context.Context, conn *sql.Conn, query string,
) []string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	var result []string
	for rows.Next() {
		var id, g, v int
		var k sql.NullString
		require.NoError(t, rows.Scan(&id, &g, &k, &v))
		key := "NULL"
		if k.Valid {
			key = k.String
		}
		result = append(result, fmt.Sprintf("%d,%d,%s,%d", id, g, key, v))
	}
	require.NoError(t, rows.Err())
	return result
}

func requireIssue31824Duplicate(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var mysqlErr *mysql.MySQLError
	require.ErrorAs(t, err, &mysqlErr)
	require.Equal(t, uint16(1062), mysqlErr.Number)
}

func requireIssue31824UniqueRowCount(t *testing.T, ctx context.Context, conn *sql.Conn, want int) {
	t.Helper()
	var total, distinctKeys int
	require.NoError(t, conn.QueryRowContext(ctx,
		"select count(*), count(distinct k) from t").Scan(&total, &distinctKeys))
	require.Equal(t, want, total)
	require.Equal(t, total, distinctKeys)
}

func queryIssue31824Rows(t *testing.T, ctx context.Context, conn *sql.Conn, query string) []string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	var result []string
	for rows.Next() {
		var id, g, v int
		var k sql.NullInt64
		require.NoError(t, rows.Scan(&id, &g, &k, &v))
		key := "NULL"
		if k.Valid {
			key = fmt.Sprint(k.Int64)
		}
		result = append(result, fmt.Sprintf("%d,%d,%s,%d", id, g, key, v))
	}
	require.NoError(t, rows.Err())
	return result
}
