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

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue31824MultiTargetUpdateIgnoreUnique(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		_, err = conn.ExecContext(ctx, "create database "+dbName)
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, "drop database if exists "+dbName)
		}()
		_, err = conn.ExecContext(ctx, "use "+dbName)
		require.NoError(t, err)

		type row struct {
			id int
			u  sql.NullInt64
			v  int
		}
		readRows := func(t *testing.T) []row {
			rs, err := conn.QueryContext(ctx, "select id, u, v from t order by id")
			require.NoError(t, err)
			defer rs.Close()

			var rows []row
			for rs.Next() {
				var r row
				require.NoError(t, rs.Scan(&r.id, &r.u, &r.v))
				rows = append(rows, r)
			}
			require.NoError(t, rs.Err())
			return rows
		}
		forceIndexCount := func(t *testing.T, index, key string) int {
			var count int
			query := fmt.Sprintf("select count(*) from t force index (%s) where %s", index, key)
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count))
			return count
		}
		prepare := func(t *testing.T, ddl string) {
			_, err := conn.ExecContext(ctx, "drop table if exists t")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, ddl)
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "insert into t values (1, 10, 100), (2, 20, 200), (3, 500, 300)")
			require.NoError(t, err)
		}

		t.Run("same new key follows target alias order", func(t *testing.T) {
			prepare(t, "create table t (id int primary key, u int, v int, unique key uk_u (u))")
			res, err := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=2 set x.u=30, y.u=30")
			require.NoError(t, err)
			affected, err := res.RowsAffected()
			require.NoError(t, err)
			require.EqualValues(t, 1, affected)
			require.Equal(t, []row{{1, sql.NullInt64{Int64: 30, Valid: true}, 100}, {2, sql.NullInt64{Int64: 20, Valid: true}, 200}, {3, sql.NullInt64{Int64: 500, Valid: true}, 300}}, readRows(t))
			require.Equal(t, 1, forceIndexCount(t, "uk_u", "u=30"))
		})

		t.Run("reversed target alias order reverses the winner", func(t *testing.T) {
			prepare(t, "create table t (id int primary key, u int, v int, unique key uk_u (u))")
			res, err := conn.ExecContext(ctx, "update ignore t x join t y on x.id=2 and y.id=1 set x.u=30, y.u=30")
			require.NoError(t, err)
			affected, err := res.RowsAffected()
			require.NoError(t, err)
			require.EqualValues(t, 1, affected)
			require.Equal(t, []row{{1, sql.NullInt64{Int64: 10, Valid: true}, 100}, {2, sql.NullInt64{Int64: 30, Valid: true}, 200}, {3, sql.NullInt64{Int64: 500, Valid: true}, 300}}, readRows(t))
			require.Equal(t, 1, forceIndexCount(t, "uk_u", "u=30"))
		})

		t.Run("composite unique conflict is atomic per alias", func(t *testing.T) {
			prepare(t, "create table t (id int primary key, u int, v int, unique key uk_uv (u, v))")
			res, err := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=2 set x.u=30, x.v=300, y.u=30, y.v=300")
			require.NoError(t, err)
			affected, err := res.RowsAffected()
			require.NoError(t, err)
			require.EqualValues(t, 1, affected)
			require.Equal(t, []row{{1, sql.NullInt64{Int64: 30, Valid: true}, 300}, {2, sql.NullInt64{Int64: 20, Valid: true}, 200}, {3, sql.NullInt64{Int64: 500, Valid: true}, 300}}, readRows(t))
			require.Equal(t, 1, forceIndexCount(t, "uk_uv", "u=30 and v=300"))
		})

		t.Run("rejected alias does not erase a valid sibling contribution", func(t *testing.T) {
			prepare(t, "create table t (id int primary key, u int, v int, unique key uk_u (u))")
			res, err := conn.ExecContext(ctx, "update ignore t x join t y on x.id=1 and y.id=1 set x.u=500, y.v=600")
			require.NoError(t, err)
			affected, err := res.RowsAffected()
			require.NoError(t, err)
			require.EqualValues(t, 1, affected)
			require.Equal(t, []row{{1, sql.NullInt64{Int64: 10, Valid: true}, 600}, {2, sql.NullInt64{Int64: 20, Valid: true}, 200}, {3, sql.NullInt64{Int64: 500, Valid: true}, 300}}, readRows(t))
			require.Equal(t, 1, forceIndexCount(t, "uk_u", "u=500"))
		})
	})
}
