// Copyright 2021 - 2026 Matrix Origin
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

// TestIssue33258CompositeEnumAndYearIndexLookup covers literal and binary-
// prepared predicates over inline and backfilled composite secondary indexes.
func TestIssue33258CompositeEnumAndYearIndexLookup(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			mustExec(t, ctx, conn, "use mo_catalog")
			mustExec(t, ctx, conn, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()

		mustExec(t, ctx, conn, "create table enum_index (id int primary key, tag varchar(8), e enum('x','y'), key ix(tag,e))")
		mustExec(t, ctx, conn, "insert into enum_index values (1,'k','x'),(2,'k','y'),(3,'k2','x'),(4,'k',null)")
		mustExec(t, ctx, conn, "create table year_index (id int primary key, tag varchar(8), y year, key ix(tag,y))")
		mustExec(t, ctx, conn, "insert into year_index values (1,'k',2026),(2,'k',2025),(3,'k2',2026),(4,'k',null)")

		readIDs := func(t *testing.T, rows *sql.Rows) []int {
			t.Helper()
			defer rows.Close()
			var got []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				got = append(got, id)
			}
			require.NoError(t, rows.Err())
			return got
		}

		runLookups := func(t *testing.T) {
			cases := []struct {
				name string
				sql  string
				want []int
			}{
				{"enum equality", "select id from enum_index where tag='k' and e='x'", []int{1}},
				{"enum in", "select id from enum_index where tag='k' and e in ('x','y') order by id", []int{1, 2}},
				{"enum forced index", "select id from enum_index force index(ix) where tag='k' and e='x'", []int{1}},
				{"enum no-index control", "select id from enum_index use index() where tag='k' and e='x'", []int{1}},
				{"year equality", "select id from year_index where tag='k' and y=2026", []int{1}},
				{"year in", "select id from year_index where tag='k' and y in (2025,2026) order by id", []int{1, 2}},
				{"year forced index", "select id from year_index force index(ix) where tag='k' and y=2026", []int{1}},
				{"year no-index control", "select id from year_index use index() where tag='k' and y=2026", []int{1}},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					rows, err := conn.QueryContext(ctx, tc.sql)
					require.NoError(t, err, tc.sql)
					defer rows.Close()
					require.Equal(t, tc.want, readIDs(t, rows))
					require.NoError(t, rows.Err())
				})
			}

			preparedCases := []struct {
				name  string
				query string
				args  []any
				want  []int
			}{
				{"enum prepared equality", "select id from enum_index force index(ix) where tag=? and e=?", []any{"k", "x"}, []int{1}},
				{"enum prepared in", "select id from enum_index force index(ix) where tag=? and e in (?,?) order by id", []any{"k", "x", "y"}, []int{1, 2}},
				{"enum prepared null", "select id from enum_index force index(ix) where tag=? and e <=> ?", []any{"k", nil}, []int{4}},
				{"year prepared equality", "select id from year_index force index(ix) where tag=? and y=?", []any{"k", 2026}, []int{1}},
				{"year prepared in", "select id from year_index force index(ix) where tag=? and y in (?,?) order by id", []any{"k", 2025, 2026}, []int{1, 2}},
				{"year prepared null", "select id from year_index force index(ix) where tag=? and y <=> ?", []any{"k", nil}, []int{4}},
			}
			for _, tc := range preparedCases {
				t.Run(tc.name, func(t *testing.T) {
					stmt, err := conn.PrepareContext(ctx, tc.query)
					require.NoError(t, err, tc.query)
					defer stmt.Close()
					rows, err := stmt.QueryContext(ctx, tc.args...)
					require.NoError(t, err, tc.query)
					defer rows.Close()
					require.Equal(t, tc.want, readIDs(t, rows))
					require.NoError(t, rows.Err())
				})
			}
		}

		t.Run("inline index", runLookups)
		mustExec(t, ctx, conn, "alter table enum_index drop index ix")
		mustExec(t, ctx, conn, "alter table enum_index add key ix(tag,e)")
		mustExec(t, ctx, conn, "alter table year_index drop index ix")
		mustExec(t, ctx, conn, "alter table year_index add key ix(tag,y)")
		t.Run("backfilled index", runLookups)
	})
}
