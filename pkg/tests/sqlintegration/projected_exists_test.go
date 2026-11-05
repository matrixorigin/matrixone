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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestProjectedExistenceNullableKeys(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists projected_existence")
		for _, q := range []string{"drop database if exists projected_existence", "create database projected_existence", "use projected_existence",
			"create table p(id int,tenant int,k int,v int)", "create table w(tenant int,k int,v int)",
			"insert into p values(1,1,1,2),(2,1,1,9),(3,1,NULL,2),(4,1,3,2),(5,2,1,2),(6,9,1,2),(8,1,1,2)",
			"insert into w values(1,1,NULL),(1,1,2),(1,1,2),(1,3,2),(2,1,NULL)"} {
			execSQLRequire(t, ctx, db, q)
		}
		query := `select t.id,
   exists(select 1 from w s where s.tenant=t.tenant and s.k=t.k and s.v=t.v),
   not exists(select 1 from w s where t.tenant=s.tenant and t.k=s.k and t.v=s.v),
   t.v in(select s.v from w s where s.tenant=t.tenant and s.k=t.k),
   t.v not in(select s.v from w s where s.tenant=t.tenant and s.k=t.k)
   from p t order by t.id`
		expected := [][]any{{1, 1, 0, 1, 0}, {2, 0, 1, nil, nil}, {3, 0, 1, 0, 1}, {4, 1, 0, 1, 0}, {5, 0, 1, nil, nil}, {6, 0, 1, 0, 1}, {8, 1, 0, 1, 0}}
		check := func(prepared *sql.Stmt) {
			t.Helper()
			var rows *sql.Rows
			var err error
			if prepared == nil {
				rows, err = db.QueryContext(ctx, query)
			} else {
				rows, err = prepared.QueryContext(ctx)
			}
			require.NoError(t, err)
			defer rows.Close()
			var got [][]any
			for rows.Next() {
				var id int
				var vals [4]sql.NullInt64
				require.NoError(t, rows.Scan(&id, &vals[0], &vals[1], &vals[2], &vals[3]))
				row := []any{id}
				for _, v := range vals {
					if v.Valid {
						row = append(row, int(v.Int64))
					} else {
						row = append(row, nil)
					}
				}
				got = append(got, row)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, expected, got)
		}
		statement, err := db.PrepareContext(ctx, query)
		require.NoError(t, err)
		defer statement.Close()
		for _, empty := range []bool{false, true} {
			if empty {
				execSQLRequire(t, ctx, db, "truncate table w")
				for _, row := range expected {
					row[1], row[2], row[3], row[4] = 0, 1, 0, 1
				}
			}
			check(nil)
			check(statement)
			// Scalar DISTINCT of an outer value shares the existing Boolean
			// witness owner but reconstructs NULL when there is no witness.
			func() {
				rows, err := db.QueryContext(ctx, "select t.id,(select distinct t.id from w s where s.tenant=t.tenant and s.k=t.k and s.v=t.v) from p t order by t.id")
				require.NoError(t, err)
				defer rows.Close()
				index := 0
				for rows.Next() {
					var id int
					var scalar sql.NullInt64
					require.NoError(t, rows.Scan(&id, &scalar))
					require.Less(t, index, len(expected))
					require.Equal(t, expected[index][0], id)
					require.Equal(t, expected[index][1] == 1, scalar.Valid)
					if scalar.Valid {
						require.Equal(t, id, int(scalar.Int64))
					}
					index++
				}
				require.NoError(t, rows.Err())
				require.Equal(t, len(expected), index)
			}()
		}
		// Invalid/fallible predicates must retain normal errors rather than disappear
		// behind a membership summary.
		execSQLRequire(t, ctx, db, "create table invalid_hll(k int,v int,payload varchar(20))")
		execSQLRequire(t, ctx, db, "insert into invalid_hll values(1,2,'bad')")
		for _, query := range []string{
			"select exists(select 1 from w s where s.missing=t.k) from p t",
			"select exists(select 1 from invalid_hll s where s.k=t.k and s.v=t.v and hll_cardinality(cast(s.payload as varbinary))=t.id) from p t",
		} {
			func() {
				rows, err := db.QueryContext(ctx, query)
				if rows != nil {
					defer rows.Close()
					for rows.Next() {
					}
					if err == nil {
						err = rows.Err()
					}
				}
				require.Error(t, err, "membership lowering must not suppress binding/runtime errors")
			}()
		}
		// The two-valued producer proof must reach physical execution for nullable
		// composite equalities; IN above independently asserts UNKNOWN remains visible.
		execSQLRequire(t, ctx, db, "insert into w values(1,1,NULL),(1,1,2)")
		for _, tc := range []struct{ query, operator string }{
			{"select exists(select 1 from w s where s.tenant=t.tenant and s.k=t.k and s.v=t.v) from p t", "hash join"},
			{"select (select distinct t.id from w s where s.tenant=t.tenant and s.k=t.k and s.v=t.v) from p t", "hash join"},
			{"select exists(select 1 from w s where s.tenant=t.tenant and s.k=t.k and s.v=t.v) from (select * from p order by id limit 1) t", "loop join"},
		} {
			func() {
				rows, err := db.QueryContext(ctx, "explain phyplan analyze "+tc.query)
				require.NoError(t, err)
				defer rows.Close()
				var lines []string
				for rows.Next() {
					var line string
					require.NoError(t, rows.Scan(&line))
					lines = append(lines, line)
				}
				require.NoError(t, rows.Err())
				text := strings.Join(lines, "\n")
				require.Contains(t, text, tc.operator)
				if tc.operator == "hash join" {
					require.NotContains(t, text, "loop join")
				}
			}()
		}
	})
}
