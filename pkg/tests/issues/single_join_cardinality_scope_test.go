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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// TestSingleJoinPushdownPreservesCardinalityErrorScope proves that a scalar
// subquery is not evaluated for rows eliminated by an earlier relational
// operator. Moving SINGLE below that operator would make its cardinality error
// observable even though the original query has no rows on which to evaluate
// the scalar predicate.
func TestSingleJoinPushdownPreservesCardinalityErrorScope(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const database = "single_join_cardinality_scope"
		execSQLMaybe(t, ctx, db, "drop database if exists "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "create database "+database)
		execSQLRequire(t, ctx, db, "use "+database)
		execSQLRequire(t, ctx, db, "create table outer_rows (id int)")
		execSQLRequire(t, ctx, db, "create table eliminating_rows (id int)")
		execSQLRequire(t, ctx, db, "create table matching_rows (id int)")
		execSQLRequire(t, ctx, db, "create table scalar_rows (id int)")
		execSQLRequire(t, ctx, db, "create table scalar_empty (id int)")
		execSQLRequire(t, ctx, db, "create table scalar_null (id int)")
		execSQLRequire(t, ctx, db, "create table scalar_no_match (id int)")
		execSQLRequire(t, ctx, db, "create table fallible_rows (id int, payload varchar(20))")
		execSQLRequire(t, ctx, db, "insert into outer_rows values (1)")
		execSQLRequire(t, ctx, db, "insert into eliminating_rows values (2)")
		execSQLRequire(t, ctx, db, "insert into matching_rows values (1)")
		execSQLRequire(t, ctx, db, "insert into scalar_rows values (1), (2)")
		execSQLRequire(t, ctx, db, "insert into scalar_null values (null)")
		execSQLRequire(t, ctx, db, "insert into scalar_no_match values (2)")
		execSQLRequire(t, ctx, db, "insert into fallible_rows values (1, 'bad')")

		emptyQueries := []struct {
			name  string
			query string
		}{
			{"inner", `select o.id
				from outer_rows o join eliminating_rows e on o.id = e.id
				where o.id = (select s.id from scalar_rows s)`},
			{"semi", `select o.id from outer_rows o
				where exists (select 1 from eliminating_rows e where e.id = o.id)
				and o.id = (select s.id from scalar_rows s)`},
			{"anti", `select o.id from outer_rows o
				where not exists (select 1 from matching_rows e where e.id = o.id)
				and o.id = (select s.id from scalar_rows s)`},
		}
		for _, test := range emptyQueries {
			t.Run(test.name+" eliminates probe row", func(t *testing.T) {
				rows, err := db.QueryContext(ctx, test.query)
				require.NoError(t, err)
				defer rows.Close()
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			})
		}

		errorQueries := []struct {
			name  string
			query string
		}{
			{"inner", `select o.id
				from outer_rows o join matching_rows e on o.id = e.id
				where o.id = (select s.id from scalar_rows s)`},
			{"semi", `select o.id from outer_rows o
				where exists (select 1 from matching_rows e where e.id = o.id)
				and o.id = (select s.id from scalar_rows s)`},
			{"anti", `select o.id from outer_rows o
				where not exists (select 1 from eliminating_rows e where e.id = o.id)
				and o.id = (select s.id from scalar_rows s)`},
		}
		for _, test := range errorQueries {
			t.Run(test.name+" preserves cardinality error", func(t *testing.T) {
				rows, queryErr := db.QueryContext(ctx, test.query)
				if queryErr == nil {
					defer func() {
						require.NoError(t, rows.Close())
					}()
					for rows.Next() {
					}
					queryErr = rows.Err()
				}
				require.ErrorContains(t, queryErr, "Subquery returns more than 1 row")
			})
		}

		// A scalar runtime filter may discard rows only on the finalized physical
		// probe lineage. If it were attached to a hash-build input, these scalar
		// states could empty that build and suppress the fallible sibling scan.
		// The error is observable before the scalar predicate at its original
		// logical position and therefore must remain observable.
		for _, scalarTable := range []string{"scalar_empty", "scalar_null", "scalar_no_match"} {
			t.Run("scalar filter does not suppress sibling error "+scalarTable, func(t *testing.T) {
				query := `select o.id from outer_rows o join fallible_rows f
					on o.id = f.id
					and hll_cardinality(cast(f.payload as varbinary)) >= 0
					where o.id = (select s.id from ` + scalarTable + ` s)`
				rows, queryErr := db.QueryContext(ctx, query)
				if queryErr == nil {
					func() {
						defer rows.Close()
						for rows.Next() {
						}
						queryErr = rows.Err()
					}()
				}
				require.Error(t, queryErr)
			})
		}
	})
}
