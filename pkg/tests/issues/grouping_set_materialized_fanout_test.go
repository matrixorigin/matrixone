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
	"github.com/stretchr/testify/require"
)

const groupingSetMaterializedFanoutBody = `
select gs.k, count(gs.payload) as n
from grouping_source gs
group by rollup(gs.k)`

func TestGroupingSetMaterializedFanoutWithLazyUnion(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const database = "grouping_set_materialized_fanout"
		execSQLMaybe(t, ctx, db, "drop database if exists "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "create database "+database)
		execSQLRequire(t, ctx, db, "use "+database)
		execSQLRequire(t, ctx, db, "create table grouping_source (k bigint primary key, payload varchar(1000))")
		execSQLRequire(t, ctx, db,
			"insert into grouping_source select result, repeat('x', 1000) from generate_series(1, 10000) g")
		execSQLRequire(t, ctx, db, "create table grouping_dim_1 (k bigint primary key)")
		execSQLRequire(t, ctx, db,
			"insert into grouping_dim_1 select result from generate_series(1, 10000) g")
		execSQLRequire(t, ctx, db, "create table grouping_nested (k int)")
		execSQLRequire(t, ctx, db, "insert into grouping_nested values (1), (null)")
		execSQLRequire(t, ctx, db, "analyze table grouping_source, grouping_dim_1")
		execSQLRequire(t, ctx, db, "analyze table grouping_nested")
		execSQLRequire(t, ctx, db, "create table grouping_empty (a int, b int, v int)")
		execSQLRequire(t, ctx, db,
			"insert into grouping_empty select result, result % 10, result from generate_series(1, 1000) g")
		execSQLRequire(t, ctx, db, "delete from grouping_empty")

		const emptyGrouping = `select a, b, count(*) as c, sum(v) as s, grouping(a,b) as g
			from grouping_empty
			group by grouping sets ((a,b),(a),())
			order by g,a,b`
		var emptyA, emptyB, emptySum sql.NullInt64
		var emptyCount, emptyGroupingID int64
		emptyRows, err := db.QueryContext(ctx, emptyGrouping)
		require.NoError(t, err)
		defer emptyRows.Close()
		require.True(t, emptyRows.Next())
		require.NoError(t, emptyRows.Scan(
			&emptyA, &emptyB, &emptyCount, &emptySum, &emptyGroupingID))
		require.False(t, emptyA.Valid)
		require.False(t, emptyB.Valid)
		require.Zero(t, emptyCount)
		require.False(t, emptySum.Valid)
		require.Equal(t, int64(3), emptyGroupingID)
		require.False(t, emptyRows.Next())
		require.NoError(t, emptyRows.Err())
		require.NoError(t, emptyRows.Close())

		const nestedGrouping = `select d.k, count(*)
			from (select k from grouping_nested group by rollup(k)) d
			cross join grouping_dim_1 big
			group by rollup(d.k)
			order by count(*), d.k`
		nestedPlan := explainSQL(t, ctx, db, "explain "+nestedGrouping)
		require.NotContains(t, nestedPlan, "Sink Scan",
			"nested grouping extensions must keep the legacy plan:\n%s", nestedPlan)
		nestedRows, err := db.QueryContext(ctx, nestedGrouping)
		require.NoError(t, err)
		defer nestedRows.Close()
		type nestedResult struct {
			key   sql.NullInt64
			count int64
		}
		var nestedResults []nestedResult
		for nestedRows.Next() {
			var result nestedResult
			require.NoError(t, nestedRows.Scan(&result.key, &result.count))
			nestedResults = append(nestedResults, result)
		}
		require.NoError(t, nestedRows.Err())
		require.NoError(t, nestedRows.Close())
		require.Equal(t, []nestedResult{
			{key: sql.NullInt64{Int64: 1, Valid: true}, count: 10000},
			{key: sql.NullInt64{}, count: 20000},
			{key: sql.NullInt64{}, count: 30000},
		}, nestedResults,
			"an inherited rollup sentinel must not split the outer SQL NULL group")

		planText := explainSQL(t, ctx, db,
			"explain "+groupingSetMaterializedFanoutBody)
		require.Equal(t, 2, strings.Count(planText, "Sink Scan"),
			"ROLLUP must use the shared two-reader fanout:\n%s", planText)
		require.Equal(t, 1, strings.Count(planText, ".grouping_source"),
			"the shared producer must scan the source once:\n%s", planText)

		// The 10,001-row shared aggregate emits more batches than the ordinary
		// two-reader broadcast spool can retain. Draining the complete result
		// forces the first lazy UNION ALL branch to reach EOF before the second
		// branch starts.
		drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
		rows, err := db.QueryContext(drainCtx, groupingSetMaterializedFanoutBody)
		require.NoError(t, err)
		defer rows.Close()
		var key sql.NullInt64
		var count int64
		rowCount := 0
		for rows.Next() {
			require.NoError(t, rows.Scan(&key, &count))
			rowCount++
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, 10001, rowCount)
		drainCancel()

		// Closing the client after its first row takes the early-stop path and must
		// release the producer and every unstarted materialized reader so the
		// session can immediately execute another statement.
		earlyRows, err := db.QueryContext(ctx, groupingSetMaterializedFanoutBody)
		require.NoError(t, err)
		defer earlyRows.Close()
		require.True(t, earlyRows.Next())
		require.NoError(t, earlyRows.Scan(&key, &count))
		require.NoError(t, earlyRows.Close())
		require.NoError(t, earlyRows.Err())

		followupCtx, followupCancel := context.WithTimeout(ctx, 10*time.Second)
		defer followupCancel()
		require.NoError(t, db.QueryRowContext(followupCtx,
			"select count(*) from grouping_source").Scan(&count))
		require.Equal(t, int64(10000), count)
	})
}
