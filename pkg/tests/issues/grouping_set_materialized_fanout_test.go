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
select gs.k, count(*) as n
from grouping_source gs
join grouping_dim_1 d1 on gs.k = d1.k
join grouping_dim_2 d2 on gs.k = d2.k
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
		for _, table := range []string{"grouping_source", "grouping_dim_1", "grouping_dim_2"} {
			execSQLRequire(t, ctx, db, "create table "+table+" (k bigint primary key)")
			execSQLRequire(t, ctx, db,
				"insert into "+table+" select result from generate_series(1, 10000) g")
		}
		execSQLRequire(t, ctx, db, "analyze table grouping_source, grouping_dim_1, grouping_dim_2")

		planText := explainSQL(t, ctx, db,
			"explain select * from ("+groupingSetMaterializedFanoutBody+") grouped limit 1 offset 1000000")
		require.Equal(t, 2, strings.Count(planText, "Sink Scan"),
			"ROLLUP must use the shared two-reader fanout:\n%s", planText)
		require.Equal(t, 1, strings.Count(planText, ".grouping_source"),
			"the shared producer must scan the source once:\n%s", planText)

		// The 10,001-row shared aggregate emits more batches than the ordinary
		// two-reader broadcast spool can retain. OFFSET forces the first lazy
		// UNION ALL branch to drain to EOF before the second branch starts.
		drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
		rows, err := db.QueryContext(drainCtx,
			"select * from ("+groupingSetMaterializedFanoutBody+") grouped limit 1 offset 1000000")
		require.NoError(t, err)
		defer rows.Close()
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		drainCancel()

		// LIMIT takes the early-stop path. Closing after its first row must release
		// the producer and every unstarted materialized reader so the session can
		// immediately execute another statement.
		earlyRows, err := db.QueryContext(ctx,
			"select * from ("+groupingSetMaterializedFanoutBody+") grouped limit 1")
		require.NoError(t, err)
		defer earlyRows.Close()
		require.True(t, earlyRows.Next())
		var key sql.NullInt64
		var count int64
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
