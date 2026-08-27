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

func TestPartialSumPushdownPreservesDuplicateDimensionAttributes(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const database = "agg_dimension_pushdown"
		execSQLMaybe(t, ctx, db, "drop database if exists "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "create database "+database)
		execSQLRequire(t, ctx, db, "use "+database)
		execSQLRequire(t, ctx, db, "create table dim (id int primary key, label varchar(20))")
		execSQLRequire(t, ctx, db, "create table fact (dim_id int, amount int)")
		execSQLRequire(t, ctx, db, "insert into dim values (1, 'same'), (2, 'same'), (3, 'other')")
		execSQLRequire(t, ctx, db, `insert into fact values
			(1, 1), (1, 2), (1, 3), (1, 4),
			(2, 10), (2, 20), (2, 30), (2, 40),
			(3, 100), (3, 200), (3, 300), (3, 400)`)
		execSQLRequire(t, ctx, db, "analyze table dim, fact")

		const query = `select d.label, sum(f.amount) as total
			from fact f join dim d on f.dim_id = d.id
			group by d.label order by d.label`
		planText := explainSQL(t, ctx, db, "explain "+query)
		require.Equal(t, 2, strings.Count(planText, "Aggregate"),
			"a partial aggregate should reduce the fact side while the final aggregate remains:\n%s", planText)

		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var label string
			var total int64
			require.NoError(t, rows.Scan(&label, &total))
			got = append(got, fmt.Sprintf("%s=%d", label, total))
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{"other=1000", "same=110"}, got,
			"equal labels from different primary keys must still merge in the final aggregate")
	})
}
